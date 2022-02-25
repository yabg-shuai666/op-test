import json
import pygdbt
import encoder
import fztools
from pygdbt.common import *
from feql import convert as feql_convert
from fesql import convert as sql_convert

class FeaInfo:
    def __init__(self, name, slot, enc, feql=None):
        self.name    = name
        self.slot    = slot
        self.encoder = enc
        self.feql    = feql or name

    def __lt__(self, other):
        return self.name < other.name

    @staticmethod
    def gen(id, slot, enc):
        name = id.split('.')[-1]
        if isinstance(enc, encoder.SepString):
            feql = "split(%s,%d)" % (id, ord(enc.sep))
        elif isinstance(enc, encoder.KvString) or isinstance(enc, encoder.KvStringNum):
            feql = "split_key(%s,%d,%d)" % (id, ord(enc.sep), ord(enc.kv_sep))
        else:
            feql = id
        return FeaInfo(name, slot, enc, feql)

def encoder_map(x):
    if x[:11] == 'ArrayString':
        sep = x[11:].strip('()')
        return encoder.SepString(sep)
    if x[:5] == 'KVNum':
        tmp = x[5:].split('[', 1)
        sep = tmp[0].strip('()')
        kv_sep = tmp[1].strip('[]')
        return encoder.KvStringNum(sep, kv_sep)
    if x[:8] == 'KVString':
        tmp = x[8:].split('[', 1)
        sep = tmp[0].strip('()')
        kv_sep = tmp[1].strip('[]')
        return encoder.KvString(sep, kv_sep)
    return {
        'SingleString'  : encoder.String(),
        'Timestamp'     : encoder.Timestamp(),
        'Date'          : encoder.StringDate(),
        'DiscreteLabel' : encoder.Num(),
        'ContinueLabel' : encoder.Num(),
        'Label'         : encoder.Num(),
        'DiscreteNum'   : encoder.DiscNum(),
        'ContinueNum'   : encoder.Num(),
        'SplitID'       : encoder.Ignore(),
    }[x]

class FZData:
    def __init__(self, name):
        self.name = name
        self.reset()

    def append_fea(self, f):
        self.col2idx[f.name] = len(self.info)
        self.info.append(f)

    def get_slots(self, names):
        return list(map(self.get_slot, names))

    def get_slot(self, name):
        return self.col2idx[name]

    def num_cols(self):
        return len(self.info)

    def reset(self):
        self.info = []
        self.data = None
        self.col2idx = {}
        self.offset = 0

    def columns(self):
        return map3(lambda f: f.name, self.info)

    def trainable_slots(self, oft = 0):
        ret = []
        for f in self.info:
            if not isinstance(f.encoder, encoder.Ignore) and not isinstance(f.encoder, encoder.Label):
                ret.append(f.slot + oft)
        return ret

class RawData(FZData):
    def __init__(self, conf, info):
        FZData.__init__(self, conf.name)
        self.conf = conf
        self.keys = []

        self.load_info(info)
        self.map_uri()

    def load_info(self, info):
        for slot, f in enumerate(info['features']):
            if f['skip']:
                enc = encoder.Ignore()
            else:
                enc  = encoder_map(f['data_type'])
            self.append_fea(FeaInfo.gen(f['id'], slot, enc))

    def load_data(self, exists, lkey, rkey, label, time=None):
        data = pygdbt.Parquet(self.conf.uri, self.name)
        enc  = ','.join(map(lambda x: str(x.encoder), self.info))
        if not label:
            enc = enc.replace("Label", "Num")
        if time is not None:
            self.time_stat = fztools.SignCountTable()
        def graph():
            block = data.Read()
            block = layers.DataFrame2InstanceBlock(block, enc)
            block = fztools.FilterBySign(block, exists, lkey, rkey)
            layers.Sink(block, self.data)
            if time is not None:
                sparse = layers.ParseSparseInput(block, layers.ParseType.SLOTS, [time])
                fztools.SignCountStat(sparse, self.time_stat)
        pygdbt.execute(graph)
        self.offset = self.num_cols()
        cols = map3(lambda x: x.name, self.info)
        printf('cached', self.name, fztools.gc_memory_size(self.data), [self.data.size(), len(self.info)], self.data.global_size(), cols)
        mem_info()

    def map_uri(self):
        uri_conf = {
            'block_size'            : self.conf.block_size,
            'is_use_global_shuffle' : self.conf.is_use_global_shuffle
        }
        if self.conf.data_type == 'parquet':
            uri_conf['format'] = 'parquet'
        else:
            sep    = ',' if self.conf.data_type == 'csv' else '\t'
            header = sep.join(map(lambda x: x.name, self.info))

            uri_conf.update({
                'format' : 'csv',
                'header_rows' : 0,
                'header' : header,
                'delimiter' : sep
            })

        for uri in self.conf.uri:
            uri.conf.update(uri_conf)

    def dump_features(self, pas, fz, feas, fixed=[]):
        if pas > 0 and pico_tools.comm_rank() == 0:
            pico_tools.file_mv( os.path.join(fz.conf.model_output_path.path, 'pass-final'),
                                os.path.join(fz.conf.model_output_path.path, 'pass-' + str(pas - 1)))

        selected_ops = []
        for f in self.info:
            if isinstance(f.encoder, encoder.Ignore):
                continue
            feql = f.feql
            if f.slot < len(fz.fea_ctx.raw_feas) and not encoder.is_multi(f.encoder):
                feql = "original(%s)" % feql
            if isinstance(f.encoder, encoder.Label):
                if fz.conf.task_type == 'binary':
                    feql = "binary_label(%s)" % feql
                else:
                    feql = "regression_label(%s)" % feql
            selected_ops.append(feql)

        selected_ops += map3(lambda x: x[1].name, fixed)

        ans = sorted(feas, key = lambda x: (x[0], x[1].name), reverse = True)
        ans = unique(ans, key = lambda x: x[0])
        ans = ans[:fz.conf.selected_op_num[1]]
        while len(ans) > fz.conf.selected_op_num[0] and ans[-1][0] <= fz.base_line:
            del ans[-1]
        for f in ans:
            printf('selected : %s : %f' % (f[1].name, f[0]))
            selected_ops.append(f[1].name)

        print("pass #%d" % pas)
        selected_ops = '\n'.join(selected_ops)
        print(selected_ops)

        ok, feql, column_feql, sign_feql, fe_config = feql_convert.get_feql(selected_ops, fz.multi_ctx.info)
        ok, sql, sql_config, fe = sql_convert.to_sql(selected_ops, fz.multi_ctx.info)
        conf = json.dumps({'app' : fz.conf.to_dict()}, indent=4)

        path = fz.conf.model_output_path.path + '/pass-final'
        pico_tools.save_metadata(path + '/metadata')
        if pico_tools.comm_rank() == 0:
            pico_tools.save_file(path + '/selected_ops.bk', selected_ops)
            pico_tools.save_file(path + '/selected_ops', feql)
            pico_tools.save_file(path + '/selected_column', column_feql)
            pico_tools.save_file(path + '/selected_sign', sign_feql)
            pico_tools.save_file(path + '/fe_config.json', fe_config)
            pico_tools.save_file(path + '/pyconf.json', conf)

            pico_tools.save_file(path + '/selected_column_sql', sql)
            pico_tools.save_file(path + '/fe_config_sql.json', sql_config)
            pico_tools.save_file(path + '/selected_sign_sql', fe)

        fz.selected_ops += ans
