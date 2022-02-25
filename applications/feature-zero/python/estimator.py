# -*- coding: utf-8 -*-

from pygdbt.common.configure import *
from pygdbt.common.checker import *
from pygdbt.app_common import *
from configure import *

class FZEstimator(ResEstimator):
    class DataConf(Configure):
        def __init__(self):
            Configure.__init__(self)
            self.declare('num_rows', 'global data row size', True, GreaterChecker(0), 0)
            self.declare('file_size', 'file size in disk', True, DefaultChecker(), '')

    class Conf(ResEstimator.Conf):
        def __init__(self):
            ResEstimator.Conf.__init__(self)
            self.declare('input_path', 'the input data path',
                False, DefaultChecker(), list_config(FZEstimator.DataConf))
            self.declare('output_path', 'result output path, empty if output to stdout',
                True, DefaultChecker(), '')
            self.declare('max_container_mem',  'max container mem',  False, GreaterChecker(6000))
            self.declare('max_available_mem',  'max container mem',  False, GreaterChecker(6000))
            self.declare('max_container_num',  'max container num',  False, GreaterChecker(0), 1)

    def __init__(self, yaml):
        self.fz_conf = FZConf()
        self.es_conf = FZEstimator.Conf()
        app_name = yaml['app_name']

        if not self.fz_conf.load_config(yaml[app_name]):
            raise ValueError('invalid configure.')
        if not self.es_conf.load_config(yaml['res_estimate']):
            raise ValueError('invalid configure.')

        info_str = self.fz_conf.feature_info_str
        if not info_str:
            info_str = pico_tools.load_file(self.fz_conf.feature_info)
        self.info = json.loads(info_str)

    def _data_size(self, type, size):
        unit_to_size = {
            'k' : 1 << 10,
            'm' : 1 << 20,
            'g' : 1 << 30,
            't' : 1 << 40
        }
        if not size.isdigit():
            try:
                unit = size[-1].lower()
                size = float(size[:-1]) * unit_to_size[unit]
            except:
                raise Exception('invalid file size format [%s]' % size)
        else:
            size = float(size)
        return size * 20 if type == 'parquet' else size * 2

    def _calc_size(self, info, conf):
        s = 0
        for fea in info['features']:
            typ = fea['feature_type']
            if typ == 'SmallInt':
                c = 2
            if typ == 'Int' or typ == 'Float':
                c = 4
            elif typ == 'String':
                c = 16
            else:
                c = 8
            s += min(conf.num_rows, self.fz_conf.max_sample_rows) * (c + 1)
        s += 100 << 10
        return s

    def estimate(self):
        maxv = 0
        tot  = 0
        main = self.info['target_entity']
        for data, conf in zip(self.fz_conf.input_path, self.es_conf.input_path):
            size = self._data_size(data.data_type, conf.file_size)
            if data.name == main and conf.num_rows > self.fz_conf.max_sample_rows:
                size = size * self.fz_conf.max_sample_rows / conf.num_rows
            detail = self.info['entity_detail'][data.name]
            size = max(size, self._calc_size(detail, conf))
            maxv = max(maxv, size * 3)
            tot += size
            if data.name == main:
                main_rows = min(conf.num_rows, self.fz_conf.max_sample_rows)

        ovhd = (200 << 20) * (len(self.fz_conf.input_path) + 4) + (10 << 30)

        pserver_mem = maxv * len(self.fz_conf.lfc.bucket_list) + (4 << 30)
        learner_mem = ovhd + maxv
        if self.fz_conf.cache_uri.path[:6] == 'mem://':
            learner_mem += tot

        rebase_shape = main_rows * self.fz_conf.auto.combine_explore_limits * 20
        learner_mem += rebase_shape * 24
        pserver_mem  = max(pserver_mem, rebase_shape * 48)

        pserver_mem /= 1<<20
        learner_mem /= 1<<20

        conf = self.es_conf
        pserver_num = int(pserver_mem / conf.max_container_mem) + 1
        learner_num = int(learner_mem / conf.max_container_mem) + 1
        pserver = max((pserver_mem / pserver_num), 4 << 10)
        learner = max((learner_mem / learner_num), 4 << 10)
        if learner + pserver < conf.max_container_num - (6 << 10):
            learner_num = conf.max_container_num

        ret = {
            'learner': {
                "mem": int(learner),
                "num": learner_num,
                "cpu": 1,
            },
            'pserver': {
                "mem": int(pserver),
                "num": pserver_num,
                "cpu": 1,
            }
        }
        printf(json.dumps(ret, indent=4))

        flag = True
        if 0 > pserver_num or pserver_num > conf.max_container_num:
            logging.warning('invalid pserver num')
            flag = False
        if 0 > learner_num or learner_num > conf.max_container_num:
            logging.warning('invalid learner num')
            flag = False
        if 0 > pserver:
            logging.warning('invalid pserver mem')
            flag = False
        if 0 > learner:
            logging.warning('invalid learner mem')
            flag = False
        if pserver > conf.max_container_mem:
            logging.warning('pserver mem out of range')
            pserver = conf.max_container_mem
        if learner > conf.max_container_mem:
            logging.warning('learner mem out of range')
            learner = conf.max_container_mem

        master = 6000 * min(conf.max_container_num, learner_num)
        tot_mem = learner * learner_num + pserver * pserver_num + master
        if tot_mem > conf.max_available_mem:
            logging.warning('available memory not enough.')
            extra = tot_mem - conf.max_available_mem
            print('extra:', extra)
            learner_delta = extra * (learner * learner_num + master) / tot_mem
            ps_delta = extra * (pserver * pserver_num) / tot_mem

            while learner_num > 1 and learner_delta > learner:
                learner_num -= 1
                learner_delta -= learner
            while pserver_num > 1 and ps_delta > pserver:
                pserver_num -= 1
                ps_delta -= pserver
            learner -= learner_delta / learner_num
            pserver -= ps_delta / pserver_num
        elif 2 * learner < conf.max_container_mem:
            remain = conf.max_available_mem - pserver * pserver_num - master
            if remain / learner_num >= conf.max_container_mem / 2:
                if learner * 10 > conf.max_container_mem:
                    learner = conf.max_container_mem / 2
                else:
                    learner_num = max(1, learner_num * learner * 2 / conf.max_container_mem)
                    learner = conf.max_container_mem / 2

        # assert(flag)
        ret = {
            'learner': {
                "mem": int(learner),
                "num": learner_num,
                "cpu": 1,
            },
            'pserver': {
                "mem": int(pserver),
                "num": pserver_num,
                "cpu": 1,
            }
        }
        return ret
