# -*- coding: utf-8 -*-

import json, copy
import pygdbt
import fztools
from pygdbt.common import *
from pygdbt.app_common import *
from fea_info import *
import encoder as ENC
from feature_generator import win_

class MultiTable:
    def __init__(self, fz):
        self.ctx      = fz
        self.data_set = {}
        self.conf     = fz.conf
        self.exists   = None

        info_str = self.conf.feature_info_str
        if not info_str:
            info_str = pico_tools.load_file(self.conf.feature_info)
        self.info = json.loads(info_str)
        fz.conf.feature_info = self.info

    def root_data(self):
        return self.data_set[self.info['target_entity']]

    def load_main_table(self):
        root = self.root_data()
        data = pygdbt.Parquet(root.conf.uri)
        root.data = pygdbt.Cache(self.conf.cache_uri, root.name)
        encoder   = ','.join(map(lambda x: str(x.encoder), root.info))
        time_slot = root.get_slot(self.info['target_pivot_timestamp'])
        time_line = tables.QuantileIntTable(self.conf.test_ratio_accuracy, False)
        def sink():
            block = data.Read()
            gcins = layers.DataFrame2InstanceBlock(block, encoder)
            gcins = fztools.AssignIndex(gcins, fztools.index_col)
            layers.Sink(gcins, root.data)

            timec = layers.ParseSparseInput(gcins, layers.ParseType.SLOTS, [time_slot])
            layers.QuantileIntStat(timec, time_line)
        pygdbt.execute(sink)

        tot_ins_num  = root.data.global_size()
        if tot_ins_num > self.conf.max_sample_rows:
            cache = pygdbt.Cache(self.conf.cache_uri, root.name)
            dist  = time_line.pull(time_slot)
            ratio = (tot_ins_num - self.conf.max_sample_rows) / tot_ins_num
            ratio, cond = fztools._count_(dist, ratio)
            def split() :
                block = root.data.Read()
                patch = fztools.SplitBySign(block, time_slot, cond, ratio, "old", "new")
                train = patch.sub("new")
                layers.Sink(train, cache)
            pygdbt.execute(split)

            root.data.reset()
            root.data = cache

        root_relation_keys = []
        for edge in self.info['relations']:
            if edge['to_entity'] not in self.data_set:
                continue
            root_relation_keys += root.get_slots(edge['from_entity_keys'])

        root.distribution = tables.QuantileIntTable(self.conf.distribution_accuracy, True)
        self.exists = tables.QuantileIntTable(2 * int(self.conf.max_sample_rows), True)
        def stat():
            block  = root.data.Read()
            sparse = layers.ParseSparseInput(block, layers.ParseType.DISC)
            layers.QuantileIntStat(sparse, root.distribution)

            if len(root_relation_keys) > 0:
                relate = layers.ParseSparseInput(block, layers.ParseType.SLOTS, root_relation_keys)
                layers.QuantileIntStat(relate, self.exists)

            timec = layers.ParseSparseInput(block, layers.ParseType.SLOTS, [time_slot])
            layers.QuantileIntStat(timec, time_line)
        pygdbt.execute(stat)
        self.time_line = time_line

        shuf = fztools.partition_by(root.data, root.distribution, fztools.index_col)
        fztools.transfer_data(root.data, shuf, len(root.info))
        root.offset = root.num_cols()
        printf("load root table", fztools.gc_memory_size(root.data),
            [root.data.size(), len(root.info)], root.data.global_size(), root.columns())

        self.ctx.lr.index_dist = root.distribution
        self.ctx.sw.index_dist = root.distribution

    def join_base(self):
        root = self.root_data()
        for edge in self.info['relations']:
            if edge['type'][2] != '1' and edge['type'] != 'SLICE':
                continue
            side = edge['to_entity']
            if side not in self.data_set:
                continue
            SCHECK(root.name == edge['from_entity'])
            side = self.data_set[side]
            key1 = root.get_slots(edge['from_entity_keys'])
            key2 = side.get_slots(edge[  'to_entity_keys'])
            side.data = pygdbt.Cache(self.conf.cache_uri, side.name)
            side.load_data(self.exists, key1, key2, edge['type'] != 'SLICE')

            if edge['type'][2] == '1':
                fztools.left_join(root, side, edge['from_entity_keys'], edge['to_entity_keys'])
            else:
                w = edge['time_windows']
                if w is None:
                    w = str(0x7fffffff)
                elif isinstance(w, list):
                    w = w[0]

                fztools.last_join(root, side,
                    edge['from_entity_keys'], edge['to_entity_keys'],
                    edge['from_entity_time_col'], edge['to_entity_time_col'], w)

            side.data.reset()
            side.data = None
        printf("joined base", fztools.gc_memory_size(root.data),
            [root.data.size(), root.num_cols()], root.data.global_size(), root.columns())

    def train_base(self):
        root = self.root_data()
        time = root.get_slot(self.info['target_pivot_timestamp'])
        train, valid = fztools.split_data(root.data, self.time_line, time, self.conf.test_ratio)
        slots = root.trainable_slots()

        if self.conf.task_type == 'binary':
            self.ctx.lr.fit(train, slots, qdata=[train, valid])
            print(pico_tools.get_accumulator())
            erase_binary_accumulator()
        else:
            self.ctx.lr.conf.lr.link_function_for_label = 'log1p'
            self.ctx.lr.fit(train, slots, qdata=[train, valid])
            score = float(self.ctx.lr.validate(valid)['validation_real_mae'])
            log1p = (score, 'log1p', self.ctx.lr.table)
            printf("testing log1p", score)

            self.ctx.lr.table = None
            self.ctx.lr.conf.lr.link_function_for_label = 'identity'
            self.ctx.lr.fit(train, slots, qdata=[train, valid])
            score = float(self.ctx.lr.validate(valid)['validation_real_mae'])
            ident = (score, 'identity', self.ctx.lr.table)
            printf("testing identity", score)

            select = min(ident, log1p)
            other  = max(ident, log1p)
            print("getting", select[1])
            other[2].clear()
            self.ctx.lr.table = select[2]
            self.ctx.lr.conf.lr.link_function_for_label = select[1]
            print(pico_tools.get_accumulator())
            erase_regression_accumulator()

        # 主表以idx做partition始终不动，所以可以只set_base一次
        self.ctx.sw.set_base(self.ctx.lr, root.data, fztools.index_col)
        self.ctx.lr.quantile.reset()
        train.reset()
        valid.reset()

    def join_ops(self, op_datas):
        printf("joining", len(op_datas))
        root = self.root_data()
        time = root.get_slot(self.info['target_pivot_timestamp'])
        idxc = fztools.index_col

        main = pygdbt.Cache(self.conf.cache_uri)
        cache = pygdbt.Cache(self.conf.cache_uri)
        scores = {}
        offset = root.offset
        fztools.select(root.data, main, [fztools.index_col, time], True)
        for side, conf in op_datas:
            slots = side.trainable_slots(offset)
            printf("testing", slots)

            lpart = fztools.partition_by(main     , root.distribution, idxc, idxc)
            rpart = fztools.partition_by(side.data, root.distribution, idxc, idxc)
            fztools.core.left_join(lpart, rpart, [idxc], [idxc], root.offset, True)
            fztools.transfer_data(cache, lpart, side.num_cols() + 1)
            rpart.reset()

            train, valid = fztools.split_data(cache, self.time_line, time, self.conf.test_ratio)

            self.ctx.sw.fit(train, slots, qdata=[train, valid])
            score = self.ctx.sw.score(valid)
            self.ctx.sw.quantile.reset()
            train.reset()
            valid.reset()

            for f, c in zip(side.info, conf):
                if (f.slot + offset) in score:
                    s = score[f.slot + offset]
                else:
                    s = 0.0
                if c.slot not in scores:
                    scores[c.slot] = []
                scores[c.slot].append((s, f, side))
                print(f.name, ':', s)

        selected = {}
        for slot, score in scores.items():
            score.sort(reverse=True)
            score = unique(score, key=lambda x: x[0])
            del score[self.conf.multi_table_select_ratio:]
            for s, f, d in score:
                if d not in selected:
                    selected[d] = []
                selected[d].append(f.slot)
                printf("selected", f.name, s)

        root_part = fztools.partition_by(root.data, root.distribution, idxc, idxc)
        for side, conf in op_datas:
            if side not in selected:
                continue
            fztools.select(side.data, cache, [fztools.index_col] + selected[side], False)
            side.data.reset()
            side_part = fztools.partition_by(cache, root.distribution, idxc, idxc)
            fztools.core.left_join(root_part, side_part, [idxc], [idxc], root.offset, True)
            side_part.reset()

            for slot in selected[side]:
                f = copy.copy(side.info[slot])
                f.slot += root.offset
                root.append_fea(f)
            root.offset += len(side.info)
        cache.reset()
        fztools.transfer_data(root.data, root_part, root.num_cols() + 1)
        root_part.reset()

    def select_op(self):
        root = self.root_data()
        for edge in self.info['relations']:
            if edge['type'][2] == '1' or edge['type'] == 'SLICE':
                continue
            side = edge['to_entity']
            if side not in self.data_set:
                continue
            SCHECK(root.name == edge['from_entity'])
            side = self.data_set[side]
            key1 = root.get_slots(edge['from_entity_keys'])
            key2 = side.get_slots(edge[  'to_entity_keys'])
            side.data = pygdbt.Cache(self.conf.cache_uri, side.name)
            time_slot = side.get_slot(edge['to_entity_time_col'])
            side.load_data(self.exists, key1, key2, False, time_slot)
            edge['time_windows'] = self.conf.auto_windows(side.time_stat, time_slot,
                edge['time_windows'], edge['window_delay'] if 'window_delay' in edge else None)

            op_datas = []
            for w in edge['time_windows']:
                conf = []
                for f in side.info:
                    if f.name in edge['to_entity_keys']:
                        continue
                    if ENC.is_numeric(f.encoder):
                        name = "multi_avg(%s,%s,%s)" % (root.name, f.feql, win_(w))
                        conf.append(fztools.AggConf(name, 1, f.slot, ENC.Num()))
                        name = "multi_max(%s,%s,%s)" % (root.name, f.feql, win_(w))
                        conf.append(fztools.AggConf(name, 2, f.slot, ENC.Num()))
                        name = "multi_min(%s,%s,%s)" % (root.name, f.feql, win_(w))
                        conf.append(fztools.AggConf(name, 3, f.slot, ENC.Num()))
                    if ENC.is_category(f.encoder):
                        name = "multi_unique_count(%s,%s,%s)" % (root.name, f.feql, win_(w))
                        conf.append(fztools.AggConf(name, 4, f.slot, ENC.Num()))
                        name = "multi_top3frequency(%s,%s,%s)" % (root.name, f.feql, win_(w))
                        conf.append(fztools.AggConf(name, 5, f.slot, ENC.SepString()))
                tmp = fztools.table_join(root, side,
                    edge['from_entity_keys'], edge['to_entity_keys'],
                    edge['from_entity_time_col'], edge['to_entity_time_col'],
                    w, conf)
                op_datas.append((tmp, conf))
            side.data.reset()
            self.join_ops(op_datas)
        root.distribution.reset()
        self.exists.reset()

    def run(self):
        target_label = None
        for data_conf in self.conf.input_path:
            rd = RawData(data_conf, self.info['entity_detail'][data_conf.name])
            for f in rd.info:
                if f.feql == self.info['target_label']:
                    target_label = rd.name
                    f.encoder = ENC.Label()
            self.data_set[rd.name] = rd

        SCHECK(target_label is not None, "missing label or format error [%s]" % self.info['target_label'])
        SCHECK(self.info['target_entity'] in self.data_set, "undefined table " + self.info['target_entity'])
        for edge in self.info['relations']:
            SCHECK(edge['from_entity'] == self.info['target_entity'], "only support connections to main table")
            if edge['to_entity'] not in self.data_set:
                continue

            d1 = self.data_set[edge['from_entity']]
            d2 = self.data_set[edge['to_entity']]
            d1.keys += edge['from_entity_keys']
            d2.keys += edge['to_entity_keys']

        mem_info('start')
        self.load_main_table()
        mem_info('load_main_table')
        pico_tools.progress_report(1)
        self.join_base()
        mem_info('join_base')
        pico_tools.progress_report(1)
        self.train_base()
        mem_info('train_base')
        pico_tools.progress_report(1)
        self.select_op()
        mem_info('selected_op: finish multi_table.run')
        pico_tools.progress_report(2)
        return self
