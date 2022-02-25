#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import math
import pygdbt
from functools import partial
from pygdbt.common import *
from pygdbt.app_common import *
from configure import *
from multi_table import *
from feature_generator import *
from estimator import FZEstimator
import trainer, trainer_reg
import fztools

class FeatureZero:
    Conf = FZConf
    Estimator = FZEstimator
    def __init__(self, conf):
        self.conf = FeatureZero.Conf()
        if not self.conf.load_config(conf):
            raise ValueError('invalid configure.')

        c = self.conf.lr.to_dict()
        if self.conf.task_type == 'binary':
            self.lr = trainer.LogisticRegression(self.conf)
            self.sw = trainer.SlotwiseLR(self.conf)
        elif self.conf.task_type == 'regression' or self.conf.task_type == 'regression_mse':
            self.lr = trainer_reg.LinearRegression(self.conf)
            self.sw = trainer_reg.SlotwiseMSE(self.conf)
        elif self.conf.task_type == 'regression_mae':
            pass
        elif self.conf.task_type == 'regression_real_mse':
            pass
        elif self.conf.task_type == 'regression_real_mae':
            pass
        else:
            raise Exception('unknown: ' + self.conf.task_type)

        self.explore_limit = self.conf.auto.combine_explore_limits
        self.temporal_cache = []
        self.combined_cache = []
        self.selected_ops = []

    def prepare(self):
        printf("main table", fztools.gc_memory_size(self.main.data),
            [self.main.data.size(), len(self.main.info)],
            self.main.data.global_size(), self.main.columns())

        self.fea_ctx = FeaCtx(self)
        def graph():
            block = self.main.data.Read()
            spars = layers.ParseSparseInput(block, layers.ParseType.DISC)
            layers.QuantileIntStat(spars, self.fea_ctx.distribution)
            fztools.SignLengthStat(spars, self.fea_ctx.sign_len)
            fztools.SignCountStat (spars, self.fea_ctx.sign_cnt)
        pygdbt.execute(graph)

        self.lr.index_dist = self.fea_ctx.distribution
        self.sw.index_dist = self.fea_ctx.distribution

        train, valid = fztools.split_data(self.main.data, self.time_line, self.timestamp, self.conf.test_ratio)
        slots = [-2] + self.main.trainable_slots()
        self.sw.base_wsum.clear()
        self.sw.fit(train, slots, qdata=[train, valid])
        self.fea_ctx.set_score(self.sw.score(valid), map3(lambda x: x.slot, self.main.info))
        self.sw.quantile.reset()
        self.fea_ctx.prepare()

        self.lr.slots = slots
        self.lr.init_table([train, valid])
        cache = self.lr.datasets(train)
        for pas in range(self.conf.min_iter - 1):
            pygdbt.execute(partial(self.lr.Train, cache), True, self.conf.debug_level)
        self.base_line = self.lr.score(valid)
        trained_pass = self.conf.min_iter - 1
        for pas in range(self.conf.min_iter - 1, self.conf.max_iter):
            pygdbt.execute(partial(self.lr.Train, cache), True, self.conf.debug_level)
            score = self.lr.score(valid)
            printf("finding lr iter num", pas + 1, self.base_line, score)
            trained_pass += 1
            if score > self.base_line:
                self.base_line = score
            else:
                self.base_line = score
                break

        printf("base_line:", self.base_line)
        self.sw.conf.lr.max_iter = trained_pass
        erase_binary_accumulator()
        cache.reset()
        train.reset()
        valid.reset()

    def _groupby_fea_(self, op, part, win, f):
        time = self.main.info[self.timestamp].feql
        part = ':'.join(map(lambda x: x.feql, part))
        win  = win_(win)
        return "%s(%s,%s,%s,%s)" % (op, time, part, win, f.feql)

    def cache_temporal_candidate(self, conf, df):
        tmp = []
        for c, d in self.temporal_cache:
            tmp += c
        for i, c in enumerate(conf):
            tmp.append((c[0], i + 1, c[1]))
        tmp.sort(reverse=True)
        vis = set(map3(lambda x: x[2].name, tmp[:self.conf.auto.rebase_limits]))

        tmp = []
        for c, d in self.temporal_cache:
            fea = list(filter(lambda x: x[2].name in vis, c))
            if len(fea) == 0:
                continue
            if len(fea) == len(c):
                tmp.append((c, d))
                continue

            slots = map3(lambda x: x[1], fea)
            cache = fztools.filter(d, slots)
            tmp.append((fea, cache))
            d.reset()

        fea = []
        for i, c in enumerate(conf):
            if c[1].name in vis:
                fea.append((c[0], i + 1, c[1]))
        if len(fea) == len(conf):
            tmp.append((fea, df.data))
        elif len(fea) > 0:
            slots = map3(lambda x: x[1], fea)
            cache = fztools.filter(df.data, slots)
            tmp.append((fea, cache))
            df.data.reset()

        self.temporal_cache = tmp

    def explore_temporal_features(self):
        self.time_line.set(0, self.time_line.pull(self.timestamp))
        ret = []
        groupby_windows = self.conf.auto_windows(self.fea_ctx.sign_cnt, self.timestamp, self.conf.auto.groupby_windows)
        raw_feas = sorted(self.fea_ctx.raw_feas, key=lambda f: self.fea_ctx.score[f.slot], reverse=True)

        while len(ret) < self.conf.auto.groupby_feature_limits:
            part_by = self.fea_ctx.pop_partition_target()
            if part_by is None:
                break

            remain = self.conf.auto.groupby_feature_limits - len(ret)
            part_n = len(self.fea_ctx.cur_partition_keys) + 1
            max_op_num = int((float(remain) / part_n + part_n / 2.0) / len(groupby_windows))
            printf("cur max_op_num", max_op_num, remain)

            part_by_slots = map3(lambda x: x.slot, part_by)
            cache = pygdbt.Cache(self.conf.cache_uri)
            data = fztools.partition_by(self.main.data, self.fea_ctx.distribution, part_by_slots[0], timec=self.timestamp)
            fztools.transfer_data(cache, data, self.main.num_cols())
            self.sw.set_base(self.lr, cache, fztools.index_col)

            data = fztools.partition_by(cache, self.fea_ctx.distribution, part_by_slots[0], timec=self.timestamp)
            cache.reset()
            for w in groupby_windows:
                conf = []
                for f in raw_feas:
                    if f.slot in part_by_slots or self.fea_ctx.is_useless(f):
                        continue
                    if isinstance(f.encoder, ENC.Num):
                        if 'window_sum' not in self.disabled:
                            name = self._groupby_fea_('window_sum', part_by, w, f)
                            conf.append(fztools.AggConf(name, 0, f.slot, ENC.Num()))
                        if 'window_avg' not in self.disabled:
                            name = self._groupby_fea_('window_avg', part_by, w, f)
                            conf.append(fztools.AggConf(name, 1, f.slot, ENC.Num()))
                        if 'window_max' not in self.disabled:
                            name = self._groupby_fea_('window_max', part_by, w, f)
                            conf.append(fztools.AggConf(name, 2, f.slot, ENC.Num()))
                        if 'window_min' not in self.disabled:
                            name = self._groupby_fea_('window_min', part_by, w, f)
                            conf.append(fztools.AggConf(name, 3, f.slot, ENC.Num()))
                    if ENC.is_single_category(f.encoder):
                        if 'window_top1_ratio' not in self.disabled:
                            name =self._groupby_fea_('window_top1_ratio', part_by, w, f)
                            conf.append(fztools.AggConf(name, 4, f.slot, ENC.Num()))
                        if 'window_count' not in self.disabled:
                            name =self._groupby_fea_('window_count', part_by, w, f)
                            conf.append(fztools.AggConf(name, 5, f.slot, ENC.Num()))
                        if 'window_unique_count' not in self.disabled:
                            name =self._groupby_fea_('window_unique_count', part_by, w, f)
                            conf.append(fztools.AggConf(name, 6, f.slot, ENC.Num()))
                    if len(conf) > max_op_num:
                        break

                conf.sort(key = lambda x: (x.slot, x.type))
                printf("ops:", len(conf), map3(lambda x: x.name, conf))
                tmp = fztools.apply_ops(data, part_by_slots, self.timestamp, w, conf)
                train, valid = fztools.split_data(tmp.data, self.time_line, 0, self.conf.test_ratio)

                slots = list(range(1, len(conf) + 1))
                self.sw.fit(train, slots, qdata=[train, valid])
                score = self.sw.score(valid)
                train.reset()
                valid.reset()

                fea = []
                for i, f in enumerate(conf):
                    print("%s:%f" % (f.name, score[i+1]), end=', ')
                    fea.append((score[i + 1], f))
                print()
                
                ret += fea
                self.cache_temporal_candidate(fea, tmp)
                mem_info()
                self.explore_limit = max(self.explore_limit, len(slots))

                if len(ret) >= self.conf.auto.groupby_feature_limits:
                    break

            data.reset()
        return ret

    def _explore_(self, conf):
        printf("ops:", len(conf), map3(lambda x: x.name, conf))

        slots = list(range(len(conf)))
        train = fztools.combine_ops(self.train_data, conf)
        valid = fztools.combine_ops(self.valid_data, conf)
        self.sw.fit(train.data, slots, qdata=[train.data, valid.data])
        score = self.sw.score(valid.data)
        train.data.reset()
        valid.data.reset()

        ret = []
        for i, f in enumerate(conf):
            print("%s:%f" % (f.name, score[i]), end=', ')
            if not math.isnan(score[i]):
                ret.append((score[i], f))
        print()
        mem_info()
        return ret

    def _explore_conf_(self, conf, ret):
        if len(conf) == 0:
            return
        def cmp(x):
            sc = sum(map(lambda y: self.fea_ctx.score[y], x.slot))
            return (sc, x.name)
        conf.sort(key = cmp, reverse = True)

        ret += self._explore_(conf[:self.explore_limit])
        conf = conf[self.explore_limit:]
        while len(conf) > 0 and len(ret) < self.conf.auto.combine_feature_limits:
            ret += self._explore_(conf[:self.explore_limit])
            conf = conf[self.explore_limit:]

    def explore_combine1_features(self, ret):
        conf = []
        for f in self.fea_ctx.all_feas:
            if isinstance(f.encoder, ENC.Num) and not self.fea_ctx.is_useless(f):
                name = "log(%s)" % f.feql
                conf.append(fztools.CombConf(name, 0, [f.slot], ENC.Num()))
            if ENC.is_time(f.encoder):
                if "isweekday" not in self.disabled:
                    name = "isweekday(%s)" % f.feql
                    conf.append(fztools.CombConf(name, 6, [f.slot], ENC.DiscNum()))
                if "dayofweek" not in self.disabled:
                    name = "dayofweek(%s)" % f.feql
                    conf.append(fztools.CombConf(name, 7, [f.slot], ENC.DiscNum()))
                if "hourofday" not in self.disabled:
                    name = "hourofday(%s)" % f.feql
                    conf.append(fztools.CombConf(name, 8, [f.slot], ENC.DiscNum()))
        self._explore_conf_(conf, ret)

    def explore_combine2_features(self, ret):
        conf = []
        for i, f1 in enumerate(self.fea_ctx.all_feas):
            if self.fea_ctx.is_useless(f1):
                continue
            for f2 in self.fea_ctx.all_feas[i+1:]:
                if isinstance(f1.encoder, ENC.Num) and isinstance(f2.encoder, ENC.Num):
                    if "add" not in self.disabled:
                        name = "add(%s,%s)" % (f1.feql, f2.feql)
                        conf.append(fztools.CombConf(name, 1, [f1.slot, f2.slot], ENC.Num()))
                    if "subtract" not in self.disabled:
                        name = "subtract(%s,%s)" % (f1.feql, f2.feql)
                        conf.append(fztools.CombConf(name, 2, [f1.slot, f2.slot], ENC.Num()))
                        name = "subtract(%s,%s)" % (f2.feql, f1.feql)
                        conf.append(fztools.CombConf(name, 2, [f2.slot, f1.slot], ENC.Num()))
                    if "multiply" not in self.disabled:
                        name = "multiply(%s,%s)" % (f1.feql, f2.feql)
                        conf.append(fztools.CombConf(name, 3, [f1.slot, f2.slot], ENC.Num()))
                    if "divide" not in self.disabled:
                        name = "divide(%s,%s)" % (f1.feql, f2.feql)
                        conf.append(fztools.CombConf(name, 4, [f1.slot, f2.slot], ENC.Num()))
                        name = "divide(%s,%s)" % (f2.feql, f1.feql)
                        conf.append(fztools.CombConf(name, 4, [f2.slot, f1.slot], ENC.Num()))
                if self.fea_ctx.is_useless(f2):
                    continue
                if self.fea_ctx.combinable(f1, f2):
                    if "combine" not in self.disabled:
                        name = "combine(%s,%s)" % (f1.feql, f2.feql)
                        conf.append(fztools.CombConf(name, 5, [f1.slot, f2.slot], ENC.SepString()))
                if ENC.is_single_category(f1.encoder) and ENC.is_multi_category(f2.encoder):
                    if "isin" not in self.disabled:
                        name = "isin(%s,%s)" % (f1.feql, f2.feql)
                        conf.append(fztools.CombConf(name, 9, [f1.slot, f2.slot], ENC.DiscNum()))
                if ENC.is_single_category(f2.encoder) and ENC.is_multi_category(f1.encoder):
                    if "isin" not in self.disabled:
                        name = "isin(%s,%s)" % (f2.feql, f1.feql)
                        conf.append(fztools.CombConf(name, 9, [f2.slot, f1.slot], ENC.DiscNum()))
        print("candidate dim 2:", len(conf))
        self._explore_conf_(conf, ret)

    def explore_combine3_features(self, ret):
        conf = []
        cont = list(filter(lambda f: isinstance(f.encoder, ENC.Num), self.fea_ctx.all_feas))
        cont.sort(key = lambda f: self.fea_ctx.score[f.slot], reverse = True)

        for i, f1 in enumerate(cont[:30]):
            for j, f2 in enumerate(cont[:30]):
                for k, f3 in enumerate(cont):
                    if i >= j or j >= k:
                        continue
                    if "add" not in self.disabled:
                        name = "add(%s,%s,%s)" % (f1.feql, f2.feql, f3.feql)
                        conf.append(fztools.CombConf(name, 1, [f1.slot, f2.slot, f3.slot], ENC.Num()))
                    if "multiply" not in self.disabled:
                        name = "multiply(%s,%s,%s)" % (f1.feql, f2.feql, f3.feql)
                        conf.append(fztools.CombConf(name, 3, [f1.slot, f2.slot, f3.slot], ENC.Num()))

        disc = list(filter(lambda f: ENC.is_single_category(f.encoder), self.fea_ctx.all_feas))
        disc.sort(key = lambda f: self.fea_ctx.score[f.slot], reverse = True)
        for i, f1 in enumerate(disc[:30]):
            for j, f2 in enumerate(disc[:30]):
                for k, f3 in enumerate(disc):
                    if i >= j or j >= k:
                        continue
                    if "combine" not in self.disabled:
                        name = "combine(%s,%s,%s)" % (f1.feql, f2.feql, f3.feql)
                        conf.append(fztools.CombConf(name, 5, [f1.slot, f2.slot, f3.slot], ENC.SepString()))
        print("candidate dim 3:", len(conf))
        self._explore_conf_(conf, ret)

    def explore_combined_features(self):
        self.train_data, self.valid_data = fztools.split_data(
            self.main.data, self.time_line, self.timestamp, self.conf.test_ratio)
        self.sw.set_base(self.lr, self.main.data, fztools.index_col)
        # self.main.data.reset()

        ret = []
        self.explore_combine1_features(ret)
        self.explore_combine2_features(ret)
        self.explore_combine3_features(ret)

        self.combined_cache = sorted(ret, key=lambda x: (x[0], x[1].name), reverse=True)[:self.conf.auto.rebase_limits]
        self.combined_cache = map3(lambda x: (x[1][0], x[0], x[1][1]), enumerate(self.combined_cache))
        return ret

    def explore_rebased_features(self):
        if len(self.temporal_cache) + len(self.combined_cache) == 0:
            return self.selected_ops

        base_select = set(map(lambda x: x[1].name, self.selected_ops))
        printf("base_select:", base_select)

        main_part = fztools.partition_by(self.main.data, self.fea_ctx.distribution, fztools.index_col)
        base_comb_f = {1:[], 2:[], 3:[]}
        test_comb_f = {1:[], 2:[], 3:[]}
        for s, i, f in self.combined_cache:
            if f.name in base_select:
                base_comb_f[len(f.slot)].append(f)
            else:
                test_comb_f[len(f.slot)].append(f)

        base_feas = map3(lambda f: (0.0, f.slot, f), self.main.info)
        for dim in [1, 2, 3]:
            feas = base_comb_f[dim]
            if len(feas) == 0:
                continue
            comb = fztools.combine_ops(self.main.data, feas)
            part = fztools.partition_by(comb.data, self.fea_ctx.distribution, fztools.index_col)
            comb.data.reset()
            fztools.core.left_join(main_part, part, [-1], [-1], self.main.offset, True)
            part.reset()
            base_feas += map3(lambda x: (0.0, x[0] + self.main.offset, x[1]), enumerate(feas))
            self.main.offset += len(feas)

        for c, d in self.temporal_cache:
            base_temp_f = []
            for f in c:
                if f[2].name in base_select:
                    base_temp_f.append(f[1])
                    base_feas.append((0.0, f[1] + self.main.offset, f[2]))

            if len(base_temp_f) > 0:
                if len(base_temp_f) < len(c):
                    data = fztools.filter(d, base_temp_f)
                else:
                    data = d
                part = fztools.partition_by(data, self.fea_ctx.distribution, -1)
                fztools.core.left_join(main_part, part, [-1], [-1], self.main.offset, True)
                self.main.offset += base_temp_f[-1] + 1
                part.reset()
                if len(base_temp_f) < len(c):
                    data.reset()

        base = pygdbt.Cache(self.conf.cache_uri)
        fztools.transfer_data(base, main_part, self.main.offset)
        main_part.reset()
        base_train, base_valid = fztools.split_data(base, self.time_line, self.timestamp, self.conf.test_ratio)

        base_slots = map3(lambda x: x[1], base_feas)
        self.lr.slots = base_slots
        self.lr.init_table([base_train, base_valid])
        cache = self.lr.datasets(base_train)
        base_train.reset()
        for pas in range(self.conf.min_iter - 1):
            pygdbt.execute(partial(self.lr.Train, cache), True, self.conf.debug_level)
        base_line = self.lr.score(base_valid)
        trained_pass = self.conf.min_iter - 1
        for pas in range(self.conf.min_iter - 1, self.conf.max_iter):
            pygdbt.execute(partial(self.lr.Train, cache), True, self.conf.debug_level)
            score = self.lr.score(base_valid)
            printf("finding lr iter num", pas + 1, base_line, score)
            trained_pass += 1
            if score > base_line:
                base_line = score
            else:
                base_line = score
                break
        printf("base_line:", base_line)
        self.sw.conf.lr.max_iter = trained_pass
        erase_binary_accumulator()
        cache.reset()
        base_valid.reset()

        self.sw.set_base(self.lr, base, fztools.index_col)
        base.reset()

        test_data = fztools.filter(self.main.data, [self.timestamp])
        test_part = fztools.partition_by(test_data, self.fea_ctx.distribution, fztools.index_col)
        test_data.reset()
        test_feas = []
        test_offset = self.main.offset
        for dim in [1, 2, 3]:
            feas = test_comb_f[dim]
            if len(feas) == 0:
                continue
            comb = fztools.combine_ops(self.main.data, feas)
            part = fztools.partition_by(comb.data, self.fea_ctx.distribution, fztools.index_col)
            comb.data.reset()
            fztools.core.left_join(test_part, part, [-1], [-1], test_offset, True)
            part.reset()
            test_feas += map3(lambda x: (0.0, x[0] + test_offset, x[1]), enumerate(feas))
            test_offset += len(feas)

        for c, d in self.temporal_cache:
            test_temp_f = []
            for f in c:
                if f[2].name not in base_select:
                    test_temp_f.append(f[1])
                    test_feas.append((0.0, f[1] + test_offset, f[2]))

            if len(test_temp_f) > 0:
                if len(test_temp_f) < len(c):
                    data = fztools.filter(d, test_temp_f)
                else:
                    data = d
                part = fztools.partition_by(data, self.fea_ctx.distribution, -1)
                fztools.core.left_join(test_part, part, [-1], [-1], test_offset, True)
                test_offset += test_temp_f[-1] + 1
                part.reset()
                if len(test_temp_f) < len(c):
                    data.reset()
            # d.reset()

        test = pygdbt.Cache(self.conf.cache_uri)
        fztools.transfer_data(test, test_part, self.main.offset)
        test_train, test_valid = fztools.split_data(test, self.time_line, self.timestamp, self.conf.test_ratio)
        test.reset()

        test_slots = map3(lambda x: x[1], test_feas)
        self.sw.fit(test_train, test_slots, qdata=[test_train, test_valid])
        test_train.reset()
        score = self.sw.score(test_valid)
        test_valid.reset()
        mem_info()
        printf("test_feas:", map3(lambda x: "%s:%f" % (x[2].name, score[x[1]]), test_feas))

        return map3(lambda x: (score[x[1]], x[2]), test_feas)

    def inner_main(self):
        pico_tools.progress_initialize(10)

        self.multi_ctx = MultiTable(self).run()
        self.main      = self.multi_ctx.root_data()
        self.time_line = self.multi_ctx.time_line
        self.timestamp = self.main.get_slot(self.multi_ctx.info['target_pivot_timestamp'])
        self.conf.auto2(self.main)
        self.prepare()
        self.main.dump_features(0, self, [])
        pico_tools.progress_report(1)

        self.sw.need_bias = True
        self.disabled = set(self.conf.disable_ops)
        feas = []
        feas += self.explore_temporal_features()
        self.main.dump_features(1, self, feas)
        pico_tools.progress_report(2)
        self.selected_ops = []
        feas += self.explore_combined_features()
        self.main.dump_features(2, self, feas)
        pico_tools.progress_report(1)
        feas  = self.explore_rebased_features()
        self.main.dump_features(3, self, feas, self.selected_ops)
        pico_tools.progress_report(1)
        feas  = self.explore_rebased_features()
        self.main.dump_features(4, self, feas, self.selected_ops)
        mem_info('learner end')

if __name__ == '__main__':
    app_main(FeatureZero)
