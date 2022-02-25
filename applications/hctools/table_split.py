#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pygdbt
from pygdbt.common import *
from pygdbt.app_common import *
import libhctools as core

class TableSplit:
    class Conf(Configure):
        def __init__(self):
            Configure.__init__(self)
            self.declare('input_path', 'the input data path',
                False, NotEmptyChecker(), list_config(TrainInputURI))
            self.declare('output_path', 'output data path',
                False, NotEmptyChecker(), list_config(TrainInputURI))
            self.declare('split_type', 'split type',
                False, EnumChecker(set(['random', 'temporal'])), '')
            self.declare('timestamp', 'timestamp column name',
                True, DefaultChecker(), '')
            self.declare('encoder', 'timestamp column encoder',
                True, EnumChecker(set(['Timestamp', 'Date'])), 'Timestamp')
            self.declare('split_ratio', 'train data ratio',
                False, GreaterChecker(0.0), 0.8)
            self.declare('quantile_accuracy', 'quantile statistic max_n',
                True,  GreaterChecker(100), 1000)
            self.declare('cache_uri', 'cache uri',
                True, NotEmptyChecker(), CacheURI("file://.?format=archive&compress=lz4"))
            self.declare('debug_level', 'debug level (FLAGS_v)',
                True, GreaterEqualChecker(0), 0)

    class Output:
        def __init__(self, path):
            self.path = path
            self.data = pygdbt.Parquet(path)

    def __init__(self, conf):
        self.conf = TableSplit.Conf()
        SCHECK(self.conf.load_config(conf), 'invalid configure')
        for path in self.conf.input_path:
            path.conf['format'] = 'parquet'

    def _gen_output_(self):
        uri = self.conf.output_path.data
        if len(self.conf.output_path.data) == 1:
            conf  = uri[0].conf
            conf['format'] = 'parquet'
            conf  = '&'.join(map(lambda x: "%s=%s" % (x[0], x[1]), conf.items()))
            self.train = TableSplit.Output(uri[0] + "/train?" + conf)
            self.test  = TableSplit.Output(uri[0] + "/test?"  + conf)
        else:
            uri[0].conf['format'] = 'parquet'
            uri[1].conf['format'] = 'parquet'
            self.train = TableSplit.Output(uri[0].uri())
            self.test  = TableSplit.Output(uri[1].uri())

    def split_by_time(self):
        timeline = tables.QuantileIntTable(self.conf.quantile_accuracy, False)
        data = pygdbt.Parquet(self.conf.input_path)
        cache = pygdbt.Cache(self.conf.cache_uri)

        def read():
            block = data.Read()
            timec = layers.SubDataFrame(block, [self.conf.timestamp])
            timec = layers.DataFrame2InstanceBlock(timec, self.conf.encoder)
            timec = layers.ParseSparseInput(timec)
            layers.QuantileIntStat(timec, timeline)
            layers.Sink(block, cache)
        pygdbt.execute(read, True, self.conf.debug_level)

        cand = timeline.pull(0)
        idx  = int(len(cand) * self.conf.split_ratio)
        cond = cand[idx]
        cnt  = [0, 0]
        for i, v in enumerate(cand):
            if v == cond and i < idx:
                cnt[0] += 1
            elif v == cond and i >= idx:
                cnt[1] += 1
        ratio = cnt[0] / float(cnt[0] + cnt[1])

        self._gen_output_()
        def sink():
            df = cache.Read()
            tm = layers.SubDataFrame(df, [self.conf.timestamp])
            gc = layers.DataFrame2InstanceBlock(tm, self.conf.encoder)
            out = core.DFFilter(df, gc, cond, ratio)
            layers.SinkFile(out[0], self.train.data, "part")
            layers.SinkFile(out[1], self.test .data, "part")
        pygdbt.execute(sink, True, self.conf.debug_level)

    def split_by_random(self):
        data  = pygdbt.Parquet(self.conf.input_path)
        self._gen_output_()

        def sink():
            df  = data.Read()
            out = core.DFSplit(df, self.conf.split_ratio)
            layers.SinkFile(out[0], self.train.data, "part")
            layers.SinkFile(out[1], self.test .data, "part")
        pygdbt.execute(sink, True, self.conf.debug_level)

    def inner_main(self):
        if self.conf.split_type == 'temporal':
            self.split_by_time()
        else:
            self.split_by_random()

        print('split train:', self.train.data.global_size(), self.train.path)
        print('split test :', self.test .data.global_size(), self.test .path)

if __name__ == '__main__':
    app_main(TableSplit)
