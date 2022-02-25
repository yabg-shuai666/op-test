#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pygdbt
from pygdbt.common import *
from pygdbt.app_common import *
import libhctools as core

class TableFilter:
    class DataConf(Configure):
        def __init__(self):
            Configure.__init__(self)
            self.declare('input_path', 'the input data path',
                False, NotEmptyChecker(), list_config(TrainInputURI))
            self.declare('output_path', 'filter data output path',
                False, NotEmptyChecker(), TrainInputURI())
            self.declare('from_entity_keys', 'left table keys', False, NotEmptyChecker())
            self.declare('to_entity_keys', 'right table keys', False, NotEmptyChecker())

    class Conf(Configure):
        def __init__(self):
            Configure.__init__(self)
            self.declare('debug_level', 'debug level (FLAGS_v)',
                True, GreaterEqualChecker(0), 0)
            self.declare('input_path', 'the input data path',
                False, NotEmptyChecker(), list_config(TrainInputURI))
            self.declare('target', 'all data need to filter',
                False, NotEmptyChecker(), list_config(TableFilter.DataConf))

    def __init__(self, conf):
        self.conf = TableFilter.Conf()
        SCHECK(self.conf.load_config(conf), 'invalid configure')

    def inner_main(self):
        self.exists = []
        for target in self.conf.target.data:
            self.exists.append(core.PSSet())

        main = pygdbt.Parquet(self.conf.input_path)
        def push():
            block = main.Read()
            for target, table in zip(self.conf.target.data, self.exists):
                core.PSSetPush(block, target.from_entity_keys, table)
        pygdbt.execute(push, True, self.conf.debug_level)

        self.filter_num = []
        for target, table in zip(self.conf.target.data, self.exists):
            side = pygdbt.Parquet(target.input_path)
            sink = pygdbt.Parquet(target.output_path)
            def pull():
                block = side.Read()
                block = core.PSSetFilter(block, target.to_entity_keys, table)
                layers.SinkFile(block, sink, "part")
            pygdbt.execute(pull, True, self.conf.debug_level)
            table.reset()
            self.filter_num.append(sink.global_size())
        print(self.filter_num)

if __name__ == '__main__':
    app_main(TableFilter)
