#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pygdbt
from pygdbt.common import *
from pygdbt.app_common import *

from common import *
from configure import *
from learner import *
from loss import *
from estimator import *
import gbmtools

class GBM:
    Conf = GBMConf
    Estimator = GBMEstimator

    def __init__(self, conf):
        self.conf = GBM.Conf()
        if not self.conf.load_config(conf):
            raise ValueError('invalid configure.')

        self.loss = loss_map[self.conf.loss_type]()
        self.learner = {}
        for param in self.conf.param:
            self.learner[param.name] = learner_map[param.type](self, param)

    def cache_data(self, path):
        if len(path.data) == 0:
            return None
        cache = pygdbt.Cache(self.conf.cache_uri)
        rdata = pygdbt.GCFormat(path)
        def graph():
            block = rdata.Read()
            index = layers.AssignIndex()
            block = layers.Merge([index, block], ['index', 'data'])
            layers.Sink(block, cache)
        pygdbt.execute(graph, True, self.conf.debug_level)
        return cache

    def need_validate(self, pas):
        return  self.valid_data is not None and (
                (pas + 1) % self.conf.validate_model_interval == 0 or
                (pas + 1) == len(self.conf.boost_list))

    def need_dump(self, pas):
        return  (pas + 1) != len(self.conf.boost_list) and \
                self.conf.sink_model_interval > 0 and \
                (pas + 1) % self.conf.sink_model_interval == 0

    def validate(self, pas):
        def _train_():
            block = self.train_data.Read()
            index = block.sub("index")
            data  = block.sub("data")
            label = layers.ParseLabel(data)
            wsum  = layers.VectorGet(index, self.train_wsum)
            self.loss.train_metric(label, wsum)
        pygdbt.execute(_train_, False, gbmtools.debug_level)

        def _valid_():
            block = self.valid_data.Read()
            index = block.sub("index")
            data  = block.sub("data")
            label = layers.ParseLabel(data)
            wsum  = layers.VectorGet(index, self.valid_wsum)
            self.loss.valid_metric(label, wsum)
        pygdbt.execute(_valid_, False, gbmtools.debug_level)

    def dump_model(self, dir_name):
        pygdbt.pico_tools.save_metadata(self.conf.model_output_path + dir_name + "/metadata")
        for name, learner in self.learner.items():
            learner.dump(self.conf.model_output_path + dir_name)

    def init_weight(self, cache, vector):
        def _graph_():
            block = cache.Read()
            index = block.sub("index")
            data  = block.sub("data")
            wsum  = None
            for name, learner in self.learner.items():
                temp = learner.Predict(data)
                if wsum is None:
                    wsum = temp
                else:
                    wsum = layers.Add(wsum, temp)
            layers.VectorSet(index, wsum, vector)
        pygdbt.execute(_graph_, True, gbmtools.debug_level)

    def inner_main(self):
        pygdbt.pico_tools.progress_initialize(len(self.conf.boost_list))

        self.train_data = self.cache_data(self.conf.input_path)
        self.valid_data = self.cache_data(self.conf.input_validation_path)
        self.train_wsum = tables.VectorTable()
        self.valid_wsum = tables.VectorTable()

        for name, learner in self.learner.items():
            learner.init(self.train_data)

        if self.conf.init_model.valid():
            for name, learner in self.learner.items():
                learner.load(self.conf.init_model.path)
            self.init_weight(self.train_data, self.train_wsum)
            self.init_weight(self.valid_data, self.valid_wsum)

        train_pass = pygdbt.pico_tools.SumAccumulator("trained_learner_num", 1);
        for i, key in enumerate(self.conf.boost_list):
            learner = self.learner[key]
            learner.train()
            if pygdbt.pico_tools.comm_rank() == 0:
                train_pass.write(1)

            if self.need_validate(i):
                self.validate(i)
            acc = pygdbt.pico_tools.get_accumulator()
            RLOG(0, "Training finished, Pass #%d Accumulator[JSON]:\t%s" % (i, acc))
            pygdbt.pico_tools.progress_report(1)

            if self.need_dump(i):
                self.dump_model("/pass-%d" % i)
        self.dump_model("/pass-final")

if __name__ == '__main__':
    app_main(GBM)
