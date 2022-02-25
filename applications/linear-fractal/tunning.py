#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
import json, copy
import pygdbt
from pygdbt.common import *
from pygdbt.app_common import *
from lfc_config import LFCConf

class LFCTunning:
    class Conf(LFCConf):
        def __init__(self):
            LFCConf.__init__(self)
            self.declare("param_search_space", "search space in json",
                False, DefaultChecker())
            self.declare("param_search_concurrency", "num of param search in same time",
                True, GreaterChecker(0), 10)
            self.declare("param_search_times", "num of param to search",
                True, GreaterChecker(0), 10)
            self.declare("param_search_random_seed", "random seed",
                True, GreaterEqualChecker(0), 0)
            self.remove(['model_output_prefix',
                        'sink_model_interval',
                        'validate_model_interval',
                        'init_model', 'early_stop',
                        'show_decay_rate',
                        'decayed_show_filter_threshold'])

        def load_config(self, args):
            if not LFCConf.load_config(self, args):
                return False
            if self.param_search_random_seed > 0:
                np.random.seed(self.param_search_random_seed)
            return True

    def __init__(self, conf):
        self.conf = LFCTunning.Conf()
        SCHECK(self.conf.load_config(conf), 'invalid configure')
        self.quantile = tables.QuantileTable(500)

        self.params = []
        search_space = json.loads(self.conf.param_search_space)["space"][0]
        generator = {
            'uniform'    : np.random.uniform,
            'loguniform' : lambda x, y: np.exp(np.random.uniform(np.log(x), np.log(y)))
        }
        for i in range(self.conf.param_search_times):
            param = {}
            for k, v in search_space.items():
                k = k.split('.')[-1]
                param[k] = generator[v["type"]](v["min"], v["max"])
            self.params.append({"params": param})

        printf(json.dumps(self.params, indent=4))

    def parser(self, data):
        label = layers.ParseLabel(data)
        sp1   = layers.ParseSparseInput(data)
        sp2   = layers.LinearFractal(sp1, self.quantile,
            self.conf.shrinkage,
            self.conf.binning_bucket_list,
            self.conf.diff_tolerance,
            self.conf.bin_value_type)
        sp3   = layers.ConcatBias(sp2, -1, 0)
        return label, sp3

    def statistic(self):
        raw_data = pygdbt.GCFormat(self.conf.input_path)
        def graph():
            block = raw_data.Read("async", self.conf.mini_batch_size)
            sparse = layers.ParseSparseInput(block, layers.ParseType.CONT)
            layers.QuantileStat(sparse, self.quantile)
            layers.Sink(block, self.train_data)
        pygdbt.execute(graph, True, self.conf.debug_level)

    def cache_data(self, data):
        cache = pygdbt.Cache(self.conf.cache_uri)
        def graph():
            label, sparse = self.parser(data.Read())
            block = layers.Merge([label, sparse], ['label', 'sparse'])
            layers.Sink(block, cache)
        pygdbt.execute(graph, False, self.conf.debug_level)
        return cache

    def train(self):
        block  = self.train_cache.Read(self.conf.training_mode, self.conf.mini_batch_size)
        label  = block.sub('label')
        sparse = block.sub('sparse')

        for i, table in enumerate(self.tables):
            wght = layers.SparseTablePull(sparse, table)
            wsum = layers.WeightReduce(sparse, wght)
            loss = layers.Loss(wsum, label, layers.LossType.SIGMOID_LOGLOSS)
            layers.Minimize(loss)

            pred = layers.Activation(wsum, layers.ActType.SIGMOID)
            layers.AucAccumulator(pred, label, "progressive_auc_%d" % i)
        layers.    AvgAccumulator(label, "training_positive_ratio")
        layers. InsNumAccumulator(pred,  "training_ins_num", 1024)

    def valid(self):
        block  = self.valid_cache.Read("async", self.conf.mini_batch_size)
        label  = block.sub('label')
        sparse = block.sub('sparse')

        for i, table in enumerate(self.tables):
            wght = layers.SparseTablePull(sparse, table)
            wsum = layers.WeightReduce(sparse, wght)
            loss = layers.Loss(wsum, label, layers.LossType.SIGMOID_LOGLOSS)
            layers.Minimize(loss)

            pred = layers.Activation(wsum, layers.ActType.SIGMOID)
            layers.AucAccumulator(pred, label, "validation_auc_%d" % i)
        layers.    AvgAccumulator(label, "validation_positive_ratio")
        layers. InsNumAccumulator(pred,  "validation_ins_num", 1024)

    def dump_model(self):
        if pygdbt.pico_tools.comm_rank() == 0:
            params = []
            for p in self.params:
                tmp = {}
                for k, v in p["params"].items():
                    tmp["app." + k] = v
                    if k == 'lambda_1':
                        tmp["app.lambda_c1"] = v
                    if k == 'lambda_2':
                        tmp["app.lambda_c2"] = v
                params.append({
                    "params" : tmp,
                    "score" : {"value" : p["score"], "type" : "validation_auc"}
                })
            path = os.path.join(self.conf.model_output_path.path, "report1")
            file = json.dumps({"values": {"param_detail": params}}, indent=2)
            pygdbt.pico_tools.save_file(path, file)

    def inner_main(self):
        conf = copy.copy(self.conf)

        self.train_data = pygdbt.Cache(conf.cache_uri)
        self.valid_data = pygdbt.GCFormat(conf.input_validation_path)
        self.statistic()

        self.train_cache = self.cache_data(self.train_data)
        self.valid_cache = self.cache_data(self.valid_data)

        idx = 0
        while idx < len(self.params):
            search_params = self.params[idx : idx + self.conf.param_search_concurrency]
            idx += self.conf.param_search_concurrency
            SLOG("searching params: %s" % json.dumps(search_params, indent=4))

            self.tables = []
            for param in search_params:
                for k, v in param["params"].items():
                    setattr(conf, k, v)
                table = tables.SparseTable(tables.FTRL(
                        alpha              = conf.alpha,
                        beta               = conf.beta,
                        lambda_1           = conf.lambda_1,
                        lambda_2           = conf.lambda_2,
                        enable_show_adjust = conf.enable_show_adjust,
                        enable_scale_free  = conf.enable_scale_free 
                    ))
                table.set_creation_ratio(1.0)
                self.tables.append(table)
                param['score'] = 0.0

            for self.pas in range(conf.training_pass_num):
                pygdbt.pico_tools.erase_accumulator([])
                pygdbt.execute(self.train, True,  conf.debug_level, "pool")
                pygdbt.execute(self.valid, False, conf.debug_level)

                acc = pygdbt.pico_tools.get_accumulator()
                RLOG(0, "Training finished, Pass #%d Accumulator[JSON]:\t%s" % (self.pas, acc))
                acc = json.loads(acc)["PicoAccumulator"]

                for i, param in enumerate(search_params):
                    score = float(acc["validation_auc_%d" % i])
                    if score > param['score']:
                        param["score"] = score
                        param["params"]["training_pass_num"] = self.pas + 1
        self.dump_model()
        return 0

if __name__ == '__main__':
    app_main(LFCTunning)
