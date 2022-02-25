# -*- coding: utf-8 -*-

import json
from functools import partial

from pygdbt.common import *
from pygdbt.common.checker import *
from pygdbt.app_common import *
import fztools

class LogisticRegression:
    def __init__(self, conf):
        self.conf = conf
        self.table = None
        self.quantile = None
        self.slots = None
        self.pas = None
        self.index_dist = None
        self.debug = False

    def Parser(self, block, bias = True):
        label = layers.ParseLabel(block)
        if self.slots is None:
            sparse = layers.ParseSparseInput(block)
        else:
            sparse = layers.ParseSparseInput(block, layers.ParseType.SLOTS, self.slots)
        sparse = layers.LinearFractal(sparse, self.quantile,
            self.conf.lfc.binning_shrinkage,
            self.conf.lfc.bucket_list,
            self.conf.lfc.binning_diff_tolerance,
            self.conf.lfc.bin_value_type)
        if bias:
            sparse = layers.ConcatBias(sparse, fztools.bias_slot, 0)
        return layers.Merge([label, sparse], ['label', 'sparse'])

    def Train(self, data):
        block  = data.Read(self.conf.lr.training_mode, self.conf.lr.mini_batch_size)
        label  = block.sub("label")
        sparse = block.sub("sparse")
        wght   = layers.SparseTablePull(sparse, self.table)
        wsum   = layers.WeightReduce(sparse, wght)
        loss   = layers.Loss(wsum, label, layers.LossType.SIGMOID_LOGLOSS)
        layers.Minimize(loss)
        if self.pas == self.conf.lr.max_iter - 1:
            pred = layers.Activation(wsum, layers.ActType.SIGMOID)
            ProgressiveBinaryMetric(pred, label, loss, [], 100000000)

    def Valid(self, data):
        block  = self.Parser(data.Read())
        label  = block.sub("label")
        sparse = block.sub("sparse")
        wght   = layers.SparseTablePull(sparse, self.table)
        wsum   = layers.WeightReduce(sparse, wght)
        loss   = layers.Loss(wsum, label, layers.LossType.SIGMOID_LOGLOSS)
        pred   = layers.Activation(wsum, layers.ActType.SIGMOID)
        ValidationBinaryMetric(pred, label, loss, [], 100000000)

    def datasets(self, data):
        if self.index_dist is not None:
            fztools._shuffle_(data, self.index_dist)
        cache = pygdbt.Cache(self.conf.cache_uri)
        graph = lambda: layers.Sink(self.Parser(data.Read()), cache)
        pygdbt.execute(graph, False, self.conf.debug_level)
        return cache

    def init_table(self, qdata):
        conf = self.conf.lr
        if self.table is None:
            self.table = tables.SparseTable(tables.FTRL(
                    alpha              = conf.alpha,
                    beta               = conf.beta,
                    lambda_1           = conf.C if conf.penalty == 'l1' else 0,
                    lambda_2           = conf.C if conf.penalty == 'l2' else 0,
                    enable_show_adjust = conf.enable_show_adjust,
                    enable_scale_free  = conf.enable_scale_free 
                ))
            self.table.set_creation_ratio(conf.feature_creation_ratio)
        else:
            self.table.clear()

        if self.quantile is not None:
            self.quantile.reset()
        if self.conf.lfc.binning_shrinkage == 0:
            self.quantile = tables.QuantileTable(500)
        else:
            self.quantile = tables.QuantileTable(500 + 1.0 / self.conf.lfc.binning_shrinkage)
        def stat(data):
            block = data.Read()
            cont  = layers.ParseSparseInput(block, layers.ParseType.CONT)
            layers.QuantileStat(cont, self.quantile)
        nq = len(qdata)
        for i in range(nq):
            pygdbt.execute(partial(stat, qdata[i]), i == nq - 1, self.conf.debug_level)

    def fit(self, data, slots = None, qdata = None):
        if slots is not None:
            self.slots = slots
        if qdata is None:
            qdata = [data]
        self.init_table(qdata)

        cache = self.datasets(data)
        for self.pas in range(self.conf.lr.max_iter):
            pygdbt.execute(partial(self.Train, cache), True, self.conf.debug_level)
        cache.reset()
        return self

    def validate(self, data):
        pygdbt.execute(partial(self.Valid, data), False, self.conf.debug_level)
        acc = pico_tools.get_accumulator()
        print(acc)
        acc = json.loads(acc)
        return acc["PicoAccumulator"]

    def score(self, data):
        erase_binary_accumulator()
        return float(self.validate(data)['validation_auc'])

class SlotwiseLR(LogisticRegression):
    def __init__(self, conf):
        LogisticRegression.__init__(self, conf)
        self.base_wsum = tables.UnorderedMapTable()
        self.need_bias = False

    def set_base(self, lr, data, slot):
        self.base_wsum.clear()
        self.index_col = slot
        def graph():
            block = data.Read()
            spars = lr.Parser(block, self.need_bias).sub("sparse")
            index = layers.ParseSparseInput(block, layers.ParseType.SLOTS, [self.index_col])
            wght  = layers.SparseTablePull(spars, lr.table)
            wsum  = layers.WeightReduce(spars, wght)
            layers.MapInsert(index, wsum, self.index_col, self.base_wsum)
        pygdbt.execute(graph)

    def datasets(self, data):
        cache = pygdbt.Cache(self.conf.cache_uri)
        graph = lambda: layers.Sink(self.Parser(data.Read()), cache)
        pygdbt.execute(graph, False, self.conf.debug_level)
        return cache

    def Parser(self, block):
        label  = layers.ParseLabel(block)
        label  = layers.Broadcast(label, len(self.slots))
        index  = layers.ParseSparseInput(block, layers.ParseType.SLOTS, [self.index_col])
        sparse = layers.ParseSparseInput(block, layers.ParseType.SLOTS, self.slots)
        sparse = layers.LinearFractal(sparse, self.quantile,
            self.conf.lfc.binning_shrinkage,
            self.conf.lfc.bucket_list,
            self.conf.lfc.binning_diff_tolerance,
            self.conf.lfc.bin_value_type)
        return layers.Merge([label, sparse, index], ['label', 'sparse', 'index'])

    def Train(self, data):
        block  = data.Read(self.conf.lr.training_mode, self.conf.lr.mini_batch_size)
        label  = block.sub("label")
        sparse = block.sub("sparse")
        index  = block.sub("index")
        wght   = layers.SparseTablePull(sparse, self.table)
        tsum   = layers.SparseCombine(sparse, wght, self.slots)
        bsum   = layers.MapAt(index, self.index_col, self.base_wsum)
        wsum   = layers.BroadcastAdd(bsum, tsum)
        loss   = layers.Loss(wsum, label, layers.LossType.SIGMOID_LOGLOSS)
        layers.Minimize(loss)

    def Valid(self, data):
        block  = self.Parser(data.Read())
        label  = block.sub("label")
        sparse = block.sub("sparse")
        index  = block.sub("index")
        wght   = layers.SparseTablePull(sparse, self.table)
        tsum   = layers.SparseCombine(sparse, wght, self.slots)
        bsum   = layers.MapAt(index, self.index_col, self.base_wsum)
        wsum   = layers.BroadcastAdd(bsum, tsum)
        loss   = layers.Loss(wsum, label, layers.LossType.SIGMOID_LOGLOSS)
        pred   = layers.Activation(wsum, layers.ActType.SIGMOID)
        EvaluateBinaryMetric(pred, label, loss, self.slots, 100000000)

    def score(self, data):
        if data.global_size() == 0:
            return dict(map(lambda x: (x, 0), self.slots))
        acc = json.loads(self.validate(data)['evaluate_auc'])
        ret = {}
        for k in self.slots:
            try:
                ret[k] = float(acc[str(k)])
            except:
                ret[k] = 0
        erase_binary_evaluate()
        return ret
