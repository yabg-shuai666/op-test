# -*- coding: utf-8 -*-

import json, math
from functools import partial

from pygdbt.common import *
from pygdbt.common.checker import *
from pygdbt.app_common import *
from trainer import *

class LinearRegression(LogisticRegression):
    def TransLabel(self, label):
        if self.conf.lr.link_function_for_label == 'log1p':
            return layers.ScaleLog(label)
        else:
            return label

    def RevertPred(self, pred):
        if self.conf.lr.link_function_for_label == 'log1p':
            return layers.ScaleExp(pred)
        else:
            return pred

    def Train(self, data):
        block  = data.Read(self.conf.lr.training_mode, self.conf.lr.mini_batch_size)
        rlabel = block.sub("label")
        label  = self.TransLabel(rlabel)
        sparse = block.sub("sparse")
        wght   = layers.SparseTablePull(sparse, self.table)
        pred   = layers.WeightReduce(sparse, wght)
        mse    = layers.Loss(pred, label, layers.LossType.MSE)
        layers.Minimize(mse)
        if self.pas == self.conf.max_iter - 1:
            rpred = self.RevertPred(pred)
            ProgressiveRegressionMetric(pred, label, mse, rpred, rlabel, [], 100000000)

    def Valid(self, data):
        block  = self.Parser(data.Read())
        rlabel = block.sub("label")
        label  = self.TransLabel(rlabel)
        sparse = block.sub("sparse")
        wght   = layers.SparseTablePull(sparse, self.table)
        pred   = layers.WeightReduce(sparse, wght)
        mse    = layers.Loss(pred, label, layers.LossType.MSE)
        rpred  = self.RevertPred(pred)
        ValidationRegressionMetric(pred, label, mse, rpred, rlabel, [], 100000000)

    def score(self, data):
        erase_regression_accumulator()
        return -float(self.validate(data)['validation_mse'])

class SlotwiseMSE(LinearRegression, SlotwiseLR):
    def Train(self, data):
        block  = data.Read(self.conf.lr.training_mode, self.conf.lr.mini_batch_size)
        rlabel = block.sub("label")
        label  = self.TransLabel(rlabel)
        sparse = block.sub("sparse")
        index  = block.sub("index")
        wght   = layers.SparseTablePull(sparse, self.table)
        tsum   = layers.SparseCombine(sparse, wght, self.slots)
        bsum   = layers.MapAt(index, self.index_col, self.base_wsum)
        pred   = layers.BroadcastAdd(bsum, tsum)
        mse    = layers.Loss(pred, label, layers.LossType.MSE)
        layers.Minimize(mse)

    def Valid(self, data):
        block  = self.Parser(data.Read())
        rlabel = block.sub("label")
        label  = self.TransLabel(rlabel)
        sparse = block.sub("sparse")
        index  = block.sub("index")
        wght   = layers.SparseTablePull(sparse, self.table)
        tsum   = layers.SparseCombine(sparse, wght, self.slots)
        bsum   = layers.MapAt(index, self.index_col, self.base_wsum)
        pred   = layers.BroadcastAdd(bsum, tsum)
        rpred  = self.RevertPred(pred)
        mse    = layers.Loss(pred, label, layers.LossType.MSE)
        EvaluateRegressionMetric(pred, label, mse, rpred, rlabel, self.slots, 100000000)

    def score(self, data):
        acc = json.loads(self.validate(data)['evaluate_mse'])
        ret = {}
        for k in self.slots:
            try:
                ret[k] = -float(acc[str(k)])
            except:
                ret[k] = -math.inf
        erase_regression_evaluate()
        return ret

