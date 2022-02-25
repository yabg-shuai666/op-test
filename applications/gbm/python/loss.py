# -*- coding: utf-8 -*-

import pygdbt
from pygdbt.common import *
from pygdbt.app_common import *
import gbmtools

class Logloss:
    def calc_pred(self, weight):
        return layers.Activation(weight, layers.ActType.SIGMOID)

    def calc_grad(self, label, weight):
        return layers.Subtract(self.calc_pred(weight), label)

    def calc_hess(self, label, weight):
        pred = self.calc_pred(weight)
        temp = layers.Subtract(layers.Constant(1, 1), pred)
        return layers.Multiply(pred, temp, 1)

    def calc_loss(self, label, weight):
        return layers.Loss(weight, label, layers.LossType.SIGMOID_LOGLOSS)

    def train_metric(self, label, weight):
        pred = self.calc_pred(weight)
        loss = self.calc_loss(label, weight)
        layers.   AucAccumulator(pred, label, "training_auc")
        layers.   AvgAccumulator(loss,  "training_logloss")
        layers.   AvgAccumulator(pred,  "training_preq")
        layers.   AvgAccumulator(label, "training_positive_ratio")
        layers.InsNumAccumulator(pred,  "training_ins_num")

    def valid_metric(self, label, weight):
        pred = self.calc_pred(weight)
        loss = self.calc_loss(label, weight)
        ValidationBinaryMetric(pred, label, loss)

    def erase_metric(self):
        erase_binary_accumulator()
        pygdbt.pico_tools.erase_accumulator(['training_auc', 'training_logloss', 'training_preq'])

loss_map = {
    'logloss' : Logloss,
}