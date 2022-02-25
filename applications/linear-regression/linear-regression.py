#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pygdbt
from pygdbt.common import *
from pygdbt.app_common import *
import liblrtools as lrtools

class IdentityFunction:
    def get_val(self, x):
        return x
    def get_inverse_val(self, x):
        return x

class SigmoidFunction:
    def get_val(self, x):
        return layers.Activation(x, layers.ActType.SIGMOID)
    def get_inverse_val(self, x):
        return None

class Log1pFunction:
    def get_val(self, x):
        return layers.ScaleLog(x)
    def get_inverse_val(self, x):
        return layers.ScaleExp(x)

class LinearRegression:
    class Conf(BaseAppConf):
        def __init__(self):
            BaseAppConf.__init__(self)
            self.declare("beta", "FTRL-beta",
                True, GreaterEqualChecker(0.0), 1.0)
            self.declare("alpha", "FTRL-alpha",
                True, GreaterChecker(0.0), 0.05)
            self.declare("lambda_1", "the weight of L1 regularization term",
                True, GreaterEqualChecker(0.0), 1.0)
            self.declare("lambda_2", "the weight of L2 regularization term",
                True, GreaterEqualChecker(0.0), 1.0)
            self.declare("feature_creation_ratio", "the feature creation ratio",
                True, RangeCheckerCC(0.0, 1.0), 1.0)
            self.declare("show_decay_rate", "the show decay rate",
                True, RangeCheckerOC(0.0, 1.0), 1.0)
            self.declare("decayed_show_filter_threshold", "decayed show filter threshold",
                True, DefaultChecker(), 0.0)
            self.declare("enable_show_adjust", "enable gradient show adjust",
                True, DefaultChecker(), False)
            self.declare("enable_scale_free", "enable regularization scale free",
                True, DefaultChecker(), False)
            self.declare('link_function_for_pred', "The function used for smoothing predict, identity/sigmoid/log1p",
                True, EnumChecker(set(["identity", "sigmoid", "log1p"])), 'identity')
            self.declare('link_function_for_label', "The function used for smoothing label, identity/sigmoid/log1p",
                True, EnumChecker(set(["identity", "sigmoid", "log1p"])), 'identity')

    def __init__(self, conf):
        self.conf = LinearRegression.Conf()
        SCHECK(self.conf.load_config(conf), 'invalid configure')

        self.table = tables.SparseTable(tables.FTRL(
                alpha              = self.conf.alpha,
                beta               = self.conf.beta,
                lambda_1           = self.conf.lambda_1,
                lambda_2           = self.conf.lambda_2,
                enable_show_adjust = self.conf.enable_show_adjust,
                enable_scale_free  = self.conf.enable_scale_free 
            ))
        link_map = {
            'identity' : IdentityFunction,
            'sigmoid'  : SigmoidFunction,
            'log1p'    : Log1pFunction,
        }
        self.link_function_for_label = link_map[self.conf.link_function_for_label]()
        self.link_function_for_pred  = link_map[self.conf.link_function_for_pred]()

    def dump_model(self, path):
        conf = self.conf
        if self.pas == conf.training_pass_num:
            lrtools.weight_decay(self.table, conf.show_decay_rate, conf.decayed_show_filter_threshold)
        else:
            lrtools.count_effect(self.table)
        self.table.dump(path + "/weights")

        pygdbt.pico_tools.accumulator_snapshot()
        pygdbt.pico_tools.save_metadata(path + "/metadata")
        if pygdbt.pico_tools.comm_rank() == 0:
            pygdbt.app_common.save_metadata(path, self)

    def load_model(self, path):
        if self.table.load(path + "/weights"):
            return True
        else:
            return False

    def parser(self, data):
        label  = layers.ParseLabel(data)
        sparse = layers.ParseSparseInput(data)
        return label, sparse

    def train(self):
        if self.pas == 1:
            read = self.train_data.Read(self.conf.training_mode, self.conf.mini_batch_size)
            rlabel, sparse = self.parser(read)
            if self.conf.training_pass_num > 1:
                block = layers.Merge([rlabel, sparse], ['label', 'sparse'])
                layers.Sink(block, self.train_cache)
        else:
            block  = self.train_cache.Read(self.conf.training_mode, self.conf.mini_batch_size)
            rlabel = block.sub('label')
            sparse = block.sub('sparse')

        label = self.link_function_for_label.get_val(rlabel)
        wght  = layers.SparseTablePull(sparse, self.table)
        wsum  = layers.WeightReduce(sparse, wght)
        pred  = self.link_function_for_pred.get_val(wsum)
        loss  = layers.Loss(pred, label, layers.LossType.SQUARE_LOSS)
        layers.Minimize(loss)

        rpred = self.link_function_for_label.get_inverse_val(pred)
        ProgressiveRegressionMetric(pred, label, loss, rpred, rlabel)

    def need_validate(self):
        return  len(self.conf.input_validation_path) > 0 and (
                self.pas %  self.conf.validate_model_interval == 0 or
                self.pas == self.conf.training_pass_num)

    def need_dump(self):
        return  self.pas == self.conf.training_pass_num or (
                self.conf.sink_model_interval > 0 and
                self.pas % self.conf.sink_model_interval == 0)

    def valid(self):
        if self.pas == self.conf.validate_model_interval:
            rlabel, sparse = self.parser(self.valid_data.Read())
            if self.conf.training_pass_num > self.conf.validate_model_interval:
                block = layers.Merge([rlabel, sparse], ['label', 'sparse'])
                layers.Sink(block, self.valid_cache)
        else:
            block  = self.valid_cache.Read()
            rlabel = block.sub('label')
            sparse = block.sub('sparse')

        label = self.link_function_for_label.get_val(rlabel)
        wght  = layers.SparseTablePull(sparse, self.table)
        wsum  = layers.WeightReduce(sparse, wght)
        pred  = self.link_function_for_pred.get_val(wsum)
        loss  = layers.Loss(pred, label, layers.LossType.SQUARE_LOSS)
        layers.Minimize(loss)

        rpred = self.link_function_for_label.get_inverse_val(pred)
        ValidationRegressionMetric(pred, label, loss, rpred, rlabel)

    def evaluate(self, gc):
        rlabel, sparse = self.parser(gc)
        label = self.link_function_for_label.get_val(rlabel)
        wght  = layers.SparseTablePull(sparse, self.table)
        wsum  = layers.WeightReduce(sparse, wght)
        pred  = self.link_function_for_pred.get_val(wsum)
        loss  = layers.Loss(pred, label, layers.LossType.SQUARE_LOSS)
        rpred = self.link_function_for_label.get_inverse_val(pred)
        EvaluateRegressionMetric(pred, label, loss, rpred, rlabel)
        return rpred

    def predict(self, gc):
        rlabel, sparse = self.parser(gc)
        label = self.link_function_for_label.get_val(rlabel)
        wght  = layers.SparseTablePull(sparse, self.table)
        wsum  = layers.WeightReduce(sparse, wght)
        pred  = self.link_function_for_pred.get_val(wsum)
        rpred = self.link_function_for_label.get_inverse_val(pred)
        return rpred

    def inner_main(self):
        conf = self.conf
        pygdbt.pico_tools.progress_initialize(conf.training_pass_num * 3 + 1)

        if conf.init_model.valid():
            SCHECK(self.load_model(conf.init_model), "load model failed.")
        pygdbt.pico_tools.progress_report(1)

        self.train_data  = pygdbt.GCFormat(conf.input_path)
        self.valid_data  = pygdbt.GCFormat(conf.input_validation_path)
        self.train_cache = pygdbt.Cache(conf.cache_uri)
        self.valid_cache = pygdbt.Cache(conf.cache_uri)

        train_pass = pygdbt.pico_tools.SumAccumulator("trained_pass", 1);
        early_stop = pygdbt.pico_tools.EarlyStop(dict_to_yaml(conf.early_stop.to_dict()))

        self.table.set_creation_ratio(self.conf.feature_creation_ratio)
        actual_trained_pass = 0
        for self.pas in range(1, conf.training_pass_num + 1):
            erase_regression_accumulator()
            pygdbt.execute(self.train, True, conf.debug_level)
            pygdbt.pico_tools.progress_report(1)

            if self.need_validate():
                pygdbt.execute(self.valid, False, conf.debug_level)
            pygdbt.pico_tools.progress_report(1)

            if pygdbt.pico_tools.comm_rank() == 0:
                train_pass.write(1)
            actual_trained_pass += 1

            acc = pygdbt.pico_tools.get_accumulator()
            RLOG(0, "Training finished, Pass #%d Accumulator[JSON]:\t%s" % (self.pas - 1, acc))

            converged = early_stop.is_converge(self.pas - 1);
            if converged or self.need_dump():
                self.dump_model(conf.model_output_path + "/pass-" + str(self.pas - 1));
                early_stop.mark(self.pas - 1);
            else:
                pygdbt.pico_tools.accumulator_snapshot()

            pygdbt.pico_tools.progress_report(1)

            if converged:
                RLOG(0, "Model convergence, finish training")
                break
            self.table.set_creation_ratio(0.0)

        final_pass_src = conf.model_output_path + "/pass-" + str(early_stop.get_final_epoch(True))
        final_pass_dst = conf.model_output_path + "/pass-final"
        if pygdbt.pico_tools.comm_rank() == 0 and pygdbt.pico_tools.file_exists(final_pass_src):
            pygdbt.pico_tools.file_mv(final_pass_src, final_pass_dst)
        else:
            WARNING("Final pass model not exists, " + final_pass_src)

        pygdbt.pico_tools.progress_report((conf.training_pass_num - actual_trained_pass) * 3)
        return 0

if __name__ == '__main__':
    app_main(LinearRegression)
