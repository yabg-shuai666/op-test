#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pygdbt
from pygdbt.common import *
from pygdbt.app_common import *

class Predict:
    class Conf(Configure):
        def __init__(self):
            Configure.__init__(self)
            self.declare('input_model_path', "input model path",
                False, DefaultChecker(), ModelInputURI())
            self.declare('input_instance_path', "input instance path",
                False, DefaultChecker(), list_config(PredInputURI))
            self.declare('output_predict_result_path', "output predict result path",
                False, DefaultChecker(), PredTextOutputURI())
            self.declare('output_predict_result_prefix', "output predict result prefix",
                True, DefaultChecker(), 'predict_result')
            self.declare('debug_level', 'debug level (FLAGS_v)',
                True, GreaterEqualChecker(0), 0)

        def load_config(self, conf):
            if not Configure.load_config(self, conf):
                return False
            self.is_copy   = self.output_predict_result_path.conf['is_copy_raw_ins']
            self.is_detail = self.output_predict_result_path.conf['is_output_predict_detail']
            self.parser    = self.output_predict_result_path.conf['parser']
            self.format    = self.output_predict_result_path.conf['format']
            self.output_predict_result_path.conf = {'format': 'txt', 'parser': 'none'}
            return True

    def __init__(self, conf):
        self.conf = Predict.Conf()
        SCHECK(self.conf.load_config(conf), "load configure failed.")

    def load_model(self):
        self.app = load_metadata(self.conf.input_model_path.path)
        return self.app.load_model(self.conf.input_model_path)

    def inner_main(self):
        conf = self.conf
        pygdbt.pico_tools.progress_initialize(2)

        SCHECK(self.load_model(), "load model failed.")
        pygdbt.pico_tools.progress_report(1)

        data = pygdbt.GCFormat(conf.input_instance_path)
        sink = pygdbt.PredictResult(conf.output_predict_result_path)
        def graph():
            block = data.Read()
            pred  = self.app.evaluate(block)
            out   = layers.PredictOutput(block, pred, conf.is_copy, conf.is_detail, conf.parser)
            layers.SinkFile(out, sink, conf.output_predict_result_prefix)
        pygdbt.execute(graph, False, conf.debug_level)
        pygdbt.pico_tools.progress_report(1)

if __name__ == '__main__':
    app_main(Predict)
