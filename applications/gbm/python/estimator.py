# -*- coding: utf-8 -*-

import math
from pygdbt.common import *
from pygdbt.app_common import *
from configure import *

class GBMEstimator(ResEstimator):
    class DataConf(Configure):
        def __init__(self):
            Configure.__init__(self)
            self.declare('num_rows', 'global data row size', True, GreaterChecker(0), 0)
            self.declare('avg_feature_num_per_ins', 'avg_feature_num_per_ins', True, GreaterChecker(0), 0)
            self.declare('avg_raw_ins_size', 'sizeof(instance_id + raw_instance)', True, GreaterChecker(0), 0)

    class Conf(ResEstimator.Conf):
        def __init__(self):
            ResEstimator.Conf.__init__(self)
            self.declare('input_path', 'the input data path', False, DefaultChecker(), list_config(GBMEstimator.DataConf))
            self.declare('output_path', 'result output path, empty if output to stdout', True, DefaultChecker(), '')
            self.declare('disc_feature_num', 'number of disc features', True, DefaultChecker(), 0)
            self.declare('cont_feature_num', 'number of cont features', True, DefaultChecker(), 0)
            self.declare('slot_num', 'number of slots', True, DefaultChecker(), 0)

    def __init__(self, yaml):
        self.gbm_conf = GBMConf()
        self.res_conf = GBMEstimator.Conf()
        self.fwk_conf = yaml['framework']
        app_name = yaml['app_name']

        if not self.gbm_conf.load_config(yaml[app_name]):
            raise ValueError('invalid configure.')
        if not self.res_conf.load_config(yaml['res_estimate']):
            raise ValueError('invalid configure.')

    def _estimate_(self, learner_num, ps_num):
        block_data_size = 0
        ins_num = 0.0
        for data in self.res_conf.input_path.data:
            block_data_size = max(block_data_size, data.avg_feature_num_per_ins * 16)
            ins_num += data.num_rows

        max_n = 0
        depth = 0
        for param in self.gbm_conf.param:
            max_n = max(max_n, param.propose.max_n)
            max_n = max(max_n, param.propose.discrete_max_n)
            depth = max(depth, param.max_depth)
        try:
            cpu = self.fwk_conf['process']['cpu_concurrency']
        except:
            import multiprocessing
            cpu = multiprocessing.cpu_count()
        learner = block_data_size * cpu + 32 * cpu * max_n * math.log(ins_num / max_n / learner_num)
        learner = max(learner, block_data_size * cpu + 24 * cpu * max_n * (self.res_conf.cont_feature_num + self.res_conf.slot_num) * (2**depth))
        learner += 12 * ins_num / learner_num
        learner += len(self.gbm_conf.boost_list) * (2**(depth+1)) * 128

        try:
            channel = self.fwk_conf['channel']['bounded_capacity']
        except:
            channel = 64
        learner += block_data_size * channel * 2
        learner += 10 << 30
        pserver  = max(self.res_conf.disc_feature_num * 16 * 4 * 1.5 / ps_num, 10<<30)
        return int(learner), int(pserver)

    def estimate(self):
        learner, pserver = self._estimate_(1, 1)
        return {
            'learner': {
                "mem": learner >> 20,
                "num": 1,
                "cpu": 1,
            },
            'pserver': {
                "mem": pserver >> 20,
                "num": 1,
                "cpu": 1,
            }
        }
