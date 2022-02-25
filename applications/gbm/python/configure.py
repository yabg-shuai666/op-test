# -*- coding: utf-8 -*-

import pygdbt
from pygdbt.common import *
from pygdbt.app_common import *
import gbmtools
from common import *

class ParamConf(Configure):
    def __init__(self):
        Configure.__init__(self)
        self.type = None
        self.declare('name', "name of param node",
            False, RegexChecker(r"^[A-Za-z]+$"))
        self.declare('row_sample_ratio', "row sample ratio by learner",
            True, RangeCheckerOC(0.0, 1.0), 1.0)
        self.declare('col_sample_ratio', "column sample ratio by learner",
            True, RangeCheckerOC(0.0, 1.0), 1.0)

class DTreeParamConf(ParamConf):
    class ProposeConf(Configure):
        def __init__(self):
            Configure.__init__(self)
            self.declare('max_n', "maximum number of candidates",
                True, GreaterChecker(0), 250)
            self.declare('discrete_max_n', "maximum number of category candidates",
                True, GreaterChecker(0), 250)

    def __init__(self):
        ParamConf.__init__(self)
        self.declare('min_child_weight', "minimum child weight for a tree leaner",
            True, GreaterEqualChecker(0.0), 0.0)
        self.declare('min_child_ratio', "minimum child ratio for a tree leaner",
            True, GreaterEqualChecker(0.0), 0.0)
        self.declare('min_child_n', "minimum child number for a tree leaner",
            True, GreaterEqualChecker(0), 0)
        self.declare('min_split_gain', "minimum split gain for a tree leaner",
            True, GreaterEqualChecker(0.0), 0.0)
        self.declare('max_depth', "maximum depth for tree learner",
            True, GreaterChecker(0), 5)
        self.declare('max_leaf_n', "maximum num of leaf in tree learner",
            True, GreaterChecker(2), 10000)
        self.declare('l0_coef', "coefficient of leaf number regularizer for tree learner",
            True, GreaterEqualChecker(0.0), 0.0)
        self.declare('l2_coef', "coefficient of l2 regularizer of weights for tree leanrer",
            True, GreaterChecker(0.0), 1e-6)
        self.declare('learning_rate', "learning rate",
            True, GreaterChecker(0.0), 1.0)
        self.declare('propose', "tree propose configure",
            True, DefaultChecker(), DTreeParamConf.ProposeConf())

class GBMConf(BaseAppConf):
    param_map = {
        'dtree' : DTreeParamConf
    }

    def __init__(self):
        BaseAppConf.__init__(self)
        self.declare('boost_regex', "boost regex",
            False, DefaultChecker(), '')
        self.declare('param', "list of params",
            False, DefaultChecker(), [])
        self.declare('loss_type', "loss type",
            True, EnumChecker(set(["logloss", "squaredloss", "crossentropy"])), "logloss")
        self.boost_list = []
        self.remove(['training_pass_num', 'mini_batch_size', 'training_mode'])

    def load_config(self, conf):
        if not Configure.load_config(self, conf):
            return False
        i = 0
        num = ''
        key = ''
        while i < len(self.boost_regex):
            if self.boost_regex[i].isdigit():
                num += self.boost_regex[i]
            else:
                key = self.boost_regex[i:]
                break
            i += 1
        self.boost_list = [key] * int(num)

        gbmtools.debug_level = self.debug_level
        gbmtools.cache_uri = self.cache_uri

        format_param = []
        for param in self.param:
            if 'type' not in param:
                WARNING("missing required configure [type] in param")
                return False

            type = dict_pop(param, 'type')
            conf = GBMConf.param_map[type]()
            if not conf.load_config(param):
                return False
            conf.type = type
            format_param.append(conf)
        self.param = format_param
        return True
