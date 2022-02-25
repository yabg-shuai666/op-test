#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pygdbt
from pygdbt.common import *
from pygdbt.app_common import *

class LFRConf(BaseAppConf):
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
        self.declare('bucket_list', "list of number of bins, seperated by space",
            True, RegexChecker(r'(^(([1-9]\d*)(\s[1-9]\d*)*|())$)'), "10 100 1000 10000 100000")
        self.declare('diff_tolerance', "difference tolerance between min and max in each bucket",
            True, GreaterEqualChecker(0.0), 1e-8)
        self.declare('shrinkage', "shrinkage, default 0.0",
            True, RangeCheckerCO(0.0, 0.5), 0.0)
        self.declare('inner_bin_value_type', "inner bin value after binning, same/norm/origin, default same",
            True, EnumChecker(set(["same", "norm", "origin"])), 'same')
        self.declare('link_function_for_pred', "The function used for smoothing predict, identity/sigmoid/log1p",
            True, EnumChecker(set(["identity", "sigmoid", "log1p"])), 'identity')
        self.declare('link_function_for_label', "The function used for smoothing label, identity/sigmoid/log1p",
            True, EnumChecker(set(["identity", "sigmoid", "log1p"])), 'identity')

    def load_config(self, conf):
        if not BaseAppConf.load_config(self, conf):
            return False
        self.binning_bucket_list = map3(int, self.bucket_list.split(' '))
        self.bin_value_type = {
            "same"   : layers.LFCType.SAME,
            "norm"   : layers.LFCType.NORM,
            "origin" : layers.LFCType.ORIGIN
        }[self.inner_bin_value_type]
        return True
