# -*- coding: utf-8 -*-

import pygdbt
from pygdbt.common import *
from pygdbt.app_common import *
import libgbmtools as core

debug_level = 0
cache_uri = "mem://.?format=mem"

InstanceBlock2DenseArray  = core.InstanceBlock2DenseArray
InstanceBlock2SparseArray = core.InstanceBlock2SparseArray
NodeIndex                 = core.NodeIndex
CalcNodeIndex             = core.CalcNodeIndex
Histogram                 = core.Histogram
GetNodeIndex              = core.GetNodeIndex
SetNodeIndex              = core.SetNodeIndex
HistogramStat             = core.HistogramStat
UpdateWeights             = core.UpdateWeights
TopCategoryStat           = core.TopCategoryStat
TreeWeights               = core.TreeWeights

def Tree(conf):
    return core.Tree(   conf.min_child_weight,
                        conf.min_child_n,
                        conf.min_split_gain,
                        conf.l0_coef,
                        conf.l2_coef,
                        conf.learning_rate)

TopCategoryTable = core.TopCategoryTable

dump_trees = core.dump_trees
load_trees = core.load_trees
