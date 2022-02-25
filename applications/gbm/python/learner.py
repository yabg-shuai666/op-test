# -*- coding: utf-8 -*-

import os
import pygdbt
from pygdbt.common import *
from pygdbt.app_common import *
import gbmtools

class DTreeLearner:
    def __init__(self, gbm, conf):
        self.ctx  = gbm
        self.conf = conf
        self.cont_dist = tables.QuantileTable(self.conf.propose.max_n)
        self.disc_dist = gbmtools.TopCategoryTable(self.conf.propose.discrete_max_n)
        self.cont_hist = gbmtools.Histogram()
        self.disc_hist = gbmtools.Histogram()
        self.nidx = gbmtools.NodeIndex(1)
        self.tree = []
        self.cur_tree = None

    def init(self, data):
        def _stat_():
            block = data.Read().sub("data")
            contv = layers.ParseSparseInput(block, layers.ParseType.CONT)
            discv = layers.ParseSparseInput(block, layers.ParseType.DISC)
            layers.QuantileStat(contv, self.cont_dist)
            gbmtools.TopCategoryStat(discv, self.disc_dist)
        pygdbt.execute(_stat_, True, gbmtools.debug_level)

        self.cont_slots = self.cont_dist.features()
        self.disc_slots = self.disc_dist.slots()
        self.cache = pygdbt.Cache(gbmtools.cache_uri)

        def _parse_():
            rdata = data.Read()
            index = rdata.sub("index")
            block = rdata.sub("data")
            label = layers.ParseLabel(block)
            cont  = gbmtools.InstanceBlock2DenseArray( block, self.cont_slots, self.cont_dist)
            disc  = gbmtools.InstanceBlock2SparseArray(block, self.disc_slots, self.disc_dist)
            merge = layers.Merge([index, label, cont, disc], ["index", "label", "cont", "disc"])
            layers.Sink(merge, self.cache)
        pygdbt.execute(_parse_, True, gbmtools.debug_level)

    def _train_one_level_(self):
        block = self.cache.Read()
        label = block.sub("label")
        cont  = block.sub("cont")
        disc  = block.sub("disc")
        gidx  = block.sub("index")
        nidx  = gbmtools.GetNodeIndex(gidx, self.nidx)
        nidx  = gbmtools.CalcNodeIndex(nidx, cont, disc, self.cur_tree)
        wsum  = layers.VectorGet(gidx, self.ctx.train_wsum)
        grad  = self.ctx.loss.calc_grad(label, wsum)
        hess  = self.ctx.loss.calc_hess(label, wsum)
        gbmtools.HistogramStat(nidx, cont, grad, hess, self.cont_hist, self.toextend)
        gbmtools.HistogramStat(nidx, disc, grad, hess, self.disc_hist, self.toextend)
        gbmtools.SetNodeIndex(gidx, nidx, self.nidx)

    def _update_weights_(self):
        def _update_train_():
            block = self.cache.Read()
            label = block.sub("label")
            gidx  = block.sub("index")
            cont  = block.sub("cont")
            disc  = block.sub("disc")
            gbmtools.UpdateWeights(gidx, label, cont, disc, self.cur_tree, self.ctx.train_wsum, self.nidx)
        pygdbt.execute(_update_train_, True, gbmtools.debug_level)
        self.nidx.reset_value()
        self.cont_hist.clear()
        self.disc_hist.clear()

        if self.ctx.valid_data is None:
            return
        def _update_valid_():
            block = self.ctx.valid_data.Read()
            index = block.sub("index")
            data  = block.sub("data")
            wsum  = layers.VectorGet(index, self.ctx.valid_wsum)
            leaf  = gbmtools.TreeWeights(data, [self.cur_tree])
            wsum  = layers.Add(wsum, leaf)
            layers.VectorSet(index, wsum, self.ctx.valid_wsum)
        pygdbt.execute(_update_valid_, True, gbmtools.debug_level)

    def Predict(self, data):
        return gbmtools.TreeWeights(data, self.tree)

    def train(self):
        conf = self.conf
        self.cur_tree = gbmtools.Tree(conf)
        self.toextend = [0]

        while len(self.toextend) > 0:
            if self.cur_tree.depth() >= conf.max_depth:
                break
            if self.cur_tree.leaf_num() >= conf.max_leaf_n:
                break

            self.cont_hist.init(len(self.cont_slots), self.cur_tree.node_num(), self.cont_dist.max_n())
            self.disc_hist.init(len(self.disc_slots), self.cur_tree.node_num(), self.disc_dist.max_n())
            pygdbt.execute(self._train_one_level_)

            self.toextend = self.cur_tree.extend(self.cont_hist, self.cont_dist, self.disc_hist, self.disc_dist)

        self._update_weights_()
        self.tree.append(self.cur_tree)

    def load(self, path):
        self.tree = gbmtools.load_trees(path)

    def dump(self, path):
        if not gbmtools.dump_trees(self.tree, path):
            raise Exception('dump model failed')

learner_map = {
    'dtree' : DTreeLearner
}
