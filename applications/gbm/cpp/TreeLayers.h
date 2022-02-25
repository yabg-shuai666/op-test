#ifndef PARADIGM4_PICO_APPLICATIONS_GBM_TREE_LAYERS_H
#define PARADIGM4_PICO_APPLICATIONS_GBM_TREE_LAYERS_H

#include "layer/layer.h"
#include "graph/node.h"
#include "executor/session.h"

#include "tree.h"
#include "dense_array.h"
#include "node_index.h"

namespace paradigm4 {
namespace pico {
namespace applications {
namespace mle {

std::string CalcNodeIndex_doc = "";
std::string UpdateWeights_doc = "";
std::string TreeWeights_doc = "";

class CalcNodeIndexNode : public Node {
public:
    CalcNodeIndexNode(Tree tree, var idx, var cont, var disc, var out)
        : Node(fmt("CalcNodeIndexNode(%s, %s, %s, %s)", idx.name(), cont.name(), disc.name(), out.name())) {
        inputs()  = { idx, cont, disc };
        outputs() = { out };
        _tree = tree.entity();
        _cont = cont.schema().type;
        _disc = disc.schema().type;
    }

    void calc(int, Session& sess) override {
        auto& idx  = sess.get(input(0)).data<std::vector<int32_t>>();
        auto& out  = sess.get(output()).data<std::vector<int32_t>>();
        auto& cont = sess.get(input(1)).data<DenseArray>();
        auto& disc = sess.get(input(2)).data<DenseArray>();
        out = idx;
        inner_calc(out, cont, disc, _cont, _disc, _tree);
    }

    static void inner_calc( std::vector<int32_t>& idx, DenseArray& cont, DenseArray& disc,
                            uint8_t cont_type, uint8_t disc_type,
                            std::shared_ptr<TreeEntity> tree) {
        for (size_t i = 0; i < idx.size(); ++i) {
            auto& node = tree->nodes()[idx[i]];
            if (node.is_leaf) continue;
            if (node.is_discrete) {
                if (disc_type == DENSE_ARRAY_8::type) {
                    int v  = disc.data<uint8_t>(i)[node.fidx];
                    idx[i] = node.son[v == node.bin];
                } else {
                    int v  = disc.data<int32_t>(i)[node.fidx];
                    idx[i] = node.son[v == node.bin];
                }
            } else {
                if (cont_type == DENSE_ARRAY_8::type) {
                    int v  = cont.data<uint8_t>(i)[node.fidx];
                    idx[i] = node.son[v == DENSE_ARRAY_8::miss  ? node.defv : (v > node.bin)];
                } else {
                    int v  = cont.data<int32_t>(i)[node.fidx];
                    idx[i] = node.son[v == DENSE_ARRAY_32::miss ? node.defv : (v > node.bin)];
                }
            }
        }
    }

private:
    std::shared_ptr<TreeEntity> _tree;
    uint8_t _cont, _disc;
};

var CalcNodeIndex_build(var nidx, var cont, var disc, Tree tree) {
    SCHECK(nidx.schema().type == DType::INT32);
    auto nm = fmt("calc_nidx:%d,%d,%d", nidx, cont, disc);
    var out = Graph::ctx().new_variable(nm, NodeIndex::schema());
    declare_forward<CalcNodeIndexNode>(tree, nidx, cont, disc, out);
    return out;
}

class UpdateWeightsNode : public Node {
public:
    UpdateWeightsNode(Tree tree, VectorTable wsum, NodeIndex nidx, var gidx, var label, var cont, var disc, var out)
        : Node(fmt("UpdateWeightsNode(%s, %s, %s, %s)", gidx.name(), label.name(), cont.name(), disc.name())) {
        inputs()  = { gidx, label, cont, disc };
        outputs() = { out };
        _cont = cont.schema().type;
        _disc = disc.schema().type;
        _tree = tree.entity();
        _wsum = wsum.entity();
        _nidx = nidx.entity();
    }

    void initialize(int tnum) override {
        _cache_wsum.resize(tnum);
        _cache_nidx.resize(tnum);
        _cache_label.resize(tnum);
        for (int i = 0; i < tnum; ++i)
            _cache_label[i].resize(_tree->nodes().size(), 0);
    }

    void calc(int tid, Session& sess) override {
        auto& gidx  = sess.get(input(0)).data<GlobalIndex>();
        auto& label = sess.get(input(1)).data<DTensor2>();
        auto& cont  = sess.get(input(2)).data<DenseArray>();
        auto& disc  = sess.get(input(3)).data<DenseArray>();
        auto& wsum  = _cache_wsum[tid];
        auto& nidx  = _cache_nidx[tid];
        auto& tree  = _cache_label[tid];

        wsum.resize(gidx.size());
        nidx.resize(gidx.size());

        int cur = 0;
        for (auto& seg : gidx.segments) {
            _wsum->get_value(wsum.data() + cur, seg.start, seg.length);
            _nidx->get_value(nidx.data() + cur, seg.start, seg.length);
            cur += seg.length;
        }

        CalcNodeIndexNode::inner_calc(nidx, cont, disc, _cont, _disc, _tree);
        auto& nodes = _tree->nodes();
        for (size_t i = 0; i < wsum.size(); ++i) {
            tree[nidx[i]] += label.raw_get_value({i, 0});
            wsum[i] += nodes[nidx[i]].weight;
        }

        cur = 0;
        for (auto& seg : gidx.segments) {
            _wsum->set_value(wsum.data() + cur, seg.start, seg.length);
            cur += seg.length;
        }
    }

    void finalize() override {
        for (size_t i = 1; i < _cache_label.size(); ++i)
            merge(_cache_label[i], _cache_label[0]);
        auto& nodes = _tree->nodes();
        for (int i = nodes.size() - 1; i > 0; --i)
            _cache_label[0][nodes[i].pre] += _cache_label[0][i];

        std::vector<double> label(_cache_label[0].size());
        pico_all_reduce(_cache_label[0], label, merge);

        for (size_t i = 0; i < nodes.size(); ++i)
            nodes[i].label_sum = label[i];
    }

private:
    static void merge(const std::vector<double>& in, std::vector<double>& out) {
        if (in.size() == 0) return;
        if (out.size() == 0) {
            out = in;
            return;
        }
        SCHECK(in.size() == out.size());
        for (size_t i = 0; i < in.size(); ++i)
            out[i] += in[i];
    }

    std::shared_ptr<TreeEntity> _tree;
    std::shared_ptr<VectorTableEntity<real_t>> _wsum;
    std::shared_ptr<VectorTableEntity<int32_t>> _nidx;
    uint8_t _cont, _disc;

    std::vector<std::vector<real_t>>  _cache_wsum;
    std::vector<std::vector<int32_t>> _cache_nidx;
    std::vector<std::vector<double>>  _cache_label;
};

var UpdateWeights_build(var gidx, var label, var cont, var disc,
                        Tree tree, VectorTable wsum, NodeIndex nidx) {
    SCHECK(gidx .schema().type == DType::INDEX);
    SCHECK(label.schema().type == DType::TENSOR2);
    auto nm = fmt("upd_wgh:%d,%d", gidx, label);
    var out = Graph::ctx().new_variable(nm, Schema::entry());
    declare_forward<UpdateWeightsNode>(tree, wsum, nidx, gidx, label, cont, disc, out);
    return out;
}

class TreeWeightsNode : public Node {
public:
    TreeWeightsNode(const std::vector<Tree>& trees, var data, var out)
        : Node(fmt("TreeWeightsNode(%s, %s)", data.name(), out.name())) {
        inputs()  = { data };
        outputs() = { out };
        for (auto& tree : trees)
            _trees.push_back(tree.entity());
    }

    void calc(int, Session& sess) override {
        auto& data = sess.get( input()).data<InstanceBlock>();
        auto& out  = sess.get(output()).data<DTensor2>();
        out.resize(data.size(), 1);

        std::unordered_map<feature_index_t, double, feature_index_hasher_t> tmp;
        for (size_t i = 0; i < data.size(); ++i) {
            tmp.clear();
            for (size_t j = data.fea_offset[i]; j < data.fea_offset[i+1]; ++j)
                tmp[data.feas[j]] = data.values[j];

            double w = 0;
            for (auto& tree : _trees) {
                int idx = 0;
                auto& nodes = tree->nodes();
                while (!nodes[idx].is_leaf) {
                    auto& node = nodes[idx];
                    auto it = tmp.find(node.fea);
                    if (it == tmp.end())
                        idx = node.son[node.defv];
                    else if (node.is_discrete)
                        idx = node.son[1];
                    else
                        idx = node.son[it->second > node.cond];
                }
                w += nodes[idx].weight;
            }
            out.raw_set_value({i, 0}, w);
        }
    }

private:
    std::vector<std::shared_ptr<TreeEntity>> _trees;
};

var TreeWeights_build(var data, std::vector<Tree> trees) {
    SCHECK(data.schema().type == DType::GCFORMAT);
    auto nm = fmt("Twgt:%d", data);
    var out = Graph::ctx().new_variable(nm, Schema::tensor2(1));
    declare_forward<TreeWeightsNode>(trees, data, out);
    return out;
}

} // namespace mle
} // namespace applications
} // namespace pico
} // namespace paradigm4

#endif
