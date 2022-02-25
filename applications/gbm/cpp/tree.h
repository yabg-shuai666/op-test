#ifndef PARADIGM4_PICO_APPLICATIONS_GBM_TREE_H
#define PARADIGM4_PICO_APPLICATIONS_GBM_TREE_H

#include "layer/layer.h"
#include "graph/node.h"
#include "executor/session.h"

#include "common.h"
#include "histogram.h"
#include "top_category.h"

namespace paradigm4 {
namespace pico {
namespace applications {
namespace mle {

struct tree_node_t {
    // before statistic
    int64_t ins_num   = 0;
    int64_t valid_num = 0;
    int32_t depth     = 0;
    int32_t is_leaf   = true;
    double label_sum  = 0;  // except root
    double weight     = 0;     // except root

    // after statistic
    int32_t is_discrete = false;
    feature_index_t fea;
    double  cond;
    int32_t fidx;
    int32_t bin;

    int32_t pre = 0;
    int32_t son[2] = {0, 0};
    int32_t defv = 0;
    double gain = std::numeric_limits<double>::lowest();
    // + is_leaf

    tree_node_t() {}

    tree_node_t(int p, int64_t n, int64_t v, int32_t d, int32_t i, double w) :
        ins_num(n), valid_num(v), depth(d), is_leaf(i), weight(w), pre(p) {}
};

struct Candidate {
    double gain = std::numeric_limits<double>::lowest();
    int val = -1, defv = -1;
    agg_t left, right, total;

    Candidate() {}
    Candidate(double g, int v, int d, agg_t l, agg_t r, agg_t t)
        : gain(g), val(v), defv(d), left(l), right(r), total(t) {}
};

class TreeEntity {
public:
    TreeEntity(double mw, int64_t mn, double mg, double l0, double l2, double lr) {
        _nodes.resize(1);
        min_child_weight = mw;
        min_child_n = mn;
        min_split_gain = mg;
        lambda_0 = l0;
        lambda_2 = l2;
        learning_rate = lr;
    }

    const std::vector<tree_node_t>& nodes() const {
        return _nodes;
    }

    std::vector<tree_node_t>& nodes() {
        return _nodes;
    }

    int depth() const {
        return _depth;
    }

    int leaf_num() const {
        return _leaf_num;
    }

    std::vector<int> extend(std::shared_ptr<HistogramEntity> cont_hist,
                            std::shared_ptr<QuantileTableEntity> cont_dist,
                            std::shared_ptr<HistogramEntity> disc_hist,
                            std::shared_ptr<TopCategoryEntity> disc_dist) {
        std::vector<int> to_extend;
        int cont_cols = cont_hist->fea_size(), cont_rows = cont_hist->max_n() - 1;
        int disc_cols = disc_hist->fea_size(), disc_rows = disc_hist->max_n() - 1;

        int extend_size = _trained_size;
        _trained_size = _nodes.size();
        for (int nidx = extend_size; nidx < _trained_size; ++nidx) {
            Candidate cand;
            int select = -1;
            bool is_discrete = false;

            if (cont_hist->histogram(nidx).size() == 0) {
                auto& node = _nodes[_nodes[nidx].pre];
                auto minus = node.son[node.son[0] == nidx];
                {
                    auto& hist = cont_hist->histogram(nidx);
                    auto& calc = cont_hist->histogram(minus);
                    if (calc.size() == 0) continue;
                    hist = cont_hist->histogram(_nodes[nidx].pre);
                    for (size_t i = 0; i < hist.size(); ++i)
                        hist[i] -= calc[i];
                } {
                    auto& hist = disc_hist->histogram(nidx);
                    auto& calc = disc_hist->histogram(minus);
                    hist = disc_hist->histogram(_nodes[nidx].pre);
                    for (size_t i = 0; i < hist.size(); ++i)
                        hist[i] -= calc[i];
                }
            }

            for (int fea = 0; fea < cont_cols; ++fea) {
                auto* hist = cont_hist->hist_ptr(nidx, fea);
                Candidate lmax, rmax, cmax;

                agg_t tot, total = hist[cont_rows];
                for (int i = 0; i < cont_rows; ++i)
                    tot += hist[i];

                agg_t left;
                for (int i = 0; i < cont_rows - 1; ++i) {
                    left += hist[i];
                    agg_t right = total - left;
                    if (!is_valid(left, right)) continue;

                    auto gain = calc_gain(left, right, total);
                    if (gain > rmax.gain)
                        rmax = Candidate(gain, i, 1, left, tot-left, total);
                }

                agg_t right;
                for (size_t i = cont_rows - 1; i > 0; --i) {
                    right += hist[i];
                    agg_t left = total - right;
                    if (!is_valid(left, right)) continue;

                    auto gain = calc_gain(left, right, total);
                    if (gain > lmax.gain)
                        lmax = Candidate(gain, i - 1, 0, tot-right, right, total);
                }
   
                if (fequal(lmax.gain, rmax.gain))
                    cmax = random() > 0.5 ? lmax : rmax;
                else
                    cmax = lmax.gain > rmax.gain ? lmax : rmax;

                if (cmax.gain > cand.gain) {
                    cand = cmax;
                    select = fea;
                }
            }

            for (int fea = 0; fea < disc_cols; ++fea) {
                auto* hist = disc_hist->hist_ptr(nidx, fea);
                Candidate dmax;

                agg_t total = hist[disc_rows];
                for (int i = 0; i < disc_rows; ++i) {
                    agg_t right = hist[i];
                    agg_t left  = total - right;
                    if (!is_valid(left, right)) continue;

                    auto gain = calc_gain(left, right, total);
                    if (gain > dmax.gain)
                        dmax = Candidate(gain, i, 0, agg_t(), right, total);
                }
                if (dmax.gain > cand.gain) {
                    cand = dmax;
                    select = fea;
                    is_discrete = true;
                }
            }


            auto& node = _nodes[nidx];
            if (nidx == 0) {
                node.ins_num   = cand.total.n;
                node.weight    = weight(cand.total);
                node.valid_num = node.ins_num;
            }
            if (select == -1 || flessq(cand.gain, min_split_gain)) continue;

            node.gain    = cand.gain;
            node.fidx    = select;
            node.bin     = cand.val;
            node.defv    = cand.defv;
            node.is_leaf = false;
            node.is_discrete = is_discrete;
            if (is_discrete) {
                node.fea.slot = disc_dist->slots()[select];
                node.fea.sign = disc_dist->pull(node.fea.slot)[cand.val];
            } else {
                node.fea  = cont_dist->features()[select];
                node.cond = cont_dist->pull(select)[cand.val];
            }

            agg_t l, r;
            if (cand.defv == 0) {
                l = cand.total - cand.right;
                r = cand.right;
            } else {
                l = cand.left;
                r = cand.total - cand.left;
            }
            node.son[0] = _nodes.size();
            node.son[1] = _nodes.size() + 1;
            _nodes.emplace_back(nidx, l.n, cand.left .n, _depth + 1, true, weight(l));
            _nodes.emplace_back(nidx, r.n, cand.right.n, _depth + 1, true, weight(r));

            ++ _leaf_num;
            to_extend.push_back(_nodes.size() - 1 - (l.n < r.n));
        }
        ++ _depth;

        return to_extend;
    }

private:
    bool is_valid(const agg_t& left, const agg_t& right) {
        if (flessq(left.h,  min_child_weight))
            return false;
        if (flessq(right.h, min_child_weight))
            return false;
        if (left .n <= min_child_n)
            return false;
        if (right.n <= min_child_n)
            return false;
        return true;
    }

    double loss(const agg_t& agg) {
        return agg.g * agg.g / (agg.h + lambda_2);
    }

    double weight(const agg_t& agg) {
        return -agg.g / (agg.h + lambda_2) * learning_rate;
    }

    double calc_gain(const agg_t& left, const agg_t& right, const agg_t& total) {
        return loss(left) + loss(right) - loss(total) - lambda_0;
    }

    std::vector<tree_node_t> _nodes;
    double min_child_weight;
    int64_t min_child_n;
    double min_split_gain;
    double lambda_0, lambda_2, learning_rate;

    int _depth = 0, _leaf_num = 1;
    int _trained_size = 0;
};

class Tree {
public:
    Tree(double mw, int64_t mn, double mg, double l0, double l2, double lr) {
        _entity = std::make_shared<TreeEntity>(mw, mn, mg, l0, l2, lr);
    }

    std::shared_ptr<TreeEntity> entity() const {
        return _entity;
    }

    int depth() const {
        return _entity->depth();
    }

    int leaf_num() const {
        return _entity->leaf_num();
    }

    int node_num() const {
        return _entity->nodes().size();
    }

    std::vector<int> extend(Histogram cont_hist, QuantileTable cont_dist,
                            Histogram disc_hist, TopCategoryTable disc_dist) {
        return _entity->extend( cont_hist.entity(), cont_dist.entity(),
                                disc_hist.entity(), disc_dist.entity());
    }

private:
    std::shared_ptr<TreeEntity> _entity;
};

} // namespace mle
} // namespace applications
} // namespace pico
} // namespace paradigm4

#endif
