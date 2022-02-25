#ifndef PARADIGM4_PICO_APPLICATIONS_GBM_HISTOGRAM_LAYERS_H
#define PARADIGM4_PICO_APPLICATIONS_GBM_HISTOGRAM_LAYERS_H

#include "layer/layer.h"
#include "graph/node.h"
#include "executor/session.h"

#include "dense_array.h"
#include "histogram.h"

namespace paradigm4 {
namespace pico {
namespace applications {
namespace mle {

std::string HistogramStat_doc = "";

template <class ARRAY>
class HistogramStatNode : public Node {
public:
    typedef typename ARRAY::value_t value_t;

    HistogramStatNode(  Histogram hist, const std::vector<int>& to_extend,
                        var idx, var d, var g, var h, var out)
        : Node(fmt("HistogramStatNode(%s, %s, %s, %s)", idx.name(), d.name(), g.name(), h.name())) {
        inputs()  = { idx, d, g, h };
        outputs() = { out };
        _table = hist.entity();
        _to_extend.insert(to_extend.begin(), to_extend.end());
    }

    void initialize(int tnum) override {
        _table->initialize(tnum, true);
    }

    void calc(int tid, Session& sess) override {
        auto& idx  = sess.get(input(0)).template data<std::vector<int32_t>>();
        auto& data = sess.get(input(1)).template data<DenseArray>();
        auto& grad = sess.get(input(2)).template data<DTensor2>();
        auto& hess = sess.get(input(3)).template data<DTensor2>();

        std::unordered_map<int, std::vector<size_t>> tmp;
        for (size_t i = 0; i < idx.size(); ++i)
            tmp[idx[i]].push_back(i);

        for (auto& p : tmp) if (_to_extend.count(p.first)) {
            auto& hist = _table->cache(tid, p.first);
            for (auto i : p.second) {
                agg_t delta(grad.raw_get_value({i, 0}), hess.raw_get_value({i, 0}));
                value_t* feas = data.template data<value_t>(i);
                for (int j = 0, k = 0; j < _table->fea_size(); ++j, k += _table->max_n()) {
                    hist[k + _table->max_n() - 1] += delta;
                    if (feas[j] != ARRAY::miss)
                        hist[k + feas[j]] += delta;
                }
            }
        }
    }

private:
    std::shared_ptr<HistogramEntity> _table;
    std::unordered_set<int> _to_extend;
};

var HistogramStat_build(var index, var data, var grad, var hess,
                        Histogram hist, std::vector<int> to_extend) {
    SCHECK(index.schema().type == DType::INT32);
    SCHECK(grad .schema().type == DType::TENSOR2);
    SCHECK(hess .schema().type == DType::TENSOR2);
    Graph::ctx().add_table(hist.entity().get());

    auto tp = data.schema().type;
    auto nm = fmt("hist_stat:(%d,%d,%d,%d)", index, data, grad, hess);
    var out = Graph::ctx().new_variable(nm, Schema::entry());

    if (tp == DENSE_ARRAY_8::type)
        declare_forward<HistogramStatNode<DENSE_ARRAY_8 >>(
            hist, to_extend, index, data, grad, hess, out);
    else if (tp == DENSE_ARRAY_32::type)
        declare_forward<HistogramStatNode<DENSE_ARRAY_32>>(
            hist, to_extend, index, data, grad, hess, out);
    else
        SLOG(FATAL) << tp;
    return out;
}

} // namespace mle
} // namespace applications
} // namespace pico
} // namespace paradigm4

#endif
