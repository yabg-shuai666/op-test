#ifndef PARADIGM4_PICO_APPLICATIONS_GBM_PARSER_LAYERS_H
#define PARADIGM4_PICO_APPLICATIONS_GBM_PARSER_LAYERS_H

#include "layer/layer.h"
#include "graph/node.h"
#include "executor/session.h"

#include "dense_array.h"
#include "top_category.h"

namespace paradigm4 {
namespace pico {
namespace applications {
namespace mle {

std::string InstanceBlock2DenseArray_doc = "";
std::string InstanceBlock2SparseArray_doc = "";

template <typename ARRAY>
class InstanceBlock2DenseArrayNode : public Node {
public:
    typedef typename ARRAY::value_t value_t;

    InstanceBlock2DenseArrayNode(
        const std::vector<feature_index_t>& feas,
        QuantileTable table, var in, var out)
        :   Node(fmt("InstanceBlock2DenseArrayNode(%s, %s)", in.name(), out.name())),
            _feas(feas), _table(table.entity()) {
        inputs()  = { in };
        outputs() = { out };
        // for (auto fea : _feas)
        //     SLOG(INFO) << _table->pull(fea);
    }

    void calc(int, Session& sess) override {
        auto& in  = sess.get( input()).template data<InstanceBlock>();
        auto& out = sess.get(output()).template data<DenseArray>();
        out.template resize<value_t>(in.size(), _feas.size());
        std::memset(out.data(), 0, in.size() * _feas.size() * sizeof(value_t));

        std::unordered_map<feature_index_t, double, feature_index_hasher_t> tmp;
        for (size_t i = 0; i < in.size(); ++i) {
            tmp.clear();
            for (size_t j = in.fea_offset[i]; j < in.fea_offset[i + 1]; ++j)
                tmp[in.feas[j]] = in.values[j];

            value_t* data = out.template data<value_t>(i);
            for (size_t j = 0; j < _feas.size(); ++j) {
                auto it = tmp.find(_feas[j]);
                if (it == tmp.end()) {
                    data[j] = ARRAY::miss;
                } else {
                    auto& dist = _table->pull(it->first);
                    data[j] = std::lower_bound(dist.begin(), dist.end(), it->second) - dist.begin();
                }
            }
        }
    }

private:
    std::vector<feature_index_t> _feas;
    std::shared_ptr<QuantileTableEntity> _table;
};

var InstanceBlock2DenseArray_build(var input, std::vector<std::pair<int64_t, int64_t>> slots, QuantileTable table) {
    SCHECK(input.schema().type == DType::GCFORMAT);
    auto nm = fmt("gc2da:%d", input);
    var out;

    std::vector<feature_index_t> feas(slots.size());
    for (size_t i = 0; i < slots.size(); ++i) {
        feas[i].slot = slots[i].first;
        feas[i].sign = slots[i].second;
    }

    if (table.max_n() < COMPRESS_BOUND) {
        out = Graph::ctx().new_variable(nm, DENSE_ARRAY_8::schema());
        declare_forward<InstanceBlock2DenseArrayNode<DENSE_ARRAY_8>>(feas, table, input, out);
    } else {
        out = Graph::ctx().new_variable(nm, DENSE_ARRAY_32::schema());
        declare_forward<InstanceBlock2DenseArrayNode<DENSE_ARRAY_32>>(feas, table, input, out);
    }
    return out;
}

template <typename ARRAY>
class InstanceBlock2SparseArrayNode : public Node {
public:
    typedef typename ARRAY::value_t value_t;

    InstanceBlock2SparseArrayNode(
        const std::vector<int64_t>& slots,
        TopCategoryTable table, var in, var out)
        :   Node(fmt("InstanceBlock2SparseArrayNode(%s, %s)", in.name(), out.name())),
            _slots(slots), _table(table.entity()) {
        inputs()  = { in };
        outputs() = { out };
    }

    void calc(int, Session& sess) override {
        auto& in  = sess.get( input()).template data<InstanceBlock>();
        auto& out = sess.get(output()).template data<DenseArray>();
        out.template resize<value_t>(in.size(), _slots.size());

        std::unordered_map<int64_t, int64_t> tmp;
        for (size_t i = 0; i < in.size(); ++i) {
            tmp.clear();
            for (size_t j = in.fea_offset[i]; j < in.fea_offset[i + 1]; ++j)
                tmp[in.feas[j].slot] = in.feas[j].sign;

            value_t* data = out.template data<value_t>(i);
            for (size_t j = 0; j < _slots.size(); ++j) {
                auto it = tmp.find(_slots[j]);
                if (it == tmp.end()) {
                    data[j] = ARRAY::miss;
                } else {
                    auto& dist = _table->pull(it->first);
                    auto ptr = std::lower_bound(dist.begin(), dist.end(), it->second);
                    if (*ptr == it->second)
                        data[j] = ptr - dist.begin();
                    else
                        data[j] = ARRAY::miss;
                }
            }
        }
    }

private:
    std::vector<int64_t> _slots;
    std::shared_ptr<TopCategoryEntity> _table;
};

var InstanceBlock2SparseArray_build(var input, std::vector<int64_t> slots, TopCategoryTable table) {
    SCHECK(input.schema().type == DType::GCFORMAT);
    auto nm = fmt("gc2sp:%d", input);
    var out;
    if (table.max_n() < COMPRESS_BOUND) {
        out = Graph::ctx().new_variable(nm, DENSE_ARRAY_8::schema());
        declare_forward<InstanceBlock2SparseArrayNode<DENSE_ARRAY_8 >>(slots, table, input, out);
    } else {
        out = Graph::ctx().new_variable(nm, DENSE_ARRAY_32::schema());
        declare_forward<InstanceBlock2SparseArrayNode<DENSE_ARRAY_32>>(slots, table, input, out);
    }
    return out;
}

} // namespace mle
} // namespace applications
} // namespace pico
} // namespace paradigm4

#endif
