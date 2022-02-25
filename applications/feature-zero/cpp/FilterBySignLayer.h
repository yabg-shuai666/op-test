#ifndef PARADIGM4_PICO_APPLICATIONS_FZ_FILTER_BY_SIGN_H
#define PARADIGM4_PICO_APPLICATIONS_FZ_FILTER_BY_SIGN_H

#include "layer/layer.h"
#include "graph/node.h"
#include "executor/session.h"
#include "table/quantile_table.h"

#include "common.h"

namespace paradigm4 {
namespace pico {
namespace applications {
namespace mle {

std::string FilterBySign_doc = "";

class FilterBySignNode : public Node {
public:
    FilterBySignNode(QuantileIntTable table,
                    const std::vector<int64_t>& lkey,
                    const std::vector<int64_t>& rkey,
                    var in, var out)
        : Node(fmt("FilterBySignNode(%s, %s)", in.name(), out.name())) {
        inputs()  = { in };
        outputs() = { out };
        _table = table.entity();
        SCHECK(lkey.size() == rkey.size());
        lfea = lkey;
        for (size_t i = 0; i < rkey.size(); ++i) {
            rfea.emplace_back(rkey[i], std::numeric_limits<int64_t>::min());
        }
    }

    void calc(int, Session& sess) override {
        auto& in  = sess.get( input()).data<InstanceBlock>();
        auto& out = sess.get(output()).data<InstanceBlock>();
        out.clear(); out.initialize();

        for (size_t i = 0; i < in.size(); ++i) {
            bool use = true;
            auto ins = in.get(i);
            for (size_t j = 0; j < lfea.size(); ++j) {
                auto& all = _table->pull(lfea[j]);
                auto  ptr = std::lower_bound(ins.feas, ins.feas + ins.fea_size, rfea[j]);
                if (ptr == ins.feas + ins.fea_size) {
                    use = false;
                    break;
                }
                auto it = std::lower_bound(all.begin(), all.end(), ptr->sign);
                if (it == all.end() || *it != ptr->sign) {
                    use = false;
                    break;
                }
            }
            if (use) gc_append(out, ins);
        }
        SCHECK(out.valid_check(out.size()));
    }

private:
    std::shared_ptr<QuantileIntTableEntity> _table;
    std::vector<int64_t> lfea;
    std::vector<feature_index_t> rfea;
};

var FilterBySign_build( var input, QuantileIntTable table,
                        std::vector<int64_t> lkey,
                        std::vector<int64_t> rkey) {
    auto schema = input.schema();
    SCHECK(schema.type == DType::GCFORMAT);

    auto nm = fmt("fltr_sign:%d", input);
    var out = Graph::ctx().new_variable(nm, Schema::gcformat(schema.x));
    declare_forward<FilterBySignNode>(table, lkey, rkey, input, out);
    return out;
}

} // namespace mle
} // namespace applications
} // namespace pico
} // namespace paradigm4

#endif
