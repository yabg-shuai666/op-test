#ifndef PARADIGM4_PICO_APPLICATIONS_FZ_BLOCK_FILTER_H
#define PARADIGM4_PICO_APPLICATIONS_FZ_BLOCK_FILTER_H

#include "layer/layer.h"
#include "graph/node.h"
#include "executor/session.h"
#include "data/sparse_input.h"

#include "common.h"

namespace paradigm4 {
namespace pico {
namespace applications {
namespace mle {

std::string BlockFilter_doc = "";

class BlockFilterNode : public Node {
public:
    BlockFilterNode(std::vector<int64_t>&& slots, bool need_label, var in, var out)
        : Node(fmt("BlockFilterNode(%s,%s)", in.name(), out.name())) {
        inputs()  = { in };
        outputs() = { out };
        need.insert(slots.begin(), slots.end());
        label = need_label;
    }

    void calc(int, Session& sess) override {
        auto& in  = sess.get(input()).data<InstanceBlock>();
        auto& out = sess.get(output()).data<InstanceBlock>();

        out.clear();
        out.raw_instance.resize(in.size());
        out.instance_id .resize(in.size());
        if (label) {
            out.label_offset = in.label_offset;
            out.labels       = in.labels;
            out.importances  = in.importances;
        } else {
            out.label_offset.resize(in.size() + 1, 0);
        }

        out.fea_offset.push_back(0);
        for (size_t i = 0; i < in.size(); ++i) {
            int begin = in.fea_offset[i];
            int end   = in.fea_offset[i+1];
            for (int j = begin; j < end; ++j) if (need.count(in.feas[j].slot)) {
                out.feas         .push_back(in.feas[j]);
                out.values       .push_back(in.values[j]);
                out.nominal_flags.push_back(in.nominal_flags[j]);
            }
            out.fea_offset.push_back(out.feas.size());
        }
        SCHECK(out.valid_check(out.size()));
    }

private:
    std::unordered_set<int64_t> need;
    bool label;
};

var BlockFilter_build(var input, std::vector<int64_t> slots, bool label) {
    auto schema = input.schema();
    SCHECK(schema.type == DType::GCFORMAT);

    auto nm = fmt("gc_fltr:%d", input);
    var out = Graph::ctx().new_variable(nm, Schema::gcformat(slots.size() + label));
    declare_forward<BlockFilterNode>(std::move(slots), label, input, out);
    return out;
}

} // namespace mle
} // namespace applications
} // namespace pico
} // namespace paradigm4

#endif
