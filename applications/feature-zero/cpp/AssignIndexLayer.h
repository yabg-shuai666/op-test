#ifndef PARADIGM4_PICO_APPLICATIONS_FZ_ASSIGN_INDEX_H
#define PARADIGM4_PICO_APPLICATIONS_FZ_ASSIGN_INDEX_H

#include "layer/layer.h"
#include "graph/node.h"
#include "executor/session.h"
#include "data/sparse_input.h"

#include "common.h"

namespace paradigm4 {
namespace pico {
namespace applications {
namespace mle {

std::string AssignIndex_doc = "";

class AssignIndexNode : public Node {
public:
    AssignIndexNode(int64_t slot, var in, var out)
        : Node(fmt("AssignIndexNode(%s,%s,%lld)", in.name(), out.name(), slot)) {
        inputs()  = { in };
        outputs() = { out };
        _slot = slot;
    }

    void calc(int, Session& sess) override {
        auto& in  = sess.get( input()).data<InstanceBlock>();
        auto& out = sess.get(output()).data<InstanceBlock>();

        out.clear();
        out.raw_instance = in.raw_instance;
        out.instance_id  = in.instance_id;
        out.label_offset = in.label_offset;
        out.labels       = in.labels;
        out.importances  = in.importances;

        out.fea_offset.push_back(0);
        for (size_t i = 0; i < in.size(); ++i) {
            auto ins = in.get(i);
            out.feas      .emplace_back(_slot, gen_sign(ins));
            out.values       .push_back(0);
            out.nominal_flags.push_back(1);

            for (size_t j = 0; j < ins.fea_size; ++j) {
                out.feas         .push_back(ins.feas[j]);
                out.values       .push_back(ins.values[j]);
                out.nominal_flags.push_back(ins.nominal_flags[j]);
            }
            out.fea_offset.push_back(out.feas.size());
        }
        SCHECK(out.valid_check(out.size()));
    }

private:
    int64_t gen_sign(const ds::InstancePtr& ins) {
        int64_t s1 = 0, s2 = 0;
        murmur_hash3_x86_32(ins.feas, ins.fea_size * sizeof(feature_index_t), MURMURHASH_SEED, &s1);
        murmur_hash3_x86_32(ins.values, ins.fea_size * sizeof(double), MURMURHASH_SEED, &s2);
        return (s1 << 32) | s2;
    }

    int64_t _slot;
};

var AssignIndex_build(var input, int64_t slot) {
    auto schema = input.schema();
    SCHECK(schema.type == DType::GCFORMAT);

    auto nm = fmt("asn_idx:%d", input);
    var out = Graph::ctx().new_variable(nm, Schema::gcformat(schema.x + 1));
    declare_forward<AssignIndexNode>(slot, input, out);
    return out;
}

} // namespace mle
} // namespace applications
} // namespace pico
} // namespace paradigm4

#endif
