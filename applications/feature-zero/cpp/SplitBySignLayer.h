#ifndef PARADIGM4_PICO_APPLICATIONS_FZ_SPLITBYSIGN_H
#define PARADIGM4_PICO_APPLICATIONS_FZ_SPLITBYSIGN_H

#include "layer/layer.h"
#include "graph/node.h"
#include "executor/session.h"
#include "data/sparse_input.h"

#include "common.h"

namespace paradigm4 {
namespace pico {
namespace applications {
namespace mle {

std::string SplitBySign_doc = "";

class SplitBySignNode : public Node {
public:
    SplitBySignNode(const std::string& front,
                    const std::string& end,
                    const feature_index_t& fea,
                    double ratio,
                    var in, var out1, var out2, var out)
        : Node(fmt("SplitBySignNode(%lld:%lld,%.1f,%s,%s)", fea.slot, fea.sign, ratio, in.name(), out.name())) {
        inputs()  = { in };
        outputs() = { out1, out2, out };
        _front = front;
        _end   = end;
        _fea   = fea;
        _ratio = ratio;
    }

    void calc(int, Session& sess) override {
        auto& in = sess.get( input( )).data<InstanceBlock>();
        auto& o1 = sess.get(output(0)).data<InstanceBlock>();
        auto& o2 = sess.get(output(1)).data<InstanceBlock>();
        auto out = std::make_shared<StructVariable>();

        o1.clear(); o1.initialize();
        o2.clear(); o2.initialize();
        for (size_t i = 0; i < in.size(); ++i) {
            auto ins = in.get(i);
            auto tar = std::lower_bound(ins.feas, ins.feas + ins.fea_size, _fea,
                [](const feature_index_t& a, const feature_index_t& b) {
                    return a.slot < b.slot;
                });
            SCHECK(tar != ins.feas + ins.fea_size && tar->slot == _fea.slot);
            SCHECK(ins.feas[0].slot == -1);
            if (tar->sign <= _fea.sign && random(ins.feas[0].sign) < _ratio)
                gc_append(o1, ins);
            else
                gc_append(o2, ins);
        }
        SCHECK(o1.valid_check(o1.size()));
        SCHECK(o2.valid_check(o2.size()));

        out->data()[_front] = sess.get(output(0)).entity();
        out->data()[_end  ] = sess.get(output(1)).entity();
        sess.get(output(2)).entity() = out;
    }

private:
    std::string _front, _end;
    feature_index_t _fea;
    double _ratio;
};

var SplitBySign_build(var input, int64_t slot, int64_t sign, double ratio, std::string front, std::string end) {
    auto& schema = input.schema();
    SCHECK(schema.type == DType::GCFORMAT);

    auto nm1 = fmt("fz_sp_1:%d", input);
    auto nm2 = fmt("fz_sp_2:%d", input);
    auto tmp = Schema::gcformat(schema.x);
    var out1 = Graph::ctx().new_variable(nm1, tmp);
    var out2 = Graph::ctx().new_variable(nm2, tmp);

    Schema::sub_schema_t sub = {{front, tmp}, {end, tmp}};
    auto sh = std::make_shared<Schema>(DType::STRUCT, 0, 0, std::move(sub));
    auto nm = fmt("fz_sp:%d", input);
    var out = Graph::ctx().new_variable(nm, sh);
    declare_forward<SplitBySignNode>(front, end, feature_index_t(slot, sign), ratio, input, out1, out2, out);
    return out;
}

} // namespace mle
} // namespace applications
} // namespace pico
} // namespace paradigm4

#endif
