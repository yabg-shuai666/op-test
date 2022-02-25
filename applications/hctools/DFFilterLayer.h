#ifndef PARADIGM4_PICO_APPLICATIONS_DF_FILTER_H
#define PARADIGM4_PICO_APPLICATIONS_DF_FILTER_H

#include "layer/layer.h"
#include "graph/node.h"
#include "executor/session.h"

namespace paradigm4 {
namespace pico {
namespace applications {
namespace mle {

std::string DFFilter_doc = "";
std::string DFSplit_doc = "";

class DFFilterNode : public Node {
public:
    DFFilterNode(int64_t cond, double ratio, var df, var gc, var out1, var out2)
        :   Node(fmt("DFFilterNode(%lld, %f, %s, %s)", cond, ratio, df.name(), gc.name())),
            cond(cond), ratio(ratio) {
        inputs()  = { df, gc };
        outputs() = { out1, out2 };
    }

    void calc(int, Session& sess) override {
        auto& df = sess.get( input(0)).data<DataFrame>();
        auto& gc = sess.get( input(1)).data<InstanceBlock>();
        auto& o1 = sess.get(output(0)).data<DataFrame>();
        auto& o2 = sess.get(output(1)).data<DataFrame>();

        o1.clear(); o2.clear();
        for (size_t i = 0; i < df.num_rows(); ++i) {
            SCHECK(gc.fea_offset[i + 1] - gc.fea_offset[i] == 1);
            auto sign = gc.feas[i].sign;
            auto part = df.slice(i, 1);
            if (sign < cond)
                o1.merge(part);
            else if (sign > cond)
                o2.merge(part);
            else if (random() < ratio)
                o1.merge(part);
            else
                o2.merge(part);
        }
    }

private:
    int64_t cond;
    double ratio;
};

std::pair<var, var> DFFilter_build(var df, var gc, int64_t cond, double ratio) {
    SCHECK(df.schema().type == DType::PARQUET);
    SCHECK(gc.schema().type == DType::GCFORMAT);

    auto name = fmt("df_filter:%d,%s", df, gc);
    auto out1 = Graph::ctx().new_variable(name, Schema::parquet(df.schema().x));
    auto out2 = Graph::ctx().new_variable(name, Schema::parquet(df.schema().x));
    declare_forward<DFFilterNode>(cond, ratio, df, gc, out1, out2);
    return std::make_pair(out1, out2);
}

class DFSplitNode : public Node {
public:
    DFSplitNode(double ratio, var df, var out1, var out2)
        :   Node(fmt("DFSplitNode(%f, %s)", ratio, df.name())), ratio(ratio) {
        inputs()  = { df };
        outputs() = { out1, out2 };
    }

    void calc(int, Session& sess) override {
        auto& df = sess.get( input(0)).data<DataFrame>();
        auto& o1 = sess.get(output(0)).data<DataFrame>();
        auto& o2 = sess.get(output(1)).data<DataFrame>();

        o1.clear(); o2.clear();
        for (size_t i = 0; i < df.num_rows(); ++i) {
            auto part = df.slice(i, 1);
            if (random() < ratio)
                o1.merge(part);
            else
                o2.merge(part);
        }
    }

private:
    double ratio;
};

std::pair<var, var> DFSplit_build(var df, double ratio) {
    SCHECK(df.schema().type == DType::PARQUET);

    auto name = fmt("df_filter:%d", df);
    auto out1 = Graph::ctx().new_variable(name, Schema::parquet(df.schema().x));
    auto out2 = Graph::ctx().new_variable(name, Schema::parquet(df.schema().x));
    declare_forward<DFSplitNode>(ratio, df, out1, out2);
    return std::make_pair(out1, out2);
}

} // namespace mle
} // namespace applications
} // namespace pico
} // namespace paradigm4

#endif
