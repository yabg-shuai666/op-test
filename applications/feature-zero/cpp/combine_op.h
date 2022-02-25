#ifndef PARADIGM4_PICO_APPLICATIONS_FZ_COMBINE_OP_H
#define PARADIGM4_PICO_APPLICATIONS_FZ_COMBINE_OP_H

#include "common.h"
#include "fz_cache.h"

namespace paradigm4 {
namespace pico {
namespace applications {
namespace mle {

struct CombConf {
    int type;
    std::vector<int64_t> slots;
};

class CombineOp : public core::VirtualObject {
public:
    int64_t slot;
    std::vector<int64_t> slots;

    virtual void init() {}
    virtual void add_missing(int) {}
    virtual void add(int, int64_t, double, uint8_t) {}
    virtual void append(InstanceBlock&) {}
};

class ValOp : public CombineOp {
public:
    void init() override { mis = 0; val = 0; }
    void append(InstanceBlock& block) override {
        if (mis != (1 << slots.size()) - 1) return;
        block.feas.emplace_back(slot, 0);
        block.values.push_back(val);
        block.nominal_flags.push_back(0);
    }
    double val;
    int mis;
};

class LogOp : public ValOp {
public:
    void add(int k, int64_t, double v, uint8_t n) override {
        if (!n && v > 0) {
            mis |= 1 << k;
            val = std::log(v);
        }
    }
};

class AddOp : public ValOp {
public:
    void add(int k, int64_t, double v, uint8_t n) override {
        if (!n) {
            mis |= 1 << k;
            val += v;
        }
    }
};

class MulOp : public ValOp {
public:
    void init() override { mis = 0; val = 1; }
    void add(int k, int64_t, double v, uint8_t n) override {
        if (!n) {
            mis |= 1 << k;
            val *= v;
        }
    }
};

class MinusOp : public ValOp {
public:
    void add(int k, int64_t, double v, uint8_t n) override {
        if (!n) {
            mis |= 1 << k;
            val += k == 0 ? v : -v;
        }
    }
};

class DivOp : public CombineOp {
public:
    void init() override { mis = 0; }
    void add(int k, int64_t, double v, uint8_t n) override {
        if (!n) {
            mis |= 1 << k;
            if (k == 0) val1 = v;
            else val2 = v;
        }
    }
    void append(InstanceBlock& block) override {
        if (mis != (1 << slots.size()) - 1 || std::isnan(val1 / val2)) return;
        block.feas.emplace_back(slot, 0);
        block.values.push_back(val1 / val2);
        block.nominal_flags.push_back(0);
    }
    int mis = 0;
    double val1, val2;
};

class CombOp : public CombineOp {
public:
    void init() override {
        signs.resize(slots.size());
        for (auto& vec : signs)
            vec.clear();
    }
    void add(int k, int64_t s, double, uint8_t n) override {
        if (n) signs[k].push_back(s);
    }
    void append(InstanceBlock& block) override {
        char tmp[50];
        if (signs.size() == 3) {
            if (signs[0].size() == 0 || signs[1].size() == 0 || signs[2].size() == 0)
                return;
            for (auto v1 : signs[0])
                for (auto v2 : signs[1])
                    for (auto v3 : signs[2]) {
                        sprintf(tmp, "%lX,%lX,%lX", v1, v2, v3);
                        _append(tmp, block);
                    }
        } else if (signs.size() == 2) {
            if (signs[0].size() == 0 && signs[1].size() == 0)
                return;
            if (signs[0].size() == 0) {
                for (auto v : signs[1]) {
                    sprintf(tmp, "%lX,", v);
                    _append(tmp, block);
                }
            } else if (signs[1].size() == 0) {
                for (auto v : signs[0]) {
                    sprintf(tmp, "%lX,", v);
                    _append(tmp, block);
                }
            } else {
                for (auto v1 : signs[0])
                    for (auto v2 : signs[1]) {
                        sprintf(tmp, "%lX,%lX", v1, v2);
                        _append(tmp, block);
                    }
            }
        }
    }
    void _append(char* val, InstanceBlock& block) {
        block.feas.emplace_back(slot, std::hash<std::string>{}(val));
        block.values.push_back(1);
        block.nominal_flags.push_back(1);
    }
    std::vector<std::vector<int64_t>> signs;
};

class TimeOp : public CombineOp {
public:
    void init() override { mis = true; }
    void add(int, int64_t t, double, uint8_t n) override {
        if (n) {
            mis = false;
            gmtime_r(&t, &val);
        }
    }
    void append(InstanceBlock& block) override {
        if (mis) return;
        block.feas.emplace_back(slot, 0);
        block.values.push_back(get_sign());
        block.nominal_flags.push_back(0);
    }
    virtual int64_t get_sign() = 0;
    bool mis = true;
    std::tm val;
};

class IsWeekDayOp : public TimeOp {
public:
    int64_t get_sign() override {
        return 1 <= val.tm_wday && val.tm_wday <= 5;
    }
};

class DayOfWeekOp : public TimeOp {
public:
    int64_t get_sign() override {
        return val.tm_wday;
    }
};

class HourOfDayOp : public TimeOp {
public:
    int64_t get_sign() override {
        return val.tm_hour;
    }
};

class IsInOp : public CombineOp {
public:
    void init() override { a.clear(); b.clear(); }
    void add(int k, int64_t s, double, uint8_t n) override {
        if (!n) return;
        if (!k) a.push_back(s);
        else b.insert(s);
    }
    void append(InstanceBlock& block) override {
        if (a.size() == 0) return;
        block.feas.emplace_back(slot, b.count(a[0]));
        block.values.push_back(1);
        block.nominal_flags.push_back(1);
    }

    std::vector<int64_t> a;
    std::unordered_set<int64_t> b;
};

namespace CombOpType {
    const int LOG   = 0;
    const int ADD   = 1;
    const int MINUS = 2;
    const int MUL   = 3;
    const int DIV   = 4;
    const int COMB  = 5;
    const int ISWD  = 6;
    const int WDAY  = 7;
    const int HOUR  = 8;
    const int ISIN  = 9;
}

class CombineNode : public Node {
public:
    CombineNode(int d, const std::vector<CombConf>& conf, var in, var out)
        : Node(fmt("CombineNode(%s, %s)", in.name(), out.name())), conf(conf) {
        inputs()  = { in };
        outputs() = { out };
        dim = d;
    }

    void initialize(int tnum) override {
        #define CASE(TYPE, CLASS)                    \
            case CombOpType::TYPE: {                 \
                auto op = std::make_shared<CLASS>(); \
                op->slots = c.slots;                 \
                op->slot  = i;                       \
                ops.push_back(op);                   \
                break;                               \
            }

        _ops.resize(tnum);
        _sorted_ops.resize(tnum);

        for (int tid = 0; tid < tnum; ++tid) {
            auto& ops = _ops[tid];
            auto& sorted_ops = _sorted_ops[tid];

            for (size_t i = 0; i < conf.size(); ++i) {
                auto& c = conf[i];
                switch (c.type) {
                    CASE(LOG  , LogOp);
                    CASE(ADD  , AddOp);
                    CASE(MINUS, MinusOp);
                    CASE(MUL  , MulOp);
                    CASE(DIV  , DivOp);
                    CASE(COMB , CombOp);
                    CASE(ISWD , IsWeekDayOp);
                    CASE(WDAY , DayOfWeekOp);
                    CASE(HOUR , HourOfDayOp);
                    CASE(ISIN , IsInOp);
                    default: SLOG(FATAL) << c.type;
                }
            }
            #undef CASE

            sorted_ops.resize(dim);
            for (int k = 0; k < dim; ++k) {
                sorted_ops[k] = ops;
                std::sort(sorted_ops[k].begin(), sorted_ops[k].end(),
                    [k](const std::shared_ptr<CombineOp>& a, const std::shared_ptr<CombineOp>& b) {
                        return a->slots[k] < b->slots[k];
                    });
            }
        }
    }

    void calc(int tid, Session& sess) override {
        auto& block = sess.get(input()) .data<InstanceBlock>();
        auto& out   = sess.get(output()).data<InstanceBlock>();
        out.clear();
        out.initialize();
        for (size_t i = 0; i < block.size(); ++i) {
            auto ins = block.get(i);
            SCHECK(ins.fea_size > 0 && ins.feas[0].slot == -1);
            out.append(-1, ins.feas[0].sign);

            for (auto& op : _ops[tid]) op->init();
            for (int k = 0; k < dim; ++k) {
                auto& op = _sorted_ops[tid][k];
                for (size_t u = 0, v = 0; u < op.size(); ++u) {
                    auto slot = op[u]->slots[k];
                    while (v < ins.fea_size && slot > ins.feas[v].slot) ++v;
                    if (v == ins.fea_size || ins.feas[v].slot != slot) {
                        op[u]->add_missing(k);
                    } else {
                        for (size_t w = v; w < ins.fea_size && ins.feas[w].slot == slot; ++w)
                            op[u]->add(k, ins.feas[w].sign, ins.values[w], ins.nominal_flags[w]);
                    }
                }
            }
            for (auto& op : _ops[tid]) op->append(out);
            out.fea_offset.push_back(out.feas.size());
        }
        out.raw_instance = block.raw_instance;
        out.instance_id  = block.instance_id;
        out.label_offset = block.label_offset;
        out.labels       = block.labels;
        out.importances  = block.importances;
    }

private:
    const std::vector<CombConf> conf;
    int dim;
    std::vector<std::vector<std::shared_ptr<CombineOp>>> _ops;
    std::vector<std::vector<std::vector<std::shared_ptr<CombineOp>>>> _sorted_ops;
};

std::string Combine_doc = "";

var Combine_build(var input, std::vector<CombConf> conf) {
    SCHECK(input.schema().type == DType::GCFORMAT);
    SCHECK(conf.size() > 0);

    auto nm = fmt("cmb_%d", input);
    var out = Graph::ctx().new_variable(nm, Schema::gcformat(conf.size()));
    auto d  = conf[0].slots.size();
    for (auto c : conf) SCHECK(d == c.slots.size());
    declare_forward<CombineNode>(d, conf, input, out);
    return out;
}

} // namespace mle
} // namespace applications
} // namespace pico
} // namespace paradigm4

#endif
