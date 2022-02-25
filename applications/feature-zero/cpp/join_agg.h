#ifndef PARADIGM4_PICO_APPLICATIONS_FZ_JOIN_AGG_H
#define PARADIGM4_PICO_APPLICATIONS_FZ_JOIN_AGG_H

#include "common.h"
#include "fz_cache.h"

namespace paradigm4 {
namespace pico {
namespace applications {
namespace mle {

enum MultiOpType : uint8_t {
    LAST_VALUE = 0,
    MEAN       = 1,
    MAX        = 2,
    MIN        = 3,
    NUM_UNIQUE = 4,
    TOP3_FREQ  = 5
};

struct AggConf {
    int type;
    int64_t slot;
    AggConf(int t, int64_t s) : type(t), slot(s) {}
};

class BaseAgg : public core::VirtualObject {
public:
    BaseAgg(int64_t s, int64_t to) : slot(s), to(to) {}
    int64_t slot, to;

    virtual void add_missing(size_t) {}
    virtual void del_missing(size_t) {}
    virtual void add(size_t, int64_t, double, uint8_t) {}
    virtual void del(size_t, int64_t, double, uint8_t) {}
    virtual void append(InstanceBlock&) {}
    virtual void append(size_t, InstanceBlock& block) { append(block); }
};

class LastValueAgg : public BaseAgg {
public:
    LastValueAgg(int64_t s, int64_t to) : BaseAgg(s, to) {
        missing = true;
    }

    void add(size_t i, int64_t s, double v, uint8_t n) override {
        missing = false;
        index = i;
        sign  = s;
        value = v;
        flag  = n;
    }

    void del(size_t i, int64_t, double, uint8_t) override {
        if (i == index) missing = true;
    }

    void append(InstanceBlock& out) override {
        if (missing) return;
        out.feas.emplace_back(to, sign);
        out.values.push_back(value);
        out.nominal_flags.push_back(flag);
    }
private:
    bool missing;
    size_t index;
    int64_t sign;
    double value;
    uint8_t flag;
};

class MeanAgg : public BaseAgg {
public:
    MeanAgg(int64_t s, int64_t to) : BaseAgg(s, to) {
        sum = 0;
        cnt = 0;
    }

    void add(size_t, int64_t, double v, uint8_t n) override {
        if (!n) {
            sum += v;
            cnt += 1;
        }
    }

    void del(size_t, int64_t, double v, uint8_t n) override {
        if (!n) {
            sum -= v;
            cnt -= 1;
        }
    }

    void append(InstanceBlock& out) override {
        if (cnt == 0) return;
        out.feas.emplace_back(to, 0);
        out.values.push_back(sum / cnt);
        out.nominal_flags.push_back(0);
    }

private:
    double sum;
    size_t cnt;
};

class MaxAgg : public BaseAgg {
public:
    MaxAgg(int64_t s, int64_t to) : BaseAgg(s, to) {}

    void add(size_t i, int64_t, double v, uint8_t n) override {
        if (n) return;
        while (que.size() > 0 && que.back().second <= v)
            que.pop_back();
        if (que.size() == 0 || que.back().first < i)
            que.push_back({i, v});
    }

    void del(size_t i, int64_t, double, uint8_t) override {
        while (que.size() > 0 && i >= que.front().first)
            que.pop_front();
    }

    void append(InstanceBlock& out) override {
        if (que.size() == 0) return;
        out.feas.emplace_back(to, 0);
        out.values.push_back(que.front().second);
        out.nominal_flags.push_back(0);
    }

private:
    std::deque<std::pair<size_t, double>> que;
};

class MinAgg : public BaseAgg {
public:
    MinAgg(int64_t s, int64_t to) : BaseAgg(s, to) {}

    void add(size_t i, int64_t, double v, uint8_t n) override {
        if (n) return;
        while (que.size() > 0 && que.back().second >= v)
            que.pop_back();
        if (que.size() == 0 || que.back().first < i)
            que.push_back({i, v});
    }

    void del(size_t i, int64_t, double, uint8_t) override {
        while (que.size() > 0 && i >= que.front().first)
            que.pop_front();
    }

    void append(InstanceBlock& out) override {
        if (que.size() == 0) return;
        out.feas.emplace_back(to, 0);
        out.values.push_back(que.front().second);
        out.nominal_flags.push_back(0);
    }

private:
    std::deque<std::pair<size_t, double>> que;
};

class NumUniqueAgg : public BaseAgg {
public:
    NumUniqueAgg(int64_t s, int64_t to) : BaseAgg(s, to) {}

    void add(size_t, int64_t s, double, uint8_t n) override {
        if (n) cnt[s] += 1;
    }

    void del(size_t, int64_t s, double, uint8_t n) override {
        if (n) {
            auto it = cnt.find(s);
            if (it->second == 1)
                cnt.erase(it);
            else
                it->second -= 1;
        }
    }

    void append(InstanceBlock& out) override {
        out.feas.emplace_back(to, 0);
        out.values.push_back(cnt.size());
        out.nominal_flags.push_back(0);
    }

private:
    std::unordered_map<int64_t, size_t> cnt;
};

class Top3FreqAgg : public BaseAgg {
public:
    Top3FreqAgg(int64_t s, int64_t to) : BaseAgg(s, to) {}

    void add(size_t, int64_t s, double, uint8_t n) override {
        if (!n) return;
        auto it = cnt.find(s);
        if (it == cnt.end()) {
            cnt[s] = 1;
            top.insert({1, s});
        } else {
            top.erase({it->second, s});
            top.insert({++it->second, s});
        }
    }

    void del(size_t, int64_t s, double, uint8_t n) override {
        if (!n) return;
        auto it = cnt.find(s);
        if (it->second == 1) {
            cnt.erase(it);
            top.erase({1, s});
        } else {
            top.erase({it->second, s});
            top.insert({--it->second, s});
        }
    }

    void append(InstanceBlock& out) override {
        if (top.size() == 0) return;
        size_t i = 0;
        for (auto it = top.rbegin(); i < 3 && it != top.rend(); ++it, ++i) {
            out.feas.emplace_back(to, it->second);
            out.values.push_back(1);
            out.nominal_flags.push_back(1);
        }
    }

private:
    std::unordered_map<int64_t, size_t> cnt;
    std::set<std::pair<int, int64_t>> top;
};

std::vector<std::shared_ptr<BaseAgg>> gen_aggs(const std::vector<AggConf>& conf) {
    std::vector<std::shared_ptr<BaseAgg>> ret;
    for (size_t i = 0; i < conf.size(); ++i) {
        #define CASE(TYPE, CLASS)                                  \
            case MultiOpType::TYPE: {                              \
                ret.push_back(std::make_shared<CLASS>(c.slot, i)); \
                break;                                             \
            }

        auto& c = conf[i];
        switch (c.type) {
            CASE(LAST_VALUE, LastValueAgg);
            CASE(MEAN      , MeanAgg);
            CASE(MAX       , MaxAgg);
            CASE(MIN       , MinAgg);
            CASE(NUM_UNIQUE, NumUniqueAgg);
            CASE(TOP3_FREQ , Top3FreqAgg);
            default: SLOG(FATAL) << c.type;
        }
        #undef CASE
    }
    return ret;
}

} // namespace mle
} // namespace applications
} // namespace pico
} // namespace paradigm4

#endif
