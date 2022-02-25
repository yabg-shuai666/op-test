#ifndef PARADIGM4_PICO_APPLICATIONS_FZ_GROUPBY_AGG_H
#define PARADIGM4_PICO_APPLICATIONS_FZ_GROUPBY_AGG_H

#include "common.h"
#include "fz_cache.h"

namespace paradigm4 {
namespace pico {
namespace applications {
namespace mle {

namespace GroupbyOpType {
    const int SUM = 0;
    const int AVG = 1;
    const int MAX = 2;
    const int MIN = 3;
    const int TOPK = 4;
    const int COUNT = 5;
    const int UNIQUE = 6;
}

class SumAgg : public BaseAgg {
public:
    SumAgg(int64_t s, int64_t to) : BaseAgg(s, to) {}

    void add(size_t, int64_t, double v, uint8_t n) override {
        if (!n) sum += v;
    }

    void del(size_t, int64_t, double v, uint8_t n) override {
        if (!n) sum -= v;
    }

    void append(InstanceBlock& out) override {
        out.feas.emplace_back(to, 0);
        out.values.push_back(sum);
        out.nominal_flags.push_back(0);
    }
private:
    double sum = 0;
};

struct count_map_t : public std::unordered_map<int64_t, size_t> {
    int64_t add(int64_t key) {
        auto it = find(key);
        if (it == end()) {
            insert({key, 1});
            return 1;
        } else {
            it->second += 1;
            return it->second;
        }
    }

    int64_t del(int64_t key) {
        auto it = find(key);
        if (it->second == 1) {
            erase(it);
            return 0;
        } else {
            it->second -= 1;
            return it->second;
        }
    }
};
class CountAgg : public BaseAgg {
public:
    CountAgg(int64_t s, int64_t to) : BaseAgg(s, to) {}

    void add(size_t i, int64_t s, double, uint8_t n) override {
        if (n) {
            cnt.add(s);
            idx[i] = s;
        }
    }

    void del(size_t i, int64_t s, double, uint8_t n) override {
        if (n) {
            cnt.del(s);
            idx.erase(i);
        }
    }

    void append(size_t i, InstanceBlock& out) override {
        if (idx.count(i)) {
            out.feas.emplace_back(to, 0);
            out.values.push_back(cnt[idx[i]]);
            out.nominal_flags.push_back(0);
        }
    }
private:
    count_map_t cnt;
    std::unordered_map<size_t, int64_t> idx;
};

class TopKAgg : public BaseAgg {
public:
    TopKAgg(int64_t s, int64_t to) : BaseAgg(s, to) {}

    void add(size_t, int64_t s, double, uint8_t n) override {
        if (n) {
            freq.add(s);
            ++ cnt;
        }
    }

    void del(size_t, int64_t s, double, uint8_t n) override {
        if (n) {
            freq.del(s);
            -- cnt;
        }
    }

    void append(InstanceBlock& out) override {
        if (freq.ans == -1) return;
        out.feas.emplace_back(to, 0);
        out.values.push_back(double(freq.ans) / cnt);
        out.nominal_flags.push_back(0);
    }

private:
    struct {
        count_map_t cnt;
        count_map_t vis;
        int64_t ans = 0;

        void add(int64_t key) {
            auto i = cnt.add(key);
            if (i > 1)
                vis.del(i - 1);
            vis.add(i);
            if (vis.count(ans + 1))
                ans += 1;
        }

        void del(int64_t key) {
            auto i = cnt.del(key);
            vis.del(i + 1);
            if (i > 0)
                vis.add(i);
            if (!vis.count(ans))
                ans -= 1;
        }
    } freq;
    size_t cnt = 0;
};

std::vector<std::shared_ptr<BaseAgg>> gen_groupby_aggs(const std::vector<AggConf>& conf) {
    std::vector<std::shared_ptr<BaseAgg>> ret;
    for (size_t i = 0; i < conf.size(); ++i) {
        #define CASE(TYPE, CLASS)                                      \
            case GroupbyOpType::TYPE: {                                \
                ret.push_back(std::make_shared<CLASS>(c.slot, i + 1)); \
                break;                                                 \
            }

        auto& c = conf[i];
        switch (c.type) {
            CASE(SUM   , SumAgg);
            CASE(AVG   , MeanAgg);
            CASE(MAX   , MaxAgg);
            CASE(MIN   , MinAgg);
            CASE(TOPK  , TopKAgg);
            CASE(COUNT , CountAgg);
            CASE(UNIQUE, NumUniqueAgg);
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
