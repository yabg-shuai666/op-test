#ifndef PARADIGM4_PICO_APPLICATIONS_FZ_COMMON_H
#define PARADIGM4_PICO_APPLICATIONS_FZ_COMMON_H

#include "pico-ds/pico-ds/parser/InstanceBlock.h"
#include "pico-ds/pico-ds/parser/InstanceBlockParser.h"

namespace paradigm4 {
namespace pico {
namespace applications {
namespace mle {

inline void offset_append(core::vector<size_t>& offset, size_t len) {
    if (offset.size() == 0) {
        offset.push_back(0);
        offset.push_back(len);
    } else {
        offset.push_back(offset.back() + len);
    }
}

template <typename T>
inline void values_append(core::vector<T>& vals, T* data, size_t len) {
    vals.insert(vals.end(), data, data + len);
}

void gc_append(ds::InstanceBlock& block, ds::InstancePtr& ins) {
    block.raw_instance.push_back(ins.raw_instance);
    block.instance_id .push_back(ins.instance_id);

    offset_append(block.label_offset, ins.label_size);
    values_append(block.labels,      ins.labels,      ins.label_size);
    values_append(block.importances, ins.importances, ins.label_size);

    offset_append(block.fea_offset, ins.fea_size);
    values_append(block.feas,          ins.feas,          ins.fea_size);
    values_append(block.values,        ins.values,        ins.fea_size);
    values_append(block.nominal_flags, ins.nominal_flags, ins.fea_size);
}

inline feature_index_t* find_slot(const ds::InstancePtr& ins, const feature_index_t& fea) {
    auto it = std::lower_bound(ins.feas, ins.feas + ins.fea_size, fea);
    if (it == ins.feas + ins.fea_size || it->slot != fea.slot)
        return nullptr;
    else
        return it;
}

inline feature_index_t* find_slot(
        const ds::InstancePtr& ins, int64_t slot,
        int64_t sign = std::numeric_limits<int64_t>::min()) {
    return find_slot(ins, feature_index_t(slot, sign));
}

template<typename T>
using pico_column_t = std::vector<std::pair<uint8_t, T>>;

struct window_t {
    bool use_time;
    int64_t delta;
    int64_t limit = std::numeric_limits<int64_t>::max();

    bool valid( const std::pair<uint8_t, int64_t>& t1,
                const std::pair<uint8_t, int64_t>& t2,
                size_t i, size_t k, size_t j) const {
        if (use_time)
            return t1.second - t2.second < delta && int64_t(k - j) < limit;
        else
            return int64_t(i - j) < delta;
    }

    std::string to_string() const {
        auto d = std::to_string(delta);
        if (use_time)
            d += 's';
        return d;
    }
};

inline window_t parse_window(std::string w) {
    window_t ret;
    auto pos = w.find(',');
    if (pos != std::string::npos) {
        ret.limit = pico_lexical_cast<int64_t>(w.substr(pos + 1));
        w = w.substr(0, pos);
    }
    if (w.size() > 0 && std::isdigit(w.back())) {
        ret.delta = pico_lexical_cast<int64_t>(w);
        ret.use_time = false;
    } else if (w.size() > 0) {
        ret.use_time = true;
        char unit    = w.back();
        w.pop_back();
        ret.delta    = pico_lexical_cast<int64_t>(w) * 1000;

        switch (unit) {
            case 's': ret.delta *= 1; break;
            case 'm': ret.delta *= 60; break;
            case 'h': ret.delta *= 60 * 60; break;
            case 'd': ret.delta *= 60 * 60 * 24; break;
            case 'w': ret.delta *= 60 * 60 * 24 * 7; break;
            default: SLOG(FATAL);
        }
    } else {
        SLOG(FATAL);
    }
    return ret;
}

inline std::pair<window_t, window_t> parse_window2(std::string w) {
    window_t low, high;
    auto pos = w.find(':');
    high = parse_window(w.substr(0, pos));

    if (pos == std::string::npos) {
        low.delta = 0;
        low.use_time = high.use_time;
    } else {
        low = parse_window(w.substr(pos + 1));
    }
    low.limit = high.limit;
    return {low, high};
}

std::string debug_print(const ds::InstancePtr& ins) {
    std::stringstream ss;
    ds::InstanceParser::dump_4paradigm_ins(ins, ss, false);
    return ss.str();
}

} // namespace mle
} // namespace applications
} // namespace pico
} // namespace paradigm4

#endif
