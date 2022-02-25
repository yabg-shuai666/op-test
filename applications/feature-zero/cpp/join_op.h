#ifndef PARADIGM4_PICO_APPLICATIONS_FZ_JOIN_OP_H
#define PARADIGM4_PICO_APPLICATIONS_FZ_JOIN_OP_H

#include "common.h"
#include "fz_cache.h"
#include "join_agg.h"

namespace paradigm4 {
namespace pico {
namespace applications {
namespace mle {

int SEED = 67373;

int32_t hash_keys(const ds::InstancePtr& ins, const std::vector<int64_t>& slots) {
    thread_local static std::vector<int64_t> buffer;
    const int64_t missing = std::numeric_limits<int64_t>::min();
    buffer.clear();
    for (auto slot : slots) {
        auto it = find_slot(ins, slot, missing);
        buffer.push_back(it ? it->sign : missing);
    }
    int32_t key = 0;
    core::murmur_hash3_x86_32(buffer.data(), buffer.size() * sizeof(int64_t), SEED, &key);
    return key;
}

void select_fea(InstanceBlock& out,
                ds::InstancePtr ins,
                const std::unordered_set<int64_t>& keep,
                int offset) {
    for (size_t i = 0; i < ins.fea_size; ++i)
        if (!keep.count(ins.feas[i].slot)) {
            out.feas      .emplace_back(ins.feas[i].slot + offset, ins.feas[i].sign);
            out.values       .push_back(ins.values[i]);
            out.nominal_flags.push_back(ins.nominal_flags[i]);
        }
}

void _left_join(std::vector<InstanceBlock>& a,
                std::vector<InstanceBlock>& b,
                const std::vector<int64_t>& lslot,
                const std::vector<int64_t>& rslot,
                int offset, bool keep_left) {
    std::unordered_map<int32_t, size_t> idx;
    for (size_t i = 0, k = 0; i < b.size(); ++i)
        for (size_t j = 0; j < b[i].size(); ++j, ++k)
            idx[hash_keys(b[i].get(j), rslot)] = k;

    std::unordered_set<int64_t> lkey;
    if (!keep_left)
        lkey.insert(lslot.begin(), lslot.end());
    std::unordered_set<int64_t> rkey(rslot.begin(), rslot.end());

    for (size_t i = 0; i < a.size(); ++i) {
        InstanceBlock out;
        out.initialize();
        for (size_t j = 0; j < a[i].size(); ++j) {
            auto ins = a[i].get(j);
            auto key = hash_keys(ins, lslot);
            select_fea(out, ins, lkey, 0);
            if (idx.count(key)) {
                size_t k = idx[key];
                size_t x = k / FZCache::BATCH, y = k % FZCache::BATCH;
                ins = b[x].get(y);
                if (ins.label_size > 0) {
                    out.labels.push_back(ins.labels[0]);
                    out.importances.push_back(ins.importances[0]);
                }
                select_fea(out, ins, rkey, offset);
            }
            out.label_offset.push_back(out.labels.size());
            out.fea_offset.push_back(out.feas.size());
        }
        if (a[i].labels.size() == 0 && out.labels.size() != 0) {
            a[i].label_offset = std::move(out.label_offset);
            a[i].labels       = std::move(out.labels);
            a[i].importances  = std::move(out.importances);
        }
        a[i].fea_offset    = std::move(out.fea_offset);
        a[i].feas          = std::move(out.feas);
        a[i].values        = std::move(out.values);
        a[i].nominal_flags = std::move(out.nominal_flags);
        SCHECK(a[i].valid_check(a[i].size()));
    }
}

void left_join( FZCache a, FZCache b,
                std::vector<int64_t> lslot,
                std::vector<int64_t> rslot,
                int offset, bool keep_left) {
    SLOG(INFO) << "left_join";
    std::deque<std::thread> process;
    for (size_t i = 0; i < P_CONF.process.cpu_concurrency; ++i) {
        auto f = std::bind(_left_join,
            std::ref((*a.data)[i]),
            std::ref((*b.data)[i]),
            std::ref(lslot),
            std::ref(rslot),
            offset, keep_left);
        process.emplace_back(f);
    }
    for (auto& thd : process) thd.join();
    SLOG(INFO);
}

pico_column_t<int64_t> get_sign(std::vector<InstanceBlock>& data, int64_t slot) {
    pico_column_t<int64_t> ret;
    for (size_t i = 0; i < data.size(); ++i) {
        auto& block = data[i];
        for (size_t j = 0; j < block.size(); ++j) {
            auto it = find_slot(block.get(j), slot);
            if (it == nullptr)
                ret.emplace_back(1, 0);
            else
                ret.emplace_back(0, it->sign);
        }
    }
    return ret;
}

inline bool time_cmp(const pico_column_t<int64_t>& time, size_t a, size_t b) {
    return !time[a].first && (time[b].first || time[a].second < time[b].second);
}

std::unordered_map<int64_t, std::vector<size_t>> get_relation(
        std::vector<InstanceBlock>& data,
        const std::vector<int64_t>& slots,
        const pico_column_t<int64_t>& time) {
    std::unordered_map<int64_t, std::vector<size_t>> ret;
    for (size_t i = 0, k = 0; i < data.size(); ++i) {
        auto& block = data[i];
        for (size_t j = 0; j < block.size(); ++j, ++k)
            ret[hash_keys(block.get(j), slots)].push_back(k);
    }

    auto cmp = std::bind(time_cmp, std::ref(time), std::placeholders::_1, std::placeholders::_2);
    for (auto& p : ret)
        std::sort(p.second.begin(), p.second.end(), cmp);
    return ret;
}

struct win_index_t {
    size_t i = 0, l = 0, r = 0;
};

void _join_op(  int tid,
                std::vector<InstanceBlock>& a,
                std::vector<InstanceBlock>& b,
                PipingProcess<Variable, void>* ret,
                const std::vector<int64_t>& lslot,
                const std::vector<int64_t>& rslot,
                int64_t ltime, int64_t rtime,
                const std::pair<window_t, window_t>& win,
                const std::vector<AggConf>& conf) {
    auto atime = get_sign(a, ltime);
    auto btime = get_sign(b, rtime);
    auto relation = get_relation(b, rslot, btime);
    std::vector<size_t> idx(atime.size());
    for (size_t i = 0; i < idx.size(); ++i)
        idx[i] = i;
    auto cmp = std::bind(time_cmp, std::ref(atime), std::placeholders::_1, std::placeholders::_2);
    std::sort(idx.begin(), idx.end(), cmp);

    std::unordered_map<int64_t, std::vector<std::shared_ptr<BaseAgg>>> all_agg;
    std::unordered_set<int64_t> lkey(lslot.begin(), lslot.end());
    std::unordered_map<int64_t, win_index_t> win_idx;
    lkey.insert(ltime);

    for (size_t i = 0; i < idx.size();) {
        Variable var;
        auto& out = var.data<InstanceBlock>();
        out.initialize();
        for (size_t j = 0; j < FZCache::BATCH && i < idx.size(); ++i, ++j) {
            size_t k = idx[i];
            size_t x = k / FZCache::BATCH;
            size_t y = k % FZCache::BATCH;
            auto ins = a[x].get(y);

            out.raw_instance.push_back(ins.raw_instance);
            out.instance_id .push_back(ins.instance_id);
            if (ins.label_size > 0) {
                out.labels.push_back(ins.labels[0]);
                out.importances.push_back(ins.importances[0]);
            }
            out.label_offset.push_back(out.labels.size());
            select_fea(out, ins, lkey, 0);

            auto hash = hash_keys(ins, lslot);
            auto& now = atime[k];
            auto& idx = win_idx[hash];
            auto& rhs = relation[hash];
            if (!all_agg.count(hash)) all_agg[hash] = gen_aggs(conf);
            auto& agg = all_agg[hash];
            while (idx.i + 1 < rhs.size() && btime[rhs[idx.i + 1]] <= now) ++idx.i;
            while (idx.r < rhs.size() && !win.first.valid(now, btime[rhs[idx.r]], idx.i, idx.r, idx.r)) {
                size_t t = rhs[idx.r];
                ins = b[t / FZCache::BATCH].get(t % FZCache::BATCH);
                for (size_t u = 0, v = 0; u < agg.size(); ++u) {
                    while (v < ins.fea_size && agg[u]->slot > ins.feas[v].slot) ++v;
                    if (v == ins.fea_size || ins.feas[v].slot != agg[u]->slot) {
                        agg[u]->add_missing(idx.r);
                    } else {
                        for (size_t w = v; w < ins.fea_size && ins.feas[w].slot == agg[u]->slot; ++w)
                            agg[u]->add(idx.r, ins.feas[w].sign, ins.values[w], ins.nominal_flags[w]);
                    }
                }
                ++ idx.r;
            }
            while (idx.l < rhs.size() && !win.second.valid(now, btime[rhs[idx.l]], idx.i, idx.r, idx.l)) {
                size_t t = rhs[idx.l];
                ins = b[t / FZCache::BATCH].get(t % FZCache::BATCH);
                for (size_t u = 0, v = 0; u < agg.size(); ++u) {
                    while (v < ins.fea_size && agg[u]->slot > ins.feas[v].slot) ++v;
                    if (v == ins.fea_size || ins.feas[v].slot != agg[u]->slot) {
                        agg[u]->del_missing(idx.l);
                    } else {
                        for (size_t w = v; w < ins.fea_size && ins.feas[w].slot == agg[u]->slot; ++w)
                            agg[u]->del(idx.l, ins.feas[w].sign, ins.values[w], ins.nominal_flags[w]);
                    }
                }
                ++ idx.l;
            }
            for (auto& op : agg) op->append(out);
            out.fea_offset.push_back(out.feas.size());
        }
        ret->run(tid, var);
    }
}

void table_join(FZCache a, FZCache b, Data out,
                std::vector<int64_t> lslot,
                std::vector<int64_t> rslot,
                int64_t ltime, int64_t rtime,
                std::string win_str,
                std::vector<AggConf> conf) {
    SLOG(INFO) << conf.size();

    start_declaration();
    var hold = Graph::ctx().new_variable("placeholder", Schema::gcformat(lslot.size() + conf.size() + 1));
    out.entity()->id() = hold;
    out.entity()->initialize_sink("");
    SequentialEntity exe(finish_declaration());

    exe.graph_initialize(P_CONF.process.cpu_concurrency, true);

    auto ptr = dynamic_cast<PipingProcess<Variable, void>*>(out.entity()->get_process().get());
    SCHECK(ptr != nullptr);

    auto win = parse_window2(win_str);
    std::deque<std::thread> process;
    for (size_t i = 0; i < P_CONF.process.cpu_concurrency; ++i) {
        auto f = std::bind(_join_op, i,
            std::ref((*a.data)[i]),
            std::ref((*b.data)[i]),
            ptr,
            std::ref(lslot),
            std::ref(rslot),
            ltime, rtime,
            std::ref(win),
            std::ref(conf));
        process.emplace_back(f);
    }
    for (auto& thd : process) thd.join();
    exe.join_process(ProcessType::CPU);
    exe.join_process(ProcessType::IO);
    exe.graph_finalize();
    SLOG(INFO);
}

} // namespace mle
} // namespace applications
} // namespace pico
} // namespace paradigm4

#endif
