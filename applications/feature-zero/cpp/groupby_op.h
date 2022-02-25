#ifndef PARADIGM4_PICO_APPLICATIONS_FZ_GROUPBY_OP_H
#define PARADIGM4_PICO_APPLICATIONS_FZ_GROUPBY_OP_H

#include "ds/output/CacheDataSink.h"
#include "common.h"
#include "fz_cache.h"
#include "join_op.h"
#include "groupby_agg.h"

namespace paradigm4 {
namespace pico {
namespace applications {
namespace mle {

void _apply_ops(int tid,
                std::vector<InstanceBlock>& data,
                PipingProcess<Variable, void>* ret,
                const std::vector<int64_t>& part_keys,
                int64_t index_col, int64_t time_col,
                const std::vector<AggConf>& conf,
                const std::pair<window_t, window_t>& win) {
    auto time = get_sign(data, time_col);
    std::vector<size_t> idx(time.size());
    std::vector<int64_t> hash(time.size());

    for (size_t i = 0; i < idx.size(); ++i) {
        auto ins = data[i / FZCache::BATCH].get(i % FZCache::BATCH);
        hash[i] = hash_keys(ins, part_keys);
        idx[i] = i;
    }
    auto cmp = [&](size_t a, size_t b) {
        return hash[a] < hash[b] || (hash[a] == hash[b] && time_cmp(time, a, b));
    };
    std::sort(idx.begin(), idx.end(), cmp);

    std::vector<std::shared_ptr<BaseAgg>> agg;
    for (size_t i = 0, l = 0, r = 0, limit = 0; i < idx.size();) {
        Variable var;
        auto& out = var.data<InstanceBlock>();
        out.initialize();
        for (size_t j = 0; j < FZCache::BATCH && i < idx.size(); ++i, ++j) {
            size_t k = idx[i];
            size_t x = k / FZCache::BATCH;
            size_t y = k % FZCache::BATCH;
            auto ins = data[x].get(y);

            out.raw_instance.push_back(ins.raw_instance);
            out.instance_id .push_back(ins.instance_id);
            if (ins.label_size != 0) {
                out.labels.push_back(ins.labels[0]);
                out.importances.push_back(ins.importances[0]);
            }
            out.label_offset.push_back(out.labels.size());

            SCHECK(index_col == -1 && ins.feas[0].slot == -1);
            out.feas.emplace_back(-1, ins.feas[0].sign);
            out.values.push_back(1);
            out.nominal_flags.push_back(1);
            if (!time[k].first) {
                out.feas.emplace_back(0, time[k].second);
                out.values.push_back(1);
                out.nominal_flags.push_back(1);
            }

            if (i == 0 || hash[k] != hash[idx[i-1]]) {
                l = i;
                r = i;
                agg = gen_groupby_aggs(conf);
                while (limit < idx.size() && hash[k] == hash[idx[limit]]) ++limit;
            }

            while (r < limit && !win.first.valid(time[k], time[idx[r]], i, r, r)) {
                size_t t = idx[r];
                ins = data[t / FZCache::BATCH].get(t % FZCache::BATCH);
                for (size_t u = 0, v = 0; u < agg.size(); ++u) {
                    while (v < ins.fea_size && agg[u]->slot > ins.feas[v].slot) ++v;
                    if (v == ins.fea_size || ins.feas[v].slot != agg[u]->slot) {
                        agg[u]->add_missing(r);
                    } else {
                        for (size_t w = v; w < ins.fea_size && ins.feas[w].slot == agg[u]->slot; ++w)
                            agg[u]->add(r, ins.feas[w].sign, ins.values[w], ins.nominal_flags[w]);
                    }
                }
                ++r;
            }
            while (l < limit && !win.second.valid(time[k], time[idx[l]], i, r, l)) {
                size_t t = idx[l];
                ins = data[t / FZCache::BATCH].get(t % FZCache::BATCH);
                for (size_t u = 0, v = 0; u < agg.size(); ++u) {
                    while (v < ins.fea_size && agg[u]->slot > ins.feas[v].slot) ++v;
                    if (v == ins.fea_size || ins.feas[v].slot != agg[u]->slot) {
                        agg[u]->del_missing(l);
                    } else {
                        for (size_t w = v; w < ins.fea_size && ins.feas[w].slot == agg[u]->slot; ++w)
                            agg[u]->del(l, ins.feas[w].sign, ins.values[w], ins.nominal_flags[w]);
                    }
                }
                ++l;
            }
            for (auto& op : agg) op->append(i, out);
            out.fea_offset.push_back(out.feas.size());
        }
        ret->run(tid, var);
    }
}

void apply_ops( FZCache data,
                Data out,
                std::vector<int64_t> part_keys,
                int64_t index_col,
                int64_t time_col,
                std::vector<AggConf> conf,
                std::string win_str) {
    SLOG(INFO) << conf.size();

    start_declaration();
    var hold = Graph::ctx().new_variable("placeholder", Schema::gcformat(conf.size() + 1));
    out.entity()->id() = hold;
    out.entity()->initialize_sink("");
    SequentialEntity exe(finish_declaration());

    exe.graph_initialize(P_CONF.process.cpu_concurrency, true);

    auto ptr = dynamic_cast<PipingProcess<Variable, void>*>(out.entity()->get_process().get());
    SCHECK(ptr != nullptr);

    auto win = parse_window2(win_str);
    std::deque<std::thread> process;

    for (size_t i = 0; i < P_CONF.process.cpu_concurrency; ++i) {
        auto f = std::bind(_apply_ops, i,
            std::ref((*data.data)[i]),
            ptr,
            std::ref(part_keys),
            index_col, time_col,
            std::ref(conf),
            std::ref(win));
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
