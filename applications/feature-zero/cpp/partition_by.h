#ifndef PARADIGM4_PICO_APPLICATIONS_FZ_PARTITIONBY_H
#define PARADIGM4_PICO_APPLICATIONS_FZ_PARTITIONBY_H

#include "common/common.h"
#include "layer/layer.h"
#include "graph/graph.h"
#include "executor/executor.h"
#include "table/quantile_table.h"

#include "common.h"
#include "fz_cache.h"

namespace paradigm4 {
namespace pico {
namespace applications {
namespace mle {

class FZDataSource {
public:
    FZDataSource(FZCache& fz) : fz(fz) {
        auto rpc_name = "fz_partition";
        pico_barrier(PICO_FILE_LINENUM);
        _server = pico_rpc_service().create_server(rpc_name, pico_comm_rank());
        pico_barrier(PICO_FILE_LINENUM);
        _client = pico_rpc_service().create_client(rpc_name, pico_comm_size());

        mutex.resize(fz.data->size());
        _recv_thds.resize(P_CONF.process.io_concurrency);
        for (auto& recv_thd : _recv_thds)
            recv_thd = std::thread(&FZDataSource::receiving, this);
    }

    ~FZDataSource() {
        for (auto& data : *(fz.data)) {
            int tot = 0;
            for (auto& block : data)
                tot += block.size();
            SLOG(INFO) << "cached: part " << tot;
        }
    }

    void join() {
        pico_barrier(PICO_FILE_LINENUM);
        _server->terminate();
        for (auto& thd : _recv_thds) thd.join();
        _server.reset();
        _client.reset();
        pico_barrier(PICO_FILE_LINENUM);
    }

    std::shared_ptr<core::Dealer> create_sender() {
        return _client->create_dealer();
    }

private:
    void receiving() {
        auto dealer = _server->create_dealer();
        RpcRequest request;
        InstanceBlock block;
        int sid;
        while (dealer->recv_request(request)) {
            request >> sid >> block;
            RpcResponse rep(request);
            dealer->send_response(std::move(rep));

            std::unique_lock<std::mutex> lock(mutex[sid]);
            auto& tar = (*fz.data)[sid];
            if (tar.size() == 0 || tar.back().size() == FZCache::BATCH) {
                tar.push_back(std::move(block));
            } else if (block.size() == FZCache::BATCH) {
                std::swap(tar.back(), block);
                tar.push_back(std::move(block));
            } else {
                tar.back().merge(block);
                if (tar.back().size() > FZCache::BATCH) {
                    tar.back().cut(FZCache::BATCH, block);
                    tar.push_back(std::move(block));
                }
            }
        }
    }

    FZCache& fz;
    std::unique_ptr<RpcClient> _client;
    std::unique_ptr<RpcServer> _server;
    std::deque<std::thread> _recv_thds;
    std::deque<std::mutex> mutex;
};

class PartitionByNode : public Node {
public:
    PartitionByNode(std::shared_ptr<QuantileIntTableEntity> t,
                    int64_t k1, int64_t k2, int64_t timec,
                    var in, var out,
                    FZDataSource& ds)
        : Node(fmt("PartitionByNode(%s,%lld,%lld)", in.name(), k1, k2)), ds(ds) {
        inputs()  = { in };
        outputs() = { out };
        table = t;
        key1 = k1;
        key2 = k2;
        time = feature_index_t(timec, std::numeric_limits<int64_t>::min());
        fea1 = feature_index_t(key1,  std::numeric_limits<int64_t>::min());
    }

    void initialize(int tnum) override {
        size_t shard_num = pico_comm_size() * P_CONF.process.cpu_concurrency;
        _cache.resize(tnum);
        _dealers.resize(tnum);
        for (int i = 0; i < tnum; ++i) {
            _cache[i].resize(shard_num);
            _dealers[i] = ds.create_sender();
        }
    }

    void finalize() override {
        parallel_run([this](int tid) {
            auto& pool = _cache[tid];
            auto& deal = _dealers[tid];
            for (size_t target = 0; target < pool.size(); ++target) if (pool[target].size() > 0) {
                int rid = target / P_CONF.process.cpu_concurrency;
                int sid = target % P_CONF.process.cpu_concurrency;
                RpcRequest req(rid);
                req << sid << pool[target];
                req.set_sid(rid);
                deal->send_request(std::move(req));
                RpcResponse rep;
                deal->recv_response(rep);
            }
        });

        _cache.clear();
        _dealers.clear();
    }

    void calc(int tid, Session& sess) override {
        auto& data = sess.get(input()).data<InstanceBlock>();
        auto& all  = table->pull(key2);
        auto& pool = _cache[tid];
        auto& deal = _dealers[tid];
        for (size_t i = 0; i < data.size(); ++i) {
            auto ins = data.get(i);
            auto ptr = find_slot(ins, fea1);

            size_t target = 0;
            if (ptr) {
                int id = std::lower_bound(all.begin(), all.end(), ptr->sign) - all.begin();
                if (time.slot >= 0) {
                    auto& dis = table->pull(time.slot);
                    int ir = std::upper_bound(all.begin(), all.end(), ptr->sign) - all.begin();
                    auto p = find_slot(ins, time);
                    if (p) {
                        int ti = std::lower_bound(dis.begin(), dis.end(), p->sign) - dis.begin();
                        id += ti * (ir - id) / (dis.size() + 1);
                    }
                }
                target = id * pool.size() / (all.size() + 1);
            }
            gc_append(pool[target], ins);
            if (pool[target].size() >= FZCache::BATCH) {
                InstanceBlock tail;
                pool[target].cut(FZCache::BATCH, tail);
                int rid = target / P_CONF.process.cpu_concurrency;
                int sid = target % P_CONF.process.cpu_concurrency;
                RpcRequest req(rid);
                req << sid << pool[target];
                req.set_sid(rid);
                deal->send_request(std::move(req));
                RpcResponse rep;
                deal->recv_response(rep);
                pool[target] = std::move(tail);
            }
        }
    }

private:
    std::shared_ptr<QuantileIntTableEntity> table;
    int64_t key1, key2;
    std::vector<std::vector<InstanceBlock>> _cache;
    FZDataSource& ds;
    std::deque<std::shared_ptr<core::Dealer>> _dealers;
    feature_index_t fea1, time;
};

FZCache partition_by(Data in, QuantileIntTable distribution, int64_t key1, int64_t key2, int64_t timec) {
    FZCache ret;
    FZDataSource ds(ret);

    start_declaration();
    var block = in.Read("async", FZCache::BATCH);
    var entry = Graph::ctx().new_variable(fmt("partby:%d", block), Schema::entry());
    declare_forward<PartitionByNode>(distribution.entity(), key1, key2, timec, block, entry, ds);

    auto graph = finish_declaration();
    SequentialExecutor exe(graph);
    exe.run(false, -1);

    ds.join();

    for (auto& pool : *ret.data) {
        for (size_t i = 0; i < pool.size(); ++i) {
            SCHECK(pool[i].size() == FZCache::BATCH || i == pool.size() - 1);
            SCHECK(pool[i].valid_check(pool[i].size()))
                << pool[i].raw_instance.size() << ' '
                << pool[i].instance_id.size() << ' '
                << pool[i].label_offset.size() << ' '
                << pool[i].labels.size() << ' '
                << pool[i].importances.size() << ' '
                << pool[i].fea_offset.size() << ' '
                << pool[i].feas.size() << ' '
                << pool[i].values.size() << ' '
                << pool[i].nominal_flags.size();
        }
    }
    return ret;
}

} // namespace mle
} // namespace applications
} // namespace pico
} // namespace paradigm4

#endif
