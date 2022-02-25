#ifndef PARADIGM4_PICO_APPLICATIONS_TABLE_FILTER_PS_SET_H
#define PARADIGM4_PICO_APPLICATIONS_TABLE_FILTER_PS_SET_H

#include "ps/include/api/TableEntry.h"
#include "pico-ds/pico-ds/record-batch/ArrowUtils.h"
#include "app-common/include/ApplicationCommon.h"
#include "table/table.h"

namespace paradigm4 {
namespace pico {
namespace applications {
namespace mle {

typedef feature_index_t key_type;
typedef ps::GoogleDenseHashMapShardStorage<
    key_type, int8_t, DefaultPartitioner<feature_index_t>
> StorageType;

class SetInit : public ps::ShardStorageOperator<StorageType> {
public:
    typedef ps::ShardStorageOperator<StorageType> base_type;
    SetInit(const Configure& conf) : base_type(conf) {}
};

class SetPush : public ps::SparseTablePushOperator<
                key_type, int8_t, int8_t, int8_t,
                DefaultPartitioner<feature_index_t>,
                StorageType, StorageType> {
public:
    typedef ps::SparseTablePushOperator<
                key_type, int8_t, int8_t, int8_t,
                DefaultPartitioner<feature_index_t>,
                StorageType, StorageType> base_type;
    SetPush(const Configure& conf) : base_type(conf) {}

    bool init_store_value(const key_type&, const int8_t&, int8_t& val) override {
        val = 1;
        return true;
    }

    void apply_push_arg(const key_type&, const int8_t&, int8_t& val) override {
        val = 1;
    }

    void merge_push_value(const key_type&, const int8_t&, int8_t& val) override {
        val = 1;
    }

    void store_push_value(const key_type&, const int8_t&, int8_t& val) override {
        val = 1;
    }
};

class SetPull : public ps::SparseTablePullOperator<
                key_type, int8_t, int8_t, int8_t,
                DefaultPartitioner<feature_index_t>,
                StorageType> {
public:
    typedef ps::SparseTablePullOperator<
                key_type, int8_t, int8_t, int8_t,
                DefaultPartitioner<feature_index_t>,
                StorageType> base_type;
    SetPull(const Configure& conf) : base_type(conf) {}

    bool init_store_value(const key_type&, int8_t& val) override {
        val = 0;
        return false;
    }

    void fetch_pull_value(const key_type&, const int8_t& store, int8_t& val) override {
        val = store;
    }

    void store_pull_value(const key_type&, const int8_t& value, int8_t& arg) override {
        arg = value;
    }
};

class PSSetEntity : public TableEntity {
public:
    PSSetEntity() {
        _table = std::make_shared<ps::TableEntry>("OP", "SetInit");
        _push_id = _table->register_push("OP", "SetPush");
        _pull_id = _table->register_pull("OP", "SetPull").second;
    }

    void store() override {}

    bool load(const URIConfig& ) override { return false; }

    void initialize(int tnum, bool) override {
        for (int i = _push_handlers.size(); i < tnum; ++i)
            _push_handlers.push_back(_table->push_handler(_push_id));
        for (int i = _pull_handlers.size(); i < tnum; ++i)
            _pull_handlers.push_back(_table->pull_handler(_pull_id));
        _push_cache.resize(tnum);
        _pull_cache.resize(tnum);
    }

    void push(int tid, const std::vector<key_type>& keys) {
        _push_cache[tid].assign(keys.size(), 1);
        _push_handlers[tid].async_push(keys.data(), _push_cache[tid].data(), keys.size());
        _push_handlers[tid].wait();
    }

    int8_t* pull(int tid, const std::vector<key_type>& keys) {
        _pull_cache[tid].resize(keys.size());
        _pull_handlers[tid].pull(keys.data(), _pull_cache[tid].data(), keys.size());
        _pull_handlers[tid].wait();
        return _pull_cache[tid].data();
    }

    void reset() {
        _table.reset();
        _pull_handlers.clear();
        _push_handlers.clear();
    }

private:
    std::shared_ptr<ps::TableEntry> _table;
    int _pull_id, _push_id;
    std::deque<ps::PushHandler> _push_handlers;
    std::deque<ps::PullHandler> _pull_handlers;

    std::vector<std::vector<int8_t>> _push_cache;
    std::vector<std::vector<int8_t>> _pull_cache;
};

class PSSet {
public:
    PSSet() {
        _entity = std::make_shared<PSSetEntity>();
    }

    std::shared_ptr<PSSetEntity> entity() {
        return _entity;
    }

    void reset() {
        _entity->reset();
        _entity.reset();
    }

private:
    std::shared_ptr<PSSetEntity> _entity;
};

} // namespace mle
} // namespace applications

namespace ps {

typedef paradigm4::pico::applications::mle::SetInit SetInit;
typedef paradigm4::pico::applications::mle::SetPush SetPush;
typedef paradigm4::pico::applications::mle::SetPull SetPull;
REGISTER_OPERATOR(OP, SetInit);
REGISTER_OPERATOR(OP, SetPush);
REGISTER_OPERATOR(OP, SetPull);

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif
