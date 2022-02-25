#ifndef PARADIGM4_PICO_APPLICATIONS_GBM_TOP_CATEGORY_H
#define PARADIGM4_PICO_APPLICATIONS_GBM_TOP_CATEGORY_H

#include "ps/include/api/TableEntry.h"
#include "app-common/include/ApplicationCommon.h"

#include "common/common.h"
#include "table/table.h"

namespace paradigm4 {
namespace pico {
namespace applications {
namespace mle {

typedef feature_index_t key_type;
typedef ps::GoogleDenseHashMapShardStorage<
    key_type, int64_t, DefaultPartitioner<feature_index_t>
> StorageType;

class InitOp : public ps::ShardStorageOperator<StorageType> {
public:
    typedef ps::ShardStorageOperator<StorageType> base_type;
    InitOp(const Configure& conf) : base_type(conf) {}
};

class PushOp  : public ps::SparseTablePushOperator<
                key_type, int64_t, int64_t, int64_t,
                DefaultPartitioner<feature_index_t>,
                StorageType, StorageType> {
public:
    typedef ps::SparseTablePushOperator<
                key_type, int64_t, int64_t, int64_t,
                DefaultPartitioner<feature_index_t>,
                StorageType, StorageType> base_type;
    PushOp(const Configure& conf) : base_type(conf) {}

    bool init_store_value(const key_type&, const int64_t& arg, int64_t& val) override {
        val += arg;
        return true;
    }
    void apply_push_arg(const key_type&, const int64_t& arg, int64_t& val) override {
        val += arg;
    }
    void merge_push_value(const key_type&, const int64_t& arg, int64_t& val) override {
        val += arg;
    }
    void store_push_value(const key_type&, const int64_t& arg, int64_t& val) override {
        val += arg;
    }
};

struct TopCategoryResult : ps::ForEachResult {
    TopCategoryResult() {}
    std::unordered_map<int64_t, std::vector<std::pair<int64_t, int64_t>>> data;
    PICO_PS_SERIALIZATION(TopCategoryResult, data);
};

class TopCategoryOp : public ps::SparseTableForEachReadOperator<StorageType, TopCategoryResult> {
public:
    typedef ps::SparseTableForEachReadOperator<StorageType, TopCategoryResult> base_type;
    typedef typename base_type::key_type   key_type;

    TopCategoryOp(const Configure& conf) : base_type(conf) {
        _max_n = conf.node()["max_n"].as<int32_t>();
    }

    std::unique_ptr<TopCategoryResult> init_result() override {
        return std::make_unique<TopCategoryResult>();
    }

    std::unique_ptr<TopCategoryResult> for_each(const key_type& key, const int64_t& val) override {
        auto ret = init_result();
        ret->data[key.slot].push_back({val, key.sign});
        return ret;
    }

    void merge_result(const TopCategoryResult& in, TopCategoryResult& out) override {
        for (const auto& pair : in.data) {
            auto it = out.data.find(pair.first);
            if (it == out.data.end()) {
                out.data.insert(pair);
            } else {
                auto& vec = it->second;
                for (const auto& ps : pair.second) {
                    if (vec.size() < _max_n) {
                        vec.push_back(ps);
                        int i = vec.size() - 1;
                        while (i) {
                            int fa = (i - 1) >> 1;
                            if (vec[i].first < vec[fa].first) {
                                vec[i] = vec[fa];
                                i = fa;
                            } else {
                                break;
                            }
                        }
                        vec[i] = ps;
                    } else if (ps.first > vec[0].first) {
                        int i = 0;
                        while (true) {
                            size_t lc = (i<<1) + 1;
                            size_t rc = (i<<1) + 2;
                            if (lc < vec.size() && ps.first > vec[lc].first &&
                                    (rc >= vec.size() || vec[lc].first < vec[rc].first)) {
                                vec[i] = vec[lc];
                                i = lc;
                            } else if (rc < vec.size() && ps.first > vec[rc].first) {
                                vec[i] = vec[rc];
                                i = rc;
                            } else {
                                break;
                            }
                        }
                        vec[i] = ps;
                    }
                }
            }
        }
    }
private:
    size_t _max_n = 100;
};

class TopCategoryEntity : public TableEntity {
public:
    TopCategoryEntity(int max_n) : _max_n(max_n) {
        _table = std::make_shared<ps::TableEntry>("OP", "init_op");
    }

    void store() override {
        Configure op_conf;
        op_conf.node()["max_n"] = _max_n;
        auto hid = _table->register_for_each("OP", "top_cat_op", op_conf);

        auto result = _table->template for_each<TopCategoryResult>(hid);
        for (auto& p : result.data) {
            auto& data = _data[p.first];
            for (auto& v : p.second)
                data.push_back(v.second);
            std::sort(data.begin(), data.end());
            _slots.push_back(p.first);
        }
        std::sort(_slots.begin(), _slots.end());
        reset();
    }

    bool load(const URIConfig& ) override { return false; }

    void initialize(int tnum, bool) override {
        auto push_id = _table->register_push("OP", "push_op");
        for (int i = _push_handlers.size(); i < tnum; ++i)
            _push_handlers.push_back(_table->push_handler(push_id));
        _push_cache.resize(tnum);
    }

    void push(int tid, const core::vector<key_type>& keys) {
        _push_cache[tid].assign(keys.size(), 1);
        _push_handlers[tid].async_push(keys.data(), _push_cache[tid].data(), keys.size());
        _push_handlers[tid].wait();
    }

    void reset() {
        _table.reset();
        _push_handlers.clear();
    }

    const std::vector<int64_t>& slots() const {
        return _slots;
    }

    const std::vector<int64_t>& pull(int64_t slot) const {
        return _data.at(slot);
    }

    int max_n() const {
        return _max_n;
    }

private:
    int _max_n;
    std::shared_ptr<ps::TableEntry> _table;
    std::deque<ps::PushHandler> _push_handlers;
    std::vector<std::vector<int8_t>> _push_cache;

    std::unordered_map<int64_t, std::vector<int64_t>> _data;
    std::vector<int64_t> _slots;
};

class TopCategoryTable {
public:
    TopCategoryTable(int max_n) {
        _entity = std::make_shared<TopCategoryEntity>(max_n);
    }

    std::shared_ptr<TopCategoryEntity> entity() {
        return _entity;
    }

    void reset() {
        _entity->reset();
        _entity.reset();
    }

    std::vector<int64_t> slots() {
        return _entity->slots();
    }

    int max_n() {
        return _entity->max_n();
    }

private:
    std::shared_ptr<TopCategoryEntity> _entity;
};

std::string TopCategoryStat_doc = "";

class TopCategoryStatNode : public Node {
public:
    TopCategoryStatNode(TopCategoryTable table, var in, var out)
        : Node(fmt("TopCategoryStatNode(%s, %s)", in.name(), out.name())) {
        inputs()  = { in };
        outputs() = { out };
        _table = table.entity();
    }

    void calc(int tid, Session& sess) override {
        auto& block = sess.get(input()).data<SparseInput>();
        _table->push(tid, block.keys);
    }
private:
    std::shared_ptr<TopCategoryEntity> _table;
};

var TopCategoryStat_build(var input, TopCategoryTable table) {
    SCHECK(input.valid() && input.schema().type == DType::SPARSEV);
    Graph::ctx().add_table(table.entity().get());

    auto name = fmt("top_cat:%d", input);
    var entry = Graph::ctx().new_variable(name, Schema::entry());
    declare_forward<TopCategoryStatNode>(table, input, entry);
    return entry;
}

} // namespace mle
} // namespace applications

namespace ps {

typedef applications::mle::InitOp init_op;
typedef applications::mle::PushOp push_op;
typedef applications::mle::TopCategoryOp top_cat_op;
REGISTER_OPERATOR(OP, init_op);
REGISTER_OPERATOR(OP, push_op);
REGISTER_OPERATOR(OP, top_cat_op);

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif
