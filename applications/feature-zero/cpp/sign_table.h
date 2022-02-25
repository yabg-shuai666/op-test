#ifndef PARADIGM4_PICO_APPLICATIONS_FZ_SIGN_TABLE_H
#define PARADIGM4_PICO_APPLICATIONS_FZ_SIGN_TABLE_H

#include "common/common.h"
#include "table/table.h"
#include "data/sparse_input.h"

namespace paradigm4 {
namespace pico {
namespace applications {
namespace mle {

class SignLengthTableEntity : public TableEntity {
public:
    using table_t = std::unordered_map<int64_t, size_t>;

    void initialize(int tnum, bool) override {
        _pool.clear();
        _pool.resize(tnum);
    }

    void store() override {
        table_t tmp;
        for (auto& pool : _pool)
            merge(pool, tmp);
        _table.clear();
        pico_all_reduce(tmp, _table, merge);
    }

    void push(int tid, SparseInput& block) {
        auto& pool = _pool[tid];
        for (size_t i = 0; i < block.size(); ++i) {
            int begin = block.offsets[i];
            int end = block.offsets[i+1];
            while (begin < end) {
                auto slot = block.keys[begin].slot;
                size_t cnt = 0;
                while (begin < end && block.keys[begin].slot == slot)
                    ++begin, ++cnt;
                pool[slot] = std::max(pool[slot], cnt);
            }
        }
    }

    bool load(const URIConfig& ) { return false; }

    static void merge(const table_t& in, table_t& out) {
        for (auto& p : in)
            out[p.first] = std::max(out[p.first], p.second);
    }

    std::vector<table_t> _pool;
    table_t _table;
};

class SignLengthTable {
public:
    SignLengthTable() {
        _entity = std::make_shared<SignLengthTableEntity>();
    }

    size_t pull(int64_t slot) {
        if (_entity->_table.count(slot))
            return _entity->_table.at(slot);
        else
            return 0;
    }

    void reset() {
        if (_entity) {
            _entity->_table.clear();
            _entity->_pool.clear();
            _entity.reset();
        }
    }

    std::shared_ptr<SignLengthTableEntity> _entity;
};

std::string SignLengthStat_doc = "";

class SignLengthStatNode : public Node {
public:
    SignLengthStatNode(SignLengthTable table, var in, var out)
        : Node(fmt("SignLengthStat(%s)", in.name())) {
        _table = table._entity;
        inputs()  = { in };
        outputs() = { out };
    }

    void calc(int tid, Session& sess) override {
        auto& block = sess.get(input()).data<SparseInput>();
        _table->push(tid, block);
    }

private:
    std::shared_ptr<SignLengthTableEntity> _table;
};

var SignLengthStat_build(var input, SignLengthTable table) {
    SCHECK(input.schema().type == DType::SPARSEV);
    Graph::ctx().add_table(table._entity.get());

    auto nm = fmt("sls_%d", input);
    var out = Graph::ctx().new_variable(nm, Schema::entry());
    declare_forward<SignLengthStatNode>(table, input, out);
    return out;
}

class SignCountTableEntity : public TableEntity {
public:
    using table_t = std::unordered_map<int64_t, size_t>;

    void initialize(int tnum, bool) override {
        _pool.clear();
        _pool.resize(tnum);
    }

    void store() override {
        table_t local;
        for (auto& pool : _pool) {
            table_t tmp;
            for (auto& p : pool)
                tmp[p.first] = p.second.size();
            merge(tmp, local);
        }
        _table.clear();
        pico_all_reduce(local, _table, merge);
    }

    void push(int tid, SparseInput& block) {
        auto& pool = _pool[tid];
        for (auto& fea : block.keys)
            pool[fea.slot].insert(fea.sign);
    }

    bool load(const URIConfig& ) { return false; }

    static void merge(const table_t& in, table_t& out) {
        for (auto& p : in)
            out[p.first] += p.second;
    }

    std::vector<std::unordered_map<int64_t, std::unordered_set<int64_t>>> _pool;
    table_t _table;
};

class SignCountTable {
public:
    SignCountTable() {
        _entity = std::make_shared<SignCountTableEntity>();
    }

    size_t pull(int64_t slot) {
        if (_entity->_table.count(slot))
            return _entity->_table.at(slot);
        else
            return 0;
    }

    void reset() {
        if (_entity) {
            _entity->_table.clear();
            _entity->_pool.clear();
            _entity.reset();
        }
    }

    std::shared_ptr<SignCountTableEntity> _entity;
};

std::string SignCountStat_doc = "";

class SignCountStatNode : public Node {
public:
    SignCountStatNode(SignCountTable table, var in, var out)
        : Node(fmt("SignCountStat(%s)", in.name())) {
        _table = table._entity;
        inputs()  = { in };
        outputs() = { out };
    }

    void calc(int tid, Session& sess) override {
        auto& block = sess.get(input()).data<SparseInput>();
        _table->push(tid, block);
    }

private:
    std::shared_ptr<SignCountTableEntity> _table;
};

var SignCountStat_build(var input, SignCountTable table) {
    SCHECK(input.schema().type == DType::SPARSEV);
    Graph::ctx().add_table(table._entity.get());

    auto nm = fmt("scs_%d", input);
    var out = Graph::ctx().new_variable(nm, Schema::entry());
    declare_forward<SignCountStatNode>(table, input, out);
    return out;
}

std::vector<int64_t> get_time_interval(SignCountTable table, int64_t slot) {
    std::vector<int64_t> sign;
    for (auto& pool : table._entity->_pool) if (pool[slot].size() > 0) {
        sign.insert(sign.end(), pool[slot].begin(), pool[slot].end());
    }

    if (sign.size() > 0) {
        std::sort(sign.begin(), sign.end());
        sign.resize(std::unique(sign.begin(), sign.end()) - sign.begin());
    }

    int64_t tmp = std::numeric_limits<int64_t>::max();
    for (size_t i = 1; i < sign.size(); ++i)
        tmp = std::min(tmp, sign[i] - sign[i - 1]);

    auto ret = tmp;
    pico_all_reduce(tmp, ret, [](const int64_t& a, int64_t& b) { b = std::min(a, b); });
    if (ret == std::numeric_limits<int64_t>::max())
        ret = 0;

    if (sign.size() == 0)
        return {ret, 0, 0};
    else
        return {ret, sign.front(), sign.back()};
}

} // namespace mle
} // namespace applications
} // namespace paradigm4
} // namespace pico

#endif
