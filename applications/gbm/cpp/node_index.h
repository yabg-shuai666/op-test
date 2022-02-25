#ifndef PARADIGM4_PICO_APPLICATIONS_GBM_NODE_INDEX_H
#define PARADIGM4_PICO_APPLICATIONS_GBM_NODE_INDEX_H

#include "layer/layer.h"
#include "graph/node.h"
#include "executor/session.h"
#include "data/global_index.h"
#include "table/vector.h"

namespace paradigm4 {
namespace pico {
namespace applications {
namespace mle {

class NodeIndex {
public:
    NodeIndex(int dim) {
        _entity = std::make_shared<VectorTableEntity<int32_t>>(10000, 0, dim);
    }

    std::shared_ptr<VectorTableEntity<int32_t>> entity() {
        return _entity;
    }

    void clear() {
        _entity->clear_storage();
    }

    void reset_value() {
        _entity->reset_value();
    }

    int dim() {
        return _entity->dim();
    }

    static std::shared_ptr<Schema> schema() {
        return std::make_shared<Schema>(DType::INT32);
    }

private:
    std::shared_ptr<VectorTableEntity<int32_t>> _entity;
};

std::string GetNodeIndex_doc = "";
std::string SetNodeIndex_doc = "";

class GetNodeIndexNode : public Node {
public:
    GetNodeIndexNode(NodeIndex table, var idx, var out)
        : Node(fmt("GetNodeIndexNode(%s, %s)", idx.name(), out.name())) {
        _table = table.entity();
        inputs()  = { idx };
        outputs() = { out };
    }

    void calc(int, Session& sess) override {
        auto& idx = sess.get( input()).data<GlobalIndex>();
        auto& out = sess.get(output()).data<std::vector<int32_t>>();
        out.resize(idx.size());
        int cur = 0;
        for (auto& seg : idx.segments) {
            _table->get_value(out.data() + cur, seg.start, seg.length);
            cur += seg.length;
        }
    }

private:
    std::shared_ptr<VectorTableEntity<int32_t>> _table;
};

var GetNodeIndex_build(var index, NodeIndex table) {
    SCHECK(index.schema().type == DType::INDEX);

    auto nm = fmt("idx_get:%d", index);
    var out = Graph::ctx().new_variable(nm, NodeIndex::schema());
    declare_forward<GetNodeIndexNode>(table, index, out);
    return out;
}

class SetNodeIndexNode : public Node {
public:
    SetNodeIndexNode(NodeIndex table, var gidx, var nidx, var out)
        : Node(fmt("SetNodeIndexNode(%s, %s, %s)", gidx.name(), nidx.name(), out.name())) {
        _table = table.entity();
        inputs()  = { gidx, nidx };
        outputs() = { out };
    }

    void calc(int, Session& sess) override {
        auto& gidx = sess.get(input(0)).data<GlobalIndex>();
        auto& nidx = sess.get(input(1)).data<std::vector<int32_t>>();
        int cur = 0;
        for (auto& seg : gidx.segments) {
            _table->set_value(nidx.data() + cur, seg.start, seg.length);
            cur += seg.length;
        }
    }

private:
    std::shared_ptr<VectorTableEntity<int32_t>> _table;
};

var SetNodeIndex_build(var gidx, var nidx, NodeIndex table) {
    SCHECK(gidx.schema().type == DType::INDEX);
    SCHECK(nidx.schema().type == DType::INT32);

    auto nm = fmt("idx_set:(%d,%d)", gidx, nidx);
    var out = Graph::ctx().new_variable(nm, Schema::entry());
    declare_forward<SetNodeIndexNode>(table, gidx, nidx, out);
    return out;
}

} // namespace mle
} // namespace applications
} // namespace pico
} // namespace paradigm4

#endif
