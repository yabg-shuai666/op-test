#ifndef PARADIGM4_PICO_APPLICATIONS_TABLE_FILTER_PSSET_H
#define PARADIGM4_PICO_APPLICATIONS_TABLE_FILTER_PSSET_H

#include "layer/layer.h"
#include "graph/node.h"
#include "executor/session.h"

namespace paradigm4 {
namespace pico {
namespace applications {
namespace mle {

std::string PSSetPush_doc = "";
std::string PSSetFilter_doc = "";

std::vector<feature_index_t> fea_to_key(DataFrame& df, const std::vector<std::string>& key) {
    std::vector<feature_index_t> out(df.num_rows());
    std::vector<std::string> cache(df.num_rows());

    for (auto& k : key) {
        auto col = df.column(k)->data();
        ds::ArrowUtils::ValueConverter<std::string> conv(col->type()->id());

        for (int64_t i = 0, k = 0; i < col->num_chunks(); ++i) {
            auto arr = col->chunk(i).get();
            for (int64_t j = 0; j < arr->length(); ++j, ++k) {
                cache[k] += conv.parse(arr, j);
            }
        }
        for (auto& s : cache)
            s += '\0';
    }

    for (size_t i = 0; i < df.num_rows(); ++i) {
        auto& s = cache[i];
        core::murmur_hash3_x64_128(s.data(), s.length(), MURMURHASH_SEED, &out[i]);
    }
    return out;
}

class PSSetPushNode : public Node {
public:
    PSSetPushNode(PSSet table, const std::vector<std::string>& key, var in, var out)
        : Node(fmt("PSSetPushNode(%s)", in.name()))
        , key(key) {
        _table = table.entity();
        inputs()  = { in };
        outputs() = { out };
    }

    void calc(int tid, Session& sess) override {
        auto& df = sess.get(input()).data<DataFrame>();
        _table->push(tid, fea_to_key(df, key));
    }
private:
    std::vector<std::string> key;
    std::shared_ptr<PSSetEntity> _table;
};

var PSSetPush_build(var input, std::vector<std::string> key, PSSet table) {
    Graph::ctx().add_table(table.entity().get());

    auto schema = input.schema();
    SCHECK(schema.type == DType::PARQUET);

    auto nm = fmt("set_push:%d", input);
    var out = Graph::ctx().new_variable(nm, Schema::entry());
    declare_forward<PSSetPushNode>(table, key, input, out);
    return out;
}

class PSSetFilterNode : public Node {
public:
    PSSetFilterNode(PSSet table, const std::vector<std::string>& key, var in, var out)
        : Node(fmt("PSSetFilterNode(%s)", in.name()))
        , key(key) {
        _table = table.entity();
        inputs()  = { in };
        outputs() = { out };
    }

    void calc(int tid, Session& sess) override {
        auto& in  = sess.get(input()) .data<DataFrame>();
        auto& out = sess.get(output()).data<DataFrame>();

        auto filter = _table->pull(tid, fea_to_key(in, key));
        for (size_t i = 0; i < in.num_rows(); ++i) if (filter[i] != 0) {
            auto tmp = in.slice(i, 1);
            out.merge(tmp);
        }
    }
private:
    std::vector<std::string> key;
    std::shared_ptr<PSSetEntity> _table;
};

var PSSetFilter_build(var input, std::vector<std::string> key, PSSet table) {
    Graph::ctx().add_table(table.entity().get());

    auto schema = input.schema();
    SCHECK(schema.type == DType::PARQUET);

    auto nm = fmt("set_filter:%d", input);
    var out = Graph::ctx().new_variable(nm, Schema::parquet(schema.x));
    declare_forward<PSSetFilterNode>(table, key, input, out);
    return out;
}

} // namespace mle
} // namespace applications
} // namespace pico
} // namespace paradigm4

#endif
