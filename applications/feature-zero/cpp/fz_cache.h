#ifndef PARADIGM4_PICO_APPLICATIONS_FZ_CACHE_H
#define PARADIGM4_PICO_APPLICATIONS_FZ_CACHE_H

#include "pico-ds/pico-ds/builder/ParserCodec.h"
#include "pico-ds/pico-ds/builder/URIBuilder.h"
#include "common/include/ThreadGroup.h"
#include "common.h"

namespace paradigm4 {
namespace pico {
namespace applications {
namespace mle {

inline size_t block_memory_size(const InstanceBlock& block) {
    size_t cnt = 0;
    cnt += sizeof(std::string)     * block.raw_instance .size();
    cnt += sizeof(std::string)     * block.instance_id  .size();
    cnt += sizeof(size_t)          * block.label_offset .size();
    cnt += sizeof(ds::real_t)      * block.labels       .size();
    cnt += sizeof(ds::real_t)      * block.importances  .size();
    cnt += sizeof(size_t)          * block.fea_offset   .size();
    cnt += sizeof(feature_index_t) * block.feas         .size();
    cnt += sizeof(ds::real_t)      * block.values       .size();
    cnt += sizeof(int8_t)          * block.nominal_flags.size();
    for (auto& s : block.raw_instance) cnt += s.length();
    for (auto& s : block.instance_id)  cnt += s.length();
    return cnt;
}

inline std::string format_memory_size(size_t mem) {
    std::string unit = "";
    if (mem > 1000) {
        mem /= 1000;
        unit = "K";
    }
    if (mem > 1000) {
        mem /= 1000;
        unit = "M";
    }
    if (mem > 1000) {
        mem /= 1000;
        unit = "G";
    }
    return std::to_string(mem) + unit;
}

struct df_builder_t {
    std::unique_ptr<ds::Codec<ds::ds_data_t<Variable>>> codec;

    template<class C>
    bool build() {
        codec = std::make_unique<C>();    
        return codec.get() != nullptr;
    }

    template<class C, class P>
    bool build() {
        codec = std::make_unique<
            ds::ReadParserCodec<
                ds::ds_data_t<Variable>,
                C,
                ds::DsMetaParser<typename P::in_type, P, typename P::out_type, ds::ds_meta_t>
            >
        >();
        return codec.get() != nullptr;
    }
};

struct FZCache {
    static const size_t BATCH = 100;

    FZCache() {
        data = std::make_shared<std::vector<std::vector<InstanceBlock>>>();
        data->resize(P_CONF.process.cpu_concurrency);
    }

    std::vector<InstanceBlock>& shard(int i) {
        return (*data)[i];
    }

    void reset() {
        data->clear();
        data = nullptr;
    }

    std::string memory_size() const {
        std::vector<size_t> tot(P_CONF.process.cpu_concurrency, 0);
        ThreadGroup tg(P_CONF.process.cpu_concurrency);
        pico_parallel_run([this, &tot](int, int tid) {
            auto& list = (*data)[tid];
            auto& cnt  = tot[tid];
            for (auto& block : list)
                cnt += block_memory_size(block);
        }, tg);
        size_t mem = 0;
        for (auto t : tot) mem += t;
        return format_memory_size(mem);
    }

    std::shared_ptr<std::vector<std::vector<InstanceBlock>>> data;
};

std::string gc_memory_size(Data data) {
    auto cache = data.entity()->get_cache();
    if (cache == nullptr || cache->path().is_file())
        return "0K";

    using ins_t = ds::ds_data_t<Variable>;
    std::vector<size_t> cnt(cache->get_cache_num());
    ThreadGroup tg(cache->get_cache_num());

    pico_parallel_run([&cache, &cnt](int, int cache_id) {
        ins_t tmp;
        auto uri = cache->get_cache(cache_id).uri;
        auto& tot = cnt[cache_id];
        df_builder_t builder;
        SCHECK(ds::pico_cache_source_build<Variable>(uri, builder));
        SCHECK(builder.codec.get() != nullptr);
        builder.codec->open(uri, ds::CodecOpenMode::READ);
        while (builder.codec->read(tmp))
            tot += block_memory_size(tmp.val.data<InstanceBlock>());
    }, tg);

    size_t tot = 0;
    for (auto t : cnt) tot += t;
    return format_memory_size(tot);
}

} // namespace mle
} // namespace applications
} // namespace pico
} // namespace paradigm4

#endif
