#ifndef PARADIGM4_PICO_APPLICATIONS_FZ_TRANSFER_DATA_H
#define PARADIGM4_PICO_APPLICATIONS_FZ_TRANSFER_DATA_H

#include "common/include/ExecutionPlan.h"

#include "common/common.h"
#include "layer/layer.h"
#include "graph/graph.h"
#include "executor/executor.h"

#include "common.h"
#include "fz_cache.h"

namespace paradigm4 {
namespace pico {
namespace applications {
namespace mle {

class ReadFZData : public PipingProcess<void, Variable> {
public:
    ReadFZData(FZCache data) : data(data.data) {}

    bool run(int tid, Variable& out) override {
        auto& pool = (*data)[tid];
        while (pool.size() > 0 && pool.back().size() == 0) pool.pop_back();
        if (pool.size() == 0) return false;
        auto var = std::make_shared<TypedVariable<InstanceBlock>>(std::move(pool.back()));
        out.entity() = var;
        pool.pop_back();
        return true;
    }

private:
    std::shared_ptr<std::vector<std::vector<InstanceBlock>>> data;
};

void transfer_data(Data cache, FZCache data, int cols) {
    start_declaration();
    var hold = Graph::ctx().new_variable("placeholder", Schema::gcformat(cols));
    cache.entity()->id() = hold;
    cache.entity()->initialize_sink("");
    finish_declaration();

    cache.entity()->initialize();

    auto ds = std::make_shared<ReadFZData>(data);
    auto sk = cache.entity()->get_process();
    SCHECK(sk != nullptr);
    connect_process(ds, sk);

    auto plan = pico_execution_plan();
    plan->add(sk);
    plan->initialize();
    plan->execute();
    plan->finalize();
    cache.entity()->finalize();
    data.reset();
}

} // namespace mle
} // namespace applications
} // namespace pico
} // namespace paradigm4

#endif
