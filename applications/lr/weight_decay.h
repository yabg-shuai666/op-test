#include "pico-ps/pico-ps/operator/Operator.h"
#include "common/include/AccumulatorReporter.h"
#include "ps/include/api/DsOperators.h"
#include "app-common/include/ApplicationCommon.h"

#include "table/common.h"
#include "table/ftrl.h"
#include "table/sparse_table.h"

namespace paradigm4 {
namespace pico {
namespace applications {
namespace mle {

typedef feature_index_t key_type;
typedef real_t val_type;
typedef ftrl_m<real_t> opt_type;
typedef std::array<uint8_t, sizeof(val_type)> val_bin_t;
typedef std::array<uint8_t, sizeof(opt_type)> opt_bin_t;
typedef TableValue<val_bin_t, opt_bin_t> table_value_type;
typedef ps::GoogleDenseHashMapShardStorage<
    key_type,
    table_value_type,
    DefaultPartitioner<key_type>
> StorageType;

struct Result : public ps::ForEachResult {
    Result() {}

    Result(int64_t fea_num, int64_t effective_fea_num)
        : fea_num(fea_num), effective_fea_num(effective_fea_num) {}

    int64_t fea_num = 0;
    int64_t effective_fea_num = 0;

    PICO_PS_SERIALIZATION(Result, fea_num, effective_fea_num);
};

class FTRLWeightDecayOp : public ps::SparseTableForEachWriteOperator<StorageType, Result> {
public:
    typedef ps::SparseTableForEachWriteOperator<StorageType, Result> base_type;

    FTRLWeightDecayOp(const Configure& conf) : base_type(conf) {
        _show_decay_rate = conf.node()["show_decay_rate"].as<double>();
        _decayed_show_filter_threshold
            = conf.node()["decayed_show_filter_threshold"].as<double>();
    }

    std::unique_ptr<Result> init_result() override {
        return std::make_unique<Result>(0, 0);
    }

    std::unique_ptr<Result> for_each(const key_type& , table_value_type& data) override {
        auto& val = cast<val_type>(data.val);
        auto& opt = cast<opt_type>(data.opt);

        if (_show_decay_rate < 1.0 - 1e-10) {
            opt.decayed_show *= _show_decay_rate;
        }
        if (opt.decayed_show >= _decayed_show_filter_threshold) {
            return std::make_unique<Result>(1, val != 0);
        }
        return init_result();
    }

    void merge_result(const Result& in, Result& out) override {
        out.fea_num += in.fea_num;
        out.effective_fea_num += in.effective_fea_num;
    }

private:
    double _show_decay_rate;
    double _decayed_show_filter_threshold;
};

class FTRLCountEffectOp : public FTRLWeightDecayOp {
public:
    FTRLCountEffectOp(const Configure& conf) : FTRLWeightDecayOp(conf) {}

    std::unique_ptr<Result> for_each(const key_type& , table_value_type& data) override {
        auto& val = cast<val_type>(data.val);
        return std::make_unique<Result>(1, val != 0);
    }
};

class FTRLEraseByShowOp : public ps::SparseTableEraseIfOperator<StorageType> {
public:
    typedef ps::SparseTableEraseIfOperator<StorageType> base_type;

    FTRLEraseByShowOp(const Configure& conf) : base_type(conf) {
        _threshold = conf.node()["decayed_show_filter_threshold"].as<double>();
    }

    bool erase_if(const feature_index_t&, const table_value_type& data) {
        auto& opt = cast<opt_type>(data.opt);
        return opt.decayed_show < _threshold;
    }
private:
    double _threshold;
};

void weight_decay(SparseTable table, double rate, double threshold) {
    Configure conf;
    conf.node()["show_decay_rate"] = rate;
    conf.node()["decayed_show_filter_threshold"] = threshold;

    auto& lr_table = *table.entity()->table_entry();
    auto decay_hid = lr_table.register_for_each("OP", "FTRLWeightDecayOp", conf);
    auto result    = lr_table.template for_each<Result>(decay_hid);

    pico_accumulator_erase({"total_feature_num", "effective_feature_num"});
    if (pico_comm_rank() == 0) {
        Accumulator<SumAggregator<int64_t>> fea_num_acc("total_feature_num", 1);
        Accumulator<SumAggregator<int64_t>> effective_fea_num_acc("effective_feature_num", 1);
        fea_num_acc.write(result.fea_num);
        effective_fea_num_acc.write(result.effective_fea_num);
    }

    auto erase_hid = lr_table.register_erase_if("OP", "FTRLEraseByShowOp", conf);
    lr_table.erase_if(erase_hid);
}

void count_effect(SparseTable table) {
    Configure conf;
    conf.node()["show_decay_rate"] = 0.0;
    conf.node()["decayed_show_filter_threshold"] = 0.0;

    auto& lr_table = *table.entity()->table_entry();
    auto decay_hid = lr_table.register_for_each("OP", "FTRLCountEffectOp", conf);
    auto result    = lr_table.template for_each<Result>(decay_hid);

    pico_accumulator_erase({"total_feature_num", "effective_feature_num"});
    if (pico_comm_rank() == 0) {
        Accumulator<SumAggregator<int64_t>> fea_num_acc("total_feature_num", 1);
        Accumulator<SumAggregator<int64_t>> effective_fea_num_acc("effective_feature_num", 1);
        fea_num_acc.write(result.fea_num);
        effective_fea_num_acc.write(result.effective_fea_num);
    }
}

} // namespace mle
} // namespace applications

namespace ps {

using FTRLWeightDecayOp = applications::mle::FTRLWeightDecayOp;
using FTRLEraseByShowOp = applications::mle::FTRLEraseByShowOp;
using FTRLCountEffectOp = applications::mle::FTRLCountEffectOp;
REGISTER_OPERATOR(OP, FTRLWeightDecayOp);
REGISTER_OPERATOR(OP, FTRLEraseByShowOp);
REGISTER_OPERATOR(OP, FTRLCountEffectOp);

} // namespace ps
} // namespace pico
} // namespace paradigm4
