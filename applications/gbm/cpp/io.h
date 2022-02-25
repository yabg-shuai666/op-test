#ifndef PARADIGM4_PICO_APPLICATIONS_GBM_IO_H
#define PARADIGM4_PICO_APPLICATIONS_GBM_IO_H

#include "layer/layer.h"
#include "graph/node.h"
#include "executor/session.h"

#include "common.h"
#include "tree.h"

namespace paradigm4 {
namespace pico {
namespace applications {
namespace mle {

template <typename T>
PicoJsonNode list_json(const T& v) {
    PicoJsonNode l;
    l.push_back(v);
    return l;
}

bool tree_to_json(const std::vector<tree_node_t>& nodes, PicoJsonNode& json) {
    PicoJsonNode tree;
    for (size_t i = 0; i < nodes.size(); ++i) {
        PicoJsonNode one;
        auto& node = nodes[i];
        one.add("node_index", i);
        one.add("ins_num",       node.ins_num);
        one.add("valid_num",     node.valid_num);
        one.add("depth",         node.depth);
        one.add("is_leaf",       node.is_leaf != 0);
        one.add("weight",        list_json(node.weight));
        one.add("weight_hex",    list_json(fmt("%la", node.weight)));
        one.add("label_sum",     list_json(node.label_sum));
        one.add("label_sum_hex", list_json(fmt("%la", node.label_sum)));
        if (!node.is_leaf) {
            std::string fea = fmt("%lld:%lld", node.fea.slot, node.fea.sign);
            one.add("feature",    fea);
            one.add("lson",       node.son[0]);
            one.add("rson",       node.son[1]);
            one.add("gain",       node.gain);
            one.add("gain_hex",   fmt("%la", node.gain));
            one.add("is_nominal", node.is_discrete != 0);
            if (!node.is_discrete) {
                one.add("condition",     node.cond);
                one.add("condition_hex", fmt("%la", node.cond));
                one.add("defv_side",     node.son[node.defv]);
            }
        }
        tree.push_back(one);
    }
    json.add("type", "dtree");
    json.add("tree", tree);
    return true;
}

bool dump_trees(const std::vector<Tree>& trees, const std::string& uri) {
    FileSystem::mkdir_p(uri + "/local", P_CONF.hadoop_bin);
    auto path = uri + "/local/model";

    PicoJsonNode model;
    for (size_t i = 0; i < trees.size(); ++i) {
        PicoJsonNode tree;
        if (!tree_to_json(trees[i].entity()->nodes(), tree))
            return false;
        tree.add("learner_index", i);
        model.push_back(tree);
    }

    PicoJsonNode sink;
    sink.add("models", model);
    sink.add("n_learner", trees.size());

    auto fp = core::ShellUtility::open_write(path, "", P_CONF.hadoop_bin);
    return sink.save(fp, true);
}

Tree json_to_tree(const PicoJsonNode& model_json) {
    auto tree_json = model_json.at("tree");
    auto n_nodes = tree_json.size();

    Tree tree(0, 0, 0, 0, 0, 0);
    auto& tree_nodes = tree.entity()->nodes();
    tree_nodes.clear();
    tree_nodes.reserve(n_nodes);
    for(size_t i = 0; i < n_nodes; ++i) {
        auto node_json = tree_json.at(i);
        SCHECK(i == node_json.at("node_index").as<size_t>()) << " node index error";

        int64_t ins_num = node_json.at("ins_num").as<int64_t>();
        int64_t valid_num = node_json.at("valid_num").as<int64_t>();
        int32_t depth = node_json.at("depth").as<int32_t>();
        int32_t is_leaf = static_cast<int32_t>(node_json.at("is_leaf").as<bool>());
        std::vector<double> weight = hex_to_vec(node_json.at("weight_hex"));
        std::vector<double> label_sum = hex_to_vec(node_json.at("label_sum_hex"));

        tree_node_t tree_node(0, ins_num, valid_num, depth, is_leaf, weight[0]);
        tree_node.label_sum = label_sum[0];
        auto parse_feature_index_t = [](const std::string& s) {
            std::stringstream ss(s);
            int64_t slot, sign;
            char c;
            ss >> slot >> c >> sign;
            return feature_index_t(slot, sign);
        };
        if (tree_node.is_leaf == false) {
            tree_node.is_discrete = node_json.at("is_nominal").as<bool>();
            tree_node.fea = parse_feature_index_t(node_json.at("feature").as<std::string>());
            tree_node.son[0] = node_json.at("lson").as<int32_t>();
            tree_node.son[1] = node_json.at("rson").as<int32_t>();
            tree_node.gain = pico_lexical_cast<double>(node_json.at("gain_hex").as<std::string>());
            if (tree_node.is_discrete == false) {
                tree_node.cond = pico_lexical_cast<double>(node_json.at("condition_hex").as<std::string>());
                tree_node.defv = tree_node.son[1] == node_json.at("defv_side").as<int32_t>();
            }
        }
        tree_nodes.push_back(tree_node);
    }
    return tree;
}

std::vector<Tree> load_trees(const std::string& uri) {
    auto path = uri + "/local/model";
    auto fp = core::ShellUtility::open_read(path, "", P_CONF.hadoop_bin);

    std::vector<Tree> ret;
    PicoJsonNode model;
    model.load(fp);

    for (auto& single : model.at("models"))
        ret.push_back(json_to_tree(single));
    return ret;
}

} // namespace mle
} // namespace applications
} // namespace pico
} // namespace paradigm4

#endif
