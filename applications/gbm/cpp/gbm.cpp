#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "histogram.h"
#include "node_index.h"
#include "top_category.h"
#include "io.h"
#include "GBMParserLayers.h"
#include "TreeLayers.h"
#include "HistogramLayers.h"

PYBIND11_MODULE(libgbmtools, m) {
    using namespace paradigm4::pico::applications::mle;

    pybind11::class_<NodeIndex>(m, "NodeIndex")
        .def(pybind11::init<int>())
        .def("clear", &NodeIndex::clear)
        .def("reset_value", &NodeIndex::reset_value)
        .def("dim", &NodeIndex::dim);

    pybind11::class_<Histogram>(m, "Histogram")
        .def(pybind11::init<>())
        .def("init", &Histogram::init)
        .def("clear", &Histogram::clear);

    pybind11::class_<Tree>(m, "Tree")
        .def(pybind11::init<double, int64_t, double, double, double, double>())
        .def("depth", &Tree::depth)
        .def("leaf_num", &Tree::leaf_num)
        .def("node_num", &Tree::node_num)
        .def("extend", &Tree::extend);

    pybind11::class_<TopCategoryTable>(m, "TopCategoryTable")
        .def(pybind11::init<int>())
        .def("reset", &TopCategoryTable::reset)
        .def("slots", &TopCategoryTable::slots)
        .def("max_n", &TopCategoryTable::max_n);

    m.def("InstanceBlock2DenseArray",  InstanceBlock2DenseArray_build,  InstanceBlock2DenseArray_doc.c_str());
    m.def("InstanceBlock2SparseArray", InstanceBlock2SparseArray_build, InstanceBlock2SparseArray_doc.c_str());
    m.def("GetNodeIndex", GetNodeIndex_build, GetNodeIndex_doc.c_str());
    m.def("SetNodeIndex", SetNodeIndex_build, SetNodeIndex_doc.c_str());
    m.def("CalcNodeIndex", CalcNodeIndex_build, CalcNodeIndex_doc.c_str());
    m.def("HistogramStat", HistogramStat_build, HistogramStat_doc.c_str());
    m.def("UpdateWeights", UpdateWeights_build, UpdateWeights_doc.c_str());
    m.def("TopCategoryStat", TopCategoryStat_build, TopCategoryStat_doc.c_str());
    m.def("TreeWeights", TreeWeights_build, TreeWeights_doc.c_str());

    m.def("dump_trees", dump_trees);
    m.def("load_trees", load_trees);
}
