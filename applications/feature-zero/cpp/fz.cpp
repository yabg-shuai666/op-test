#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "AssignIndexLayer.h"
#include "BlockFilterLayer.h"
#include "FilterBySignLayer.h"
#include "SplitBySignLayer.h"

#include "partition_by.h"
#include "join_op.h"
#include "transfer_data.h"
#include "sign_table.h"
#include "groupby_op.h"
#include "combine_op.h"

PYBIND11_MODULE(libfz, m) {
    using namespace paradigm4::pico::applications::mle;

    pybind11::class_<FZCache>(m, "FZCache")
        .def("memory_size", &FZCache::memory_size)
        .def("reset", &FZCache::reset);
    pybind11::class_<AggConf>(m, "AggConf")
        .def(pybind11::init<int, int64_t>());
    pybind11::class_<CombConf>(m, "CombConf")
        .def(pybind11::init<int, std::vector<int64_t>>());
    pybind11::class_<SignLengthTable>(m, "SignLengthTable")
        .def(pybind11::init<>())
        .def("pull",  &SignLengthTable::pull)
        .def("reset", &SignLengthTable::reset);
    pybind11::class_<SignCountTable>(m, "SignCountTable")
        .def(pybind11::init<>())
        .def("pull",  &SignCountTable::pull)
        .def("reset", &SignCountTable::reset);

    m.def("AssignIndex", AssignIndex_build, AssignIndex_doc.c_str());
    m.def("BlockFilter", BlockFilter_build, BlockFilter_doc.c_str());
    m.def("FilterBySign", FilterBySign_build, FilterBySign_doc.c_str());
    m.def("SplitBySign", SplitBySign_build, SplitBySign_doc.c_str());
    m.def("SignLengthStat", SignLengthStat_build, SignLengthStat_doc.c_str());
    m.def("SignCountStat", SignCountStat_build, SignCountStat_doc.c_str());
    m.def("Combine", Combine_build, Combine_doc.c_str());

    m.def("partition_by", partition_by);
    m.def("left_join", left_join);
    m.def("table_join", table_join);
    m.def("transfer_data", transfer_data);
    m.def("gc_memory_size", gc_memory_size);
    m.def("apply_ops", apply_ops);
    m.def("get_time_interval", get_time_interval);
}
