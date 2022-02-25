#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "ps_set.h"
#include "PSSetLayer.h"
#include "DFFilterLayer.h"

PYBIND11_MODULE(libhctools, m) {
    using namespace paradigm4::pico::applications::mle;

    pybind11::class_<PSSet>(m, "PSSet")
        .def(pybind11::init<>())
        .def("reset", &PSSet::reset);

    m.def("PSSetPush", PSSetPush_build, PSSetPush_doc.c_str());
    m.def("PSSetFilter", PSSetFilter_build, PSSetFilter_doc.c_str());
    m.def("DFFilter", DFFilter_build, DFFilter_doc.c_str());
    m.def("DFSplit", DFSplit_build, DFSplit_doc.c_str());
}
