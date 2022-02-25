#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "weight_decay.h"

namespace paradigm4 {
namespace pico {
namespace applications {
namespace mle {

} // namespace mle
} // namespace applications
} // namespace paradigm4
} // namespace pico

PYBIND11_MODULE(liblrtools, m) {
    using namespace paradigm4::pico::applications::mle;

    m.def("weight_decay", weight_decay);
    m.def("count_effect", count_effect);
}
