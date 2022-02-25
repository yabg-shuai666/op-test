#ifndef PARADIGM4_PICO_APPLICATIONS_GBM_COMMON_H
#define PARADIGM4_PICO_APPLICATIONS_GBM_COMMON_H

#include <limits>

namespace paradigm4 {
namespace pico {
namespace applications {
namespace mle {

const double GBM_COMPARE_EPS = std::numeric_limits<double>::epsilon();

inline bool fequal(double a, double b) {
    return std::fabs(a - b) < GBM_COMPARE_EPS;
}

inline bool flessq(double a, double b) {
    return a <= b + GBM_COMPARE_EPS;
}

inline bool fless(double a, double b) {
    return a < b - GBM_COMPARE_EPS;
}

inline bool fgreater(double a, double b) {
    return !flessq(a, b);
}

inline bool fgreaterq(double a, double b) {
    return !fless(a, b);
}

inline bool fzero(double x) {
    return std::fabs(x) < GBM_COMPARE_EPS;
}

inline std::vector<double> hex_to_vec(PicoJsonNode& node) {
    std::vector<double> result(node.size());
    for(size_t i = 0; i < result.size(); ++i) {
        result[i] = pico_lexical_cast<double>(node.at(i).as<std::string>());
    }
    return result;
}

} // namespace mle
} // namespace applications
} // namespace pico
} // namespace paradigm4

#endif
