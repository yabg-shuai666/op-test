#ifndef PARADIGM4_PICO_APPLICATIONS_GBM_DENSE_ARRAY_H
#define PARADIGM4_PICO_APPLICATIONS_GBM_DENSE_ARRAY_H

#include "pico-ds/pico-ds/common/pico_container_divide.h"
#include "common/common.h"

namespace paradigm4 {
namespace pico {
namespace applications {
namespace mle {

struct DENSE_ARRAY_8 {
    static const uint8_t type = 100;
    static const uint8_t miss = 255;
    typedef uint8_t value_t;

    static std::shared_ptr<Schema> schema() {
        return std::make_shared<Schema>(DType(type));
    }
};

struct DENSE_ARRAY_32 {
    static const uint8_t type = 101;
    static const int32_t miss = -1;
    typedef int32_t value_t;

    static std::shared_ptr<Schema> schema() {
        return std::make_shared<Schema>(DType(type));
    }
};

const int COMPRESS_BOUND = 255;

class DenseArray {
public:
    size_t size() const {
        return _size;
    }

    template <typename value_t = uint8_t>
    void resize(size_t row, size_t col) {
        _size = row;
        _data.resize(row * col * sizeof(value_t));
    }

    template <typename value_t = uint8_t>
    value_t* data(size_t i = 0) {
        return (value_t*)(&_data[_data.size() / _size * i]);
    }

    template <typename value_t = uint8_t>
    const value_t* data(size_t i = 0) const {
        return (const value_t*)(&_data[_data.size() / _size * i]);
    }

    template <typename value_t = uint8_t>
    size_t num_columns() const {
        return _data.size() / _size / sizeof(value_t);
    }

    void cut(size_t num, DenseArray& tail) {
        tail.clear();
        if (num >= _size) return;

        auto col = num_columns();
        tail.resize(_size - num, col);

        std::memcpy(tail.data(), data(num), tail._data.size());
        resize(num, col);
    }

    void merge(const DenseArray& tail) {
        if (tail.size() == 0) return;
        resize(_size + tail._size, tail.num_columns());
        std::memcpy(data(_size - tail._size), tail.data(), tail._data.size());
    }

    void clear() {
        _data.clear();
        _size = 0;
    }

private:
    size_t _size = 0;
    std::vector<uint8_t> _data;
    PICO_SERIALIZE(_size, _data);
};

REGISTER_VARIABLE_TYPE(DenseArray);

} // namespace mle
} // namespace applications
} // namespace pico
} // namespace paradigm4

PICO_CONTAINER_FUNC(DenseArray);

#endif
