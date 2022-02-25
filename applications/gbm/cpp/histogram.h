#ifndef PARADIGM4_PICO_APPLICATIONS_GBM_HISTOGRAM_H
#define PARADIGM4_PICO_APPLICATIONS_GBM_HISTOGRAM_H

#include "common/common.h"
#include "table/table.h"

namespace paradigm4 {
namespace pico {
namespace applications {
namespace mle {

struct agg_t {
    double g;
    double h;
    int64_t n;

    agg_t() : agg_t(0, 0, 0) {};
    agg_t(double g, double h) : agg_t(g, h, 1) {}
    agg_t(double g, double h, int64_t n):  g(g), h(h), n(n) {}

    void operator += (const agg_t& rhs) {
        g += rhs.g;
        h += rhs.h;
        n += rhs.n;
    }

    void operator -= (const agg_t& rhs) {
        g -= rhs.g;
        h -= rhs.h;
        n -= rhs.n;
    }

    agg_t operator + (const agg_t& rhs) const {
        return agg_t(g + rhs.g, h + rhs.h, n + rhs.n);
    }

    agg_t operator - (const agg_t& rhs) const {
        return agg_t(g - rhs.g, h - rhs.h, n - rhs.n);
    }

    std::string to_string() {
        return fmt("[%.3f, %.3f, %lld]", g, h, n);
    }

    PICO_SERIALIZATION(g, h, n);
};

typedef std::vector<agg_t> hist_t;

class HistogramEntity : public TableEntity {
public:
    bool load(const URIConfig& ) override { return false; }

    void clear_storage() {
        _hist.clear();
        _data.clear();
    }

    void init(int fea_size, int node_size, int max_n) {
        _fsize = fea_size;
        _nsize = node_size;
        _max_n = max_n + 1;
    }

    void initialize(int tnum, bool) override {
        _hist.clear();
        _hist.resize(tnum);
        for (auto& h1 : _hist)
            h1.resize(_nsize);
    }

    void store() override {
        ThreadGroup tg(P_CONF.process.cpu_concurrency);
        pico_parallel_run([this](int, int tid) {
            for (int i = tid; i < _nsize; i += P_CONF.process.cpu_concurrency) {
                for (size_t tid = 1; tid < _hist.size(); ++tid) {
                    merge(_hist[tid][i], _hist[0][i]);
                }
            }
        }, tg);

        _data.resize(_nsize);
        for (int i = 0; i < _nsize; ++i)
            pico_all_reduce(_hist[0][i], _data[i], merge);

        // for (int i = 0; i < _nsize; ++i) {
        //     std::stringstream ss;
        //     ss << i;
        //     for (size_t j = 0; j < _data[i].size(); ++j) {
        //         if (j % _max_n == 0) ss << "\n";
        //         ss << _data[i][j].to_string() << ' ';
        //     }
        //     SLOG(INFO) << ss.str();
        // }
    }

    hist_t& cache(int tid, int nidx) {
        if (_hist[tid][nidx].size() == 0)
            _hist[tid][nidx].resize(_fsize * _max_n);
        return _hist[tid][nidx];
    }

    const agg_t* hist_ptr(int nidx, int fea) const {
        SCHECK(_data[nidx].size() > 0) << nidx << ' ' << fea;
        return _data[nidx].data() + fea * _max_n;
    }

    hist_t& histogram(int nidx) {
        return _data[nidx];
    }

    int fea_size() const {
        return _fsize;
    }

    int max_n() const {
        return _max_n;
    }

private:
    static void merge(const hist_t& in, hist_t& out) {
        if (in.size() == 0) return;
        if (out.size() == 0) {
            out = in;
            return;
        }
        SCHECK(in.size() == out.size());
        for (size_t i = 0; i < in.size(); ++i)
            out[i] += in[i];
    }

    std::vector<std::vector<hist_t>> _hist;
    std::vector<hist_t> _data;
    int _fsize = -1, _nsize = -1, _max_n = -1;
};

class Histogram {
public:
    Histogram() {
        _entity = std::make_shared<HistogramEntity>();
    }

    std::shared_ptr<HistogramEntity> entity() {
        return _entity;
    }

    void init(int fea_size, int node_size, int max_n) {
        _entity->init(fea_size, node_size, max_n);
    }

    void clear() {
        _entity->clear_storage();
    }

private:
    std::shared_ptr<HistogramEntity> _entity;
};

} // namespace mle
} // namespace applications
} // namespace pico
} // namespace paradigm4

#endif
