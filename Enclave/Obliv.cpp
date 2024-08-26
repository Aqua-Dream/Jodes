#include "Obliv.h"
#include <functional>
#include <vector>

namespace obliv {

// return the largest y that y<=x and y is a power of 2 <= x
inline int prev_pow_two(int x) {
    if (x <= 2) return x;
    int y = 1;
    while (y <= x) y <<= 1;
    return y >>= 1;
}

class Obliv {
  public:
    virtual int isDummy(int i) = 0;

  protected:
    int size = 0;
    int* M = NULL;  // markers used for compact/partition
    inline int prev_pow_two(int x) {
        if (x <= 2)
            return x;
        int y = 1;
        while (y <= x)
            y <<= 1;
        return y >>= 1;
    }

    template <typename Compare, typename CSwap>
    void _bitonic_swap(int ascend, int i, int j, Compare compare, CSwap cswap) {
        int cond = compare(i, j) ^ ascend;
        cswap(i, j, cond);
    }

    template <typename Compare, typename CSwap>
    void _bitonic_merge(int ascend, int lo, int hi, Compare compare, CSwap cswap) {
        if (hi <= lo + 1)
            return;
        int mid_len = prev_pow_two(hi - lo - 1);
        for (int i = lo; i < hi - mid_len; i++)
            _bitonic_swap(ascend, i, i + mid_len, compare, cswap);
        _bitonic_merge(ascend, lo, lo + mid_len, compare, cswap);
        _bitonic_merge(ascend, lo + mid_len, hi, compare, cswap);
    }

    template <typename Compare, typename CSwap>
    void _bitonic_sort(int ascend, int lo, int hi, Compare compare, CSwap cswap) {
        int mid = (lo + hi) / 2;
        if (mid == lo)
            return;
        _bitonic_sort(!ascend, lo, mid, compare, cswap);
        _bitonic_sort(ascend, mid, hi, compare, cswap);
        _bitonic_merge(ascend, lo, hi, compare, cswap);
    }

    template <typename CSwap>
    void _oroffcompact(int lo, int hi, int z, CSwap cswap) {
        int n = hi - lo;
        if (n <= 1)
            return;
        if (n == 2) {
            int cond = ((!M[lo]) & M[lo + 1]) ^ z;
            cswap(lo, lo + 1, cond);
            return;
        }
        int _n = n >> 1;
        int m = 0;
        for (int i = lo; i < lo + _n; i++)
            m += M[i];
        int zleft = z & (_n - 1);
        int zright = (z + m) & (_n - 1);
        _oroffcompact(lo, lo + _n, zleft, cswap);
        _oroffcompact(lo + _n, hi, zright, cswap);
        int s = ((zleft + m) >= _n) ^ (z >= _n);
        for (int i = 0; i < _n; i++) {
            int cond = s ^ (i >= zright);
            cswap(i + lo, i + _n + lo, cond);
        }
    }

    template <typename CSwap>
    void _compact(int lo, int hi, CSwap cswap) {
        int n = hi - lo;
        if (n <= 1)
            return;
        int n1 = prev_pow_two(n);
        int n2 = n - n1;
        int m = 0;
        for (int i = lo; i < lo + n2; i++)
            m += M[i];
        if (n2 > 0)
            _compact(lo, lo + n2, cswap);
        _oroffcompact(lo + n2, hi, (n1 - n2 + m) % n1, cswap);
        for (int i = 0; i < n2; i++)
            cswap(lo + i, lo + i + n1, i >= m);
    }

    template <typename Compare, typename CSwap>
    void _partition_rec(int lo, int hi, int plo, int phi, int U, Compare compare, CSwap cswap) {
        if (phi - plo < 1)
            return;
        int pmid = (phi + plo) / 2;
        int left_padded_size = (pmid - plo + 1) * U;
        int left_num = 0;
        for (int i = lo; i < hi; i++)
            left_num += (!isDummy(i)) & compare(i, pmid);
        for (int i = lo; i < hi; i++) {
            int z = (left_num < left_padded_size) & isDummy(i);
            left_num += z;
            M[i] = z | (compare(i, pmid) & (!isDummy(i)));
        }
        _compact(lo, hi, cswap);
        _partition_rec(lo, lo + left_padded_size, plo, pmid, U, compare, cswap);
        _partition_rec(lo + left_padded_size, hi, pmid + 1, phi, U, compare, cswap);
    }

    template <typename Compare, typename CSwap>
    void _sort(int ascend, Compare compare, CSwap cswap) {
        if (size <= 1)
            return;
        _bitonic_sort(ascend, 0, size, compare, cswap);
    }

    template <typename CSwap>
    void _compact(std::vector<int>& _M, CSwap cswap) {
        if (_M.size() != size)
            throw;
        if (size <= 1)
            return;
        this->M = &_M[0];
        _compact(0, size, cswap);
    }

    template <typename Compare, typename CSwap>
    void _partition(int psize, int U, Compare compare, CSwap cswap) {
        M = new int[size];
        _partition_rec(0, size, 0, psize, U, compare, cswap);
        delete[] M;
    }
};

class IntObliv : public Obliv {
  public:
    const int dummy = 999999;

    IntObliv(std::vector<int>& input) : X(input) {
        size = input.size();
    }

    int isDummy(int i) {
        return X[i] == dummy;
    }

    void sort(int ascend = true) {
        _sort(
            ascend, [this](int i, int j) { return X[i] < X[j]; },
            [this](int i, int j, int cond) { 
                cswapInt(X[i], X[j], cond); });
    }

    void compact(std::vector<int>& M) {
        _compact(M, [this](int i, int j, int cond) {
                 cswapInt(X[i], X[j], cond); });
    }

    void partitionByPivots(std::vector<int>& pivots, int U) {
        int padded_size = (pivots.size() + 1) * U;
        X.insert(X.end(), padded_size - X.size(), dummy);
        size = padded_size;
        _partition(
            pivots.size(), U, [this, &pivots](int i, int j) { return X[i] < pivots[j]; },
            [this](int i, int j, int cond) { 
                cswapInt(X[i], X[j], cond); });
    }

    void shuffle(std::vector<int>& targets, int p, int U) {
        int padded_size = p * U;
        X.insert(X.end(), padded_size - X.size(), dummy);
        targets.insert(targets.end(), padded_size - targets.size(), 0);
        size = padded_size;
        _partition(
            p - 1, U, [this, &targets](int i, int j) { return targets[i] <= j; },
            [this, &targets](int i, int j, int cond) {
                cswapInt(X[i], X[j], cond);
                cswapInt(targets[i], targets[j], cond);
            });
    }

  private:
    std::vector<int>& X;
};

class TupleObliv : public Obliv {
  public:
    TupleObliv(std::vector<Tuple>& input) : X(input) {
        size = input.size();
    }

    inline int isDummy(int i) {
        return X[i].is_dummy;
    }

    void sortByCols(const std::vector<int>& columns, int ascend = true) {
        _sort(
            ascend, [this, &columns](int i, int j) { return X[i].less_in_cols(X[j], columns); },
            [this](int i, int j, int cond) { cswapTuple(X[i], X[j], cond); });
    }

    void sortByKey(std::vector<int>& key, int ascend = true) {
        _sort(
            ascend, [this, &key](int i, int j) { return key[i] < key[j]; },
            [this, &key](int i, int j, int cond) { 
                cswapTuple(X[i], X[j], cond); 
                cswapInt(key[i], key[j], cond); });
    }

    void compact(std::vector<int>& M) {
        _compact(M, [this](int i, int j, int cond) { cswapTuple(X[i], X[j], cond); });
    }

    void partitionByPivots(std::vector<Tuple>& pivots, std::vector<int>& columns, int U, Tuple dummy) {
        int padded_size = (pivots.size() + 1) * U;
        X.insert(X.end(), padded_size - size, dummy);
        size = padded_size;
        _partition(
            pivots.size(), U, [this, &pivots, &columns](int i, int j) { return X[i].less_in_cols(pivots[j], columns); },
            [this](int i, int j, int cond) { cswapTuple(X[i], X[j], cond); });
    }

    void shuffle(std::vector<int>& targets, int p, int U, Tuple dummy) {
        int padded_size = p * U;
        X.insert(X.end(), padded_size - size, dummy);
        targets.insert(targets.end(), padded_size - targets.size(), 0);
        size = padded_size;
        _partition(
            p - 1, U, [this, &targets](int i, int j) { return targets[i] <= j; },
            [this, &targets](int i, int j, int cond) {
                cswapTuple(X[i], X[j], cond);
                cswapInt(targets[i], targets[j], cond);
            });
    }

    void distribute(std::vector<int>& targets) {
        int m = size;
        for (int j = prev_pow_two(m - 1); j >= 1; j /= 2) {
            for (int i = m - j - 1; i >= 0; i--) {
                int cond = (targets[i] >= i + j) & (!isDummy(i));
                cswapTuple(X[i], X[i + j], cond);
                cswapInt(targets[i], targets[i + j], cond);
            }
        }
    }

  private:
    std::vector<Tuple>& X;
};

void sort(std::vector<Tuple>& X, const std::vector<int>& columns, bool ascend) {
    TupleObliv(X).sortByCols(columns, ascend);
}

void compact(std::vector<Tuple>& X, std::vector<int>& M) {
    TupleObliv(X).compact(M);
}

void shuffle_soda(std::vector<Tuple>& X, std::vector<int>& targets, int p, int U, Tuple dummy) {
    auto obl = TupleObliv(X);
    obl.sortByKey(targets);
    int padded_size = p * U;
    X.insert(X.end(), padded_size - X.size(), dummy);
    targets.insert(targets.end(), padded_size - targets.size(), p + 1);
    obl.distribute(targets);
}

void shuffle(std::vector<Tuple>& X, std::vector<int>& targets, int p, int U, Tuple dummy) {
    TupleObliv(X).shuffle(targets, p, U, dummy);
}

void partition(std::vector<Tuple>& X, std::vector<Tuple>& pivots, std::vector<int>& columns, int U, Tuple dummy) {
    TupleObliv(X).partitionByPivots(pivots, columns, U, dummy);
}

void distribute(std::vector<Tuple>& X, std::vector<int>& targets, bool sorted) {
    auto obl = TupleObliv(X);
    if (!sorted)
        obl.sortByKey(targets);
    obl.distribute(targets);
}

void distributeByCol(std::vector<Tuple>& X, int col) {
    int m = X.size();
    for (int j = prev_pow_two(m - 1); j >= 1; j /= 2) {
        for (int i = m - j - 1; i >= 0; i--) {
            int cond = (X[i].data[col] >= i + j) & (!X[i].is_dummy);
            cswapTuple(X[i], X[i + j], cond);
        }
    }
}

};  // namespace obliv

