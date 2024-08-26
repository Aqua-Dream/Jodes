#pragma once

#include <vector>
#include "Tuple.h"

namespace obliv {

inline void cmove(int& x, int v, int cond) {
    x ^= (-cond) & (v ^ x);
}

inline void cmove(Tuple& x, const Tuple& y, int cond) {
    for (int i = 0; i < x.size(); i++)
        cmove(x.data[i], y.data[i], cond);
    cmove(x.is_dummy, y.is_dummy, cond);
}

inline void cswapInt(int& x, int& y, int cond) {
    int xored = (-cond) & (x ^ y);
    x ^= xored;
    y ^= xored;
}

inline void cswapTuple(Tuple& x, Tuple& y, int cond) {
    for (int i = 0; i < x.size(); i++)
        cswapInt(x.data[i], y.data[i], cond);
    cswapInt(x.is_dummy, y.is_dummy, cond);
}

void sort(std::vector<Tuple>& X, const std::vector<int> &columns, bool ascend = true);
void compact(std::vector<Tuple>& D, std::vector<int>& M);
void shuffle(std::vector<Tuple>& X, std::vector<int>& targets, int p, int U, Tuple dummy);
void shuffle_soda(std::vector<Tuple>& X, std::vector<int>& targets, int p, int U, Tuple dummy);
void partition(std::vector<Tuple>& X, std::vector<Tuple>& pivots, std::vector<int>& columns, int U, Tuple dummy);

/*
    sorted: Indicate whether the input has been sorted by targets
    Please make sure that dummy tuples are associated with largest targets
*/
void distribute(std::vector<Tuple>& X, std::vector<int>& targets, bool sorted);
void distributeByCol(std::vector<Tuple>& X, int col);

};  // namespace obliv