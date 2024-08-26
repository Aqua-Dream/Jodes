#pragma once
#include <climits>
#include <functional>
#include "Tuple.h"

class AssociateOperator {
  public:
    enum OperatorList {
        ADD,
        MUL,
        MAX,
        MIN,
        COPY
    };
    int zero;
    OperatorList op_id;
    AssociateOperator(){};
    AssociateOperator(std::vector<int> _group_by_columns, int _aggregate_column, int _zero, OperatorList _op_id) : group_by_columns(_group_by_columns), aggregate_column(_aggregate_column), zero(_zero), op_id(_op_id) {}
    virtual bool apply(Tuple& a, Tuple& b) = 0;  // return whether the operator applied (Tuple b is changed)
    std::vector<int> group_by_columns;
    int aggregate_column;
};

class OperatorAdd : public AssociateOperator {
  public:
    OperatorAdd(std::vector<int> _group_by_columns, int _aggregate_column) : AssociateOperator(_group_by_columns, _aggregate_column, 0, ADD) {}

    bool apply(Tuple& a, Tuple& b) {
        return false;
    }
};

class OperatorMul : public AssociateOperator {
  public:
    OperatorMul(std::vector<int> _group_by_columns, int _aggregate_column) : AssociateOperator(_group_by_columns, _aggregate_column, 1, MUL) {}

    bool apply(Tuple& a, Tuple& b) {
        return false;
    }
};

class OperatorMax : public AssociateOperator {
  public:
    OperatorMax(std::vector<int> _group_by_columns, int _aggregate_column) : AssociateOperator(_group_by_columns, _aggregate_column, INT_MIN, MAX) {}

    bool apply(Tuple& a, Tuple& b) {
        return false;
    }
};

class OperatorMin : public AssociateOperator {
  public:
    OperatorMin(std::vector<int> _group_by_columns, int _aggregate_column) : AssociateOperator(_group_by_columns, _aggregate_column, INT_MAX, MIN) {}

    bool apply(Tuple& a, Tuple& b) {
        return false;
    }
};

class OperatorCopy : public AssociateOperator {
  public:
    OperatorCopy() {}

    bool apply(Tuple& a, Tuple& b) {
        return false;
    }
};