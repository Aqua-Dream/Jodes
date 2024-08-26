#include <climits>
#include <functional>
#include "Enclave.h"
#include "Obliv.h"
#include "Tuple.h"
#include "define.h"

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
        int cond = a.equal_in_cols(b, group_by_columns);
        int& b_val = b.data[aggregate_column];
        obliv::cmove(b_val, b_val + a.data[aggregate_column], cond);
        return cond;
    }
};

class OperatorMul : public AssociateOperator {
  public:
    OperatorMul(std::vector<int> _group_by_columns, int _aggregate_column) : AssociateOperator(_group_by_columns, _aggregate_column, 1, MUL) {}

    bool apply(Tuple& a, Tuple& b) {
        int cond = a.equal_in_cols(b, group_by_columns);
        int& b_val = b.data[aggregate_column];
        obliv::cmove(b_val, b_val * a.data[aggregate_column], cond);
        return cond;
    }
};

class OperatorMax : public AssociateOperator {
  public:
    OperatorMax(std::vector<int> _group_by_columns, int _aggregate_column) : AssociateOperator(_group_by_columns, _aggregate_column, INT_MIN, MAX) {}

    bool apply(Tuple& a, Tuple& b) {
        int cond = a.equal_in_cols(b, group_by_columns);
        int& b_val = b.data[aggregate_column];
        int a_val = a.data[aggregate_column];
        obliv::cmove(b_val, a_val, cond & (a_val > b_val));
        return cond;
    }
};

class OperatorMin : public AssociateOperator {
  public:
    OperatorMin(std::vector<int> _group_by_columns, int _aggregate_column) : AssociateOperator(_group_by_columns, _aggregate_column, INT_MAX, MIN) {}

    bool apply(Tuple& a, Tuple& b) {
        int cond = a.equal_in_cols(b, group_by_columns);
        int& b_val = b.data[aggregate_column];
        int a_val = a.data[aggregate_column];
        obliv::cmove(b_val, a_val, cond & (a_val < b_val));
        return cond;
    }
};

// copy the previous tuple to this one if this tuple is non-dummy
class OperatorCopy : public AssociateOperator {
  public:
    OperatorCopy() {}

    bool apply(Tuple& a, Tuple& b) {
        obliv::cmove(b, a, b.is_dummy);
        return b.is_dummy;
    }
};