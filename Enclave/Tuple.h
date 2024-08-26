#pragma once
#include <string>
#include <vector>
#include "define.h"

//TODO::: Delete this file when it has been totally moved to Enclave
class Tuple {
  public:
    std::vector<int> data;
    // static Tuple genDummy(int num_columns) {
    //     Tuple tuple;
    //     tuple.is_dummy = true;
	// 	tuple.data.resize(num_columns);
    //     return tuple;
    // }

	static inline int rowLength(int num_columns) {
		return (num_columns+1) * VAL_LENGTH;
	}

    int is_dummy = false;
    Tuple() {}
    Tuple copy();
    Tuple(std::vector<int>& data_) : data(data_) {}
    Tuple(std::vector<int>& data_, bool is_dummy_) : data(data_), is_dummy(is_dummy_) {}
	Tuple(const char* buffer, int num_columns);

    //std::vector<int>& getData();
    inline int size() const { return data.size(); }

    long long int hash(const std::vector<int>& columns, int seed = 0) const;

    // Tuple concat(const Tuple& t) const;

	// serialize to pointer to the buffer
    void serialize(char *buffer);

    std::string show();

    void add_val(int val);

    void insert_val(int val, int pos);

    void delete_col(int pos);
    void set_data(std::vector<int> data_);

    int equal_in_cols(Tuple& tup, const std::vector<int> &columns);
	int less_in_cols(const Tuple& tup, const std::vector<int>& columns) const;

    void setTupleDummy();
};