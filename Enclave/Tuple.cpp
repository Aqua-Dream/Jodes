#include "Tuple.h"
#include <cassert>
#include <cstring>
#include <iostream>
#include <memory>
#include <sstream>
#include "Enclave.h"
#include "define.h"

// std::vector<int> &Tuple::data
// {
//     assert(!is_dummy && "Trying to visit dummy tuple!");
//     return data;
// }

Tuple::Tuple(const char* buffer, int num_columns) {
    data.resize(num_columns);
    memcpy(&data[0], buffer, num_columns * VAL_LENGTH);
    memcpy(&is_dummy, buffer + num_columns * VAL_LENGTH, VAL_LENGTH);
}

void Tuple::set_data(std::vector<int> data_)
{
    data = data_;
}


void Tuple::serialize(char* buffer) {
    memcpy(buffer, &data[0], size() * VAL_LENGTH);
    memcpy(buffer + size() * VAL_LENGTH, &is_dummy, VAL_LENGTH);
}

Tuple Tuple::copy() {
    std::vector<int> _data(this->data);
    return Tuple(_data, is_dummy);
}

void Tuple::add_val(int val) {
    data.push_back(val);
}

void Tuple::insert_val(int val, int pos) {
    data.insert(data.begin() + pos, val);
}

void Tuple::delete_col(int pos) {
    if (pos < data.size()) {
        data.erase(data.begin() + pos);
    }
}

void Tuple::setTupleDummy(){
    // for(int i = 0; i < data.size(); i++) {
    //     data[i] = DUMMY_VAL;
    // }
    is_dummy = true;
}

/* return whether the two tuples equal in input columns. Ignore dummy. */
int Tuple::equal_in_cols(Tuple& tup, const std::vector<int>& columns) {
    int ret = true;
    int size = data.size();
    std::vector<int>& tup_data = tup.data;
    int tup_size = tup_data.size();
    for (int i : columns) {
        ret &= data[i] == tup_data[i];
    }

    return ret;
}

/* return whether the first tuple less than the second in input columns */
// dummy > non-dummy
int Tuple::less_in_cols(const Tuple& tup, const std::vector<int>& columns) const {
    int ret = (!is_dummy) & tup.is_dummy;
    int all_eq = !(is_dummy ^ tup.is_dummy);
    for (auto col : columns) {
        ret |= all_eq & (data[col] < tup.data[col]);
        all_eq &= data[col] == tup.data[col];
    }
    return ret;
}

long long int Tuple::hash(const std::vector<int>& columns, int seed) const {
    long long int res = columns.size();
    for (auto& i : columns)
        res ^= data[i] + 0x9e3779b99e3779b9 + (res << 6) + (res >> 2);
    res ^= seed + 0x9e3779b99e3779b9 + (res << 6) + (res >> 2);
    return res;
}

std::string Tuple::show() {
    std::string ss;
    if (is_dummy) {
        /* In files, dummy is only one column no matter how big m_num_columns is */
        ss = DUMMY_STR;
    } else {
        for (int i = 0; i < data.size(); ++i) {
            ss += std::to_string(data[i]) + " ";
        }
        ss.erase(ss.length() - 1);
    }

    return ss;
}

// std::vector<int> deserialize(const std::string& str) {
//     std::vector<int> numbers;
//     std::stringstream ss(str);
//     int num;
//     while (ss >> num) {
//         numbers.push_back(num);
//     }
//     return numbers;
// }