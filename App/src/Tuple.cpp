#include <cassert>
#include "Tuple.h"

std::vector<int> &Tuple::getData()
{
    assert(!is_dummy && "Trying to visit dummy tuple!");
    return data;
}

Tuple Tuple::copy()
{
    std::vector<int> _data(this->data);
    return Tuple(_data);
}

bool Tuple::operator==(const Tuple &t) const
{
    if (is_dummy || t.is_dummy)
        return is_dummy && t.is_dummy;
    int size = (int)data.size();
    assert(size == (int)t.data.size() && "Tuples not comparable!");
    for (int i = 0; i < size; i++)
        if (data[i] != t.data[i])
            return false;
    return true;
}

bool Tuple::operator<(const Tuple &t) const
{
    if (is_dummy)
        return false;
    if (t.is_dummy)
        return true;
    int size = (int)data.size();
    assert(size == (int)t.data.size() && "Tuples not comparable!");
    for (int i = 0; i < size; i++)
    {
        if (data[i] < t.data[i])
            return true;
        else if (data[i] > t.data[i])
            return false;
    }
    return false;
}

Tuple Tuple::getSubTuple(std::vector<int> index_list)
{
    Tuple new_tuple;
    if (is_dummy)
    {
        new_tuple.is_dummy = true;
        return new_tuple;
    }
    for (auto id : index_list)
        new_tuple.data.push_back(data[id]);
    return new_tuple;
}

long long int Tuple::hash(int seed) const
{
    if (is_dummy)
        return 0;
    long long int res = data.size();
    for (auto &i : data)
        res ^= i + 0x9e3779b99e3779b9 + (res << 6) + (res >> 2);
    res ^= seed + 0x9e3779b99e3779b9 + (res << 6) + (res >> 2);
    return res;
}

Tuple Tuple::concat(const Tuple &t) const // concatenate
{
    assert(!is_dummy && !t.is_dummy && "Trying to concatenate dummy tuples!");
    Tuple ret;
    ret.data = this->data; // vector copy happens
    ret.data.insert(ret.data.end(), t.data.begin(), t.data.end());
    return ret;
}
