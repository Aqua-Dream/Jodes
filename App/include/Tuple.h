#pragma once
#include <vector>
//TODO::: Delete this file when it has been totally moved to Enclave
class Tuple
{
public:
	static Tuple &DUMMY_TUPLE() { static Tuple tuple; tuple.is_dummy = true; return tuple;}
	bool is_dummy = false;
	Tuple(){}
	Tuple copy();
	Tuple(std::vector<int> &data_) : data(data_) {}

	std::vector<int>& getData();

	// dictionary comparison
	bool operator==(const Tuple &t) const; 
	bool operator<(const Tuple &t) const; 
	
	// [a,b,c,d,e].slice([2,4])=[c,e]
	Tuple getSubTuple(std::vector<int> index_list);

	long long int hash(int seed=0) const;

	Tuple concat(const Tuple &t) const;

private:
	std::vector<int> data;
};