#include <cassert>
#include <string>
#include <vector>
#include "Tuple.h"
// #include "Operators.h"
#include <random>
#include "Enclave.h"
#include "Operators.h"

class LocalTable {
public:
  LocalTable(int global_id_, int id_, uint8_t* file, size_t file_length);
  LocalTable(int global_id_, int id_, std::vector<Tuple>& tuples) : global_id(global_id_), id(id_), m_tuples(tuples) {}

  const int size();
  const int num_columns();
  // std::vector<Tuple> &getData();
  // long long int hash(int seed = 0) const;
  LocalTable* copy(int new_global_id);
  void print(int limit_size, bool show_dummy = false);
  void mvJoinColsAhead(int num_partitions, const std::vector<int> join_cols);

  // int getSizeBound(int num_partitions);
  void shuffle_non_obliv(int num_partitions, std::vector<int>& index_list);
  // void shuffle(int num_partitions, std::vector<int> &index_list, int size_bound);
  void shuffle(int num_partitions, std::vector<int>& index_list, int size_bound);
  void randomShuffle(int num_partitions);
  void shuffleByKey(int num_partitions, const std::vector<int>& key, int seed, int size_bound);
  void shuffleByCol(int num_partitions, int i_col_id, int size_bound);
  void shuffleMerge(int num_partitions);
  void refresh();

  void groupByAggregateBase(int num_partitions, AssociateOperator* op, bool doPrefix, int phase, bool reverse);
  void getPivots(int num_partitions, const std::vector<int>& columns);
  void refreshAndMerge(int num_partitions);
  void sortMerge(int num_partitions, const std::vector<int>& columns);
  void localSort(int num_partitions, const std::vector<int>& columns);
  void pad_to_size(int num_partitions, int n);

  void foreignTableModifyColZ(int num_partitions, const std::vector<int>& columns);
  void finalizePkjoinResult(int num_partitions, const std::vector<int>& columns, int ori_r_col_num, int r_align_col_num);
  void addCol(int num_partitions, int defaultVal, int col_index);
  void copyCol(int num_partitions, int col_index);
  void expansion_prepare(int num_partitions, int d_index);
  void add_and_calculate_col_t_p(int d_index, int m);
  void expansion_distribute_and_clear(int num_partitions, int m);
  void deleteCol(int num_partitions, int col_index);
  void expansion_suffix_sum(int num_partitions, int phase);
  long long sum(int column);
  // void shuffle(int num_partitions, const std::vector<int> key, int seed, std::vector<std::vector<Tuple>> &output);
  // sort table by columns (dictionary order); dummy tuples are always moved to the end
  void sort(const std::vector<int>& columns);
  // // Note: this project operation does not elimiate duplicate tuples!
  // // Note2: if project the same column for multiple times, then getColumnIdsByNames may not work correctly due to duplicate names
  void project(const std::vector<int>& columns);
  // void appendColumn(int default_value);
  // void removeDummy();
  // long long sum(int column);
  // // Please ensure that the tuples are already sorted by the group by columns; return the last non-dummy tuple
  // Tuple groupByPrefixAggregate(AssociateOperator &op);
  // // Which is used for groupByPrefixAggregate only
  void _addValueByKey(AssociateOperator* op);
  void partitionByPivots(std::vector<int>& columns, int num_partitions, int size_bound);
  void shuffleWrite(int num_partitions, std::vector<std::vector<Tuple>>& output);
  void pkJoinCombine(int num_partitions, int ori_r_col_num, int r_align_col_num, std::vector<int>& combine_sort_cols, std::vector<int>& join_cols, LocalTable& s_table_local);
  void joinComputeAlignment(int m);
  void joinFinalCombine(LocalTable& r_table, int num_join_cols);

  std::vector<Tuple> getTuples();
  int getNumRows();
  // must be called from the last server to the first server
  void remove_dup_after_prefix(int num_partitions, std::vector<int>& columns);

  int max(int column);
  void union_table(LocalTable& other_table);
  long long SODA_step1(std::vector<int>& columns, int num_partitions, int aggCol);
  void SODA_step2(int num_partitions, int p);
  void SODA_step3(int num_partitions, int p);
  int localJoin(LocalTable& other_table, int num_cols, int output_size);
  void SODA_step5(int num_partitions);
  void assignColE(int num_partitions, int col);

  /* For benchmark usage */
  void generateData(int num_cols, int num_rows);
  void opaque_prepare_shuffle_col(int num_partitions, int col_id, int tuple_num);
  void soda_shuffleByKey(int num_partitions, const std::vector<int>& key, int seed, int size_bound);

private:
  int global_id;  //the global table's id it belongs to
  int id;
  // std::string filePath;
  std::vector<Tuple> m_tuples;
  int m_num_columns = 0;
  // int m_num_rows = 0; //it does not take dummy rows into account
  bool isKeyUnique(const std::vector<int>& key);
};

const char* serialize_tuple_vector(std::vector<Tuple>& tuple_list, int col_num, size_t* ser_length, int* ser_row_num);
bool TupleCompare(Tuple& tl, Tuple& tr, const std::vector<int>& columns);