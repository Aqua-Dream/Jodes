#include <cassert>
#include <random>
#include <string>
#include <vector>
#include "Operators.h"
#include "Tuple.h"

class LocalTable {
public:
  LocalTable(int global_id_, int id_, std::string filePath, bool is_distributed = false, std::string url = "");
  // LocalTable(int global_id_, int id_, std::vector<Tuple> &tuples, std::string filePath);

  LocalTable() {};
  LocalTable(int global_id_, int id_) : LocalTable(global_id_, id_, "") {};
  LocalTable(int global_id_, int id_, bool is_distributed) : LocalTable(global_id_, id_, "", is_distributed) {};

  void pkJoinCombine(int s_table_local_global_id, int s_table_local_id, std::vector<int>& combine_sort_cols, std::vector<int>& new_join_cols, int ori_r_col_num, int r_align_col_num);
  void mvJoinColsAhead(std::vector<int> join_cols);
  void joinComputeAlignment(int m);
  void joinFinalCombine(LocalTable& r_table, int num_join_cols);
  void showInfo();
  // int getSizeBound(int num_partitions);
  const int size();
  const int num_columns();
  std::vector<Tuple>& getData();
  long long int hash(int seed = 0) const;
  LocalTable copy(int new_global_id);
  void print(int limit_size, bool show_dummy = false);
  // void shuffle(int num_partitions, const std::vector<int> key, int seed, std::vector<std::vector<Tuple>> &output);
  //void shuffle(int num_partitions, std::vector<int> &index_list, std::vector<std::vector<Tuple>> &output);
  void randomShuffle(int num_partitions);
  void shuffleByKey(int num_partitions, const std::vector<int> key, int seed, int size_bound);
  void shuffleByCol(int num_partitions, int i_col_id, int size_bound);
  void shuffleMerge(int num_partitions);
  // sort table by columns (dictionary order); dummy tuples are always moved to the end
  // Note: this project operation does not elimiate duplicate tuples!
  // Note2: if project the same column for multiple times, then getColumnIdsByNames may not work correctly due to duplicate names
  void removeDummy();
  long long sum(int column);
  // Please ensure that the tuples are already sorted by the group by columns; return the last non-dummy tuple
  Tuple groupByPrefixAggregate(AssociateOperator& op, int phase = 1, bool reverse = false);
  // Which is used for groupByPrefixAggregate only
  void _addValueByKey(AssociateOperator& op);
  // Please ensure that the tuples are already sorted by the group by columns
  void groupByAggregate(AssociateOperator& op);
  void sample(double rate, std::vector<Tuple>& output);
  void partitionByPivots(const std::vector<int> columns, int size_bound);
  // void expandMoveStep1(int column, int num_partitions, int divisor);
  // void expandMoveStep2(int column);
  // void distribute1(int column, int num_partitions, int output_size, std::vector<std::vector<Tuple>> &output);
  // void distribute2(int column, int output_size);
  void getPivots(const std::vector<int> columns);
  void sortMerge(const std::vector<int> columns);
  void localSort(const std::vector<int> columns);
  void pad_to_size(int n);
  void opaque_prepare_shuffle_col(int col_id, int tuple_num);
  void foreignTableModifyColZ(const std::vector<int> columns);                                        //this function is only used in r_table in pkjoin
  void finalizePkjoinResult(const std::vector<int> columns, int ori_r_col_num, int r_align_col_num);  //this function is only used in r_table in pkjoin
  void addCol(int defaultVal, int col_index = -1);                                                    // the new col is added to the last column by default
  void copyCol(int col_index = -1);                                                                   // the new col will copy the last column and add to the last by default
  void expansion_prepare(int d_index);
  void add_and_calculate_col_t_p(int d_index, int m);
  void expansion_suffix_sum(int phase);
  void expansion_distribute_and_clear(int m);
  void remove_dup_after_prefix(const std::vector<int>& columns);
  void project(const std::vector<int>& columns);
  void deleteCol(int col_index = -1);
  int getId() const;
  int getGlobalId();

  int max(int column);
  long long SODA_step1(std::vector<int> columns, int aggCol);
  void SODA_step2(int p);
  void SODA_step3(int p);
  int localJoin(int other_global_id, int other_id, int join_col_num, int output_bound);
  void SODA_step5();
  int union_table(int table_global_id, int table_id);
  void assignColE(int col);

  void generateData(int num_cols, int num_rows);
  void soda_shuffleByKey(int num_partitions, const std::vector<int>& key, int size_bound);

  void destroy();

private:
  bool is_handle_;
  std::string url_;

  int global_id;  //the global table's id it belongs to
  int id;
  std::string filePath;
  std::vector<Tuple> m_tuples;
  bool isKeyUnique(const std::vector<int> key);
  Tuple groupByAggregateBase(AssociateOperator& op, bool doPrefix, int phase = -1, bool reverse = false);
};