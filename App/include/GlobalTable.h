#include <cassert>
#include <string>
#include <vector>
#include "Enclave_u.h"
#include "LocalTable.h"

#define SHUFFLE_BY_KEY 0
#define RANDOM_SHUFFLE 1
#define SHUFFLE_BY_COL 2

extern sgx_enclave_id_t global_eid; /* global enclave id */

class GlobalTable {
public:
    static int TOTAL;
    // GlobalTable(const char *filePath, int num_partitions);
    GlobalTable(const char* filePath, sgx_enclave_id_t eid = global_eid);
    GlobalTable(const std::string& filePath, sgx_enclave_id_t eid = global_eid) : GlobalTable(filePath.c_str(), eid) {}
    GlobalTable(std::vector<LocalTable>& local_tables, const std::vector<std::string> column_names);
    ~GlobalTable();
    const int size();
    const int numColumns();
    GlobalTable copy();
    void print(int limit_size = 10, bool show_dummy = false);
    std::vector<int> getColumnIdsByNames(std::vector<std::string> column_names);
    void randomShuffle();
    void shuffleByKey(const std::vector<int> key);
    void shuffle(int shuffleType, const std::vector<int> key, int seed = -1, int shuffle_by_col_padding_size = 0);  // zero padding means non-oblivious
    void pkjoin(GlobalTable& r_table, std::vector<int> r_cols, std::vector<int> s_cols, bool need_move_cols = true);
    void opaque_pkjoin(GlobalTable& r_table, std::vector<int> r_cols, std::vector<int> s_cols);
    void join(GlobalTable& r_table, std::vector<int> r_cols, std::vector<int> s_cols, long long& M);
    void soda_join(GlobalTable& r_table, std::vector<int> r_cols, std::vector<int> s_cols, int& a1, int& a2, long long& M);
    void mvJoinColsAhead(std::vector<int> join_cols);

    void showInfo();

    // sort table by columns (dictionary order); dummy tuples are always moved to the end
    void sort(const std::vector<int>& columns);
    void opaque_sort(const std::vector<int>& columns);
    void union_table(GlobalTable& r_table);
    int max(int column);
    long long SODA_step1(std::vector<int> columns, int aggCol);
    void SODA_step2(int p);
    void SODA_step3(int p);
    void localJoin(GlobalTable& r_table, std::vector<int> r_cols, std::vector<int> s_cols, int& output_bound);
    void SODA_step5();
    // int size_bound_soda(int N1, int N2, int a1, int a2, int p);

    // void sortTest(const std::vector<int> columns);
    // Note: this project operation does not elimiate duplicate tuples!
    // Note2: if project the same column for multiple times, then getColumnIdsByNames may not work correctly due to duplicate names
    void project(const std::vector<int> columns);
    // remove dummy elements locally (non-oblivious); then re-balance
    void removeDummy();
    long long sum(int column);
    // Please ensure that the tuples are already sorted by the group by columns
    void groupByPrefixAggregate(AssociateOperator& op, bool reverse = false);
    void sodaGroupByAggregate(AssociateOperator& op);
    void soda_shuffleByKey(const std::vector<int>& cols);
    int expansion(int d_index, long long M, bool delete_expand_col = true);
    std::vector<LocalTable>& getLocalTables();
    int appendCol(int defaultVal);

    template<typename Func>
    void parallel_for_each(Func func);

    template<typename Func>
    void parallel_for_each(Func func) const;

private:
    int id;
    sgx_enclave_id_t m_eid;
    std::string m_filePath;
    std::vector<LocalTable> m_localTables;
    std::vector<std::string> m_columnNames;
    // Statistics &m_stat;
    // Load data into the Table 
    void loadDataFromFile(const char* filePath, std::vector<Tuple>& output);
    // Split data to partitions
    void splitData();
    static void computeDegrees(GlobalTable& r_table, GlobalTable& s_table, const std::vector<int>& r_cols, const std::vector<int>& s_cols);
};
