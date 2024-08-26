#include "LocalTable.h"
#include <algorithm>
#include <cmath>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>
#include "Obliv.h"
#include "define.h"

// compute the p-quatile, i.e., splite {0,1,...,n-1} to p even parts (start included, end included)
std::vector<int> computeQuatileIndex(int n, int p) {
    int q = n / p, r = n % p;
    int id = 0;
    std::vector<int> out;
    // std::shared_ptr<std::vector<int>> out = std::make_shared<new std::vector<int>>();

    for (int i = 0; i < r; i++) {
        out.push_back(id);
        id += q + 1;
    }
    for (int i = r; i < p; i++) {
        out.push_back(id);
        id += q;
    }
    assert(id == n && "Quantile computing error!");
    out.push_back(n);

    return out;
}

LocalTable::LocalTable(int global_id_, int id_, uint8_t* file, size_t file_length) : global_id(global_id_), id(id_) {
    std::stringstream ss((const char*)file);  // 将const char* 转换为字符串流
    std::string line;

    // read header
    std::getline(ss, line);
    std::istringstream header(line);
    std::string column_name;
    std::vector<std::string> m_columnNames;
    while (header >> column_name) {
        m_columnNames.push_back(column_name);
        m_num_columns++;
    }

    // read rows
    while (std::getline(ss, line)) {
        if (line.length() == 0)
            continue;  // ignore empty lines
        std::istringstream ssline(line);
        std::vector<int> data;

        std::string numStr;
        while (ssline >> numStr) {
            int num = std::stoi(numStr);
            data.push_back(num);
        }

        m_tuples.push_back(Tuple(data));
    }
}

const int LocalTable::size() {
    return (int)m_tuples.size();
}

const int LocalTable::num_columns() {
    if (m_tuples.size() == 0) {
        log_error("Calling num_columns on empty table!");
    }
    return m_tuples[0].data.size();
}

void LocalTable::mvJoinColsAhead(int num_partitions, const std::vector<int> join_cols) {
    std::unordered_set<int> join_cols_set(join_cols.begin(), join_cols.end());
    for (int i = 0; i < m_tuples.size(); i++) {
        auto bak_data = m_tuples[i].data;
        for (int j = 0; j < join_cols.size(); j++)
            m_tuples[i].data[j] = bak_data[join_cols[j]];
        int id = join_cols.size();
        for (int j = 0; j < num_columns(); j++)
            if (join_cols_set.find(j) == join_cols_set.end())
                m_tuples[i].data[id++] = bak_data[j];
    }
}

void LocalTable::print(int limit_size, bool show_dummy) {
    printf("****** global_id: %i, m_tuples size: %i, m_num_rows: %i, show_dummy is %d\n", global_id, m_tuples.size(), getNumRows(), show_dummy);
    int counter = 0;
    for (auto& tuple : m_tuples) {
        if (counter > limit_size) {
            printf("%s...\n", " ");
            break;
        }
        if (tuple.is_dummy) {
            if (show_dummy) {
                printf("dummy%s", "\n");
                counter++;
            }
        } else {
            for (int v : tuple.data)
                printf(" %i", v);

            printf(";%s\n", " ");
            counter++;
        }
    }
}

LocalTable* LocalTable::copy(int new_global_id) {
    /* the global id of the copied table is designed to be original table's global id + 1000 */
    LocalTable* ret = new LocalTable(new_global_id, id, m_tuples);
    ret->m_num_columns = m_num_columns;

    return ret;
}

void LocalTable::shuffleByKey(int num_partitions, const std::vector<int>& key, int seed, int size_bound) {
    if (m_tuples.size() == 0) {
        log_warn("m_tuples size is 0, skip shuffle");
        return;
    }

    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<> distr(0, num_partitions - 1);

    std::vector<int> index_list;
    for (auto& tuple : m_tuples) {
        int index = (unsigned long long int)tuple.hash(key, seed) % num_partitions;
        obliv::cmove(index, distr(rng), tuple.is_dummy);
        // if (tuple.is_dummy)
        //     index = distr(rng);
        index_list.push_back(index);
    }

    shuffle(num_partitions, index_list, size_bound);
}

/* Used in pkjoin, shuffle the tuples back to r_table */
void LocalTable::shuffleByCol(int num_partitions, int i_col_id, int size_bound) {
    if (size_bound > m_tuples.size())
        size_bound = m_tuples.size();
    log_debug("size bound = %d, p = %d, size = %d", size_bound, num_partitions, m_tuples.size());
    if (size_bound * num_partitions < m_tuples.size()) {
        std::vector<int> M;
        for (auto& tuple : m_tuples)
            M.push_back(!tuple.is_dummy);
        obliv::compact(m_tuples, M);
        m_tuples.resize(size_bound * num_partitions);
    }
    std::vector<int> index_list;
    if (num_columns() <= i_col_id) {
        log_error("ShuffleByCol error: num_cols = %d, i_col_id = %d", num_columns(), i_col_id);
    }
    for (auto& tuple : m_tuples) {
        int index = tuple.data[i_col_id];
        // if (!tuple.is_dummy && (index >= num_partitions || index < 0)) {
        //     log_error("In shuffleByCol, I column value %i out of index", index);
        // }
        index_list.push_back(index);
    }

    if (size_bound == 0)
        shuffle_non_obliv(num_partitions, index_list);
    else {
        shuffle(num_partitions, index_list, size_bound);
    }
}

void LocalTable::randomShuffle(int num_partitions) {
    std::vector<int> index_list;

    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<> distr(0, num_partitions - 1);

    for (auto& tuple : m_tuples) {
        int index = distr(rng);
        index_list.push_back(index);
    }

    shuffle_non_obliv(num_partitions, index_list);
}

std::string genFileName(int gid, int lid, int tlid) {
    std::string file_name = "../data/shuffle_buffer/gid" + std::to_string(gid) + "_lid" + std::to_string(lid) + "_tlid" + std::to_string(tlid);

    return file_name;
}

std::string genPivotsFileName(int gid, int lid, int tlid) {
    std::string file_name = "../data/shuffle_buffer/gid" + std::to_string(gid) + "_lid" + std::to_string(lid) + "_tlid" + std::to_string(tlid) + "_pivots";
    return file_name;
}

void LocalTable::refresh() {
    m_tuples.clear();
    // m_num_rows = 0;
}

void LocalTable::refreshAndMerge(int num_partitions) {
    int uniq_counter = globalTimingCounter++;
    // profile_record_time_start("read_write", uniq_counter, global_id, id);
    refresh();
    for (int i = 0; i < num_partitions; i++) {
        FileInfo* ret = read_file(genFileName(global_id, i, id).c_str(), global_id, id);
        load_file_str(ret, &m_tuples, m_num_columns);
        freeFileInfo(ret);
    }
    // profile_record_time_end("read_write", uniq_counter, global_id, id);
}

void LocalTable::shuffleMerge(int num_partitions) {
    refreshAndMerge(num_partitions);
}

void LocalTable::sortMerge(int num_partitions, const std::vector<int>& columns) {
    refreshAndMerge(num_partitions);
    sort(columns);
}

void LocalTable::localSort(int num_partitions, const std::vector<int>& columns) {
    sort(columns);
    //obliv::sort(m_tuples, columns);
}

void LocalTable::pad_to_size(int num_partitions, int n) {
    if (size() == 0) {
        log_error("Cannot pad an empty table!");
    }
    Tuple dummy = m_tuples[0].copy();
    dummy.is_dummy = true;
    m_tuples.insert(m_tuples.end(), n - m_tuples.size(), dummy);
}

/* parameter columns records the col id to be joined (col B in paper)
 */
void LocalTable::foreignTableModifyColZ(int num_partitions, const std::vector<int>& columns) {
    for (int i = 1; i < m_tuples.size(); i++) {
        int cond = m_tuples[i].equal_in_cols(m_tuples[i - 1], columns);
        obliv::cmove(m_tuples[i].data[m_num_columns - 1], i, cond);
    }
}

void LocalTable::finalizePkjoinResult(int num_partitions, const std::vector<int>& columns, int ori_r_col_num, int r_align_col_num) {
    if (m_tuples.size() == 0) {
        log_error("finalize empty table");
    }
    sort(columns);

    /* As columns is join_cols+col_Z_id, so pop back will get the join_cols */
    std::vector<int> join_cols = columns;
    join_cols.pop_back();
    int n_cols = num_columns();
    int Z_col = n_cols - 1;
    m_tuples[0].is_dummy |= m_tuples[0].data[Z_col] != 0;
    for (int i = 1; i < m_tuples.size(); i++) {
        Tuple& this_tuple = m_tuples[i - 1];
        Tuple& next_tuple = m_tuples[i];
        int equal_key = next_tuple.equal_in_cols(this_tuple, join_cols);
        int cond = (!this_tuple.is_dummy) & equal_key;
        for (int j = ori_r_col_num; j < ori_r_col_num + r_align_col_num; j++) {
            obliv::cmove(next_tuple.data[j], this_tuple.data[j], cond);
        }
        // mark inactive as dummy if no copy is done
        next_tuple.is_dummy |= (this_tuple.is_dummy | !equal_key) & next_tuple.data[Z_col] != 0;
    }
}

std::vector<Tuple> LocalTable::getTuples() {
    return m_tuples;
}

int LocalTable::getNumRows() {
    int n = 0;
    for (auto& tuple : m_tuples)
        n += !tuple.is_dummy;
    return n;
}

void LocalTable::pkJoinCombine(int num_partitions, int ori_r_col_num, int r_align_col_num, std::vector<int>& combine_sort_cols, std::vector<int>& join_cols, LocalTable& s_table_local) {
    std::vector<Tuple> s_tuples = s_table_local.getTuples();

    int r_num_rows = m_tuples.size();
    m_tuples.insert(m_tuples.end(), s_tuples.begin(), s_tuples.end());
    if (m_tuples.size() == 0)
        return;

    int n_cols = num_columns();
    int I_col = n_cols - 2;
    int Z_col = n_cols - 1;
    auto sort_cols = join_cols;
    sort_cols.push_back(Z_col);
    sort_cols.push_back(I_col);
    // sort by (join key, Z, I)
    sort(sort_cols);
    m_tuples[0].is_dummy |= m_tuples[0].data[Z_col] == 0 & m_tuples[0].data[I_col] != -1;
    for (int i = 1; i < m_tuples.size(); i++) {
        Tuple& this_tuple = m_tuples[i - 1];
        Tuple& next_tuple = m_tuples[i];
        int equal_key = next_tuple.equal_in_cols(this_tuple, join_cols);
        int cond = (!this_tuple.is_dummy) & equal_key & (next_tuple.data[Z_col] == 0);
        for (int j = ori_r_col_num; j < ori_r_col_num + r_align_col_num; j++) {
            obliv::cmove(next_tuple.data[j], this_tuple.data[j], cond);
        }
        // mark representatives from R as dummy if its previous tuple is dummy, or it has different key from the previous one
        next_tuple.is_dummy |= (this_tuple.is_dummy | !equal_key) & next_tuple.data[Z_col] == 0 & next_tuple.data[I_col] != -1;
    }

    std::vector<int> M(m_tuples.size());
    for (int i = 0; i < m_tuples.size(); i++)
        M[i] = m_tuples[i].data[I_col] != -1;
    obliv::compact(m_tuples, M);
    m_tuples.resize(r_num_rows);
}

void LocalTable::joinComputeAlignment(int m) {
    int k = num_columns();
    for (auto& tuple : m_tuples) {
        uint32_t j = (uint32_t)tuple.data[k - 1];
        int q = tuple.data[k - 2] - 1;
        int deg_r = tuple.data[k - 3];
        int deg_s = tuple.data[k - 4];
        obliv::cmove(deg_r, 1, tuple.is_dummy);  // avoid divide by 0 error
        if (deg_r == 0) {
            log_error("j=%d, q=%d, deg_r=%d, deg_s=%d", j, q, deg_r, deg_s);
        }
        uint32_t l = q / deg_r + (q % deg_r) * deg_s + j - 1;
        tuple.data.push_back(l % m);
        tuple.data.push_back(l / m);
    }
    m_num_columns += 2;
}

void LocalTable::joinFinalCombine(LocalTable& r_table, int num_join_cols) {
    sort({num_columns() - 2});
    for (int i = 0; i < r_table.size(); i++)
        r_table.m_tuples[i].data.insert(r_table.m_tuples[i].data.end(), m_tuples[i].data.begin() + num_join_cols, m_tuples[i].data.end() - 6);
}

void LocalTable::addCol(int num_partitions, int defaultVal, int col_index) {
    for (auto& tuple : m_tuples) {
        if (col_index == -1)
            tuple.add_val(defaultVal);
        else
            tuple.insert_val(defaultVal, col_index);
    }
    m_num_columns++;
}

void LocalTable::copyCol(int num_partitions, int col_index) {
    for (auto& tuple : m_tuples) {
        if (col_index == -1) {
            int val = tuple.data[tuple.data.size() - 1];
            tuple.add_val(val);
        } else {
            int val = tuple.data[col_index];
            tuple.insert_val(val, col_index);
        }
    }
    m_num_columns++;
}

void LocalTable::expansion_prepare(int num_partitions, int d_index) {
    for (Tuple& t : m_tuples) {
        if (t.data.size() != num_columns() && !t.is_dummy)
            log_error("Error: t.data.size() %i != num_columns() %i", t.data.size(), num_columns());
    }
    for (auto& tuple : m_tuples) {
        obliv::cmove(tuple.data[d_index], 0, tuple.is_dummy);
    }
}

void LocalTable::add_and_calculate_col_t_p(int d_index, int m) {
    if (m_tuples.size() == 0)
        return;

    int num_cols = m_num_columns;

    for (auto& tuple : m_tuples) {
        tuple.data.resize(num_cols + 2);
        tuple.is_dummy |= tuple.data[d_index] == 0;
        int l_val = tuple.data[num_cols - 1] - 1;
        int t_val = l_val / m;
        int p_val = l_val % m;

        tuple.data[num_cols] = t_val;      // process T
        tuple.data[num_cols + 1] = p_val;  // process P
    }
    m_num_columns += 2;
}

void LocalTable::expansion_suffix_sum(int num_partitions, int phase) {
    if (m_tuples.size() == 0) {
        log_error("Error: in expansion_suffix_sum phase %d, empty table %d!", phase, id);
    }
    // int v_col = num_columns() - 1;
    size_t ser_length;
    int ser_row_num;
    if (phase == 1) {
        for (int i = m_tuples.size() - 1; i > 0; i--) {
            Tuple& this_tuple = m_tuples[i - 1];
            Tuple& next_tuple = m_tuples[i];
            int cond = this_tuple.is_dummy;
            // int v_sum = this_tuple.data[v_col] + next_tuple.data[v_col];
            obliv::cmove(this_tuple, next_tuple, cond);
            // obliv::cmove(this_tuple.data[v_col], v_sum, cond);
        }
        std::vector<Tuple> first_tuple = {m_tuples[0]};
        const char* ser = serialize_tuple_vector(first_tuple, m_num_columns, &ser_length, &ser_row_num);
        write_file(genFileName(global_id, id, 0).c_str(), ser, ser_length, ser_row_num, global_id, id, 0);  //TODO: here we write to 0 node for convenience. But may lead to bug in distributed sys
        return;
    }
    // following code with phase=2
    if (id == 0) {
        std::vector<Tuple> tups;

        for (int i = 0; i < num_partitions; i++) {
            FileInfo* f = read_file(genFileName(global_id, i, 0).c_str(), global_id, id);
            load_file_str(f, &tups, m_num_columns);
            freeFileInfo(f);
        }

        int num_rows = tups.size();

        if (num_rows != num_partitions) {
            log_error("Error: in expansion_suffix_sum phase 2, the num_rows %i is not equal to the num_partitions %i", num_rows, num_partitions);
        }

        for (int i = num_rows - 1; i > 0; i--) {
            Tuple& this_tuple = tups[i - 1];
            Tuple& next_tuple = tups[i];
            int cond = this_tuple.is_dummy;
            //int v_sum = this_tuple.data[v_col] + next_tuple.data[v_col];
            obliv::cmove(this_tuple, next_tuple, cond);
            //obliv::cmove(this_tuple.data[v_col], v_sum, cond);
        }

        for (int i = 1; i < num_rows; i++) {
            std::vector<Tuple> output = {tups[i]};
            const char* ser = serialize_tuple_vector(output, m_num_columns, &ser_length, &ser_row_num);
            write_file(genFileName(global_id, 0, i - 1).c_str(), ser, ser_length, ser_row_num, global_id, id, i - 1);
        }
    }
    if (id == num_partitions - 1)
        return;
    std::vector<Tuple> tups;
    FileInfo* f = read_file(genFileName(global_id, 0, id).c_str(), global_id, id);
    load_file_str(f, &tups, m_num_columns);
    freeFileInfo(f);
    if (tups.size() != 1) {
        log_error("Error: in expansion_suffix_sum phase 2, the num_rows %i is not equal to 1", tups.size());
        throw;
    }

    Tuple& next_tuple = tups[0];
    for (auto& this_tuple : m_tuples) {
        int cond = this_tuple.is_dummy;
        obliv::cmove(this_tuple, next_tuple, cond);
    }
}

void LocalTable::expansion_distribute_and_clear(int num_partitions, int m) {
    int num_cols = m_num_columns;
    std::vector<int> non_dummies(m_tuples.size());
    for (int i = 0; i < m_tuples.size(); i++)
        non_dummies[i] = !m_tuples[i].is_dummy;
    obliv::compact(m_tuples, non_dummies);

    int expand_col = num_columns() - 1;
    obliv::sort(m_tuples, {expand_col});

    int ori_size = m_tuples.size();
    if (m < ori_size) {
        m_tuples.resize(m);
    } else {
        Tuple dummy = m_tuples[0];
        dummy.is_dummy = true;
        m_tuples.resize(m);
        for (int i = ori_size; i < m; i++)
            m_tuples[i] = dummy;
    }

    obliv::distributeByCol(m_tuples, expand_col);

    for (auto& tuple : m_tuples) {
        tuple.data.resize(tuple.data.size() - 3);  //remove P,T,L
    }
    m_num_columns -= 3;
}

void LocalTable::deleteCol(int num_partitions, int col_index) {
    if (col_index == -1)
        col_index = m_num_columns - 1;

    for (auto& tuple : m_tuples) {
        tuple.delete_col(col_index);
    }
    m_num_columns--;
}

/* This function is only called inside LocalTable.cpp
 * it padding each layer of output and write each layer
 * to its desitination.
 */
void LocalTable::shuffleWrite(int num_partitions, std::vector<std::vector<Tuple>>& output) {
    for (int i = 0; i < num_partitions; i++) {
        size_t ser_length = -1;
        int ser_row_num = -1;
        const char* ser = serialize_tuple_vector(output[i], num_columns(), &ser_length, &ser_row_num);
        write_file(genFileName(global_id, id, i).c_str(), ser, ser_length, ser_row_num, global_id, id, i);
    }
}

void LocalTable::union_table(LocalTable& other_table) {
    std::vector<Tuple> other_tuples = other_table.getTuples();
    m_tuples.insert(m_tuples.end(), other_tuples.begin(), other_tuples.end());
}

void print_tuples(std::vector<Tuple>& m_tuples, int limit_size, bool show_dummy = false) {
    int counter = 0;
    for (auto& tuple : m_tuples) {
        if (counter > limit_size) {
            printf("%s...\n", " ");
            break;
        }
        if (tuple.is_dummy) {
            if (show_dummy) {
                printf("dummy%s", "\n");
                counter++;
            }
        } else {
            for (int v : tuple.data)
                printf(" %i", v);

            printf(";%s\n", " ");
            counter++;
        }
    }
}

void aggregate(std::vector<Tuple>& tuples, const std::vector<int>& join_cols) {
    int v = 1;
    int agg_col = join_cols.size();
    int num_out_cols = agg_col + 1;
    tuples[0].data.resize(num_out_cols);
    tuples[0].data[agg_col] = 1;
    for (int i = 0; i < tuples.size() - 1; i++) {
        int can_agg = tuples[i].equal_in_cols(tuples[i + 1], join_cols) & (!tuples[i + 1].is_dummy);
        obliv::cmove(v, v + 1, can_agg);
        obliv::cmove(v, 1, !can_agg);
        tuples[i + 1].data.resize(num_out_cols);
        tuples[i + 1].data[agg_col] = v;
        tuples[i].is_dummy |= can_agg;
    }
}

int pkJoinAndExpand(std::vector<Tuple>& R, std::vector<Tuple>& S, std::vector<int>& cols, int output_bound) {
    int orig_size = R.size();
    int agg_col_in_S = cols.size();
    int agg_col_in_R = R[0].size();
    int mark_col_in_R = agg_col_in_R + 1;
    int num_cols_R = mark_col_in_R + 1;
    for (auto& tuple : R) {
        tuple.data.resize(num_cols_R);
        tuple.data[mark_col_in_R] = 1;  // from R gets 1
    }
    for (auto& tuple : S) {
        tuple.data.resize(num_cols_R);
        tuple.data[agg_col_in_R] = tuple.data[agg_col_in_S];
        tuple.data[mark_col_in_R] = 0;  // from S gets 0
    }
    auto sort_col = cols;
    sort_col.push_back(mark_col_in_R);
    R.insert(R.end(), S.begin(), S.end());
    obliv::sort(R, sort_col);
    std::vector<int> M(R.size(), 0);
    for (int i = 0; i < R.size() - 1; i++) {
        auto& this_tuple = R[i];
        auto& next_tuple = R[i + 1];
        int eq = this_tuple.equal_in_cols(next_tuple, cols);
        int can_copy = (!this_tuple.is_dummy) & eq;
        obliv::cmove(next_tuple.data[agg_col_in_R], this_tuple.data[agg_col_in_R], eq);
        M[i + 1] = (!next_tuple.is_dummy) & (next_tuple.data[mark_col_in_R]);  // keep next tuple only when it is from R and non-dummy
        M[i + 1] &= !this_tuple.is_dummy;                                      // keep next tuple only if this tuple is non dummy
        M[i + 1] &= eq;                                                        // keep next tuple only if it has the same join cols as this tuple
        M[i + 1] &= M[i] | (!this_tuple.data[mark_col_in_R]);                  // keep next tuple only if this tuple is from S or this tuple should also be kept
        this_tuple.is_dummy = !M[i];
    }
    R[R.size() - 1].is_dummy = !M[R.size() - 1];
    obliv::compact(R, M);
    R.resize(orig_size);
    int ori_size = R.size();
    for (auto& tuple : R) {
        tuple.data.pop_back();
    }
    int m = output_bound;
    if (m == 0) {
        for (auto& tuple : R) {
            obliv::cmove(m, m + tuple.data[agg_col_in_R], !tuple.is_dummy);
        }
    }
    if (m <= ori_size)
        R.resize(m);
    else {
        Tuple dummy = R[0].copy();
        dummy.is_dummy = true;
        R.insert(R.end(), m - ori_size, dummy);
    }
    obliv::distributeByCol(R, agg_col_in_R);
    return m;
}

// join on the first num_cols columns obliviously
int LocalTable::localJoin(LocalTable& other_table, int num_cols, int output_bound) {
    std::vector<int> join_cols(num_cols);
    std::iota(join_cols.begin(), join_cols.end(), 0);
    auto& R = other_table.m_tuples;
    auto& S = m_tuples;
    int r_total_cols = R[0].size();
    int s_total_cols = S[0].size();
    int new_num_columns = r_total_cols + s_total_cols - num_cols;
    int m = 0;
    if (output_bound == -1)  // pk join
    {
        m = -1;
        int orig_size = R.size();
        R.insert(R.end(), S.begin(), S.end());
        for (auto& tuple : R)
            tuple.data.resize(new_num_columns);
        obliv::sort(R, join_cols);
        std::vector<int> M(R.size(), 0);
        obliv::compact(R, M);
        R.resize(orig_size);
    } else {
        obliv::sort(R, join_cols);
        obliv::sort(S, join_cols);
        auto S1 = S;
        auto R1 = R;
        aggregate(R1, join_cols);
        aggregate(S1, join_cols);
        int m1 = pkJoinAndExpand(R, S1, join_cols, output_bound);
        int m2 = pkJoinAndExpand(S, R1, join_cols, m1);
        log_info("m1=%d, m2=%d", m1, m2);
        m = m1;
        obliv::sort(R, {0});  // simluate the alignment
        for (int i = 0; i < m; i++) {
            R[i].data.resize(new_num_columns);
            for (int j = num_cols; j < s_total_cols; j++)
                R[j + r_total_cols - num_cols] = S[j];
        }
    }
    m_num_columns = m_tuples[0].size();
    other_table.m_num_columns = other_table.m_tuples[0].size();
    return m;
}

long long LocalTable::SODA_step1(std::vector<int>& columns, int num_partitions, int aggCol) {
    int tup_size = m_tuples.size();
    if (tup_size == 0)
        return 0;

    sort(columns);

    Tuple pre_tup = m_tuples[0];
    long long ret = 0;

    for (int i = 1; i < m_tuples.size(); i++) {
        Tuple& pre_tup = m_tuples[i - 1];
        Tuple& cur_tup = m_tuples[i];
        int cond = (!cur_tup.is_dummy) & cur_tup.equal_in_cols(pre_tup, columns);
        int v = pre_tup.data[aggCol] * cur_tup.data[aggCol];
        obliv::cmove(v, 0, !cond);
        ret += v;
    }
    return ret;
}

void LocalTable::SODA_step2(int num_partitions, int p) {
    int bin_size = 0;
    int cur_bin_size = 0;
    int cur_bin_id = 0;
    int deg_col = num_columns() - 1;
    for (auto& tuple : m_tuples) {
        int this_size = tuple.data[deg_col];
        cur_bin_size += this_size;
        int cond = cur_bin_size > bin_size;
        obliv::cmove(cur_bin_size, this_size, cond);
        cur_bin_id += cond;
        obliv::cmove(tuple.data[deg_col], cur_bin_id, 0);
    }
}

void LocalTable::SODA_step3(int num_partitions, int p) {
    int n = m_tuples.size();
    if (n < p) p = n;
    std::vector<Tuple> slice(m_tuples.begin(), m_tuples.begin() + p);
    std::vector<Tuple> weights;
    Tuple weight;
    weight.data.push_back(1);
    for (int i = 0; i < p; i++)
        weights.emplace_back(weight);
    int deg_col = num_columns() - 1;
    for (int i = 0; i < n / p + 1; i++) {
        obliv::sort(slice, {deg_col});
        obliv::distributeByCol(slice, deg_col);
        obliv::sort(weights, {0});
        obliv::sort(weights, {0}, false);
        obliv::sort(slice, {deg_col});
    }
    for (int i = 0; i < p; i++)
        obliv::cmove(m_tuples[i], slice[i], 0);
    std::vector<int> M(n, 0);
    obliv::compact(m_tuples, M);  // divide T to S and R
}

void LocalTable::SODA_step5(int num_partitions) {
    int n = m_tuples.size();
    std::vector<int> M(n, 0);
    obliv::compact(m_tuples, M);
}

void LocalTable::assignColE(int num_partitions, int col) {
    for (int i = 0; i < m_tuples.size(); i++) {
        m_tuples[i].data[col] = i % num_partitions;
    }
}

int LocalTable::max(int column) {
    int ret = INT_MIN;

    for (auto& tuple : m_tuples) {
        int cond = (!tuple.is_dummy) & (tuple.data[column] > ret);
        obliv::cmove(ret, tuple.data[column], cond);
    }

    return ret;
}

void LocalTable::shuffle_non_obliv(int num_partitions, std::vector<int>& index_list) {
    std::vector<std::vector<Tuple>> output;
    output.resize(num_partitions);
    if (index_list.size() != size()) {
        log_error("ERROR: index_list size %i not equal to local table size %i", index_list.size(), size());
    }
    int i = -1;
    for (auto& tuple : m_tuples) {
        i++;
        int index = index_list[i];
        output[index].push_back(tuple);
    }
    shuffleWrite(num_partitions, output);
}

void LocalTable::shuffle(int num_partitions, std::vector<int>& index_list, int size_bound) {
    if (index_list.size() != size()) {
        log_error("ERROR: index_list size %i not equal to local table size %i", index_list.size(), size());
        throw;
    }
    if (size_bound > size()) size_bound = size();
    if (m_tuples.size() == 0) {
        log_warn("Empty table. No need to shuffle.");
        return;
    }

    std::vector<std::vector<Tuple>> output;
    output.resize(num_partitions);
    Tuple dummy = m_tuples[0];
    dummy.is_dummy = true;

    obliv::shuffle(m_tuples, index_list, num_partitions, size_bound, dummy);

    if (size_bound * num_partitions != m_tuples.size()) {
        log_error("Padding error! size_bound=%d,num_partitions=%d,tuple_size=%d", size_bound, num_partitions, m_tuples.size());
    }

    auto it = m_tuples.begin();
    for (int i = 0; i < num_partitions; i++) {
        output[i].insert(output[i].end(), it, it + size_bound);
        it += size_bound;
    }

    shuffleWrite(num_partitions, output);
}

const char* serialize_tuple_vector(std::vector<Tuple>& tuple_list, int col_num, size_t* ser_length, int* ser_row_num) {
    size_t tup_length = Tuple::rowLength(col_num);
    *ser_row_num = tuple_list.size();
    *ser_length = tuple_list.size() * tup_length;
    char* buffer = new char[*ser_length];
    char* pointer = buffer;

    for (auto& tuple : tuple_list) {
        tuple.serialize(pointer);
        pointer += tup_length;
    }

    return buffer;
    // return ss.str();
}

void LocalTable::project(const std::vector<int>& columns) {
    int k = columns.size();
    for (auto& tuple : m_tuples) {
        std::vector<int> new_tuple_data(k);
        std::transform(columns.begin(), columns.end(), new_tuple_data.begin(), [tuple](size_t pos) { return tuple.data[pos]; });
        tuple.data = new_tuple_data;
    }
    m_num_columns = k;
}

bool LocalTable::isKeyUnique(const std::vector<int>& key) {
    std::unordered_set<long long int> hash_list;
    std::random_device rd;
    auto seed = rd();
    for (auto& tuple : m_tuples) {
        if (tuple.is_dummy)
            continue;
        long long int hash_value = tuple.hash(key, seed);
        if (hash_list.find(hash_value) != hash_list.end())
            return false;
        hash_list.insert(hash_value);
    }
    return true;
}

// void LocalTable::sample(double rate, std::vector<Tuple> &output)
// {
//     output.clear();
//     std::random_device rd;
//     std::mt19937 rng(rd());
//     std::uniform_real_distribution<> dist;
//     for (auto tuple : m_tuples)
//     {
//         if (dist(rng) < rate)
//             output.push_back(tuple);
//     }
// }

void LocalTable::partitionByPivots(std::vector<int>& columns, int num_partitions, int size_bound) {
    std::vector<Tuple> pivots;
    FileInfo* f = read_file(genPivotsFileName(global_id, 0, id).c_str(), global_id, id);
    load_file_str(f, &pivots, m_num_columns);
    freeFileInfo(f);

    std::vector<std::vector<Tuple>> output;
    output.resize(num_partitions);

    if (m_tuples.size() == 0) {
        log_warn("Partition empty table.");
        return;
    }
    Tuple dummy = m_tuples[0];
    dummy.is_dummy = true;
    obliv::partition(m_tuples, pivots, columns, size_bound, dummy);
    if (size_bound * num_partitions != m_tuples.size()) {
        log_error("Padding error! size_bound=%d,num_partitions=%d,tuple_size=%d", size_bound, num_partitions, m_tuples.size());
    }

    auto it = m_tuples.begin();
    for (int i = 0; i < num_partitions; i++) {
        output[i].insert(output[i].end(), it, it + size_bound);
        it += size_bound;
    }

    shuffleWrite(num_partitions, output);
}

void LocalTable::sort(const std::vector<int>& columns) {
    obliv::sort(m_tuples, columns);
}

void LocalTable::getPivots(int num_partitions, const std::vector<int>& columns) {
    if (id != 0) {
        log_error("Error: in getPivots, only local_table with local_id 0 can perform, not %i", id);
        throw;
    }

    // profile_record_time_start("sss", 1000, global_id, id);
    sort(columns);
    // profile_record_time_end("sss", 1000, global_id, id);

    // std::vector<int> index;
    // index.clear();
    auto index = computeQuatileIndex(size(), num_partitions);

    std::vector<Tuple> pivots;
    for (int i = 1; i < num_partitions; i++) {
        pivots.push_back(m_tuples[index[i]]);
    }

    for (int i = 0; i < num_partitions; i++) {
		size_t ser_length = -1;
		int ser_row_num = -1;
		const char* ser = serialize_tuple_vector(pivots, m_num_columns, &ser_length, &ser_row_num);
	    write_file(genPivotsFileName(global_id, id, i).c_str(), ser, ser_length, ser_row_num, global_id, id, i);
    }
}

long long LocalTable::sum(int column) {
    long long ret = 0;
    for (auto& tuple : m_tuples){
        int v = tuple.data[column];
        obliv::cmove(v, 0, tuple.is_dummy);
        ret += v;
    }
    return ret;
}

Tuple aggCore(std::vector<Tuple>* tuples, AssociateOperator* op, bool doPrefix, bool reverse) {
    int n = tuples->size();
    int start = 0, end = n - 1, step = 1;
    if (reverse) {
        start = n - 1;
        end = 0;
        step = -1;
    }
    Tuple last_tuple = tuples->at(start);
    last_tuple.is_dummy = true;
    for (int i = start; i != end; i += step) {
        bool applied = op->apply(tuples->at(i), tuples->at(i + step));
        tuples->at(i).is_dummy |= !doPrefix && applied && !tuples->at(i + step).is_dummy;
        obliv::cmove(last_tuple, tuples->at(i + step), !tuples->at(i + step).is_dummy);
    }
    return last_tuple;
}

void LocalTable::groupByAggregateBase(int num_partitions, AssociateOperator* op, bool doPrefix, int phase, bool reverse) {
    if (m_tuples.size() == 0) {
        log_error("Error: in groupByAggregateBase phase %d, empty table %d!", phase, id);
    }
    if (phase == -1) {
        /* group by aggregate */
        aggCore(&m_tuples, op, doPrefix, reverse);
    }
    if (phase == 1) {
        Tuple last_tuple = aggCore(&m_tuples, op, doPrefix, reverse);
        std::vector<Tuple> output;
        output.push_back(last_tuple);
        size_t ser_length = -1;
        int ser_row_num = -1;
        const char* ser = serialize_tuple_vector(output, m_num_columns, &ser_length, &ser_row_num);
        write_file(genFileName(global_id, id, 0).c_str(), ser, ser_length, ser_row_num, global_id, id, 0);
    } else if (phase == 2) {
        if (id != 0) {
            log_error("Error: in groupByAggregateBase phase 2, only local_table with local_id 0 can perform, not %i", id);
        }

        std::vector<Tuple> tups;

        for (int i = 0; i < num_partitions; i++) {
            FileInfo* f = read_file(genFileName(global_id, i, 0).c_str(), global_id, id);
            load_file_str(f, &tups, m_num_columns);
            freeFileInfo(f);
        }

        int num_rows = tups.size();

        if (num_rows != num_partitions) {
            log_error("Error: in groupByAggregateBase phase 2, the num_rows %i is not equal to the num_partitions %i", num_rows, num_partitions);
        }

        aggCore(&tups, op, doPrefix, reverse);
        size_t ser_length = -1;
        int ser_row_num = -1;
        if (reverse) {
            for (int i = num_rows - 1; i > 0; i--) {
                std::vector<Tuple> output;
                output.push_back(tups[i]);
                const char* ser = serialize_tuple_vector(output, m_num_columns, &ser_length, &ser_row_num);
                write_file(genFileName(global_id, 0, i - 1).c_str(), ser, ser_length, ser_row_num, global_id, id, i - 1);
            }
        } else {
            for (int i = 0; i < num_rows - 1; i++) {
                std::vector<Tuple> output;
                output.push_back(tups[i]);
                const char* ser = serialize_tuple_vector(output, m_num_columns, &ser_length, &ser_row_num);
                write_file(genFileName(global_id, 0, i + 1).c_str(), ser, ser_length, ser_row_num, global_id, id, i + 1);
            }
        }
    }
}

void LocalTable::_addValueByKey(AssociateOperator* op) {
    std::vector<Tuple> tups;
    FileInfo* f = read_file(genFileName(global_id, 0, id).c_str(), global_id, id);
    load_file_str(f, &tups, m_num_columns);
    freeFileInfo(f);
    if (tups.size() != 1) {
        log_error("Error: in _addValueByKey, the num_rows %i is not equal to 1", tups.size());
    }
    for (auto& t : m_tuples)
        op->apply(tups[0], t);
}

void LocalTable::remove_dup_after_prefix(int num_partitions, std::vector<int>& columns) {
    for (int i = 1; i < m_tuples.size(); i++) {
        Tuple& this_tuple = m_tuples[i - 1];
        Tuple& next_tuple = m_tuples[i];
        int cond = (!next_tuple.is_dummy) & this_tuple.equal_in_cols(next_tuple, columns);
        this_tuple.is_dummy |= cond;
    }
}

void LocalTable::soda_shuffleByKey(int num_partitions, const std::vector<int>& key, int seed, int size_bound) {
    if (m_tuples.size() == 0) {
        log_warn("m_tuples size is 0, skip shuffle");
        return;
    }

    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<> distr(0, num_partitions - 1);

    std::vector<int> index_list;
    for (auto& tuple : m_tuples) {
        int index = (unsigned long long int)tuple.hash(key, seed) % num_partitions;
        obliv::cmove(index, distr(rng), tuple.is_dummy);
        index_list.push_back(index);
    }
    std::vector<std::vector<Tuple>> output;
    output.resize(num_partitions);
    Tuple dummy = m_tuples[0];
    dummy.is_dummy = true;
    obliv::shuffle_soda(m_tuples, index_list, num_partitions, size_bound, dummy);
    if (size_bound * num_partitions != m_tuples.size()) {
        log_error("Padding error! size_bound=%d,num_partitions=%d,tuple_size=%d", size_bound, num_partitions, m_tuples.size());
    }

    auto it = m_tuples.begin();
    for (int i = 0; i < num_partitions; i++) {
        output[i].insert(output[i].end(), it, it + size_bound);
        it += size_bound;
    }

    shuffleWrite(num_partitions, output);
}

void LocalTable::generateData(int num_cols, int num_rows) {
    m_tuples.resize(num_rows);
    for (int i = 0; i < num_rows; i++) {
        m_tuples[i].data.resize(num_cols);
        for (int j = 0; j < num_cols; j++)
            m_tuples[i].data[j] = i + j;
    }
}

void LocalTable::opaque_prepare_shuffle_col(int num_partitions, int col_id, int tuple_num) {
    m_tuples.resize(size() / 2);
}
