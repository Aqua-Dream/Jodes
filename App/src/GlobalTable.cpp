#include "GlobalTable.h"
#include <algorithm>
#include <boost/thread.hpp>
#include <cmath>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include "App.h"
#include "ReqSender.h"
#include "log.h"
#include "utils.h"

int GlobalTable::TOTAL = 0;

// compute the p-quatile, i.e., splite {0,1,...,n-1} to p even parts (start included, end included)
std::vector<int> computeQuatileIndex(int n, int p) {
    int q = n / p, r = n % p;
    int id = 0;
    std::vector<int> out;
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

GlobalTable::GlobalTable(
    const char* filePath,
    sgx_enclave_id_t eid) : m_filePath(filePath), m_eid(eid) {
    id = TOTAL++;
    splitData();
}

GlobalTable::GlobalTable(std::vector<LocalTable>& local_tables, const std::vector<std::string> column_names) {
    id = TOTAL++;
    m_localTables = local_tables;
    int ret;
    // ecall_setup_env(global_eid, &ret, utils::num_partitions);
    if (!utils::is_distributed) {
        sgx_status_t ecall_status = ecall_setup_env(global_eid, &ret, utils::num_partitions);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }

    m_columnNames = column_names;
}

GlobalTable::~GlobalTable() {
    if (!utils::is_distributed) {
        //if (!enclave_destroyed) {
        int ret;
        sgx_status_t ecall_status = ecall_destroy(global_eid, &ret, id, utils::num_partitions);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall_destroy failed");
        }
        //}
    } else {
        parallel_for_each([](LocalTable& table) { table.destroy(); });
    }
}

template <typename Func>
void GlobalTable::parallel_for_each(Func func) {
    if (utils::is_distributed) {
        std::vector<boost::thread> threads;
        for (auto& table : m_localTables) {
            threads.emplace_back(func, std::ref(table));
        }
        for (auto& t : threads) {
            if (t.joinable()) {
                t.join();
            }
        }
    } else {
        for (auto& table : m_localTables) {
            func(table);
        }
    }
}

template <typename Func>
void parallel_for_each_i(Func func) {
    if (utils::is_distributed) {
        std::vector<boost::thread> threads;
        for (int i = 0; i < utils::num_partitions; i++) {
            threads.emplace_back(func, i);
        }
        for (auto& t : threads) {
            if (t.joinable()) {
                t.join();
            }
        }
    } else {
        for (int i = 0; i < utils::num_partitions; i++) {
            func(i);
        }
    }
}

template <typename Func>
void GlobalTable::parallel_for_each(Func func) const {
    if (utils::is_distributed) {
        std::vector<boost::thread> threads;
        for (const auto& table : m_localTables) {
            threads.emplace_back(func, std::ref(table));
        }
        for (auto& t : threads) {
            if (t.joinable()) {
                t.join();
            }
        }
    } else {
        for (auto& table : m_localTables) {
            func(table);
        }
    }
}

// is only called by creator
void GlobalTable::loadDataFromFile(const char* filePath, std::vector<Tuple>& output) {
    output.clear();

    // open file filePath
    std::ifstream fin(filePath, std::ios::in);
    if (!fin.is_open()) {
        std::cerr << "ERROR: Cannot open data file: " << filePath << std::endl;
        throw;
    }
    std::string line;
    getline(fin, line);
    std::istringstream header(line);
    std::string column_name;
    while (header >> column_name)
        m_columnNames.push_back(column_name);
    int num_columns = numColumns();
    int num_rows = 0;
    while (getline(fin, line)) {
        num_rows++;
        if (line.length() == 0)
            continue;  // ignore empty lines
        std::istringstream ssline(line);
        std::vector<int> data(num_columns);
        for (int i = 0; i < num_columns; i++) {
            if (!(ssline >> data[i])) {
                std::cerr << "ERROR: Cannot read line " << num_rows << std::endl;
                throw;
            }
        }
        output.push_back(Tuple(data));
    }
    fin.close();
}
void GlobalTable::splitData() {
    m_localTables.resize(utils::num_partitions);
    parallel_for_each_i([&](int i) {
        std::string url = utils::is_distributed ? utils::worker_urls[i] : "";
        std::string real_path = utils::num_partitions == 1 ? m_filePath : m_filePath + "_p" + std::to_string(i);  // ==1 for single join only
        LocalTable table(id, i, real_path, utils::is_distributed, url);
        m_localTables[i] = std::move(table);
    });
}

const int GlobalTable::size() {
    int sz = 0;
    boost::mutex mutex;
    parallel_for_each([&sz, &mutex](LocalTable& table) {
        int this_size = table.size();
        {
            boost::lock_guard<boost::mutex> lock(mutex);
            sz += this_size;
        }
    });
    return sz;
}

void GlobalTable::showInfo() {
    std::cout << "<<<<<<<<<   ";
    for (auto& table : m_localTables)
        table.showInfo();
    std::cout << "   >>>>>>>>>" << std::endl;
}

const int GlobalTable::numColumns() {
    return m_localTables[0].num_columns();
}

/* the global id of the copied table is designed to be original table's global id + 1000 */
GlobalTable GlobalTable::copy() {
    std::vector<LocalTable> tables(utils::num_partitions);
    parallel_for_each_i([this, &tables](int i) {
        tables[i] = m_localTables[i].copy(TOTAL);
    });
    return GlobalTable(tables, m_columnNames);
}

void GlobalTable::print(int limit_size, bool show_dummy) {
    if (utils::is_distributed) {
        parallel_for_each([limit_size, show_dummy](LocalTable& table) {
            table.print(limit_size, show_dummy);
        });
        return;
    }

    std::cout << "(" << size() << " rows)" << std::endl;
    for (int i = 0; i < utils::num_partitions; i++) {
        auto& table = m_localTables[i];
        std::cout << "(Partition " << i << ", " << table.size() << " rows)" << std::endl;
        table.print(limit_size, show_dummy);
    }
    std::cout << std::endl;
}

std::vector<int> GlobalTable::getColumnIdsByNames(std::vector<std::string> column_names) {
    std::vector<int> column_ids;
    for (auto column_name : column_names)
        for (int i = 0; i < numColumns(); i++)
            if (column_name == m_columnNames[i])
                column_ids.push_back(i);
    return column_ids;
}

void GlobalTable::project(const std::vector<int> columns) {
    parallel_for_each([&columns](LocalTable& table) {
        table.project(columns);
    });
}

void GlobalTable::shuffle(int shuffleType, const std::vector<int> key, int seed, int by_col_size_bound) {
    int origin_size = size();
    std::string shuffleTypeStr;
    if (shuffleType == RANDOM_SHUFFLE) {
        shuffleTypeStr = "random shuffle";
    } else if (shuffleType == SHUFFLE_BY_KEY) {
        shuffleTypeStr = "shuffle by key";
    } else if (shuffleType == SHUFFLE_BY_COL) {
        shuffleTypeStr = "shuffle by col";
    } else {
        log_error("shuffleType %i not supported", shuffleType);
        return;
    }

    // get the max local table size for shuffleByKey
    int max_n = 0, size_bound = 0;
    if (shuffleType == SHUFFLE_BY_KEY) {
        boost::mutex mutex;

        parallel_for_each([&max_n, &mutex](LocalTable& table) {
            int cur_size = table.size();
            {
                boost::lock_guard<boost::mutex> lock(mutex);
                if (cur_size > max_n) {
                    max_n = cur_size;
                }
            }
        });

        size_bound = utils::getSizeBound(max_n, utils::num_partitions);
        if (size_bound > max_n) size_bound = max_n;
    }

    parallel_for_each([&shuffleType, &key, this, &seed, &size_bound, &by_col_size_bound](LocalTable& table) {
        if (shuffleType == RANDOM_SHUFFLE) {
            table.randomShuffle(utils::num_partitions);
        } else if (shuffleType == SHUFFLE_BY_KEY) {
            table.shuffleByKey(utils::num_partitions, key, seed, size_bound);
        } else if (shuffleType == SHUFFLE_BY_COL) {
            table.shuffleByCol(utils::num_partitions, key[0], by_col_size_bound);
        }
    });

    utils::update_phase("<" + shuffleTypeStr + " write>");

    parallel_for_each([](LocalTable& table) {
        table.shuffleMerge(utils::num_partitions);
    });
    utils::update_phase("<shuffle read>");

    int padded_size = size();
    float padding_rate = 100.0 * (padded_size - origin_size) / origin_size;
    log_info("Padding_rate is %.2f%% in shuffle", padding_rate);
}

void GlobalTable::randomShuffle() {
    shuffle(RANDOM_SHUFFLE, {});
}

int size_bound_for_sorting(int N, int p, int n0, int ni) {
    // compute c1
    double b = 2.0 * (utils::KAPPA + 1 + log(N)) * p / n0;
    double c1 = (b + sqrt(b * b + 4 * b)) / 2;
    // compute c2
    b = (utils::KAPPA + 1.0 + 2 * log(p)) * p * (1 + c1) / ni;
    double c2 = (b + sqrt(b * b + 8 * b)) / 2;
    // compute U
    int U = ceil((1 + c1) * (1 + c2) * ni / p);

    return (U > ni) ? ni : U;
}

void GlobalTable::mvJoinColsAhead(std::vector<int> join_cols) {
    bool no_need_to_mv = true;
    for (int i = 0; i < join_cols.size(); i++) {
        if (i != join_cols[i]) {
            no_need_to_mv = false;
            break;
        }
    }
    if (no_need_to_mv) {
        return;
    }

    parallel_for_each([&](LocalTable& table) { table.mvJoinColsAhead(join_cols); });
}

std::vector<LocalTable>& GlobalTable::getLocalTables() {
    return m_localTables;
}

void GlobalTable::union_table(GlobalTable& r_table) {
    if (numColumns() != r_table.numColumns()) {
        log_error("Table num cols not equal in Union");
    }
    parallel_for_each_i([&](int i) {
        m_localTables[i].union_table(r_table.getLocalTables()[i].getGlobalId(), r_table.getLocalTables()[i].getId());
    });
}

int size_bound_soda(int N1, int N2, int a1, int a2, int p, int threshold) {
    int bin_size_max = a1 + a2;
    int bin_size_min = ceil(bin_size_max / 2.0);
    int num_bin_max = (N1 + N2) / bin_size_min;
    int num_bins_min = (N1 + N2) / bin_size_max;
    double n = (double)num_bins_min / p;
    // x^2=bx+c
    double b = (utils::KAPPA + 2.0l * log(p)) * p / n;
    double c = 2.0l * b;
    double x = (sqrt(b * b + 4.0 * c) + b) / 2.0l;
    // By chernoff bound
    int U = (int)ceil((1 + x) * num_bin_max * bin_size_max / p / p);
    return U > threshold ? threshold : U;
}

void GlobalTable::soda_join(GlobalTable& r_table, std::vector<int> r_cols, std::vector<int> s_cols, int& a1, int& a2, long long& M) {
    int join_col_num = r_cols.size();
    int p = utils::num_partitions;
    int N1 = r_table.size();
    int N2 = size();

    // step 1
    r_table.randomShuffle();
    randomShuffle();

    // step 2
    auto s1 = copy();
    // s1.print();
    // s1.project(s_cols);
    s1.project(s_cols);
    // s1.print();
    s1.appendCol(1);

    auto r1 = r_table.copy();
    r1.project(r_cols);
    r1.appendCol(1);

    std::vector<int> new_join_cols;
    for (int i = 0; i < join_col_num; i++)
        new_join_cols.push_back(i);
    OperatorAdd op_add(new_join_cols, join_col_num);
    s1.sodaGroupByAggregate(op_add);
    r1.sodaGroupByAggregate(op_add);

    // step 3
    a1 = s1.max(join_col_num);
    a2 = r1.max(join_col_num);

    // step 4
    // s1.print(50, true);
    s1.union_table(r1);  //  s1 is t1 now
    // s1.print(50, true);

    // step 5, 6
    M = s1.SODA_step1(new_join_cols, join_col_num);
    // s1.print(50, true);

    //  step 7

    s1.appendCol(1);
    s1.appendCol(1);

    // s1.print(50, true);

    // step 8
    s1.SODA_step2(p);
    utils::update_phase("<soda assign bins>");

    //  step 9
    OperatorAdd op_add2(new_join_cols, s1.numColumns() - 1);
    s1.groupByPrefixAggregate(op_add2);
    // s1.print(50, true);

    // step 10
    s1.SODA_step3(p);
    utils::update_phase("<soda assign partitions>");

    // step 11
    OperatorAdd op_add3(new_join_cols, s1.numColumns() - 1);
    s1.groupByPrefixAggregate(op_add3);
    // s1.print(50, true);

    // shuffle back
    r_table.copy().shuffle(SHUFFLE_BY_KEY, {0});
    copy().shuffle(SHUFFLE_BY_KEY, {0});

    //  step 12
    r_table.parallel_for_each([&](LocalTable& table) {
        table.addCol(0);
        table.assignColE(r_table.numColumns() - 1);
    });

    // step 13
    int threshold = utils::getSizeBound(M / p + a1 * a2, p);
    int shuffle_by_col_padding_size = size_bound_soda(N1, N2, a1, a2, utils::num_partitions, threshold);
    r_table.shuffle(SHUFFLE_BY_COL, {r_table.numColumns() - 1}, -1, shuffle_by_col_padding_size);

    // step 14
    int r_col_e_id = r_table.numColumns() - 1;
    r_table.parallel_for_each([&](LocalTable& table) {
        table.deleteCol(r_col_e_id);
    });

    //  step 15
    parallel_for_each([&](LocalTable& table) {
        table.addCol(0);
        table.assignColE(numColumns() - 1);
    });

    shuffle(SHUFFLE_BY_COL, {numColumns() - 1}, -1, shuffle_by_col_padding_size);

    int s_col_e_id = numColumns() - 1;
    parallel_for_each([&](LocalTable& table) {
        table.deleteCol(s_col_e_id);
    });

    // step 16

    /* insert columns into r_table and s_table to align */
    int ori_r_col_num = r_table.numColumns();
    int ori_s_col_num = numColumns();

    // step 17
    int output_bound = ceil(M / p) + a1 * a2;
    if (a1 == 1 || a2 == 1)  // pk join
        output_bound = -1;
    localJoin(r_table, r_cols, s_cols, output_bound);
    utils::update_phase("<local join>");

    //step 18
    // shuffle(SHUFFLE_BY_KEY,{0});

    int max_n = r_table.m_localTables[0].size();
    int size_bound = utils::getSizeBound(max_n, utils::num_partitions);
    if (size_bound > max_n) size_bound = max_n;
    r_table.shuffle(SHUFFLE_BY_KEY, {0}, 0, size_bound);
}

void GlobalTable::opaque_pkjoin(GlobalTable& r_table, std::vector<int> r_cols, std::vector<int> s_cols) {
    //step 1
    r_table.mvJoinColsAhead(r_cols);
    mvJoinColsAhead(s_cols);

    int ori_r_col_num = r_table.numColumns();
    int ori_s_col_num = numColumns();
    int join_col_num = r_cols.size();

    /* insert columns into r_table and s_table to align */
    int r_start = ori_r_col_num;
    int r_align_col_num = ori_s_col_num - join_col_num;
    r_table.parallel_for_each([&](LocalTable& table) {
        for (int i = 0; i < r_align_col_num; i++)
            table.addCol(utils::DUMMY_VAL, r_start + i);
    });

    int s_start = join_col_num;
    int s_align_col_num = ori_r_col_num - join_col_num;
    parallel_for_each([&](LocalTable& table) {
        for (int i = 0; i < s_align_col_num; i++)
            table.addCol(utils::DUMMY_VAL, s_start + i);
    });
    union_table(r_table);

    // step 2
    std::vector<int> new_join_cols;
    for (int i = 0; i < join_col_num; i++)
        new_join_cols.push_back(i);
    opaque_sort(new_join_cols);

    //step 3
    appendCol(1);
    OperatorAdd op_add(new_join_cols, numColumns() - 1);
    groupByPrefixAggregate(op_add);

    //step 4
    opaque_sort(new_join_cols);
}

int GlobalTable::appendCol(int defaultVal) {
    parallel_for_each([&](LocalTable& table) {
        table.addCol(defaultVal);
    });
    return numColumns() - 1;
}

void GlobalTable::computeDegrees(GlobalTable& r_table, GlobalTable& s_table, const std::vector<int>& r_cols, const std::vector<int>& s_cols) {
    // compute self degrees
    int rr_col = r_table.appendCol(1);
    OperatorAdd o1(r_cols, rr_col);
    r_table.groupByPrefixAggregate(o1);
    OperatorMax o2(r_cols, rr_col);
    r_table.groupByPrefixAggregate(o2, true);

    int ss_col = s_table.appendCol(1);
    OperatorAdd o3(s_cols, ss_col);
    s_table.groupByPrefixAggregate(o3);
    OperatorMax o4(s_cols, ss_col);
    s_table.groupByPrefixAggregate(o4, true);

    // make a copy
    auto _s_table = s_table.copy();
    auto _r_table = r_table.copy();

    // pk join attach degrees
    auto projected_cols = s_cols;
    projected_cols.push_back(ss_col);
    _s_table.project(projected_cols);
    // auto _s_cols = s_cols;
    // std::iota(_s_cols.begin(), _s_cols.end(), 0);  // _s_cols = {0,1,...}
    _s_table.parallel_for_each([&](LocalTable& table) {
        table.remove_dup_after_prefix(s_cols);
    });
    _s_table.pkjoin(r_table, r_cols, s_cols, false);

    projected_cols = r_cols;
    projected_cols.push_back(rr_col);
    _r_table.project(projected_cols);
    _r_table.parallel_for_each([&](LocalTable& table) {
        table.remove_dup_after_prefix(r_cols);
    });
    _r_table.pkjoin(s_table, s_cols, r_cols, false);
}

void GlobalTable::join(GlobalTable& r_table, std::vector<int> r_cols, std::vector<int> s_cols, long long& M) {
    int r_num_cols = r_table.numColumns();
    int s_num_cols = numColumns();
    r_table.mvJoinColsAhead(r_cols);
    mvJoinColsAhead(s_cols);
    std::iota(r_cols.begin(), r_cols.end(), 0);
    std::iota(s_cols.begin(), s_cols.end(), 0);

    //step 1
    sort(s_cols);
    r_table.sort(r_cols);
    computeDegrees(r_table, *this, r_cols, s_cols);
    r_num_cols += 2;
    s_num_cols += 2;

    //step 2
    int rr_col = r_num_cols - 2, rs_col = r_num_cols - 1;
    M = r_table.sum(rs_col);
    
    int m = r_table.expansion(rs_col, M);
    r_table.parallel_for_each([&](LocalTable& table) {
        table.deleteCol(rr_col);
    });
    r_num_cols -= 2;  // rr_col and rs_col were removed
    int ss_col = s_num_cols - 2, sr_col = s_num_cols - 1;
    expansion(sr_col, M, false);

    //step 3
    int i_col = appendCol(1);
    OperatorAdd o1(s_cols, i_col);
    groupByPrefixAggregate(o1);
    int j_col = appendCol(1);
    OperatorAdd o2({}, j_col);
    groupByPrefixAggregate(o2);
    OperatorMin o3(s_cols, j_col);
    groupByPrefixAggregate(o3);
    s_num_cols += 2;

    //step 4
    parallel_for_each([&](LocalTable& table) {
        table.joinComputeAlignment(m);
    });
    s_num_cols += 2;
    randomShuffle();
    int size_bound = utils::getSizeBound(m, utils::num_partitions);
    shuffle(SHUFFLE_BY_COL, {s_num_cols - 1}, -1, size_bound);
    parallel_for_each_i([&](int i) {
        m_localTables[i].joinFinalCombine(r_table.m_localTables[i], r_cols.size());
    });
}

void GlobalTable::pkjoin(GlobalTable& r_table, std::vector<int> r_cols, std::vector<int> s_cols, bool need_move_cols) {
    std::random_device rd;
    int seed = std::abs(static_cast<int>(rd()));

    int join_col_num = r_cols.size();
    if (s_cols.size() != join_col_num) {
        log_error("r_table and s_table num join cols not same in pkjoin");
    }

    int ori_r_col_num = r_table.numColumns();
    int ori_s_col_num = numColumns();

    if (need_move_cols) {
        r_table.mvJoinColsAhead(r_cols);
        mvJoinColsAhead(s_cols);
    }

    /* As the join cols have been moved ahead, we need to use the new_join_cols instead */
    std::vector<int> new_join_cols;
    for (int i = 0; i < join_col_num; i++)
        new_join_cols.push_back(i);
    r_table.parallel_for_each([&](LocalTable& table) {
        table.localSort(new_join_cols);
        table.addCol(table.getId());  //add column I
        table.addCol(0);              //add column Z
        table.foreignTableModifyColZ(new_join_cols);
    });
    utils::update_phase("<pkjoin local sort R>");

    std::vector<int> r_shuffle_cols = new_join_cols;
    r_shuffle_cols.push_back(r_table.numColumns() - 1);

    r_table.shuffle(SHUFFLE_BY_KEY, r_shuffle_cols, seed, true);

    parallel_for_each([&](LocalTable& table) {
        table.addCol(-1);  //  add column I, -1 represents that this row is from s_table
        table.addCol(0);   //  add column Z, all Z values are 0 in s_table
    });
    std::vector<int> s_shuffle_cols = new_join_cols;
    s_shuffle_cols.push_back(numColumns() - 1);
    shuffle(SHUFFLE_BY_KEY, s_shuffle_cols, seed);
    // print(20);

    /* insert columns into r_table and s_table to align */
    int r_start = ori_r_col_num;
    int r_align_col_num = ori_s_col_num - join_col_num;
    r_table.parallel_for_each([&](LocalTable& table) {
        for (int i = 0; i < r_align_col_num; i++)
            table.addCol(utils::DUMMY_VAL, r_start + i);
    });

    int s_start = join_col_num;
    int s_align_col_num = ori_r_col_num - join_col_num;
    parallel_for_each([&](LocalTable& table) {
        for (int i = 0; i < s_align_col_num; i++)
            table.addCol(utils::DUMMY_VAL, s_start + i);
    });

    /* Combine r_table and s_table's local tables in the same partition */
    std::vector<int> combine_sort_cols = new_join_cols;
    combine_sort_cols.push_back(numColumns() - 1);
    combine_sort_cols.push_back(numColumns() - 2);
    parallel_for_each_i([&](int i) {
        r_table.getLocalTables()[i].pkJoinCombine(m_localTables[i].getGlobalId(), m_localTables[i].getId(), combine_sort_cols, new_join_cols, ori_r_col_num, r_align_col_num);
    });
    utils::update_phase("<pkjoin combine1>");

    int loc_size = r_table.m_localTables[0].size();
    if (loc_size % utils::num_partitions != 0) {
        log_error("shuffle by col error in pk join! loc_size=%d, num partitions=%d", loc_size, utils::num_partitions);
    }
    r_table.shuffle(SHUFFLE_BY_COL, {r_table.numColumns() - 2}, -1, loc_size / utils::num_partitions);

    /* Finalize r_table's join result */
    std::vector<int> result_sort_cols = new_join_cols;
    result_sort_cols.push_back(numColumns() - 1);  // sort by join_cols + Z col
    r_table.parallel_for_each([&](LocalTable& table) {
        table.finalizePkjoinResult(result_sort_cols, ori_r_col_num, r_align_col_num);
    });
    utils::update_phase("<pkjoin combine2>");

    /* Delete Col I and Col Z */
    int cur_col_num = ori_r_col_num + ori_s_col_num - join_col_num + 2;

    r_table.parallel_for_each([&](LocalTable& table) {
        table.deleteCol(cur_col_num - 1);  //delete column Z
        table.deleteCol(cur_col_num - 2);  //delete column I
    });
}

int GlobalTable::expansion(int d_index, long long M, bool delete_expand_col) {
    int num_cols = numColumns();
    int m = M / utils::num_partitions + (M % utils::num_partitions != 0);

    /* set dummy tuple's col D to 0 */
    parallel_for_each([&](LocalTable& table) {
        table.expansion_prepare(d_index);
    });

    /* calculate column L */
    parallel_for_each([&](LocalTable& table) {
        table.copyCol();  //  copy col D
    });
    int l_index = num_cols++;  // one column added
    OperatorAdd op_add({}, l_index);

    // print(50, true);
    groupByPrefixAggregate(op_add);
    // print(50, true);

    /* add column TP and calculate */

    parallel_for_each([&](LocalTable& table) {
        //  add column T, P and calculate their values
        table.add_and_calculate_col_t_p(d_index, m);
    });
    num_cols += 2;  //  two columns added
    randomShuffle();
    int padding_size = utils::getSizeBound(m, utils::num_partitions);
    shuffle(SHUFFLE_BY_COL, {num_cols - 2}, -1, padding_size);

    /* distribute the tuples according to col P, and delete col T,P, and add col I */

    parallel_for_each([&](LocalTable& table) {
        table.expansion_distribute_and_clear(m);
        // distribute
        // delete col P,T,L
    });
    utils::update_phase("<expansion distribute>");

    // print(5, true);

    /* Suffix sum */
    parallel_for_each([&](LocalTable& table) {
        table.expansion_suffix_sum(1);
    });
    utils::update_phase("<suffix sum phase 1>");
    // print(5, true);
    m_localTables[0].expansion_suffix_sum(2);
    utils::update_phase("<suffix sum phase 2 id 0>");
    // this two phase should be separated
    parallel_for_each_i([&](int i) {
        if (i > 0)
            m_localTables[i].expansion_suffix_sum(2);
    });
    utils::update_phase("<suffix sum phase 2 id not 0>");
    // print(5, true);

    /* delete col D */
    if (delete_expand_col) {
        parallel_for_each([&](LocalTable& table) {
            table.deleteCol(d_index);  //   delete col D
        });
    }
    return m;
}

void GlobalTable::sort(const std::vector<int>& columns) {
    randomShuffle();  // dummy elements would also be removed
    int N = size();
    int p = utils::num_partitions;
    int n0 = m_localTables[0].size();

    m_localTables[0].getPivots(columns);

    utils::update_phase("<sort get pivots>");
    parallel_for_each([&](LocalTable& table) {
        int ni = table.size();
        int size_bound = size_bound_for_sorting(N, p, n0, ni);
        table.partitionByPivots(columns, size_bound);
    });
    utils::update_phase("<sort partition by pivots>");

    parallel_for_each([&](LocalTable& table) {
        table.sortMerge(columns);
    });

    utils::update_phase("<sort local sort>");
}

void GlobalTable::opaque_sort(const std::vector<int>& columns) {
    for (int i = 0; i < 2; i++) {
        parallel_for_each([&](LocalTable& table) {
            table.localSort(columns);
        });
        utils::update_phase();
        randomShuffle();
    }
    for (int i = 0; i < 2; i++) {
        auto half = copy();
        parallel_for_each_i([&](int j) {
            m_localTables[j].localSort(columns);
            half.getLocalTables()[j].opaque_prepare_shuffle_col(-1, -1);
        });
        utils::update_phase();
        half.randomShuffle();
    }
    utils::update_phase();
}

long long GlobalTable::sum(int column) {
    long long ret = 0;
    boost::mutex mutex;
    parallel_for_each([&ret, &mutex, column](LocalTable& table) {
        long long cur_sum = table.sum(column);
        {
            boost::lock_guard<boost::mutex> lock(mutex);
            ret += cur_sum;
        }
    });
    utils::update_phase("<sum>");
    return ret;
}

int GlobalTable::max(int column) {
    int ret = 0;
    boost::mutex mutex;
    parallel_for_each([&ret, &mutex, column](LocalTable& table) {
        int cur = table.max(column);
        int cond = cur > ret;
        {
            boost::lock_guard<boost::mutex> lock(mutex);
            ret ^= (-cond) & (ret ^ cur);
        }
    });
    utils::update_phase("<max>");
    return ret;
}

long long GlobalTable::SODA_step1(std::vector<int> columns, int aggCol) {
    long long ret = 0;
    boost::mutex mutex;
    parallel_for_each([&ret, &mutex, &columns, aggCol](LocalTable& table) {
        long long cur_ret = table.SODA_step1(columns, aggCol);
        {
            boost::lock_guard<boost::mutex> lock(mutex);
            ret += cur_ret;
        }
    });

    return ret;
}

void GlobalTable::SODA_step2(int p) {
    parallel_for_each([p](LocalTable& table) {
        table.SODA_step2(p);
    });
}

void GlobalTable::SODA_step3(int p) {
    parallel_for_each([p](LocalTable& table) {
        table.SODA_step3(p);
    });
}

void GlobalTable::localJoin(GlobalTable& r_table, std::vector<int> r_cols, std::vector<int> s_cols, int& output_bound) {
    mvJoinColsAhead(s_cols);
    r_table.mvJoinColsAhead(r_cols);
    // when output_bound = 0 (single join), it is modified to the true value
    parallel_for_each_i([&](int i) {
        output_bound = m_localTables[i].localJoin(r_table.id, r_table.m_localTables[i].getId(), r_cols.size(), output_bound);
    });
}

void GlobalTable::SODA_step5() {
    parallel_for_each([](LocalTable& table) {
        table.SODA_step5();
    });
}

void GlobalTable::groupByPrefixAggregate(AssociateOperator& op, bool reverse) {
    // std::vector<Tuple> last_tuples;

    /* phase1:
     * each local table do prefix sum and write the last tuple into files 
     * local_table i write to gid(x)_lid(i)_tlid0
    */
    parallel_for_each([&](LocalTable& table) {
        table.groupByPrefixAggregate(op, 1, reverse);
    });
    // m_stat.addComm(utils::num_partitions * (1 + op.group_by_columns.size()));

    /* phase2:
     * local table 0 acts as a role to calculate, write the result to files, but not change its own data 
     * the read tuple num will be (num_partitions - 1), after do prefix sum to those tuples,
     * write the i'th result tuple to gid(x)_lid0_tlid(i+1)
    */
    m_localTables[0].groupByPrefixAggregate(op, 2, reverse);
    // LocalTable last_tuples_table(last_tuples);
    // last_tuples_table.groupByPrefixAggregate(op, 2);

    /* phase3: 
     * add to [1, ..., utils::num_partitions-1 ] local_tables
     */
    int start = 0, end = utils::num_partitions - 1, step = 1;
    if (reverse) {
        start = 1;
        end = utils::num_partitions;
        step = -1;
    }
    parallel_for_each_i([&](int i) {
        if ((!reverse && i > 0) || (reverse && i < utils::num_partitions - 1))
            m_localTables[i]._addValueByKey(op);
    });
}

// Input not need to be sorted
void GlobalTable::sodaGroupByAggregate(AssociateOperator& op) {
    auto group_by_columns = op.group_by_columns;
    int num_columns = group_by_columns.size();
    parallel_for_each([&](LocalTable& table) {
        table.localSort(group_by_columns);
        table.groupByAggregate(op);
    });

    /* do projection */
    auto project_cols = group_by_columns;
    project_cols.push_back(op.aggregate_column);
    project(project_cols);

    utils::update_phase("<local agg1>");

    // form new group_by_columns after projection
    for (int i = 0; i < num_columns; i++)
        group_by_columns[i] = i;

    shuffle(SHUFFLE_BY_KEY, group_by_columns);

    op.group_by_columns = group_by_columns;
    op.aggregate_column = num_columns;

    parallel_for_each([&](LocalTable& table) {
        table.localSort(group_by_columns);
        table.groupByAggregate(op);
    });
    utils::update_phase("<local agg2>");
}

void GlobalTable::soda_shuffleByKey(const std::vector<int>& cols) {
    parallel_for_each([&](LocalTable& table) {
        table.soda_shuffleByKey(utils::num_partitions, cols, 0);
    });
    utils::update_phase();
}

void GlobalTable::removeDummy() {
    parallel_for_each([&](LocalTable& table) {
        table.removeDummy();
    });
}

// input must be vectors with equal size
void transpose(std::vector<LocalTable>& input, std::vector<LocalTable>& output) {
    int p = input.size();
    int n = input[0].size();
    auto index = computeQuatileIndex(n, p);
    output.clear();
    output.resize(p);
    for (int i = 0; i < p; i++)
        for (int j = index[i]; j < index[i + 1]; j++)
            for (int k = 0; k < p; k++)
                output[i].getData().push_back(input[k].getData()[j]);
}

void antiTranspose(std::vector<LocalTable>& input, std::vector<LocalTable>& output) {
    int p = input.size();
    output.clear();
    output.resize(p);
    for (int i = 0; i < p; i++) {
        auto tuples = input[i].getData();
        int n = tuples.size();
        assert(n % p == 0 && "Anti Transpose: Input Shape Error");
        for (int j = 0; j < n / p; j++)
            for (int k = 0; k < p; k++)
                output[k].getData().push_back(tuples[j * p + k]);
    }
}
