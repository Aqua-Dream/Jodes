#include "LocalTable.h"
#include <algorithm>
#include <iostream>
#include <random>
//#include "define.h"
#include <cmath>
#include <fstream>
#include <unordered_set>
#include <vector>
#include "App.h"
#include "Enclave_u.h"
#include "ReqSender.h"
#include "log.h"
#include "utils.h"

extern sgx_enclave_id_t global_eid; /* global enclave id */

LocalTable::LocalTable(int global_id_, int id_, std::string filePath_, bool is_handle, std::string url) : global_id(global_id_), id(id_), filePath(filePath_), is_handle_(is_handle), url_(url) {
    if (is_handle_) {
        // if it is handle itself, send request to remote LocalTable to execute
        unordered_map<string, string> header_map = {
            {"task",      "create_local_table"      },
            {"global_id", std::to_string(global_id_)},
            {"local_id",  std::to_string(id_)       },
            {"file_path", filePath_                 }
        };
        send_get_request(url_, header_map);
    } else {
        m_tuples = std::vector<Tuple>();
        if (filePath_ == "") {
            return;
        }

        size_t file_length;
        uint8_t* file = utils::readPartitionFile(filePath, &file_length);

        if (file_length > 0) {
            int ret;
            sgx_status_t ecall_status = ecall_read_file(global_eid, &ret, global_id_, id_, file, file_length);
            delete[] file;
            if (ecall_status) {
                print_error_message(ecall_status);
                log_error("ecall failed");
            }
        }
    }
}

const int LocalTable::size() {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",      "size"                   },
            {"global_id", std::to_string(global_id)}
        };
        std::string ret = utils::extractRet(send_get_request(url_, header_map));
        return std::stoi(ret);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_size(global_eid, &ret, global_id, id);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }

        return ret;
    }
}

const int LocalTable::num_columns() {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",      "num_columns"            },
 // {"worker_id", std::to_string(id_)},
            {"global_id", std::to_string(global_id)}
        };
        std::string ret = utils::extractRet(send_get_request(url_, header_map));
        log_debug("num_columns ret is %s", ret.c_str());
        return std::stoi(ret);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_num_columns(global_eid, &ret, global_id, id);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }

        return ret;
    }
}

std::vector<Tuple>& LocalTable::getData() {
    return m_tuples;
}

void LocalTable::print(int limit_size, bool show_dummy)  //TODO::: move the logic to enclave
{
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",       "print"                   },
            {"limit_size", std::to_string(limit_size)},
            {"show_dummy", std::to_string(show_dummy)},
            {"global_id",  std::to_string(global_id) }
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_print(global_eid, &ret, global_id, id, limit_size, show_dummy);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

void LocalTable::showInfo() {
    std::cout << "global_id is " << global_id << "; local_id is " << id << std::endl;
}

LocalTable LocalTable::copy(int new_global_id) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",          "copy"                       },
            {"global_id",     std::to_string(global_id)    },
            {"new_global_id", std::to_string(new_global_id)}
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_copy(global_eid, &ret, global_id, id, new_global_id);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }

    if (is_handle_)
        return LocalTable(new_global_id, id, "", true, url_);
    else
        return LocalTable(new_global_id, id, false);
}

long long int LocalTable::hash(int seed) const {
    long long int res = 0;
    for (auto& tuple : m_tuples)
        res ^= tuple.hash(seed);
    return res;
}

bool LocalTable::isKeyUnique(const std::vector<int> key) {
    std::unordered_set<long long int> hash_list;
    std::random_device rd;
    auto seed = rd();
    for (auto& tuple : m_tuples) {
        if (tuple.is_dummy)
            continue;
        long long int hash_value = tuple.getSubTuple(key).hash(seed);
        if (hash_list.find(hash_value) != hash_list.end())
            return false;
        hash_list.insert(hash_value);
    }
    return true;
}

void LocalTable::sample(double rate, std::vector<Tuple>& output) {
    output.clear();
    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_real_distribution<> dist;
    for (auto tuple : m_tuples) {
        if (dist(rng) < rate)
            output.push_back(tuple);
    }
}

void LocalTable::partitionByPivots(const std::vector<int> columns, int size_bound) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",       "partitionByPivots"           },
            {"columns",    utils::vectorToString(columns)},
            {"size_bound", std::to_string(size_bound)    },
            {"global_id",  std::to_string(global_id)     }
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_partitionByPivots(global_eid, &ret, global_id, id, const_cast<int*>(columns.data()), columns.size(), size_bound);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

void LocalTable::shuffleByCol(int num_partitions, int i_col_id, int size_bound) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",           "shuffleByCol"                },
            {"global_id",      std::to_string(global_id)     },
            {"num_partitions", std::to_string(num_partitions)},
            {"i_col_id",       std::to_string(i_col_id)      },
            {"size_bound",     std::to_string(size_bound)    }
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_shuffleByCol(global_eid, &ret, global_id, id, num_partitions, i_col_id, size_bound);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

void LocalTable::shuffleByKey(int num_partitions, const std::vector<int> key, int seed, int size_bound) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",           "shuffleByKey"                },
            {"global_id",      std::to_string(global_id)     },
            {"num_partitions", std::to_string(num_partitions)},
            {"key",            utils::vectorToString(key)    },
            {"seed",           std::to_string(seed)          },
            {"size_bound",     std::to_string(size_bound)    }
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_shuffleByKey(global_eid, &ret, global_id, id, num_partitions, const_cast<int*>(key.data()), key.size(), seed, size_bound);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

void LocalTable::shuffleMerge(int num_partitions) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",           "shuffleMerge"                },
            {"global_id",      std::to_string(global_id)     },
            {"num_partitions", std::to_string(num_partitions)}
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_shuffleMerge(global_eid, &ret, global_id, id, num_partitions);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

void LocalTable::randomShuffle(int num_partitions) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",           "randomShuffle"               },
            {"global_id",      std::to_string(global_id)     },
            {"num_partitions", std::to_string(num_partitions)}
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_randomShuffle(global_eid, &ret, global_id, id, num_partitions);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

void LocalTable::sortMerge(const std::vector<int> columns) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",      "sortMerge"                   },
            {"columns",   utils::vectorToString(columns)},
            {"global_id", std::to_string(global_id)     }
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_sortMerge(global_eid, &ret, global_id, id, const_cast<int*>(columns.data()), columns.size());
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

void LocalTable::localSort(const std::vector<int> columns) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",      "localSort"                   },
            {"columns",   utils::vectorToString(columns)},
            {"global_id", std::to_string(global_id)     }
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_localSort(global_eid, &ret, global_id, id, const_cast<int*>(columns.data()), columns.size());
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

void LocalTable::pad_to_size(int n) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",      "pad_to_size"            },
            {"global_id", std::to_string(global_id)},
            {"n",         std::to_string(n)        }
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_pad_to_size(global_eid, &ret, global_id, id, n);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

void LocalTable::opaque_prepare_shuffle_col(int col_id, int tuple_num) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",      "opaque_prepare_shuffle_col"},
            {"global_id", std::to_string(global_id)   },
            {"col_id",    std::to_string(col_id)      },
            {"tuple_num", std::to_string(tuple_num)   }
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_opaque_prepare_shuffle_col(global_eid, &ret, global_id, id, col_id, tuple_num);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

void LocalTable::foreignTableModifyColZ(const std::vector<int> columns) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",      "foreignTableModifyColZ"      },
            {"columns",   utils::vectorToString(columns)},
            {"global_id", std::to_string(global_id)     }
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_foreignTableModifyColZ(global_eid, &ret, global_id, id, const_cast<int*>(columns.data()), columns.size());
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

void LocalTable::copyCol(int col_index) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",      "copyCol"                },
            {"global_id", std::to_string(global_id)},
            {"col_index", std::to_string(col_index)}
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_copyCol(global_eid, &ret, global_id, id, col_index);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

void LocalTable::expansion_prepare(int d_index) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",      "expansion_prepare"      },
            {"global_id", std::to_string(global_id)},
            {"d_index",   std::to_string(d_index)  }
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_expansion_prepare(global_eid, &ret, global_id, id, d_index);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

void LocalTable::add_and_calculate_col_t_p(int d_index, int m) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",      "add_and_calculate_col_t_p"},
            {"global_id", std::to_string(global_id)  },
            {"d_index",   std::to_string(d_index)    },
            {"m",         std::to_string(m)          }
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_add_and_calculate_col_t_p(global_eid, &ret, global_id, id, d_index, m);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

void LocalTable::expansion_suffix_sum(int phase) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",      "expansion_suffix_sum"   },
            {"global_id", std::to_string(global_id)},
            {"phase",     std::to_string(phase)    }
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_expansion_suffix_sum(global_eid, &ret, global_id, id, phase);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

void LocalTable::expansion_distribute_and_clear(int m) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",      "expansion_distribute_and_clear"},
            {"global_id", std::to_string(global_id)       },
            {"m",         std::to_string(m)               }
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_expansion_distribute_and_clear(global_eid, &ret, global_id, id, m);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

void LocalTable::finalizePkjoinResult(const std::vector<int> columns, int ori_r_col_num, int r_align_col_num) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",            "finalizePkjoinResult"         },
            {"columns",         utils::vectorToString(columns) },
            {"ori_r_col_num",   std::to_string(ori_r_col_num)  },
            {"r_align_col_num", std::to_string(r_align_col_num)},
            {"global_id",       std::to_string(global_id)      }
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_finalizePkjoinResult(global_eid, &ret, global_id, id, const_cast<int*>(columns.data()), columns.size(), ori_r_col_num, r_align_col_num);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

void LocalTable::joinComputeAlignment(int m) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",      "joinComputeAlignment"   },
            {"m",         std::to_string(m)        },
            {"global_id", std::to_string(global_id)}
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_joinComputeAlignment(global_eid, &ret, global_id, id, m);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

void LocalTable::joinFinalCombine(LocalTable& r_table, int num_join_cols) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",              "joinFinalCombine"                   },
            {"num_join_cols",     std::to_string(num_join_cols)        },
            {"global_id",         std::to_string(global_id)            },
            {"r_table_global_id", std::to_string(r_table.getGlobalId())}
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_joinFinalCombine(global_eid, &ret, global_id, id, r_table.getGlobalId(), num_join_cols);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

int LocalTable::getId() const {
    return id;
}

int LocalTable::getGlobalId() {
    return global_id;
}

void LocalTable::pkJoinCombine(
    int s_table_local_global_id,
    int s_table_local_id,
    std::vector<int>& combine_sort_cols,
    std::vector<int>& new_join_cols,
    int ori_r_col_num,
    int r_align_col_num) {
    if (id != s_table_local_id) {
        log_error("The r_table_local's id(%d) not equal to s_table_local's id(%d)", id, s_table_local_id);
    }

    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",                    "pkJoinCombine"                         },
            {"global_id",               std::to_string(global_id)               },
            {"s_table_local_global_id", std::to_string(s_table_local_global_id) },
            {"s_table_local_id",        std::to_string(s_table_local_id)        },
            {"combine_sort_cols",       utils::vectorToString(combine_sort_cols)},
            {"new_join_cols",           utils::vectorToString(new_join_cols)    },
            {"ori_r_col_num",           std::to_string(ori_r_col_num)           },
            {"r_align_col_num",         std::to_string(r_align_col_num)         },
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_pkJoinCombine(global_eid, &ret, global_id, id,
                                                        s_table_local_global_id, s_table_local_id,
                                                        ori_r_col_num, r_align_col_num,
                                                        const_cast<int*>(combine_sort_cols.data()), combine_sort_cols.size(),
                                                        const_cast<int*>(new_join_cols.data()), new_join_cols.size());
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

void LocalTable::addCol(int defaultVal, int col_index) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",       "addCol"                  },
            {"global_id",  std::to_string(global_id) },
            {"defaultVal", std::to_string(defaultVal)},
            {"col_index",  std::to_string(col_index) }
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_addCol(global_eid, &ret, global_id, id, defaultVal, col_index);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

void LocalTable::deleteCol(int col_index) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",      "deleteCol"              },
            {"global_id", std::to_string(global_id)},
            {"col_index", std::to_string(col_index)}
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_deleteCol(global_eid, &ret, global_id, id, col_index);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

void LocalTable::getPivots(const std::vector<int> columns) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",      "getPivots"                   },
            {"columns",   utils::vectorToString(columns)},
            {"global_id", std::to_string(global_id)     }
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_getPivots(global_eid, &ret, global_id, id, const_cast<int*>(columns.data()), columns.size());
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

void LocalTable::remove_dup_after_prefix(const std::vector<int>& columns) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",      "remove_dup_after_prefix"     },
            {"columns",   utils::vectorToString(columns)},
            {"global_id", std::to_string(global_id)     }
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_remove_dup_after_prefix(global_eid, &ret, global_id, id, const_cast<int*>(columns.data()), columns.size());
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

void LocalTable::project(const std::vector<int>& columns) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",      "project"                     },
            {"columns",   utils::vectorToString(columns)},
            {"global_id", std::to_string(global_id)     }
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_project(global_eid, &ret, global_id, id, const_cast<int*>(columns.data()), columns.size());
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

long long LocalTable::sum(int column) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",      "sum"                    },
            {"column",    std::to_string(column)   },
            {"global_id", std::to_string(global_id)}
        };
        std::string ret = utils::extractRet(send_get_request(url_, header_map));
        log_debug("sum ret is %s", ret.c_str());

        return std::stoll(ret);
    } else {
        long long ret;
        sgx_status_t ecall_status = ecall_sum(global_eid, &ret, global_id, id, column);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
        return ret;
    }
}

Tuple LocalTable::groupByAggregateBase(AssociateOperator& op, bool doPrefix, int phase, bool reverse) {
    if (doPrefix && phase != 1 && phase != 2) {
        std::cerr << "ERROR: groupByAggregateBase requires phase to be 1 or 2 in groupByPrefixAggregate, " << phase << " is not supported" << std::endl;
        throw;
    }

    int ret;
    sgx_status_t ecall_status = ecall_groupByAggregateBase(global_eid, &ret, global_id, id, static_cast<void*>(&op), doPrefix, phase, reverse);
    if (ecall_status) {
        print_error_message(ecall_status);
        log_error("ecall failed");
    }

    return Tuple();
}

void LocalTable::mvJoinColsAhead(std::vector<int> join_cols) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",      "mvJoinColsAhead"               },
            {"join_cols", utils::vectorToString(join_cols)},
            {"global_id", std::to_string(global_id)       }
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_mvJoinColsAhead(global_eid, &ret, global_id, id, const_cast<int*>(join_cols.data()), join_cols.size());
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

Tuple LocalTable::groupByPrefixAggregate(AssociateOperator& op, int phase, bool reverse) {
    /* pre-check */
    if (phase != 1 && phase != 2) {
        std::cerr << "ERROR: Phase can only be 1 or 2 in groupByPrefixAggregate, " << phase << " is not supported" << std::endl;
        throw;
    }
    if (phase == 2) {
        if (id != 0) {
            std::cerr << "ERROR: phase 2 can only be performed by local_table 0, local_table " << id << " can not do this" << std::endl;
            throw;
        }
    }

    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",      "groupByPrefixAggregate"    },
            {"op",        utils::serializeOperator(op)},
            {"phase",     std::to_string(phase)       },
            {"reverse",   std::to_string(reverse)     },
            {"global_id", std::to_string(global_id)   }
        };
        send_get_request(url_, header_map);
        return Tuple();  // always return dummy tuple. Meaningless.
    } else {
        return groupByAggregateBase(op, true, phase, reverse);
    }
}

void LocalTable::_addValueByKey(AssociateOperator& op) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",      "_addValueByKey"            },
            {"op",        utils::serializeOperator(op)},
            {"global_id", std::to_string(global_id)   }
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_addValueByKey(global_eid, &ret, global_id, id, static_cast<void*>(&op));
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

void LocalTable::generateData(int num_cols, int num_rows) {
    int ret;
    sgx_status_t ecall_status = ecall_generateData(global_eid, &ret, global_id, id, num_cols, num_rows);
    if (ecall_status) {
        print_error_message(ecall_status);
        log_error("ecall failed");
    }
}

void LocalTable::soda_shuffleByKey(int num_partitions, const std::vector<int>& key, int size_bound) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",      "soda_shuffleByKey"       },
            {"key",       utils::vectorToString(key)},
            {"global_id", std::to_string(global_id) }
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_soda_shuffleByKey(global_eid, &ret, global_id, id, num_partitions, const_cast<int*>(key.data()), key.size(), 0, size_bound);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

int LocalTable::max(int column) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",      "max"                    },
            {"column",    std::to_string(column)   },
            {"global_id", std::to_string(global_id)}
        };
        std::string ret = utils::extractRet(send_get_request(url_, header_map));
        log_debug("max ret is %d", ret);

        return std::stoi(ret);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_max(global_eid, &ret, global_id, id, column);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }

        return ret;
    }
}

int LocalTable::union_table(int table_global_id, int table_id) {
    if (table_id != id) {
        std::cerr << "ERROR: table_id " << table_id << " is not equal to id " << id << std::endl;
        throw;
    }
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",            "union_table"                  },
            {"global_id",       std::to_string(global_id)      },
            {"table_global_id", std::to_string(table_global_id)},
            {"table_id",        std::to_string(table_id)       }
        };
        send_get_request(url_, header_map);
        return 0;
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_union(global_eid, &ret, global_id, id, table_global_id, table_id);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }

        return ret;
    }
}

long long LocalTable::SODA_step1(std::vector<int> columns, int aggCol) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",      "SODA_step1"                  },
            {"columns",   utils::vectorToString(columns)},
            {"aggCol",    std::to_string(aggCol)        },
            {"global_id", std::to_string(global_id)     }
        };
        std::string ret = utils::extractRet(send_get_request(url_, header_map));
        log_debug("SODA_step1 ret is %s", ret.c_str());

        return std::stoll(ret);
    } else {
        long long ret;

        sgx_status_t ecall_status = ecall_SODA_step1(global_eid, &ret, global_id, id, const_cast<int*>(columns.data()), columns.size(), aggCol);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }

        return ret;
    }
}

void LocalTable::SODA_step2(int p) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",      "SODA_step2"             },
            {"global_id", std::to_string(global_id)},
            {"p",         std::to_string(p)        }
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_SODA_step2(global_eid, &ret, global_id, id, p);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

void LocalTable::SODA_step3(int p) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",      "SODA_step3"             },
            {"global_id", std::to_string(global_id)},
            {"p",         std::to_string(p)        }
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_SODA_step3(global_eid, &ret, global_id, id, p);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

// this function only works in standalone setting
int LocalTable::localJoin(int other_global_id, int other_id, int join_col_num, int output_bound) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",            "localJoin"                    },
            {"join_col_num",    std::to_string(join_col_num)   },
            {"output_bound",    std::to_string(output_bound)   },
            {"global_id",       std::to_string(global_id)      },
            {"other_global_id", std::to_string(other_global_id)},
            {"other_id",        std::to_string(other_id)       }
        };
        std::string ret = utils::extractRet(send_get_request(url_, header_map));

        return std::stoi(ret);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_localJoin(global_eid, &ret, global_id, id, other_global_id, other_id, join_col_num, output_bound);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
        return ret;
    }
}

void LocalTable::SODA_step5() {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",      "SODA_step5"             },
            {"global_id", std::to_string(global_id)}
        };
        utils::extractRet(send_get_request(url_, header_map));

    } else {
        int ret;
        sgx_status_t ecall_status = ecall_SODA_step5(global_eid, &ret, global_id, id);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

void LocalTable::assignColE(int col) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",      "assignColE"             },
            {"global_id", std::to_string(global_id)},
            {"col",       std::to_string(col)      }
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_assignColE(global_eid, &ret, global_id, id, col);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}

void LocalTable::groupByAggregate(AssociateOperator& op) {
    if (is_handle_) {
        unordered_map<string, string> header_map = {
            {"task",      "groupByAggregate"          },
            {"op",        utils::serializeOperator(op)},
            {"global_id", std::to_string(global_id)   }
        };
        send_get_request(url_, header_map);
    } else {
        groupByAggregateBase(op, false);
    }
}

void LocalTable::removeDummy() {
    std::vector<Tuple> nondummy_tuples;
    for (auto& tuple : m_tuples)
        if (!tuple.is_dummy)
            nondummy_tuples.push_back(tuple);
    m_tuples = nondummy_tuples;
}

void LocalTable::destroy() {
    if (is_handle_) {
        std::unordered_map<std::string, std::string> header_map = {
            {"task",      "destroy"                },
            {"global_id", std::to_string(global_id)}
        };
        send_get_request(url_, header_map);
    } else {
        int ret;
        sgx_status_t ecall_status = ecall_destroy(global_eid, &ret, global_id, utils::num_partitions);
        if (ecall_status) {
            print_error_message(ecall_status);
            log_error("ecall failed");
        }
    }
}