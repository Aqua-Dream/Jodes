/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "EchoHandler.h"

#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/httpserver/ResponseBuilder.h>

// #include <mutex>
#include "App.h"
#include "EchoStats.h"
#include "Enclave_u.h"
#include "LocalTable.h"
#include "log.h"
#include "utils.h"

// std::mutex mtx;
extern sgx_enclave_id_t global_eid; /* global enclave id */

using namespace proxygen;
using namespace std;
using namespace utils;

DEFINE_bool(request_number,
            true,
            "Include request sequence number in response");

namespace EchoService {

EchoHandler::EchoHandler(EchoStats* stats) : stats_(stats) {
}

unordered_map<string, string> headerToMap(HTTPHeaders header) {
    unordered_map<string, string> ret;
    header.forEach([&](std::string& name, std::string& value) {
        ret.emplace(name, value);
    });

    return ret;
}

std::unordered_map<int, std::shared_ptr<LocalTable> > localTableMap;

std::string executeTask(unordered_map<string, string>& task) {
    std::string task_ret = "";
    if (task["task"] != "destroy")
        log_info("Executing task: %s", task["task"].c_str());
    else
        log_debug("Destroying table");
    if (task["task"] == "read_config_file") {
        utils::read_config_file("../data/shuffle_buffer/config_" + task["worker_id"], true);
    } else if (task["task"] == "create_local_table") {
        int global_id = stoi(task["global_id"]);
        int local_id = stoi(task["local_id"]);
        std::string file_path = task["file_path"];
        localTableMap[global_id] = make_shared<LocalTable>(global_id, local_id, file_path);
    } else if (task["task"] == "copy") {
        int global_id = stoi(task["global_id"]);
        int new_global_id = stoi(task["new_global_id"]);
        localTableMap[new_global_id] = std::make_shared<LocalTable>(localTableMap[global_id]->copy(new_global_id));
    } else if (task["task"] == "size") {
        int global_id = stoi(task["global_id"]);
        int ret = localTableMap[global_id]->size();
        log_debug("ret size is %d", ret);
        task_ret = std::to_string(ret);
    } else if (task["task"] == "sum") {
        int global_id = stoi(task["global_id"]);
        int column = stoi(task["column"]);
        long long ret = localTableMap[global_id]->sum(column);
        log_debug("ret sum is %d", ret);
        task_ret = std::to_string(ret);
    } else if (task["task"] == "max") {
        int global_id = stoi(task["global_id"]);
        int column = stoi(task["column"]);
        int ret = localTableMap[global_id]->max(column);
        log_debug("ret max is %d", ret);
        task_ret = std::to_string(ret);
    } else if (task["task"] == "SODA_step1") {
        int global_id = stoi(task["global_id"]);
        const std::vector<int> columns = stringToVector(task["columns"]);
        int aggCol = stoi(task["aggCol"]);
        long long ret = localTableMap[global_id]->SODA_step1(columns, aggCol);
        log_debug("ret SODA_step1 is %lld", ret);
        task_ret = std::to_string(ret);
    } else if (task["task"] == "SODA_step2") {
        int global_id = stoi(task["global_id"]);
        int p = stoi(task["p"]);
        localTableMap[global_id]->SODA_step2(p);
    } else if (task["task"] == "SODA_step3") {
        int global_id = stoi(task["global_id"]);
        int p = stoi(task["p"]);
        localTableMap[global_id]->SODA_step3(p);
    } else if (task["task"] == "localJoin") {
        int global_id = stoi(task["global_id"]);
        int other_global_id = stoi(task["other_global_id"]);
        int other_id = stoi(task["other_id"]);
        int join_col_num = stoi(task["join_col_num"]);
        int output_bound = stoi(task["output_bound"]);
        int ret = localTableMap[global_id]->localJoin(other_global_id, other_id, join_col_num, output_bound);
        task_ret = std::to_string(ret);
    } else if (task["task"] == "SODA_step5") {
        int global_id = stoi(task["global_id"]);
        localTableMap[global_id]->SODA_step5();
    } else if (task["task"] == "assignColE") {
        int global_id = stoi(task["global_id"]);
        int col = stoi(task["col"]);
        localTableMap[global_id]->assignColE(col);
    } else if (task["task"] == "num_columns") {
        int global_id = stoi(task["global_id"]);
        int ret = localTableMap[global_id]->num_columns();
        log_debug("ret num_columns is %d", ret);
        task_ret = std::to_string(ret);
    } else if (task["task"] == "print") {
        int global_id = stoi(task["global_id"]);
        int limit_size = stoi(task["limit_size"]);
        bool show_dummy = stoi(task["show_dummy"]);
        localTableMap[global_id]->print(limit_size, show_dummy);
    } else if (task["task"] == "union_table") {
        int global_id = stoi(task["global_id"]);
        int table_global_id = stoi(task["table_global_id"]);
        int table_id = stoi(task["table_id"]);
        localTableMap[global_id]->union_table(table_global_id, table_id);
    } else if (task["task"] == "pad_to_size") {
        int global_id = stoi(task["global_id"]);
        int n = stoi(task["n"]);
        localTableMap[global_id]->pad_to_size(n);
    } else if (task["task"] == "opaque_prepare_shuffle_col") {
        int global_id = stoi(task["global_id"]);
        int col_id = stoi(task["col_id"]);
        int tuple_num = stoi(task["tuple_num"]);
        localTableMap[global_id]->opaque_prepare_shuffle_col(col_id, tuple_num);
    } else if (task["task"] == "shuffleMerge") {
        int global_id = stoi(task["global_id"]);
        int num_partitions = stoi(task["num_partitions"]);
        localTableMap[global_id]->shuffleMerge(num_partitions);
    } else if (task["task"] == "randomShuffle") {
        int global_id = stoi(task["global_id"]);
        int num_partitions = stoi(task["num_partitions"]);
        localTableMap[global_id]->randomShuffle(num_partitions);
    } else if (task["task"] == "shuffleByKey") {
        int global_id = stoi(task["global_id"]);
        int num_partitions = stoi(task["num_partitions"]);
        const std::vector<int> key = stringToVector(task["key"]);
        int seed = stoi(task["seed"]);
        int size_bound = stoi(task["size_bound"]);
        localTableMap[global_id]->shuffleByKey(num_partitions, key, seed, size_bound);
    } else if (task["task"] == "shuffleByCol") {
        int global_id = stoi(task["global_id"]);
        int num_partitions = stoi(task["num_partitions"]);
        int i_col_id = stoi(task["i_col_id"]);
        int size_bound = stoi(task["size_bound"]);
        localTableMap[global_id]->shuffleByCol(num_partitions, i_col_id, size_bound);
    } else if (task["task"] == "partitionByPivots") {
        int global_id = stoi(task["global_id"]);
        const std::vector<int> columns = stringToVector(task["columns"]);
        int size_bound = stoi(task["size_bound"]);
        localTableMap[global_id]->partitionByPivots(columns, size_bound);
    } else if (task["task"] == "groupByPrefixAggregate") {
        int global_id = stoi(task["global_id"]);
        AssociateOperator* op = deserializeOperator(task["op"]);
        int phase = stoi(task["phase"]);
        bool reverse = task["reverse"] == "1";
        localTableMap[global_id]->groupByPrefixAggregate(*op, phase);
    } else if (task["task"] == "groupByAggregate") {
        int global_id = stoi(task["global_id"]);
        AssociateOperator* op = deserializeOperator(task["op"]);
        localTableMap[global_id]->groupByAggregate(*op);
    } else if (task["task"] == "_addValueByKey") {
        int global_id = stoi(task["global_id"]);
        AssociateOperator* op = deserializeOperator(task["op"]);
        localTableMap[global_id]->_addValueByKey(*op);
    } else if (task["task"] == "mvJoinColsAhead") {
        int global_id = stoi(task["global_id"]);
        const std::vector<int> join_cols = stringToVector(task["join_cols"]);
        localTableMap[global_id]->mvJoinColsAhead(join_cols);
    } else if (task["task"] == "pkJoinCombine") {
        int global_id = stoi(task["global_id"]);
        int s_table_local_global_id = stoi(task["s_table_local_global_id"]);
        int s_table_local_id = stoi(task["s_table_local_id"]);
        std::vector<int> combine_sort_cols = stringToVector(task["combine_sort_cols"]);
        std::vector<int> new_join_cols = stringToVector(task["new_join_cols"]);
        int ori_r_col_num = stoi(task["ori_r_col_num"]);
        int r_align_col_num = stoi(task["r_align_col_num"]);

        localTableMap[global_id]->pkJoinCombine(s_table_local_global_id,
                                                s_table_local_id,
                                                combine_sort_cols,
                                                new_join_cols,
                                                ori_r_col_num,
                                                r_align_col_num);
    } else if (task["task"] == "foreignTableModifyColZ") {
        int global_id = stoi(task["global_id"]);
        const std::vector<int> columns = stringToVector(task["columns"]);
        localTableMap[global_id]->foreignTableModifyColZ(columns);
    } else if (task["task"] == "remove_dup_after_prefix") {
        int global_id = stoi(task["global_id"]);
        const std::vector<int> columns = stringToVector(task["columns"]);
        localTableMap[global_id]->remove_dup_after_prefix(columns);
    } else if (task["task"] == "finalizePkjoinResult") {
        int global_id = stoi(task["global_id"]);
        const std::vector<int> columns = stringToVector(task["columns"]);
        int ori_r_col_num = stoi(task["ori_r_col_num"]);
        int r_align_col_num = stoi(task["r_align_col_num"]);
        localTableMap[global_id]->finalizePkjoinResult(columns, ori_r_col_num, r_align_col_num);
    } else if (task["task"] == "joinComputeAlignment") {
        int global_id = stoi(task["global_id"]);
        int m = stoi(task["m"]);
        localTableMap[global_id]->joinComputeAlignment(m);
    } else if (task["task"] == "joinFinalCombine") {
        int global_id = stoi(task["global_id"]);
        int r_table_global_id = stoi(task["r_table_global_id"]);
        int num_join_cols = stoi(task["num_join_cols"]);
        localTableMap[global_id]->joinFinalCombine(*localTableMap[r_table_global_id], num_join_cols);
    } else if (task["task"] == "sortMerge") {
        int global_id = stoi(task["global_id"]);
        const std::vector<int> columns = stringToVector(task["columns"]);
        localTableMap[global_id]->sortMerge(columns);
    } else if (task["task"] == "project") {
        int global_id = stoi(task["global_id"]);
        const std::vector<int> columns = stringToVector(task["columns"]);
        localTableMap[global_id]->project(columns);
    } else if (task["task"] == "getPivots") {
        int global_id = stoi(task["global_id"]);
        const std::vector<int> columns = stringToVector(task["columns"]);
        localTableMap[global_id]->getPivots(columns);
    } else if (task["task"] == "localSort") {
        int global_id = stoi(task["global_id"]);
        const std::vector<int> columns = stringToVector(task["columns"]);
        localTableMap[global_id]->localSort(columns);
    } else if (task["task"] == "expansion_prepare") {
        int global_id = stoi(task["global_id"]);
        int d_index = stoi(task["d_index"]);
        localTableMap[global_id]->expansion_prepare(d_index);
    } else if (task["task"] == "copyCol") {
        int global_id = stoi(task["global_id"]);
        int col_index = stoi(task["col_index"]);
        localTableMap[global_id]->copyCol(col_index);
    } else if (task["task"] == "addCol") {
        int global_id = stoi(task["global_id"]);
        int defaultVal = stoi(task["defaultVal"]);
        int col_index = stoi(task["col_index"]);
        localTableMap[global_id]->addCol(defaultVal, col_index);
    } else if (task["task"] == "add_and_calculate_col_t_p") {
        int global_id = stoi(task["global_id"]);
        int d_index = stoi(task["d_index"]);
        int m = stoi(task["m"]);
        localTableMap[global_id]->add_and_calculate_col_t_p(d_index, m);
    } else if (task["task"] == "expansion_distribute_and_clear") {
        int global_id = stoi(task["global_id"]);
        int m = stoi(task["m"]);
        localTableMap[global_id]->expansion_distribute_and_clear(m);
    } else if (task["task"] == "expansion_suffix_sum") {
        int global_id = stoi(task["global_id"]);
        int phase = stoi(task["phase"]);
        localTableMap[global_id]->expansion_suffix_sum(phase);
    } else if (task["task"] == "deleteCol") {
        int global_id = stoi(task["global_id"]);
        int col_index = stoi(task["col_index"]);
        localTableMap[global_id]->deleteCol(col_index);
    } else if (task["task"] == "soda_shuffleByKey") {
        int global_id = stoi(task["global_id"]);
        auto key = stringToVector(task["key"]);
        localTableMap[global_id]->soda_shuffleByKey(utils::num_partitions, key, 0);
    } else if (task["task"] == "destroy") {
        int global_id = stoi(task["global_id"]);
        localTableMap[global_id]->destroy();
    } else if (task["task"] == "get_comm_and_reset") {
        log_info("header size = %d, body size = %d", utils::header_total_size, utils::body_total_size );
        long long ret = utils::body_total_size + utils::header_total_size;
        task_ret = std::to_string(ret);
        utils::body_total_size = utils::header_total_size = 0;
    } else {
        log_warn("Unknown task %s", task["task"].c_str());
        return "Unsupported";
    }
    return task_ret;
}

EchoHandler::~EchoHandler() {
    if (fileStream_.is_open()) {
        fileStream_.close();
    }
}

void EchoHandler::onRequest(std::unique_ptr<HTTPMessage> req) noexcept {
    unordered_map<string, string> task = headerToMap(req->getHeaders());

    //Debug
    const proxygen::HTTPHeaders& headers = req->getHeaders();
    std::string headers_str;
    headers.forEach([&](const std::string& header, const std::string& value) {
        if (header != "task")
            headers_str += header + ": " + value + "; ";
    });
    if(!headers.exists("task")){
        log_warn("Unknown request: %s", headers_str.c_str());
        return;
    }
    headers_str = "task: " + headers.rawGet("task") + "; " + headers_str;
    log_info("%s", headers_str.c_str());

    stats_->recordRequest();

    if (task["task"] == "write_file") {
        log_debug("write_file task reached");
        fileStream_.open(task["file_name"], std::ios::binary);
    } else {
        ret = executeTask(task);
    }
}

void EchoHandler::onBody(std::unique_ptr<folly::IOBuf> body) noexcept {
    if (body) {
        // 检查文件流是否打开
        if (!fileStream_.is_open()) {
            log_error("File stream is not open");
            return;
        }

        // 遍历所有链表成员
        const folly::IOBuf* current = body.get();
        do {
            log_debug("onBody called with data length %d", current->length());
            fileStream_.write(reinterpret_cast<const char*>(current->data()), current->length());
            current = current->next();
        } while (current != body.get());
        // body_是IOBuf链表上一个IOBuf对象的unique_ptr，可能是尾部IOBuf的，所以需要遍历链条
    }
}

void EchoHandler::onEOM() noexcept {
    log_debug("onEOM called");

    if (fileStream_.is_open()) {
        fileStream_.close();
    }

    // ResponseBuilder(downstream_).sendWithEOM();
    std::string body_ret = "(ret:" + ret + ")";

    ResponseBuilder(downstream_)
        .status(200, "OK")
        .header("Content-Length", "6")
        .body(body_ret)
        .sendWithEOM();  // 最后发送响应，并用 sendWithEOM() 标记请求已处理完毕
}

void EchoHandler::onUpgrade(UpgradeProtocol /*protocol*/) noexcept {
    // handler doesn't support upgrades
}

void EchoHandler::requestComplete() noexcept {
    delete this;
}

void EchoHandler::onError(ProxygenError /*err*/) noexcept {
    delete this;
}
}  // namespace EchoService
