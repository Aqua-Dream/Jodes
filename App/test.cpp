/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/init/Init.h>
#include <folly/portability/GFlags.h>
#include "App.h"
#include "CurlClient.h"
#include "GlobalTable.h"
#include "ReqSender.h"
#include "log.h"
#include "utils.h"
 // #include <folly/json.h>

 // using namespace CurlService;
using namespace folly;
using namespace proxygen;
using namespace std;

DEFINE_string(http_method,
    "GET",
    "HTTP method to use. GET or POST are supported");
DEFINE_string(url,
    "https://github.com/facebook/proxygen",
    "URL to perform the HTTP method against");
DEFINE_string(config, "../config/config.ini", "Path to config file");
DEFINE_string(input_filename, "", "Filename to read from for POST requests");
DEFINE_int32(http_client_connect_timeout,
    1000,
    "connect timeout in milliseconds");
DEFINE_string(ca_path,
    "/etc/ssl/certs/ca-certificates.crt",
    "Path to trusted CA file");  // default for Ubuntu 14.04
DEFINE_string(cert_path, "", "Path to client certificate file");
DEFINE_string(key_path, "", "Path to client private key file");
DEFINE_string(next_protos,
    "h2,h2-14,spdy/3.1,spdy/3,http/1.1",
    "Next protocol string for NPN/ALPN");
DEFINE_string(plaintext_proto, "", "plaintext protocol");
DEFINE_int32(recv_window, 65536, "Flow control receive window for h2/spdy");
DEFINE_bool(h2c, false, "Attempt HTTP/1.1 -> HTTP/2 upgrade");
DEFINE_string(headers, "", "List of N=V headers separated by ,");
DEFINE_string(proxy, "", "HTTP proxy URL");
DEFINE_bool(log_response,
    false,
    "Whether to log the response content to stderr");
DEFINE_string(task, "", "Which Task to execute");

int test_print(GlobalTable& gtable) {
    gtable.print();
    return 0;
}

int test_copy(GlobalTable& gtable) {
    auto gtable2 = gtable.copy();

    gtable2.sort({ 0, 2 });
    gtable2.print();
    gtable.print();

    return 0;
}

int test_random_shuffle(GlobalTable& gtable) {
    gtable.print(40);
    gtable.randomShuffle();
    std::cout << "--- Table Shuffled ---" << std::endl;
    gtable.print(40);
    return 0;
}

int test_sort(GlobalTable& gtable) {
    std::cout << "--- Table Sorted by {0,2} ---" << std::endl;
    gtable.sort({ 0, 2 });
    gtable.print(50);
    return 0;
}

int test_prefix_aggregate(GlobalTable& gtable) {
    OperatorAdd op_add({}, 1);
    auto gtable2 = gtable.copy();
    // gtable2.showInfo();

    gtable2.groupByPrefixAggregate(op_add);
    std::cout << "--- Original Table ---" << std::endl;
    gtable.print();

    std::cout << "--- Table Prefix Sum 1 Group by {} ---" << std::endl;
    gtable2.print();

    return 0;
}

int test_group_by_prefix_aggregate(GlobalTable& gtable) {
    OperatorAdd op_add({ 0 }, 1);
    auto gtable2 = gtable.copy();

    std::cout << "--- Original Table ---" << std::endl;
    gtable2.print();
    gtable2.sort({ 0, 1 });
    gtable2.groupByPrefixAggregate(op_add);
    std::cout << "--- Table Prefix Sum 1 Group by {0} ---" << std::endl;
    gtable2.print();

    return 0;
}

int test_group_by_aggregate(GlobalTable& gtable) {
    OperatorAdd op_add({ 0 }, 1);
    auto gtable2 = gtable.copy();
    gtable2.sodaGroupByAggregate(op_add);
    gtable.print(50, true);
    std::cout << "--- Table Sum 1 Group by {0} ---" << std::endl;
    gtable2.print(50, true);
    return 0;
}

int test_expansion(GlobalTable& gtable) {
    gtable.print(30, true);

    std::cout << "--- Table Expansion by {3} ---" << std::endl;

    gtable.expansion(3, 120);  //real out put size is 114

    gtable.print(60, true);
    return 0;
}

int test_pkjoin(GlobalTable& r_table, std::vector<int> r_cols, GlobalTable& s_table, std::vector<int> s_cols) {
    // r_table.print();

    s_table.pkjoin(r_table, r_cols, s_cols);
    r_table.print(50);
    return 0;
}

int test_join(GlobalTable& r_table, std::vector<int> r_cols, GlobalTable& s_table, std::vector<int> s_cols) {
    long long M;
    s_table.join(r_table, r_cols, s_cols, M);
    r_table.print(50);

    return 0;
}

int main(int argc, char* argv[]) {
    using namespace utils;
    folly::init(&argc, &argv, false);

    read_config_file(FLAGS_config, false);

    std::string ori_path = "../data/test";

    int ret;
    if (FLAGS_task == "test_print") {       //done
        GlobalTable gtable(ori_path.c_str(), 0);
        ret = test_print(gtable);
    }
    else if (FLAGS_task == "test_copy") {
        GlobalTable gtable(ori_path.c_str(), 0);
        ret = test_copy(gtable);
    }
    else if (FLAGS_task == "test_sort") {  //done
        GlobalTable gtable(ori_path.c_str(), 0);
        ret = test_sort(gtable);
    }
    else if (FLAGS_task == "test_group_by_prefix_aggregate") {
        GlobalTable gtable(ori_path.c_str(), 0);
        ret = test_group_by_prefix_aggregate(gtable);
    }
    else if (FLAGS_task == "test_group_by_aggregate") {
        GlobalTable gtable(ori_path.c_str(), 0);
        ret = test_group_by_aggregate(gtable);
    }
    else if (FLAGS_task == "test_random_shuffle") {  // done
        GlobalTable gtable(ori_path.c_str(), 0);
        ret = test_random_shuffle(gtable);
    }
    else if (FLAGS_task == "test_expansion") {
        GlobalTable expansion_table((ori_path + "_expansion").c_str());
        ret = test_expansion(expansion_table);
    }
    else if (FLAGS_task == "test_prefix_aggregate") {
        GlobalTable gtable(ori_path.c_str(), 0);
        ret = test_prefix_aggregate(gtable);
    }
    else if (FLAGS_task == "test_pkjoin") {
        GlobalTable r_table((ori_path + "_pkjoin_r").c_str());
        GlobalTable s_table((ori_path + "_pkjoin_s").c_str());
        ret = test_pkjoin(r_table, { 1, 3 }, s_table, { 1, 2 });
    }
    else if (FLAGS_task == "test_join") {
        GlobalTable r_join_table((ori_path + "_join_r").c_str());
        GlobalTable s_join_table((ori_path + "_join_s").c_str());
        ret = test_join(r_join_table, { 1, 3 }, s_join_table, { 1, 2 });
    }
    else {
        log_error("Unknown task: %s", FLAGS_task.c_str());
    }
    if (ret != 0) {
        log_error("Execute task %s failed!", FLAGS_task.c_str());
    }

    return EXIT_SUCCESS;
}
