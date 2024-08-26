#include <boost/program_options.hpp>

#include <sgx_urts.h>
#include <fstream>
#include <functional>
#include <iostream>
#include <string>
#include <vector>
#include "App.h"
#include "GlobalTable.h"
#include "log.h"
#include "utils.h"
#include <iomanip> 
#include <boost/thread.hpp>



namespace po = boost::program_options;

int N = 0;
long long M = 0;

void print_result() {
    using namespace utils;
    if (is_distributed)
        return;
    std::cout << "SIZE IN = " << N << std::endl;
    std::cout << "TIME COMM = " << read_write_ms << " ms" << std::endl;
    std::cout << "TIME COMP = " << comp_ms << " ms" << std::endl;
    std::cout << "SIZE COMM = " << total_comm << std::endl;
    std::cout << "*** TIME TOTAL = " << read_write_ms + comp_ms << " ms ***" << std::endl;
    std::cout << "*** SIZE COMM/IN = " << (float)total_comm / (N + M) << " ***" << std::endl
        << std::endl;
}
void opartition(const po::variables_map& vm) {
    if (!vm.count("opartition.table")) {
        log_error("Opartition.table is not set!");
    }
    std::string table_path = vm["opartition.table"].as<std::string>();
    std::string alg = vm["opartition.alg"].as<std::string>();
    if (alg != "soda" && alg != "jodes") {
        log_error("Unknown pkjoin alg: %s", alg.c_str());
    }
    GlobalTable gtable(table_path);
    utils::reset();
    N = gtable.size();
    int n = gtable.getLocalTables()[0].size();
    int size_bound = utils::getSizeBound(n, utils::num_partitions);
    if (size_bound > n) size_bound = n;
    std::vector<boost::thread> threads;
    if (alg == "soda") {
        std::cout << "*** N=" << N << ", SODA ***" << std::endl;
        auto lambda = [&size_bound](LocalTable& table) {
            table.soda_shuffleByKey(utils::num_partitions, { 0 }, size_bound);
            };
        for (auto& table : gtable.getLocalTables()) {
            threads.emplace_back(lambda, std::ref(table));
        }
    }
    else if (alg == "jodes") {
        std::cout << "*** N=" << N << ", JODES ***" << std::endl;
        auto lambda = [&size_bound](LocalTable& table) {
            table.shuffleByKey(utils::num_partitions, { 0 }, 0, size_bound);
            };
        for (auto& table : gtable.getLocalTables()) {
            threads.emplace_back(lambda, std::ref(table));
        }
    }
    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
    utils::update_phase();
    print_result();
}

void pkjoin(const po::variables_map& vm) {
    if (!vm.count("pkjoin.table.R") || !vm.count("pkjoin.table.S") || !vm.count("pkjoin.z")) {
        log_error("Either pkjoin.table.R / pkjoin.table.S / pkjoin.z is not set!");
    }
    std::string R_path = vm["pkjoin.table.R"].as<std::string>() + "_" + vm["pkjoin.z"].as<std::string>();
    std::string S_path = vm["pkjoin.table.S"].as<std::string>();
    const std::vector<int> r_cols = { 1 };
    const std::vector<int> s_cols = { 0 };
    std::string alg = vm["pkjoin.alg"].as<std::string>();
    if (alg == "opaque") {
        GlobalTable r_table(R_path);
        GlobalTable s_table(S_path);
        int N1 = r_table.size();
        int N2 = s_table.size();
        N = N1 + N2;
        utils::reset();
        std::cout << "*** N1=" << N1 << ", N2=" << N2 << ", Opaque ***" << std::endl;
        s_table.opaque_pkjoin(r_table, r_cols, s_cols);
        utils::update_phase();
        r_table.print();
        print_result();
    }
    else if (alg == "soda") {
        GlobalTable r_table(R_path);
        GlobalTable s_table(S_path);
        int N1 = r_table.size();
        int N2 = s_table.size();
        N = N1 + N2;
        utils::reset();
        std::cout << "*** N1=" << N1 << ", N2=" << N2 << ", SODA ***" << std::endl;
        int a1, a2;
        s_table.soda_join(r_table, r_cols, s_cols, a1, a2, M);
        std::cout << "a1=" << a1 << ", a2=" << a2 << ", M=" << M << std::endl;
        utils::update_phase();
        M = 0;
        r_table.print();
        print_result();
    }
    else if (alg == "jodes") {
        GlobalTable r_table(R_path);
        GlobalTable s_table(S_path);
        int N1 = r_table.size();
        int N2 = s_table.size();
        N = N1 + N2;
        utils::reset();
        std::cout << "*** N1=" << N1 << ", N2=" << N2 << ", JODES ***" << std::endl;
        s_table.pkjoin(r_table, r_cols, s_cols);
        utils::update_phase();
        r_table.print();
        print_result();
    } else {
        log_error("Unknown pkjoin alg: %s", alg.c_str());
    }
}

void join(const po::variables_map& vm) {
    if (!vm.count("join.config.id")) {
        log_error("join.config.id is not set!");
    }
    int id = vm["join.config.id"].as<int>();
    std::string table_path_key = "join.config." + std::to_string(id) + ".table";
    if (!vm.count("join.config.id")) {
        log_error("%s is not set!", table_path_key.c_str());
    }
    std::string table_path = vm[table_path_key].as<std::string>();
    if (id == 3) {
        if (!vm.count("join.config.3.prob")) {
            log_error("join.config.3.prob is not set!");
        }
        table_path += "_" + vm["join.config.3.prob"].as<std::string>();
    }
    std::string alg = vm["join.alg"].as<std::string>();

    const std::vector<int> r_cols = { 1 };
    const std::vector<int> s_cols = { 0 };

    if (alg == "jodes") {
        GlobalTable r_table(table_path);
        GlobalTable s_table(table_path);
        int N1 = r_table.size();
        int N2 = s_table.size();
        N = N1 + N2;
        utils::reset();
        std::cout << "*** N1=" << N1 << ", N2=" << N2 << ", JODES ***" << std::endl;
        s_table.join(r_table, r_cols, s_cols, M);
        std::cout << "M = " << M << std::endl;
        r_table.print();
    }
    else if (alg == "soda") {
        GlobalTable r_table(table_path);
        GlobalTable s_table(table_path);
        int N1 = r_table.size();
        int N2 = s_table.size();
        N = N1 + N2;
        utils::reset();
        std::cout << "*** N1=" << N1 << ", N2=" << N2 << ", SODA ***" << std::endl;
        int a1, a2;
        s_table.soda_join(r_table, r_cols, s_cols, a1, a2, M);
        std::cout << "a1=" << a1 << ", a2=" << a2 << ", M=" << M << std::endl;
        r_table.print();
    }
    else if (alg == "single") {
        utils::num_partitions = 1;
        if (utils::is_distributed) {
            log_error("Cannot run single join in distributed setting!");
        }
        GlobalTable r_table(table_path);
        GlobalTable s_table(table_path);
        int N1 = r_table.size();
        int N2 = s_table.size();
        N = N1 + N2;
        utils::reset();
        std::cout << "*** N1=" << N1 << ", N2=" << N2 << ", Single ***" << std::endl;
        int m = 0;
        s_table.localJoin(r_table, r_cols, s_cols, m);
        M = m;
        r_table.print();
    }
    else {
        log_error("Unknown join alg: %s", alg.c_str());
    }
    utils::update_phase();
    print_result();
}

const std::unordered_map<std::string, std::function<void(const po::variables_map& vm)>> task_string_map = {
    {"join",       join      },
    {"pkjoin",     pkjoin    },
    {"opartition", opartition}
};

po::variables_map read_settings(int argc, char** argv) {
    po::options_description command_desc("command");
    command_desc.add_options()("config", po::value<std::string>()->default_value("../config/config.ini"));
    command_desc.add_options()("task", po::value<std::string>()->default_value("./config/benchmark.ini"));
    po::variables_map command_vm;
    po::store(po::parse_command_line(argc, argv, command_desc), command_vm);
    po::notify(command_vm);
    utils::read_config_file(command_vm["config"].as<std::string>(), false);

    po::options_description task_desc("task");
    task_desc.add_options()("task", po::value<std::string>()->required());
    task_desc.add_options()("opartition.table", po::value<std::string>());
    task_desc.add_options()("opartition.alg", po::value<std::string>());
    task_desc.add_options()("pkjoin.table.R", po::value<std::string>());
    task_desc.add_options()("pkjoin.table.S", po::value<std::string>());
    task_desc.add_options()("pkjoin.z", po::value<std::string>());
    task_desc.add_options()("pkjoin.alg", po::value<std::string>());
    task_desc.add_options()("join.config.id", po::value<int>());
    task_desc.add_options()("join.config.0.table", po::value<std::string>());
    task_desc.add_options()("join.config.1.table", po::value<std::string>());
    task_desc.add_options()("join.config.2.table", po::value<std::string>());
    task_desc.add_options()("join.config.3.table", po::value<std::string>());
    task_desc.add_options()("join.config.3.prob", po::value<std::string>());
    task_desc.add_options()("join.alg", po::value<std::string>());

    log_info("Read task file start");
    po::variables_map task_vm;
    std::ifstream task_file(command_vm["task"].as<std::string>());
    po::store(po::parse_config_file(task_file, task_desc), task_vm);
    po::notify(task_vm);
    log_info("Read task file finished");
    log_info("task = %s", task_vm["task"].as<std::string>().c_str());
    return task_vm;
}

int main(int argc, char** argv) {
    auto vm = read_settings(argc, argv);
    auto start = std::chrono::high_resolution_clock::now();
    auto task_func = task_string_map.at(vm["task"].as<std::string>());
    task_func(vm);
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    if (utils::is_distributed) {
        long long comm = utils::coordinator_get_comm_and_reset();
        std::cout << "Total comm: " << std::fixed << std::setprecision(3) << comm / 1024 / 1024.0 << " MB" << std::endl;
    }
    std::cout << "Total time: " << duration.count() / 1000.0 << " s" << std::endl;
    return 0;
}