#include "include/ocalls.h"
#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include "ReqSender.h"
#include "include/log.h"
#include "include/utils.h"

void ocall_log(int level, const char* file, int line, const char* msg) {
    log_log(level, file, line, msg);
}

void unsafe_ocall_malloc(size_t size, char** ret) {
    *ret = (char*)(malloc(size));
}

void ocall_free(char* buf) {
    free(buf);
}

/* OCall functions */
void ocall_print_string(const char* str) {
    printf("%s", str);
}

//int gid = 0xabcde;
void ocall_write_file(const char* file_name, char* content, size_t length, int row_num, int global_id, int source_local_id, int target_local_id) {
    if (utils::is_distributed && source_local_id != target_local_id) {
        unordered_map<string, string> header_map = {
            {"task",           "write_file"             },
            {"global_id",      std::to_string(global_id)},
            {"file_name",      file_name                },
            {"Content-Length", std::to_string(length)   }
        };
        send_post_request(
            utils::worker_urls[target_local_id],
            header_map,
            "", timeout_const,
            content, length);
    } else {
        std::ofstream fout(file_name, std::ios::binary);  // 打开文件用于写入，使用二进制模式
        if (!fout || !fout.is_open()) {
            log_error("Cannot open file %s", file_name);
        }
        // fout << content;
        fout.write(content, length);
        fout.close();

        if (utils::comm_map.count(global_id) > 0)
            utils::comm_map[global_id] = utils::comm_map[global_id] + row_num;
        else
            utils::comm_map[global_id] = row_num;

        utils::total_comm += row_num;
    }

    // ocall_record_time_end(label, gid, 0, source_local_id);
}

struct FileInfo {
    uint8_t* file_content;
    uint64_t file_length;
};

//TODO: figure out how to free the return value, ocall_free?
void* ocall_read_file(const char* file_name) {
    size_t file_length;
    uint8_t* file = utils::readPartitionFile(file_name, &file_length);
    FileInfo* ret = new FileInfo;
    ret->file_content = file;
    ret->file_length = file_length;

    return static_cast<void*>(ret);
}

// bool record_start = false;
void ocall_record_time_start(const char* log, int uniq_counter, int global_id, int local_id) {
    using namespace utils;
    auto it = flag_map.find(uniq_counter);
    if (it != flag_map.end()) {
        log_error("%d already exists in flag_map", uniq_counter);
    }
    flag_map[uniq_counter] = true;

    start = std::chrono::high_resolution_clock::now();
    time_map[uniq_counter] = start;
}

void ocall_record_time_end(const char* log, int uniq_counter, int global_id, int local_id) {
    using namespace utils;
    auto it = flag_map.find(uniq_counter);
    if (it == flag_map.end() || flag_map[uniq_counter] == false) {
        log_error("%d not exists in flag_map; or it is false", uniq_counter);
    }

    auto end = std::chrono::high_resolution_clock::now();
    // record_start = false;
    flag_map[uniq_counter] = false;

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - time_map[uniq_counter]);
    double ms_duration = duration.count() / 1000.0;

    std::string content = std::string(log);
    if (exist_in_matrix(local_id, content)) {
        duration_matrix[local_id][content] = duration_matrix[local_id][content] + ms_duration;
    } else {
        duration_matrix[local_id][content] = ms_duration;
    }
}
