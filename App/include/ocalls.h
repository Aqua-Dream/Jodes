#pragma once

#include <cstddef>

#if defined(__cplusplus)
extern "C" {
#endif

void ocall_print_string(const char* str);
void ocall_log(int level, const char* file, int line, const char* msg);
void ocall_write_file(const char* file_name, char* content, size_t length, int row_num, int global_id, int source_local_id, int target_local_id);
void* ocall_read_file(const char* file_name);

void ocall_record_time_start(const char* log, int uniq_counter, int global_id, int local_id);
void ocall_record_time_end(const char* log, int uniq_counter, int global_id, int local_id);

void unsafe_ocall_malloc(size_t size, char** ret);
void ocall_free(char* buf);

#if defined(__cplusplus)
}  // extern "C"
#endif
