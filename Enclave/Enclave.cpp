/*
 * Copyright (C) 2011-2019 Intel Corporation. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *   * Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in
 *     the documentation and/or other materials provided with the
 *     distribution.
 *   * Neither the name of Intel Corporation nor the names of its
 *     contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <memory>
#include <string>

#include <atomic>
#include <set>
#include <unordered_map>
#include "Enclave.h"
#include "Enclave_t.h"
#include "LocalTable.h"
#include "sgx_tcrypto.h"
#include "sgx_trts.h"

#define BUFLEN 800000
static sgx_aes_gcm_128bit_key_t key = {0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf};
int globalTimingCounter = 0;

int e_num_partitions = -1;
// std::vector<LocalTable> local_tables;
std::unordered_map<int, std::vector<LocalTable*>> tableMap;  //key is the global table's id; value corresponds to its local tables

uint8_t* decryptMsg(const char* msg, size_t decMessageLen) {
    // size_t decMessageLen = strlen(msg) - SGX_AESGCM_MAC_SIZE - SGX_AESGCM_IV_SIZE;

    uint8_t* decMessage = new uint8_t[decMessageLen];

    // ret = decryptMessage(msg,strlen(msg),decMessage,decMessageLen);

    uint8_t* encMessage = (uint8_t*)msg;
    //uint8_t p_dst[BUFLEN] = {0};

    sgx_rijndael128GCM_decrypt(
        &key,
        encMessage + SGX_AESGCM_MAC_SIZE + SGX_AESGCM_IV_SIZE,
        decMessageLen,
        decMessage,
        encMessage + SGX_AESGCM_MAC_SIZE, SGX_AESGCM_IV_SIZE,
        NULL, 0,
        (sgx_aes_gcm_128bit_tag_t*)encMessage);

    return decMessage;
}

uint8_t* encryptMsg(const char* msg, size_t msg_length, size_t* encLen) {
    // The encrypted message will contain the MAC, the IV, and the encrypted message itself.
    size_t encMessageLen = (SGX_AESGCM_MAC_SIZE + SGX_AESGCM_IV_SIZE + msg_length);
    uint8_t* encMessage = new uint8_t[encMessageLen];

    uint8_t* origMessage = (uint8_t*)msg;
    //uint8_t p_dst[BUFLEN] = {0};

    // Generate the IV (nonce)
    sgx_read_rand(encMessage + SGX_AESGCM_MAC_SIZE, SGX_AESGCM_IV_SIZE);
    sgx_status_t sgx_status = sgx_rijndael128GCM_encrypt(
        &key,
        origMessage, msg_length,
        encMessage + SGX_AESGCM_MAC_SIZE + SGX_AESGCM_IV_SIZE,
        encMessage + SGX_AESGCM_MAC_SIZE, SGX_AESGCM_IV_SIZE,
        NULL, 0,
        (sgx_aes_gcm_128bit_tag_t*)(encMessage));
    //memcpy(encMessage,p_dst,encMessageLen);
    //encMessage[encMessageLen] = '\0';

    if (SGX_SUCCESS != sgx_status) {
        log_error("ERROR: SGX_NOT_SUCCESS, sgx_status is %i", sgx_status);
    }

    *encLen = encMessageLen;

    return encMessage;

    // encryptMessage(msg, strlen(msg), encMessage, encMessageLen);
}

LocalTable* getLocalTable(int global_id, int local_id) {
    if (tableMap.find(global_id) == tableMap.end()) {
        log_error("Error, global_id %i not exists", global_id);
    }
    if (local_id >= e_num_partitions) {
        log_error("Error, local_id %i larger than num_partitions %i", local_id, e_num_partitions);
    }
    return tableMap[global_id][local_id];
}

void initGlobalTable(int global_id) {
    if (e_num_partitions < 0) {
        log_error("Error, num_partitions %i not initialized yet", e_num_partitions);
        return;
    }

    if (tableMap.find(global_id) == tableMap.end()) {
        //the global id not exist in tableMap, will create one
        std::vector<LocalTable*> local_tables(e_num_partitions);
        tableMap.insert(std::make_pair(global_id, local_tables));
    }
}

void log_log(int level, const char* file, int line, const char* fmt, ...) {
    char buf[BUFSIZ] = {'\0'};
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, BUFSIZ, fmt, ap);
    va_end(ap);
    ocall_log(level, file, line, buf);
}

/* 
 * printf: 
 *   Invokes OCALL to display the enclave buffer to the terminal.
 */
int printf(const char* fmt, ...) {
    char buf[BUFSIZ] = {'\0'};
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, BUFSIZ, fmt, ap);
    va_end(ap);
    ocall_print_string(buf);
    return (int)strnlen(buf, BUFSIZ - 1) + 1;
}

//TODO: free a lot of malloc/new objects...
/* encLen is the length of content */
char* packBlock(size_t plainLen, size_t encLen, char* encData, size_t* blockSize) {
    char* block = (char*)malloc((encLen + 16 + 1) * sizeof(char));
    memcpy(block, &plainLen, sizeof(size_t));
    memcpy(block + sizeof(size_t), &encLen, sizeof(size_t));
    // ((size_t *)block)[0] = plainLen;
    // ((size_t *)block)[1] = encLen;
    memcpy(block + 16, encData, encLen);
    block[encLen + 16] = '\0';

    *blockSize = encLen + 16 + 1;

    free(encData);
    return block;
}

/* return: the enc data */
char* unpackBlock(char* block, size_t* plainLen, size_t* encLen) {
    *plainLen = ((size_t*)block)[0];
    *encLen = ((size_t*)block)[1];

    return block + 16;
}

/* Currently we only read and write 1 block each time */

void write_file(const char* file_name, const char* content, size_t content_length, int row_num, int global_id, int local_id, int target_local_id) {
    int uniq_counter = globalTimingCounter++;
    size_t plainLen = content_length;
    size_t encLen = 0;
    ocall_record_time_start("read_write", uniq_counter, global_id, local_id);
    auto encData = encryptMsg(content, content_length, &encLen);

    free((void*)content);
    size_t blockSize = 0;
    char* block = packBlock(plainLen, encLen, (char*)encData, &blockSize);
    char* unsafe_buf;
    unsafe_ocall_malloc(blockSize, &unsafe_buf);
    memcpy(unsafe_buf, block, blockSize);
    ocall_write_file(file_name, unsafe_buf, blockSize, row_num, global_id, local_id, target_local_id);
    free(block);
    ocall_free(unsafe_buf);
    ocall_record_time_end("read_write", uniq_counter, global_id, local_id);
}

void freeFileInfo(FileInfo* f) {
    free(f->file_content);
    free(f);
};

void profile_record_time_start(const char* log, int uniq_counter, int global_id, int local_id) {
    ocall_record_time_start(log, uniq_counter, global_id, local_id);
}

void profile_record_time_end(const char* log, int uniq_counter, int global_id, int local_id) {
    ocall_record_time_end(log, uniq_counter, global_id, local_id);
}

void load_file_str(FileInfo* f, std::vector<Tuple>* tuples, int num_columns) {
    int row_length = Tuple::rowLength(num_columns);

    if (f->file_length % row_length != 0) {
        log_error("Error: %llu can not be divided by row_length %llu", f->file_length, row_length);
    }

    //this num_rows contains dummy rows
    int num_all_rows = f->file_length / row_length;
    int ori_tuples_size = tuples->size();

    tuples->reserve(num_all_rows + ori_tuples_size);

    //std::vector<int> data(num_columns);
    char* pointer = (char*)f->file_content;
    for (int i = 0; i < num_all_rows; i++) {
        tuples->emplace_back(pointer, num_columns);
        pointer += row_length;
    }
}

FileInfo* read_file(const char* file_name, int global_id, int local_id) {
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("read_write", uniq_counter, global_id, local_id);

    void* result;
    ocall_read_file(&result, file_name);
    FileInfo* ret = static_cast<FileInfo*>(result);

    size_t plainLen = 0;
    size_t encLen = 0;

    char* encData = unpackBlock(reinterpret_cast<char*>(ret->file_content), &plainLen, &encLen);

    // size_t decLen = 0;
    auto decFile = decryptMsg(encData, plainLen);
    FileInfo* decRet = new FileInfo;
    decRet->file_content = decFile;
    decRet->file_length = plainLen;

    ocall_record_time_end("read_write", uniq_counter, global_id, local_id);

    return decRet;
}

int ecall_merge_and_print_string(char* s1, char* s2) {
    size_t len = strlen(s1) + strlen(s2);
    char* buf = (char*)malloc(sizeof(char) * (len + 1));
    for (size_t idx = 0; idx < strlen(s1); idx++) {
        buf[idx] = s1[idx];
    }
    for (size_t idx = 0; idx < strlen(s2); idx++) {
        buf[strlen(s1) + idx] = s2[idx];
    }
    int o_ret;
    ocall_print_msg(&o_ret, buf, len);
    free(buf);
    return 0;
}

int ecall_setup_env(int num_partitions_) {
    e_num_partitions = num_partitions_;
    return 0;
}

int ecall_read_file(int global_id, int local_id, uint8_t* file, size_t file_length) {
    if (tableMap.find(global_id) == tableMap.end()) {
        // 键不存在
        if (e_num_partitions < 1) {
            log_error("Error: num_partitions=%i is invalid, it should be larger than 0", e_num_partitions);
            return -1;
        }

        initGlobalTable(global_id);
        // std::vector<LocalTable*> local_tables(num_partitions);
        // tableMap.insert(std::make_pair(global_id, local_tables));
    }

    LocalTable* local_table = new LocalTable(global_id, local_id, file, file_length);
    tableMap[global_id][local_id] = local_table;

    return 0;
}

int ecall_print(int global_id, int local_id, int limit_size, bool show_dummy) {
    getLocalTable(global_id, local_id)->print(limit_size, show_dummy);
    return 0;
}

int ecall_copy(int global_id, int local_id, int new_global_id) {
    LocalTable* local_table = getLocalTable(global_id, local_id)->copy(new_global_id);

    // assert(id < num_data && "Parition error!");

    /* the global id of the copied table is designed to be original table's global id + 1000 */
    initGlobalTable(new_global_id);
    tableMap[new_global_id][local_id] = local_table;

    return 0;
}

int ecall_destroy(int global_id, int num_partitions) {
    for (int i = 0; i < num_partitions; i++) {
        delete tableMap[global_id][i];
    }

    tableMap.erase(global_id);

    return 0;
}

AssociateOperator* createOp(void* op) {
    AssociateOperator* aop = static_cast<AssociateOperator*>(op);
    switch (aop->op_id) {
        case AssociateOperator::ADD:
            return new OperatorAdd(aop->group_by_columns, aop->aggregate_column);
            break;
        case AssociateOperator::MUL:
            return new OperatorMul(aop->group_by_columns, aop->aggregate_column);
            break;
        case AssociateOperator::MAX:
            return new OperatorMax(aop->group_by_columns, aop->aggregate_column);
            break;
        case AssociateOperator::MIN:
            return new OperatorMin(aop->group_by_columns, aop->aggregate_column);
            break;
        case AssociateOperator::COPY:
            return new OperatorCopy();
            break;
        default:
            log_error("Error: op_id %i not recognized in define.h", aop->op_id);
    }
    return nullptr;
}

int ecall_groupByAggregateBase(int global_id,
                               int local_id,
                               void* op,
                               bool doPrefix,
                               int phase,
                               bool reverse) {
    // AssociateOperator *aop = static_cast<AssociateOperator *>(op);
    AssociateOperator* aop = createOp(op);

    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->groupByAggregateBase(e_num_partitions, aop, doPrefix, phase, reverse);
    free(aop);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

int ecall_addValueByKey(int global_id,
                        int local_id,
                        void* op) {
    // AssociateOperator *aop = static_cast<AssociateOperator *>(op);
    AssociateOperator* aop = createOp(op);
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->_addValueByKey(aop);
    free(aop);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

int ecall_shuffleByKey(int global_id,
                       int local_id,
                       int num_partitions,
                       int* key_data,
                       size_t key_size,
                       int seed,
                       int size_bound) {
    std::vector<int> key(key_data, key_data + key_size);
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->shuffleByKey(num_partitions, key, seed, size_bound);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

int ecall_shuffleByCol(int global_id,
                       int local_id,
                       int num_partitions,
                       int i_col_id,
                       int shuffle_by_col_padding_size) {
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->shuffleByCol(num_partitions, i_col_id, shuffle_by_col_padding_size);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

int ecall_randomShuffle(int global_id,
                        int local_id,
                        int num_partitions) {
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->randomShuffle(num_partitions);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);

    return 0;
}

int ecall_shuffleMerge(int global_id,
                       int local_id,
                       int num_partitions) {
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->shuffleMerge(num_partitions);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);

    return 0;
}

int ecall_getPivots(int global_id,
                    int local_id,
                    int* columns_data,
                    size_t columns_size) {
    std::vector<int> columns(columns_data, columns_data + columns_size);
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->getPivots(e_num_partitions, columns);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

long long ecall_sum(int global_id,
              int local_id,
              int column) {
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    long long ret = getLocalTable(global_id, local_id)->sum(column);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return ret;
}

int ecall_max(int global_id,
              int local_id,
              int column) {
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    int ret = getLocalTable(global_id, local_id)->max(column);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return ret;
}

int ecall_union(int global_id,
                int local_id,
                int other_table_global_id,
                int other_table_local_id) {
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->union_table(*getLocalTable(other_table_global_id, other_table_local_id));
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

long long ecall_SODA_step1(int global_id,
                     int local_id,
                     int* columns_data,
                     size_t columns_size,
                     int aggCol) {
    std::vector<int> columns(columns_data, columns_data + columns_size);
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    long long ret = getLocalTable(global_id, local_id)->SODA_step1(columns, e_num_partitions, aggCol);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return ret;
}

int ecall_SODA_step2(int global_id,
                     int local_id,
                     int p) {
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->SODA_step2(e_num_partitions, p);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

int ecall_SODA_step3(int global_id,
                     int local_id,
                     int p) {
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->SODA_step3(e_num_partitions, p);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

int ecall_localJoin(int global_id,
                    int local_id,
                    int other_table_global_id,
                    int other_table_local_id,
                    int num_cols,
                    int output_size) {
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    int ret = getLocalTable(global_id, local_id)->localJoin(*getLocalTable(other_table_global_id, other_table_local_id), num_cols, output_size);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return ret;
}

int ecall_SODA_step5(int global_id,
                     int local_id) {
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->SODA_step5(e_num_partitions);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

int ecall_assignColE(int global_id,
                     int local_id,
                     int col) {
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->assignColE(e_num_partitions, col);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

int ecall_mvJoinColsAhead(int global_id,
                          int local_id,
                          int* columns_data,
                          size_t columns_size) {
    std::vector<int> columns(columns_data, columns_data + columns_size);
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->mvJoinColsAhead(e_num_partitions, columns);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

int ecall_partitionByPivots(int global_id,
                            int local_id,
                            int* columns_data,
                            size_t columns_size,
                            int size_bound) {
    std::vector<int> columns(columns_data, columns_data + columns_size);
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->partitionByPivots(columns, e_num_partitions, size_bound);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

int ecall_sortMerge(int global_id,
                    int local_id,
                    int* columns_data,
                    size_t columns_size) {
    std::vector<int> columns(columns_data, columns_data + columns_size);
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->sortMerge(e_num_partitions, columns);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

int ecall_localSort(int global_id,
                    int local_id,
                    int* columns_data,
                    size_t columns_size) {
    std::vector<int> columns(columns_data, columns_data + columns_size);
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->localSort(e_num_partitions, columns);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

int ecall_pad_to_size(int global_id,
                      int local_id,
                      int n) {
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->pad_to_size(e_num_partitions, n);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

int ecall_opaque_prepare_shuffle_col(int global_id,
                                     int local_id,
                                     int col_id,
                                     int tuple_num) {
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->opaque_prepare_shuffle_col(e_num_partitions, col_id, tuple_num);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

int ecall_foreignTableModifyColZ(int global_id,
                                 int local_id,
                                 int* columns_data,
                                 size_t columns_size) {
    std::vector<int> columns(columns_data, columns_data + columns_size);
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->foreignTableModifyColZ(e_num_partitions, columns);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

int ecall_finalizePkjoinResult(int global_id,
                               int local_id,
                               int* columns_data,
                               size_t columns_size,
                               int ori_r_col_num,
                               int r_align_col_num) {
    std::vector<int> columns(columns_data, columns_data + columns_size);
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->finalizePkjoinResult(e_num_partitions, columns, ori_r_col_num, r_align_col_num);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

int ecall_pkJoinCombine(int global_id,
                        int local_id,
                        int s_table_global_id,
                        int s_table_local_id,
                        int ori_r_col_num,
                        int r_align_col_num,
                        int* combine_sort_cols_data,
                        size_t combine_sort_cols_size,
                        int* join_cols_data,
                        size_t join_cols_size) {
    std::vector<int> combine_sort_cols(combine_sort_cols_data, combine_sort_cols_data + combine_sort_cols_size);
    std::vector<int> join_cols(join_cols_data, join_cols_data + join_cols_size);
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->pkJoinCombine(e_num_partitions, ori_r_col_num, r_align_col_num, combine_sort_cols, join_cols, *getLocalTable(s_table_global_id, s_table_local_id));
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

int ecall_joinComputeAlignment(int global_id,
                               int local_id,
                               int m) {
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->joinComputeAlignment(m);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

int ecall_joinFinalCombine(int global_id,
                           int local_id,
                           int r_table_global_id,
                           int num_join_cols) {
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->joinFinalCombine(*getLocalTable(r_table_global_id, local_id), num_join_cols);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

int ecall_addCol(int global_id,
                 int local_id,
                 int defaultVal,
                 int col_index) {
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->addCol(e_num_partitions, defaultVal, col_index);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

int ecall_copyCol(int global_id,
                  int local_id,
                  int col_index) {
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->copyCol(e_num_partitions, col_index);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

int ecall_expansion_prepare(int global_id,
                            int local_id,
                            int d_index) {
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->expansion_prepare(e_num_partitions, d_index);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

int ecall_add_and_calculate_col_t_p(int global_id,
                                    int local_id, int d_index, int m) {
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->add_and_calculate_col_t_p(d_index, m);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

int ecall_expansion_distribute_and_clear(int global_id,
                                         int local_id, int m) {
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->expansion_distribute_and_clear(e_num_partitions, m);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

int ecall_expansion_suffix_sum(int global_id,
                               int local_id, int phase) {
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->expansion_suffix_sum(e_num_partitions, phase);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

int ecall_deleteCol(int global_id,
                    int local_id,
                    int col_index) {
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->deleteCol(e_num_partitions, col_index);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

int ecall_size(int global_id,
               int local_id) {
    return getLocalTable(global_id, local_id)->size();
    ;
}

int ecall_num_columns(int global_id,
                      int local_id) {
    return getLocalTable(global_id, local_id)->num_columns();
    ;
}

int ecall_remove_dup_after_prefix(int global_id,
                                  int local_id,
                                  int* columns_data,
                                  size_t columns_size) {
    std::vector<int> columns(columns_data, columns_data + columns_size);
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->remove_dup_after_prefix(e_num_partitions, columns);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

int ecall_project(int global_id,
                  int local_id,
                  int* columns_data,
                  size_t columns_size) {
    std::vector<int> columns(columns_data, columns_data + columns_size);
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->project(columns);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

int ecall_soda_shuffleByKey(int global_id,
                            int local_id,
                            int num_partitions,
                            int* key_data,
                            size_t key_size,
                            int seed,
                            int size_bound) {
    std::vector<int> key(key_data, key_data + key_size);
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->soda_shuffleByKey(num_partitions, key, seed, size_bound);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}

int ecall_generateData(int global_id,
                       int local_id,
                       int num_cols,
                       int num_rows) {
    int uniq_counter = globalTimingCounter++;
    ocall_record_time_start("TOTAL", uniq_counter, global_id, local_id);
    getLocalTable(global_id, local_id)->generateData(num_cols, num_rows);
    ocall_record_time_end("TOTAL", uniq_counter, global_id, local_id);
    return 0;
}
