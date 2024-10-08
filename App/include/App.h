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

#ifndef APP_H_
#define APP_H_

#include <cassert>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>

#include "sgx_eid.h"   /* sgx_enclave_id_t */
#include "sgx_error.h" /* sgx_status_t */

#ifndef TRUE
#define TRUE 1
#endif

#ifndef FALSE
#define FALSE 0
#endif

// #define TOKEN_FILENAME "enclave.token"
#define ENCLAVE_FILENAME "Enclave/enclave.signed.so"

extern sgx_enclave_id_t global_eid; /* global enclave id */
extern bool enclave_created;

typedef struct sgx_errlist_t_ {
  sgx_status_t err;
  const char *msg;
  const char *sug; /* Suggestion */
} sgx_errlist_t;

/* Error code returned by sgx_create_enclave */
static sgx_errlist_t sgx_errlist[] = {
    {SGX_ERROR_UNEXPECTED, "Unexpected error occurred.", nullptr},
    {SGX_ERROR_INVALID_PARAMETER, "Invalid parameter.", nullptr},
    {SGX_ERROR_OUT_OF_MEMORY, "Out of memory.", nullptr},
    {SGX_ERROR_ENCLAVE_LOST, "Power transition occurred.",
     "Please refer to the sample \"PowerTransition\" for details."},
    {SGX_ERROR_INVALID_ENCLAVE, "Invalid enclave image.", nullptr},
    {SGX_ERROR_INVALID_ENCLAVE_ID, "Invalid enclave identification.", nullptr},
    {SGX_ERROR_INVALID_SIGNATURE, "Invalid enclave signature.", nullptr},
    {SGX_ERROR_OUT_OF_EPC, "Out of EPC memory.", nullptr},
    {SGX_ERROR_NO_DEVICE, "Invalid SGX device.",
     "Please make sure SGX module is enabled in the BIOS, and install SGX "
     "driver afterwards."},
    {SGX_ERROR_MEMORY_MAP_CONFLICT, "Memory map conflicted.", nullptr},
    {SGX_ERROR_INVALID_METADATA, "Invalid enclave metadata.", nullptr},
    {SGX_ERROR_DEVICE_BUSY, "SGX device was busy.", nullptr},
    {SGX_ERROR_INVALID_VERSION, "Enclave version was invalid.", nullptr},
    {SGX_ERROR_INVALID_ATTRIBUTE, "Enclave was not authorized.", nullptr},
    {SGX_ERROR_ENCLAVE_FILE_ACCESS, "Can't open enclave file.", nullptr},
};

void print_error_message(sgx_status_t ret);
int initialize_enclave(bool enable_sgx_switchless);

#if defined(__cplusplus)
extern "C" {
#endif

#if defined(__cplusplus)
}
#endif

#endif /* !APP_H_ */
