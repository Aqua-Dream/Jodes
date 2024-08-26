
#include "App.h"
#include <sgx_urts.h>
#include <sgx_uswitchless.h>
#include "Enclave_u.h"
#include "log.h"
// #include "utils.h"

/* Global EID shared by multiple threads */
sgx_enclave_id_t global_eid = 0;
bool enclave_created = false;

/* Check error conditions for loading enclave */
void print_error_message(sgx_status_t ret) {
    size_t idx;
    size_t ttl = sizeof sgx_errlist / sizeof sgx_errlist[0];

    for (idx = 0; idx < ttl; idx++) {
        if (ret == sgx_errlist[idx].err) {
            if (nullptr != sgx_errlist[idx].sug)
                log_info("%s", sgx_errlist[idx].sug);
            log_error("%s", sgx_errlist[idx].msg);
            break;
        }
    }

    if (idx == ttl)
        log_error("Unexpected error occurred.");
}

/* Initialize the enclave:
 *   Call sgx_create_enclave to initialize an enclave instance
 */
int initialize_enclave(bool enable_sgx_switchless) {
    /* Configuration for Switchless SGX */
    sgx_uswitchless_config_t us_config = SGX_USWITCHLESS_CONFIG_INITIALIZER;
    /* The only entry to set partition nums */
    us_config.num_uworkers = 2;
    us_config.num_tworkers = 2;

    sgx_status_t ret;

    /* Call sgx_create_enclave to initialize an enclave instance */
    /* Debug Support: set 2nd parameter to 1 */

    const void* enclave_ex_p[32] = {nullptr};

    enclave_ex_p[SGX_CREATE_ENCLAVE_EX_SWITCHLESS_BIT_IDX] =
        (const void*)&us_config;

    if (enable_sgx_switchless)
        ret = sgx_create_enclave_ex(ENCLAVE_FILENAME, SGX_DEBUG_FLAG, nullptr,
                                    nullptr, &global_eid, nullptr,
                                    SGX_CREATE_ENCLAVE_EX_SWITCHLESS, enclave_ex_p);
    else
        ret = sgx_create_enclave(ENCLAVE_FILENAME, SGX_DEBUG_FLAG, NULL, NULL,
                                 &global_eid, NULL);

    if (ret != SGX_SUCCESS) {
        print_error_message(ret);
        return -1;
    }

    int ret2;
    /* Do the ecall and ocall computation, the following ecall_merge_and_print_string is just an example */

    char s1[] = "Hello ";
    char s2[] = "World!";
    sgx_status_t ecall_status;

    ecall_status = ecall_merge_and_print_string(global_eid, &ret2, s1, s2);
    if (ecall_status) {
        log_error("ecall failed");
    }
    enclave_created = true;
    return 0;
}

// OCALLs implementation
int ocall_print_msg(const char* str, size_t len) {
    char* buf = (char*)malloc(sizeof(char) * (len + 1));
    memset(buf, 0, len + 1);
    strncpy(buf, str, len);
    log_info("OCALL PRINT: %s", buf);
    free(buf);
    return 0;
}
