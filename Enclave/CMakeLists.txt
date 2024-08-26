# add the trusted source files
set(TRUSTED_SRCS
    utils/log.cpp
)

# add the core enclave files
set(ENCLAVE_SRCS Enclave.cpp LocalTable.cpp Tuple.cpp Obliv.cpp)

# configure lds file
if(SGX_HW AND SGX_MODE STREQUAL Release)
    set(LDS_FILE config/Enclave.lds)
else()
    message("SGX_HW:${SGX_HW}    SGX_MODE:${SGX_MODE} ")
    set(LDS_FILE config/Enclave_debug.lds)
    message(STATUS "[${PROJECT_NAME}] build sgx in debug/simulation mode")
endif()

# configure trusted library
add_trusted_library(trusted_lib SRCS ${TRUSTED_SRCS} EDL config/Enclave.edl EDL_SEARCH_PATHS ${EDL_SEARCH_PATHS})

# configure enclave
add_enclave_library(enclave
        SRCS ${ENCLAVE_SRCS} TRUSTED_LIBS trusted_lib EDL config/Enclave.edl
        EDL_SEARCH_PATHS ${EDL_SEARCH_PATHS} LDSCRIPT ${LDS_FILE})

# sign enclave
enclave_sign(enclave KEY config/Enclave_private.pem CONFIG config/Enclave.config.xml)
