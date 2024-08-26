# modifided out of https://github.com/xzhangxa/SGX-CMake
cmake_minimum_required(VERSION 3.10)
# project(MyProject)

# 设定 C++ 标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

###############################
# setup compilation options
###############################

project(Jodes)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Set build type")
endif()

set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS Debug Release RelWithDebInfo MinSizeRel)

# configure compilation flags
if(CMAKE_BUILD_TYPE STREQUAL Debug)
    set(SGX_MODE Debug)
    set(PROJECT_COMMON_CFLAGS "-O0 -g -DDEBUG -UNDEBUG -UEDEBUG")
else()
    set(SGX_MODE PreRelease CACHE STRING "SGX build mode: Debug; PreRelease; Release.")
    set_property(CACHE SGX_MODE PROPERTY STRINGS PreRelease Release)

    if(SGX_MODE STREQUAL "PreRelease")
        set(PROJECT_COMMON_CFLAGS "-O3 -UDEBUG -DNDEBUG -DEDEBUG")
    elseif(SGX_MODE STREQUAL "Release")
        set(PROJECT_COMMON_CFLAGS "-O3 -UDEBUG -DNDEBUG -UEDEBUG")
    endif()
endif()
message(STATUS "[${PROJECT_NAME}] build project with flags: ${PROJECT_COMMON_CFLAGS}")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

# configure SGX environment
list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
find_package(SGX REQUIRED)

# check OpenSSL >= 1.1.1
# FindOpenSSL depends on OPENSSL_ROOT_DIR
set(OPENSSL_ROOT_DIR ${PROJECT_SOURCE_DIR}/third_party/openssl-1.1.1k)
find_package(OpenSSL 1.1.1 REQUIRED)
# FindOpenSSL does not provide OPENSSL_LIBRARY_DIR, which is used for building host library
if(NOT DEFINED OPENSSL_LIBRARY_DIR)
    # do not need PARENT_SCOPE, this file is imported by `include()` instead of `add_subdirectory()`
    get_filename_component(OPENSSL_LIBRARY_DIR ${OPENSSL_CRYPTO_LIBRARY} DIRECTORY)
endif()

set(APP_LINK_FLAGS "-L${OPENSSL_LIBRARY_DIR} -Wl,--exclude-libs=libcrypto -l:libcrypto.a -ldl")

###############################
# build project
###############################

# configure include path for the untrusted part
set(APP_INC_DIRS ${APP_INC_DIRS} ${CMAKE_CURRENT_SOURCE_DIR}/App ${CMAKE_CURRENT_SOURCE_DIR}/App/include)

# configure include path for the trusted part
set(ENCLAVE_INC_DIRS ${ENCLAVE_INC_DIRS} ${CMAKE_CURRENT_SOURCE_DIR}/Enclave ${CMAKE_CURRENT_SOURCE_DIR}/Enclave/include ${CMAKE_CURRENT_SOURCE_DIR}/App/include)

# configure edl search path
set(EDL_SEARCH_PATHS Enclave Enclave/config)

add_subdirectory(Enclave)
add_subdirectory(App)

# add_dependencies(obliv_spark enclave-sign)