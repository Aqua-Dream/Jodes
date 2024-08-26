cmake_minimum_required(VERSION 3.10)
# project(MyProject)

# 设定 C++ 标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_PREFIX_PATH "${CMAKE_PREFIX_PATH};${PROJECT_SOURCE_DIR}/third_party/proxygen/proxygen/_build/deps/lib/cmake/folly")
set(CMAKE_PREFIX_PATH "${CMAKE_PREFIX_PATH};${PROJECT_SOURCE_DIR}/third_party/proxygen/proxygen/_build/deps/lib/cmake/wangle")
set(CMAKE_PREFIX_PATH "${CMAKE_PREFIX_PATH};${PROJECT_SOURCE_DIR}/third_party/proxygen/proxygen/_build/deps/lib/cmake/fizz")
set(CMAKE_PREFIX_PATH "${CMAKE_PREFIX_PATH};${PROJECT_SOURCE_DIR}/third_party/proxygen/proxygen/_build/deps/lib64/cmake/fmt")
find_package(folly CONFIG REQUIRED)
find_package(proxygen REQUIRED
			HINTS ${PROJECT_SOURCE_DIR}/third_party/proxygen/proxygen/_build)

message("FOLLY_INCLUDE_DIR: ${FOLLY_INCLUDE_DIR}")
message("FOLLY_LIBRARIES: ${FOLLY_LIBRARIES}")
message("proxygen_INCLUDE_DIRS: ${proxygen_INCLUDE_DIRS}")

find_package(gflags COMPONENTS shared)

include_directories(${FOLLY_INCLUDE_DIR} ${proxygen_INCLUDE_DIRS} )


# add the untrusted source files
set(SRCS
    App.cpp
    src/ocalls.cpp
    src/utils/log.cpp
    src/utils/utils.cpp
    src/GlobalTable.cpp
    src/LocalTable.cpp
    src/Tuple.cpp
	src/EchoHandler.cpp
	src/CurlClient.cpp
	src/ReqSender.cpp
)

# build executable file
add_untrusted_executable(test LINK_LIBS proxygen::proxygen proxygen::proxygenhttpserver Folly::folly SRCS ${SRCS} test.cpp EDL ${CMAKE_SOURCE_DIR}/Enclave/config/Enclave.edl EDL_SEARCH_PATHS ${EDL_SEARCH_PATHS})
add_untrusted_executable(worker LINK_LIBS proxygen::proxygen proxygen::proxygenhttpserver Folly::folly SRCS ${SRCS} worker.cpp EDL ${CMAKE_SOURCE_DIR}/Enclave/config/Enclave.edl EDL_SEARCH_PATHS ${EDL_SEARCH_PATHS})
add_untrusted_executable(benchmark LINK_LIBS proxygen::proxygen Folly::folly SRCS ${SRCS} benchmark.cpp EDL ${CMAKE_SOURCE_DIR}/Enclave/config/Enclave.edl EDL_SEARCH_PATHS ${EDL_SEARCH_PATHS})



