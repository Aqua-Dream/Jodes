content="#  Copyright (c) 2018, Facebook, Inc.
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

# This module sets the following variables:
#   proxygen_FOUND
#   proxygen_INCLUDE_DIRS
#
# This module exports the following target:
#    proxygen::proxygen
#
# which can be used with target_link_libraries() to pull in the proxygen
# library.

@PACKAGE_INIT@

include(CMakeFindDependencyMacro)
find_dependency(fmt)
find_dependency(folly)
find_dependency(wangle)
if (\"@BUILD_QUIC@\")
  find_dependency(mvfst)
endif()
find_dependency(Fizz)
# For now, anything that depends on Proxygen has to copy its FindZstd.cmake
# and issue a \`find_package(Zstd)\`.  Uncommenting this won't work because
# this Zstd module exposes a library called \`zstd\`.  The right fix is
# discussed on D24686032.
#
# find_dependency(Zstd)
find_dependency(ZLIB)
find_dependency(OpenSSL)
find_dependency(Threads)
find_dependency(Boost 1.58 REQUIRED
  COMPONENTS
    iostreams
    context
    filesystem
    program_options
    regex
    system
    thread
)

set(JODES_PATH \$ENV{JODES_PATH})
set(PROXYGEN_TARGETS_CMAKE \"\${JODES_PATH}/third_party/proxygen/proxygen/_build/lib/cmake/proxygen/proxygen-targets.cmake\")

if(NOT TARGET proxygen::proxygen)
    include(\${PROXYGEN_TARGETS_CMAKE})
    get_target_property(proxygen_INCLUDE_DIRS proxygen::proxygen INTERFACE_INCLUDE_DIRECTORIES)
endif()

if(NOT proxygen_FIND_QUIETLY)
    message(STATUS \"Found proxygen: \${PACKAGE_PREFIX_DIR}\")
endif()"

# Write the content to a.in
echo "$content" > proxygen/cmake/proxygen-config.cmake.in
echo "proxygen-config.cmake.in amend succeed"