# Building
## Requirements
```
CMake >= 2.9
SGX SDK >= 2.14
OpenSSL >= 1.1.1
```
To install SGX SDK, you may refer to [Build an SGX confidential computing environment
](https://www.alibabacloud.com/help/en/ecs/user-guide/build-an-sgx-encrypted-computing-environment).

Other requirements could be installed as follows: For CentOS,  
``` shell
yum install -y boost-devel double-conversion-devel gflags-devel glog-devel libevent-devel libsodium-devel gperf
```
or for Ubuntu,
``` shelll
apt install -y libboost-dev libdouble-conversion-dev libgflags-dev libgoogle-glog-dev libevent-dev libsodium-dev gperf
```

## Compiling
First clone the repo with submodules:
``` shell
git clone --recurse-submodules <repo_url>
```

Then configure environment variables:
``` shell
export SGX_SDK={Your SGX SDK Installation Path}
export JODES_PATH={Your JODES Repo Path}
```

Next, build and install proxygen: 
```shell
# Install proxygen
cd $JODES_PATH/third_party/ && ./proxygen_cmake_amend.sh
cd proxygen/proxygen && ./build.sh
./install.sh
```

Now you are ready to build Jodes:
``` shell
cd $JODES_PATH
mkdir -p build && cd build

# Generate the build configuration
cmake ..

# [Optional] Customize the build settings, default is Simulation Mode
ccmake .               

# Compile the project
make -j 8
```
Note: Using `ccmake` you could configure SGX Simulation Mode (SGX_HW=OFF; SGX_MODE=PreRelease) which does not require SGX hardware, or SGX Hardware Mode (SGX_HW=ON; SGX_MODE=Release).
