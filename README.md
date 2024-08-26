# Jodes: Efficient Oblivious Join in the Distributed Setting
See full version at [Jodes_full.pdf](Jodes_full.pdf).

## Building
See [BUILDING.md](BUILDING.md)

## Configuration
Edit `config/config.ini` to configure:

 - `real_distributed`: if false, then simulate the distribution mode in a single machine with single process; otherwise, both data and code should be deployed to a coordinator and several workers.
 - `log_level`: the log level.
 - `num_partitions`: the number of partitions (workers).
 - `sigma`: security parameter.
 - `worker_urls`: the url of workers (not used if `real_distributed=false`).

Note that only the coordinator needs configuration. If `real_distributed=true`, you should run the workers and wait for the enclaves initialization before starting the coordinator. For example, the coordinator configures
``` ini
real_distributed = false
log_level = WARN
num_partitions =  4
sigma = 40
worker_urls = http://10.10.10.10:10101/
worker_urls = http://10.10.10.11:10101/
worker_urls = http://10.10.10.12:10101/
worker_urls = http://10.10.10.13:10101/
```
Each of the four workers (with ip 10.10.10.1x) runs
``` shell
[root@xxxxxxxxx build]# ./App/worker --http_port=10101
INFO  /root/Jodes/App/worker.cpp:63: http_port is 10101
INFO  /root/Jodes/App/App.cpp:84: OCALL PRINT: Hello World!
INFO  /root/Jodes/App/worker.cpp:96: enclave initialization succeed
```
After enclaves initialization, coordinator can run either test or benchmark.

## Testing
See [TESTING.md](TESTING.md).

## Benchmark
Test data in the paper can be download here: [com-DBLP](https://snap.stanford.edu/data/bigdata/communities/com-dblp.ungraph.txt.gz), [email-EuAll](https://snap.stanford.edu/data/email-EuAll.txt.gz), [com-Youtube](https://snap.stanford.edu/data/bigdata/communities/com-youtube.ungraph.txt.gz), [wiki-topcats](https://snap.stanford.edu/data/wiki-topcats.txt.gz).

Make sure to clean and split the data to the same format as in test data.

Despite `config.ini`, you also need to configure `config/benchmark.ini`. Then run the following command for benchmark:
```  shell
cd build
./App/benchmark --config [config_file_path] --task [benchmark_file_path]
```
Here config_file_path and benchmark_file_path are optional.

### Remark
If you encounter the following error:
``` shell
./App/test: error while loading shared libraries: libsgx_urts_sim.so: cannot open shared object file: No such file or directory
```
Try:
``` shell
echo "$SGX_SDK/lib64" | sudo tee /etc/ld.so.conf.d/sgx.conf
sudo ldconfig
```

For benchmark with large size, you may need to increase the enclave heap size by editing `Enclave/config/Enclave.config.xml` and change line `<HeapMaxSize>0x10000000</HeapMaxSize>` to a large enough value. Note that larger size results in longer initialization time. Then remember to rebuild the project before rerunning the benchmark:
``` shell
cd build
make -j 8
./App/benchmark
```

## LICENSE
See [LICENSE](LICENSE).