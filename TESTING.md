
# Testing
```shell
cd build
./App/test --task [task] --config [config_file_path]
```

Here task could be one of the following: test_print, test_sort, test_expansion, test_pkjoin, test_join. The config file path is the path of config file you set in the previous step, which is optional.

## Example Output
With config 
``` ini
real_distributed = false
log_level = WARN
num_partitions =  4
sigma = 40
```

### test print
``` shell
[root@xxxxxxxxx build]# ./App/test --task=test_print
(37 rows)
(Partition 0, 9 rows)
****** global_id: 0, m_tuples size: 9, m_num_rows: 9, show_dummy is 0
 0 64 68 3; 
 32 50 12 1; 
 32 14 24 1; 
 30 4 66 1; 
 30 36 100 1; 
 24 46 2 1; 
 30 40 56 1; 
 30 52 96 1; 
 16 0 60 1; 
(Partition 1, 9 rows)
****** global_id: 0, m_tuples size: 9, m_num_rows: 9, show_dummy is 0
 2 14 44 1; 
 4 54 16 3; 
 28 56 40 1; 
 4 76 34 2; 
 8 32 38 1; 
 32 24 24 4; 
 10 76 90 9; 
 10 88 72 1; 
 6 96 22 1; 
(Partition 2, 9 rows)
****** global_id: 0, m_tuples size: 9, m_num_rows: 9, show_dummy is 0
 18 68 34 1; 
 20 28 16 1; 
 22 4 6 1; 
 32 74 12 1; 
 8 64 58 1; 
 10 8 94 1; 
 26 36 52 1; 
 26 54 94 1; 
 14 38 96 1; 
(Partition 3, 10 rows)
****** global_id: 0, m_tuples size: 10, m_num_rows: 10, show_dummy is 0
 14 76 26 1; 
 8 40 64 2; 
 12 10 74 1; 
 12 60 98 1; 
 12 76 66 1; 
 14 96 18 1; 
 30 0 28 1; 
 34 2 82 2; 
 22 16 14 1; 
 24 8 18 1; 
 ```

 ### test sort
 ``` shell
 [root@xxxxxxxxx build]# ./App/test --task=test_sort
--- Table Sorted by {0,2} ---
phase <random shuffle write> max_read_write : 5.519 ms
phase <random shuffle write> max_comp : 0.041 ms
phase <shuffle read> max_read_write : 0.052 ms
phase <shuffle read> max_comp : 0.03 ms
phase <sort get pivots> max_read_write : 0.161 ms
phase <sort get pivots> max_comp : 0.033 ms
phase <sort partition by pivots> max_read_write : 0.164 ms
phase <sort partition by pivots> max_comp : 0.046 ms
phase <sort local sort> max_read_write : 0.044 ms
phase <sort local sort> max_comp : 0.072 ms
(148 rows)
(Partition 0, 37 rows)
****** global_id: 0, m_tuples size: 37, m_num_rows: 15, show_dummy is 0
 0 64 68 3; 
 2 14 44 1; 
 4 54 16 3; 
 4 76 34 2; 
 6 96 22 1; 
 8 32 38 1; 
 8 64 58 1; 
 8 40 64 2; 
 10 88 72 1; 
 10 76 90 9; 
 10 8 94 1; 
 12 76 66 1; 
 12 10 74 1; 
 12 60 98 1; 
 14 96 18 1; 
(Partition 1, 37 rows)
****** global_id: 0, m_tuples size: 37, m_num_rows: 5, show_dummy is 0
 14 76 26 1; 
 14 38 96 1; 
 16 0 60 1; 
 18 68 34 1; 
 20 28 16 1; 
(Partition 2, 37 rows)
****** global_id: 0, m_tuples size: 37, m_num_rows: 16, show_dummy is 0
 22 4 6 1; 
 22 16 14 1; 
 24 46 2 1; 
 24 8 18 1; 
 26 36 52 1; 
 26 54 94 1; 
 28 56 40 1; 
 30 0 28 1; 
 30 40 56 1; 
 30 4 66 1; 
 30 52 96 1; 
 30 36 100 1; 
 32 74 12 1; 
 32 50 12 1; 
 32 24 24 4; 
 32 14 24 1; 
(Partition 3, 37 rows)
****** global_id: 0, m_tuples size: 37, m_num_rows: 1, show_dummy is 0
 34 2 82 2; 
 ```

 ### test expansion
 ``` shell
 [root@xxxxxxxxx build]# ./App/test --task=test_expansion
(37 rows)
(Partition 0, 9 rows)
****** global_id: 0, m_tuples size: 9, m_num_rows: 9, show_dummy is 1
 0 64 68 5;
 32 50 12 3;
 32 14 24 1;
 30 4 66 4;
 30 36 100 0;
 24 46 2 0;
 30 40 56 1;
 30 52 96 3;
 16 0 60 2;
(Partition 1, 9 rows)
****** global_id: 0, m_tuples size: 9, m_num_rows: 9, show_dummy is 1
 2 14 44 2;
 4 54 16 3;
 28 56 40 7;
 4 76 34 2;
 8 32 38 0;
 32 24 24 4;
 10 76 90 9;
 10 88 72 0;
 6 96 22 8;
(Partition 2, 9 rows)
****** global_id: 0, m_tuples size: 9, m_num_rows: 9, show_dummy is 1
 18 68 34 1;
 20 28 16 5;
 22 4 6 0;
 32 74 12 3;
 8 64 58 5;
 10 8 94 7;
 26 36 52 2;
 26 54 94 3;
 14 38 96 1;
(Partition 3, 10 rows)
****** global_id: 0, m_tuples size: 10, m_num_rows: 10, show_dummy is 1
 14 76 26 3;
 8 40 64 5;
 12 10 74 7;
 12 60 98 3;
 12 76 66 5;
 14 96 18 2;
 30 0 28 0;
 34 2 82 0;
 22 16 14 0;
 24 8 18 8;

--- Table Expansion by {3} ---
phase <random shuffle write> max_read_write : 0.296 ms
phase <random shuffle write> max_comp : 0.116 ms
phase <shuffle read> max_read_write : 0.034 ms
phase <shuffle read> max_comp : 0.025 ms
phase <shuffle by col write> max_read_write : 0.118 ms
phase <shuffle by col write> max_comp : 0.049 ms
phase <shuffle read> max_read_write : 0.034 ms
phase <shuffle read> max_comp : 0.028 ms
phase <expansion distribute> max_read_write : 0 ms
phase <expansion distribute> max_comp : 0.021 ms
phase <suffix sum phase 1> max_read_write : 0.033 ms
phase <suffix sum phase 1> max_comp : 0.01 ms
phase <suffix sum phase 2 id 0> max_read_write : 0.104 ms
phase <suffix sum phase 2 id 0> max_comp : 0.05 ms
phase <suffix sum phase 2 id not 0> max_read_write : 0.008 ms
phase <suffix sum phase 2 id not 0> max_comp : 0.009 ms
(120 rows)
(Partition 0, 30 rows)
****** global_id: 0, m_tuples size: 30, m_num_rows: 30, show_dummy is 1
 0 64 68;
 0 64 68;
 0 64 68;
 0 64 68;
 0 64 68;
 32 50 12;
 32 50 12;
 32 50 12;
 32 14 24;
 30 4 66;
 30 4 66;
 30 4 66;
 30 4 66;
 30 40 56;
 30 52 96;
 30 52 96;
 30 52 96;
 16 0 60;
 16 0 60;
 2 14 44;
 2 14 44;
 4 54 16;
 4 54 16;
 4 54 16;
 28 56 40;
 28 56 40;
 28 56 40;
 28 56 40;
 28 56 40;
 28 56 40;
(Partition 1, 30 rows)
****** global_id: 0, m_tuples size: 30, m_num_rows: 30, show_dummy is 1
 28 56 40;
 4 76 34;
 4 76 34;
 32 24 24;
 32 24 24;
 32 24 24;
 32 24 24;
 10 76 90;
 10 76 90;
 10 76 90;
 10 76 90;
 10 76 90;
 10 76 90;
 10 76 90;
 10 76 90;
 10 76 90;
 6 96 22;
 6 96 22;
 6 96 22;
 6 96 22;
 6 96 22;
 6 96 22;
 6 96 22;
 6 96 22;
 18 68 34;
 20 28 16;
 20 28 16;
 20 28 16;
 20 28 16;
 20 28 16;
(Partition 2, 30 rows)
****** global_id: 0, m_tuples size: 30, m_num_rows: 30, show_dummy is 1
 32 74 12;
 32 74 12;
 32 74 12;
 8 64 58;
 8 64 58;
 8 64 58;
 8 64 58;
 8 64 58;
 10 8 94;
 10 8 94;
 10 8 94;
 10 8 94;
 10 8 94;
 10 8 94;
 10 8 94;
 26 36 52;
 26 36 52;
 26 54 94;
 26 54 94;
 26 54 94;
 14 38 96;
 14 76 26;
 14 76 26;
 14 76 26;
 8 40 64;
 8 40 64;
 8 40 64;
 8 40 64;
 8 40 64;
 12 10 74;
(Partition 3, 30 rows)
****** global_id: 0, m_tuples size: 30, m_num_rows: 24, show_dummy is 1
 12 10 74;
 12 10 74;
 12 10 74;
 12 10 74;
 12 10 74;
 12 10 74;
 12 60 98;
 12 60 98;
 12 60 98;
 12 76 66;
 12 76 66;
 12 76 66;
 12 76 66;
 12 76 66;
 14 96 18;
 14 96 18;
 24 8 18;
 24 8 18;
 24 8 18;
 24 8 18;
 24 8 18;
 24 8 18;
 24 8 18;
 24 8 18;
dummy
dummy
dummy
dummy
dummy
dummy
```

### test pkjoin
``` shell
[root@xxxxxxxxx build]# ./App/test --task=test_pkjoin
phase <pkjoin local sort R> max_read_write : 0 ms
phase <pkjoin local sort R> max_comp : 0.028 ms
phase <shuffle by key write> max_read_write : 0.184 ms
phase <shuffle by key write> max_comp : 0.045 ms
phase <shuffle read> max_read_write : 0.039 ms
phase <shuffle read> max_comp : 0.027 ms
phase <shuffle by key write> max_read_write : 0.216 ms
phase <shuffle by key write> max_comp : 0.045 ms
phase <shuffle read> max_read_write : 0.036 ms
phase <shuffle read> max_comp : 0.026 ms
phase <pkjoin combine1> max_read_write : 0 ms
phase <pkjoin combine1> max_comp : 0.044 ms
phase <shuffle by col write> max_read_write : 0.109 ms
phase <shuffle by col write> max_comp : 0.037 ms
phase <shuffle read> max_read_write : 0.033 ms
phase <shuffle read> max_comp : 0.027 ms
phase <pkjoin combine2> max_read_write : 0 ms
phase <pkjoin combine2> max_comp : 0.008 ms
(80 rows)
(Partition 0, 20 rows)
****** global_id: 0, m_tuples size: 20, m_num_rows: 2, show_dummy is 0
 4 200 -1 -2 -3 -4 -5;
 6 100 -1 -2 -3 -4 -5;
(Partition 1, 20 rows)
****** global_id: 0, m_tuples size: 20, m_num_rows: 0, show_dummy is 0
(Partition 2, 20 rows)
****** global_id: 0, m_tuples size: 20, m_num_rows: 4, show_dummy is 0
 2 200 -1 -2 -3 -4 -5;
 7 200 -1 -2 -3 -4 -5;
 7 200 -1 -2 -3 -4 -5;
 7 200 -1 -2 -3 -4 -5;
(Partition 3, 20 rows)
****** global_id: 0, m_tuples size: 20, m_num_rows: 4, show_dummy is 0
 2 200 -1 -2 -3 -4 -5;
 7 200 -1 -2 -3 -4 -5;
 8 200 -1 -2 -3 -4 -5;
 9 100 -1 -2 -3 -4 -5;
```

### test join
``` shell
[root@xxxxxxxxx build]# ./App/test --task=test_join
phase <random shuffle write> max_read_write : 0.193 ms
phase <random shuffle write> max_comp : 0.049 ms
phase <shuffle read> max_read_write : 0.037 ms
phase <shuffle read> max_comp : 0.026 ms
phase <sort get pivots> max_read_write : 0.11 ms
phase <sort get pivots> max_comp : 0.034 ms
phase <sort partition by pivots> max_read_write : 0.12 ms
phase <sort partition by pivots> max_comp : 0.049 ms
phase <sort local sort> max_read_write : 0.034 ms
phase <sort local sort> max_comp : 0.038 ms
phase <random shuffle write> max_read_write : 0.108 ms
phase <random shuffle write> max_comp : 0.031 ms
phase <shuffle read> max_read_write : 0.033 ms
phase <shuffle read> max_comp : 0.024 ms
phase <sort get pivots> max_read_write : 0.095 ms
phase <sort get pivots> max_comp : 0.026 ms
phase <sort partition by pivots> max_read_write : 0.107 ms
phase <sort partition by pivots> max_comp : 0.039 ms
phase <sort local sort> max_read_write : 0.033 ms
phase <sort local sort> max_comp : 0.03 ms
phase <pkjoin local sort R> max_read_write : 0.501 ms
phase <pkjoin local sort R> max_comp : 0.271 ms
phase <shuffle by key write> max_read_write : 0.103 ms
phase <shuffle by key write> max_comp : 0.051 ms
phase <shuffle read> max_read_write : 0.039 ms
phase <shuffle read> max_comp : 0.033 ms
phase <shuffle by key write> max_read_write : 0.099 ms
phase <shuffle by key write> max_comp : 0.063 ms
phase <shuffle read> max_read_write : 0.038 ms
phase <shuffle read> max_comp : 0.037 ms
phase <pkjoin combine1> max_read_write : 0 ms
phase <pkjoin combine1> max_comp : 0.152 ms
phase <shuffle by col write> max_read_write : 0.12 ms
phase <shuffle by col write> max_comp : 0.048 ms
phase <shuffle read> max_read_write : 0.037 ms
phase <shuffle read> max_comp : 0.033 ms
phase <pkjoin combine2> max_read_write : 0 ms
phase <pkjoin combine2> max_comp : 0.022 ms
phase <pkjoin local sort R> max_read_write : 0 ms
phase <pkjoin local sort R> max_comp : 0.042 ms
phase <shuffle by key write> max_read_write : 0.111 ms
phase <shuffle by key write> max_comp : 0.056 ms
phase <shuffle read> max_read_write : 0.055 ms
phase <shuffle read> max_comp : 0.042 ms
phase <shuffle by key write> max_read_write : 0.105 ms
phase <shuffle by key write> max_comp : 0.044 ms
phase <shuffle read> max_read_write : 0.045 ms
phase <shuffle read> max_comp : 0.031 ms
phase <pkjoin combine1> max_read_write : 0 ms
phase <pkjoin combine1> max_comp : 0.148 ms
phase <shuffle by col write> max_read_write : 0.126 ms
phase <shuffle by col write> max_comp : 0.057 ms
phase <shuffle read> max_read_write : 0.038 ms
phase <shuffle read> max_comp : 0.041 ms
phase <pkjoin combine2> max_read_write : 0 ms
phase <pkjoin combine2> max_comp : 0.044 ms
phase <sum> max_read_write : 0 ms
phase <sum> max_comp : 0.012 ms
phase <random shuffle write> max_read_write : 0.23 ms
phase <random shuffle write> max_comp : 0.115 ms
phase <shuffle read> max_read_write : 0.035 ms
phase <shuffle read> max_comp : 0.031 ms
phase <shuffle by col write> max_read_write : 0.101 ms
phase <shuffle by col write> max_comp : 0.055 ms
phase <shuffle read> max_read_write : 0.037 ms
phase <shuffle read> max_comp : 0.037 ms
phase <expansion distribute> max_read_write : 0 ms
phase <expansion distribute> max_comp : 0.041 ms
phase <suffix sum phase 1> max_read_write : 0.029 ms
phase <suffix sum phase 1> max_comp : 0.01 ms
phase <suffix sum phase 2 id 0> max_read_write : 0.113 ms
phase <suffix sum phase 2 id 0> max_comp : 0.048 ms
phase <suffix sum phase 2 id not 0> max_read_write : 0.008 ms
phase <suffix sum phase 2 id not 0> max_comp : 0.009 ms
phase <random shuffle write> max_read_write : 0.218 ms
phase <random shuffle write> max_comp : 0.133 ms
phase <shuffle read> max_read_write : 0.04 ms
phase <shuffle read> max_comp : 0.043 ms
phase <shuffle by col write> max_read_write : 0.121 ms
phase <shuffle by col write> max_comp : 0.06 ms
phase <shuffle read> max_read_write : 0.037 ms
phase <shuffle read> max_comp : 0.04 ms
phase <expansion distribute> max_read_write : 0 ms
phase <expansion distribute> max_comp : 0.043 ms
phase <suffix sum phase 1> max_read_write : 0.029 ms
phase <suffix sum phase 1> max_comp : 0.011 ms
phase <suffix sum phase 2 id 0> max_read_write : 0.108 ms
phase <suffix sum phase 2 id 0> max_comp : 0.049 ms
phase <suffix sum phase 2 id not 0> max_read_write : 0.008 ms
phase <suffix sum phase 2 id not 0> max_comp : 0.009 ms
phase <random shuffle write> max_read_write : 0.456 ms
phase <random shuffle write> max_comp : 0.204 ms
phase <shuffle read> max_read_write : 0.032 ms
phase <shuffle read> max_comp : 0.025 ms
phase <shuffle by col write> max_read_write : 0.091 ms
phase <shuffle by col write> max_comp : 0.042 ms
phase <shuffle read> max_read_write : 0.04 ms
phase <shuffle read> max_comp : 0.028 ms
(40 rows)
(Partition 0, 10 rows)
****** global_id: 0, m_tuples size: 10, m_num_rows: 10, show_dummy is 0
 1 100 -1 -2 -3 -4 -5;
 1 100 -1 -2 -3 -4 -5;
 1 100 -1 -2 -3 -4 -5;
 1 100 -1 -2 -3 -4 -5;
 2 100 -1 -2 -3 -4 -5;
 2 100 -1 -2 -3 -4 -5;
 2 200 -1 -2 -3 -4 -5;
 2 200 -1 -2 -3 -4 -5;
 2 200 -1 -2 -3 -4 -5;
 2 200 -1 -2 -3 -4 -5;
(Partition 1, 10 rows)
****** global_id: 0, m_tuples size: 10, m_num_rows: 10, show_dummy is 0
 3 200 -1 -2 -3 -4 -5;
 3 200 -1 -2 -3 -4 -5;
 3 200 -1 -2 -3 -4 -5;
 3 200 -1 -2 -3 -4 -5;
 4 200 -1 -2 -3 -4 -5;
 4 200 -1 -2 -3 -4 -5;
 4 200 -1 -2 -3 -4 -5;
 4 200 -1 -2 -3 -4 -5;
 5 100 -1 -2 -3 -4 -5;
 5 100 -1 -2 -3 -4 -5;
(Partition 2, 10 rows)
****** global_id: 0, m_tuples size: 10, m_num_rows: 10, show_dummy is 0
 5 100 -1 -2 -3 -4 -5;
 5 100 -1 -2 -3 -4 -5;
 6 100 -1 -2 -3 -4 -5;
 6 100 -1 -2 -3 -4 -5;
 6 100 -1 -2 -3 -4 -5;
 6 100 -1 -2 -3 -4 -5;
 7 200 -1 -2 -3 -4 -5;
 7 200 -1 -2 -3 -4 -5;
 7 200 -1 -2 -3 -4 -5;
 7 200 -1 -2 -3 -4 -5;
(Partition 3, 10 rows)
****** global_id: 0, m_tuples size: 10, m_num_rows: 10, show_dummy is 0
 7 200 -1 -2 -3 -4 -5;
 7 200 -1 -2 -3 -4 -5;
 7 200 -1 -2 -3 -4 -5;
 7 200 -1 -2 -3 -4 -5;
 8 200 -1 -2 -3 -4 -5;
 8 200 -1 -2 -3 -4 -5;
 9 100 -1 -2 -3 -4 -5;
 9 100 -1 -2 -3 -4 -5;
 9 100 -1 -2 -3 -4 -5;
 9 100 -1 -2 -3 -4 -5;
```