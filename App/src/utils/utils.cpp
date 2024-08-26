#include "utils.h"
#include <boost/program_options.hpp>
#include <cmath>
#include <fstream>
#include <iostream>
#include <sstream>
#include "App.h"
#include "Enclave_u.h"
#include "ReqSender.h"
#include "log.h"

namespace utils {
    // float SIGMA = 40.0; // failure probability = 2^{-sigma}
    float KAPPA = 40.0 * 0.69314718;  // failure probability = exp{-kappa}

    int num_partitions;
    bool is_distributed = false;
    std::vector<std::string> worker_urls;
    long long header_total_size = 0, body_total_size = 0;
    CommStatTransportCallback* _callback = new CommStatTransportCallback();

    // coordinator send config file to workers
    void send_config_file(const std::string& config_file) {
        std::ifstream file(config_file, std::ios::binary);
        if (!file) {
            std::cerr << "Cannot open file: " << config_file << std::endl;
            throw;
        }
        file.seekg(0, std::ios::end);
        int file_size = (int)file.tellg();
        file.seekg(0, std::ios::beg);
        char* buffer = new char[file_size];
        file.read(buffer, file_size);
        file.close();
        for (int i = 0; i < num_partitions; i++) {
            std::unordered_map<std::string, std::string> header_map = {
                {"task",           "write_file"                                                      },
                {"file_name",      "../data/shuffle_buffer/config_" + std::to_string(i)},
                {"Content-Length", std::to_string(file_size)                                         }
            };
            send_post_request(
                worker_urls[i],
                header_map,
                "", timeout_const,
                buffer, file_size);
        }
        delete[] buffer;
    }

    // read config from file
    void parse_config(const std::string& config_file) {
        namespace po = boost::program_options;
        po::options_description desc("config");
        desc.add_options()("num_partitions", po::value<int>()->notifier([](int _num_partitions) {
            num_partitions = _num_partitions;
            }))("sigma", po::value<float>()->notifier([](float sigma) {
                KAPPA = sigma * 0.69314718;
                }))("real_distributed", po::value<bool>()->notifier([](bool real_distributed) {
                    is_distributed = real_distributed;
                    }))("worker_urls", po::value<std::vector<std::string>>()->composing()->notifier([](const std::vector<std::string>& _worker_urls) {
                        worker_urls = _worker_urls;
                        }))("log_level", po::value<std::string>()->notifier([](const std::string& level) {
                            if (level == "DEBUG")
                                log_set_level(LOG_DEBUG);
                            else if (level == "INFO")
                                log_set_level(LOG_INFO);
                            else if (level == "WARN")
                                log_set_level(LOG_WARN);
                            else if (level == "ERROR")
                                log_set_level(LOG_ERROR);
                            else
                                log_error("Unknown log_level");
                            }));
                        po::variables_map vm;
                        //log_info("Read config start");
                        std::ifstream if_config_file(config_file);
                        po::store(po::parse_config_file(if_config_file, desc), vm);
                        po::notify(vm);
                        //log_info("Read config finished");
                        log_info("num_partitions = %d, sigma = %.1f", num_partitions, vm["sigma"].as<float>());
    }

    void read_config_file(const std::string& config_file, bool is_worker) {
        if (!is_worker) {  // if this is not worker, then directly read config file
            parse_config(config_file);
            if (is_distributed) {  // send config to workers and let them read config
                send_config_file(config_file);
                for (int i = 0; i < num_partitions; i++) {
                    std::unordered_map<std::string, std::string> header_map = {
                        {"task",      "read_config_file"},
                        {"worker_id", std::to_string(i) }
                    };
                    send_get_request(worker_urls[i], header_map);
                }
            }
        }
        else {
            parse_config(config_file);
        }
        // initialize enclave
        if (!is_distributed) {
            if (initialize_enclave(false) < 0) {
                log_error("Enclave initialization failed");
            }
        }
        if (is_worker || !is_distributed) {
            // set num_partitions inside enclave
            int ret;
            sgx_status_t ecall_status = ecall_setup_env(global_eid, &ret, utils::num_partitions);
            if (ecall_status) {
                print_error_message(ecall_status);
                log_error("ecall failed");
            }
        }
    }

    // 将 vector<int> 转换为由逗号分隔的 string
    std::string vectorToString(const std::vector<int>& vec) {
        std::ostringstream oss;
        for (size_t i = 0; i < vec.size(); ++i) {
            if (i != 0) {
                oss << "_";  // 添加分隔符，除了第一个元素前
            }
            oss << vec[i];
        }
        return oss.str();
    }

    // 将由逗号分隔的 string 转换为 vector<int>
    std::vector<int> stringToVector(const std::string& str) {
        std::vector<int> vec;
        std::istringstream iss(str);
        std::string token;
        while (std::getline(iss, token, '_')) {  // 使用逗号作为分隔符
            try {
                int num = std::stoi(token);
                vec.push_back(num);
            }
            catch (const std::invalid_argument& e) {
                // Handle the exception, e.g., you can continue or return an empty vector
                std::cerr << "Invalid number: " << token << std::endl;
                continue;  // Optionally skip this token
                //return std::vector<int>(); // Or return an empty vector
            }
            catch (const std::out_of_range& e) {
                // Handle the large number or negative overflow
                std::cerr << "Number out of range: " << token << std::endl;
                continue;
            }
        }
        return vec;
    }

    /* ret format is (ret:xxx), extract xxx from ret */
    std::string extractRet(const std::string& str) {
        size_t startPos = str.find("ret:");
        if (startPos == std::string::npos) {
            // "ret:" 没有找到，返回空字符串或适当的错误值
            return "";
        }
        startPos += 4;  // 移动到 "ret:" 的后面，即 "xxx" 的起始位置

        // 我们假设结尾是闭的括号 ')'
        size_t endPos = str.find(")", startPos);
        if (endPos == std::string::npos) {
            // 没有闭括号找到，返回从 "ret:" 开始到字符串结尾的子字符串
            return str.substr(startPos);
        }

        // 提取并返回从startPos到endPos的子字符串
        return str.substr(startPos, endPos - startPos);
    }

    int extractPort(const std::string& addr) {
        // 查找 "://" 的位置
        size_t protocol_end = addr.find("://");
        if (protocol_end == std::string::npos) {
            throw std::invalid_argument("Invalid address format: protocol not found");
        }

        // 跳过协议部分，从后面的部分开始查找 ":"
        size_t host_start = protocol_end + 3;
        size_t port_start = addr.find(":", host_start);
        if (port_start == std::string::npos) {
            throw std::invalid_argument("Invalid address format: port not found");
        }

        // 查找端口号后的 "/"，如果没有找到，就到字符串的末尾
        size_t port_end = addr.find("/", port_start);
        if (port_end == std::string::npos) {
            port_end = addr.length();
        }

        // 提取端口号字符串并转换为整数
        std::string port_str = addr.substr(port_start + 1, port_end - port_start - 1);
        try {
            int port = std::stoi(port_str);
            return port;
        }
        catch (const std::invalid_argument& e) {
            throw std::invalid_argument("Invalid port number");
        }
        catch (const std::out_of_range& e) {
            throw std::invalid_argument("Port number out of range");
        }
    }

    uint8_t* readPartitionFile(std::string filePath, size_t* file_length) {
        std::ifstream file(filePath, std::ios::binary);  // 打开文件，以二进制模式读取

        if (!file) {
            std::cerr << "无法打开文件 {" << filePath << "}" << std::endl;
            throw;
        }

        // 获取文件长度
        file.seekg(0, std::ios::end);
        int file_size = (int)file.tellg();
        file.seekg(0, std::ios::beg);

        char* buffer = new char[file_size + 1];  // 需要 +1 来存储结尾的空字符
        file.read(buffer, file_size);
        buffer[file_size] = '\0';
        file.close();

        *file_length = file_size + 1;
        return (uint8_t*)buffer;
    }

    std::chrono::time_point<std::chrono::high_resolution_clock> start;
    std::unordered_map<int, bool> flag_map;
    std::unordered_map<int, std::chrono::time_point<std::chrono::high_resolution_clock>> time_map;
    std::unordered_map<int, std::unordered_map<std::string, double>> duration_matrix;
    std::unordered_map<int, long long> comm_map;
    long long total_comm = 0;
    int time_phase = 0;
    double read_write_ms = 0;
    double comp_ms = 0;

    long long total_bytes_sent = 0;

    void update_phase(std::string phase_name) {
        time_phase++;
        double max_read_write = 0;
        double max_comp = 0;
        for (auto it = duration_matrix.begin(); it != duration_matrix.end(); ++it) {
            double read_write = it->second["read_write"];
            double comp = it->second["TOTAL"] - it->second["read_write"];

            if (read_write > max_read_write)
                max_read_write = read_write;
            if (comp > max_comp)
                max_comp = comp;

            /* Just for profile */
            for (auto it2 = it->second.begin(); it2 != it->second.end(); ++it2) {
                if (it2->first != "TOTAL" && it2->first != "read_write")
                    std::cout << "<<< profile " << it2->first << ": " << it2->second << "ms" << std::endl;
            }

            it->second.clear();
        }
        if (phase_name.empty())
            phase_name = std::to_string(time_phase);
        if (!is_distributed) {
            std::cout << "phase " << phase_name << " max_read_write "
                << ": " << max_read_write << " ms" << std::endl;
            std::cout << "phase " << phase_name << " max_comp "
                << ": " << max_comp << " ms" << std::endl;
        }
        read_write_ms += max_read_write;
        comp_ms += max_comp;
    }

    bool exist_in_matrix(int id, std::string content) {
        auto it = duration_matrix.find(id);
        if (it == duration_matrix.end())
            return false;
        auto it2 = duration_matrix[id].find(content);
        if (it2 == duration_matrix[id].end())
            return false;
        return true;
    }

    void reset() {
        total_comm = 0;
        time_phase = 0;
        read_write_ms = 0;
        comp_ms = 0;
        flag_map.clear();
        duration_matrix.clear();
        comm_map.clear();
    }

    int getSizeBound(int n, int p) {
        // x^2=bx+c
        double b = (KAPPA + 2.0l * log(p)) * p / n;
        double c = 2.0l * b;
        double x = (sqrt(b * b + 4.0 * c) + b) / 2.0l;
        // By chernoff bound
        return (int)ceil((1 + x) * n / p);
    }

    std::string serializeOperator(const AssociateOperator& op) {
        std::ostringstream out;
        out << "(" << op.op_id;

        if (op.op_id != AssociateOperator::COPY) {
            out << " " << op.group_by_columns.size();
            for (int col : op.group_by_columns) {
                out << " " << col;
            }
            out << " " << op.aggregate_column;
        }

        out << ")";
        return out.str();
    }

    AssociateOperator* deserializeOperator(const std::string& data) {
        // Ensure the data starts with '(' and ends with ')'
        if (data.front() != '(' || data.back() != ')') {
            throw std::invalid_argument("Invalid serialization format");
        }

        // Remove the parentheses
        std::string trimmed_data = data.substr(1, data.size() - 2);

        std::istringstream in(trimmed_data);
        int opId;
        in >> opId;

        if (opId == AssociateOperator::ADD) {
            int num_group_by;
            in >> num_group_by;
            std::vector<int> group_by_columns(num_group_by);
            for (int i = 0; i < num_group_by; ++i) {
                in >> group_by_columns[i];
            }
            int aggregate_column;
            in >> aggregate_column;
            return new OperatorAdd(group_by_columns, aggregate_column);
        }
        else if (opId == AssociateOperator::MUL) {
            int num_group_by;
            in >> num_group_by;
            std::vector<int> group_by_columns(num_group_by);
            for (int i = 0; i < num_group_by; ++i) {
                in >> group_by_columns[i];
            }
            int aggregate_column;
            in >> aggregate_column;
            return new OperatorMul(group_by_columns, aggregate_column);
        }
        else if (opId == AssociateOperator::MAX) {
            int num_group_by;
            in >> num_group_by;
            std::vector<int> group_by_columns(num_group_by);
            for (int i = 0; i < num_group_by; ++i) {
                in >> group_by_columns[i];
            }
            int aggregate_column;
            in >> aggregate_column;
            return new OperatorMax(group_by_columns, aggregate_column);
        }
        else if (opId == AssociateOperator::MIN) {
            int num_group_by;
            in >> num_group_by;
            std::vector<int> group_by_columns(num_group_by);
            for (int i = 0; i < num_group_by; ++i) {
                in >> group_by_columns[i];
            }
            int aggregate_column;
            in >> aggregate_column;
            return new OperatorMin(group_by_columns, aggregate_column);
        }
        else if (opId == AssociateOperator::COPY) {
            return new OperatorCopy();
        }
        else {
            throw std::invalid_argument("Unknown OperatorList value");
        }
    }

    long long coordinator_get_comm_and_reset()
    {
        log_info("header size = %d, body size = %d", utils::header_total_size, utils::body_total_size);
        long long ret = utils::body_total_size + utils::header_total_size;
        utils::body_total_size = utils::header_total_size = 0;
        for (int i = 0; i < num_partitions; i++) {
            std::unordered_map<std::string, std::string> header_map = {
                {"task",  "get_comm_and_reset"  },
            };
            auto ret_str = utils::extractRet(send_get_request(worker_urls[i], header_map));
            ret += std::stoll(ret_str);
        }
        return ret;
    }
}  // namespace utils