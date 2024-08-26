#ifndef UTILS_H
#define UTILS_H

#include <chrono>
#include <string>
#include <unordered_map>
#include <vector>
#include "Operators.h"
#include <proxygen/lib/http/session/HTTPTransaction.h>
#include "log.h"

namespace utils {
  extern int num_partitions;
  extern std::vector<std::string> worker_urls;

  extern bool is_distributed;

  const int DUMMY_VAL = 9999;

  extern float KAPPA;  // failure probability = exp{-kappa}

  extern std::chrono::time_point<std::chrono::high_resolution_clock> start;
  extern std::unordered_map<int, bool> flag_map;
  extern std::unordered_map<int, std::chrono::time_point<std::chrono::high_resolution_clock>> time_map;

  extern std::unordered_map<int, std::unordered_map<std::string, double>> duration_matrix;
  extern std::unordered_map<int, long long> comm_map;
  extern long long total_comm;
  extern int time_phase;
  extern double read_write_ms;
  extern double comp_ms;

  extern long long header_total_size;
  extern long long body_total_size;

  long long coordinator_get_comm_and_reset();

  // void set_distributed(bool val);
  // bool is_distributed();

  void read_config_file(const std::string& config_file, bool is_worker);

  bool exist_in_matrix(int id, std::string content);
  void update_phase(std::string phase_name = "");
  void ReadConfig(const std::string& filename);
  std::string extractRet(const std::string& str);
  std::string vectorToString(const std::vector<int>& vec);
  std::vector<int> stringToVector(const std::string& str);

  int extractPort(const std::string& addr);

  uint8_t* readPartitionFile(std::string filePath, size_t* file_length);

  // clear all information
  void reset();

  int getSizeBound(int n, int p);

  // Utility functions for serializing and deserializing AssociateOperator objects
  std::string serializeOperator(const AssociateOperator& op);
  AssociateOperator* deserializeOperator(const std::string& data);

  class CommStatTransportCallback : public proxygen::HTTPTransactionTransportCallback {
    void firstHeaderByteFlushed() noexcept override {
    }

    void firstByteFlushed() noexcept override {
    }

    void lastByteFlushed() noexcept override {
    }

    void trackedByteFlushed() noexcept override {
    }

    void lastByteAcked(std::chrono::milliseconds latency) noexcept override {
    }

    void headerBytesGenerated(proxygen::HTTPHeaderSize& size) noexcept override {
      header_total_size += size.uncompressed;
    }

    void headerBytesReceived(const proxygen::HTTPHeaderSize& size) noexcept override {
    }

    void bodyBytesGenerated(size_t  nbytes) noexcept override {
      body_total_size += nbytes;
    }

    void bodyBytesReceived(size_t size) noexcept override {
    }
  };


  extern CommStatTransportCallback* _callback;

}  // namespace utils

#endif  // UTILS_H