#include <folly/SocketAddress.h>
#include <folly/init/Init.h>
#include <folly/io/SocketOptionMap.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/SSLContext.h>
#include <folly/portability/GFlags.h>
#include <folly/ssl/Init.h>
#include <proxygen/lib/http/HTTPConnector.h>
#include "CurlClient.h"

using namespace proxygen;
using namespace std;

const int timeout_const = 10000000;  //10000s, may need to be longer in huge data scenario

std::string send_request(HTTPMethod httpMethod,
                         const std::string url_str,
                         const std::string proxy_str = "",
                         const unordered_map<string, string> headers_map = {},
                         const std::string input_filename_str = "",
                         int http_client_connect_timeout = timeout_const,
                         const char* post_content = "",
                         size_t post_content_len = 0,
                         int recv_window = 65536,
                         bool log_response = false,
                         bool h2c = false,
                         const std::string plaintext_proto_str = "");

std::string send_get_request(const std::string url_str,
                             const unordered_map<string, string> headers_map,
                             int http_client_connect_timeout = timeout_const);

std::string send_post_request(const std::string url_str,
                              const unordered_map<string, string> headers_map,
                              const std::string input_filename_str,
                              int http_client_connect_timeout = timeout_const,
                              const char* post_content = "",
                              size_t post_content_len = 0);
