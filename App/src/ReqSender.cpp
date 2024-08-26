#include "ReqSender.h"

using namespace CurlService;
using namespace folly;
using namespace proxygen;

std::string send_get_request(const std::string url_str,
                             const unordered_map<string, string> headers_map,
                             int http_client_connect_timeout) {
    return send_request(HTTPMethod::GET, url_str, "", headers_map, "", http_client_connect_timeout);
}

std::string send_post_request(const std::string url_str,
                              const unordered_map<string, string> headers_map,
                              const std::string input_filename_str,
                              int http_client_connect_timeout,
                              const char* post_content,
                              size_t post_content_len) {
    return send_request(HTTPMethod::POST, url_str, "", headers_map, input_filename_str, http_client_connect_timeout, post_content, post_content_len);
}

std::string send_request(HTTPMethod httpMethod,
                         const std::string url_str,
                         const std::string proxy_str,
                         const unordered_map<string, string> headers_map,
                         const std::string input_filename_str,
                         int http_client_connect_timeout,
                         const char* post_content,
                         size_t post_content_len,
                         int recv_window,
                         bool log_response,
                         bool h2c,
                         const std::string plaintext_proto_str) {
#if FOLLY_HAVE_LIBGFLAGS
    // Enable glog logging to stderr by default.
    gflags::SetCommandLineOptionWithMode(
        "logtostderr", "1", gflags::SET_FLAGS_DEFAULT);
#endif

    EventBase evb;
    URL url(url_str);
    URL proxy(proxy_str);

    if (httpMethod != HTTPMethod::GET && httpMethod != HTTPMethod::POST) {
        log_error("httpMethod must be either GET or POST");
    }

    if (httpMethod == HTTPMethod::POST) {
        if (!post_content || post_content_len == 0) {
            log_error("post_content is null, try input_filename_str");
            try {
                File f(input_filename_str);
                (void)f;
            } catch (const std::system_error& se) {
                log_error("Couldn't open file for POST method %s", se.what());
            }
        }
    }

    HTTPHeaders headers = CurlClient::parseHeaders(headers_map);

    CurlClient curlClient(&evb,
                          httpMethod,
                          url,
                          proxy_str.empty() ? nullptr : &proxy,
                          headers,
                          input_filename_str,
                          h2c,
                          1,
                          1,
                          post_content,
                          post_content_len);
    curlClient.setFlowControlSettings(recv_window);
    curlClient.setLogging(log_response);

    SocketAddress addr;
    if (!proxy_str.empty()) {
        addr = SocketAddress(proxy.getHost(), proxy.getPort(), true);
    } else {
        addr = SocketAddress(url.getHost(), url.getPort(), true);
    }
    log_debug("Trying to connect to %s", addr.describe().c_str());

    // Note: HHWheelTimer is a large object and should be created at most
    // once per thread
    HHWheelTimer::UniquePtr timer{HHWheelTimer::newTimer(
        &evb,
        std::chrono::milliseconds(HHWheelTimer::DEFAULT_TICK_INTERVAL),
        AsyncTimeout::InternalEnum::NORMAL,
        std::chrono::milliseconds(http_client_connect_timeout))};
    HTTPConnector connector(&curlClient, timer.get());
    if (!plaintext_proto_str.empty()) {
        connector.setPlaintextProtocol(plaintext_proto_str);
    }
    static const SocketOptionMap opts{
        {{SOL_SOCKET, SO_REUSEADDR}, 1}
    };

    if (url.isSecure()) {
        log_error("Https not supported!");
    } else {
        connector.connect(
            &evb,
            addr,
            std::chrono::milliseconds(http_client_connect_timeout),
            opts);
    }

    evb.loop();
    auto ret = curlClient.getTaskRet();
    return ret;
}
