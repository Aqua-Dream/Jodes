/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "CurlClient.h"

#include <sys/stat.h>
#include <iostream>

#include <folly/FileUtil.h>
#include <folly/String.h>
#include <folly/io/async/SSLContext.h>
#include <folly/io/async/SSLOptions.h>
#include <folly/portability/GFlags.h>
#include <proxygen/lib/http/HTTPMessage.h>
#include <proxygen/lib/http/codec/HTTP2Codec.h>
#include <proxygen/lib/http/session/HTTPUpstreamSession.h>
#include "utils.h"

using namespace folly;
using namespace proxygen;
using namespace std;

DECLARE_int32(recv_window);

namespace CurlService {

    CurlClient::CurlClient(EventBase* evb,
        HTTPMethod httpMethod,
        const URL& url,
        const proxygen::URL* proxy,
        const HTTPHeaders& headers,
        const string& inputFilename,
        bool h2c,
        unsigned short httpMajor,
        unsigned short httpMinor,
        const char* post_content,
        size_t post_content_len)
        : evb_(evb),
        httpMethod_(httpMethod),
        url_(url),
        inputFilename_(inputFilename),
        h2c_(h2c),
        httpMajor_(httpMajor),
        httpMinor_(httpMinor),
        post_content_(post_content),
        post_content_len_(post_content_len) {
        if (proxy != nullptr) {
            proxy_ = std::make_unique<URL>(proxy->getUrl());
        }

        outputStream_ = std::make_unique<std::ostream>(std::cout.rdbuf());
        headers.forEach([this](const string& header, const string& val) {
            request_.getHeaders().add(header, val);
            });
    }

    bool CurlClient::saveResponseToFile(const std::string& outputFilename) {
        std::streambuf* buf;
        if (outputFilename.empty()) {
            return false;
        }
        uint16_t tries = 0;
        while (tries < std::numeric_limits<uint16_t>::max()) {
            std::string suffix = (tries == 0) ? "" : folly::to<std::string>("_", tries);
            auto filename = folly::to<std::string>(outputFilename, suffix);
            struct stat statBuf;
            if (stat(filename.c_str(), &statBuf) == -1) {
                outputFile_ =
                    std::make_unique<ofstream>(filename, ios::out | ios::binary);
                if (*outputFile_ && outputFile_->good()) {
                    buf = outputFile_->rdbuf();
                    outputStream_ = std::make_unique<std::ostream>(buf);
                    return true;
                }
            }
            tries++;
        }
        return false;
    }

    HTTPHeaders CurlClient::parseHeaders(const unordered_map<string, string>& headersString) {
        HTTPHeaders headers;
        for (const auto& headerPair : headersString) {
            headers.add(headerPair.first, headerPair.second);
        }
        return headers;
    }

    void CurlClient::initializeSsl(const string& caPath,
        const string& nextProtos,
        const string& certPath,
        const string& keyPath) {
        sslContext_ = std::make_shared<folly::SSLContext>();
        sslContext_->setOptions(SSL_OP_NO_COMPRESSION);
        folly::ssl::setCipherSuites<folly::ssl::SSLCommonOptions>(*sslContext_);
        if (!caPath.empty()) {
            sslContext_->loadTrustedCertificates(caPath.c_str());
        }
        if (!certPath.empty() && !keyPath.empty()) {
            sslContext_->loadCertKeyPairFromFiles(certPath.c_str(), keyPath.c_str());
        }
        list<string> nextProtoList;
        folly::splitTo<string>(
            ',', nextProtos, std::inserter(nextProtoList, nextProtoList.begin()));
        sslContext_->setAdvertisedNextProtocols(nextProtoList);
        h2c_ = false;
    }

    void CurlClient::sslHandshakeFollowup(HTTPUpstreamSession* session) noexcept {
        AsyncSSLSocket* sslSocket =
            dynamic_cast<AsyncSSLSocket*>(session->getTransport());

        const unsigned char* nextProto = nullptr;
        unsigned nextProtoLength = 0;
        sslSocket->getSelectedNextProtocol(&nextProto, &nextProtoLength);
        if (nextProto) {
            VLOG(1) << "Client selected next protocol "
                << string((const char*)nextProto, nextProtoLength);
        }
        else {
            VLOG(1) << "Client did not select a next protocol";
        }

        // Note: This ssl session can be used by defining a member and setting
        // something like sslSession_ = sslSocket->getSSLSession() and then
        // passing it to the connector::connectSSL() method
    }

    void CurlClient::setFlowControlSettings(int32_t recvWindow) {
        recvWindow_ = recvWindow;
    }

    void CurlClient::connectSuccess(HTTPUpstreamSession* session) {
        if (url_.isSecure()) {
            sslHandshakeFollowup(session);
        }

        session->setFlowControl(recvWindow_, recvWindow_, recvWindow_);
        sendRequest(session->newTransaction(this));
        session->closeWhenIdle();
    }

    void CurlClient::setupHeaders() {
        request_.setMethod(httpMethod_);
        request_.setHTTPVersion(httpMajor_, httpMinor_);
        if (proxy_) {
            request_.setURL(url_.getUrl());
        }
        else {
            request_.setURL(url_.makeRelativeURL());
        }
        request_.setSecure(url_.isSecure());
        if (h2c_) {
            HTTP2Codec::requestUpgrade(request_);
        }

        if (!request_.getHeaders().getNumberOfValues(HTTP_HEADER_USER_AGENT)) {
            request_.getHeaders().add(HTTP_HEADER_USER_AGENT, "proxygen_curl");
        }
        if (!request_.getHeaders().getNumberOfValues(HTTP_HEADER_HOST)) {
            request_.getHeaders().add(HTTP_HEADER_HOST, url_.getHostAndPort());
        }
        if (!request_.getHeaders().getNumberOfValues(HTTP_HEADER_ACCEPT)) {
            request_.getHeaders().add("Accept", "*/*");
        }
        if (loggingEnabled_) {
            request_.dumpMessage(4);
        }
    }



    void CurlClient::sendRequest(HTTPTransaction* txn) {
        txn->setTransportCallback(utils::_callback);
        log_debug("Sending request for %s", url_.getUrl().c_str());
        txn_ = txn;
        setupHeaders();

        //Debug
        // log_debug("Method: %s", request_.getMethodString().c_str());
        const proxygen::HTTPHeaders& headers = request_.getHeaders();
        std::string headers_str = "task: " + headers.rawGet("task") + "; ";
        headers.forEach([&](const std::string& header, const std::string& value) {
            if (header != "task")
                headers_str += header + ": " + value + "; ";
            });
        log_info(headers_str.c_str());

        txnStartTime_ = std::chrono::steady_clock::now();
        txn_->sendHeaders(request_);

        if (httpMethod_ == HTTPMethod::POST) {
            log_debug("post_content_len_ is %d", post_content_len_);
            // log_debug("post_content_ is %s", post_content_);
            if (post_content_len_ > 0) {
                txn_->sendBody(folly::IOBuf::wrapBuffer(post_content_, post_content_len_));
                txn_->sendEOM();
            }
            else {
                inputFile_ =
                    std::make_unique<ifstream>(inputFilename_, ios::in | ios::binary);
                sendBodyFromFile();
            }

        }
        else {
            txn_->sendEOM();
        }
    }

    void CurlClient::sendBodyFromFile() {
        const uint16_t kReadSize = 4096;
        CHECK(inputFile_);
        // Reading from the file by chunks
        // Important note: It's pretty bad to call a blocking i/o function like
        // ifstream::read() in an eventloop - but for the sake of this simple
        // example, we'll do it.
        // An alternative would be to put this into some folly::AsyncReader
        // object.
        while (inputFile_->good() && !egressPaused_) {
            unique_ptr<IOBuf> buf = IOBuf::createCombined(kReadSize);
            inputFile_->read((char*)buf->writableData(), kReadSize);
            buf->append(inputFile_->gcount());
            txn_->sendBody(std::move(buf));
        }
        if (!egressPaused_) {
            txn_->sendEOM();
        }
    }

    void CurlClient::printMessageImpl(proxygen::HTTPMessage* msg,
        const std::string& tag) {
        if (!loggingEnabled_) {
            return;
        }
        cout << tag;
        msg->dumpMessage(10);
    }

    void CurlClient::connectError(const folly::AsyncSocketException& ex) {
        LOG(ERROR)
            << "Coudln't connect to " << url_.getHostAndPort() << ":" << ex.what();
    }

    void CurlClient::setTransaction(HTTPTransaction*) noexcept {
    }

    void CurlClient::detachTransaction() noexcept {
    }

    void CurlClient::onHeadersComplete(unique_ptr<HTTPMessage> msg) noexcept {
        response_ = std::move(msg);
        printMessageImpl(response_.get());
        if (!headersLoggingEnabled_) {
            return;
        }
        response_->describe(*outputStream_);
        *outputStream_ << std::endl;
    }

    void CurlClient::onBody(std::unique_ptr<folly::IOBuf> chain) noexcept {
        log_debug("onBody called");
        if (chain) {
            std::string ret;
            const IOBuf* p = chain.get();
            do {
                //outputStream_->write((const char*)p->data(), p->length());
                ret.append(reinterpret_cast<const char*>(p->data()), p->length());
                //outputStream_->flush();
                p = p->next();
            } while (p != chain.get());
            task_ret = ret;
        }
    }

    void CurlClient::onTrailers(std::unique_ptr<HTTPHeaders>) noexcept {
        LOG_IF(INFO, loggingEnabled_) << "Discarding trailers";
    }

    void CurlClient::onEOM() noexcept {
        log_debug("Got EOM for %s. Txn Time= %d ms",
            url_.getUrl().c_str(),
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - txnStartTime_)
            .count());
        if (eomFunc_) {
            eomFunc_.value()();
        }
    }

    void CurlClient::onUpgrade(UpgradeProtocol) noexcept {
        LOG_IF(INFO, loggingEnabled_) << "Discarding upgrade protocol";
    }

    void CurlClient::onError(const HTTPException& error) noexcept {
        LOG_IF(ERROR, loggingEnabled_) << "An error occurred: " << error.what();
    }

    void CurlClient::onEgressPaused() noexcept {
        VLOG_IF(1, loggingEnabled_) << "Egress paused";
        egressPaused_ = true;
    }

    void CurlClient::onEgressResumed() noexcept {
        VLOG_IF(1, loggingEnabled_) << "Egress resumed";
        egressPaused_ = false;
        if (inputFile_) {
            sendBodyFromFile();
        }
    }

    void CurlClient::onPushedTransaction(
        proxygen::HTTPTransaction* pushedTxn) noexcept {
        //
        pushTxnHandlers_.emplace_back(std::make_unique<CurlPushHandler>(this));
        pushedTxn->setHandler(pushTxnHandlers_.back().get());
        // Add implementation of the push transaction reception here
    }

    const string& CurlClient::getServerName() const {
        const string& res = request_.getHeaders().getSingleOrEmpty(HTTP_HEADER_HOST);
        if (res.empty()) {
            return url_.getHost();
        }
        return res;
    }

    // CurlPushHandler methods
    void CurlClient::CurlPushHandler::setTransaction(
        proxygen::HTTPTransaction* txn) noexcept {
        LOG_IF(INFO, parent_->loggingEnabled_) << "Received pushed transaction";
        pushedTxn_ = txn;
    }

    void CurlClient::CurlPushHandler::detachTransaction() noexcept {
        LOG_IF(INFO, parent_->loggingEnabled_) << "Detached pushed transaction";
    }

    void CurlClient::CurlPushHandler::onHeadersComplete(
        std::unique_ptr<proxygen::HTTPMessage> msg) noexcept {
        if (!seenOnHeadersComplete_) {
            seenOnHeadersComplete_ = true;
            promise_ = std::move(msg);
            parent_->printMessageImpl(promise_.get(), "[PP] ");
        }
        else {
            response_ = std::move(msg);
            parent_->printMessageImpl(response_.get(), "[PR] ");
        }
    }

    void CurlClient::CurlPushHandler::onBody(
        std::unique_ptr<folly::IOBuf> chain) noexcept {
        parent_->onBody(std::move(chain));
    }

    void CurlClient::CurlPushHandler::onEOM() noexcept {
        LOG_IF(INFO, parent_->loggingEnabled_) << "Got PushTxn EOM";
    }

    void CurlClient::CurlPushHandler::onError(
        const proxygen::HTTPException& error) noexcept {
        parent_->onError(error);
    }

}  // namespace CurlService
