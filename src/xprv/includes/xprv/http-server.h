#pragma once

// HTTP server for xprv — thin wrapper over ProofEngine.
//
// Coroutine-based Beast sessions. Routes:
//   GET  /health              → engine status JSON
//   GET  /prove?tx=<hash>     → JSON proof chain
//   GET  /prove?tx=...&format=bin → binary XPRV
//   POST /verify              → verify proof from request body

#include "proof-engine.h"

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <chrono>
#include <memory>
#include <string>

namespace catl::core {
struct RequestContext;
}

namespace xprv {

struct HttpServerOptions
{
    std::string bind_address = "127.0.0.1";
    uint16_t port = 8080;
    size_t max_request_body = 512 * 1024;  // 512KB
    std::chrono::seconds read_timeout{30};
    std::chrono::seconds write_timeout{30};
    std::string build_id;  // git short hash, set from XPRV_BUILD_ID env
};

class HttpServer : public std::enable_shared_from_this<HttpServer>
{
public:
    static std::shared_ptr<HttpServer>
    create(
        boost::asio::io_context& io,
        std::shared_ptr<ProofEngine> engine,
        HttpServerOptions opts = {});

    /// Start accepting connections.
    void
    start();

    /// Stop accepting new connections and close active sessions.
    void
    stop();

    bool
    is_stopping() const
    {
        return !accepting_;
    }

private:
    HttpServer(
        boost::asio::io_context& io,
        std::shared_ptr<ProofEngine> engine,
        HttpServerOptions opts);

    boost::asio::awaitable<void>
    accept_loop();

    boost::asio::awaitable<void>
    handle_session(
        boost::asio::ip::tcp::socket socket,
        std::shared_ptr<catl::core::RequestContext> req_ctx);

    /// Add X-XPRV metadata header to any response.
    template <typename Body>
    void
    stamp(boost::beast::http::response<Body>& res) const
    {
        res.set("X-XPRV", xprv_header_);
    }

    boost::asio::io_context& io_;
    std::shared_ptr<ProofEngine> engine_;
    HttpServerOptions opts_;
    boost::asio::ip::tcp::acceptor acceptor_;
    bool accepting_ = false;
    std::string xprv_header_;  // pre-built X-XPRV header value
};

}  // namespace xprv
