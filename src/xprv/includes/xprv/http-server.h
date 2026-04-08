#pragma once

// HTTP server for xprv — thin wrapper over ProofEngine.
//
// Supports single-network mode (backwards compat) and multi-network
// mode where multiple ProofEngines are registered.
//
// Routes:
//   GET  /health                          → aggregate or single-network status
//   GET  /network/{slug}/health           → single network health
//   GET  /prove?tx=<hash>                 → auto-detect network, JSON proof
//   GET  /prove?tx=...&format=bin         → auto-detect network, binary XPRV
//   GET  /network/{slug}/prove?tx=<hash>  → network-specific prove
//   GET  /peers                           → peers for default network, or
//                                           per-network list in multi-network
//   GET  /network/{slug}/peers            → single network peers
//   POST /verify                          → verify proof from request body

#include "proof-engine.h"

#include <atomic>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/http.hpp>
#include <chrono>
#include <map>
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
    /// Single-engine constructor (backwards compat).
    static std::shared_ptr<HttpServer>
    create(
        boost::asio::io_context& io,
        std::shared_ptr<ProofEngine> engine,
        HttpServerOptions opts = {});

    /// Multi-engine constructor.
    static std::shared_ptr<HttpServer>
    create(
        boost::asio::io_context& io,
        std::map<uint32_t, std::shared_ptr<ProofEngine>> engines,
        uint32_t default_network_id,
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
        std::map<uint32_t, std::shared_ptr<ProofEngine>> engines,
        uint32_t default_network_id,
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

    /// Find engine for a network ID. Returns nullptr if not found.
    ProofEngine*
    engine_for(uint32_t network_id) const;

    /// Find engine matching a network slug (name or numeric ID).
    /// Returns {network_id, engine*} or {0, nullptr} if not found.
    std::pair<uint32_t, ProofEngine*>
    engine_for_slug(std::string_view slug) const;

    /// Look up a tx hash across all engines in parallel.
    /// Returns {network_id, ledger_seq} of the first engine to find it,
    /// or {0, 0} if not found on any network.
    boost::asio::awaitable<std::pair<uint32_t, uint32_t>>
    lookup_tx_all(std::string const& tx_hash);

    boost::asio::io_context& io_;
    std::map<uint32_t, std::shared_ptr<ProofEngine>> engines_;
    uint32_t default_network_id_ = 0;
    HttpServerOptions opts_;
    boost::asio::ip::tcp::acceptor acceptor_;
    bool accepting_ = false;
    std::string xprv_header_;  // pre-built X-XPRV header value

    // Request stats
    std::atomic<uint64_t> total_requests_{0};
    std::atomic<uint64_t> total_proofs_{0};
    std::atomic<uint32_t> active_proofs_{0};
};

}  // namespace xprv
