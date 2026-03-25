#pragma once

// Minimal JSON-RPC client for XRPL/Xahau nodes.
//
// Uses boost::beast HTTP client over SSL.
// Two API layers:
//   - Callback-based (core): call(), server_definitions(), tx()
//   - Coroutine-based (co_ prefix): co_call(), co_server_definitions(), co_tx()
//
// Currently supports:
//   - server_definitions
//   - tx (lookup transaction by hash, get ledger_index)

#include <catl/core/logger.h>

#include <boost/asio/io_context.hpp>
#include <boost/json.hpp>
#include <cstdint>
#include <functional>
#include <string>

namespace catl::rpc {

namespace asio = boost::asio;

// ─── Exception hierarchy ─────────────────────────────────────────────

/// Base for all RPC errors. Carries the tx hash (if applicable) for
/// diagnostics. The HTTP server can catch these to return proper status
/// codes instead of generic 500.
class RpcException : public std::runtime_error
{
public:
    using std::runtime_error::runtime_error;
};

/// RPC endpoint returned an error or malformed response (retryable).
/// e.g. "no 'result' field", "server too busy", rate limiting.
class RpcTransientError : public RpcException
{
public:
    using RpcException::RpcException;
};

/// Transaction not found (permanent for this tx hash).
class RpcTxNotFound : public RpcException
{
public:
    using RpcException::RpcException;
};

/// Network/DNS/connection error reaching the RPC endpoint.
class RpcConnectionError : public RpcException
{
public:
    using RpcException::RpcException;
};

/// Result of an RPC call.
struct RpcResult
{
    boost::json::value result;  // the "result" field from the response
    bool success = false;       // true if status == "success"
    std::string error;          // error message if !success
};

using RpcCallback = std::function<void(RpcResult)>;

/// Minimal XRPL JSON-RPC client.
/// One instance per (host, port) pair. Connections are per-call (no pooling).
class RpcClient
{
public:
    RpcClient(asio::io_context& io, std::string host, uint16_t port);

    // ─── Callback API ────────────────────────────────────────────

    /// Raw RPC call — send method + params, get result via callback.
    void
    call(
        std::string const& method,
        boost::json::object params,
        RpcCallback callback);

    /// Fetch server_definitions.
    void
    server_definitions(RpcCallback callback);

    /// Look up a transaction by hash.
    void
    tx(std::string const& tx_hash, RpcCallback callback);

    // ─── Accessors ───────────────────────────────────────────────

    std::string const&
    host() const
    {
        return host_;
    }
    uint16_t
    port() const
    {
        return port_;
    }

private:
    asio::io_context& io_;
    std::string host_;
    uint16_t port_;

    static LogPartition log_;
};

}  // namespace catl::rpc
