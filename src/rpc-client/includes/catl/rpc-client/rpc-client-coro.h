#pragma once

// Coroutine layer for RpcClient.
// co_ prefixed free functions wrapping the callback API.

#include "rpc-client.h"

#include <boost/asio/awaitable.hpp>
#include <stdexcept>

namespace catl::rpc {

namespace detail {

using RpcPublicationHook = std::function<void()>;

asio::awaitable<RpcResult>
callback_to_awaitable(
    std::function<void(RpcCallback)> initiator,
    RpcPublicationHook before_terminal_commit = {});

}  // namespace detail

inline asio::awaitable<RpcResult>
co_call(RpcClient& client, std::string method, boost::json::object params = {})
{
    co_return co_await detail::callback_to_awaitable(
        [&](RpcCallback cb) { client.call(method, params, std::move(cb)); });
}

inline asio::awaitable<boost::json::value>
co_server_definitions(RpcClient& client)
{
    auto result = co_await detail::callback_to_awaitable(
        [&](RpcCallback cb) { client.server_definitions(std::move(cb)); });
    if (!result.success)
        throw RpcException("server_definitions: " + result.error);
    co_return std::move(result.result);
}

inline asio::awaitable<boost::json::value>
co_tx(RpcClient& client, std::string tx_hash)
{
    auto result = co_await detail::callback_to_awaitable(
        [&](RpcCallback cb) { client.tx(tx_hash, std::move(cb)); });
    if (!result.success)
    {
        auto msg = "tx " + tx_hash.substr(0, 16) + "...: " + result.error;
        // Distinguish connection errors from RPC-level errors
        if (result.error.find("Host not found") != std::string::npos ||
            result.error.find("Connection refused") != std::string::npos ||
            result.error.find("connect") != std::string::npos)
        {
            throw RpcConnectionError(msg);
        }
        // "no 'result' field" or "server too busy" are transient
        if (result.error.find("no 'result'") != std::string::npos ||
            result.error.find("too busy") != std::string::npos)
        {
            throw RpcTransientError(msg);
        }
        // "txnNotFound" is permanent
        if (result.error.find("txnNotFound") != std::string::npos)
        {
            throw RpcTxNotFound(msg);
        }
        throw RpcException(msg);
    }
    co_return std::move(result.result);
}

}  // namespace catl::rpc
