#pragma once

// Coroutine layer for RpcClient.
// co_ prefixed free functions wrapping the callback API.

#include "rpc-client.h"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <stdexcept>

namespace catl::rpc {

namespace detail {

inline asio::awaitable<RpcResult>
callback_to_awaitable(std::function<void(RpcCallback)> initiator)
{
    auto executor = co_await asio::this_coro::executor;
    asio::steady_timer signal(executor, asio::steady_timer::time_point::max());

    RpcResult rpc_result;

    initiator([&signal, &rpc_result](RpcResult r) {
        rpc_result = std::move(r);
        signal.cancel();
    });

    boost::system::error_code ec;
    co_await signal.async_wait(asio::redirect_error(asio::use_awaitable, ec));

    co_return std::move(rpc_result);
}

}  // namespace detail

inline asio::awaitable<RpcResult>
co_call(
    RpcClient& client,
    std::string const& method,
    boost::json::object params = {})
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
        throw std::runtime_error("server_definitions failed: " + result.error);
    co_return std::move(result.result);
}

inline asio::awaitable<boost::json::value>
co_tx(RpcClient& client, std::string const& tx_hash)
{
    auto result = co_await detail::callback_to_awaitable(
        [&](RpcCallback cb) { client.tx(tx_hash, std::move(cb)); });
    if (!result.success)
        throw std::runtime_error("tx lookup failed: " + result.error);
    co_return std::move(result.result);
}

}  // namespace catl::rpc
