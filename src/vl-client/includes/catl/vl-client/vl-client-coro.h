#pragma once

#include "vl-client.h"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <stdexcept>

namespace catl::vl {

/// Cancellation-safe: signal + result are shared_ptr so they survive
/// coroutine frame destruction if cancellation fires while the VL
/// fetch callback is still in-flight.
inline asio::awaitable<ValidatorList>
co_fetch_vl(VlClient& client)
{
    auto executor = co_await asio::this_coro::executor;
    auto signal = std::make_shared<asio::steady_timer>(
        executor, asio::steady_timer::time_point::max());
    auto result = std::make_shared<VlResult>();

    client.fetch([signal, result](VlResult r) {
        *result = std::move(r);
        signal->cancel();
    });

    boost::system::error_code ec;
    co_await signal->async_wait(asio::redirect_error(asio::use_awaitable, ec));

    if (!result->success)
        throw std::runtime_error("VL fetch failed: " + result->error);

    co_return std::move(result->vl);
}

}  // namespace catl::vl
