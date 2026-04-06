#include "xprv/validation-buffer.h"

#include <catl/xdata/protocol.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <gtest/gtest.h>

#include <chrono>
#include <string>

namespace asio = boost::asio;

namespace {

catl::xdata::Protocol
test_protocol()
{
    return catl::xdata::Protocol::load_embedded_xrpl_protocol();
}

}  // namespace

TEST(ValidationBuffer, TimedOutWaiterIsRemoved)
{
    asio::io_context io;
    auto buffer = xprv::ValidationBuffer::create(io, test_protocol(), 0);

    bool done = false;
    bool timed_out = false;
    std::string error;
    xprv::ValidationBuffer::Stats stats;

    asio::co_spawn(
        io,
        [&, buffer]() -> asio::awaitable<void> {
            bool local_timed_out = false;
            std::string local_error;
            try
            {
                (void)co_await buffer->co_wait_quorum(std::chrono::seconds(0));
            }
            catch (std::exception const& e)
            {
                local_timed_out = true;
                local_error = std::string(e.what());
            }

            stats = co_await buffer->co_stats();
            timed_out = local_timed_out;
            error = std::move(local_error);
            done = true;
        },
        asio::detached);

    io.run();

    EXPECT_TRUE(done);
    EXPECT_TRUE(timed_out);
    EXPECT_NE(error.find("Timed out waiting for validation quorum"), std::string::npos);
    EXPECT_EQ(stats.waiters, 0u);
}

TEST(ValidationBuffer, CancelledWaiterIsRemoved)
{
    asio::io_context io;
    auto buffer = xprv::ValidationBuffer::create(io, test_protocol(), 0);

    bool done = false;
    bool cancel_branch_won = false;
    xprv::ValidationBuffer::Stats stats;

    asio::co_spawn(
        io,
        [&, buffer]() -> asio::awaitable<void> {
            auto ex = co_await asio::this_coro::executor;
            auto result =
                co_await asio::experimental::make_parallel_group(
                    asio::co_spawn(
                        ex,
                        buffer->co_wait_quorum(std::chrono::seconds(30)),
                        asio::deferred),
                    asio::co_spawn(
                        ex,
                        []() -> asio::awaitable<void> {
                            auto ex = co_await asio::this_coro::executor;
                            asio::steady_timer timer(ex, std::chrono::milliseconds(10));
                            boost::system::error_code ec;
                            co_await timer.async_wait(
                                asio::redirect_error(asio::use_awaitable, ec));
                        },
                        asio::deferred))
                    .async_wait(
                        asio::experimental::wait_for_one(),
                        asio::use_awaitable);

            cancel_branch_won = (std::get<0>(result)[0] == 1);

            // Give the canceled waiter branch one turn to run its cleanup.
            asio::steady_timer drain(ex, std::chrono::milliseconds(1));
            boost::system::error_code drain_ec;
            co_await drain.async_wait(
                asio::redirect_error(asio::use_awaitable, drain_ec));

            stats = co_await buffer->co_stats();
            done = true;
        },
        asio::detached);

    io.run();

    EXPECT_TRUE(done);
    EXPECT_TRUE(cancel_branch_won);
    EXPECT_EQ(stats.waiters, 0u);
}
