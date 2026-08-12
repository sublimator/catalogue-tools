#include <catl/rpc-client/rpc-client.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

TEST(RpcConcurrencyGate, EnforcesBoundAcrossConcurrentRunners)
{
    boost::asio::io_context io;
    auto gate = std::make_shared<catl::rpc::detail::RpcConcurrencyGate>(
        io.get_executor(), 3);
    std::atomic<int> active{0};
    std::atomic<int> maximum{0};
    std::vector<std::future<void>> futures;
    futures.reserve(100);

    for (int i = 0; i < 100; ++i)
    {
        futures.push_back(boost::asio::co_spawn(
            io,
            [gate, &active, &maximum]() -> boost::asio::awaitable<void> {
                co_await gate->acquire();
                auto const now_active = active.fetch_add(1) + 1;
                auto observed = maximum.load();
                while (observed < now_active &&
                       !maximum.compare_exchange_weak(observed, now_active))
                {
                }

                auto executor = co_await boost::asio::this_coro::executor;
                boost::asio::steady_timer hold(executor, 1ms);
                co_await hold.async_wait(boost::asio::use_awaitable);
                active.fetch_sub(1);
                gate->release();
            },
            boost::asio::use_future));
    }

    auto work = boost::asio::make_work_guard(io);
    std::vector<std::thread> runners;
    for (int i = 0; i < 4; ++i)
        runners.emplace_back([&io]() { io.run(); });
    for (auto& future : futures)
        EXPECT_NO_THROW(future.get());
    work.reset();
    io.stop();
    for (auto& runner : runners)
        runner.join();

    EXPECT_EQ(active.load(), 0);
    EXPECT_EQ(maximum.load(), 3);
}

TEST(RpcConcurrencyGate, RejectsNonPositiveCapacity)
{
    boost::asio::io_context io;
    EXPECT_THROW(
        catl::rpc::detail::RpcConcurrencyGate(io.get_executor(), 0),
        std::invalid_argument);
}
