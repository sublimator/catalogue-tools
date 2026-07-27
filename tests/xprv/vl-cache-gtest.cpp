#include "xprv/vl-cache.h"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <utility>

namespace xprv {

class VlCacheTestAccess
{
public:
    static void
    set_fetcher(
        std::shared_ptr<VlCache> const& cache,
        std::function<void(catl::vl::VlCallback)> fetcher)
    {
        cache->fetch_for_test_ = std::move(fetcher);
    }
};

namespace {

using namespace std::chrono_literals;

TEST(VlCacheShutdown, InFlightFetchIsReceiptedAndJoinedStopsShareTerminal)
{
    boost::asio::io_context io;
    auto cache = VlCache::create(io, "unused.invalid");
    catl::vl::VlCallback pending;
    std::size_t fetches = 0;
    VlCacheTestAccess::set_fetcher(
        cache,
        [&pending, &fetches](catl::vl::VlCallback callback) {
            ++fetches;
            pending = std::move(callback);
        });

    cache->start();
    io.run();
    ASSERT_TRUE(pending);
    ASSERT_EQ(fetches, 1u);

    io.restart();
    auto first = boost::asio::co_spawn(
        io, cache->co_stop(), boost::asio::use_future);
    auto joined = boost::asio::co_spawn(
        io, cache->co_stop(), boost::asio::use_future);
    io.poll();
    EXPECT_EQ(first.wait_for(0ms), std::future_status::timeout);
    EXPECT_EQ(joined.wait_for(0ms), std::future_status::timeout);

    catl::vl::VlResult failed;
    failed.error = "controlled terminal";
    pending(std::move(failed));
    io.restart();
    io.run();

    EXPECT_NO_THROW(first.get());
    EXPECT_NO_THROW(joined.get());

    cache->start();
    io.restart();
    io.poll();
    EXPECT_EQ(fetches, 1u) << "stopped lifecycle must refuse re-admission";

    io.restart();
    auto later = boost::asio::co_spawn(
        io, cache->co_stop(), boost::asio::use_future);
    io.run();
    EXPECT_NO_THROW(later.get());
}

TEST(VlCacheShutdown, StopReceiptsCanceledRetryHandler)
{
    boost::asio::io_context io;
    auto cache = VlCache::create(io, "unused.invalid");
    catl::vl::VlCallback pending;
    VlCacheTestAccess::set_fetcher(
        cache,
        [&pending](catl::vl::VlCallback callback) {
            pending = std::move(callback);
        });

    cache->start();
    io.run();
    ASSERT_TRUE(pending);

    catl::vl::VlResult failed;
    failed.error = "arm retry";
    pending(std::move(failed));
    io.restart();
    ASSERT_EQ(io.run_one(), 1u) << "consume result and arm retry timer";

    io.restart();
    auto stopped = boost::asio::co_spawn(
        io, cache->co_stop(), boost::asio::use_future);
    ASSERT_EQ(io.run_one(), 1u) << "commit stop and cancel retry timer";
    EXPECT_EQ(stopped.wait_for(0ms), std::future_status::timeout)
        << "timer cancellation is a request, not its handler receipt";

    io.restart();
    io.run();
    EXPECT_NO_THROW(stopped.get());
}

TEST(VlCacheShutdown, OrdinaryWaiterTerminatesWhenStopIsRequested)
{
    boost::asio::io_context io;
    auto cache = VlCache::create(io, "unused.invalid");
    catl::vl::VlCallback pending;
    VlCacheTestAccess::set_fetcher(
        cache,
        [&pending](catl::vl::VlCallback callback) {
            pending = std::move(callback);
        });

    cache->start();
    io.run();
    ASSERT_TRUE(pending);

    io.restart();
    auto waiting =
        boost::asio::co_spawn(io, cache->co_get(), boost::asio::use_future);
    auto stopped = boost::asio::co_spawn(
        io, cache->co_stop(), boost::asio::use_future);
    io.poll();
    ASSERT_EQ(waiting.wait_for(0ms), std::future_status::ready);
    EXPECT_THROW(waiting.get(), boost::system::system_error);
    EXPECT_EQ(stopped.wait_for(0ms), std::future_status::timeout);

    catl::vl::VlResult failed;
    failed.error = "controlled terminal";
    pending(std::move(failed));
    io.restart();
    io.run();

    EXPECT_NO_THROW(stopped.get());
}

}  // namespace
}  // namespace xprv
