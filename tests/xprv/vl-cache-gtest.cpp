#include "xprv/vl-cache.h"

#include "../test-support/deterministic-scheduler.h"

#include <boost/asio/co_spawn.hpp>
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
    catl::test::DeterministicScheduler scheduler;
    auto& io = scheduler.io();
    auto cache = VlCache::create(io, "unused.invalid");
    catl::vl::VlCallback pending;
    std::size_t fetches = 0;
    VlCacheTestAccess::set_fetcher(
        cache, [&pending, &fetches](catl::vl::VlCallback callback) {
            ++fetches;
            pending = std::move(callback);
        });

    cache->start();
    scheduler.run_until_idle();
    ASSERT_TRUE(pending);
    ASSERT_EQ(fetches, 1u);

    auto first =
        boost::asio::co_spawn(io, cache->co_stop(), boost::asio::use_future);
    auto joined =
        boost::asio::co_spawn(io, cache->co_stop(), boost::asio::use_future);
    scheduler.run_until_idle();
    EXPECT_EQ(first.wait_for(0ms), std::future_status::timeout);
    EXPECT_EQ(joined.wait_for(0ms), std::future_status::timeout);

    catl::vl::VlResult failed;
    failed.error = "controlled terminal";
    pending(std::move(failed));
    scheduler.run_until_idle();

    EXPECT_NO_THROW(first.get());
    EXPECT_NO_THROW(joined.get());

    cache->start();
    scheduler.run_until_idle();
    EXPECT_EQ(fetches, 1u) << "stopped lifecycle must refuse re-admission";

    auto later =
        boost::asio::co_spawn(io, cache->co_stop(), boost::asio::use_future);
    scheduler.run_until_idle();
    EXPECT_NO_THROW(later.get());
}

TEST(VlCacheShutdown, StopReceiptsCanceledRetryHandler)
{
    catl::test::DeterministicScheduler scheduler;
    auto& io = scheduler.io();
    auto cache = VlCache::create(io, "unused.invalid");
    catl::vl::VlCallback pending;
    VlCacheTestAccess::set_fetcher(
        cache, [&pending](catl::vl::VlCallback callback) {
            pending = std::move(callback);
        });

    cache->start();
    scheduler.run_until_idle();
    ASSERT_TRUE(pending);

    catl::vl::VlResult failed;
    failed.error = "arm retry";
    pending(std::move(failed));
    ASSERT_TRUE(scheduler.step()) << "consume result and arm retry timer";

    auto stopped =
        boost::asio::co_spawn(io, cache->co_stop(), boost::asio::use_future);
    ASSERT_TRUE(scheduler.step()) << "commit stop and cancel retry timer";
    EXPECT_EQ(stopped.wait_for(0ms), std::future_status::timeout)
        << "timer cancellation is a request, not its handler receipt";

    scheduler.run_until_idle();
    EXPECT_NO_THROW(stopped.get());
}

TEST(VlCacheShutdown, OrdinaryWaiterTerminatesWhenStopIsRequested)
{
    catl::test::DeterministicScheduler scheduler;
    auto& io = scheduler.io();
    auto cache = VlCache::create(io, "unused.invalid");
    catl::vl::VlCallback pending;
    VlCacheTestAccess::set_fetcher(
        cache, [&pending](catl::vl::VlCallback callback) {
            pending = std::move(callback);
        });

    cache->start();
    scheduler.run_until_idle();
    ASSERT_TRUE(pending);

    auto waiting =
        boost::asio::co_spawn(io, cache->co_get(), boost::asio::use_future);
    auto stopped =
        boost::asio::co_spawn(io, cache->co_stop(), boost::asio::use_future);
    scheduler.run_until_idle();
    ASSERT_EQ(waiting.wait_for(0ms), std::future_status::ready);
    EXPECT_THROW(waiting.get(), boost::system::system_error);
    EXPECT_EQ(stopped.wait_for(0ms), std::future_status::timeout);

    catl::vl::VlResult failed;
    failed.error = "controlled terminal";
    pending(std::move(failed));
    scheduler.run_until_idle();

    EXPECT_NO_THROW(stopped.get());
}

}  // namespace
}  // namespace xprv
