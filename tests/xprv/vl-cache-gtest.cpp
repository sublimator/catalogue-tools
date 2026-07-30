#include "xprv/vl-cache.h"

#include "../test-support/deterministic-scheduler.h"
#include "../test-support/io-context-runners.h"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <utility>

namespace xprv {

class VlCacheTestAccess
{
public:
    struct Snapshot
    {
        bool running = false;
        bool stopping = false;
        bool stopped = false;
        bool fetch_in_progress = false;
        std::size_t refresh_wait_outstanding = 0;
        std::chrono::steady_clock::time_point refresh_expiry;
        catl::vl::VlFetchOperationPtr active_fetch;
    };

    static void
    set_fetcher(
        std::shared_ptr<VlCache> const& cache,
        std::function<void(catl::vl::VlCallback)> fetcher,
        std::function<bool()> cancel = nullptr)
    {
        cache->fetch_for_test_ = std::move(fetcher);
        cache->cancel_fetch_for_test_ = std::move(cancel);
    }

    static std::future<Snapshot>
    snapshot(std::shared_ptr<VlCache> const& cache)
    {
        auto result = std::make_shared<std::promise<Snapshot>>();
        auto future = result->get_future();
        boost::asio::post(cache->strand_, [cache, result] {
            result->set_value(Snapshot{
                .running = cache->phase_ == VlCache::Phase::running,
                .stopping = cache->phase_ == VlCache::Phase::stopping,
                .stopped = cache->phase_ == VlCache::Phase::stopped,
                .fetch_in_progress = cache->fetch_in_progress_,
                .refresh_wait_outstanding = cache->refresh_wait_outstanding_,
                .refresh_expiry = cache->refresh_.expiry(),
                .active_fetch = cache->active_fetch_,
            });
        });
        return future;
    }

    static void
    set_fetch_terminal_observer(
        std::shared_ptr<VlCache> const& cache,
        std::function<void(catl::vl::VlFetchOperationPtr const&)> observer)
    {
        cache->fetch_terminal_observer_for_test_ = std::move(observer);
    }

    static std::future<void>
    replace_active_fetch(
        std::shared_ptr<VlCache> const& cache,
        catl::vl::VlFetchOperationPtr replacement)
    {
        auto committed = std::make_shared<std::promise<void>>();
        auto future = committed->get_future();
        boost::asio::post(
            cache->strand_,
            [cache, replacement = std::move(replacement), committed]() mutable {
                cache->active_fetch_ = std::move(replacement);
                committed->set_value();
            });
        return future;
    }

    static void
    fail_next_operation_start(std::shared_ptr<VlCache> const& cache)
    {
        cache->fail_fetch_operation_starts_for_test_ = 1;
    }
};

namespace {

using namespace std::chrono_literals;

catl::vl::VlResult
successful_vl_result()
{
    catl::vl::VlResult result;
    result.success = true;
    result.vl.sequence = 1;
    result.vl.has_sequence = true;
    result.vl.effective = 0;
    result.vl.expiration = std::numeric_limits<std::uint32_t>::max();
    result.vl.has_expiration = true;
    result.vl.has_validators = true;
    return result;
}

TEST(VlClientCancellation, ExactOperationCancelsHeldTlsHandshakeOnce)
{
    boost::asio::io_context io;
    auto acceptor = std::make_shared<boost::asio::ip::tcp::acceptor>(
        io,
        boost::asio::ip::tcp::endpoint(
            boost::asio::ip::make_address("127.0.0.1"), 0));
    auto accepted = std::make_shared<boost::asio::ip::tcp::socket>(io);
    std::promise<boost::system::error_code> accept_terminal;
    auto accept_future = accept_terminal.get_future();
    acceptor->async_accept(
        *accepted, [&accept_terminal](boost::system::error_code ec) {
            accept_terminal.set_value(ec);
        });

    catl::vl::VlClient client(
        io, "127.0.0.1", acceptor->local_endpoint().port());
    std::promise<catl::vl::VlResult> fetch_terminal;
    auto fetch_future = fetch_terminal.get_future();
    auto operation = client.prepare_fetch();
    operation->start(
        [&fetch_terminal](
            catl::vl::VlFetchOperationPtr, catl::vl::VlResult result) {
            fetch_terminal.set_value(std::move(result));
        });

    catl::test::IoContextRunners runners(io, 2);
    ASSERT_EQ(accept_future.wait_for(5s), std::future_status::ready);
    ASSERT_FALSE(accept_future.get());
    ASSERT_EQ(fetch_future.wait_for(0ms), std::future_status::timeout)
        << "the server deliberately withholds the TLS handshake receipt";

    ASSERT_TRUE(operation->request_cancel());
    EXPECT_TRUE(operation->request_cancel())
        << "duplicate owner intent must remain idempotent";
    ASSERT_EQ(fetch_future.wait_for(5s), std::future_status::ready);
    auto result = fetch_future.get();
    EXPECT_FALSE(result.success);
    EXPECT_EQ(
        operation->cancellation_disposition(),
        catl::vl::VlFetchCancellationDisposition::emitted);

    boost::system::error_code ignored;
    accepted->close(ignored);
    acceptor->close(ignored);
    io.stop();
    runners.join();
}

TEST(VlCacheShutdown, PublicStopCancelsProductionFetchAndJoinsItsCallback)
{
    boost::asio::io_context io;
    auto acceptor = std::make_shared<boost::asio::ip::tcp::acceptor>(
        io,
        boost::asio::ip::tcp::endpoint(
            boost::asio::ip::make_address("127.0.0.1"), 0));
    auto accepted = std::make_shared<boost::asio::ip::tcp::socket>(io);
    std::promise<boost::system::error_code> accept_terminal;
    auto accept_future = accept_terminal.get_future();
    acceptor->async_accept(
        *accepted, [&accept_terminal](boost::system::error_code ec) {
            accept_terminal.set_value(ec);
        });

    auto cache =
        VlCache::create(io, "127.0.0.1", acceptor->local_endpoint().port());
    cache->start();

    catl::test::IoContextRunners runners(io, 2);
    ASSERT_EQ(accept_future.wait_for(5s), std::future_status::ready);
    ASSERT_FALSE(accept_future.get());

    auto first =
        boost::asio::co_spawn(io, cache->co_stop(), boost::asio::use_future);
    auto joined =
        boost::asio::co_spawn(io, cache->co_stop(), boost::asio::use_future);
    ASSERT_EQ(first.wait_for(5s), std::future_status::ready);
    ASSERT_EQ(joined.wait_for(5s), std::future_status::ready);
    EXPECT_NO_THROW(first.get());
    EXPECT_NO_THROW(joined.get());

    boost::system::error_code ignored;
    accepted->close(ignored);
    acceptor->close(ignored);
    io.stop();
    runners.join();
}

TEST(VlCacheShutdown, StalePhysicalTerminalCannotRetireReplacementOperation)
{
    boost::asio::io_context io;
    auto work = boost::asio::make_work_guard(io);
    auto acceptor = std::make_shared<boost::asio::ip::tcp::acceptor>(
        io,
        boost::asio::ip::tcp::endpoint(
            boost::asio::ip::make_address("127.0.0.1"), 0));
    auto accepted = std::make_shared<boost::asio::ip::tcp::socket>(io);
    std::promise<boost::system::error_code> accept_terminal;
    auto accept_future = accept_terminal.get_future();
    acceptor->async_accept(
        *accepted, [&accept_terminal](boost::system::error_code ec) {
            accept_terminal.set_value(ec);
        });

    auto cache =
        VlCache::create(io, "127.0.0.1", acceptor->local_endpoint().port());
    std::promise<catl::vl::VlFetchOperationPtr> terminal_observed;
    auto terminal_future = terminal_observed.get_future();
    VlCacheTestAccess::set_fetch_terminal_observer(
        cache,
        [&terminal_observed](catl::vl::VlFetchOperationPtr const& operation) {
            terminal_observed.set_value(operation);
        });
    cache->start();

    catl::test::IoContextRunners runners(io, 2);
    ASSERT_EQ(accept_future.wait_for(5s), std::future_status::ready);
    ASSERT_FALSE(accept_future.get());

    auto active_snapshot = VlCacheTestAccess::snapshot(cache);
    ASSERT_EQ(active_snapshot.wait_for(5s), std::future_status::ready);
    auto const incumbent = active_snapshot.get().active_fetch;
    ASSERT_TRUE(incumbent);

    catl::vl::VlClient replacement_factory(io, "unused.invalid");
    auto replacement = replacement_factory.prepare_fetch();
    auto replacement_committed =
        VlCacheTestAccess::replace_active_fetch(cache, replacement);
    ASSERT_EQ(replacement_committed.wait_for(5s), std::future_status::ready);
    replacement_committed.get();

    ASSERT_TRUE(incumbent->request_cancel());
    ASSERT_EQ(terminal_future.wait_for(5s), std::future_status::ready);
    EXPECT_EQ(terminal_future.get(), incumbent);

    auto terminal_snapshot = VlCacheTestAccess::snapshot(cache);
    ASSERT_EQ(terminal_snapshot.wait_for(5s), std::future_status::ready);
    auto const after_stale_terminal = terminal_snapshot.get();
    EXPECT_TRUE(after_stale_terminal.fetch_in_progress);
    EXPECT_EQ(after_stale_terminal.active_fetch, replacement)
        << "an exact terminal may retire only the operation incarnation it "
           "identifies";

    boost::system::error_code ignored;
    accepted->close(ignored);
    acceptor->close(ignored);
    work.reset();
    io.stop();
    runners.join();
}

TEST(VlCacheShutdown, InFlightFetchIsReceiptedAndJoinedStopsShareTerminal)
{
    catl::test::DeterministicScheduler scheduler;
    auto& io = scheduler.io();
    auto cache = VlCache::create(io, "unused.invalid");
    catl::vl::VlCallback pending;
    std::size_t fetches = 0;
    std::size_t cancellation_requests = 0;
    VlCacheTestAccess::set_fetcher(
        cache,
        [&pending, &fetches](catl::vl::VlCallback callback) {
            ++fetches;
            pending = std::move(callback);
        },
        [&cancellation_requests] {
            ++cancellation_requests;
            return true;
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
    EXPECT_EQ(cancellation_requests, 1u)
        << "the VlCache owner must actively request cancellation once";
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

TEST(VlCacheShutdown, PostInstallStartRefusalRetiresExactOperation)
{
    catl::test::DeterministicScheduler scheduler;
    auto& io = scheduler.io();
    auto cache = VlCache::create(io, "unused.invalid");
    VlCacheTestAccess::fail_next_operation_start(cache);

    cache->start();
    scheduler.run_until_idle();

    auto failed_snapshot = VlCacheTestAccess::snapshot(cache);
    scheduler.run_until_idle();
    auto const after_refusal = failed_snapshot.get();
    EXPECT_FALSE(after_refusal.fetch_in_progress);
    EXPECT_FALSE(after_refusal.active_fetch);
    EXPECT_EQ(after_refusal.refresh_wait_outstanding, 1u)
        << "the exact failed terminal may arm retry only after retiring the "
           "installed operation";

    auto stopped =
        boost::asio::co_spawn(io, cache->co_stop(), boost::asio::use_future);
    scheduler.run_until_idle();
    ASSERT_EQ(stopped.wait_for(0ms), std::future_status::ready);
    EXPECT_NO_THROW(stopped.get());
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

TEST(VlCacheShutdown, SuccessfulInitialLoadStopsWithoutStartingRefreshFetch)
{
    catl::test::DeterministicScheduler scheduler;
    auto& io = scheduler.io();
    auto cache = VlCache::create(io, "unused.invalid");
    catl::vl::VlCallback pending;
    std::size_t fetches = 0;
    std::size_t refreshes = 0;
    VlCacheTestAccess::set_fetcher(
        cache, [&pending, &fetches](catl::vl::VlCallback callback) {
            ++fetches;
            pending = std::move(callback);
        });
    cache->set_on_refresh(
        [&refreshes](catl::vl::ValidatorList const&) { ++refreshes; });

    cache->start();
    scheduler.run_until_idle();
    ASSERT_TRUE(pending);
    ASSERT_EQ(fetches, 1u);

    auto const before_success = std::chrono::steady_clock::now();
    pending(successful_vl_result());
    scheduler.run_until_idle();
    auto const after_success = std::chrono::steady_clock::now();

    auto running_snapshot = VlCacheTestAccess::snapshot(cache);
    scheduler.run_until_idle();
    auto const running = running_snapshot.get();
    EXPECT_TRUE(running.running);
    EXPECT_FALSE(running.fetch_in_progress);
    EXPECT_EQ(running.refresh_wait_outstanding, 1u);
    EXPECT_GE(running.refresh_expiry, before_success + 900s);
    EXPECT_LE(running.refresh_expiry, after_success + 900s);
    EXPECT_EQ(refreshes, 1u);
    EXPECT_EQ(fetches, 1u);

    auto stopped =
        boost::asio::co_spawn(io, cache->co_stop(), boost::asio::use_future);
    scheduler.run_until_idle();

    ASSERT_EQ(stopped.wait_for(0ms), std::future_status::ready);
    EXPECT_NO_THROW(stopped.get());
    auto stopped_snapshot = VlCacheTestAccess::snapshot(cache);
    scheduler.run_until_idle();
    auto const terminal = stopped_snapshot.get();
    EXPECT_TRUE(terminal.stopped);
    EXPECT_FALSE(terminal.fetch_in_progress);
    EXPECT_EQ(terminal.refresh_wait_outstanding, 0u);
    EXPECT_EQ(fetches, 1u)
        << "stop must consume the armed refresh handler without admitting "
           "the 900-second successor fetch";
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
