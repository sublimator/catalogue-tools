#include "xprv/vl-cache.h"

#include "../test-support/deterministic-scheduler.h"
#include "../test-support/io-context-runners.h"

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/cancellation_type.hpp>
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
#include <new>
#include <ostream>
#include <streambuf>
#include <utility>

namespace catl::vl {

class VlFetchOperationTestAccess
{
public:
    static void
    set_after_start_admission(
        VlFetchOperationPtr const& operation,
        std::function<void()> hook)
    {
        operation->set_after_start_admission_for_test(std::move(hook));
    }
};

}  // namespace catl::vl

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
        std::shared_ptr<writ::retained_signal> fetch_wait_signal;
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
                .fetch_wait_signal = cache->fetch_wait_signal_,
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

    static void
    set_initiation_failure_diagnostic(
        std::shared_ptr<VlCache> const& cache,
        std::function<void()> hook)
    {
        cache->initiation_failure_diagnostic_for_test_ = std::move(hook);
    }

    static void
    observe_stop_entry_executor(
        std::shared_ptr<VlCache> const& cache,
        std::shared_ptr<std::promise<bool>> result)
    {
        cache->stop_entry_observer_for_test_ = [strand = cache->strand_,
                                                result = std::move(result)] {
            result->set_value(strand.running_in_this_thread());
        };
    }

    static void
    observe_public_entry_executor(
        std::shared_ptr<VlCache> const& cache,
        std::shared_ptr<std::promise<bool>> result)
    {
        cache->public_entry_observer_for_test_ = [strand = cache->strand_,
                                                  result = std::move(result)] {
            result->set_value(strand.running_in_this_thread());
        };
    }

    static void
    observe_public_resume_executor(
        std::shared_ptr<VlCache> const& cache,
        std::shared_ptr<std::promise<bool>> result)
    {
        cache->public_resume_observer_for_test_ = [strand = cache->strand_,
                                                   result = std::move(result)] {
            result->set_value(strand.running_in_this_thread());
        };
    }

    /// Reads the armed-timer obligation count with NO executor hop. Legal
    /// only from code already running on the cache strand. snapshot() cannot
    /// answer this question: a cancelled steady_timer handler is posted, so
    /// it is always queued AHEAD of a posted snapshot and has already
    /// decremented the count by the time that snapshot runs.
    static std::size_t
    refresh_wait_outstanding_on_strand(std::shared_ptr<VlCache> const& cache)
    {
        return cache->refresh_wait_outstanding_;
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

class ThrowingLogBuffer final : public std::streambuf
{
    int_type
    overflow(int_type) override
    {
        throw std::bad_alloc{};
    }

    std::streamsize
    xsputn(char const*, std::streamsize) override
    {
        throw std::bad_alloc{};
    }
};

class ScopedThrowingInfoLog final
{
public:
    ScopedThrowingInfoLog()
    {
        stream_.exceptions(std::ios::badbit | std::ios::failbit);
        Logger::set_output_stream(&stream_);
    }

    ~ScopedThrowingInfoLog()
    {
        Logger::set_output_stream(nullptr);
    }

private:
    ThrowingLogBuffer buffer_;
    std::ostream stream_{&buffer_};
    ScopedLogPartitionLevel partition_{"vl-cache", LogLevel::INFO};
};

TEST(VlClientCancellation, CancelBeforeStartIsRetainedAndEmittedOnce)
{
    boost::asio::io_context io;
    auto acceptor = std::make_shared<boost::asio::ip::tcp::acceptor>(
        io,
        boost::asio::ip::tcp::endpoint(
            boost::asio::ip::make_address("127.0.0.1"), 0));

    catl::vl::VlClient client(
        io, "127.0.0.1", acceptor->local_endpoint().port());
    auto operation = client.prepare_fetch();
    ASSERT_TRUE(operation->request_cancel());
    EXPECT_TRUE(operation->request_cancel())
        << "duplicate retained intent must remain idempotent before start";
    EXPECT_EQ(
        operation->cancellation_disposition(),
        catl::vl::VlFetchCancellationDisposition::submitted);

    std::promise<catl::vl::VlResult> fetch_terminal;
    auto fetch_future = fetch_terminal.get_future();
    operation->start(
        [&fetch_terminal](
            catl::vl::VlFetchOperationPtr, catl::vl::VlResult result) {
            fetch_terminal.set_value(std::move(result));
        });

    catl::test::IoContextRunners runners(io, 2);
    ASSERT_EQ(fetch_future.wait_for(5s), std::future_status::ready);
    EXPECT_FALSE(fetch_future.get().success);
    EXPECT_EQ(
        operation->cancellation_disposition(),
        catl::vl::VlFetchCancellationDisposition::emitted);
    EXPECT_FALSE(operation->request_cancel())
        << "terminality must close cancellation authority";

    boost::system::error_code ignored;
    acceptor->close(ignored);
    io.stop();
    runners.join();
}

TEST(VlClientCancellation, TerminalWithoutRequestClosesCancellationAuthority)
{
    boost::asio::io_context io;
    boost::asio::ip::tcp::acceptor acceptor(
        io,
        boost::asio::ip::tcp::endpoint(
            boost::asio::ip::make_address("127.0.0.1"), 0));
    auto const unused_port = acceptor.local_endpoint().port();
    boost::system::error_code ignored;
    acceptor.close(ignored);

    catl::vl::VlClient client(io, "127.0.0.1", unused_port);
    auto operation = client.prepare_fetch();
    std::promise<catl::vl::VlResult> fetch_terminal;
    auto fetch_future = fetch_terminal.get_future();
    operation->start(
        [&fetch_terminal](
            catl::vl::VlFetchOperationPtr, catl::vl::VlResult result) {
            fetch_terminal.set_value(std::move(result));
        });

    catl::test::IoContextRunners runners(io, 2);
    ASSERT_EQ(fetch_future.wait_for(5s), std::future_status::ready);
    EXPECT_FALSE(fetch_future.get().success);
    EXPECT_EQ(
        operation->cancellation_disposition(),
        catl::vl::VlFetchCancellationDisposition::closed_by_terminal);
    EXPECT_FALSE(operation->request_cancel());

    io.stop();
    runners.join();
}

TEST(VlClientCancellation, StartAdmissionRefusalClosesCancellationAuthority)
{
    for (bool const cancel_before_start : {false, true})
    {
        SCOPED_TRACE(
            cancel_before_start ? "cancel-before-start" : "no-cancel-request");
        boost::asio::io_context io;
        catl::vl::VlClient client(io, "unused.invalid");
        auto operation = client.prepare_fetch();
        if (cancel_before_start)
            ASSERT_TRUE(operation->request_cancel());

        catl::vl::VlFetchOperationTestAccess::set_after_start_admission(
            operation, [] { throw std::bad_alloc{}; });

        std::size_t callbacks = 0;
        EXPECT_THROW(
            operation->start([&callbacks](
                                 catl::vl::VlFetchOperationPtr,
                                 catl::vl::VlResult) { ++callbacks; }),
            std::bad_alloc);
        EXPECT_EQ(
            operation->cancellation_disposition(),
            catl::vl::VlFetchCancellationDisposition::closed_by_terminal);
        EXPECT_FALSE(operation->request_cancel());
        EXPECT_THROW(
            operation->start(
                [](catl::vl::VlFetchOperationPtr, catl::vl::VlResult) {}),
            std::logic_error);
        io.poll();
        EXPECT_EQ(callbacks, 0u)
            << "a synchronous start refusal must not counterfeit a physical "
               "terminal callback";
    }
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

TEST(VlCacheShutdown, DiagnosticFailureBeforeAdmissionCannotOrphanFetch)
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

    {
        ScopedThrowingInfoLog throwing_log;
        cache->start();
        ASSERT_TRUE(scheduler.step());
    }

    ASSERT_EQ(fetches, 1u)
        << "fallible diagnostics must not sit after admission and before "
           "launch";
    ASSERT_TRUE(pending);

    auto active_snapshot = VlCacheTestAccess::snapshot(cache);
    scheduler.run_until_idle();
    EXPECT_TRUE(active_snapshot.get().fetch_in_progress);

    pending(successful_vl_result());
    scheduler.run_until_idle();

    auto terminal_snapshot = VlCacheTestAccess::snapshot(cache);
    scheduler.run_until_idle();
    auto const after_terminal = terminal_snapshot.get();
    EXPECT_FALSE(after_terminal.fetch_in_progress);
    EXPECT_FALSE(after_terminal.active_fetch);

    auto stopped =
        boost::asio::co_spawn(io, cache->co_stop(), boost::asio::use_future);
    scheduler.run_until_idle();
    ASSERT_EQ(stopped.wait_for(0ms), std::future_status::ready);
    EXPECT_NO_THROW(stopped.get());
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

TEST(VlCacheShutdown, StopCheckAndWaitInstallationRunOnOwnerStrand)
{
    boost::asio::io_context io;
    auto control = boost::asio::make_strand(io);
    auto cache = VlCache::create(io, "unused.invalid");
    auto observed = std::make_shared<std::promise<bool>>();
    auto observed_future = observed->get_future();
    VlCacheTestAccess::observe_stop_entry_executor(cache, observed);

    auto stopped = boost::asio::co_spawn(
        control, cache->co_stop(), boost::asio::use_future);
    catl::test::IoContextRunners runners(io, 4);

    ASSERT_EQ(observed_future.wait_for(5s), std::future_status::ready);
    EXPECT_TRUE(observed_future.get())
        << "the stopped-state check and progress-wait installation must be "
           "serialized with refresh terminal publication";
    ASSERT_EQ(stopped.wait_for(5s), std::future_status::ready);
    EXPECT_NO_THROW(stopped.get());

    io.stop();
    runners.join();
}

TEST(VlCacheExecution, PublicAwaitableEstablishesOwnerStrandBirth)
{
    boost::asio::io_context io;
    auto cache = VlCache::create(io, "unused.invalid");
    auto observed = std::make_shared<std::promise<bool>>();
    auto observation = observed->get_future();
    VlCacheTestAccess::observe_public_entry_executor(cache, observed);

    auto foreign = boost::asio::make_strand(io);
    auto loaded = boost::asio::co_spawn(
        foreign, cache->co_is_loaded(), boost::asio::use_future);
    auto work = boost::asio::make_work_guard(io);
    std::thread first([&io]() { io.run(); });
    std::thread second([&io]() { io.run(); });

    EXPECT_FALSE(loaded.get());
    EXPECT_TRUE(observation.get());
    work.reset();
    io.stop();
    first.join();
    second.join();
}

TEST(VlCacheExecution, SuspendedPublicAwaitableResumesOnOwnerStrand)
{
    boost::asio::io_context io;
    auto cache = VlCache::create(io, "unused.invalid");
    auto fetch_admitted = std::make_shared<
        std::promise<catl::vl::VlCallback>>();
    auto fetch = fetch_admitted->get_future();
    VlCacheTestAccess::set_fetcher(
        cache,
        [fetch_admitted](catl::vl::VlCallback callback) {
            fetch_admitted->set_value(std::move(callback));
        });
    auto resumed = std::make_shared<std::promise<bool>>();
    auto resume_observation = resumed->get_future();
    VlCacheTestAccess::observe_public_resume_executor(cache, resumed);

    auto foreign = boost::asio::make_strand(io);
    auto loaded = boost::asio::co_spawn(
        foreign, cache->co_get(), boost::asio::use_future);
    auto work = boost::asio::make_work_guard(io);
    std::thread first([&io]() { io.run(); });
    std::thread second([&io]() { io.run(); });

    ASSERT_EQ(fetch.wait_for(5s), std::future_status::ready);
    EXPECT_EQ(loaded.wait_for(0ms), std::future_status::timeout);
    fetch.get()(successful_vl_result());
    ASSERT_EQ(loaded.wait_for(5s), std::future_status::ready);
    EXPECT_EQ(loaded.get().sequence, 1u);
    ASSERT_EQ(resume_observation.wait_for(5s), std::future_status::ready);
    EXPECT_TRUE(resume_observation.get());

    auto stopped = boost::asio::co_spawn(
        foreign, cache->co_stop(), boost::asio::use_future);
    ASSERT_EQ(stopped.wait_for(5s), std::future_status::ready);
    EXPECT_NO_THROW(stopped.get());
    work.reset();
    io.stop();
    first.join();
    second.join();
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

TEST(VlCacheShutdown, InitiationDiagnosticRefusalCannotOrphanFetch)
{
    catl::test::DeterministicScheduler scheduler;
    auto& io = scheduler.io();
    auto cache = VlCache::create(io, "unused.invalid");
    std::size_t diagnostic_attempts = 0;
    VlCacheTestAccess::fail_next_operation_start(cache);
    VlCacheTestAccess::set_initiation_failure_diagnostic(
        cache, [&diagnostic_attempts] {
            ++diagnostic_attempts;
            throw std::bad_alloc{};
        });

    cache->start();
    scheduler.run_until_idle();

    EXPECT_EQ(diagnostic_attempts, 1u);
    auto failed_snapshot = VlCacheTestAccess::snapshot(cache);
    scheduler.run_until_idle();
    auto const after_refusal = failed_snapshot.get();
    EXPECT_FALSE(after_refusal.fetch_in_progress);
    EXPECT_FALSE(after_refusal.active_fetch);
    EXPECT_EQ(after_refusal.refresh_wait_outstanding, 1u)
        << "diagnostic refusal cannot prevent exact rollback and retry";

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

// The tests below back the corrected VlCache retained-signal role
// claims (writ.core/retained-signal/v1, motif anchored at prv.S0140) with
// runtime behavior the demand graph does not itself claim. Green here is
// behavioral evidence for the named roles, NOT schema conformance.

// writ-claim: retained_signal.publish, witness prv.T0333 (wake on
// advance): an ARMED waiter (registered before the terminal) completes
// when the revision advances. The late co_get half deliberately claims NO
// retained-signal role: it returns through co_get_on_strand's immediate
// active-cached-list path BEFORE revision sampling or registration, so it
// witnesses the cache-hit short-circuit only (Codex blocker 2026-08-14:
// the earlier version of this comment falsely claimed
// retained_fact/retain here). The true retained-completion race control
// is SynchronousAdmissionCompletion below.
TEST(VlCacheRetainedSignal, CompletionWakesArmedWaiterAndLateGetShortCircuits)
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

    auto early =
        boost::asio::co_spawn(io, cache->co_get(), boost::asio::use_future);
    scheduler.run_until_idle();
    ASSERT_EQ(early.wait_for(0ms), std::future_status::timeout);

    pending(successful_vl_result());
    scheduler.run_until_idle();
    ASSERT_EQ(early.wait_for(0ms), std::future_status::ready);
    EXPECT_EQ(early.get().sequence, 1u);

    auto late =
        boost::asio::co_spawn(io, cache->co_get(), boost::asio::use_future);
    scheduler.run_until_idle();
    ASSERT_EQ(late.wait_for(0ms), std::future_status::ready)
        << "a co_get after completion returns via the active cached-list "
           "path without registering";
    EXPECT_EQ(late.get().sequence, 1u);
    EXPECT_EQ(fetches, 1u) << "the cache hit must not admit a second fetch";
}

// writ-claim: retained_signal.retained-completion, witnessed by the
// application wait at prv.T0334 and the library's closed rule.
// A fetch may complete synchronously during co_get admission, before its
// packaged wait registers. The completion must remain observable as state;
// timer cancellation alone cannot carry this wake.
TEST(
    VlCacheRetainedSignal,
    SynchronousAdmissionCompletionCannotLoseRetainedWake)
{
    catl::test::DeterministicScheduler scheduler;
    auto& io = scheduler.io();
    auto cache = VlCache::create(io, "unused.invalid");
    std::size_t fetches = 0;
    VlCacheTestAccess::set_fetcher(
        cache, [&fetches](catl::vl::VlCallback callback) {
            ++fetches;
            callback(successful_vl_result());  // synchronous terminal
        });

    // No start(): co_get itself admits the fetch, which completes inline.
    auto waiter =
        boost::asio::co_spawn(io, cache->co_get(), boost::asio::use_future);
    scheduler.run_until_idle();
    ASSERT_EQ(fetches, 1u);

    ASSERT_EQ(waiter.wait_for(0ms), std::future_status::ready)
        << "a synchronous terminal must be consumed through the retained "
           "signal rather than lost before waiter registration";
    EXPECT_EQ(waiter.get().sequence, 1u);
}

// writ-claim: retained_signal.cancellation-dominates-shared-wake and
// subscriber isolation, consumed through the application outcome mapping.
// One waiter's terminal cancellation must consume ONLY its own wait: the
// sibling keeps its registration and receives the publication.
TEST(VlCacheRetainedSignal, WaiterCancellationCannotConsumeWakeOrDisturbSibling)
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

    boost::asio::cancellation_signal cancel_a;
    auto waiter_a = boost::asio::co_spawn(
        io,
        cache->co_get(),
        boost::asio::bind_cancellation_slot(
            cancel_a.slot(), boost::asio::use_future));
    auto waiter_b =
        boost::asio::co_spawn(io, cache->co_get(), boost::asio::use_future);
    scheduler.run_until_idle();
    ASSERT_EQ(waiter_a.wait_for(0ms), std::future_status::timeout);
    ASSERT_EQ(waiter_b.wait_for(0ms), std::future_status::timeout);

    cancel_a.emit(boost::asio::cancellation_type::terminal);
    scheduler.run_until_idle();
    ASSERT_EQ(waiter_a.wait_for(0ms), std::future_status::ready);
    EXPECT_THROW(waiter_a.get(), boost::system::system_error);
    ASSERT_EQ(waiter_b.wait_for(0ms), std::future_status::timeout)
        << "cancelling one waiter must not consume the sibling's wait";

    pending(successful_vl_result());
    scheduler.run_until_idle();
    ASSERT_EQ(waiter_b.wait_for(0ms), std::future_status::ready);
    EXPECT_EQ(waiter_b.get().sequence, 1u)
        << "the sibling must still receive the retained publication";
}

// writ-claim: retained_signal.generations-rotate, witness prv.T0336. The
// already-retained completion from the previous fetch must not satisfy the
// refresh subscriber; a new fetch admission mints a fresh signal generation.
TEST(VlCacheRetainedSignal, RefreshWaiterUsesFreshSignalGeneration)
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
    pending(successful_vl_result());
    scheduler.run_until_idle();
    ASSERT_EQ(fetches, 1u);

    auto refresh =
        boost::asio::co_spawn(io, cache->co_refresh(), boost::asio::use_future);
    scheduler.run_until_idle();
    ASSERT_EQ(fetches, 2u) << "co_refresh must admit a new fetch";
    ASSERT_EQ(refresh.wait_for(0ms), std::future_status::timeout)
        << "the refresh subscriber must not be satisfied by the retained "
           "completion of the first fetch";

    auto second = successful_vl_result();
    second.vl.sequence = 2;
    pending(std::move(second));
    scheduler.run_until_idle();
    ASSERT_EQ(refresh.wait_for(0ms), std::future_status::ready);
    EXPECT_EQ(refresh.get().sequence, 2u)
        << "the refresh subscriber completes only on the next revision "
           "advance, with the new list";
}

// writ-claim: retained_signal.retained-completion, witness prv.T0336. A
// synchronous refresh terminal can arrive before the packaged wait registers;
// it must still consume the retained completion without suspending.
TEST(
    VlCacheRetainedSignal,
    SynchronousRefreshCompletionCannotLoseRetainedWake)
{
    catl::test::DeterministicScheduler scheduler;
    auto& io = scheduler.io();
    auto cache = VlCache::create(io, "unused.invalid");
    catl::vl::VlCallback first;
    std::size_t fetches = 0;
    VlCacheTestAccess::set_fetcher(
        cache, [&first, &fetches](catl::vl::VlCallback callback) {
            ++fetches;
            if (fetches == 1)
            {
                first = std::move(callback);
                return;
            }
            auto refreshed = successful_vl_result();
            refreshed.vl.sequence = 2;
            callback(std::move(refreshed));
        });

    cache->start();
    scheduler.run_until_idle();
    ASSERT_TRUE(first);
    first(successful_vl_result());
    scheduler.run_until_idle();

    auto refresh =
        boost::asio::co_spawn(io, cache->co_refresh(), boost::asio::use_future);
    scheduler.run_until_idle();
    ASSERT_EQ(fetches, 2u);
    ASSERT_EQ(refresh.wait_for(0ms), std::future_status::ready)
        << "a synchronous refresh terminal must be consumed through the "
           "retained revision";
    EXPECT_EQ(refresh.get().sequence, 2u);
}

TEST(VlCacheRetainedSignal, CanceledGenerationCannotPoisonFreshFetchWait)
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

    boost::asio::cancellation_signal cancel;
    auto canceled = boost::asio::co_spawn(
        io,
        cache->co_get(),
        boost::asio::bind_cancellation_slot(
            cancel.slot(), boost::asio::use_future));
    scheduler.run_until_idle();
    cancel.emit(boost::asio::cancellation_type::terminal);
    scheduler.run_until_idle();
    ASSERT_EQ(canceled.wait_for(0ms), std::future_status::ready);
    EXPECT_THROW(canceled.get(), boost::system::system_error);

    pending(successful_vl_result());
    scheduler.run_until_idle();

    auto fresh =
        boost::asio::co_spawn(io, cache->co_refresh(), boost::asio::use_future);
    scheduler.run_until_idle();
    ASSERT_EQ(fetches, 2u);
    ASSERT_EQ(fresh.wait_for(0ms), std::future_status::timeout)
        << "a canceled subscriber from the prior generation must not poison "
           "the fresh generation";

    auto refreshed = successful_vl_result();
    refreshed.vl.sequence = 2;
    pending(std::move(refreshed));
    scheduler.run_until_idle();
    ASSERT_EQ(fresh.wait_for(0ms), std::future_status::ready);
    EXPECT_EQ(fresh.get().sequence, 2u);
}

TEST(VlCacheRetainedSignal, LateOldCompletionCannotWakeFreshGeneration)
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
    auto first_snapshot = VlCacheTestAccess::snapshot(cache);
    scheduler.run_until_idle();
    auto const old_signal = first_snapshot.get().fetch_wait_signal;
    ASSERT_TRUE(old_signal);

    pending(successful_vl_result());
    scheduler.run_until_idle();

    auto refresh =
        boost::asio::co_spawn(io, cache->co_refresh(), boost::asio::use_future);
    scheduler.run_until_idle();
    ASSERT_EQ(fetches, 2u);
    auto second_snapshot = VlCacheTestAccess::snapshot(cache);
    scheduler.run_until_idle();
    auto const fresh_signal = second_snapshot.get().fetch_wait_signal;
    ASSERT_TRUE(fresh_signal);
    ASSERT_NE(old_signal, fresh_signal);

    old_signal->post_complete();
    scheduler.run_until_idle();
    ASSERT_EQ(refresh.wait_for(0ms), std::future_status::timeout)
        << "a late completion of generation N-1 must not satisfy generation N";

    auto refreshed = successful_vl_result();
    refreshed.vl.sequence = 2;
    pending(std::move(refreshed));
    scheduler.run_until_idle();
    ASSERT_EQ(refresh.wait_for(0ms), std::future_status::ready);
    EXPECT_EQ(refresh.get().sequence, 2u);
}

// ── V2 census evidence ──────────────────────────────────────────────────
//
// The proposed V2 fitting keys a writ::census by a closed obligation KIND
// {fetch, refresh_timer}. writ::census::admit refuses a duplicate LIVE key,
// so that key type is sound only if each kind holds at most one obligation
// at a time. prv.S0025 (fetch_in_progress_) is a bool and structurally
// holds one. prv.S0024 (refresh_wait_outstanding_) is a size_t, and this
// witness is why it must be: asio delivers a cancelled timer handler
// THROUGH the executor, so arming a successor while the predecessor's
// handler is still queued leaves two unreceipted obligations of one kind.
TEST(VlCacheCensus, SuccessorRetryIsArmedWhilePredecessorHandlerIsUnreceipted)
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

    // A failed terminal arms the predecessor retry: exactly one obligation.
    catl::vl::VlResult failed;
    failed.error = "arm predecessor retry";
    pending(std::move(failed));
    scheduler.run_until_idle();

    auto armed = VlCacheTestAccess::snapshot(cache);
    scheduler.run_until_idle();
    ASSERT_EQ(armed.get().refresh_wait_outstanding, 1u)
        << "the predecessor retry handler must be outstanding";

    // The next fetch terminates SYNCHRONOUSLY on the owner strand — the shape
    // production reaches through publish_start_refusal (prv.T0216) when
    // VlClient construction, prepare_fetch, or operation->start throws. That
    // terminal arms a successor retry whose expires_after cancels the
    // predecessor. Cancellation is a request, not a receipt.
    std::size_t at_successor_admission = 0;
    bool successor_armed = false;
    VlCacheTestAccess::set_fetcher(
        cache,
        [weak = std::weak_ptr<VlCache>(cache),
         &at_successor_admission,
         &successor_armed](catl::vl::VlCallback callback) {
            catl::vl::VlResult sync_failed;
            sync_failed.error = "synchronous successor terminal";
            callback(std::move(sync_failed));
            // Still inside the same strand turn: nothing posted during that
            // terminal — the cancelled predecessor handler included — has run.
            auto const owner = weak.lock();
            ASSERT_TRUE(owner);
            at_successor_admission =
                VlCacheTestAccess::refresh_wait_outstanding_on_strand(owner);
            successor_armed = true;
        });

    auto get =
        boost::asio::co_spawn(io, cache->co_get(), boost::asio::use_future);
    scheduler.run_until_idle();
    ASSERT_TRUE(successor_armed);
    EXPECT_THROW(get.get(), std::runtime_error);

    EXPECT_EQ(at_successor_admission, 2u)
        << "prv.S0024 is not a single-obligation latch: at the instant the "
           "successor retry is admitted the cancelled predecessor handler is "
           "still owed, so a census keyed by obligation KIND alone would have "
           "to refuse a legitimate live re-admission";

    // Both obligations are receipted independently; only the second receipt
    // can commit stopped.
    auto stopped =
        boost::asio::co_spawn(io, cache->co_stop(), boost::asio::use_future);
    scheduler.run_until_idle();
    ASSERT_EQ(stopped.wait_for(0ms), std::future_status::ready);
    EXPECT_NO_THROW(stopped.get());

    auto terminal = VlCacheTestAccess::snapshot(cache);
    scheduler.run_until_idle();
    auto const drained = terminal.get();
    EXPECT_TRUE(drained.stopped);
    EXPECT_EQ(drained.refresh_wait_outstanding, 0u);
}

// Cardinality is multiplicity-many, not two: each concurrently queued
// public entrant can cancel the currently armed wait (leaving its handler
// queued and owed) and admit a fetch whose synchronous terminal arms a
// successor. Two back-to-back co_refresh entrants therefore stack THREE
// owed obligations under the one refresh_timer kind. Any census contract
// claiming a small closed maximum for this kind is falsified here; the
// honest declaration is multiplicity-many, bounded only by concurrent
// entrants + 1.
TEST(VlCacheCensus, ConcurrentRefreshEntrantsStackThreeTimerObligations)
{
    catl::test::DeterministicScheduler scheduler;
    auto& io = scheduler.io();
    auto cache = VlCache::create(io, "unused.invalid");

    catl::vl::VlCallback pending;
    std::vector<std::size_t> at_admission;
    std::size_t fetches = 0;
    VlCacheTestAccess::set_fetcher(
        cache,
        [weak = std::weak_ptr<VlCache>(cache),
         &pending,
         &at_admission,
         &fetches](catl::vl::VlCallback callback) {
            ++fetches;
            if (fetches == 1)
            {
                pending = std::move(callback);
                return;
            }
            catl::vl::VlResult sync_failed;
            sync_failed.error = "synchronous refusal";
            callback(std::move(sync_failed));
            auto const owner = weak.lock();
            ASSERT_TRUE(owner);
            at_admission.push_back(
                VlCacheTestAccess::refresh_wait_outstanding_on_strand(owner));
        });

    cache->start();
    scheduler.run_until_idle();
    ASSERT_TRUE(pending);

    // Failed first fetch arms the predecessor retry: one owed obligation.
    catl::vl::VlResult failed;
    failed.error = "arm predecessor retry";
    pending(std::move(failed));
    scheduler.run_until_idle();

    // Two refresh entrants queued back-to-back. Entrant A cancels the armed
    // retry (its handler now queued and owed) and its synchronous terminal
    // arms a successor: 2 at admission. Entrant B runs BEFORE either queued
    // abort delivers, cancels A's successor, and arms a third: 3.
    auto a =
        boost::asio::co_spawn(io, cache->co_refresh(), boost::asio::use_future);
    auto b =
        boost::asio::co_spawn(io, cache->co_refresh(), boost::asio::use_future);
    scheduler.run_until_idle();

    ASSERT_EQ(fetches, 3u);
    ASSERT_EQ(at_admission.size(), 2u);
    EXPECT_EQ(at_admission[0], 2u);
    EXPECT_EQ(at_admission[1], 3u)
        << "two queued entrants must stack a third owed obligation; a census "
           "contract pinning refresh_timer at <=2 is falsified";

    EXPECT_THROW(a.get(), std::runtime_error);
    EXPECT_THROW(b.get(), std::runtime_error);

    // Every stacked obligation is receipted independently; stop drains.
    auto stopped =
        boost::asio::co_spawn(io, cache->co_stop(), boost::asio::use_future);
    scheduler.run_until_idle();
    ASSERT_EQ(stopped.wait_for(0ms), std::future_status::ready);
    EXPECT_NO_THROW(stopped.get());
    auto terminal = VlCacheTestAccess::snapshot(cache);
    scheduler.run_until_idle();
    EXPECT_EQ(terminal.get().refresh_wait_outstanding, 0u);
}

// Negative control for the witness above. Identical probe, identical
// synchronous terminal — but with NO predecessor handler armed. It must read
// one, not two, or the reading of two proves nothing about the predecessor.
TEST(VlCacheCensus, SynchronousTerminalWithoutPredecessorOwesOneTimerObligation)
{
    catl::test::DeterministicScheduler scheduler;
    auto& io = scheduler.io();
    auto cache = VlCache::create(io, "unused.invalid");

    std::size_t at_first_admission = 0;
    bool armed = false;
    VlCacheTestAccess::set_fetcher(
        cache,
        [weak = std::weak_ptr<VlCache>(cache), &at_first_admission, &armed](
            catl::vl::VlCallback callback) {
            catl::vl::VlResult sync_failed;
            sync_failed.error = "synchronous first terminal";
            callback(std::move(sync_failed));
            auto const owner = weak.lock();
            ASSERT_TRUE(owner);
            at_first_admission =
                VlCacheTestAccess::refresh_wait_outstanding_on_strand(owner);
            armed = true;
        });

    cache->start();
    scheduler.run_until_idle();

    ASSERT_TRUE(armed);
    EXPECT_EQ(at_first_admission, 1u)
        << "with no predecessor owed, the same probe at the same point in the "
           "same strand turn must read exactly one";

    auto stopped =
        boost::asio::co_spawn(io, cache->co_stop(), boost::asio::use_future);
    scheduler.run_until_idle();
    ASSERT_EQ(stopped.wait_for(0ms), std::future_status::ready);
    EXPECT_NO_THROW(stopped.get());
}

}  // namespace
}  // namespace xprv
