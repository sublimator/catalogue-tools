#include <xprv/http-server.h>
#include <xprv/proof-engine.h>

#include "../peer-client/ledger-fetch-coordinator-test-access.h"
#include "../peer-client/peer-set-test-access.h"
#include "../test-support/deterministic-scheduler.h"
#include "../test-support/io-context-runners.h"
#include "http-child-completion.h"
#include "proof-engine-test-access.h"
#include "service-lifecycle.h"

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http.hpp>
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace xprv {

class RequestDrainTestAccess
{
public:
    static std::shared_ptr<RequestDrainAuthority>
    make_authority(std::uint64_t epoch)
    {
        auto authority = std::make_shared<RequestDrainAuthority>();
        authority->current_epoch_.store(epoch, std::memory_order_release);
        return authority;
    }

    static RequestDrainReceipt
    mint(
        std::shared_ptr<RequestDrainAuthority> const& authority,
        std::uint64_t epoch)
    {
        return RequestDrainReceipt{authority, epoch};
    }

    static bool
    matches(
        RequestDrainReceipt const& receipt,
        std::shared_ptr<RequestDrainAuthority> const& authority)
    {
        return receipt.matches(authority);
    }

    static void
    advance(
        std::shared_ptr<RequestDrainAuthority> const& authority,
        std::uint64_t epoch)
    {
        authority->current_epoch_.store(epoch, std::memory_order_release);
    }

    static void
    bind(
        RequestDrainBinding& binding,
        std::shared_ptr<RequestDrainAuthority> const& authority)
    {
        binding.bind(authority);
    }

    static bool
    accepts(
        RequestDrainBinding const& binding,
        RequestDrainReceipt const& receipt)
    {
        return binding.accepts(receipt);
    }
};

// ProofEngineTestAccess moved to proof-engine-test-access.h so other
// test TUs in this binary can share it (issue 0074 witnesses).

class HttpServerTestAccess
{
public:
    static std::uint16_t
    port(HttpServer const& server)
    {
        return server.acceptor_.local_endpoint().port();
    }

    static void
    set_regular_path_hooks(
        HttpServer& server,
        std::function<void()> before_race,
        std::function<boost::asio::awaitable<void>()> before_watcher_receipt)
    {
        server.test_before_regular_race_ = std::move(before_race);
        server.test_before_regular_watcher_receipt_ =
            std::move(before_watcher_receipt);
    }

    static boost::asio::awaitable<bool>
    wait_for_transport_failure(boost::asio::ip::tcp::socket& socket)
    {
        co_return co_await HttpServer::co_wait_for_transport_failure(socket);
    }
};

}  // namespace xprv

namespace {

static_assert(
    noexcept(std::declval<xprv::detail::ServiceLifecycle&>().begin_shutdown()));

template <class Predicate>
bool
pump_until(
    boost::asio::io_context& io,
    Predicate&& predicate,
    std::chrono::steady_clock::duration timeout = std::chrono::seconds(2))
{
    namespace asio = boost::asio;

    auto expired = std::make_shared<std::atomic<bool>>(false);
    asio::steady_timer watchdog(io, timeout);
    watchdog.async_wait([expired](boost::system::error_code const& ec) {
        if (!ec)
            expired->store(true, std::memory_order_release);
    });

    while (!predicate() && !expired->load(std::memory_order_acquire))
    {
        if (io.stopped())
            io.restart();
        if (io.run_one() == 0)
            break;
    }

    watchdog.cancel();
    return predicate();
}

template <class Engine>
concept CanStopWithoutRequestDrain =
    requires(Engine& engine) { engine.co_stop(); };

template <class Engine>
concept CanStopWithRequestDrain =
    requires(Engine& engine, xprv::RequestDrainReceipt const& receipt) {
        engine.co_stop(receipt);
    };

static_assert(!std::is_default_constructible_v<xprv::RequestDrainReceipt>);
static_assert(!std::is_copy_constructible_v<xprv::RequestDrainReceipt>);
static_assert(std::is_move_constructible_v<xprv::RequestDrainReceipt>);
static_assert(
    !std::is_default_constructible_v<xprv::ProofEngine::DrainReceipt>);
static_assert(!std::is_copy_constructible_v<xprv::ProofEngine::DrainReceipt>);
static_assert(!std::is_copy_assignable_v<xprv::ProofEngine::DrainReceipt>);
static_assert(std::is_move_constructible_v<xprv::ProofEngine::DrainReceipt>);
static_assert(std::is_move_assignable_v<xprv::ProofEngine::DrainReceipt>);
static_assert(!std::is_constructible_v<
              xprv::ProofEngine::DrainReceipt,
              std::shared_ptr<xprv::ProofEngine const>>);
using EngineStopFunction =
    std::function<boost::asio::awaitable<xprv::ProofEngine::DrainReceipt>(
        xprv::RequestDrainReceipt const&)>;
static_assert(
    !std::is_default_constructible_v<xprv::detail::EngineDrainOperation>);
static_assert(!std::is_constructible_v<
              xprv::detail::EngineDrainOperation,
              std::shared_ptr<xprv::ProofEngine const>,
              EngineStopFunction>);
static_assert(!CanStopWithoutRequestDrain<xprv::ProofEngine>);
static_assert(CanStopWithRequestDrain<xprv::ProofEngine>);

template <class Stop>
xprv::detail::EngineDrainOperation
test_engine_stop(boost::asio::io_context& io, Stop stop)
{
    auto engine =
        xprv::ProofEngine::create(io, xprv::NetworkConfig::for_network(0));
    return xprv::ProofEngineTestAccess::make_engine_drain_operation(
        engine,
        [engine, stop = std::move(stop)](
            xprv::RequestDrainReceipt const& request_drain) mutable
        -> boost::asio::awaitable<xprv::ProofEngine::DrainReceipt> {
            co_await stop(request_drain);
            co_return xprv::ProofEngineTestAccess::mint_drain_receipt(engine);
        });
}

template <class CompletionToken>
auto
async_wait_started(
    std::shared_ptr<boost::asio::steady_timer> timer,
    std::promise<void>& entered,
    CompletionToken&& token)
{
    return boost::asio::async_initiate<
        CompletionToken,
        void(boost::system::error_code)>(
        [timer = std::move(timer), &entered](auto handler) mutable {
            timer->async_wait(std::move(handler));
            // Publish only after async_wait initiation has returned. The test
            // thread may then cancel without racing timer object mutation.
            entered.set_value();
        },
        token);
}

xprv::detail::ServiceTimerControl
timer_control(catl::test::DeterministicScheduler::Timer& timer)
{
    return {
        .expires_after =
            [&timer](auto duration) { timer.expires_after(duration); },
        .async_wait =
            [&timer](auto executor, auto handler) {
                timer.async_wait([executor = std::move(executor),
                                  handler = std::move(handler)](
                                     boost::system::error_code error) mutable {
                    boost::asio::post(
                        std::move(executor),
                        [error, handler = std::move(handler)]() mutable {
                            handler(error);
                        });
                });
            },
        .cancel = [&timer]() { (void)timer.cancel(); },
    };
}

class ControlledServiceTimer
{
public:
    xprv::detail::ServiceTimerControl
    control()
    {
        return {
            .expires_after =
                [this](auto) {
                    if (throw_on_expires)
                        throw std::runtime_error("timer expires failpoint");
                },
            .async_wait =
                [this](auto executor, auto handler) {
                    if (throw_on_wait)
                        throw std::runtime_error("timer wait failpoint");
                    if (handler_)
                        throw std::logic_error("timer already has a handler");
                    executor_ = std::move(executor);
                    handler_ = std::move(handler);
                },
            .cancel =
                [this]() {
                    ++cancel_calls;
                    if (throw_on_cancel)
                        throw std::runtime_error("timer cancel failpoint");
                    queue(cancel_error);
                },
        };
    }

    void
    fire()
    {
        queue({});
    }

    void
    complete(boost::system::error_code error)
    {
        queue(error);
    }

    bool throw_on_expires = false;
    bool throw_on_wait = false;
    bool throw_on_cancel = false;
    boost::system::error_code cancel_error =
        boost::asio::error::operation_aborted;
    std::size_t cancel_calls = 0;
    bool raw_handler_entered = false;
    bool raw_handler_returned = false;

private:
    void
    queue(boost::system::error_code error)
    {
        if (!handler_ || !executor_)
            return;
        auto handler = std::move(handler_);
        handler_ = {};
        boost::asio::post(
            *executor_, [this, error, handler = std::move(handler)]() mutable {
                raw_handler_entered = true;
                handler(error);
                raw_handler_returned = true;
            });
    }

    std::optional<boost::asio::any_io_executor> executor_;
    xprv::detail::ServiceTimerControl::Handler handler_;
};

std::string
exception_message(std::exception_ptr const& failure)
{
    if (!failure)
        return {};
    try
    {
        std::rethrow_exception(failure);
    }
    catch (std::exception const& error)
    {
        return error.what();
    }
    catch (...)
    {
        return "non-standard exception";
    }
}

class BlockingReturnTimer
{
public:
    BlockingReturnTimer()
        : release_future_(release_promise_.get_future().share())
        , handler_return_pending_future_(
              handler_return_pending_promise_.get_future())
    {
    }

    xprv::detail::ServiceTimerControl
    control()
    {
        return {
            .expires_after = [](auto) {},
            .async_wait =
                [this](auto executor, auto handler) {
                    executor_ = std::move(executor);
                    handler_ = std::move(handler);
                },
            .cancel =
                [this]() {
                    if (!executor_ || !handler_)
                        return;
                    auto handler = std::move(handler_);
                    handler_ = {};
                    boost::asio::post(
                        *executor_,
                        [this, handler = std::move(handler)]() mutable {
                            handler(boost::asio::error::operation_aborted);
                            handler_return_pending_promise_.set_value();
                            release_future_.wait();
                            raw_handler_returned_.store(
                                true, std::memory_order_release);
                        });
                },
        };
    }

    std::future_status
    wait_until_return_is_blocked(std::chrono::steady_clock::duration timeout)
    {
        return handler_return_pending_future_.wait_for(timeout);
    }

    void
    release_return()
    {
        release_promise_.set_value();
    }

    bool
    raw_handler_returned() const
    {
        return raw_handler_returned_.load(std::memory_order_acquire);
    }

private:
    std::optional<boost::asio::any_io_executor> executor_;
    xprv::detail::ServiceTimerControl::Handler handler_;
    std::promise<void> release_promise_;
    std::shared_future<void> release_future_;
    std::promise<void> handler_return_pending_promise_;
    std::future<void> handler_return_pending_future_;
    std::atomic<bool> raw_handler_returned_{false};
};

xprv::detail::ServiceLifecycleOperations
basic_service_operations(
    std::shared_ptr<xprv::RequestDrainAuthority> authority,
    std::size_t& stop_io_calls)
{
    return {
        .report_status = []() -> boost::asio::awaitable<void> { co_return; },
        .stop_http = [authority = std::move(authority)]()
            -> boost::asio::awaitable<xprv::RequestDrainReceipt> {
            co_return xprv::RequestDrainTestAccess::mint(authority, 1);
        },
        .stop_engines = {},
        .report_diagnostic = {},
        .terminal_diagnostic = {},
        .stop_io = [&stop_io_calls]() { ++stop_io_calls; },
    };
}

TEST(ServiceRootLifecycle, SealsEveryEngineRowBeforeLaunchingAny)
{
    namespace asio = boost::asio;
    using Outcome = xprv::detail::ServiceTerminalOutcome;

    catl::test::DeterministicScheduler scheduler;
    catl::test::DeterministicScheduler::Timer status_timer(scheduler);
    catl::test::DeterministicScheduler::Timer deadline_timer(scheduler);
    catl::test::Gate first_gate(scheduler.io());
    catl::test::Gate second_gate(scheduler.io());
    auto authority = xprv::RequestDrainTestAccess::make_authority(1);
    std::size_t stop_io_calls = 0;
    std::array<std::size_t, 2> entered{};
    auto operations = basic_service_operations(authority, stop_io_calls);
    operations.stop_engines = {
        test_engine_stop(
            scheduler.io(),
            [&](xprv::RequestDrainReceipt const&) -> asio::awaitable<void> {
                ++entered[0];
                co_await first_gate.async_wait(asio::use_awaitable);
            }),
        test_engine_stop(
            scheduler.io(),
            [&](xprv::RequestDrainReceipt const&) -> asio::awaitable<void> {
                ++entered[1];
                co_await second_gate.async_wait(asio::use_awaitable);
            }),
    };

    auto lifecycle = xprv::detail::ServiceLifecycle::create(
        scheduler.io(),
        timer_control(status_timer),
        timer_control(deadline_timer),
        std::move(operations),
        std::chrono::seconds(30),
        std::chrono::seconds(10));
    lifecycle->begin_shutdown();
    scheduler.run_until_idle();

    EXPECT_EQ(entered[0], 1);
    EXPECT_EQ(entered[1], 1);
    EXPECT_FALSE(lifecycle->terminal_result());

    first_gate.open();
    second_gate.open();
    scheduler.run_until_idle();

    auto const result = lifecycle->terminal_result();
    ASSERT_TRUE(result);
    EXPECT_EQ(result->outcome, Outcome::graceful);
    EXPECT_EQ(stop_io_calls, 1);
}

TEST(ServiceRootLifecycle, SealedZeroEngineCensusIsNotTransientEmpty)
{
    using Outcome = xprv::detail::ServiceTerminalOutcome;

    catl::test::DeterministicScheduler scheduler;
    catl::test::DeterministicScheduler::Timer status_timer(scheduler);
    catl::test::DeterministicScheduler::Timer deadline_timer(scheduler);
    auto authority = xprv::RequestDrainTestAccess::make_authority(1);
    std::size_t stop_io_calls = 0;
    bool reached_pre_seal = false;
    std::weak_ptr<xprv::detail::ServiceLifecycle> weak;
    auto operations = basic_service_operations(authority, stop_io_calls);
    operations.before_engine_admission_seal_for_test = [&]() {
        reached_pre_seal = true;
        auto lifecycle = weak.lock();
        ASSERT_TRUE(lifecycle);
        EXPECT_FALSE(lifecycle->terminal_result());
    };

    auto lifecycle = xprv::detail::ServiceLifecycle::create(
        scheduler.io(),
        timer_control(status_timer),
        timer_control(deadline_timer),
        std::move(operations),
        std::chrono::seconds(30),
        std::chrono::seconds(10));
    weak = lifecycle;
    lifecycle->begin_shutdown();
    scheduler.run_until_idle();

    EXPECT_TRUE(reached_pre_seal);
    auto const result = lifecycle->terminal_result();
    ASSERT_TRUE(result);
    EXPECT_EQ(result->outcome, Outcome::graceful);
    EXPECT_EQ(stop_io_calls, 1);
}

TEST(ServiceRootLifecycle, MiddleLaunchFailureDoesNotOrphanLaterRows)
{
    namespace asio = boost::asio;
    using Outcome = xprv::detail::ServiceTerminalOutcome;

    catl::test::DeterministicScheduler scheduler;
    catl::test::DeterministicScheduler::Timer status_timer(scheduler);
    catl::test::DeterministicScheduler::Timer deadline_timer(scheduler);
    auto authority = xprv::RequestDrainTestAccess::make_authority(1);
    std::size_t stop_io_calls = 0;
    std::array<std::size_t, 3> launch_attempts{};
    std::array<std::size_t, 3> entered{};
    auto operations = basic_service_operations(authority, stop_io_calls);
    operations.stop_engines = {
        test_engine_stop(
            scheduler.io(),
            [&](xprv::RequestDrainReceipt const&) -> asio::awaitable<void> {
                ++entered[0];
                co_return;
            }),
        test_engine_stop(
            scheduler.io(),
            [&](xprv::RequestDrainReceipt const&) -> asio::awaitable<void> {
                ++entered[1];
                co_return;
            }),
        test_engine_stop(
            scheduler.io(),
            [&](xprv::RequestDrainReceipt const&) -> asio::awaitable<void> {
                ++entered[2];
                co_return;
            }),
    };
    operations.before_engine_launch_for_test = [&](std::size_t index) {
        ++launch_attempts[index];
        if (index == 1)
            throw std::runtime_error("middle engine launch failpoint");
    };

    auto lifecycle = xprv::detail::ServiceLifecycle::create(
        scheduler.io(),
        timer_control(status_timer),
        timer_control(deadline_timer),
        std::move(operations),
        std::chrono::seconds(30),
        std::chrono::seconds(10));
    lifecycle->begin_shutdown();
    scheduler.run_until_idle();

    EXPECT_EQ(launch_attempts, (std::array<std::size_t, 3>{1, 1, 1}));
    EXPECT_EQ(entered, (std::array<std::size_t, 3>{1, 0, 1}));
    auto const result = lifecycle->terminal_result();
    ASSERT_TRUE(result);
    EXPECT_EQ(result->outcome, Outcome::failed);
    EXPECT_TRUE(result->failure);
    EXPECT_EQ(stop_io_calls, 1);
}

TEST(ServiceRootLifecycle, ActiveStatusReportBlocksGracefulTerminal)
{
    namespace asio = boost::asio;
    using Outcome = xprv::detail::ServiceTerminalOutcome;

    catl::test::DeterministicScheduler scheduler;
    catl::test::DeterministicScheduler::Timer status_timer(scheduler);
    catl::test::DeterministicScheduler::Timer deadline_timer(scheduler);
    catl::test::Gate report_gate(scheduler.io());
    auto authority = xprv::RequestDrainTestAccess::make_authority(1);
    std::size_t stop_io_calls = 0;
    bool report_entered = false;
    auto operations = basic_service_operations(authority, stop_io_calls);
    operations.report_status = [&]() -> asio::awaitable<void> {
        report_entered = true;
        co_await report_gate.async_wait(asio::use_awaitable);
    };

    auto lifecycle = xprv::detail::ServiceLifecycle::create(
        scheduler.io(),
        timer_control(status_timer),
        timer_control(deadline_timer),
        std::move(operations),
        std::chrono::seconds(1),
        std::chrono::seconds(10));
    lifecycle->start();
    scheduler.run_until_idle();
    scheduler.advance(std::chrono::seconds(1));
    ASSERT_TRUE(report_entered);

    lifecycle->begin_shutdown();
    scheduler.run_until_idle();
    EXPECT_FALSE(lifecycle->terminal_result());

    report_gate.open();
    scheduler.run_until_idle();
    auto const result = lifecycle->terminal_result();
    ASSERT_TRUE(result);
    EXPECT_EQ(result->outcome, Outcome::graceful);
    EXPECT_EQ(stop_io_calls, 1);
}

TEST(ServiceRootLifecycle, TimerReceiptIsConsumedOnlyAfterRawHandlerReturns)
{
    using Outcome = xprv::detail::ServiceTerminalOutcome;

    catl::test::DeterministicScheduler scheduler;
    ControlledServiceTimer status_timer;
    ControlledServiceTimer deadline_timer;
    auto authority = xprv::RequestDrainTestAccess::make_authority(1);
    std::size_t stop_io_calls = 0;
    auto operations = basic_service_operations(authority, stop_io_calls);

    auto lifecycle = xprv::detail::ServiceLifecycle::create(
        scheduler.io(),
        status_timer.control(),
        deadline_timer.control(),
        std::move(operations),
        std::chrono::seconds(30),
        std::chrono::seconds(10));
    lifecycle->begin_shutdown();
    while (deadline_timer.cancel_calls == 0)
        ASSERT_TRUE(scheduler.step());

    EXPECT_FALSE(deadline_timer.raw_handler_entered);
    EXPECT_FALSE(lifecycle->terminal_result());

    ASSERT_TRUE(scheduler.step());
    EXPECT_TRUE(deadline_timer.raw_handler_entered);
    EXPECT_TRUE(deadline_timer.raw_handler_returned);
    EXPECT_FALSE(lifecycle->terminal_result());

    ASSERT_TRUE(scheduler.step());
    auto const result = lifecycle->terminal_result();
    ASSERT_TRUE(result);
    EXPECT_EQ(result->outcome, Outcome::graceful);
    EXPECT_EQ(stop_io_calls, 1);
}

TEST(ServiceRootLifecycle, DeadlineWinsWithoutRewritingLaterCompletion)
{
    namespace asio = boost::asio;
    using Outcome = xprv::detail::ServiceTerminalOutcome;

    catl::test::DeterministicScheduler scheduler;
    catl::test::DeterministicScheduler::Timer status_timer(scheduler);
    catl::test::DeterministicScheduler::Timer deadline_timer(scheduler);
    catl::test::Gate engine_gate(scheduler.io());
    auto authority = xprv::RequestDrainTestAccess::make_authority(1);
    std::size_t stop_io_calls = 0;
    auto operations = basic_service_operations(authority, stop_io_calls);
    operations.stop_engines = {
        test_engine_stop(
            scheduler.io(),
            [&](xprv::RequestDrainReceipt const&) -> asio::awaitable<void> {
                co_await engine_gate.async_wait(asio::use_awaitable);
            }),
    };

    auto lifecycle = xprv::detail::ServiceLifecycle::create(
        scheduler.io(),
        timer_control(status_timer),
        timer_control(deadline_timer),
        std::move(operations),
        std::chrono::seconds(30),
        std::chrono::seconds(2));
    lifecycle->begin_shutdown();
    scheduler.run_until_idle();
    ASSERT_FALSE(lifecycle->terminal_result());

    scheduler.advance(std::chrono::seconds(2));
    auto result = lifecycle->terminal_result();
    ASSERT_TRUE(result);
    EXPECT_EQ(result->outcome, Outcome::forced);
    EXPECT_EQ(stop_io_calls, 1);

    engine_gate.open();
    scheduler.run_until_idle();
    result = lifecycle->terminal_result();
    ASSERT_TRUE(result);
    EXPECT_EQ(result->outcome, Outcome::forced);
    EXPECT_EQ(stop_io_calls, 1);
}

TEST(ServiceRootLifecycle, DuplicateShutdownReplaysOneRetainedOutcome)
{
    using Outcome = xprv::detail::ServiceTerminalOutcome;

    catl::test::DeterministicScheduler scheduler;
    catl::test::DeterministicScheduler::Timer status_timer(scheduler);
    catl::test::DeterministicScheduler::Timer deadline_timer(scheduler);
    auto authority = xprv::RequestDrainTestAccess::make_authority(1);
    std::size_t stop_io_calls = 0;
    auto operations = basic_service_operations(authority, stop_io_calls);
    auto lifecycle = xprv::detail::ServiceLifecycle::create(
        scheduler.io(),
        timer_control(status_timer),
        timer_control(deadline_timer),
        std::move(operations),
        std::chrono::seconds(30),
        std::chrono::seconds(10));

    lifecycle->begin_shutdown();
    lifecycle->begin_shutdown();
    scheduler.run_until_idle();
    auto first = lifecycle->terminal_result();
    ASSERT_TRUE(first);
    EXPECT_EQ(first->outcome, Outcome::graceful);
    EXPECT_EQ(stop_io_calls, 1);

    lifecycle->begin_shutdown();
    scheduler.run_until_idle();
    auto second = lifecycle->terminal_result();
    ASSERT_TRUE(second);
    EXPECT_EQ(second->outcome, first->outcome);
    EXPECT_EQ(second->failure, first->failure);
    EXPECT_EQ(stop_io_calls, 1);
}

TEST(ServiceRootLifecycle, StatusTimerLaunchRefusalIsRetainedFailure)
{
    using Outcome = xprv::detail::ServiceTerminalOutcome;

    catl::test::DeterministicScheduler scheduler;
    ControlledServiceTimer status_timer;
    ControlledServiceTimer deadline_timer;
    status_timer.throw_on_wait = true;
    auto authority = xprv::RequestDrainTestAccess::make_authority(1);
    std::size_t stop_io_calls = 0;
    auto operations = basic_service_operations(authority, stop_io_calls);
    auto lifecycle = xprv::detail::ServiceLifecycle::create(
        scheduler.io(),
        status_timer.control(),
        deadline_timer.control(),
        std::move(operations),
        std::chrono::seconds(30),
        std::chrono::seconds(10));

    lifecycle->start();
    scheduler.run_until_idle();
    lifecycle->begin_shutdown();
    scheduler.run_until_idle();

    auto const result = lifecycle->terminal_result();
    ASSERT_TRUE(result);
    EXPECT_EQ(result->outcome, Outcome::failed);
    EXPECT_TRUE(result->failure);
    EXPECT_EQ(stop_io_calls, 1);
}

TEST(ServiceRootLifecycle, StatusTimerErrorWithoutDiagnosticIsRetainedFailure)
{
    using Outcome = xprv::detail::ServiceTerminalOutcome;

    catl::test::DeterministicScheduler scheduler;
    ControlledServiceTimer status_timer;
    ControlledServiceTimer deadline_timer;
    status_timer.cancel_error = boost::asio::error::operation_not_supported;
    auto authority = xprv::RequestDrainTestAccess::make_authority(1);
    std::size_t stop_io_calls = 0;
    auto operations = basic_service_operations(authority, stop_io_calls);

    auto lifecycle = xprv::detail::ServiceLifecycle::create(
        scheduler.io(),
        status_timer.control(),
        deadline_timer.control(),
        std::move(operations),
        std::chrono::seconds(30),
        std::chrono::seconds(10));
    lifecycle->start();
    scheduler.run_until_idle();
    lifecycle->begin_shutdown();
    scheduler.run_until_idle();

    auto const result = lifecycle->terminal_result();
    ASSERT_TRUE(result);
    EXPECT_EQ(result->outcome, Outcome::failed);
    ASSERT_TRUE(result->failure);
    EXPECT_NE(
        exception_message(result->failure).find("not supported"),
        std::string::npos);
    EXPECT_EQ(stop_io_calls, 1);
}

TEST(ServiceRootLifecycle, StatusTimerCancelFailureSurvivesExactReceipt)
{
    using Outcome = xprv::detail::ServiceTerminalOutcome;

    catl::test::DeterministicScheduler scheduler;
    ControlledServiceTimer status_timer;
    ControlledServiceTimer deadline_timer;
    status_timer.throw_on_cancel = true;
    auto authority = xprv::RequestDrainTestAccess::make_authority(1);
    std::size_t stop_io_calls = 0;
    auto operations = basic_service_operations(authority, stop_io_calls);

    auto lifecycle = xprv::detail::ServiceLifecycle::create(
        scheduler.io(),
        status_timer.control(),
        deadline_timer.control(),
        std::move(operations),
        std::chrono::seconds(30),
        std::chrono::seconds(10));
    lifecycle->start();
    scheduler.run_until_idle();
    lifecycle->begin_shutdown();
    scheduler.run_until_idle();

    EXPECT_FALSE(lifecycle->terminal_result());
    status_timer.complete(boost::asio::error::operation_aborted);
    scheduler.run_until_idle();

    auto const result = lifecycle->terminal_result();
    ASSERT_TRUE(result);
    EXPECT_EQ(result->outcome, Outcome::failed);
    ASSERT_TRUE(result->failure);
    EXPECT_EQ(exception_message(result->failure), "timer cancel failpoint");
    EXPECT_EQ(stop_io_calls, 1);
}

TEST(ServiceRootLifecycle, OrdinaryEngineFailureWaitsForEveryAdmittedResult)
{
    namespace asio = boost::asio;
    using Outcome = xprv::detail::ServiceTerminalOutcome;

    catl::test::DeterministicScheduler scheduler;
    catl::test::DeterministicScheduler::Timer status_timer(scheduler);
    catl::test::DeterministicScheduler::Timer deadline_timer(scheduler);
    catl::test::Gate held_engine(scheduler.io());
    auto authority = xprv::RequestDrainTestAccess::make_authority(1);
    std::size_t stop_io_calls = 0;
    auto operations = basic_service_operations(authority, stop_io_calls);
    operations.stop_engines = {
        test_engine_stop(
            scheduler.io(),
            [](xprv::RequestDrainReceipt const&) -> asio::awaitable<void> {
                throw std::runtime_error("ordinary engine failure");
                co_return;
            }),
        test_engine_stop(
            scheduler.io(),
            [&](xprv::RequestDrainReceipt const&) -> asio::awaitable<void> {
                co_await held_engine.async_wait(asio::use_awaitable);
            }),
    };

    auto lifecycle = xprv::detail::ServiceLifecycle::create(
        scheduler.io(),
        timer_control(status_timer),
        timer_control(deadline_timer),
        std::move(operations),
        std::chrono::seconds(30),
        std::chrono::seconds(10));
    lifecycle->begin_shutdown();
    scheduler.run_until_idle();

    EXPECT_FALSE(lifecycle->terminal_result());
    held_engine.open();
    scheduler.run_until_idle();

    auto const result = lifecycle->terminal_result();
    ASSERT_TRUE(result);
    EXPECT_EQ(result->outcome, Outcome::failed);
    ASSERT_TRUE(result->failure);
    EXPECT_EQ(exception_message(result->failure), "ordinary engine failure");
    EXPECT_EQ(stop_io_calls, 1);
}

TEST(ServiceRootLifecycle, ForeignEngineReceiptCannotSatisfyAdmittedRow)
{
    namespace asio = boost::asio;
    using Outcome = xprv::detail::ServiceTerminalOutcome;

    catl::test::DeterministicScheduler scheduler;
    catl::test::DeterministicScheduler::Timer status_timer(scheduler);
    catl::test::DeterministicScheduler::Timer deadline_timer(scheduler);
    auto authority = xprv::RequestDrainTestAccess::make_authority(1);
    auto expected_engine = xprv::ProofEngine::create(
        scheduler.io(), xprv::NetworkConfig::for_network(0));
    auto foreign_engine = xprv::ProofEngine::create(
        scheduler.io(), xprv::NetworkConfig::for_network(0));
    std::size_t stop_io_calls = 0;
    auto operations = basic_service_operations(authority, stop_io_calls);
    operations.stop_engines = {
        xprv::ProofEngineTestAccess::make_engine_drain_operation(
            expected_engine,
            [foreign_engine](xprv::RequestDrainReceipt const&)
                -> asio::awaitable<xprv::ProofEngine::DrainReceipt> {
                co_return xprv::ProofEngineTestAccess::mint_drain_receipt(
                    foreign_engine);
            }),
    };

    auto lifecycle = xprv::detail::ServiceLifecycle::create(
        scheduler.io(),
        timer_control(status_timer),
        timer_control(deadline_timer),
        std::move(operations),
        std::chrono::seconds(30),
        std::chrono::seconds(10));
    lifecycle->begin_shutdown();
    scheduler.run_until_idle();

    auto const result = lifecycle->terminal_result();
    ASSERT_TRUE(result);
    EXPECT_EQ(result->outcome, Outcome::failed);
    ASSERT_TRUE(result->failure);
    EXPECT_EQ(
        exception_message(result->failure),
        "engine drain completed without its exact receipt");
    EXPECT_EQ(stop_io_calls, 1);
}

TEST(ServiceRootLifecycle, ForcedStopKeepsBorrowedReceiptAliveUntilFrameDies)
{
    namespace asio = boost::asio;
    using Outcome = xprv::detail::ServiceTerminalOutcome;

    bool probe_destroyed = false;
    bool receipt_was_valid = false;
    {
        catl::test::DeterministicScheduler scheduler;
        catl::test::DeterministicScheduler::Timer status_timer(scheduler);
        catl::test::DeterministicScheduler::Timer deadline_timer(scheduler);
        auto authority = xprv::RequestDrainTestAccess::make_authority(1);
        std::size_t stop_io_calls = 0;
        auto operations = basic_service_operations(authority, stop_io_calls);
        operations.stop_io = [&]() {
            ++stop_io_calls;
            scheduler.io().stop();
        };
        operations.stop_engines = {test_engine_stop(
            scheduler.io(),
            [authority, &probe_destroyed, &receipt_was_valid](
                xprv::RequestDrainReceipt const& receipt)
                -> asio::awaitable<void> {
                struct Probe
                {
                    xprv::RequestDrainReceipt const& receipt;
                    std::shared_ptr<xprv::RequestDrainAuthority> authority;
                    bool& destroyed;
                    bool& valid;

                    ~Probe()
                    {
                        valid = xprv::RequestDrainTestAccess::matches(
                            receipt, authority);
                        destroyed = true;
                    }
                } probe{receipt, authority, probe_destroyed, receipt_was_valid};

                auto executor = co_await asio::this_coro::executor;
                asio::steady_timer never(
                    executor, asio::steady_timer::time_point::max());
                co_await never.async_wait(asio::use_awaitable);
            })};

        auto lifecycle = xprv::detail::ServiceLifecycle::create(
            scheduler.io(),
            timer_control(status_timer),
            timer_control(deadline_timer),
            std::move(operations),
            std::chrono::seconds(30),
            std::chrono::seconds(1));
        lifecycle->begin_shutdown();
        scheduler.run_until_idle();
        scheduler.advance(std::chrono::seconds(1));

        auto const result = lifecycle->terminal_result();
        ASSERT_TRUE(result);
        EXPECT_EQ(result->outcome, Outcome::forced);
        EXPECT_EQ(stop_io_calls, 1);
        EXPECT_FALSE(probe_destroyed);
        lifecycle.reset();
    }

    EXPECT_TRUE(probe_destroyed);
    EXPECT_TRUE(receipt_was_valid);
}

class ServiceRootRunnerModes : public ::testing::TestWithParam<std::size_t>
{
};

TEST_P(ServiceRootRunnerModes, ConcurrentShutdownRequestsCommitOneOutcome)
{
    namespace asio = boost::asio;
    using Outcome = xprv::detail::ServiceTerminalOutcome;

    asio::io_context io;
    auto status_timer = std::make_shared<asio::steady_timer>(io);
    auto deadline_timer = std::make_shared<asio::steady_timer>(io);
    auto authority = xprv::RequestDrainTestAccess::make_authority(1);
    std::size_t stop_io_calls = 0;
    auto operations = basic_service_operations(authority, stop_io_calls);
    operations.stop_io = [&]() {
        ++stop_io_calls;
        io.stop();
    };

    auto lifecycle = xprv::detail::ServiceLifecycle::create(
        io,
        xprv::detail::make_service_timer_control(status_timer),
        xprv::detail::make_service_timer_control(deadline_timer),
        std::move(operations),
        std::chrono::hours(1),
        std::chrono::hours(1));

    std::vector<std::thread> callers;
    callers.reserve(8);
    for (std::size_t index = 0; index < 8; ++index)
        callers.emplace_back([lifecycle]() { lifecycle->begin_shutdown(); });
    for (auto& caller : callers)
        caller.join();

    catl::test::IoContextRunners runners(io, GetParam());
    runners.join();

    auto const result = lifecycle->terminal_result();
    ASSERT_TRUE(result);
    EXPECT_EQ(result->outcome, Outcome::graceful);
    EXPECT_EQ(stop_io_calls, 1);
}

TEST_P(ServiceRootRunnerModes, ParallelEngineReceiptsDrainOnEveryRunnerMode)
{
    namespace asio = boost::asio;
    using Outcome = xprv::detail::ServiceTerminalOutcome;

    asio::io_context io;
    auto status_timer = std::make_shared<asio::steady_timer>(io);
    auto deadline_timer = std::make_shared<asio::steady_timer>(io);
    auto first_engine = std::make_shared<asio::steady_timer>(
        io, asio::steady_timer::time_point::max());
    auto second_engine = std::make_shared<asio::steady_timer>(
        io, asio::steady_timer::time_point::max());
    std::promise<void> first_entered;
    std::promise<void> second_entered;
    auto first_entered_future = first_entered.get_future();
    auto second_entered_future = second_entered.get_future();
    auto authority = xprv::RequestDrainTestAccess::make_authority(1);
    std::size_t stop_io_calls = 0;
    auto operations = basic_service_operations(authority, stop_io_calls);
    operations.stop_io = [&]() {
        ++stop_io_calls;
        io.stop();
    };
    operations.stop_engines = {
        test_engine_stop(
            io,
            [first_engine, &first_entered](
                xprv::RequestDrainReceipt const&) -> asio::awaitable<void> {
                boost::system::error_code error;
                co_await async_wait_started(
                    first_engine,
                    first_entered,
                    asio::redirect_error(asio::use_awaitable, error));
            }),
        test_engine_stop(
            io,
            [second_engine, &second_entered](
                xprv::RequestDrainReceipt const&) -> asio::awaitable<void> {
                boost::system::error_code error;
                co_await async_wait_started(
                    second_engine,
                    second_entered,
                    asio::redirect_error(asio::use_awaitable, error));
            }),
    };

    auto lifecycle = xprv::detail::ServiceLifecycle::create(
        io,
        xprv::detail::make_service_timer_control(status_timer),
        xprv::detail::make_service_timer_control(deadline_timer),
        std::move(operations),
        std::chrono::hours(1),
        std::chrono::hours(1));
    lifecycle->begin_shutdown();
    catl::test::IoContextRunners runners(io, GetParam());

    auto const first_ready =
        first_entered_future.wait_for(std::chrono::seconds(2)) ==
        std::future_status::ready;
    auto const second_ready =
        second_entered_future.wait_for(std::chrono::seconds(2)) ==
        std::future_status::ready;
    if (first_ready && second_ready)
    {
        std::thread first_release(
            [first_engine]() { (void)first_engine->cancel(); });
        std::thread second_release(
            [second_engine]() { (void)second_engine->cancel(); });
        first_release.join();
        second_release.join();
    }
    else
    {
        io.stop();
    }
    runners.join();

    ASSERT_TRUE(first_ready);
    ASSERT_TRUE(second_ready);
    auto const result = lifecycle->terminal_result();
    ASSERT_TRUE(result);
    EXPECT_EQ(result->outcome, Outcome::graceful);
    EXPECT_EQ(stop_io_calls, 1);
}

INSTANTIATE_TEST_SUITE_P(
    OneTwoAndFourRunners,
    ServiceRootRunnerModes,
    ::testing::Values(1u, 2u, 4u));

class ServiceRootConcurrentRunnerModes
    : public ::testing::TestWithParam<std::size_t>
{
};

TEST_P(
    ServiceRootConcurrentRunnerModes,
    TimerReceiptCannotOvertakeBoundHandlerReturn)
{
    namespace asio = boost::asio;
    using Outcome = xprv::detail::ServiceTerminalOutcome;

    asio::io_context io;
    ControlledServiceTimer status_timer;
    BlockingReturnTimer deadline_timer;
    auto authority = xprv::RequestDrainTestAccess::make_authority(1);
    std::size_t unused_stop_calls = 0;
    std::atomic<std::size_t> stop_io_calls{0};
    auto operations = basic_service_operations(authority, unused_stop_calls);
    operations.stop_io = [&]() {
        stop_io_calls.fetch_add(1, std::memory_order_acq_rel);
        io.stop();
    };

    auto lifecycle = xprv::detail::ServiceLifecycle::create(
        io,
        status_timer.control(),
        deadline_timer.control(),
        std::move(operations),
        std::chrono::hours(1),
        std::chrono::hours(1));
    lifecycle->begin_shutdown();
    catl::test::IoContextRunners runners(io, GetParam());

    auto const blocked =
        deadline_timer.wait_until_return_is_blocked(std::chrono::seconds(2)) ==
        std::future_status::ready;
    if (blocked)
    {
        EXPECT_FALSE(deadline_timer.raw_handler_returned());
        EXPECT_FALSE(lifecycle->terminal_result());
        EXPECT_EQ(stop_io_calls.load(std::memory_order_acquire), 0u);
    }

    deadline_timer.release_return();
    if (!blocked)
        io.stop();
    runners.join();

    ASSERT_TRUE(blocked);
    EXPECT_TRUE(deadline_timer.raw_handler_returned());
    auto const result = lifecycle->terminal_result();
    ASSERT_TRUE(result);
    EXPECT_EQ(result->outcome, Outcome::graceful);
    EXPECT_EQ(stop_io_calls.load(std::memory_order_acquire), 1u);
}

INSTANTIATE_TEST_SUITE_P(
    TwoAndFourRunners,
    ServiceRootConcurrentRunnerModes,
    ::testing::Values(2u, 4u));

TEST(RequestDrainReceipt, RefusesDifferentAdmissionAuthority)
{
    auto first = xprv::RequestDrainTestAccess::make_authority(1);
    auto second = xprv::RequestDrainTestAccess::make_authority(1);
    auto receipt = xprv::RequestDrainTestAccess::mint(first, 1);

    EXPECT_TRUE(xprv::RequestDrainTestAccess::matches(receipt, first));
    EXPECT_FALSE(xprv::RequestDrainTestAccess::matches(receipt, second));
}

TEST(RequestDrainReceipt, RefusesPriorLifecycleEpoch)
{
    auto authority = xprv::RequestDrainTestAccess::make_authority(7);
    auto receipt = xprv::RequestDrainTestAccess::mint(authority, 7);
    ASSERT_TRUE(xprv::RequestDrainTestAccess::matches(receipt, authority));

    xprv::RequestDrainTestAccess::advance(authority, 8);

    EXPECT_FALSE(xprv::RequestDrainTestAccess::matches(receipt, authority));
    auto current = xprv::RequestDrainTestAccess::mint(authority, 8);
    EXPECT_TRUE(xprv::RequestDrainTestAccess::matches(current, authority));
}

TEST(RequestDrainReceipt, RefusesUnstartedEpoch)
{
    auto authority = xprv::RequestDrainTestAccess::make_authority(0);
    auto receipt = xprv::RequestDrainTestAccess::mint(authority, 0);

    EXPECT_FALSE(xprv::RequestDrainTestAccess::matches(receipt, authority));
}

TEST(RequestDrainReceipt, EngineBindingRefusesSecondLiveAdmissionOwner)
{
    xprv::RequestDrainBinding binding;
    auto first = xprv::RequestDrainTestAccess::make_authority(1);
    auto second = xprv::RequestDrainTestAccess::make_authority(1);
    xprv::RequestDrainTestAccess::bind(binding, first);

    auto first_receipt = xprv::RequestDrainTestAccess::mint(first, 1);
    auto second_receipt = xprv::RequestDrainTestAccess::mint(second, 1);
    EXPECT_TRUE(xprv::RequestDrainTestAccess::accepts(binding, first_receipt));
    EXPECT_FALSE(
        xprv::RequestDrainTestAccess::accepts(binding, second_receipt));
    EXPECT_THROW(
        xprv::RequestDrainTestAccess::bind(binding, second), std::logic_error);
}

TEST(HttpChildCompletion, BodyExceptionStillPublishesJoinReceipt)
{
    namespace asio = boost::asio;

    catl::test::DeterministicScheduler scheduler;
    auto& io = scheduler.io();
    auto receipt = std::make_shared<xprv::detail::HttpChildCompletionChannel>(
        io.get_executor(), 1);
    boost::system::error_code received;
    bool joined = false;

    asio::co_spawn(
        io,
        []() -> asio::awaitable<void> {
            throw std::runtime_error("watcher body failpoint");
            co_return;
        },
        xprv::detail::HttpChildCompletion{receipt});
    asio::co_spawn(
        io,
        [receipt, &received, &joined]() -> asio::awaitable<void> {
            co_await receipt->async_receive(
                asio::redirect_error(asio::use_awaitable, received));
            joined = true;
        },
        asio::detached);

    scheduler.run_until_idle();

    EXPECT_TRUE(joined);
    EXPECT_FALSE(received);
}

TEST(HttpChildCompletion, ThrowingDiagnosticCannotRetractJoinReceipt)
{
    namespace asio = boost::asio;

    catl::test::DeterministicScheduler scheduler;
    auto& io = scheduler.io();
    auto receipt = std::make_shared<xprv::detail::HttpChildCompletionChannel>(
        io.get_executor(), 1);
    bool diagnostic_ran = false;
    bool joined = false;

    asio::co_spawn(
        io,
        []() -> asio::awaitable<void> {
            throw std::runtime_error("SSE child failpoint");
            co_return;
        },
        xprv::detail::HttpChildCompletion{
            receipt, [&diagnostic_ran](std::exception_ptr const&) {
                diagnostic_ran = true;
                throw std::runtime_error("diagnostic failpoint");
            }});
    asio::co_spawn(
        io,
        [receipt, &joined]() -> asio::awaitable<void> {
            co_await xprv::detail::join_http_child(receipt);
            joined = true;
        },
        asio::detached);

    scheduler.run_until_idle();

    EXPECT_TRUE(diagnostic_ran);
    EXPECT_TRUE(joined);
}

TEST(HttpChildCompletion, ParentCancellationCannotCounterfeitJoinReceipt)
{
    namespace asio = boost::asio;

    catl::test::DeterministicScheduler scheduler;
    auto& io = scheduler.io();
    asio::cancellation_signal cancel;
    auto receipt = std::make_shared<xprv::detail::HttpChildCompletionChannel>(
        io.get_executor(), 1);
    bool entered = false;
    bool joined = false;
    std::exception_ptr failure;

    asio::co_spawn(
        io,
        [receipt, &entered, &joined]() -> asio::awaitable<void> {
            entered = true;
            co_await xprv::detail::join_http_child(receipt);
            joined = true;
        },
        asio::bind_cancellation_slot(
            cancel.slot(), [&failure](std::exception_ptr result) {
                failure = std::move(result);
            }));

    scheduler.run_until_idle();
    ASSERT_TRUE(entered);
    ASSERT_FALSE(joined);

    cancel.emit(asio::cancellation_type::all);
    scheduler.run_until_idle();
    EXPECT_FALSE(joined);

    ASSERT_TRUE(receipt->try_send(boost::system::error_code{}));
    scheduler.run_until_idle();

    EXPECT_TRUE(joined);
    EXPECT_FALSE(failure);
}

TEST(HttpChildCompletion, BodyLocalDestructionPrecedesReceiptPublication)
{
    namespace asio = boost::asio;

    catl::test::DeterministicScheduler scheduler;
    auto& io = scheduler.io();
    auto receipt = std::make_shared<xprv::detail::HttpChildCompletionChannel>(
        io.get_executor(), 1);
    bool body_local_destroyed = false;
    bool completion_observed_destruction = false;
    bool joined = false;

    asio::co_spawn(
        io,
        [&body_local_destroyed]() -> asio::awaitable<void> {
            struct BodyLocalProbe
            {
                bool& destroyed;

                ~BodyLocalProbe()
                {
                    destroyed = true;
                }
            } probe{body_local_destroyed};

            co_return;
        },
        [receipt,
         &body_local_destroyed,
         &completion_observed_destruction](std::exception_ptr failure) {
            // This observation is deliberately inside the completion token
            // and before publication. A parent-only assertion after join
            // would not detect a receipt moved ahead of body-frame teardown.
            completion_observed_destruction = body_local_destroyed;
            xprv::detail::HttpChildCompletion{receipt}(std::move(failure));
        });
    asio::co_spawn(
        io,
        [receipt, &joined]() -> asio::awaitable<void> {
            co_await xprv::detail::join_http_child(receipt);
            joined = true;
        },
        asio::detached);

    scheduler.run_until_idle();

    EXPECT_TRUE(body_local_destroyed);
    EXPECT_TRUE(completion_observed_destruction);
    EXPECT_TRUE(joined);
}

TEST(
    HttpChildCompletionCounterexample,
    CopiedPublisherCanPublishAgainAfterFirstReceiptIsConsumed)
{
    namespace asio = boost::asio;

    catl::test::DeterministicScheduler scheduler;
    auto& io = scheduler.io();
    auto receipt = std::make_shared<xprv::detail::HttpChildCompletionChannel>(
        io.get_executor(), 1);
    xprv::detail::HttpChildCompletion first_publisher{receipt};
    auto copied_publisher = first_publisher;
    bool first_joined = false;
    bool second_joined = false;

    first_publisher({});
    asio::co_spawn(
        io,
        [receipt, &first_joined]() -> asio::awaitable<void> {
            co_await xprv::detail::join_http_child(receipt);
            first_joined = true;
        },
        asio::detached);
    scheduler.run_until_idle();
    ASSERT_TRUE(first_joined);

    // Rejected behavior evidence: capacity one is not one-publisher
    // authority. Once the first receipt is consumed, the copied publisher
    // can successfully fill the same channel again.
    copied_publisher({});
    asio::co_spawn(
        io,
        [receipt, &second_joined]() -> asio::awaitable<void> {
            co_await xprv::detail::join_http_child(receipt);
            second_joined = true;
        },
        asio::detached);
    scheduler.run_until_idle();

    EXPECT_TRUE(second_joined);
}

TEST(HttpTransportWatcher, ReadHalfCloseDoesNotCancelResponseWork)
{
    namespace asio = boost::asio;
    using tcp = asio::ip::tcp;

    asio::io_context io;
    tcp::acceptor acceptor(
        io, tcp::endpoint{asio::ip::make_address("127.0.0.1"), 0});
    tcp::socket client(io);
    client.connect(acceptor.local_endpoint());
    tcp::socket server = acceptor.accept();

    auto observed = asio::co_spawn(
        io,
        xprv::HttpServerTestAccess::wait_for_transport_failure(server),
        asio::use_future);

    boost::system::error_code shutdown_ec;
    client.shutdown(tcp::socket::shutdown_send, shutdown_ec);
    ASSERT_FALSE(shutdown_ec);
    ASSERT_TRUE(pump_until(io, [&] {
        return observed.wait_for(std::chrono::seconds(0)) ==
            std::future_status::ready;
    }));
    EXPECT_FALSE(observed.get())
        << "request-side EOF does not prove response abandonment";

    std::array<char, 2> const response{'o', 'k'};
    EXPECT_EQ(server.send(asio::buffer(response)), response.size());
    std::array<char, 2> received{};
    EXPECT_EQ(client.receive(asio::buffer(received)), received.size());
    EXPECT_EQ(received, response)
        << "a read-half-closed client still consumes the response";
}

TEST(
    HttpServerLifecycle,
    RegularProofFailureJoinsDisconnectWatcherBeforeResponse)
{
    namespace asio = boost::asio;
    namespace beast = boost::beast;
    namespace http = beast::http;
    using tcp = asio::ip::tcp;
    using Response = http::response<http::string_body>;

    asio::io_context io;
    auto engine =
        xprv::ProofEngine::create(io, xprv::NetworkConfig::for_network(0));
    auto server = xprv::HttpServer::create(
        io,
        engine,
        xprv::HttpServerOptions{
            .bind_address = "127.0.0.1",
            .port = 0,
        });

    auto watcher_gate = std::make_shared<asio::steady_timer>(
        io, asio::steady_timer::time_point::max());
    auto watcher_waiting = std::make_shared<std::atomic<bool>>(false);
    xprv::HttpServerTestAccess::set_regular_path_hooks(
        *server,
        [] { throw std::runtime_error("regular race failpoint"); },
        [watcher_gate, watcher_waiting]() -> asio::awaitable<void> {
            watcher_waiting->store(true, std::memory_order_release);
            boost::system::error_code ec;
            co_await watcher_gate->async_wait(
                asio::redirect_error(asio::use_awaitable, ec));
        });

    server->start();
    auto const port = xprv::HttpServerTestAccess::port(*server);
    auto response = asio::co_spawn(
        io,
        [port]() -> asio::awaitable<Response> {
            auto ex = co_await asio::this_coro::executor;
            tcp::socket socket(ex);
            co_await socket.async_connect(
                tcp::endpoint{asio::ip::make_address("127.0.0.1"), port},
                asio::use_awaitable);

            http::request<http::empty_body> request{
                http::verb::get,
                "/prove?tx="
                "00000000000000000000000000000000000000000000000000000000000000"
                "00",
                11};
            request.set(http::field::host, "127.0.0.1");
            request.keep_alive(false);
            co_await http::async_write(socket, request, asio::use_awaitable);

            beast::flat_buffer buffer;
            Response result;
            co_await http::async_read(
                socket, buffer, result, asio::use_awaitable);
            co_return result;
        },
        asio::use_future);

    ASSERT_TRUE(pump_until(
        io, [&] { return watcher_waiting->load(std::memory_order_acquire); }));

    // The proof race has already failed, but the regular HTTP frame may not
    // report that failure—or release its stream—until the watcher publishes
    // the exact child receipt.
    EXPECT_EQ(
        response.wait_for(std::chrono::seconds(0)),
        std::future_status::timeout);

    auto stopped = asio::co_spawn(
        io,
        [server]() -> asio::awaitable<void> {
            (void)co_await server->co_stop();
        },
        asio::use_future);
    io.restart();
    io.poll();
    EXPECT_EQ(
        stopped.wait_for(std::chrono::seconds(0)), std::future_status::timeout);

    watcher_gate->cancel();
    ASSERT_TRUE(pump_until(io, [&] {
        return response.wait_for(std::chrono::seconds(0)) ==
            std::future_status::ready &&
            stopped.wait_for(std::chrono::seconds(0)) ==
            std::future_status::ready;
    }));
    try
    {
        auto result = response.get();
        EXPECT_EQ(result.result(), http::status::internal_server_error);
    }
    catch (boost::system::system_error const& error)
    {
        // co_stop() may close the client transport while the server frame
        // remains alive waiting for the exact watcher receipt. The client
        // therefore observes either the prepared 500 or a shutdown terminal;
        // neither is a service-drain receipt.
        EXPECT_TRUE(
            error.code() == http::error::end_of_stream ||
            error.code() == asio::error::eof ||
            error.code() == asio::error::operation_aborted ||
            error.code() == asio::error::connection_reset)
            << error.code().message();
    }
    EXPECT_NO_THROW(stopped.get());
}

TEST(HttpServerLifecycle, OneShotStartSurvivesSecondStartRefusal)
{
    namespace asio = boost::asio;

    asio::io_context io;
    auto engine =
        xprv::ProofEngine::create(io, xprv::NetworkConfig::for_network(0));
    auto server = xprv::HttpServer::create(
        io,
        engine,
        xprv::HttpServerOptions{
            .bind_address = "127.0.0.1",
            .port = 0,
        });

    ASSERT_NO_THROW(server->start());
    EXPECT_THROW(server->start(), std::logic_error);

    auto stopped = asio::co_spawn(
        io,
        [server, engine]() -> asio::awaitable<void> {
            auto receipt = co_await server->co_stop();
            (void)co_await engine->co_stop(receipt);
        },
        asio::use_future);
    io.run();

    EXPECT_NO_THROW(stopped.get());
    EXPECT_THROW(server->start(), std::logic_error);
}

TEST(HttpServerLifecycle, EngineDrainAwaitsValidationQuorumTrackerOwnerReceipt)
{
    namespace asio = boost::asio;
    using namespace std::chrono_literals;

    for (auto const runner_count : {2u, 4u})
    {
        SCOPED_TRACE(testing::Message() << "runners=" << runner_count);

        asio::io_context io;
        auto work = asio::make_work_guard(io);
        auto engine =
            xprv::ProofEngine::create(io, xprv::NetworkConfig::for_network(0));
        auto protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
        auto validation_quorum_tracker =
            xprv::ValidationQuorumTracker::create(io, protocol, 0);
        xprv::ProofEngineTestAccess::set_validation_quorum_tracker(
            *engine, validation_quorum_tracker);
        auto server = xprv::HttpServer::create(
            io,
            engine,
            xprv::HttpServerOptions{
                .bind_address = "127.0.0.1",
                .port = 0,
            });
        server->start();

        std::promise<void> owner_entered;
        std::promise<void> release_owner;
        auto release_owner_future = release_owner.get_future().share();
        asio::post(validation_quorum_tracker->owner_strand_for_test(), [&]() {
            owner_entered.set_value();
            release_owner_future.wait();
        });

        std::promise<void> engine_stop_entered;
        auto drained = asio::co_spawn(
            io,
            [server, engine, &engine_stop_entered]() -> asio::awaitable<void> {
                auto receipt = co_await server->co_stop();
                engine_stop_entered.set_value();
                (void)co_await engine->co_stop(receipt);
            },
            asio::use_future);

        catl::test::IoContextRunners runners(io, runner_count);
        auto const owner_status = owner_entered.get_future().wait_for(5s);
        auto const engine_status =
            engine_stop_entered.get_future().wait_for(5s);
        EXPECT_EQ(owner_status, std::future_status::ready);
        EXPECT_EQ(engine_status, std::future_status::ready);

        if (owner_status == std::future_status::ready &&
            engine_status == std::future_status::ready)
        {
            EXPECT_EQ(drained.wait_for(50ms), std::future_status::timeout);
        }

        release_owner.set_value();
        auto const drained_status = drained.wait_for(5s);
        EXPECT_EQ(drained_status, std::future_status::ready);
        if (drained_status == std::future_status::ready)
            EXPECT_NO_THROW(drained.get());

        work.reset();
        runners.join();
    }
}

TEST(HttpServerLifecycle, EngineDrainFenceAwaitsValidationQuorumTrackerPreSeal)
{
    namespace asio = boost::asio;
    using namespace std::chrono_literals;

    for (auto const runner_count : {2u, 4u})
    {
        SCOPED_TRACE(testing::Message() << "runners=" << runner_count);

        asio::io_context io;
        auto work = asio::make_work_guard(io);
        auto engine =
            xprv::ProofEngine::create(io, xprv::NetworkConfig::for_network(0));
        auto protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
        auto validation_quorum_tracker =
            xprv::ValidationQuorumTracker::create(io, protocol, 0);
        xprv::ProofEngineTestAccess::set_validation_quorum_tracker(
            *engine, validation_quorum_tracker);
        auto server = xprv::HttpServer::create(
            io,
            engine,
            xprv::HttpServerOptions{
                .bind_address = "127.0.0.1",
                .port = 0,
            });
        server->start();

        std::promise<void> publication_entered;
        auto entered = publication_entered.get_future();
        std::promise<void> release_publication;
        auto release = release_publication.get_future().share();
        std::atomic<unsigned> effects{0};
        validation_quorum_tracker->set_inside_heartbeat_publication_hook_for_test(
            [&]() {
                publication_entered.set_value();
                release.wait();
            });
        validation_quorum_tracker->set_heartbeat_effect_hook_for_test(
            [&]() { effects.fetch_add(1, std::memory_order_relaxed); });

        std::promise<void> drain_fence;
        auto fenced = drain_fence.get_future();
        xprv::ProofEngineTestAccess::set_before_engine_draining_log(
            *engine, [&]() { drain_fence.set_value(); });

        validation_quorum_tracker->post_heartbeat_tick_for_test();
        auto drained = asio::co_spawn(
            io,
            [server, engine]() -> asio::awaitable<void> {
                auto receipt = co_await server->co_stop();
                (void)co_await engine->co_stop(receipt);
            },
            asio::use_future);

        catl::test::IoContextRunners runners(io, runner_count);
        auto const entered_status = entered.wait_for(5s);
        auto const fence_while_parked = fenced.wait_for(50ms);

        release_publication.set_value();
        auto const fence_status = fenced.wait_for(5s);
        auto const drained_status = drained.wait_for(5s);
        if (drained_status == std::future_status::ready)
            EXPECT_NO_THROW(drained.get());

        work.reset();
        io.stop();
        runners.join();

        EXPECT_EQ(entered_status, std::future_status::ready);
        EXPECT_EQ(fence_while_parked, std::future_status::timeout);
        EXPECT_EQ(fence_status, std::future_status::ready);
        EXPECT_EQ(drained_status, std::future_status::ready);
        EXPECT_EQ(effects.load(std::memory_order_relaxed), 1u);
    }
}

TEST(HttpServerLifecycle, EngineDrainAwaitsNodeCacheStopDriverReceipt)
{
    namespace asio = boost::asio;
    using namespace std::chrono_literals;

    for (auto const runner_count : {2u, 4u})
    {
        SCOPED_TRACE(testing::Message() << "runners=" << runner_count);

        asio::io_context io;
        auto work = asio::make_work_guard(io);
        auto engine =
            xprv::ProofEngine::create(io, xprv::NetworkConfig::for_network(0));
        auto node_cache = catl::peer_client::NodeCache::create(
            io, catl::peer_client::NodeCache::Options{});
        xprv::ProofEngineTestAccess::set_node_cache(*engine, node_cache);
        auto server = xprv::HttpServer::create(
            io,
            engine,
            xprv::HttpServerOptions{
                .bind_address = "127.0.0.1",
                .port = 0,
            });
        server->start();

        std::promise<void> stop_driver_entered;
        std::promise<void> release_stop_driver;
        auto release_stop_driver_future =
            release_stop_driver.get_future().share();
        node_cache->set_before_node_stop_driver_launch_for_test([&]() {
            stop_driver_entered.set_value();
            release_stop_driver_future.wait();
        });

        auto drained = asio::co_spawn(
            io,
            [server, engine]() -> asio::awaitable<void> {
                auto receipt = co_await server->co_stop();
                (void)co_await engine->co_stop(receipt);
            },
            asio::use_future);

        catl::test::IoContextRunners runners(io, runner_count);
        auto const entered_status =
            stop_driver_entered.get_future().wait_for(5s);
        EXPECT_EQ(entered_status, std::future_status::ready);
        if (entered_status == std::future_status::ready)
            EXPECT_EQ(drained.wait_for(50ms), std::future_status::timeout);

        release_stop_driver.set_value();
        auto const drained_status = drained.wait_for(5s);
        EXPECT_EQ(drained_status, std::future_status::ready);
        if (drained_status == std::future_status::ready)
            EXPECT_NO_THROW(drained.get());
        else
            io.stop();

        work.reset();
        runners.join();
    }
}

TEST(HttpServerLifecycle, EngineDrainAwaitsNodeCacheHeaderClearReceipt)
{
    namespace asio = boost::asio;
    using namespace std::chrono_literals;

    for (auto const runner_count : {2u, 4u})
    {
        SCOPED_TRACE(testing::Message() << "runners=" << runner_count);

        asio::io_context io;
        auto work = asio::make_work_guard(io);
        auto engine =
            xprv::ProofEngine::create(io, xprv::NetworkConfig::for_network(0));
        auto node_cache = catl::peer_client::NodeCache::create(
            io, catl::peer_client::NodeCache::Options{});
        xprv::ProofEngineTestAccess::set_node_cache(*engine, node_cache);
        auto server = xprv::HttpServer::create(
            io,
            engine,
            xprv::HttpServerOptions{
                .bind_address = "127.0.0.1",
                .port = 0,
            });
        server->start();

        std::promise<void> clear_driver_entered;
        std::promise<void> release_clear_driver;
        auto release_clear_driver_future =
            release_clear_driver.get_future().share();
        node_cache->set_before_header_clear_driver_launch_for_test([&]() {
            clear_driver_entered.set_value();
            release_clear_driver_future.wait();
        });

        auto drained = asio::co_spawn(
            io,
            [server, engine]() -> asio::awaitable<void> {
                auto receipt = co_await server->co_stop();
                (void)co_await engine->co_stop(receipt);
            },
            asio::use_future);

        catl::test::IoContextRunners runners(io, runner_count);
        auto const entered_status =
            clear_driver_entered.get_future().wait_for(5s);
        EXPECT_EQ(entered_status, std::future_status::ready);
        if (entered_status == std::future_status::ready)
            EXPECT_EQ(drained.wait_for(50ms), std::future_status::timeout);

        release_clear_driver.set_value();
        auto const drained_status = drained.wait_for(5s);
        EXPECT_EQ(drained_status, std::future_status::ready);
        if (drained_status == std::future_status::ready)
            EXPECT_NO_THROW(drained.get());
        else
            io.stop();

        work.reset();
        runners.join();
    }
}

TEST(HttpServerLifecycle, EngineDrainAwaitsCoordinatorFenceReceipt)
{
    namespace asio = boost::asio;
    using namespace std::chrono_literals;

    for (auto const runner_count : {2u, 4u})
    {
        SCOPED_TRACE(testing::Message() << "runners=" << runner_count);

        asio::io_context io;
        auto work = asio::make_work_guard(io);
        auto engine =
            xprv::ProofEngine::create(io, xprv::NetworkConfig::for_network(0));
        auto peers = catl::peer_client::PeerSet::create(
            io, catl::peer_client::PeerSetOptions{});
        auto coordinator =
            catl::peer_client::LedgerFetchCoordinator::create(io, peers);
        xprv::ProofEngineTestAccess::set_peer_set(*engine, peers);
        xprv::ProofEngineTestAccess::set_ledger_fetch_coordinator(
            *engine, coordinator);
        auto server = xprv::HttpServer::create(
            io,
            engine,
            xprv::HttpServerOptions{
                .bind_address = "127.0.0.1",
                .port = 0,
            });
        server->start();

        std::promise<void> fence_commit_entered;
        std::promise<void> release_fence_commit;
        auto release_fence_commit_future =
            release_fence_commit.get_future().share();
        LedgerFetchCoordinatorTestAccess::set_before_fence_terminal_commit(
            coordinator, [&]() {
                fence_commit_entered.set_value();
                release_fence_commit_future.wait();
            });

        auto drained = asio::co_spawn(
            io,
            [server, engine]() -> asio::awaitable<void> {
                auto receipt = co_await server->co_stop();
                (void)co_await engine->co_stop(receipt);
            },
            asio::use_future);

        catl::test::IoContextRunners runners(io, runner_count);
        auto const entered_status =
            fence_commit_entered.get_future().wait_for(5s);
        EXPECT_EQ(entered_status, std::future_status::ready);
        if (entered_status == std::future_status::ready)
            EXPECT_EQ(drained.wait_for(50ms), std::future_status::timeout);

        release_fence_commit.set_value();
        auto const drained_status = drained.wait_for(5s);
        EXPECT_EQ(drained_status, std::future_status::ready);
        if (drained_status == std::future_status::ready)
            EXPECT_NO_THROW(drained.get());
        else
            io.stop();

        work.reset();
        runners.join();
    }
}

TEST(HttpServerLifecycle, EngineDrainAwaitsBothPeerSetPinReleaseBarriers)
{
    namespace asio = boost::asio;
    using namespace std::chrono_literals;

    for (auto const held_barrier : {1u, 2u})
    {
        for (auto const runner_count : {2u, 4u})
        {
            SCOPED_TRACE(
                testing::Message()
                << "held_barrier=" << held_barrier
                << " runners=" << runner_count);

            asio::io_context io;
            auto work = asio::make_work_guard(io);
            auto engine = xprv::ProofEngine::create(
                io, xprv::NetworkConfig::for_network(0));
            auto peers = catl::peer_client::PeerSet::create(
                io, catl::peer_client::PeerSetOptions{});
            auto coordinator =
                catl::peer_client::LedgerFetchCoordinator::create(io, peers);
            xprv::ProofEngineTestAccess::set_peer_set(*engine, peers);
            xprv::ProofEngineTestAccess::set_ledger_fetch_coordinator(
                *engine, coordinator);
            auto server = xprv::HttpServer::create(
                io,
                engine,
                xprv::HttpServerOptions{
                    .bind_address = "127.0.0.1",
                    .port = 0,
                });
            server->start();

            std::atomic<unsigned> barriers_seen{0};
            std::promise<void> held_barrier_entered;
            std::promise<void> release_held_barrier;
            auto release_held_barrier_future =
                release_held_barrier.get_future().share();
            PeerSetTestAccess::
                set_before_generation_pin_release_barrier_return(
                    peers, [&]() {
                        auto const ordinal = ++barriers_seen;
                        if (ordinal != held_barrier)
                            return;
                        held_barrier_entered.set_value();
                        release_held_barrier_future.wait();
                    });

            auto drained = asio::co_spawn(
                io,
                [server, engine]() -> asio::awaitable<void> {
                    auto receipt = co_await server->co_stop();
                    (void)co_await engine->co_stop(receipt);
                },
                asio::use_future);

            catl::test::IoContextRunners runners(io, runner_count);
            auto const entered_status =
                held_barrier_entered.get_future().wait_for(5s);
            EXPECT_EQ(entered_status, std::future_status::ready);
            if (entered_status == std::future_status::ready)
                EXPECT_EQ(drained.wait_for(50ms), std::future_status::timeout);

            release_held_barrier.set_value();
            auto const drained_status = drained.wait_for(5s);
            EXPECT_EQ(drained_status, std::future_status::ready);
            if (drained_status == std::future_status::ready)
            {
                EXPECT_NO_THROW(drained.get());
                EXPECT_GE(barriers_seen.load(), 2u);
            }
            else
            {
                io.stop();
            }

            work.reset();
            runners.join();
        }
    }
}

TEST(
    HttpServerLifecycle,
    EngineDrainAwaitsCoordinatorProjectionReconciliationReceipt)
{
    namespace asio = boost::asio;
    using namespace std::chrono_literals;

    for (auto const runner_count : {2u, 4u})
    {
        SCOPED_TRACE(testing::Message() << "runners=" << runner_count);

        asio::io_context io;
        auto work = asio::make_work_guard(io);
        auto engine =
            xprv::ProofEngine::create(io, xprv::NetworkConfig::for_network(0));
        auto peers = catl::peer_client::PeerSet::create(
            io, catl::peer_client::PeerSetOptions{});
        auto coordinator =
            catl::peer_client::LedgerFetchCoordinator::create(io, peers);
        xprv::ProofEngineTestAccess::set_peer_set(*engine, peers);
        xprv::ProofEngineTestAccess::set_ledger_fetch_coordinator(
            *engine, coordinator);
        auto server = xprv::HttpServer::create(
            io,
            engine,
            xprv::HttpServerOptions{
                .bind_address = "127.0.0.1",
                .port = 0,
            });
        server->start();

        std::promise<void> reconciliation_commit_entered;
        std::promise<void> release_reconciliation_commit;
        auto release_reconciliation_commit_future =
            release_reconciliation_commit.get_future().share();
        LedgerFetchCoordinatorTestAccess::
            set_before_shutdown_reconciliation_commit(coordinator, [&]() {
                reconciliation_commit_entered.set_value();
                release_reconciliation_commit_future.wait();
            });

        auto drained = asio::co_spawn(
            io,
            [server, engine]() -> asio::awaitable<void> {
                auto receipt = co_await server->co_stop();
                (void)co_await engine->co_stop(receipt);
            },
            asio::use_future);

        catl::test::IoContextRunners runners(io, runner_count);
        auto const entered_status =
            reconciliation_commit_entered.get_future().wait_for(5s);
        EXPECT_EQ(entered_status, std::future_status::ready);
        if (entered_status == std::future_status::ready)
            EXPECT_EQ(drained.wait_for(50ms), std::future_status::timeout);

        release_reconciliation_commit.set_value();
        auto const drained_status = drained.wait_for(5s);
        EXPECT_EQ(drained_status, std::future_status::ready);
        if (drained_status == std::future_status::ready)
            EXPECT_NO_THROW(drained.get());
        else
            io.stop();

        work.reset();
        runners.join();
    }
}

TEST(HttpServerLifecycle, EngineDrainAwaitsPeerSetAggregateStopReceipt)
{
    namespace asio = boost::asio;
    using namespace std::chrono_literals;

    for (auto const runner_count : {2u, 4u})
    {
        SCOPED_TRACE(testing::Message() << "runners=" << runner_count);

        asio::io_context io;
        auto work = asio::make_work_guard(io);
        auto engine =
            xprv::ProofEngine::create(io, xprv::NetworkConfig::for_network(0));
        auto peers = catl::peer_client::PeerSet::create(
            io, catl::peer_client::PeerSetOptions{});
        auto coordinator =
            catl::peer_client::LedgerFetchCoordinator::create(io, peers);
        xprv::ProofEngineTestAccess::set_peer_set(*engine, peers);
        xprv::ProofEngineTestAccess::set_ledger_fetch_coordinator(
            *engine, coordinator);
        auto server = xprv::HttpServer::create(
            io,
            engine,
            xprv::HttpServerOptions{
                .bind_address = "127.0.0.1",
                .port = 0,
            });
        server->start();

        std::promise<void> stop_receipt_held;
        std::promise<void> release_stop_receipt;
        auto release_stop_receipt_future =
            release_stop_receipt.get_future().share();
        PeerSetTestAccess::set_before_stop_receipt_return(peers, [&]() {
            stop_receipt_held.set_value();
            release_stop_receipt_future.wait();
        });

        auto drained = asio::co_spawn(
            io,
            [server, engine]() -> asio::awaitable<void> {
                auto receipt = co_await server->co_stop();
                (void)co_await engine->co_stop(receipt);
            },
            asio::use_future);

        catl::test::IoContextRunners runners(io, runner_count);
        auto const held_status = stop_receipt_held.get_future().wait_for(5s);
        EXPECT_EQ(held_status, std::future_status::ready);
        if (held_status == std::future_status::ready)
            EXPECT_EQ(drained.wait_for(50ms), std::future_status::timeout);

        release_stop_receipt.set_value();
        auto const drained_status = drained.wait_for(5s);
        EXPECT_EQ(drained_status, std::future_status::ready);
        if (drained_status == std::future_status::ready)
            EXPECT_NO_THROW(drained.get());
        else
            io.stop();

        work.reset();
        runners.join();
    }
}

TEST(
    HttpServerLifecycle,
    ServiceTerminalAwaitsExactProofEngineDrainReceipt)
{
    namespace asio = boost::asio;
    using namespace std::chrono_literals;
    using Outcome = xprv::detail::ServiceTerminalOutcome;

    for (auto const runner_count : {2u, 4u})
    {
        SCOPED_TRACE(testing::Message() << "runners=" << runner_count);

        asio::io_context io;
        auto work = asio::make_work_guard(io);
        auto engine =
            xprv::ProofEngine::create(io, xprv::NetworkConfig::for_network(0));
        auto server = xprv::HttpServer::create(
            io,
            engine,
            xprv::HttpServerOptions{
                .bind_address = "127.0.0.1",
                .port = 0,
            });
        server->start();

        std::promise<void> receipt_publish_entered;
        std::promise<void> release_receipt_publish;
        auto release_receipt_publish_future =
            release_receipt_publish.get_future().share();
        xprv::ProofEngineTestAccess::set_before_drain_receipt_publish(
            *engine, [&]() {
                receipt_publish_entered.set_value();
                release_receipt_publish_future.wait();
            });

        auto status_timer = std::make_shared<asio::steady_timer>(io);
        auto deadline_timer = std::make_shared<asio::steady_timer>(io);
        std::promise<void> stop_io_called;
        std::atomic<std::size_t> terminal_diagnostic_calls{0};
        std::atomic<std::size_t> stop_io_calls{0};
        xprv::detail::ServiceLifecycleOperations operations;
        operations.report_status = []() -> asio::awaitable<void> { co_return; };
        operations.stop_http =
            [server]() -> asio::awaitable<xprv::RequestDrainReceipt> {
            co_return co_await server->co_stop();
        };
        operations.stop_engines.emplace_back(
            xprv::detail::EngineDrainOperation::for_engine(engine));
        operations.terminal_diagnostic =
            [&](xprv::detail::ServiceTerminalResult const&) {
                terminal_diagnostic_calls.fetch_add(
                    1, std::memory_order_relaxed);
            };
        operations.stop_io = [&]() {
            if (stop_io_calls.fetch_add(1, std::memory_order_relaxed) == 0)
                stop_io_called.set_value();
        };

        auto lifecycle = xprv::detail::ServiceLifecycle::create(
            io,
            xprv::detail::make_service_timer_control(status_timer),
            xprv::detail::make_service_timer_control(deadline_timer),
            std::move(operations),
            1h,
            1h);
        lifecycle->begin_shutdown();

        catl::test::IoContextRunners runners(io, runner_count);
        auto const entered_status =
            receipt_publish_entered.get_future().wait_for(5s);
        EXPECT_EQ(entered_status, std::future_status::ready);
        if (entered_status == std::future_status::ready)
        {
            EXPECT_FALSE(lifecycle->terminal_result());
            EXPECT_EQ(stop_io_calls.load(std::memory_order_relaxed), 0u);
        }

        release_receipt_publish.set_value();
        auto const stop_io_status = stop_io_called.get_future().wait_for(5s);
        EXPECT_EQ(stop_io_status, std::future_status::ready);
        if (stop_io_status == std::future_status::ready)
        {
            auto const terminal = lifecycle->terminal_result();
            ASSERT_TRUE(terminal);
            EXPECT_EQ(terminal->outcome, Outcome::graceful);
            EXPECT_EQ(
                terminal_diagnostic_calls.load(std::memory_order_relaxed), 1u);
            EXPECT_EQ(stop_io_calls.load(std::memory_order_relaxed), 1u);
        }
        else
        {
            io.stop();
        }

        work.reset();
        runners.join();
    }
}

TEST(HttpServerLifecycle, FailedStartCannotMintOrRetryLifecycle)
{
    namespace asio = boost::asio;

    asio::io_context io;
    auto engine =
        xprv::ProofEngine::create(io, xprv::NetworkConfig::for_network(0));
    auto server = xprv::HttpServer::create(
        io,
        engine,
        xprv::HttpServerOptions{
            .bind_address = "not-an-ip-address",
            .port = 0,
        });

    EXPECT_THROW(server->start(), boost::system::system_error);
    EXPECT_THROW(server->start(), std::logic_error);

    auto stopped = asio::co_spawn(
        io,
        [server]() -> asio::awaitable<void> { co_await server->co_stop(); },
        asio::use_future);
    io.run();
    EXPECT_THROW(stopped.get(), std::logic_error);
}

}  // namespace
