#include <xprv/http-server.h>
#include <xprv/proof-engine.h>

#include "../test-support/deterministic-scheduler.h"
#include "http-child-completion.h"

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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>

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
};

}  // namespace xprv

namespace {

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
static_assert(!CanStopWithoutRequestDrain<xprv::ProofEngine>);
static_assert(CanStopWithRequestDrain<xprv::ProofEngine>);

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
            co_await engine->co_stop(receipt);
        },
        asio::use_future);
    io.run();

    EXPECT_NO_THROW(stopped.get());
    EXPECT_THROW(server->start(), std::logic_error);
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
