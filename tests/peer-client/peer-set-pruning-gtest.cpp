#include <algorithm>
#include <catl/peer-client/peer-set.h>
#include "peer-set-test-access.h"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <exception>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using namespace catl::peer_client;
namespace asio = boost::asio;

namespace {

bool
contains_endpoint(
    std::vector<std::string> const& endpoints,
    std::string const& endpoint)
{
    return std::find(endpoints.begin(), endpoints.end(), endpoint) !=
        endpoints.end();
}

}  // namespace

TEST(PeerSetPruning, WaitSignalWakesOnDiscoveryAcrossThreads)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{
        .network_id = 0,
        .max_in_flight_connects = 0,
        .max_in_flight_crawls = 0,
    });
    auto signal = PeerSetTestAccess::attach_wait_signal(io, peers);

    bool done = false;
    bool woke_on_signal = false;

    asio::co_spawn(
        io,
        [&, signal]() -> asio::awaitable<void> {
            auto ex = co_await asio::this_coro::executor;
            auto result =
                co_await asio::experimental::make_parallel_group(
                    asio::co_spawn(
                        ex,
                        [signal]() -> asio::awaitable<void> {
                            boost::system::error_code ec;
                            co_await signal->async_wait(
                                asio::redirect_error(asio::use_awaitable, ec));
                        },
                        asio::deferred),
                    asio::co_spawn(
                        ex,
                        []() -> asio::awaitable<void> {
                            auto ex = co_await asio::this_coro::executor;
                            asio::steady_timer timer(
                                ex, std::chrono::seconds(5));
                            boost::system::error_code ec;
                            co_await timer.async_wait(
                                asio::redirect_error(asio::use_awaitable, ec));
                        },
                        asio::deferred))
                    .async_wait(
                        asio::experimental::wait_for_one(),
                        asio::use_awaitable);

            woke_on_signal = (std::get<0>(result)[0] == 0);
            done = true;
        },
        asio::detached);

    asio::steady_timer trigger(io, std::chrono::milliseconds(10));
    trigger.async_wait([peers](boost::system::error_code const&) {
        PeerSetTestAccess::post_note_discovered(
            peers, "wake.example.com:51235");
    });

    std::thread background([&]() { io.run(); });
    io.run();
    background.join();

    EXPECT_TRUE(done);
    EXPECT_TRUE(woke_on_signal);
}

TEST(PeerSetPruning, WaitSignalWakesOnStatusUpdate)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{
        .network_id = 0,
        .max_in_flight_connects = 0,
        .max_in_flight_crawls = 0,
    });
    auto signal = PeerSetTestAccess::attach_wait_signal(io, peers);

    bool done = false;
    bool woke_on_signal = false;

    asio::co_spawn(
        io,
        [&, signal]() -> asio::awaitable<void> {
            auto ex = co_await asio::this_coro::executor;
            auto result =
                co_await asio::experimental::make_parallel_group(
                    asio::co_spawn(
                        ex,
                        [signal]() -> asio::awaitable<void> {
                            boost::system::error_code ec;
                            co_await signal->async_wait(
                                asio::redirect_error(asio::use_awaitable, ec));
                        },
                        asio::deferred),
                    asio::co_spawn(
                        ex,
                        []() -> asio::awaitable<void> {
                            auto ex = co_await asio::this_coro::executor;
                            asio::steady_timer timer(
                                ex, std::chrono::seconds(5));
                            boost::system::error_code ec;
                            co_await timer.async_wait(
                                asio::redirect_error(asio::use_awaitable, ec));
                        },
                        asio::deferred))
                    .async_wait(
                        asio::experimental::wait_for_one(),
                        asio::use_awaitable);

            woke_on_signal = (std::get<0>(result)[0] == 0);
            done = true;
        },
        asio::detached);

    asio::steady_timer trigger(io, std::chrono::milliseconds(10));
    trigger.async_wait([peers](boost::system::error_code const&) {
        PeerSetTestAccess::post_note_status(
            peers,
            "status.example.com:51235",
            PeerStatus{
                .first_seq = 100,
                .last_seq = 200,
                .current_seq = 150,
            });
    });

    io.run();

    EXPECT_TRUE(done);
    EXPECT_TRUE(woke_on_signal);
}

TEST(PeerSetPruning, RemovesExpiredAndOrphanedDiscoveryState)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{
        .network_id = 0,
        .max_in_flight_connects = 0,
        .max_in_flight_crawls = 0,
    });

    auto const now = std::chrono::steady_clock::now();

    PeerSetTestAccess::add_discovered(io, peers, "tracked.example.com:51235");
    PeerSetTestAccess::add_discovered(io, peers, "tracked-fail.example.com:51235");

    PeerSetTestAccess::set_crawled_at(
        io, peers, "tracked.example.com:51235", now);
    PeerSetTestAccess::set_crawled_at(
        io,
        peers,
        "expired.example.com:51235",
        now - std::chrono::minutes(31));
    PeerSetTestAccess::set_crawled_at(
        io, peers, "orphan.example.com:51235", now);

    PeerSetTestAccess::set_failed_at(
        io, peers, "tracked-fail.example.com:51235", now);
    PeerSetTestAccess::set_failed_at(
        io,
        peers,
        "expired-fail.example.com:51235",
        now - std::chrono::minutes(31));
    PeerSetTestAccess::set_failed_at(
        io, peers, "orphan-fail.example.com:51235", now);

    PeerSetTestAccess::prune_discovery_state(io, peers);

    EXPECT_TRUE(
        PeerSetTestAccess::has_crawled(io, peers, "tracked.example.com:51235"));
    EXPECT_FALSE(
        PeerSetTestAccess::has_crawled(io, peers, "expired.example.com:51235"));
    EXPECT_FALSE(
        PeerSetTestAccess::has_crawled(io, peers, "orphan.example.com:51235"));

    EXPECT_TRUE(PeerSetTestAccess::has_failed(
        io, peers, "tracked-fail.example.com:51235"));
    EXPECT_FALSE(PeerSetTestAccess::has_failed(
        io, peers, "expired-fail.example.com:51235"));
    EXPECT_FALSE(PeerSetTestAccess::has_failed(
        io, peers, "orphan-fail.example.com:51235"));
}

TEST(PeerSetPruning, CapsTrackerAndDeduplicatesCanonicalEndpoints)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{
        .network_id = 0,
        .max_in_flight_connects = 0,
        .max_in_flight_crawls = 0,
    });

    auto const active = "active.example.com:51235";
    PeerSetTestAccess::add_discovered(io, peers, active);
    PeerSetTestAccess::mark_queued(io, peers, active);

    std::vector<std::string> endpoints_to_add = {
        "1.2.3.4:51235",
        "::ffff:1.2.3.4:51235",
    };
    endpoints_to_add.reserve(PeerSetTestAccess::max_tracked_endpoints + 34);
    for (std::size_t i = 0; i < PeerSetTestAccess::max_tracked_endpoints + 32; ++i)
    {
        endpoints_to_add.push_back(
            "peer-" + std::to_string(i) + ".example.com:51235");
    }
    PeerSetTestAccess::add_discovered_many(io, peers, std::move(endpoints_to_add));

    auto const initial_size = peers->tracker()->size();
    ASSERT_GT(initial_size, PeerSetTestAccess::max_tracked_endpoints);

    PeerSetTestAccess::prune_discovery_state(io, peers);

    auto const endpoints = peers->tracker()->all_endpoints();
    EXPECT_LE(
        endpoints.size(), PeerSetTestAccess::max_tracked_endpoints + 1);
    EXPECT_TRUE(contains_endpoint(endpoints, active));
    EXPECT_TRUE(
        contains_endpoint(endpoints, "1.2.3.4:51235") ||
        contains_endpoint(endpoints, "::ffff:1.2.3.4:51235"));
    EXPECT_FALSE(
        contains_endpoint(endpoints, "1.2.3.4:51235") &&
        contains_endpoint(endpoints, "::ffff:1.2.3.4:51235"));
}

TEST(PeerSetPruning, TimedOutWaitForPeerClearsWantedLedger)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{
        .network_id = 0,
        .max_in_flight_connects = 0,
        .max_in_flight_crawls = 0,
    });

    bool done = false;
    std::string error;
    constexpr uint32_t wanted_ledger = 123456u;

    asio::co_spawn(
        io,
        [&, peers]() -> asio::awaitable<void> {
            try
            {
                auto client = co_await peers->wait_for_peer(wanted_ledger, 0);
                EXPECT_EQ(client, nullptr);
            }
            catch (std::exception const& e)
            {
                error = e.what();
            }
            done = true;
        },
        asio::detached);

    for (int i = 0; i < 4 && !done; ++i)
    {
        io.poll();
        io.restart();
    }

    EXPECT_TRUE(done);
    EXPECT_TRUE(error.empty()) << error;
    EXPECT_FALSE(PeerSetTestAccess::has_wanted_ledger(
        io, peers, wanted_ledger));
}

TEST(PeerSetPruning, CancelledWaitForPeerClearsWantedLedger)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{
        .network_id = 0,
        .max_in_flight_connects = 0,
        .max_in_flight_crawls = 0,
    });

    bool done = false;
    std::string error;
    constexpr uint32_t wanted_ledger = 654321u;
    auto cancel = std::make_shared<std::atomic<bool>>(false);
    auto const started = std::chrono::steady_clock::now();

    asio::co_spawn(
        io,
        [&, peers, cancel]() -> asio::awaitable<void> {
            try
            {
                auto client =
                    co_await peers->wait_for_peer(wanted_ledger, 30, cancel);
                EXPECT_EQ(client, nullptr);
            }
            catch (std::exception const& e)
            {
                error = e.what();
            }
            done = true;
        },
        asio::detached);

    asio::steady_timer cancel_timer(io, std::chrono::milliseconds(10));
    cancel_timer.async_wait([cancel](boost::system::error_code const&) {
        cancel->store(true, std::memory_order_relaxed);
    });

    io.run();

    auto const elapsed = std::chrono::steady_clock::now() - started;
    EXPECT_TRUE(done);
    EXPECT_TRUE(error.empty()) << error;
    EXPECT_LT(elapsed, std::chrono::seconds(1));
    io.restart();
    EXPECT_FALSE(PeerSetTestAccess::has_wanted_ledger(
        io, peers, wanted_ledger));
}
