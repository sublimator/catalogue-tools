#include "peer-set-test-access.h"
#include <algorithm>
#include <catl/peer-client/peer-set.h>

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

PeerSet::SnapshotEntry const*
find_snapshot_entry(
    PeerSet::Snapshot const& snapshot,
    std::string const& endpoint)
{
    auto it = std::find_if(
        snapshot.peers.begin(), snapshot.peers.end(), [&](auto const& entry) {
            return entry.endpoint == endpoint;
        });
    if (it == snapshot.peers.end())
        return nullptr;
    return &*it;
}

}  // namespace

TEST(PeerSetPruning, WaitSignalWakesOnDiscoveryAcrossThreads)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
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
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
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

TEST(PeerSetPruning, RemovesExpiredStateButRetainsFreshRetryEvidence)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });

    auto const now = std::chrono::steady_clock::now();

    PeerSetTestAccess::add_discovered(io, peers, "tracked.example.com:51235");
    PeerSetTestAccess::add_discovered(
        io, peers, "tracked-fail.example.com:51235");

    PeerSetTestAccess::set_crawled_at(
        io, peers, "tracked.example.com:51235", now);
    PeerSetTestAccess::set_crawled_at(
        io, peers, "expired.example.com:51235", now - std::chrono::minutes(31));
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
    EXPECT_TRUE(PeerSetTestAccess::has_failed(
        io, peers, "orphan-fail.example.com:51235"))
        << "the unified retry gate is itself owned evidence; unlike crawl "
           "history, it does not require a tracker row to remain valid";
}

TEST(PeerSetPruning, CapsTrackerAndDeduplicatesCanonicalEndpoints)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
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
    for (std::size_t i = 0; i < PeerSetTestAccess::max_tracked_endpoints + 32;
         ++i)
    {
        endpoints_to_add.push_back(
            "peer-" + std::to_string(i) + ".example.com:51235");
    }
    PeerSetTestAccess::add_discovered_many(
        io, peers, std::move(endpoints_to_add));

    auto const initial_size = peers->tracker()->size();
    ASSERT_GT(initial_size, PeerSetTestAccess::max_tracked_endpoints);

    PeerSetTestAccess::prune_discovery_state(io, peers);

    auto const endpoints = peers->tracker()->all_endpoints();
    EXPECT_LE(endpoints.size(), PeerSetTestAccess::max_tracked_endpoints + 1);
    EXPECT_TRUE(contains_endpoint(endpoints, active));
    EXPECT_TRUE(
        contains_endpoint(endpoints, "1.2.3.4:51235") ||
        contains_endpoint(endpoints, "::ffff:1.2.3.4:51235"));
    EXPECT_FALSE(
        contains_endpoint(endpoints, "1.2.3.4:51235") &&
        contains_endpoint(endpoints, "::ffff:1.2.3.4:51235"));
}

TEST(PeerSetPruning, CandidateRankingPrefersHealthyConnectedPeers)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });

    auto const status = PeerStatus{
        .first_seq = 100,
        .last_seq = 200,
        .current_seq = 150,
    };

    PeerSetTestAccess::add_discovered(io, peers, "healthy.example.com:51235");
    PeerSetTestAccess::note_connect_success(
        io, peers, "healthy.example.com:51235", status);

    PeerSetTestAccess::add_discovered(io, peers, "stale.example.com:51235");
    PeerSetTestAccess::note_connect_success(
        io, peers, "stale.example.com:51235", status);
    PeerSetTestAccess::note_connect_failure(
        io, peers, "stale.example.com:51235");

    EXPECT_TRUE(PeerSetTestAccess::candidate_better(
        io, peers, "healthy.example.com:51235", "stale.example.com:51235"));
    EXPECT_FALSE(PeerSetTestAccess::candidate_better(
        io, peers, "stale.example.com:51235", "healthy.example.com:51235"));
}

TEST(PeerSetPruning, RetryBackoffUsesFailureCountAndCapsAtFiveMinutes)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });

    auto const endpoint = std::string("retry.example.com:51235");
    PeerSetTestAccess::add_discovered(io, peers, endpoint);

    EXPECT_EQ(
        PeerSetTestAccess::retry_backoff_for(io, peers, endpoint),
        std::chrono::steady_clock::duration::zero());

    PeerSetTestAccess::note_connect_failure(io, peers, endpoint);
    EXPECT_EQ(
        PeerSetTestAccess::retry_backoff_for(io, peers, endpoint),
        std::chrono::seconds(5));

    PeerSetTestAccess::note_connect_failure(io, peers, endpoint);
    EXPECT_EQ(
        PeerSetTestAccess::retry_backoff_for(io, peers, endpoint),
        std::chrono::seconds(10));

    PeerSetTestAccess::note_connect_failure(io, peers, endpoint);
    EXPECT_EQ(
        PeerSetTestAccess::retry_backoff_for(io, peers, endpoint),
        std::chrono::seconds(20));

    for (int i = 0; i < 8; ++i)
    {
        PeerSetTestAccess::note_connect_failure(io, peers, endpoint);
    }
    EXPECT_EQ(
        PeerSetTestAccess::retry_backoff_for(io, peers, endpoint),
        std::chrono::minutes(5));
}

TEST(PeerSetPruning, PumpConnectsHonorsComputedRetryBackoff)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 1,
            .max_in_flight_crawls = 0,
        });

    auto const endpoint = std::string("backoff.example.com:51235");
    PeerSetTestAccess::add_discovered(io, peers, endpoint);
    PeerSetTestAccess::note_connect_failure(io, peers, endpoint);
    PeerSetTestAccess::note_connect_failure(io, peers, endpoint);
    PeerSetTestAccess::note_connect_failure(io, peers, endpoint);
    PeerSetTestAccess::set_failed_at(
        io,
        peers,
        endpoint,
        std::chrono::steady_clock::now() - std::chrono::seconds(10));
    PeerSetTestAccess::push_pending_connect(io, peers, endpoint);
    PeerSetTestAccess::pump_connects(io, peers);

    EXPECT_FALSE(PeerSetTestAccess::in_flight_contains(io, peers, endpoint));
}

TEST(PeerSetPruning, QueueCrawlSkipsPrivatePeersUntilRecheck)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 21337,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 1,
        });

    auto const endpoint = std::string("private.example.com:21337");
    PeerSetTestAccess::note_peer_headers(
        io, peers, endpoint, {{"Crawl", "private"}});
    PeerSetTestAccess::queue_crawl(io, peers, endpoint);

    auto snapshot = PeerSetTestAccess::snapshot(io, peers);
    auto const* entry = find_snapshot_entry(snapshot, endpoint);
    ASSERT_NE(entry, nullptr);
    EXPECT_FALSE(entry->queued_crawl);

    PeerSetTestAccess::set_crawl_private_until(
        io,
        peers,
        endpoint,
        std::chrono::steady_clock::now() - std::chrono::seconds(1));
    PeerSetTestAccess::queue_crawl(io, peers, endpoint);

    snapshot = PeerSetTestAccess::snapshot(io, peers);
    entry = find_snapshot_entry(snapshot, endpoint);
    ASSERT_NE(entry, nullptr);
    EXPECT_TRUE(entry->queued_crawl);
}

TEST(PeerSetPruning, SnapshotSummarizesRecentFailures)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });

    auto const forbidden = std::string("forbidden.example.com:51235");
    auto const flakey = std::string("flakey.example.com:51235");
    PeerSetTestAccess::add_discovered(io, peers, forbidden);
    PeerSetTestAccess::add_discovered(io, peers, flakey);

    PeerSetTestAccess::note_connect_failure(
        io, peers, forbidden, "http upgrade status=403 reason=\"Forbidden\"");
    PeerSetTestAccess::note_connect_failure(
        io, peers, forbidden, "http upgrade status=403 reason=\"Forbidden\"");
    PeerSetTestAccess::note_connect_failure(
        io, peers, flakey, "PeerClient: Disconnected");

    auto const snapshot = PeerSetTestAccess::snapshot(io, peers);

    ASSERT_GE(snapshot.recent_failures.size(), 2u);
    EXPECT_EQ(snapshot.recent_failures[0].kind, "connect-403");
    EXPECT_EQ(snapshot.recent_failures[0].count, 2u);
    EXPECT_EQ(snapshot.recent_failures[1].kind, "connect-disconnect");
    EXPECT_EQ(snapshot.recent_failures[1].count, 1u);

    ASSERT_GE(snapshot.top_failing_endpoints.size(), 2u);
    EXPECT_EQ(snapshot.top_failing_endpoints[0].endpoint, forbidden);
    EXPECT_EQ(snapshot.top_failing_endpoints[0].count, 2u);
    EXPECT_EQ(snapshot.top_failing_endpoints[1].endpoint, flakey);
    EXPECT_EQ(snapshot.top_failing_endpoints[1].count, 1u);
}

TEST(PeerSetPruning, TimedOutWaitForPeerClearsWantedLedger)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
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
                auto client =
                    co_await peers->wait_for_covering_peer(wanted_ledger, 0);
                EXPECT_FALSE(client);
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
    EXPECT_FALSE(
        PeerSetTestAccess::has_wanted_ledger(io, peers, wanted_ledger));
}

TEST(PeerSetPruning, CancelledWaitForPeerClearsWantedLedger)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
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
                auto client = co_await peers->wait_for_covering_peer(
                    wanted_ledger, 30, cancel);
                EXPECT_FALSE(client);
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
    EXPECT_FALSE(
        PeerSetTestAccess::has_wanted_ledger(io, peers, wanted_ledger));
}

TEST(PeerSetPruning, WantedLedgerRegistrationIsReferenceCounted)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    constexpr uint32_t ledger_seq = 1234;

    PeerSetTestAccess::acquire_wanted_ledger(io, peers, ledger_seq);
    PeerSetTestAccess::acquire_wanted_ledger(io, peers, ledger_seq);
    EXPECT_EQ(
        PeerSetTestAccess::wanted_ledger_count(io, peers, ledger_seq), 2u);

    PeerSetTestAccess::release_wanted_ledger(io, peers, ledger_seq);
    EXPECT_EQ(
        PeerSetTestAccess::wanted_ledger_count(io, peers, ledger_seq), 1u);
    PeerSetTestAccess::release_wanted_ledger(io, peers, ledger_seq);
    EXPECT_FALSE(PeerSetTestAccess::has_wanted_ledger(io, peers, ledger_seq));
}

TEST(PeerSetPruning, PersistentPrioritySurvivesTransientWaiterRelease)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    constexpr uint32_t ledger_seq = 100;

    PeerSetTestAccess::acquire_wanted_ledger(io, peers, ledger_seq);
    PeerSetTestAccess::prioritize_ledger(io, peers, ledger_seq);
    EXPECT_EQ(
        PeerSetTestAccess::wanted_ledger_count(io, peers, ledger_seq), 2u);

    PeerSetTestAccess::release_wanted_ledger(io, peers, ledger_seq);
    EXPECT_EQ(
        PeerSetTestAccess::wanted_ledger_count(io, peers, ledger_seq), 1u);

    for (uint32_t seq = 101; seq <= 110; ++seq)
        PeerSetTestAccess::prioritize_ledger(io, peers, seq);
    EXPECT_EQ(PeerSetTestAccess::wanted_ledger_size(io, peers), 10u);
    EXPECT_FALSE(PeerSetTestAccess::has_wanted_ledger(io, peers, ledger_seq));
}

TEST(PeerSetPruning, TargetedCandidateProbePrefersCoveringArchivalPeer)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .archival_range_threshold = 500,
        });
    constexpr uint32_t ledger_seq = 500;

    PeerSetTestAccess::set_status(
        io,
        peers,
        "narrow.example.com:1",
        {.first_seq = 450, .last_seq = 550, .current_seq = 550});
    PeerSetTestAccess::set_status(
        io,
        peers,
        "archive.example.com:1",
        {.first_seq = 1, .last_seq = 1000, .current_seq = 1000});
    PeerSetTestAccess::set_status(
        io,
        peers,
        "recent.example.com:1",
        {.first_seq = 800, .last_seq = 1800, .current_seq = 1800});

    auto queued =
        PeerSetTestAccess::try_candidates_for(io, peers, ledger_seq, {}, 1);
    ASSERT_EQ(queued.size(), 1u);
    EXPECT_EQ(queued.front(), "archive.example.com:1");
    EXPECT_TRUE(PeerSetTestAccess::is_targeted_connect(
        io, peers, "archive.example.com:1"));
}

TEST(PeerSetPruning, TargetedCandidateProbeExcludesKnownNonCoveringPeer)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    constexpr uint32_t ledger_seq = 500;

    PeerSetTestAccess::set_status(
        io,
        peers,
        "recent.example.com:1",
        {.first_seq = 800, .last_seq = 1800, .current_seq = 1800});
    PeerSetTestAccess::add_discovered(io, peers, "unknown.example.com:1");

    auto queued =
        PeerSetTestAccess::try_candidates_for(io, peers, ledger_seq, {}, 3);
    ASSERT_EQ(queued.size(), 1u);
    EXPECT_EQ(queued.front(), "unknown.example.com:1");
    EXPECT_FALSE(PeerSetTestAccess::is_targeted_connect(
        io, peers, "recent.example.com:1"));
}

TEST(PeerSetPruning, RediscoveredUnknownEndpointIsNotExcludedByStaleRange)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    constexpr uint32_t ledger_seq = 500;
    auto const endpoint = std::string("rediscovered.example.com:1");

    PeerSetTestAccess::set_status(
        io,
        peers,
        endpoint,
        {.first_seq = 800, .last_seq = 1800, .current_seq = 1800});
    peers->tracker()->remove(endpoint);
    PeerSetTestAccess::add_discovered(io, peers, endpoint);
    PeerSetTestAccess::post_note_discovered(peers, endpoint);
    io.run();
    io.restart();

    auto queued =
        PeerSetTestAccess::try_candidates_for(io, peers, ledger_seq, {}, 1);
    ASSERT_EQ(queued.size(), 1u);
    EXPECT_EQ(queued.front(), endpoint);
    EXPECT_TRUE(PeerSetTestAccess::is_targeted_connect(io, peers, endpoint));
}

TEST(PeerSetPruning, TargetedCandidateUpgradesExistingQueuedConnect)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    constexpr uint32_t ledger_seq = 500;
    auto const endpoint = std::string("queued.example.com:1");

    PeerSetTestAccess::set_status(
        io,
        peers,
        endpoint,
        {.first_seq = 400, .last_seq = 600, .current_seq = 600});
    PeerSetTestAccess::push_pending_connect(io, peers, endpoint);

    auto queued =
        PeerSetTestAccess::try_candidates_for(io, peers, ledger_seq, {}, 1);
    ASSERT_EQ(queued.size(), 1u);
    EXPECT_EQ(queued.front(), endpoint);
    EXPECT_TRUE(PeerSetTestAccess::is_targeted_connect(io, peers, endpoint));
}

TEST(PeerSetPruning, TimedOutWaitRevokesQueuedReplacementProbe)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    constexpr uint32_t ledger_seq = 500;
    auto const endpoint = std::string("late-probe.example.com:1");
    PeerSetTestAccess::set_status(
        io,
        peers,
        endpoint,
        {.first_seq = 400, .last_seq = 600, .current_seq = 600});

    PeerGenerationPin result;
    asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
            result = co_await peers->wait_for_covering_peer(ledger_seq, 0);
        },
        asio::detached);
    io.run();

    EXPECT_FALSE(result);
    io.restart();
    EXPECT_FALSE(PeerSetTestAccess::is_targeted_connect(io, peers, endpoint));
    EXPECT_FALSE(PeerSetTestAccess::has_wanted_ledger(io, peers, ledger_seq));
}

TEST(PeerSetPruning, TargetedQueuedCandidateExtendsVisibleWaitBound)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    constexpr uint32_t ledger_seq = 500;
    auto const endpoint = std::string("queued-target.example.com:1");
    PeerSetTestAccess::set_status(
        io,
        peers,
        endpoint,
        {.first_seq = 400, .last_seq = 600, .current_seq = 600});

    PeerGenerationPin result;
    auto const started = std::chrono::steady_clock::now();
    asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
            result = co_await peers->wait_for_covering_peer(ledger_seq, 1);
        },
        asio::detached);
    io.run();
    auto const elapsed = std::chrono::steady_clock::now() - started;

    EXPECT_FALSE(result);
    EXPECT_GE(elapsed, 1500ms)
        << "a selected queued targeted candidate must not report terminality "
           "at the first requester deadline";
    io.restart();
    EXPECT_FALSE(PeerSetTestAccess::is_targeted_connect(io, peers, endpoint));
    EXPECT_FALSE(PeerSetTestAccess::has_wanted_ledger(io, peers, ledger_seq));
}

TEST(PeerSetPruning, ConnectDeadlineUsesOnlyLiveQueuedAuthority)
{
    using namespace std::chrono_literals;
    auto const now = std::chrono::steady_clock::now();

    auto claimed = PeerSetTestAccess::claim_connect_authority(
        now, false, {}, {now + 5s, now + 20s});
    ASSERT_TRUE(claimed.deadline);
    EXPECT_EQ(*claimed.deadline, now + 20s);
    EXPECT_EQ(claimed.leases, 2u);

    claimed = PeerSetTestAccess::claim_connect_authority(
        now, false, {}, {now + 120s});
    ASSERT_TRUE(claimed.deadline);
    EXPECT_EQ(*claimed.deadline, now + 60s)
        << "request authority cannot extend the ordinary absolute maximum";

    claimed = PeerSetTestAccess::claim_connect_authority(
        now, false, {}, {now - 1s, now + 10s}, {true, false});
    EXPECT_FALSE(claimed.deadline);
    EXPECT_EQ(claimed.leases, 0u);

    claimed = PeerSetTestAccess::claim_connect_authority(
        now, true, now + 30s, {now + 20s});
    ASSERT_TRUE(claimed.deadline);
    EXPECT_EQ(*claimed.deadline, now + 30s);
    EXPECT_EQ(claimed.leases, 1u);
}

TEST(PeerSetPruning, TargetedQueueCapacityRefusesWithoutFalseSelection)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto const endpoint = std::string("bounded-targets.example.com:1");
    auto const accepted =
        PeerSetTestAccess::queue_target_authorizations(io, peers, endpoint, 65);
    ASSERT_EQ(accepted.size(), 65u);
    EXPECT_TRUE(std::ranges::all_of(
        accepted.begin(), accepted.begin() + 64, [](bool value) {
            return value;
        }));
    EXPECT_FALSE(accepted.back());
    EXPECT_TRUE(PeerSetTestAccess::is_queued_connect(io, peers, endpoint));
}

TEST(PeerSetPruning, TargetCleanupRemovesOnlyTargetOnlyQueueRows)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });

    auto const target_only = std::string("target-only.example.com:1");
    auto target_authorization =
        PeerSetTestAccess::acquire_targeted_wanted_ledger(io, peers, 500);
    ASSERT_TRUE(PeerSetTestAccess::queue_targeted_authorization(
        io, peers, target_only, target_authorization));
    ASSERT_TRUE(PeerSetTestAccess::is_queued_connect(io, peers, target_only));
    PeerSetTestAccess::release_targeted_wanted_ledger(
        io, peers, std::move(target_authorization));
    EXPECT_FALSE(PeerSetTestAccess::is_queued_connect(io, peers, target_only));

    auto const ordinary = std::string("ordinary-plus-target.example.com:1");
    PeerSetTestAccess::push_pending_connect(io, peers, ordinary);
    auto joined_authorization =
        PeerSetTestAccess::acquire_targeted_wanted_ledger(io, peers, 501);
    ASSERT_TRUE(PeerSetTestAccess::queue_targeted_authorization(
        io, peers, ordinary, joined_authorization));
    PeerSetTestAccess::release_targeted_wanted_ledger(
        io, peers, std::move(joined_authorization));
    EXPECT_TRUE(PeerSetTestAccess::is_queued_connect(io, peers, ordinary));
    EXPECT_FALSE(PeerSetTestAccess::is_targeted_connect(io, peers, ordinary));
}

TEST(PeerSetPruning, AsioCanceledWaitRevokesReplacementProbe)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    constexpr uint32_t ledger_seq = 500;
    auto const endpoint = std::string("canceled-probe.example.com:1");
    PeerSetTestAccess::set_status(
        io,
        peers,
        endpoint,
        {.first_seq = 400, .last_seq = 600, .current_seq = 600});

    bool done = false;
    bool cancel_branch_won = false;
    asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
            auto ex = co_await asio::this_coro::executor;
            auto result =
                co_await asio::experimental::make_parallel_group(
                    asio::co_spawn(
                        ex,
                        peers->wait_for_covering_peer(ledger_seq, 5),
                        asio::deferred),
                    asio::co_spawn(
                        ex,
                        [ex]() -> asio::awaitable<void> {
                            asio::steady_timer timer(ex, 10ms);
                            boost::system::error_code ec;
                            co_await timer.async_wait(
                                asio::redirect_error(asio::use_awaitable, ec));
                        },
                        asio::deferred))
                    .async_wait(
                        asio::experimental::wait_for_one(),
                        asio::use_awaitable);

            cancel_branch_won = (std::get<0>(result)[0] == 1);
            done = true;
        },
        asio::detached);
    io.run();

    EXPECT_TRUE(done);
    EXPECT_TRUE(cancel_branch_won);
    io.restart();
    EXPECT_FALSE(PeerSetTestAccess::is_targeted_connect(io, peers, endpoint));
    EXPECT_FALSE(PeerSetTestAccess::has_wanted_ledger(io, peers, ledger_seq));
}

TEST(PeerSetPruning, DuplicateLedgerFailureCountsOncePerWindow)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto const endpoint = std::string("history-poor.example.com:1");

    PeerSetTestAccess::set_status(
        io,
        peers,
        endpoint,
        {.first_seq = 900, .last_seq = 1000, .current_seq = 1000});
    PeerSetTestAccess::note_ledger_failure(io, peers, endpoint, 500);
    PeerSetTestAccess::note_ledger_failure(io, peers, endpoint, 500);

    auto const snapshot = PeerSetTestAccess::snapshot(io, peers);
    auto const* entry = find_snapshot_entry(snapshot, endpoint);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry->ledger_failure_count, 1u);
    EXPECT_EQ(entry->recent_failed_ledgers, 1u);
}
