/**
 * Integration tests for NodeCache signal/notify mechanism.
 *
 * These test the ACTUAL ensure_present wait loop and insert_and_notify,
 * not just Asio timer behavior.
 */

#include <catl/peer-client/node-cache.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <vector>

using namespace catl::peer_client;
namespace asio = boost::asio;

namespace {

Hash256
make_hash(uint8_t fill)
{
    Hash256 h;
    std::memset(h.data(), fill, 32);
    return h;
}

std::vector<uint8_t>
make_data(size_t size, uint8_t fill = 0x42)
{
    return std::vector<uint8_t>(size, fill);
}

}  // namespace

/// Test access shim — exposes private members for integration testing.
class NodeCacheTestAccess
{
public:
    /// Create an in-flight entry with a signal timer (simulates a
    /// MISS that sent a fetch but data hasn't arrived yet).
    static void
    create_in_flight(
        NodeCache& cache,
        Hash256 const& hash,
        std::chrono::milliseconds timeout)
    {
        std::lock_guard lock(cache.mutex_);
        auto [it, inserted] = cache.store_.try_emplace(hash);
        if (inserted || !it->second.present)
        {
            it->second.signal = std::make_shared<asio::steady_timer>(
                cache.io_, timeout);
            it->second.present = false;
            it->second.last_fetch_at = std::chrono::steady_clock::now();
        }
    }

    /// Call ensure_present in wait mode (entry already in-flight).
    /// Returns the wire data shared_ptr (nullptr on timeout).
    static asio::awaitable<std::shared_ptr<std::vector<uint8_t>>>
    wait_for_hash(
        std::shared_ptr<NodeCache> cache,
        Hash256 hash)
    {
        co_return co_await cache->ensure_present(
            hash,
            make_hash(0xBB),  // ledger_hash (unused in wait path)
            2,                // tree_type
            SHAMapNodeID::root(),
            hash,             // target_key
            1,                // speculative_depth
            nullptr,          // no peer (wait path)
            nullptr,          // no peers
            0,
            nullptr);         // ledger_seq / cancel
    }

    static asio::awaitable<std::shared_ptr<std::vector<uint8_t>>>
    wait_for_hash_with_cancel(
        std::shared_ptr<NodeCache> cache,
        Hash256 hash,
        std::shared_ptr<std::atomic<bool>> cancel)
    {
        co_return co_await cache->ensure_present(
            hash,
            make_hash(0xBB),  // ledger_hash (unused in wait path)
            2,                // tree_type
            SHAMapNodeID::root(),
            hash,             // target_key
            1,                // speculative_depth
            nullptr,          // no peer (wait path)
            nullptr,          // no peers
            0,
            cancel);          // ledger_seq / cancel
    }

    /// Get the signal for an entry (for testing expiry behavior).
    static std::shared_ptr<asio::steady_timer>
    get_signal(NodeCache& cache, Hash256 const& hash)
    {
        std::lock_guard lock(cache.mutex_);
        auto it = cache.store_.find(hash);
        if (it == cache.store_.end())
            return nullptr;
        return it->second.signal;
    }

    /// Publish a progress event on a node entry.
    static void
    publish_progress(
        NodeCache& cache,
        Hash256 const& hash)
    {
        cache.publish_node_progress(
            hash,
            NodeCache::ProgressKind::retry_same_peer,
            100000, 0, 1, 3, "test-peer:51235");
    }

    static void
    create_placeholder(NodeCache& cache, Hash256 const& hash)
    {
        std::lock_guard lock(cache.mutex_);
        auto [it, inserted] = cache.store_.try_emplace(hash);
        it->second.present = false;
        it->second.signal = nullptr;
        it->second.last_fetch_at = std::chrono::steady_clock::now();
        cache.touch_lru(hash);
        cache.evict_if_needed();
    }

    static bool
    has_entry(NodeCache& cache, Hash256 const& hash)
    {
        std::lock_guard lock(cache.mutex_);
        return cache.store_.count(hash) > 0;
    }

    static void
    create_cached_header(NodeCache& cache, uint32_t ledger_seq)
    {
        std::lock_guard lock(cache.mutex_);
        auto& entry = cache.header_cache_[ledger_seq];
        entry.present = true;
        entry.signal = nullptr;
        cache.touch_header_lru(ledger_seq);
        cache.evict_headers_if_needed();
    }

    static void
    create_in_flight_header(
        NodeCache& cache,
        uint32_t ledger_seq,
        std::chrono::milliseconds timeout)
    {
        std::lock_guard lock(cache.mutex_);
        auto& entry = cache.header_cache_[ledger_seq];
        entry.present = false;
        entry.signal = std::make_shared<asio::steady_timer>(cache.io_, timeout);
        cache.touch_header_lru(ledger_seq);
        cache.evict_headers_if_needed();
    }

    static bool
    has_header(NodeCache& cache, uint32_t ledger_seq)
    {
        std::lock_guard lock(cache.mutex_);
        return cache.header_cache_.count(ledger_seq) > 0;
    }

    static size_t
    max_header_entries(NodeCache& cache)
    {
        std::lock_guard lock(cache.mutex_);
        return cache.max_header_entries_;
    }
};

// ─── insert_and_notify wakes a waiter in ensure_present ─────────

TEST(NodeCacheIntegration, InsertNotifyWakesEnsurePresent)
{
    asio::io_context io;
    auto cache = NodeCache::create(io, {
        .max_entries = 1024,
        .fetch_timeout = std::chrono::milliseconds(5000),
    });

    auto hash = make_hash(0xAA);
    auto data = make_data(100);

    // Create in-flight entry
    NodeCacheTestAccess::create_in_flight(*cache, hash, std::chrono::milliseconds(5000));

    // Start waiter
    std::shared_ptr<std::vector<uint8_t>> result;
    bool done = false;
    asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
            result = co_await NodeCacheTestAccess::wait_for_hash(cache, hash);
            done = true;
        },
        asio::detached);

    // Run until waiter is blocked
    io.poll();
    EXPECT_FALSE(done);

    // Data arrives
    cache->insert_and_notify(hash, data);

    // Waiter should wake
    io.poll();
    EXPECT_TRUE(done);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->size(), 100u);
}

// ─── Two waiters on same hash both get data ─────────────────────

TEST(NodeCacheIntegration, TwoWaitersBothGetData)
{
    asio::io_context io;
    auto cache = NodeCache::create(io, {
        .max_entries = 1024,
        .fetch_timeout = std::chrono::milliseconds(5000),
    });

    auto hash = make_hash(0xBB);
    auto data = make_data(200, 0x77);

    NodeCacheTestAccess::create_in_flight(*cache, hash, std::chrono::milliseconds(5000));

    std::shared_ptr<std::vector<uint8_t>> r1, r2;
    int done_count = 0;

    for (auto* rp : {&r1, &r2})
    {
        asio::co_spawn(
            io,
            [&, rp]() -> asio::awaitable<void> {
                *rp = co_await NodeCacheTestAccess::wait_for_hash(cache, hash);
                done_count++;
            },
            asio::detached);
    }

    io.poll();
    EXPECT_EQ(done_count, 0);

    cache->insert_and_notify(hash, data);
    io.poll();

    EXPECT_EQ(done_count, 2);
    ASSERT_NE(r1, nullptr);
    ASSERT_NE(r2, nullptr);
    EXPECT_EQ(r1->size(), 200u);
    EXPECT_EQ(r2->size(), 200u);
}

// ─── Timeout returns nullptr ────────────────────────────────────

TEST(NodeCacheIntegration, TimeoutReturnsNullptr)
{
    asio::io_context io;
    auto cache = NodeCache::create(io, {
        .max_entries = 1024,
        .fetch_timeout = std::chrono::milliseconds(1000),  // short timeout
    });

    auto hash = make_hash(0xCC);
    NodeCacheTestAccess::create_in_flight(*cache, hash, std::chrono::milliseconds(1000));

    std::shared_ptr<std::vector<uint8_t>> result;
    bool done = false;
    asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
            result = co_await NodeCacheTestAccess::wait_for_hash(cache, hash);
            done = true;
        },
        asio::detached);

    // Run until timeout fires (~1s)
    io.run();
    EXPECT_TRUE(done);
    EXPECT_EQ(result, nullptr);
}

// ─── Second waiter after timeout gets fresh signal (THE BUG FIX) ─

TEST(NodeCacheIntegration, SecondWaiterAfterTimeoutGetsFreshSignal)
{
    asio::io_context io;
    auto cache = NodeCache::create(io, {
        .max_entries = 1024,
        .fetch_timeout = std::chrono::milliseconds(1000),  // 1s timeout
        .fetch_stale_multiplier = 10,  // stale at 10s (won't trigger)
    });

    auto hash = make_hash(0xDD);
    auto data = make_data(50);

    NodeCacheTestAccess::create_in_flight(*cache, hash, std::chrono::milliseconds(1000));

    // First waiter — will timeout after 1s
    bool first_done = false;
    asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
            auto r = co_await NodeCacheTestAccess::wait_for_hash(cache, hash);
            EXPECT_EQ(r, nullptr);  // timeout
            first_done = true;
        },
        asio::detached);

    io.run();
    EXPECT_TRUE(first_done);
    io.restart();

    // Second waiter — WITHOUT the fix, this would immediately timeout
    // because the expired signal is still on the entry. WITH the fix,
    // ensure_present detects the expired signal and creates a fresh one.
    std::shared_ptr<std::vector<uint8_t>> second_result;
    bool second_done = false;
    asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
            second_result =
                co_await NodeCacheTestAccess::wait_for_hash(cache, hash);
            second_done = true;
        },
        asio::detached);

    io.poll();
    // With the fix: second waiter should be blocked (fresh signal)
    // Without the fix: second waiter would immediately return nullptr
    EXPECT_FALSE(second_done) << "Second waiter returned immediately — "
                                 "expired signal not replaced (THE BUG)";

    // Now deliver the data
    cache->insert_and_notify(hash, data);
    io.poll();

    EXPECT_TRUE(second_done);
    ASSERT_NE(second_result, nullptr);
    EXPECT_EQ(second_result->size(), 50u);
}

// ─── Progress event wakes waiter, waiter continues waiting ──────

TEST(NodeCacheIntegration, ProgressEventRelayedThenContinuesWaiting)
{
    asio::io_context io;
    auto cache = NodeCache::create(io, {
        .max_entries = 1024,
        .fetch_timeout = std::chrono::milliseconds(5000),
    });

    auto hash = make_hash(0xEE);
    auto data = make_data(75);

    NodeCacheTestAccess::create_in_flight(*cache, hash, std::chrono::milliseconds(5000));

    std::shared_ptr<std::vector<uint8_t>> result;
    bool done = false;
    asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
            result = co_await NodeCacheTestAccess::wait_for_hash(cache, hash);
            done = true;
        },
        asio::detached);

    io.poll();
    EXPECT_FALSE(done);

    // Send a progress event — waiter should wake, process it, then
    // go back to waiting (not return nullptr)
    NodeCacheTestAccess::publish_progress(*cache, hash);
    io.poll();
    EXPECT_FALSE(done) << "Waiter returned after progress event instead of continuing";

    // Now deliver actual data
    cache->insert_and_notify(hash, data);
    io.poll();

    EXPECT_TRUE(done);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->size(), 75u);
}

// ─── Data arrives during progress processing ────────────────────

TEST(NodeCacheIntegration, DataArrivesDuringProgressProcessing)
{
    asio::io_context io;
    auto cache = NodeCache::create(io, {
        .max_entries = 1024,
        .fetch_timeout = std::chrono::milliseconds(5000),
    });

    auto hash = make_hash(0xFF);
    auto data = make_data(120);

    NodeCacheTestAccess::create_in_flight(*cache, hash, std::chrono::milliseconds(5000));

    std::shared_ptr<std::vector<uint8_t>> result;
    bool done = false;
    asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
            result = co_await NodeCacheTestAccess::wait_for_hash(cache, hash);
            done = true;
        },
        asio::detached);

    io.poll();
    EXPECT_FALSE(done);

    // Fire progress AND data at the same time
    NodeCacheTestAccess::publish_progress(*cache, hash);
    cache->insert_and_notify(hash, data);

    io.poll();
    EXPECT_TRUE(done);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->size(), 120u);
}

// ─── Cache hit on second call (no waiting) ──────────────────────

TEST(NodeCacheIntegration, CacheHitOnSecondCall)
{
    asio::io_context io;
    auto cache = NodeCache::create(io, {
        .max_entries = 1024,
        .fetch_timeout = std::chrono::milliseconds(5000),
    });

    auto hash = make_hash(0x11);
    auto data = make_data(60);

    // Insert data directly (present=true)
    cache->insert(hash, data);

    // ensure_present should return immediately (HIT)
    std::shared_ptr<std::vector<uint8_t>> result;
    bool done = false;
    asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
            result = co_await NodeCacheTestAccess::wait_for_hash(cache, hash);
            done = true;
        },
        asio::detached);

    io.poll();
    EXPECT_TRUE(done);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->size(), 60u);
}

TEST(NodeCacheIntegration, PlaceholderEntriesAreEvictable)
{
    asio::io_context io;
    auto cache = NodeCache::create(io, {
        .max_entries = 2,
        .fetch_timeout = std::chrono::milliseconds(5000),
    });

    auto h1 = make_hash(0x21);
    auto h2 = make_hash(0x22);
    auto h3 = make_hash(0x23);

    NodeCacheTestAccess::create_placeholder(*cache, h1);
    NodeCacheTestAccess::create_placeholder(*cache, h2);
    NodeCacheTestAccess::create_placeholder(*cache, h3);

    EXPECT_FALSE(NodeCacheTestAccess::has_entry(*cache, h1));
    EXPECT_TRUE(NodeCacheTestAccess::has_entry(*cache, h2));
    EXPECT_TRUE(NodeCacheTestAccess::has_entry(*cache, h3));

    auto stats = cache->stats();
    EXPECT_EQ(stats.entries, 2u);
    EXPECT_EQ(stats.resident_entries, 0u);
}

TEST(NodeCacheIntegration, HeaderCacheIsBounded)
{
    asio::io_context io;
    auto cache = NodeCache::create(io, {
        .max_entries = 1024,
        .fetch_timeout = std::chrono::milliseconds(5000),
    });

    auto max_headers = NodeCacheTestAccess::max_header_entries(*cache);
    ASSERT_GT(max_headers, 0u);

    for (size_t seq = 1; seq <= max_headers + 1; ++seq)
        NodeCacheTestAccess::create_cached_header(*cache, static_cast<uint32_t>(seq));

    EXPECT_FALSE(NodeCacheTestAccess::has_header(*cache, 1));
    EXPECT_TRUE(
        NodeCacheTestAccess::has_header(*cache, static_cast<uint32_t>(max_headers + 1)));

    auto stats = cache->stats();
    EXPECT_EQ(stats.header_entries, max_headers);
}

TEST(NodeCacheIntegration, TimedOutHeaderWaiterClearsStaleEntry)
{
    asio::io_context io;
    auto cache = NodeCache::create(io, {
        .max_entries = 1024,
        .fetch_timeout = std::chrono::milliseconds(1000),
    });

    constexpr uint32_t ledger_seq = 123456u;
    NodeCacheTestAccess::create_in_flight_header(
        *cache, ledger_seq, std::chrono::milliseconds(25));

    bool done = false;
    bool timed_out = false;

    asio::co_spawn(
        io,
        [&, cache]() -> asio::awaitable<void> {
            try
            {
                (void)co_await cache->get_header(ledger_seq, nullptr, nullptr);
            }
            catch (std::exception const&)
            {
                timed_out = true;
            }
            done = true;
        },
        asio::detached);

    io.run();

    EXPECT_TRUE(done);
    EXPECT_TRUE(timed_out);
    EXPECT_FALSE(NodeCacheTestAccess::has_header(*cache, ledger_seq));
}

TEST(NodeCacheIntegration, CancelledEnsurePresentReturnsPromptly)
{
    asio::io_context io;
    auto cache = NodeCache::create(io, {
        .max_entries = 1024,
        .fetch_timeout = std::chrono::seconds(5),
    });

    auto hash = make_hash(0x31);
    auto cancel = std::make_shared<std::atomic<bool>>(false);
    NodeCacheTestAccess::create_in_flight(
        *cache, hash, std::chrono::seconds(5));

    bool done = false;
    std::shared_ptr<std::vector<uint8_t>> result;
    auto const started = std::chrono::steady_clock::now();

    asio::co_spawn(
        io,
        [&, cache, cancel]() -> asio::awaitable<void> {
            result = co_await NodeCacheTestAccess::wait_for_hash_with_cancel(
                cache, hash, cancel);
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
    EXPECT_EQ(result, nullptr);
    EXPECT_LT(elapsed, std::chrono::milliseconds(500));
    EXPECT_TRUE(NodeCacheTestAccess::has_entry(*cache, hash));
}

TEST(NodeCacheIntegration, CancelledEnsurePresentDoesNotPoisonSharedEntry)
{
    asio::io_context io;
    auto cache = NodeCache::create(io, {
        .max_entries = 1024,
        .fetch_timeout = std::chrono::seconds(5),
    });

    auto hash = make_hash(0x32);
    auto data = make_data(88, 0x55);
    auto cancel = std::make_shared<std::atomic<bool>>(false);
    NodeCacheTestAccess::create_in_flight(
        *cache, hash, std::chrono::seconds(5));

    bool canceled_done = false;
    bool second_done = false;
    std::shared_ptr<std::vector<uint8_t>> second_result;

    asio::co_spawn(
        io,
        [&, cache, cancel]() -> asio::awaitable<void> {
            auto result = co_await NodeCacheTestAccess::wait_for_hash_with_cancel(
                cache, hash, cancel);
            EXPECT_EQ(result, nullptr);
            canceled_done = true;
        },
        asio::detached);

    asio::steady_timer cancel_timer(io, std::chrono::milliseconds(10));
    cancel_timer.async_wait([cancel](boost::system::error_code const&) {
        cancel->store(true, std::memory_order_relaxed);
    });

    io.run();

    EXPECT_TRUE(canceled_done);
    io.restart();

    cache->insert_and_notify(hash, data);

    asio::co_spawn(
        io,
        [&, cache]() -> asio::awaitable<void> {
            second_result = co_await NodeCacheTestAccess::wait_for_hash(cache, hash);
            second_done = true;
        },
        asio::detached);

    io.poll();

    EXPECT_TRUE(second_done);
    ASSERT_NE(second_result, nullptr);
    EXPECT_EQ(second_result->size(), 88u);
}

TEST(NodeCacheIntegration, CancelledHeaderFetchClearsInFlightEntry)
{
    asio::io_context io;
    auto cache = NodeCache::create(io, {
        .max_entries = 1024,
        .fetch_timeout = std::chrono::seconds(5),
    });
    auto peers = PeerSet::create(io, PeerSetOptions{
        .network_id = 0,
        .max_in_flight_connects = 0,
        .max_in_flight_crawls = 0,
    });

    bool done = false;
    bool cancel_branch_won = false;
    constexpr uint32_t ledger_seq = 654321u;

    asio::co_spawn(
        io,
        [&, cache, peers]() -> asio::awaitable<void> {
            auto ex = co_await asio::this_coro::executor;
            auto result =
                co_await asio::experimental::make_parallel_group(
                    asio::co_spawn(
                        ex,
                        cache->get_header(ledger_seq, peers, nullptr),
                        asio::deferred),
                    asio::co_spawn(
                        ex,
                        []() -> asio::awaitable<void> {
                            auto ex = co_await asio::this_coro::executor;
                            asio::steady_timer timer(
                                ex, std::chrono::milliseconds(10));
                            boost::system::error_code ec;
                            co_await timer.async_wait(
                                asio::redirect_error(
                                    asio::use_awaitable, ec));
                        },
                        asio::deferred))
                    .async_wait(
                        asio::experimental::wait_for_one(),
                        asio::use_awaitable);

            cancel_branch_won = (std::get<0>(result)[0] == 1);

            asio::steady_timer drain(ex, std::chrono::milliseconds(1));
            boost::system::error_code drain_ec;
            co_await drain.async_wait(
                asio::redirect_error(asio::use_awaitable, drain_ec));

            done = true;
        },
        asio::detached);

    io.run();

    EXPECT_TRUE(done);
    EXPECT_TRUE(cancel_branch_won);
    EXPECT_FALSE(NodeCacheTestAccess::has_header(*cache, ledger_seq));
}
