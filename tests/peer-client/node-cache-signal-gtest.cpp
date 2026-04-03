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
        int timeout_secs)
    {
        std::lock_guard lock(cache.mutex_);
        auto [it, inserted] = cache.store_.try_emplace(hash);
        if (inserted || !it->second.present)
        {
            it->second.signal = std::make_shared<asio::steady_timer>(
                cache.io_, std::chrono::seconds(timeout_secs));
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
            0);               // ledger_seq
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
};

// ─── insert_and_notify wakes a waiter in ensure_present ─────────

TEST(NodeCacheIntegration, InsertNotifyWakesEnsurePresent)
{
    asio::io_context io;
    auto cache = NodeCache::create(io, {
        .max_entries = 1024,
        .fetch_timeout_secs = 5,
    });

    auto hash = make_hash(0xAA);
    auto data = make_data(100);

    // Create in-flight entry
    NodeCacheTestAccess::create_in_flight(*cache, hash, 5);

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
        .fetch_timeout_secs = 5,
    });

    auto hash = make_hash(0xBB);
    auto data = make_data(200, 0x77);

    NodeCacheTestAccess::create_in_flight(*cache, hash, 5);

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
        .fetch_timeout_secs = 1,  // short timeout
    });

    auto hash = make_hash(0xCC);
    NodeCacheTestAccess::create_in_flight(*cache, hash, 1);

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
        .fetch_timeout_secs = 1,  // 1s timeout
        .fetch_stale_multiplier = 10,  // stale at 10s (won't trigger)
    });

    auto hash = make_hash(0xDD);
    auto data = make_data(50);

    NodeCacheTestAccess::create_in_flight(*cache, hash, 1);

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
        .fetch_timeout_secs = 5,
    });

    auto hash = make_hash(0xEE);
    auto data = make_data(75);

    NodeCacheTestAccess::create_in_flight(*cache, hash, 5);

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
        .fetch_timeout_secs = 5,
    });

    auto hash = make_hash(0xFF);
    auto data = make_data(120);

    NodeCacheTestAccess::create_in_flight(*cache, hash, 5);

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
        .fetch_timeout_secs = 5,
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
