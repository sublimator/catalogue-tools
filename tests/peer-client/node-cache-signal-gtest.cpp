/**
 * Integration tests for NodeCache signal/notify mechanism.
 *
 * These test the ACTUAL ensure_present wait loop and insert_and_notify,
 * not just Asio timer behavior.
 */

#include <catl/peer-client/node-cache.h>

#include "node-batch-test-session.h"
#include "peer-set-test-access.h"

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
#include <future>
#include <limits>
#include <memory>
#include <thread>
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

TEST(SerializedTimerSignal, CompleteBeforeWaitIsLatched)
{
    asio::io_context io;
    auto signal = SerializedTimerSignal::create(
        io, std::chrono::seconds(30));

    signal->complete();
    io.poll();
    io.restart();

    bool done = false;
    boost::system::error_code result;
    asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
            result = co_await signal->wait();
            done = true;
        },
        asio::detached);

    io.run();
    EXPECT_TRUE(done);
    EXPECT_EQ(result, asio::error::operation_aborted);
}

TEST(SerializedTimerSignal, CompleteBeforeExpiryUpdateCannotResurrect)
{
    asio::io_context io;
    auto signal = SerializedTimerSignal::create(
        io, std::chrono::seconds(30));

    signal->complete();
    signal->expires_after(std::chrono::seconds(30));

    bool done = false;
    boost::system::error_code result;
    asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
            result = co_await signal->wait();
            done = true;
        },
        asio::detached);

    io.run();
    EXPECT_TRUE(done);
    EXPECT_EQ(result, asio::error::operation_aborted);
}

TEST(SerializedTimerSignal, CompletionWakesEveryWaiterExactlyOnce)
{
    asio::io_context io;
    auto signal = SerializedTimerSignal::create(
        io, std::chrono::seconds(30));

    std::array<boost::system::error_code, 3> results;
    std::atomic<unsigned> completions = 0;
    for (auto& result : results)
    {
        asio::co_spawn(
            io,
            [&signal, &result, &completions]() -> asio::awaitable<void> {
                result = co_await signal->wait();
                completions.fetch_add(1, std::memory_order_relaxed);
            },
            asio::detached);
    }

    io.poll();
    EXPECT_EQ(completions.load(std::memory_order_relaxed), 0u);
    io.restart();
    signal->complete();
    io.run();

    EXPECT_EQ(completions.load(std::memory_order_relaxed), results.size());
    for (auto const& result : results)
        EXPECT_EQ(result, asio::error::operation_aborted);
}

TEST(SerializedTimerSignal, CancellingOneWaitDoesNotPoisonAnotherExecutor)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto work = asio::make_work_guard(io);
    auto first_executor = asio::make_strand(io);
    auto second_executor = asio::make_strand(io);
    auto signal = SerializedTimerSignal::create(io, 30s);

    std::promise<void> first_done;
    auto first_done_future = first_done.get_future();
    std::promise<void> second_done;
    auto second_done_future = second_done.get_future();
    boost::system::error_code first_result;
    boost::system::error_code second_result;
    std::atomic<bool> second_completed = false;

    asio::co_spawn(
        first_executor,
        [signal, &first_result, &first_done]() -> asio::awaitable<void> {
            auto ex = co_await asio::this_coro::executor;
            auto result =
                co_await asio::experimental::make_parallel_group(
                    asio::co_spawn(ex, signal->wait(), asio::deferred),
                    asio::co_spawn(
                        ex,
                        []() -> asio::awaitable<void> {
                            auto ex = co_await asio::this_coro::executor;
                            asio::steady_timer timer(ex, 20ms);
                            boost::system::error_code ec;
                            co_await timer.async_wait(asio::redirect_error(
                                asio::use_awaitable, ec));
                        },
                        asio::deferred))
                    .async_wait(
                        asio::experimental::wait_for_one(),
                        asio::use_awaitable);
            first_result = std::get<2>(result);
            first_done.set_value();
        },
        asio::detached);

    asio::co_spawn(
        second_executor,
        [signal,
         &second_result,
         &second_completed,
         &second_done]() -> asio::awaitable<void> {
            second_result = co_await signal->wait();
            second_completed.store(true, std::memory_order_release);
            second_done.set_value();
        },
        asio::detached);

    std::thread first_runner([&io]() { io.run(); });
    std::thread second_runner([&io]() { io.run(); });

    auto first_status = first_done_future.wait_for(2s);
    EXPECT_EQ(first_status, std::future_status::ready);
    if (first_status != std::future_status::ready)
    {
        work.reset();
        io.stop();
        first_runner.join();
        second_runner.join();
        return;
    }
    EXPECT_EQ(first_result, asio::error::operation_aborted);
    EXPECT_FALSE(second_completed.load(std::memory_order_acquire));

    signal->complete();
    auto second_status = second_done_future.wait_for(2s);
    EXPECT_EQ(second_status, std::future_status::ready);
    if (second_status != std::future_status::ready)
    {
        work.reset();
        io.stop();
        first_runner.join();
        second_runner.join();
        return;
    }
    EXPECT_EQ(second_result, asio::error::operation_aborted);

    std::promise<boost::system::error_code> late_done;
    auto late_done_future = late_done.get_future();
    asio::co_spawn(
        first_executor,
        [signal, &late_done]() -> asio::awaitable<void> {
            late_done.set_value(co_await signal->wait());
        },
        asio::detached);
    auto late_status = late_done_future.wait_for(2s);
    EXPECT_EQ(late_status, std::future_status::ready);
    if (late_status != std::future_status::ready)
    {
        work.reset();
        io.stop();
        first_runner.join();
        second_runner.join();
        return;
    }
    EXPECT_EQ(late_done_future.get(), asio::error::operation_aborted);

    work.reset();
    io.stop();
    first_runner.join();
    second_runner.join();
}

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
            it->second.signal =
                SerializedTimerSignal::create(cache.io_, timeout);
            it->second.present = false;
            it->second.last_fetch_at = std::chrono::steady_clock::now();
        }
    }

    /// Call ensure_present in wait mode (entry already in-flight).
    /// Returns the wire data shared_ptr (nullptr on timeout).
    static asio::awaitable<std::shared_ptr<std::vector<uint8_t> const>>
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
            {},               // no managed generation pin
            nullptr,          // no peers
            0,
            nullptr);         // ledger_seq / cancel
    }

    static asio::awaitable<std::shared_ptr<std::vector<uint8_t> const>>
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
            {},               // no managed generation pin
            nullptr,          // no peers
            0,
            cancel);          // ledger_seq / cancel
    }

    static std::shared_ptr<SharedHistoryFetch>
    create_participant_in_flight(
        NodeCache& cache,
        Hash256 const& hash,
        std::chrono::milliseconds timeout)
    {
        std::lock_guard lock(cache.mutex_);
        auto [it, inserted] = cache.store_.try_emplace(hash);
        EXPECT_TRUE(inserted || !it->second.present);

        auto round = std::make_shared<NodeCache::NodeFetchRound>();
        round->phase = NodeCache::NodeFetchRound::Phase::response;
        round->response_possible = true;
        round->submissions_closed = true;
        round->servants_done_signal = SerializedTimerSignal::create(
            cache.io_, asio::steady_timer::time_point::max());
        round->operation_id = ++cache.next_node_operation_id_;
        round->participant_authority = SharedHistoryFetch::create(
            HistoryFetchId{round->operation_id}, round->operation_id);
        EXPECT_TRUE(round->participant_authority);
        cache.active_node_operations_.emplace(round->operation_id, round);

        it->second.present = false;
        it->second.signal = SerializedTimerSignal::create(cache.io_, timeout);
        it->second.round = round;
        it->second.round_opened_at = std::chrono::steady_clock::now();
        it->second.last_fetch_at = it->second.round_opened_at;
        return round->participant_authority;
    }

    static asio::awaitable<std::shared_ptr<std::vector<uint8_t> const>>
    wait_for_hash_scoped(
        std::shared_ptr<NodeCache> cache,
        Hash256 hash,
        uint32_t ledger_seq,
        std::shared_ptr<PeerSet> peers,
        std::shared_ptr<catl::core::RequestScope> scope,
        std::shared_ptr<std::atomic<bool>> cancel)
    {
        co_return co_await cache->ensure_present(
            hash,
            make_hash(0xBB),
            2,
            SHAMapNodeID::root(),
            hash,
            1,
            nullptr,
            {},
            std::move(peers),
            ledger_seq,
            std::move(cancel),
            nullptr,
            nullptr,
            nullptr,
            nullptr,
            std::move(scope));
    }

    static void
    set_after_node_waiter_attached(NodeCache& cache, std::function<void()> hook)
    {
        cache.after_node_waiter_attached_for_test_ = std::move(hook);
    }

    /// Get the signal for an entry (for testing expiry behavior).
    static std::shared_ptr<SerializedTimerSignal>
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
        entry.fetch.reset();
        cache.touch_header_lru(ledger_seq);
        cache.evict_headers_if_needed();
    }

    static void
    create_in_flight_header(
        NodeCache& cache,
        uint32_t ledger_seq,
        std::chrono::milliseconds timeout)
    {
        std::shared_ptr<NodeCache::HeaderFetchState> fetch;
        {
            std::lock_guard lock(cache.mutex_);
            auto& entry = cache.header_cache_[ledger_seq];
            entry.present = false;
            fetch = std::make_shared<NodeCache::HeaderFetchState>();
            fetch->completion_signal = SerializedTimerSignal::create(
                cache.io_, asio::steady_timer::time_point::max());
            fetch->deadline_signal =
                SerializedTimerSignal::create(cache.io_, timeout);
            fetch->driver_done_signal = SerializedTimerSignal::create(
                cache.io_, asio::steady_timer::time_point::max());
            fetch->deadline_done_signal = SerializedTimerSignal::create(
                cache.io_, asio::steady_timer::time_point::max());
            fetch->operation_id = ++cache.next_header_fetch_id_;
            fetch->epoch = cache.header_epoch_;
            fetch->driver_frame_done = true;
            fetch->driver_done_signal->complete();
            cache.active_header_fetches_.emplace(
                fetch->operation_id, fetch);
            entry.fetch = fetch;
            cache.touch_header_lru(ledger_seq);
            cache.evict_headers_if_needed();
        }

        auto weak = cache.weak_from_this();
        asio::co_spawn(
            cache.header_state_strand_,
            [weak, ledger_seq, fetch]() -> asio::awaitable<void> {
                auto const ec = co_await fetch->deadline_signal->wait();
                if (ec == asio::error::operation_aborted)
                    co_return;
                if (auto self = weak.lock())
                    self->finish_header_fetch(ledger_seq, fetch);
            },
            [weak, fetch](std::exception_ptr) {
                if (auto self = weak.lock())
                    self->on_header_servant_done(fetch, false);
            });
    }

    static bool
    has_header(NodeCache& cache, uint32_t ledger_seq)
    {
        std::lock_guard lock(cache.mutex_);
        return cache.header_cache_.count(ledger_seq) > 0;
    }

    static std::optional<SharedHistoryFetchSnapshot>
    header_participant_snapshot(NodeCache& cache, uint32_t ledger_seq)
    {
        std::shared_ptr<SharedHistoryFetch> authority;
        {
            std::lock_guard lock(cache.mutex_);
            auto const it = cache.header_cache_.find(ledger_seq);
            if (it == cache.header_cache_.end() || !it->second.fetch)
                return std::nullopt;
            authority = it->second.fetch->participant_authority;
        }
        if (!authority)
            return std::nullopt;
        return authority->snapshot();
    }

    static size_t
    active_header_fetches(NodeCache& cache)
    {
        return cache.active_header_fetches_.size();
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
    std::shared_ptr<std::vector<uint8_t> const> result;
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

    std::shared_ptr<std::vector<uint8_t> const> r1, r2;
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

    std::shared_ptr<std::vector<uint8_t> const> result;
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
    std::shared_ptr<std::vector<uint8_t> const> second_result;
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

    std::shared_ptr<std::vector<uint8_t> const> result;
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

    std::shared_ptr<std::vector<uint8_t> const> result;
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
    std::shared_ptr<std::vector<uint8_t> const> result;
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

TEST(NodeCacheIntegration, ClearWakesHeaderJoiner)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto cache = NodeCache::create(io, {
        .max_entries = 1024,
        .fetch_timeout = 5s,
    });
    constexpr uint32_t ledger_seq = 123457u;
    NodeCacheTestAccess::create_in_flight_header(
        *cache, ledger_seq, std::chrono::hours(1));

    bool done = false;
    std::optional<Error> terminal_error;
    asio::co_spawn(
        io,
        [&, cache]() -> asio::awaitable<void> {
            try
            {
                (void)co_await cache->get_header(ledger_seq, nullptr, nullptr);
            }
            catch (PeerClientException const& e)
            {
                terminal_error = e.error;
            }
            done = true;
        },
        asio::detached);

    asio::steady_timer clear_timer(io, 10ms);
    clear_timer.async_wait([&io, cache](boost::system::error_code ec) {
        if (!ec)
            asio::co_spawn(io, cache->co_clear_headers(), asio::detached);
    });

    io.run();

    EXPECT_TRUE(done);
    ASSERT_TRUE(terminal_error);
    EXPECT_EQ(*terminal_error, Error::Cancelled);
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
    std::shared_ptr<std::vector<uint8_t> const> result;
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
    std::shared_ptr<std::vector<uint8_t> const> second_result;

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

TEST(NodeCacheIntegration, CoalescedNodeWaitersKeepDistinctDemandOwners)
{
    asio::io_context io;
    auto cache = NodeCache::create(
        io,
        {
            .max_entries = 1024,
            .fetch_timeout = std::chrono::seconds(5),
        });
    auto first_peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto second_peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 21337,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto const hash = make_hash(0xD1);
    constexpr uint32_t ledger_seq = 654324u;
    auto authority = NodeCacheTestAccess::create_participant_in_flight(
        *cache, hash, std::chrono::seconds(5));
    ASSERT_TRUE(authority);

    auto const deadline =
        catl::core::RequestScope::Clock::now() + std::chrono::seconds(5);
    auto first_scope = catl::core::RequestScope::create(deadline);
    auto second_scope = catl::core::RequestScope::create(deadline);
    auto first_cancel = std::make_shared<std::atomic<bool>>(false);
    auto second_cancel = std::make_shared<std::atomic<bool>>(false);
    std::atomic<int> completions = 0;
    bool first_returned_null = false;
    bool second_returned_null = false;
    bool admission_timed_out = false;
    std::optional<SharedHistoryFetchSnapshot> sibling_snapshot;

    asio::co_spawn(
        io,
        [&, cache, first_peers, first_scope, first_cancel]()
            -> asio::awaitable<void> {
            auto result = co_await NodeCacheTestAccess::wait_for_hash_scoped(
                cache,
                hash,
                ledger_seq,
                first_peers,
                first_scope,
                first_cancel);
            first_returned_null = !result;
            sibling_snapshot = authority->snapshot();
            second_scope->cancel();
            second_cancel->store(true, std::memory_order_release);
            completions.fetch_add(1, std::memory_order_relaxed);
        },
        asio::detached);
    asio::co_spawn(
        io,
        [&, cache, second_peers, second_scope, second_cancel]()
            -> asio::awaitable<void> {
            auto result = co_await NodeCacheTestAccess::wait_for_hash_scoped(
                cache,
                hash,
                ledger_seq,
                second_peers,
                second_scope,
                second_cancel);
            second_returned_null = !result;
            completions.fetch_add(1, std::memory_order_relaxed);
            cache->clear_nodes();
        },
        asio::detached);
    asio::co_spawn(
        io,
        [&, authority, first_scope, first_cancel, second_scope, second_cancel]()
            -> asio::awaitable<void> {
            auto ex = co_await asio::this_coro::executor;
            for (int attempt = 0; attempt < 200; ++attempt)
            {
                if (authority->snapshot().active_interests == 2)
                {
                    first_scope->cancel();
                    first_cancel->store(true, std::memory_order_release);
                    co_return;
                }
                asio::steady_timer timer(ex, std::chrono::milliseconds(1));
                boost::system::error_code ec;
                co_await timer.async_wait(
                    asio::redirect_error(asio::use_awaitable, ec));
            }
            admission_timed_out = true;
            first_scope->cancel();
            second_scope->cancel();
            first_cancel->store(true, std::memory_order_release);
            second_cancel->store(true, std::memory_order_release);
        },
        asio::detached);

    io.run();

    EXPECT_FALSE(admission_timed_out);
    EXPECT_EQ(completions.load(std::memory_order_relaxed), 2);
    EXPECT_TRUE(first_returned_null);
    EXPECT_TRUE(second_returned_null);
    ASSERT_TRUE(sibling_snapshot);
    EXPECT_EQ(sibling_snapshot->interests, 2u);
    EXPECT_EQ(sibling_snapshot->active_interests, 1u);
    EXPECT_FALSE(sibling_snapshot->terminal);
    auto const final_snapshot = authority->snapshot();
    EXPECT_EQ(final_snapshot.interests, 2u);
    EXPECT_EQ(final_snapshot.active_interests, 0u);
    EXPECT_EQ(
        final_snapshot.terminal, SharedHistoryFetchTerminal::no_interests);
}

TEST(NodeCacheIntegration, NodeResultPublishesToEveryCoalescedParticipant)
{
    asio::io_context io;
    auto cache = NodeCache::create(
        io,
        {
            .max_entries = 1024,
            .fetch_timeout = std::chrono::seconds(5),
        });
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto const hash = make_hash(0xD2);
    constexpr uint32_t ledger_seq = 654325u;
    auto authority = NodeCacheTestAccess::create_participant_in_flight(
        *cache, hash, std::chrono::seconds(5));
    ASSERT_TRUE(authority);

    auto const deadline =
        catl::core::RequestScope::Clock::now() + std::chrono::seconds(5);
    auto first_scope = catl::core::RequestScope::create(deadline);
    auto second_scope = catl::core::RequestScope::create(deadline);
    std::array<std::shared_ptr<std::vector<uint8_t> const>, 2> results;
    std::atomic<int> completions = 0;
    bool admission_timed_out = false;

    for (std::size_t index = 0; index < results.size(); ++index)
    {
        auto scope = index == 0 ? first_scope : second_scope;
        asio::co_spawn(
            io,
            [&, cache, peers, scope, index]() -> asio::awaitable<void> {
                results[index] =
                    co_await NodeCacheTestAccess::wait_for_hash_scoped(
                        cache,
                        hash,
                        ledger_seq,
                        peers,
                        scope,
                        std::make_shared<std::atomic<bool>>(false));
                if (completions.fetch_add(1, std::memory_order_relaxed) == 1)
                {
                    first_scope->cancel();
                    second_scope->cancel();
                }
            },
            asio::detached);
    }
    asio::co_spawn(
        io,
        [&, cache, authority]() -> asio::awaitable<void> {
            auto ex = co_await asio::this_coro::executor;
            for (int attempt = 0; attempt < 200; ++attempt)
            {
                if (authority->snapshot().active_interests == 2)
                {
                    cache->insert_and_notify(hash, std::vector<uint8_t>(88, 7));
                    co_return;
                }
                asio::steady_timer timer(ex, std::chrono::milliseconds(1));
                boost::system::error_code ec;
                co_await timer.async_wait(
                    asio::redirect_error(asio::use_awaitable, ec));
            }
            admission_timed_out = true;
            cache->clear_nodes();
        },
        asio::detached);

    io.run();

    EXPECT_FALSE(admission_timed_out);
    EXPECT_EQ(completions.load(std::memory_order_relaxed), 2);
    for (auto const& result : results)
    {
        ASSERT_TRUE(result);
        EXPECT_EQ(result->size(), 88u);
    }
    auto const snapshot = authority->snapshot();
    EXPECT_EQ(snapshot.interests, 2u);
    EXPECT_EQ(snapshot.active_interests, 0u);
    EXPECT_EQ(snapshot.terminal, SharedHistoryFetchTerminal::completed);
    EXPECT_EQ(snapshot.publication_revision, 1u);
}

TEST(
    NodeCacheIntegration,
    NodeResultBeforeParticipantRegistrationReplaysWithoutFalseRefusal)
{
    asio::io_context io;
    auto cache = NodeCache::create(
        io,
        {
            .max_entries = 1024,
            .fetch_timeout = std::chrono::seconds(5),
        });
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto const hash = make_hash(0xD4);
    constexpr uint32_t ledger_seq = 654327u;
    auto authority = NodeCacheTestAccess::create_participant_in_flight(
        *cache, hash, std::chrono::seconds(5));
    ASSERT_TRUE(authority);

    auto scope = catl::core::RequestScope::create(
        catl::core::RequestScope::Clock::now() + std::chrono::seconds(5));
    NodeCacheTestAccess::set_after_node_waiter_attached(
        *cache, [cache, hash]() {
            cache->insert_and_notify(hash, std::vector<uint8_t>(88, 9));
        });

    std::shared_ptr<std::vector<uint8_t> const> result;
    std::exception_ptr failure;
    asio::co_spawn(
        io,
        [&, cache, peers, scope]() -> asio::awaitable<void> {
            try
            {
                result = co_await NodeCacheTestAccess::wait_for_hash_scoped(
                    cache,
                    hash,
                    ledger_seq,
                    peers,
                    scope,
                    std::make_shared<std::atomic<bool>>(false));
            }
            catch (...)
            {
                failure = std::current_exception();
            }
            scope->cancel();
        },
        asio::detached);
    io.run();

    EXPECT_FALSE(failure);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->size(), 88u);
    auto const snapshot = authority->snapshot();
    EXPECT_EQ(snapshot.interests, 0u);
    EXPECT_EQ(snapshot.terminal, SharedHistoryFetchTerminal::completed);
    EXPECT_EQ(snapshot.publication_revision, 1u);
}

TEST(NodeCacheIntegration, NodeClearPublishesExactEpochReplacement)
{
    asio::io_context io;
    auto cache = NodeCache::create(
        io,
        {
            .max_entries = 1024,
            .fetch_timeout = std::chrono::seconds(5),
        });
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto const hash = make_hash(0xD3);
    constexpr uint32_t ledger_seq = 654326u;
    auto authority = NodeCacheTestAccess::create_participant_in_flight(
        *cache, hash, std::chrono::seconds(5));
    ASSERT_TRUE(authority);

    auto scope = catl::core::RequestScope::create(
        catl::core::RequestScope::Clock::now() + std::chrono::seconds(5));
    bool returned_null = false;
    bool admission_timed_out = false;
    asio::co_spawn(
        io,
        [&, cache, peers, scope]() -> asio::awaitable<void> {
            auto result = co_await NodeCacheTestAccess::wait_for_hash_scoped(
                cache,
                hash,
                ledger_seq,
                peers,
                scope,
                std::make_shared<std::atomic<bool>>(false));
            returned_null = !result;
            scope->cancel();
        },
        asio::detached);
    asio::co_spawn(
        io,
        [&, cache, authority]() -> asio::awaitable<void> {
            auto ex = co_await asio::this_coro::executor;
            for (int attempt = 0; attempt < 200; ++attempt)
            {
                if (authority->snapshot().active_interests == 1)
                {
                    cache->clear_nodes();
                    co_return;
                }
                asio::steady_timer timer(ex, std::chrono::milliseconds(1));
                boost::system::error_code ec;
                co_await timer.async_wait(
                    asio::redirect_error(asio::use_awaitable, ec));
            }
            admission_timed_out = true;
            cache->clear_nodes();
        },
        asio::detached);

    io.run();

    EXPECT_FALSE(admission_timed_out);
    EXPECT_TRUE(returned_null);
    auto const snapshot = authority->snapshot();
    EXPECT_EQ(snapshot.interests, 1u);
    EXPECT_EQ(snapshot.active_interests, 0u);
    EXPECT_EQ(snapshot.terminal, SharedHistoryFetchTerminal::replaced);
    EXPECT_EQ(snapshot.publication_revision, 1u);
}

TEST(NodeCacheIntegration, CancelledHeaderWaiterDoesNotOwnSharedFetch)
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
    auto session =
        std::make_shared<NodeBatchTestSession>("127.0.0.1:51235");
    session->attach_strand(io);
    PeerSetTestAccess::insert_ready_peer_with_generation(io, peers, session);

    bool done = false;
    bool cancel_branch_won = false;
    bool fetch_survived_caller = false;
    std::optional<SharedHistoryFetchSnapshot> participant_snapshot;
    constexpr uint32_t ledger_seq = 50u;
    auto scope = catl::core::RequestScope::create(
        catl::core::RequestScope::Clock::now() + std::chrono::seconds(5));
    LedgerDemandRef parent;

    asio::co_spawn(
        io,
        [&, cache, peers, scope]() -> asio::awaitable<void> {
            parent = co_await peers->co_get_or_create_ledger_demand(
                scope,
                LedgerDemandOptions{
                    .ledger_seq = ledger_seq,
                    .remaining_new_dials =
                        std::numeric_limits<std::size_t>::max(),
                    .remaining_promotions =
                        std::numeric_limits<std::size_t>::max(),
                    .remaining_blind_uses =
                        std::numeric_limits<std::size_t>::max()});
            auto ex = co_await asio::this_coro::executor;
            auto result =
                co_await asio::experimental::make_parallel_group(
                    asio::co_spawn(
                        ex,
                        cache->get_header(
                            ledger_seq, peers, PeerSessionPtr{}, scope),
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
            scope->cancel();
            done = true;
        },
        asio::detached);

    asio::steady_timer inspect_timer(io, std::chrono::milliseconds(20));
    inspect_timer.async_wait([&](boost::system::error_code const& ec) {
        if (ec)
            return;
        fetch_survived_caller =
            NodeCacheTestAccess::has_header(*cache, ledger_seq);
        participant_snapshot = NodeCacheTestAccess::header_participant_snapshot(
            *cache, ledger_seq);
        asio::co_spawn(io, cache->co_clear_headers(), asio::detached);
    });

    io.run();

    EXPECT_TRUE(done);
    EXPECT_TRUE(cancel_branch_won);
    EXPECT_TRUE(fetch_survived_caller);
    ASSERT_TRUE(participant_snapshot);
    EXPECT_EQ(participant_snapshot->interests, 1u);
    EXPECT_EQ(participant_snapshot->active_interests, 0u);
    EXPECT_FALSE(participant_snapshot->terminal)
        << "the cancelled rider owns no interest, while the already-submitted "
           "exact use still owns the shared physical fetch";
    EXPECT_FALSE(NodeCacheTestAccess::has_header(*cache, ledger_seq));

    io.restart();
    std::optional<LedgerDemandSnapshot> demand_snapshot;
    asio::co_spawn(
        io,
        [&, peers]() -> asio::awaitable<void> {
            demand_snapshot = co_await peers->co_ledger_demand_snapshot(parent);
        },
        asio::detached);
    io.run();
    ASSERT_TRUE(demand_snapshot);
    EXPECT_EQ(demand_snapshot->acquisitions, 1u);
    EXPECT_EQ(demand_snapshot->active_acquisitions, 0u);
    EXPECT_EQ(demand_snapshot->terminal, LedgerDemandTerminal::cancelled);
}

TEST(NodeCacheIntegration, CoalescedHeaderWaitersKeepDistinctDemandOwners)
{
    asio::io_context io;
    auto cache = NodeCache::create(
        io,
        {
            .max_entries = 1024,
            .fetch_timeout = std::chrono::seconds(5),
        });
    auto first_peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto second_peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 21337,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto first_session =
        std::make_shared<NodeBatchTestSession>("127.0.0.1:51236");
    auto second_session =
        std::make_shared<NodeBatchTestSession>("127.0.0.1:51237");
    first_session->attach_strand(io);
    second_session->attach_strand(io);
    PeerSetTestAccess::insert_ready_peer_with_generation(
        io, first_peers, first_session);
    PeerSetTestAccess::insert_ready_peer_with_generation(
        io, second_peers, second_session);
    constexpr uint32_t ledger_seq = 51u;
    auto const deadline =
        catl::core::RequestScope::Clock::now() + std::chrono::seconds(5);
    auto first_scope = catl::core::RequestScope::create(deadline);
    auto second_scope = catl::core::RequestScope::create(deadline);
    LedgerDemandRef first_parent;
    LedgerDemandRef second_parent;
    bool first_done = false;
    bool second_done = false;
    bool first_timer_won = false;
    bool second_timer_won = false;

    auto launch = [&](std::shared_ptr<PeerSet> peers,
                      std::shared_ptr<catl::core::RequestScope> scope,
                      LedgerDemandRef& parent,
                      bool& timer_won,
                      bool& done,
                      std::chrono::milliseconds delay) {
        auto* parent_out = &parent;
        auto* timer_won_out = &timer_won;
        auto* done_out = &done;
        asio::co_spawn(
            io,
            [cache,
             peers = std::move(peers),
             scope = std::move(scope),
             parent_out,
             timer_won_out,
             done_out,
             delay]() -> asio::awaitable<void> {
                *parent_out = co_await peers->co_get_or_create_ledger_demand(
                    scope,
                    LedgerDemandOptions{
                        .ledger_seq = ledger_seq,
                        .remaining_new_dials =
                            std::numeric_limits<std::size_t>::max(),
                        .remaining_promotions =
                            std::numeric_limits<std::size_t>::max(),
                        .remaining_blind_uses =
                            std::numeric_limits<std::size_t>::max()});
                auto ex = co_await asio::this_coro::executor;
                auto result =
                    co_await asio::experimental::make_parallel_group(
                        asio::co_spawn(
                            ex,
                            cache->get_header(
                                ledger_seq, peers, PeerSessionPtr{}, scope),
                            asio::deferred),
                        asio::co_spawn(
                            ex,
                            [delay]() -> asio::awaitable<void> {
                                auto ex = co_await asio::this_coro::executor;
                                asio::steady_timer timer(ex, delay);
                                boost::system::error_code ec;
                                co_await timer.async_wait(asio::redirect_error(
                                    asio::use_awaitable, ec));
                            },
                            asio::deferred))
                        .async_wait(
                            asio::experimental::wait_for_one(),
                            asio::use_awaitable);
                *timer_won_out = (std::get<0>(result)[0] == 1);
                scope->cancel();
                *done_out = true;
            },
            asio::detached);
    };

    launch(
        first_peers,
        first_scope,
        first_parent,
        first_timer_won,
        first_done,
        std::chrono::milliseconds(10));
    launch(
        second_peers,
        second_scope,
        second_parent,
        second_timer_won,
        second_done,
        std::chrono::milliseconds(15));

    std::optional<SharedHistoryFetchSnapshot> participant_snapshot;
    asio::steady_timer inspect_timer(io, std::chrono::milliseconds(30));
    inspect_timer.async_wait([&](boost::system::error_code const& ec) {
        if (ec)
            return;
        participant_snapshot = NodeCacheTestAccess::header_participant_snapshot(
            *cache, ledger_seq);
        asio::co_spawn(io, cache->co_clear_headers(), asio::detached);
    });
    io.run();

    EXPECT_TRUE(first_done);
    EXPECT_TRUE(second_done);
    EXPECT_TRUE(first_timer_won);
    EXPECT_TRUE(second_timer_won);
    ASSERT_TRUE(participant_snapshot);
    EXPECT_EQ(participant_snapshot->interests, 2u);
    EXPECT_EQ(participant_snapshot->active_interests, 0u);
    EXPECT_FALSE(participant_snapshot->terminal)
        << "detached riders do not erase already-submitted exact uses";

    io.restart();
    std::optional<LedgerDemandSnapshot> first_snapshot;
    std::optional<LedgerDemandSnapshot> second_snapshot;
    asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
            first_snapshot =
                co_await first_peers->co_ledger_demand_snapshot(first_parent);
            second_snapshot =
                co_await second_peers->co_ledger_demand_snapshot(second_parent);
        },
        asio::detached);
    io.run();

    ASSERT_TRUE(first_snapshot);
    ASSERT_TRUE(second_snapshot);
    EXPECT_EQ(first_snapshot->acquisitions, 1u);
    EXPECT_EQ(second_snapshot->acquisitions, 1u);
    EXPECT_EQ(first_snapshot->active_acquisitions, 0u);
    EXPECT_EQ(second_snapshot->active_acquisitions, 0u);
    EXPECT_EQ(first_snapshot->terminal, LedgerDemandTerminal::cancelled);
    EXPECT_EQ(second_snapshot->terminal, LedgerDemandTerminal::cancelled);
}

TEST(NodeCacheIntegration, ParticipantAdmissionRefusalRetiresUnlaunchedFetch)
{
    asio::io_context io;
    auto cache = NodeCache::create(
        io,
        {
            .max_entries = 1024,
            .fetch_timeout = std::chrono::seconds(5),
        });
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_active_ledger_demands = 0,
        });
    constexpr uint32_t ledger_seq = 654323u;
    auto scope = catl::core::RequestScope::create(
        catl::core::RequestScope::Clock::now() + std::chrono::seconds(5));
    bool done = false;
    bool refused = false;

    asio::co_spawn(
        io,
        [&, cache, peers, scope]() -> asio::awaitable<void> {
            try
            {
                (void)co_await cache->get_header(
                    ledger_seq, peers, PeerSessionPtr{}, scope);
            }
            catch (PeerClientException const&)
            {
                refused = true;
            }
            done = true;
        },
        asio::detached);
    io.run();

    EXPECT_TRUE(done);
    EXPECT_TRUE(refused);
    EXPECT_FALSE(NodeCacheTestAccess::has_header(*cache, ledger_seq));
    EXPECT_EQ(NodeCacheTestAccess::active_header_fetches(*cache), 0u);
}
