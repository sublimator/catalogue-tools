#include "peer-client-test-access.h"
#include "peer-set-test-access.h"
#include <catl/peer-client/ledger-fetch-coordinator.h>
#include <catl/peer-client/node-cache.h>
#include <catl/peer-client/peer-set.h>

#include <catl/core/context-executor.h>
#include <catl/core/request-context.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

using namespace catl::peer_client;
namespace asio = boost::asio;

static_assert(std::is_move_constructible_v<PeerGenerationPin>);
static_assert(std::is_move_assignable_v<PeerGenerationPin>);
static_assert(!std::is_copy_constructible_v<PeerGenerationPin>);
static_assert(!std::is_copy_assignable_v<PeerGenerationPin>);

class FakePeerSession : public PeerSession,
                        public std::enable_shared_from_this<FakePeerSession>
{
public:
    using SendGetNodesHandler = std::function<void(
        Hash256 const&,
        int,
        std::vector<SHAMapNodeID> const&,
        std::function<void(boost::system::error_code)>)>;
    using PingHandler = std::function<void(Callback<PingResult>)>;
    using GetLedgerHeaderHandler =
        std::function<void(uint32_t, Callback<LedgerHeaderResult>)>;

    FakePeerSession(
        asio::io_context& io,
        std::string endpoint,
        bool ready,
        uint32_t current_seq,
        uint32_t first_seq,
        uint32_t last_seq)
        : strand_(asio::make_strand(io))
        , endpoint_(std::move(endpoint))
        , ready_(ready)
        , current_seq_(current_seq)
        , first_seq_(first_seq)
        , last_seq_(last_seq)
    {
    }

    std::string const&
    endpoint() const override
    {
        return endpoint_;
    }

    bool
    is_ready() const override
    {
        return ready_.load(std::memory_order_relaxed);
    }

    uint32_t
    peer_ledger_seq() const override
    {
        return current_seq_.load(std::memory_order_relaxed);
    }

    uint32_t
    peer_first_seq() const override
    {
        return first_seq_.load(std::memory_order_relaxed);
    }

    uint32_t
    peer_last_seq() const override
    {
        return last_seq_.load(std::memory_order_relaxed);
    }

    size_t
    pending_count() const override
    {
        return pending_.load(std::memory_order_relaxed);
    }

    void
    disconnect() override
    {
        ready_.store(false, std::memory_order_relaxed);
    }

    asio::strand<asio::io_context::executor_type>&
    strand() override
    {
        return strand_;
    }

    void
    set_unsolicited_handler(UnsolicitedHandler handler) override
    {
        std::lock_guard lock(mutex_);
        unsolicited_handler_ = std::move(handler);
    }

    void
    set_node_response_handler(NodeResponseHandler handler) override
    {
        std::lock_guard lock(mutex_);
        node_response_handler_ = std::move(handler);
    }

    void
    get_ledger_header(
        uint32_t ledger_seq,
        Callback<LedgerHeaderResult> callback,
        RequestOptions) override
    {
        GetLedgerHeaderHandler handler;
        bool invoke_inline = false;
        {
            std::lock_guard lock(mutex_);
            handler = get_ledger_header_handler_;
            invoke_inline = invoke_header_handler_inline_;
        }
        if (handler)
        {
            if (invoke_inline)
            {
                handler(ledger_seq, std::move(callback));
                return;
            }
            asio::post(
                strand_,
                [handler = std::move(handler),
                 ledger_seq,
                 callback = std::move(callback)]() mutable {
                    handler(ledger_seq, std::move(callback));
                });
            return;
        }
        complete(std::move(callback), Error::Timeout, LedgerHeaderResult{});
    }

    void
    get_ledger_header(
        Hash256 const&,
        Callback<LedgerHeaderResult> callback,
        RequestOptions) override
    {
        complete(std::move(callback), Error::Timeout, LedgerHeaderResult{});
    }

    void
    get_state_nodes(
        Hash256 const&,
        std::vector<SHAMapNodeID> const&,
        Callback<LedgerNodesResult> callback,
        RequestOptions) override
    {
        complete(std::move(callback), Error::Timeout, LedgerNodesResult{});
    }

    void
    get_tx_nodes(
        Hash256 const&,
        std::vector<SHAMapNodeID> const&,
        Callback<LedgerNodesResult> callback,
        RequestOptions) override
    {
        complete(std::move(callback), Error::Timeout, LedgerNodesResult{});
    }

    void
    get_tx_proof_path(
        Hash256 const&,
        Hash256 const&,
        Callback<ProofPathResult> callback,
        RequestOptions) override
    {
        complete(std::move(callback), Error::Timeout, ProofPathResult{});
    }

    void
    get_state_proof_path(
        Hash256 const&,
        Hash256 const&,
        Callback<ProofPathResult> callback,
        RequestOptions) override
    {
        complete(std::move(callback), Error::Timeout, ProofPathResult{});
    }

    void
    ping(Callback<PingResult> callback, RequestOptions) override
    {
        PingHandler handler;
        {
            std::lock_guard lock(mutex_);
            handler = ping_handler_;
        }

        if (handler)
        {
            asio::post(
                strand_,
                [handler = std::move(handler),
                 callback = std::move(callback)]() mutable {
                    handler(std::move(callback));
                });
            return;
        }

        complete(std::move(callback), Error::Timeout, PingResult{});
    }

    void
    send_get_nodes(
        Hash256 const& ledger_hash,
        int type,
        std::vector<SHAMapNodeID> const& node_ids,
        std::function<void(boost::system::error_code)> on_error) override
    {
        SendGetNodesHandler handler;
        {
            std::lock_guard lock(mutex_);
            handler = send_get_nodes_handler_;
        }

        asio::post(
            strand_,
            [handler = std::move(handler),
             ledger_hash,
             type,
             node_ids,
             on_error = std::move(on_error)]() mutable {
                if (handler)
                {
                    handler(ledger_hash, type, node_ids, std::move(on_error));
                    return;
                }

                if (on_error)
                    on_error({});
            });
    }

    bool
    submit_node_batch(NodeBatchCommand command, NodeBatchWriteHandler) override
    {
        node_batch_node_count_.store(
            command.node_ids().size(), std::memory_order_relaxed);
        node_batch_tree_.store(
            static_cast<int>(command.tree()), std::memory_order_relaxed);
        node_batch_submissions_.fetch_add(1, std::memory_order_relaxed);
        return node_batch_accepts_.load(std::memory_order_relaxed);
    }

    bool
    acknowledge_node_batch_receipt(NodeBatchReceiptAck) override
    {
        return false;
    }

    void
    set_send_get_nodes_handler(SendGetNodesHandler handler)
    {
        std::lock_guard lock(mutex_);
        send_get_nodes_handler_ = std::move(handler);
    }

    void
    set_node_batch_accepts(bool accepts)
    {
        node_batch_accepts_.store(accepts, std::memory_order_relaxed);
    }

    std::size_t
    node_batch_submissions() const
    {
        return node_batch_submissions_.load(std::memory_order_relaxed);
    }

    std::size_t
    node_batch_node_count() const
    {
        return node_batch_node_count_.load(std::memory_order_relaxed);
    }

    int
    node_batch_tree() const
    {
        return node_batch_tree_.load(std::memory_order_relaxed);
    }

    void
    set_ping_handler(PingHandler handler)
    {
        std::lock_guard lock(mutex_);
        ping_handler_ = std::move(handler);
    }

    void
    set_get_ledger_header_handler(GetLedgerHeaderHandler handler)
    {
        std::lock_guard lock(mutex_);
        get_ledger_header_handler_ = std::move(handler);
    }

    void
    set_invoke_header_handler_inline(bool value)
    {
        std::lock_guard lock(mutex_);
        invoke_header_handler_inline_ = value;
    }

    void
    set_ready(bool ready)
    {
        ready_.store(ready, std::memory_order_relaxed);
    }

private:
    template <class Result>
    void
    complete(Callback<Result> callback, Error error, Result result)
    {
        pending_.fetch_add(1, std::memory_order_relaxed);
        asio::post(
            strand_,
            [this,
             callback = std::move(callback),
             error,
             result = std::move(result)]() mutable {
                pending_.fetch_sub(1, std::memory_order_relaxed);
                callback(error, std::move(result));
            });
    }

    asio::strand<asio::io_context::executor_type> strand_;
    std::string endpoint_;
    std::atomic<bool> ready_{false};
    std::atomic<uint32_t> current_seq_{0};
    std::atomic<uint32_t> first_seq_{0};
    std::atomic<uint32_t> last_seq_{0};
    std::atomic<size_t> pending_{0};
    std::atomic<bool> node_batch_accepts_{false};
    std::atomic<std::size_t> node_batch_submissions_{0};
    std::atomic<std::size_t> node_batch_node_count_{0};
    std::atomic<int> node_batch_tree_{0};

    std::mutex mutex_;
    UnsolicitedHandler unsolicited_handler_;
    NodeResponseHandler node_response_handler_;
    PingHandler ping_handler_;
    GetLedgerHeaderHandler get_ledger_header_handler_;
    bool invoke_header_handler_inline_ = false;
    SendGetNodesHandler send_get_nodes_handler_;
};

class NodeCacheTestAccess
{
public:
    static std::shared_ptr<SerializedTimerSignal>
    signal(NodeCache& cache, Hash256 const& hash)
    {
        std::lock_guard lock(cache.mutex_);
        auto const it = cache.store_.find(hash);
        return it == cache.store_.end() ? nullptr : it->second.signal;
    }

    static void
    make_round_stale(NodeCache& cache, Hash256 const& hash)
    {
        std::lock_guard lock(cache.mutex_);
        auto const it = cache.store_.find(hash);
        ASSERT_NE(it, cache.store_.end());
        it->second.round_opened_at =
            std::chrono::steady_clock::now() - std::chrono::hours(1);
    }

    static void
    seed_stale_round(NodeCache& cache, Hash256 const& hash)
    {
        std::lock_guard lock(cache.mutex_);
        auto& entry = cache.store_[hash];
        entry.signal =
            SerializedTimerSignal::create(cache.io_, std::chrono::hours(1));
        entry.round_opened_at =
            std::chrono::steady_clock::now() - std::chrono::hours(2);
        entry.last_fetch_at = entry.round_opened_at;
    }

    static bool
    early_managed_outcome_survives_reserved_return(NodeCache& cache)
    {
        auto round = std::make_shared<NodeCache::NodeFetchRound>();
        round->phase = NodeCache::NodeFetchRound::Phase::admission;
        round->admission_deadline =
            SerializedTimerSignal::create(cache.io_, std::chrono::hours(1));
        std::shared_ptr<SerializedTimerSignal> admission_signal;
        {
            std::lock_guard lock(cache.mutex_);
            auto& entry = cache.store_[Hash256{}];
            entry.round = round;
            entry.signal =
                SerializedTimerSignal::create(cache.io_, std::chrono::hours(1));
            admission_signal = entry.signal;
        }

        auto const slot = cache.register_managed_submission(
            Hash256{}, round, "early-outcome:1");
        if (!slot)
            return false;
        cache.settle_managed_submission(
            Hash256{},
            round,
            *slot,
            NodeCache::ManagedSubmissionOutcome::written);
        cache.commit_managed_submission(Hash256{}, round, *slot);

        {
            std::lock_guard lock(cache.mutex_);
            auto const it = cache.store_.find(Hash256{});
            if (it == cache.store_.end() ||
                it->second.signal != admission_signal ||
                round->phase != NodeCache::NodeFetchRound::Phase::admission)
            {
                return false;
            }
        }
        if (!cache.retain_node_fanout_pin(Hash256{}, round, {}))
            return false;

        auto const fanout_slot = cache.register_managed_submission(
            Hash256{}, round, "early-outcome-fanout:1");
        if (!fanout_slot)
            return false;
        cache.commit_managed_submission(Hash256{}, round, *fanout_slot);
        cache.settle_managed_submission(
            Hash256{},
            round,
            *fanout_slot,
            NodeCache::ManagedSubmissionOutcome::not_written);
        cache.close_managed_submissions(Hash256{}, round);

        std::lock_guard lock(cache.mutex_);
        auto const it = cache.store_.find(Hash256{});
        return it != cache.store_.end() && it->second.round == round &&
            it->second.signal != admission_signal &&
            round->phase == NodeCache::NodeFetchRound::Phase::response &&
            round->dispatches[*slot].state ==
            NodeCache::NodeFetchRound::DispatchState::written &&
            round->dispatches[*fanout_slot].state ==
            NodeCache::NodeFetchRound::DispatchState::not_written;
    }

    static bool
    admission_expiry_before_refusal_rolls_back(NodeCache& cache)
    {
        auto round = std::make_shared<NodeCache::NodeFetchRound>();
        round->phase = NodeCache::NodeFetchRound::Phase::admission;
        round->admission_deadline =
            SerializedTimerSignal::create(cache.io_, std::chrono::hours(1));
        std::shared_ptr<SerializedTimerSignal> admission_signal;
        {
            std::lock_guard lock(cache.mutex_);
            auto& entry = cache.store_[Hash256{}];
            entry.round = round;
            entry.signal =
                SerializedTimerSignal::create(cache.io_, std::chrono::hours(1));
            admission_signal = entry.signal;
        }

        auto const before = cache.managed_indeterminate_.load();
        auto const slot = cache.register_managed_submission(
            Hash256{}, round, "timeout-before-refusal:1");
        if (!slot)
            return false;
        cache.expire_node_fetch_round(
            Hash256{}, round, round->admission_deadline);
        {
            std::lock_guard lock(cache.mutex_);
            if (!round->admission_expired || !round->submissions_closed ||
                round->phase != NodeCache::NodeFetchRound::Phase::admission ||
                round->dispatches[*slot].state !=
                    NodeCache::NodeFetchRound::DispatchState::submitting)
            {
                return false;
            }
        }
        cache.rollback_managed_submission(Hash256{}, round, *slot);

        std::lock_guard lock(cache.mutex_);
        auto const it = cache.store_.find(Hash256{});
        return it != cache.store_.end() && !it->second.signal &&
            round->phase == NodeCache::NodeFetchRound::Phase::retired &&
            round->dispatch_count == 0 &&
            cache.managed_indeterminate_.load() == before;
    }

    static bool
    admission_expiry_before_reserved_return_becomes_indeterminate(
        NodeCache& cache)
    {
        auto round = std::make_shared<NodeCache::NodeFetchRound>();
        round->phase = NodeCache::NodeFetchRound::Phase::admission;
        round->admission_deadline =
            SerializedTimerSignal::create(cache.io_, std::chrono::hours(1));
        std::shared_ptr<SerializedTimerSignal> admission_signal;
        {
            std::lock_guard lock(cache.mutex_);
            auto& entry = cache.store_[Hash256{}];
            entry.round = round;
            entry.signal =
                SerializedTimerSignal::create(cache.io_, std::chrono::hours(1));
            admission_signal = entry.signal;
        }

        auto const slot = cache.register_managed_submission(
            Hash256{}, round, "timeout-before-reserved:1");
        if (!slot)
            return false;
        cache.expire_node_fetch_round(
            Hash256{}, round, round->admission_deadline);
        cache.commit_managed_submission(Hash256{}, round, *slot);

        std::lock_guard lock(cache.mutex_);
        auto const it = cache.store_.find(Hash256{});
        return it != cache.store_.end() &&
            it->second.signal != admission_signal &&
            round->phase == NodeCache::NodeFetchRound::Phase::response &&
            round->dispatches[*slot].state ==
            NodeCache::NodeFetchRound::DispatchState::indeterminate;
    }

    static bool
    waiter_timeout_defers_admission_to_round_owner(NodeCache& cache)
    {
        auto round = std::make_shared<NodeCache::NodeFetchRound>();
        round->phase = NodeCache::NodeFetchRound::Phase::admission;
        round->admission_deadline =
            SerializedTimerSignal::create(cache.io_, std::chrono::hours(1));
        std::shared_ptr<SerializedTimerSignal> admission_signal;
        NodeCache::WaitRegistration wait;
        {
            std::lock_guard lock(cache.mutex_);
            auto& entry = cache.store_[Hash256{}];
            entry.round = round;
            entry.signal =
                SerializedTimerSignal::create(cache.io_, std::chrono::hours(1));
            admission_signal = entry.signal;
            wait = cache.attach_waiter_locked(entry);
        }

        if (cache.handle_node_waiter_timeout(
                Hash256{}, round, admission_signal, wait))
        {
            return false;
        }
        std::lock_guard lock(cache.mutex_);
        auto const it = cache.store_.find(Hash256{});
        return it != cache.store_.end() &&
            it->second.signal == admission_signal &&
            round->phase == NodeCache::NodeFetchRound::Phase::admission;
    }

    static asio::awaitable<void>
    header_barrier(std::shared_ptr<NodeCache> cache)
    {
        co_await asio::co_spawn(
            cache->header_state_strand_,
            []() -> asio::awaitable<void> { co_return; },
            asio::use_awaitable);
    }

    static void
    set_node_round_hooks(
        NodeCache& cache,
        std::function<void()> before_send_commit,
        std::function<void()> after_waiter_attached)
    {
        cache.before_node_send_commit_for_test_ = std::move(before_send_commit);
        cache.after_node_waiter_attached_for_test_ =
            std::move(after_waiter_attached);
    }

    static void
    set_node_deadline_hooks(
        NodeCache& cache,
        std::function<void()> servant_body,
        std::function<void()> after_deadline_wait,
        std::function<void()> after_deadline_commit,
        std::function<void()> after_exact_retirement)
    {
        cache.node_deadline_servant_hook_for_test_ = std::move(servant_body);
        cache.after_node_deadline_wait_for_test_ =
            std::move(after_deadline_wait);
        cache.after_node_deadline_commit_for_test_ =
            std::move(after_deadline_commit);
        cache.node_round_retired_hook_for_test_ =
            std::move(after_exact_retirement);
    }

    static void
    set_managed_ingress_hooks(
        NodeCache& cache,
        std::function<void()> after_slot_registered,
        std::function<void()> after_ingress_reconciled)
    {
        cache.after_managed_slot_registered_for_test_ =
            std::move(after_slot_registered);
        cache.after_managed_ingress_reconciled_for_test_ =
            std::move(after_ingress_reconciled);
    }

    static void
    set_before_node_fanout_hook(
        NodeCache& cache,
        std::function<void()> before_fanout)
    {
        cache.before_node_fanout_for_test_ = std::move(before_fanout);
    }

    static void
    submit_node_fetch(
        NodeCache& cache,
        PeerSession& peer,
        PeerGenerationPin pin = {})
    {
        Hash256 ledger_hash{};
        Hash256 target_key{};
        cache.send_fetch(
            Hash256{},
            ledger_hash,
            1,
            SHAMapNodeID::root(),
            target_key,
            1,
            peer,
            std::move(pin),
            nullptr);
    }

    static asio::awaitable<std::shared_ptr<std::vector<uint8_t>>>
    ensure_node(
        NodeCache& cache,
        PeerSession& peer,
        PeerGenerationPin pin,
        std::unordered_set<std::string>* attempted_peers,
        bool* initiated_fetch,
        std::shared_ptr<std::atomic<bool>> cancel = nullptr,
        std::unordered_set<std::string>* written_peers = nullptr,
        std::shared_ptr<PeerSet> peers = nullptr)
    {
        co_return co_await cache.ensure_present(
            Hash256{},
            Hash256{},
            1,
            SHAMapNodeID::root(),
            Hash256{},
            1,
            &peer,
            std::move(pin),
            std::move(peers),
            5500,
            std::move(cancel),
            attempted_peers,
            nullptr,
            initiated_fetch,
            written_peers);
    }
};

namespace {

Hash256
hash_with_byte(uint8_t value)
{
    std::array<uint8_t, 32> bytes{};
    bytes.fill(value);
    return Hash256(bytes);
}

std::vector<uint8_t>
make_leaf_wire(Hash256 const& key)
{
    std::vector<uint8_t> wire = {0xAA, 0xBB, 0xCC};
    wire.insert(wire.end(), key.data(), key.data() + Hash256::size());
    wire.push_back(static_cast<uint8_t>(WireType::TransactionWithMeta));
    return wire;
}

template <class T>
T
wait_for_future(std::future<T>& fut, std::chrono::seconds timeout)
{
    auto const status = fut.wait_for(timeout);
    EXPECT_EQ(status, std::future_status::ready);
    return fut.get();
}

}  // namespace

TEST(PeerSessionInterface, NodeCacheRoutesOnlyManagedPinsThroughCoordinator)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto session = std::make_shared<FakePeerSession>(
        io, "managed-node-route:1", true, 5500, 5400, 5600);
    PeerSetTestAccess::insert_connected_peer(io, peers, session);
    auto coordinator = LedgerFetchCoordinator::create(io, peers);
    ASSERT_TRUE(coordinator);
    auto cache = NodeCache::create(io, {}, coordinator);
    session->set_node_batch_accepts(true);

    std::atomic<std::size_t> raw_writes = 0;
    session->set_send_get_nodes_handler(
        [&raw_writes](
            Hash256 const&,
            int,
            std::vector<SHAMapNodeID> const&,
            std::function<void(boost::system::error_code)>) { ++raw_writes; });

    auto managed_pin =
        PeerSetTestAccess::make_peer_pin(io, peers, session->endpoint());
    ASSERT_TRUE(managed_pin);
    NodeCacheTestAccess::submit_node_fetch(
        *cache, *session, std::move(managed_pin));

    auto managed = cache->stats();
    EXPECT_EQ(managed.managed_fetches_reserved, 1u);
    EXPECT_EQ(managed.managed_fetches_refused, 0u);
    EXPECT_EQ(raw_writes.load(), 0u);

    for (int i = 0; i < 256 && session->node_batch_submissions() == 0; ++i)
    {
        io.restart();
        io.poll_one();
    }
    EXPECT_EQ(session->node_batch_submissions(), 1u);
    EXPECT_EQ(session->node_batch_node_count(), 1u);
    EXPECT_EQ(
        session->node_batch_tree(), static_cast<int>(NodeTree::transaction));

    EXPECT_EQ(
        coordinator->fence(), LedgerFetchCoordinator::Fence::drain_queued);
    auto refused_pin =
        PeerSetTestAccess::make_peer_pin(io, peers, session->endpoint());
    ASSERT_TRUE(refused_pin);
    NodeCacheTestAccess::submit_node_fetch(
        *cache, *session, std::move(refused_pin));

    auto after_refusal = cache->stats();
    EXPECT_EQ(after_refusal.managed_fetches_reserved, 1u);
    EXPECT_EQ(after_refusal.managed_fetches_refused, 1u);
    EXPECT_EQ(raw_writes.load(), 0u);

    // Managed mode fails closed when its generation capability is absent.
    NodeCacheTestAccess::submit_node_fetch(*cache, *session);
    auto after_unpinned = cache->stats();
    EXPECT_EQ(after_unpinned.managed_fetches_reserved, 1u);
    EXPECT_EQ(after_unpinned.managed_fetches_refused, 2u);
    EXPECT_EQ(after_unpinned.raw_fetches_queued, 0u);
    EXPECT_EQ(raw_writes.load(), 0u);

    // Raw transport remains available only when selected at construction.
    auto raw_cache = NodeCache::create(io, {});
    NodeCacheTestAccess::submit_node_fetch(*raw_cache, *session);
    for (int i = 0; i < 64 && raw_writes.load() == 0; ++i)
    {
        io.restart();
        io.poll_one();
    }

    auto after_raw = raw_cache->stats();
    EXPECT_EQ(after_raw.raw_fetches_queued, 1u);
    EXPECT_EQ(after_raw.managed_fetches_reserved, 0u);
    EXPECT_EQ(after_raw.managed_fetches_refused, 0u);
    EXPECT_EQ(raw_writes.load(), 1u);
}

TEST(PeerSessionInterface, ManagedOutcomeMayBeatTheReservedReturn)
{
    asio::io_context io;
    auto cache = NodeCache::create(io, {});
    EXPECT_TRUE(
        NodeCacheTestAccess::early_managed_outcome_survives_reserved_return(
            *cache));
    cache->clear_nodes();
}

TEST(PeerSessionInterface, ManagedAdmissionExpiryWaitsForRefusedReturn)
{
    asio::io_context io;
    auto cache = NodeCache::create(io, {});
    EXPECT_TRUE(NodeCacheTestAccess::admission_expiry_before_refusal_rolls_back(
        *cache));
    cache->clear_nodes();
}

TEST(PeerSessionInterface, ManagedAdmissionExpiryReconcilesReservedReturn)
{
    asio::io_context io;
    auto cache = NodeCache::create(io, {});
    EXPECT_TRUE(
        NodeCacheTestAccess::
            admission_expiry_before_reserved_return_becomes_indeterminate(
                *cache));
    cache->clear_nodes();
}

TEST(PeerSessionInterface, ManagedWaiterCannotRetireAdmissionPhase)
{
    asio::io_context io;
    auto cache = NodeCache::create(io, {});
    EXPECT_TRUE(
        NodeCacheTestAccess::waiter_timeout_defers_admission_to_round_owner(
            *cache));
    cache->clear_nodes();
}

TEST(PeerSessionInterface, NodeResponseHandlerDoesNotOwnManagedCacheGraph)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto session = std::make_shared<FakePeerSession>(
        io, "managed-cache-lifetime:1", true, 5500, 5400, 5600);
    PeerSetTestAccess::insert_connected_peer(io, peers, session);
    auto coordinator = LedgerFetchCoordinator::create(io, peers);
    ASSERT_TRUE(coordinator);
    auto cache = NodeCache::create(io, {}, coordinator);
    std::weak_ptr<NodeCache> weak_cache = cache;
    std::weak_ptr<LedgerFetchCoordinator> weak_coordinator = coordinator;

    std::atomic<std::size_t> raw_writes = 0;
    session->set_send_get_nodes_handler(
        [&raw_writes](
            Hash256 const&,
            int,
            std::vector<SHAMapNodeID> const&,
            std::function<void(boost::system::error_code)>) { ++raw_writes; });

    // ProofEngine is the lifecycle owner; NodeCache retains only the configured
    // managed-mode tag plus a weak capability.
    coordinator.reset();
    ASSERT_TRUE(weak_coordinator.expired());

    auto pin = PeerSetTestAccess::make_peer_pin(io, peers, session->endpoint());
    ASSERT_TRUE(pin);
    NodeCacheTestAccess::submit_node_fetch(*cache, *session, std::move(pin));
    auto stats = cache->stats();
    EXPECT_EQ(stats.managed_fetches_refused, 1u);
    EXPECT_EQ(stats.raw_fetches_queued, 0u);
    EXPECT_EQ(raw_writes.load(), 0u);

    cache.reset();
    EXPECT_TRUE(weak_cache.expired());
}

TEST(PeerSessionInterface, ManagedWriteStartsASeparateResponseDeadline)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto client = PeerClientTestAccess::make(io, State::Ready, 1);
    asio::ssl::context ssl{asio::ssl::context::tlsv12_client};
    PeerClientTestAccess::seed_open_connection(*client, ssl);
    PeerClientTestAccess::seed_successful_packet_writes(*client);
    auto const generation =
        PeerSetTestAccess::insert_ready_peer_with_generation(io, peers, client);
    ASSERT_EQ(generation, 1u);

    auto coordinator = LedgerFetchCoordinator::create(io, peers);
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .fetch_timeout = 20ms,
            .managed_admission_timeout = 1s,
        },
        coordinator);
    auto pin = PeerSetTestAccess::make_peer_pin_for_session(io, peers, client);
    ASSERT_TRUE(pin);

    bool initiated_fetch = false;
    std::unordered_set<std::string> reserved;
    std::unordered_set<std::string> written;
    io.restart();
    auto result = asio::co_spawn(
        io,
        NodeCacheTestAccess::ensure_node(
            *cache,
            *client,
            std::move(pin),
            &reserved,
            &initiated_fetch,
            nullptr,
            &written),
        asio::use_future);
    auto const started = std::chrono::steady_clock::now();
    io.run_for(200ms);
    auto const elapsed = std::chrono::steady_clock::now() - started;

    ASSERT_EQ(result.wait_for(0ms), std::future_status::ready);
    EXPECT_LT(elapsed, 500ms)
        << "the response deadline must finish well before the one-second "
           "admission budget";
    EXPECT_EQ(result.get(), nullptr);
    EXPECT_TRUE(initiated_fetch);
    EXPECT_EQ(reserved.count(client->endpoint()), 1u);
    EXPECT_EQ(written.count(client->endpoint()), 1u);
    EXPECT_EQ(cache->stats().managed_fetches_reserved, 1u);
    EXPECT_EQ(cache->stats().raw_fetches_queued, 0u);
}

TEST(PeerSessionInterface, ManagedNotWrittenRetiresWithoutWaitingForADeadline)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto client = PeerClientTestAccess::make(io, State::Ready, 1);
    PeerSetTestAccess::insert_ready_peer_with_generation(io, peers, client);
    auto coordinator = LedgerFetchCoordinator::create(io, peers);
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .fetch_timeout = 1s,
            .managed_admission_timeout = 1s,
        },
        coordinator);
    auto pin = PeerSetTestAccess::make_peer_pin_for_session(io, peers, client);
    ASSERT_TRUE(pin);

    bool initiated_fetch = false;
    std::unordered_set<std::string> reserved;
    std::unordered_set<std::string> written;
    io.restart();
    auto result = asio::co_spawn(
        io,
        NodeCacheTestAccess::ensure_node(
            *cache,
            *client,
            std::move(pin),
            &reserved,
            &initiated_fetch,
            nullptr,
            &written),
        asio::use_future);
    for (int step = 0;
         step < 1024 && result.wait_for(0ms) != std::future_status::ready;
         ++step)
    {
        io.restart();
        io.poll_one();
    }

    ASSERT_EQ(result.wait_for(0ms), std::future_status::ready)
        << "an exact not-written receipt must not consume either timeout";
    EXPECT_EQ(result.get(), nullptr);
    EXPECT_TRUE(initiated_fetch);
    EXPECT_EQ(reserved.count(client->endpoint()), 1u);
    EXPECT_TRUE(written.empty());
    EXPECT_EQ(NodeCacheTestAccess::signal(*cache, Hash256{}), nullptr);
}

TEST(PeerSessionInterface, LostManagedOutcomeGetsAnUnblamedResponseWindow)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto session = std::make_shared<FakePeerSession>(
        io, "managed-lost-outcome:1", true, 5500, 5400, 5600);
    session->set_node_batch_accepts(true);
    PeerSetTestAccess::insert_connected_peer(io, peers, session);
    auto coordinator = LedgerFetchCoordinator::create(io, peers);
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .fetch_timeout = 100ms,
            .managed_admission_timeout = 5ms,
        },
        coordinator);
    auto pin = PeerSetTestAccess::make_peer_pin(io, peers, session->endpoint());
    ASSERT_TRUE(pin);

    bool initiated_fetch = false;
    std::unordered_set<std::string> reserved;
    std::unordered_set<std::string> written;
    io.restart();
    auto result = asio::co_spawn(
        io,
        NodeCacheTestAccess::ensure_node(
            *cache,
            *session,
            std::move(pin),
            &reserved,
            &initiated_fetch,
            nullptr,
            &written),
        asio::use_future);
    io.run_for(25ms);

    EXPECT_EQ(result.wait_for(0ms), std::future_status::timeout)
        << "admission expiry must not retire the round before its fresh "
           "response window";
    EXPECT_EQ(cache->stats().managed_indeterminate, 1u);
    EXPECT_NE(NodeCacheTestAccess::signal(*cache, Hash256{}), nullptr);

    io.restart();
    io.run_for(200ms);

    ASSERT_EQ(result.wait_for(0ms), std::future_status::ready)
        << "admission expiry must compose a fresh bounded response window";
    EXPECT_EQ(result.get(), nullptr);
    EXPECT_TRUE(initiated_fetch);
    EXPECT_EQ(reserved.count(session->endpoint()), 1u);
    EXPECT_TRUE(written.empty())
        << "unknown publication is selection evidence, never peer blame";
    EXPECT_EQ(NodeCacheTestAccess::signal(*cache, Hash256{}), nullptr);
}

TEST(
    PeerSessionInterface,
    ManagedAdmissionDeadlineCannotWakeWaiterBeforeOwnerCommit)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto work = asio::make_work_guard(io);
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto session = std::make_shared<FakePeerSession>(
        io, "managed-admission-refusal-race:1", true, 5500, 5400, 5600);
    PeerSetTestAccess::insert_connected_peer(io, peers, session);
    auto coordinator = LedgerFetchCoordinator::create(io, peers);
    ASSERT_EQ(
        coordinator->fence(), LedgerFetchCoordinator::Fence::drain_queued);
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .fetch_timeout = 1s,
            .managed_admission_timeout = 10ms,
        },
        coordinator);
    auto pin = PeerSetTestAccess::make_peer_pin(io, peers, session->endpoint());
    ASSERT_TRUE(pin);

    std::promise<void> slot_registered;
    auto slot_registered_future = slot_registered.get_future();
    std::promise<void> deadline_observed;
    auto deadline_observed_future = deadline_observed.get_future();
    std::promise<void> deadline_committed;
    auto deadline_committed_future = deadline_committed.get_future();
    std::promise<void> ingress_reconciled;
    auto ingress_reconciled_future = ingress_reconciled.get_future();
    std::promise<void> release_slot;
    auto release_slot_future = release_slot.get_future().share();
    std::promise<void> release_deadline;
    auto release_deadline_future = release_deadline.get_future().share();
    std::atomic<bool> slot_reported{false};
    std::atomic<bool> deadline_reported{false};
    std::atomic<bool> deadline_commit_reported{false};
    std::atomic<bool> ingress_reported{false};

    NodeCacheTestAccess::set_managed_ingress_hooks(
        *cache,
        [&]() {
            if (!slot_reported.exchange(true))
            {
                slot_registered.set_value();
                release_slot_future.wait();
            }
        },
        [&]() {
            if (!ingress_reported.exchange(true))
                ingress_reconciled.set_value();
        });
    NodeCacheTestAccess::set_node_deadline_hooks(
        *cache,
        {},
        [&]() {
            if (!deadline_reported.exchange(true))
            {
                deadline_observed.set_value();
                release_deadline_future.wait();
            }
        },
        [&]() {
            if (!deadline_commit_reported.exchange(true))
                deadline_committed.set_value();
        },
        {});

    bool initiated_fetch = false;
    std::unordered_set<std::string> reserved;
    auto result = asio::co_spawn(
        io,
        NodeCacheTestAccess::ensure_node(
            *cache, *session, std::move(pin), &reserved, &initiated_fetch),
        asio::use_future);

    std::thread first_runner([&]() { io.run(); });
    std::thread second_runner([&]() { io.run(); });
    auto stop = [&]() {
        work.reset();
        io.stop();
        first_runner.join();
        second_runner.join();
    };
    auto release_all = [&]() {
        release_deadline.set_value();
        release_slot.set_value();
    };

    auto const slot_status = slot_registered_future.wait_for(2s);
    auto const deadline_status = deadline_observed_future.wait_for(2s);
    EXPECT_EQ(slot_status, std::future_status::ready);
    EXPECT_EQ(deadline_status, std::future_status::ready);
    if (slot_status != std::future_status::ready ||
        deadline_status != std::future_status::ready)
    {
        release_all();
        stop();
        return;
    }

    EXPECT_EQ(result.wait_for(0ms), std::future_status::timeout)
        << "observing the private admission deadline must not complete the "
           "waiter-visible phase latch";

    release_deadline.set_value();
    auto const committed_status = deadline_committed_future.wait_for(2s);
    EXPECT_EQ(committed_status, std::future_status::ready);
    if (committed_status != std::future_status::ready)
    {
        release_slot.set_value();
        stop();
        return;
    }
    EXPECT_EQ(result.wait_for(0ms), std::future_status::timeout)
        << "a submitting slot keeps the admission owner alive after expiry";
    EXPECT_EQ(cache->stats().managed_indeterminate, 0u)
        << "a provisional submitting slot is not publication evidence";

    release_slot.set_value();
    auto const ingress_status = ingress_reconciled_future.wait_for(2s);
    EXPECT_EQ(ingress_status, std::future_status::ready);
    auto const result_status = result.wait_for(2s);
    EXPECT_EQ(result_status, std::future_status::ready);
    if (ingress_status != std::future_status::ready ||
        result_status != std::future_status::ready)
    {
        stop();
        return;
    }
    EXPECT_EQ(result.get(), nullptr);
    EXPECT_FALSE(initiated_fetch)
        << "a synchronously refused publication never initiated transport";
    EXPECT_TRUE(reserved.empty());
    EXPECT_EQ(cache->stats().managed_fetches_refused, 1u);
    EXPECT_EQ(cache->stats().managed_indeterminate, 0u);
    EXPECT_EQ(NodeCacheTestAccess::signal(*cache, Hash256{}), nullptr);

    stop();
}

TEST(
    PeerSessionInterface,
    ManagedReservedReturnAfterAdmissionExpiryGetsOneResponseWindow)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto work = asio::make_work_guard(io);
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto session = std::make_shared<FakePeerSession>(
        io, "managed-admission-reserved-race:1", true, 5500, 5400, 5600);
    session->set_node_batch_accepts(true);
    PeerSetTestAccess::insert_connected_peer(io, peers, session);
    auto coordinator = LedgerFetchCoordinator::create(io, peers);
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .fetch_timeout = 250ms,
            .managed_admission_timeout = 10ms,
        },
        coordinator);
    auto pin = PeerSetTestAccess::make_peer_pin(io, peers, session->endpoint());
    ASSERT_TRUE(pin);

    std::promise<void> slot_registered;
    auto slot_registered_future = slot_registered.get_future();
    std::promise<void> deadline_observed;
    auto deadline_observed_future = deadline_observed.get_future();
    std::promise<void> deadline_committed;
    auto deadline_committed_future = deadline_committed.get_future();
    std::promise<void> ingress_reconciled;
    auto ingress_reconciled_future = ingress_reconciled.get_future();
    std::promise<void> release_slot;
    auto release_slot_future = release_slot.get_future().share();
    std::promise<void> release_deadline;
    auto release_deadline_future = release_deadline.get_future().share();
    std::atomic<bool> slot_reported{false};
    std::atomic<bool> deadline_reported{false};
    std::atomic<bool> deadline_commit_reported{false};
    std::atomic<bool> ingress_reported{false};

    NodeCacheTestAccess::set_managed_ingress_hooks(
        *cache,
        [&]() {
            if (!slot_reported.exchange(true))
            {
                slot_registered.set_value();
                release_slot_future.wait();
            }
        },
        [&]() {
            if (!ingress_reported.exchange(true))
                ingress_reconciled.set_value();
        });
    NodeCacheTestAccess::set_node_deadline_hooks(
        *cache,
        {},
        [&]() {
            if (!deadline_reported.exchange(true))
            {
                deadline_observed.set_value();
                release_deadline_future.wait();
            }
        },
        [&]() {
            if (!deadline_commit_reported.exchange(true))
                deadline_committed.set_value();
        },
        {});

    bool initiated_fetch = false;
    std::unordered_set<std::string> reserved;
    auto result = asio::co_spawn(
        io,
        NodeCacheTestAccess::ensure_node(
            *cache, *session, std::move(pin), &reserved, &initiated_fetch),
        asio::use_future);

    auto const started = std::chrono::steady_clock::now();
    std::thread first_runner([&]() { io.run(); });
    std::thread second_runner([&]() { io.run(); });
    auto stop = [&]() {
        work.reset();
        io.stop();
        first_runner.join();
        second_runner.join();
    };

    auto const slot_status = slot_registered_future.wait_for(2s);
    auto const deadline_status = deadline_observed_future.wait_for(2s);
    EXPECT_EQ(slot_status, std::future_status::ready);
    EXPECT_EQ(deadline_status, std::future_status::ready);
    if (slot_status != std::future_status::ready ||
        deadline_status != std::future_status::ready)
    {
        release_deadline.set_value();
        release_slot.set_value();
        stop();
        return;
    }

    release_deadline.set_value();
    auto const committed_status = deadline_committed_future.wait_for(2s);
    EXPECT_EQ(committed_status, std::future_status::ready);
    if (committed_status != std::future_status::ready)
    {
        release_slot.set_value();
        stop();
        return;
    }
    EXPECT_EQ(result.wait_for(0ms), std::future_status::timeout);
    EXPECT_EQ(cache->stats().managed_indeterminate, 0u);

    release_slot.set_value();
    auto const ingress_status = ingress_reconciled_future.wait_for(2s);
    EXPECT_EQ(ingress_status, std::future_status::ready);
    if (ingress_status != std::future_status::ready)
    {
        stop();
        return;
    }
    EXPECT_EQ(result.wait_for(50ms), std::future_status::timeout)
        << "the reserved return must open a fresh response window";
    EXPECT_EQ(cache->stats().managed_fetches_reserved, 1u);
    EXPECT_EQ(cache->stats().managed_indeterminate, 1u);
    EXPECT_NE(NodeCacheTestAccess::signal(*cache, Hash256{}), nullptr);

    auto const result_status = result.wait_for(2s);
    EXPECT_EQ(result_status, std::future_status::ready);
    if (result_status != std::future_status::ready)
    {
        stop();
        return;
    }
    auto const elapsed = std::chrono::steady_clock::now() - started;
    EXPECT_GE(elapsed, 200ms);
    EXPECT_EQ(result.get(), nullptr);
    EXPECT_TRUE(initiated_fetch);
    EXPECT_EQ(reserved.count(session->endpoint()), 1u);
    EXPECT_EQ(session->node_batch_submissions(), 1u);
    EXPECT_EQ(NodeCacheTestAccess::signal(*cache, Hash256{}), nullptr);

    stop();
}

TEST(PeerSessionInterface, ManagedFanoutUsesTheStableRoundIdentity)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    std::array<std::shared_ptr<FakePeerSession>, 6> sessions;
    for (std::size_t i = 0; i < sessions.size(); ++i)
    {
        sessions[i] = std::make_shared<FakePeerSession>(
            io,
            "managed-fanout-" + std::to_string(i) + ":1",
            true,
            5500,
            5400,
            5600);
        sessions[i]->set_node_batch_accepts(true);
        PeerSetTestAccess::insert_connected_peer(io, peers, sessions[i]);
    }

    auto coordinator = LedgerFetchCoordinator::create(io, peers);
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .fetch_timeout = 20ms,
            .managed_admission_timeout = 10ms,
        },
        coordinator);
    auto pin = PeerSetTestAccess::make_peer_pin(
        io, peers, sessions.front()->endpoint());
    ASSERT_TRUE(pin);

    bool initiated_fetch = false;
    std::unordered_set<std::string> attempted;
    std::unordered_set<std::string> written;
    auto result = asio::co_spawn(
        io,
        NodeCacheTestAccess::ensure_node(
            *cache,
            *sessions.front(),
            std::move(pin),
            &attempted,
            &initiated_fetch,
            nullptr,
            &written,
            peers),
        asio::use_future);
    io.run_for(200ms);

    ASSERT_EQ(result.wait_for(0ms), std::future_status::ready);
    EXPECT_EQ(result.get(), nullptr);
    EXPECT_TRUE(initiated_fetch);
    EXPECT_EQ(cache->stats().managed_fetches_reserved, 3u);
    EXPECT_EQ(attempted.size(), 3u);
    EXPECT_TRUE(written.empty());
    std::size_t submissions = 0;
    for (auto const& session : sessions)
        submissions += session->node_batch_submissions();
    EXPECT_EQ(submissions, 3u);
    EXPECT_EQ(sessions.front()->node_batch_submissions(), 1u);
}

TEST(PeerSessionInterface, ManagedUnknownPublicationConsumesCoveredPeerRetry)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    std::array<std::shared_ptr<FakePeerSession>, 2> sessions;
    for (std::size_t i = 0; i < sessions.size(); ++i)
    {
        sessions[i] = std::make_shared<FakePeerSession>(
            io,
            "managed-covered-retry-" + std::to_string(i) + ":1",
            true,
            5500,
            5400,
            5600);
        sessions[i]->set_node_batch_accepts(true);
        PeerSetTestAccess::insert_connected_peer(io, peers, sessions[i]);
    }

    auto coordinator = LedgerFetchCoordinator::create(io, peers);
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .fetch_timeout = 5ms,
            .managed_admission_timeout = 5ms,
            .max_walk_peer_retries = 1,
            .fetch_stale_multiplier = 1,
            .peer_refresh_timeout_secs = 0,
        },
        coordinator);
    auto pin = PeerSetTestAccess::make_peer_pin(
        io, peers, sessions.front()->endpoint());
    ASSERT_TRUE(pin);

    auto result = asio::co_spawn(
        io,
        cache->walk_to(
            hash_with_byte(0x71),
            hash_with_byte(0x72),
            hash_with_byte(0x73),
            5500,
            1,
            peers,
            std::move(pin)),
        asio::use_future);
    io.run_for(500ms);

    ASSERT_EQ(result.wait_for(0ms), std::future_status::ready);
    EXPECT_FALSE(result.get().found);
    EXPECT_EQ(cache->stats().managed_fetches_reserved, 3u)
        << "the first two-peer round plus one covered-peer retry must consume "
           "the bounded retry budget";
    std::size_t submissions = 0;
    for (auto const& session : sessions)
        submissions += session->node_batch_submissions();
    EXPECT_GE(submissions, 3u)
        << "coordinator publication repair may retransmit each reserved row";
}

TEST(PeerSessionInterface, ManagedFanoutExceptionClosesSubmissionSet)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto session = std::make_shared<FakePeerSession>(
        io, "managed-fanout-exception:1", true, 5500, 5400, 5600);
    PeerSetTestAccess::insert_connected_peer(io, peers, session);
    auto coordinator = LedgerFetchCoordinator::create(io, peers);
    auto cache = NodeCache::create(io, {}, coordinator);
    ASSERT_EQ(
        coordinator->fence(), LedgerFetchCoordinator::Fence::drain_queued);
    auto pin = PeerSetTestAccess::make_peer_pin(io, peers, session->endpoint());
    ASSERT_TRUE(pin);

    NodeCacheTestAccess::set_before_node_fanout_hook(
        *cache, []() { throw std::runtime_error("forced fanout failure"); });
    bool initiated_fetch = false;
    std::unordered_set<std::string> attempted;
    auto result = asio::co_spawn(
        io,
        NodeCacheTestAccess::ensure_node(
            *cache,
            *session,
            std::move(pin),
            &attempted,
            &initiated_fetch,
            nullptr,
            nullptr,
            peers),
        asio::use_future);
    io.run_for(1s);

    ASSERT_EQ(result.wait_for(0ms), std::future_status::ready);
    EXPECT_THROW(result.get(), std::runtime_error);
    EXPECT_EQ(NodeCacheTestAccess::signal(*cache, Hash256{}), nullptr)
        << "exceptional fanout exit must not leave registration open";
}

TEST(PeerSessionInterface, ManagedRefusalDoesNotLeavePhantomFetchRound)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto session = std::make_shared<FakePeerSession>(
        io, "managed-node-refusal:1", true, 5500, 5400, 5600);
    PeerSetTestAccess::insert_connected_peer(io, peers, session);
    auto coordinator = LedgerFetchCoordinator::create(io, peers);
    ASSERT_TRUE(coordinator);
    auto cache = NodeCache::create(io, {}, coordinator);
    EXPECT_EQ(
        coordinator->fence(), LedgerFetchCoordinator::Fence::drain_queued);

    for (bool const cancelled : {false, true})
    {
        io.restart();
        auto pin =
            PeerSetTestAccess::make_peer_pin(io, peers, session->endpoint());
        ASSERT_TRUE(pin);
        bool initiated_fetch = false;
        std::unordered_set<std::string> attempted_peers;
        auto cancel = std::make_shared<std::atomic<bool>>(false);
        NodeCacheTestAccess::set_node_round_hooks(
            *cache,
            cancelled ? std::function<void()>([cancel] {
                cancel->store(true, std::memory_order_relaxed);
            })
                      : std::function<void()>{},
            {});
        auto result = asio::co_spawn(
            io,
            NodeCacheTestAccess::ensure_node(
                *cache,
                *session,
                std::move(pin),
                &attempted_peers,
                &initiated_fetch,
                std::move(cancel)),
            asio::use_future);
        for (int step = 0; step < 1024 &&
             result.wait_for(std::chrono::milliseconds(0)) !=
                 std::future_status::ready;
             ++step)
        {
            io.restart();
            io.poll_one();
        }

        ASSERT_EQ(
            result.wait_for(std::chrono::milliseconds(0)),
            std::future_status::ready);
        EXPECT_EQ(result.get(), nullptr);
        EXPECT_FALSE(initiated_fetch);
        EXPECT_TRUE(attempted_peers.empty());
        EXPECT_EQ(NodeCacheTestAccess::signal(*cache, Hash256{}), nullptr);
    }
    auto stats = cache->stats();
    EXPECT_EQ(stats.managed_fetches_reserved, 0u);
    EXPECT_EQ(stats.managed_fetches_refused, 2u);
}

TEST(PeerSessionInterface, WaitForAnyPeerWakesOnReadyFake)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });

    std::promise<PeerGenerationPin> promise;
    auto future = promise.get_future();

    asio::co_spawn(
        io,
        [peers, &promise]() -> asio::awaitable<void> {
            try
            {
                promise.set_value(co_await peers->wait_for_any_peer(2));
            }
            catch (...)
            {
                promise.set_exception(std::current_exception());
            }
        },
        asio::detached);

    std::thread runner([&]() { io.run(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    auto peer = std::make_shared<FakePeerSession>(
        io, "fake-ready:51235", true, 120, 100, 140);
    PeerSetTestAccess::insert_connected_peer(io, peers, peer);

    auto selected = wait_for_future(future, std::chrono::seconds(2));
    ASSERT_TRUE(selected);
    EXPECT_EQ(selected->endpoint(), "fake-ready:51235");

    io.stop();
    runner.join();
}

TEST(PeerSessionInterface, WaitForPeerIgnoresUnsuitableThenSelectsCovering)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });

    constexpr uint32_t wanted = 500;
    std::promise<PeerGenerationPin> promise;
    auto future = promise.get_future();

    asio::co_spawn(
        io,
        [peers, &promise]() -> asio::awaitable<void> {
            try
            {
                promise.set_value(co_await peers->wait_for_peer(wanted, 2));
            }
            catch (...)
            {
                promise.set_exception(std::current_exception());
            }
        },
        asio::detached);

    std::thread runner([&]() { io.run(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(30));

    auto not_ready = std::make_shared<FakePeerSession>(
        io, "fake-not-ready:1", false, 500, 450, 550);
    auto out_of_range = std::make_shared<FakePeerSession>(
        io, "fake-out-range:1", true, 900, 800, 850);
    auto covering = std::make_shared<FakePeerSession>(
        io, "fake-covering:1", true, 520, 450, 650);

    PeerSetTestAccess::insert_connected_peer(io, peers, not_ready);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    PeerSetTestAccess::insert_connected_peer(io, peers, out_of_range);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    PeerSetTestAccess::insert_connected_peer(io, peers, covering);

    auto selected = wait_for_future(future, std::chrono::seconds(3));
    ASSERT_TRUE(selected);
    EXPECT_EQ(selected->endpoint(), "fake-covering:1");

    io.stop();
    runner.join();
}

TEST(PeerSessionInterface, WaitForPeerContinuesAfterDisconnectWake)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });

    constexpr uint32_t wanted = 700;
    std::promise<PeerGenerationPin> promise;
    auto future = promise.get_future();

    asio::co_spawn(
        io,
        [peers, &promise]() -> asio::awaitable<void> {
            try
            {
                promise.set_value(co_await peers->wait_for_peer(wanted, 3));
            }
            catch (...)
            {
                promise.set_exception(std::current_exception());
            }
        },
        asio::detached);

    std::thread runner([&]() { io.run(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(30));

    auto short_lived = std::make_shared<FakePeerSession>(
        io, "fake-short-lived:1", true, 700, 650, 800);
    auto stable = std::make_shared<FakePeerSession>(
        io, "fake-stable:1", true, 710, 650, 820);

    PeerSetTestAccess::insert_then_disconnect(io, peers, short_lived);
    std::this_thread::sleep_for(std::chrono::milliseconds(80));
    PeerSetTestAccess::insert_connected_peer(io, peers, stable);

    auto selected = wait_for_future(future, std::chrono::seconds(3));
    ASSERT_TRUE(selected);
    EXPECT_EQ(selected->endpoint(), "fake-stable:1");

    io.stop();
    runner.join();
}

TEST(PeerSessionInterface, CoPingUsesPeerSessionInterface)
{
    asio::io_context io;

    auto peer = std::make_shared<FakePeerSession>(
        io, "fake-ping:1", true, 1200, 1100, 1300);
    peer->set_ping_handler([](Callback<PingResult> callback) {
        PingResult result;
        result.seq = 77;
        result.sent_at = std::chrono::steady_clock::now();
        result.received_at = result.sent_at + std::chrono::milliseconds(4);
        callback(Error::Success, result);
    });

    std::promise<PingResult> promise;
    auto future = promise.get_future();
    asio::co_spawn(
        io,
        [peer, &promise]() -> asio::awaitable<void> {
            try
            {
                promise.set_value(co_await co_ping(peer));
            }
            catch (...)
            {
                promise.set_exception(std::current_exception());
            }
        },
        asio::detached);

    std::thread runner([&]() { io.run(); });
    auto const result = wait_for_future(future, std::chrono::seconds(2));

    EXPECT_EQ(result.seq, 77u);
    EXPECT_EQ(result.rtt(), std::chrono::milliseconds(4));

    io.stop();
    runner.join();
}

TEST(PeerSessionInterface, CoPingRepostsCompletionToBirthExecutor)
{
    asio::io_context io;
    auto consumer = asio::make_strand(io);
    auto peer = std::make_shared<FakePeerSession>(
        io, "fake-cross-strand-ping:1", true, 1200, 1100, 1300);

    std::atomic<bool> callback_on_peer_strand{false};
    std::atomic<bool> resumed_on_consumer_strand{false};
    std::weak_ptr<FakePeerSession> weak_peer = peer;
    peer->set_ping_handler([weak_peer, &callback_on_peer_strand](
                               Callback<PingResult> callback) {
        if (auto locked = weak_peer.lock())
            callback_on_peer_strand = locked->strand().running_in_this_thread();
        PingResult result;
        result.seq = 91;
        callback(Error::Success, result);
    });

    std::promise<PingResult> promise;
    auto future = promise.get_future();
    asio::co_spawn(
        consumer,
        [peer, &consumer, &resumed_on_consumer_strand, &promise]()
            -> asio::awaitable<void> {
            try
            {
                auto result = co_await co_ping(peer);
                resumed_on_consumer_strand = consumer.running_in_this_thread();
                promise.set_value(std::move(result));
            }
            catch (...)
            {
                promise.set_exception(std::current_exception());
            }
        },
        asio::detached);

    std::barrier start{3};
    std::thread first([&io, &start]() {
        start.arrive_and_wait();
        io.run();
    });
    std::thread second([&io, &start]() {
        start.arrive_and_wait();
        io.run();
    });
    start.arrive_and_wait();
    auto result = wait_for_future(future, std::chrono::seconds(2));
    first.join();
    second.join();

    EXPECT_EQ(result.seq, 91u);
    EXPECT_TRUE(callback_on_peer_strand.load());
    EXPECT_TRUE(resumed_on_consumer_strand.load());
}

TEST(PeerSessionInterface, NodeCacheWalkToCompletesAgainstScriptedFake)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .max_entries = 1024,
            .fetch_timeout = std::chrono::milliseconds(120),
            .max_walk_peer_retries = 2,
            .fetch_stale_multiplier = 1,
        });

    auto const root_hash = hash_with_byte(0x11);
    auto const target_key = hash_with_byte(0x22);
    auto const ledger_hash = hash_with_byte(0x33);
    auto const wire = make_leaf_wire(target_key);

    auto peer = std::make_shared<FakePeerSession>(
        io, "fake-nodecache:1", true, 1000, 900, 1100);
    peer->set_send_get_nodes_handler(
        [cache, root_hash, wire](
            Hash256 const&,
            int,
            std::vector<SHAMapNodeID> const&,
            std::function<void(boost::system::error_code)> on_error) mutable {
            cache->insert_and_notify(root_hash, wire);
            if (on_error)
                on_error({});
        });

    std::promise<WalkResult> promise;
    auto future = promise.get_future();
    asio::co_spawn(
        io,
        [cache, root_hash, target_key, ledger_hash, peer, &promise]()
            -> asio::awaitable<void> {
            try
            {
                promise.set_value(co_await cache->walk_to(
                    root_hash,
                    target_key,
                    ledger_hash,
                    1000,
                    1,
                    nullptr,
                    peer));
            }
            catch (...)
            {
                promise.set_exception(std::current_exception());
            }
        },
        asio::detached);

    std::thread runner([&]() { io.run(); });
    auto result = wait_for_future(future, std::chrono::seconds(3));

    EXPECT_TRUE(result.found);
    ASSERT_EQ(result.path.size(), 1u);
    ASSERT_EQ(result.leaf_data.size() + 1u, wire.size());
    EXPECT_TRUE(std::equal(
        result.leaf_data.begin(), result.leaf_data.end(), wire.begin()));

    io.stop();
    runner.join();
}

TEST(PeerSessionInterface, NodeCacheWalkToFailsOverAcrossFakePeers)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .max_entries = 1024,
            .fetch_timeout = std::chrono::milliseconds(60),
            .max_walk_peer_retries = 2,
            .fetch_stale_multiplier = 1,
        });

    auto const root_hash = hash_with_byte(0x44);
    auto const target_key = hash_with_byte(0x55);
    auto const ledger_hash = hash_with_byte(0x66);
    auto const wire = make_leaf_wire(target_key);

    auto first = std::make_shared<FakePeerSession>(
        io, "a-fake-timeout:1", true, 2000, 1900, 2100);
    auto second = std::make_shared<FakePeerSession>(
        io, "z-fake-success:1", true, 2001, 1900, 2100);

    first->set_send_get_nodes_handler(
        [](Hash256 const&,
           int,
           std::vector<SHAMapNodeID> const&,
           std::function<void(boost::system::error_code)>) {
            // Deliberately no response: forces timeout and peer failover.
        });
    second->set_send_get_nodes_handler(
        [cache, root_hash, wire](
            Hash256 const&,
            int,
            std::vector<SHAMapNodeID> const&,
            std::function<void(boost::system::error_code)> on_error) mutable {
            cache->insert_and_notify(root_hash, wire);
            if (on_error)
                on_error({});
        });

    PeerSetTestAccess::insert_connected_peer(io, peers, first);
    PeerSetTestAccess::insert_connected_peer(io, peers, second);

    std::promise<WalkResult> promise;
    auto future = promise.get_future();
    asio::co_spawn(
        io,
        [cache, peers, root_hash, target_key, ledger_hash, first, &promise]()
            -> asio::awaitable<void> {
            try
            {
                promise.set_value(co_await cache->walk_to(
                    root_hash, target_key, ledger_hash, 2000, 1, peers, first));
            }
            catch (...)
            {
                promise.set_exception(std::current_exception());
            }
        },
        asio::detached);

    std::thread runner([&]() { io.run(); });
    auto result = wait_for_future(future, std::chrono::seconds(5));

    EXPECT_TRUE(result.found);
    ASSERT_EQ(result.path.size(), 1u);

    io.stop();
    runner.join();
}

TEST(PeerSessionInterface, NodeCacheWalkFailureExcludesEveryAttemptedPeer)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .max_entries = 1024,
            .fetch_timeout = std::chrono::milliseconds(30),
            .max_walk_peer_retries = 2,
            .fetch_stale_multiplier = 1,
            .peer_refresh_timeout_secs = 0,
        });

    auto const root_hash = hash_with_byte(0x77);
    auto const target_key = hash_with_byte(0x88);
    auto const ledger_hash = hash_with_byte(0x99);
    constexpr uint32_t ledger_seq = 3000;

    auto first = std::make_shared<FakePeerSession>(
        io, "a-fake-missing:1", true, ledger_seq, 2900, 3100);
    auto second = std::make_shared<FakePeerSession>(
        io, "z-fake-missing:1", true, ledger_seq, 2900, 3100);

    std::atomic<int> first_requests{0};
    std::atomic<int> second_requests{0};
    first->set_send_get_nodes_handler(
        [&first_requests](
            Hash256 const&,
            int,
            std::vector<SHAMapNodeID> const&,
            std::function<void(boost::system::error_code)>) {
            first_requests.fetch_add(1, std::memory_order_relaxed);
        });
    second->set_send_get_nodes_handler(
        [&second_requests](
            Hash256 const&,
            int,
            std::vector<SHAMapNodeID> const&,
            std::function<void(boost::system::error_code)>) {
            second_requests.fetch_add(1, std::memory_order_relaxed);
        });

    PeerSetTestAccess::insert_connected_peer(io, peers, first);
    PeerSetTestAccess::insert_connected_peer(io, peers, second);

    std::promise<std::pair<WalkResult, PeerGenerationPin>> promise;
    auto future = promise.get_future();
    asio::co_spawn(
        io,
        [cache, peers, root_hash, target_key, ledger_hash, first, &promise]()
            -> asio::awaitable<void> {
            try
            {
                auto result = co_await cache->walk_to(
                    root_hash,
                    target_key,
                    ledger_hash,
                    ledger_seq,
                    1,
                    peers,
                    first);
                auto remaining = co_await peers->wait_for_peer(ledger_seq, 0);
                promise.set_value({std::move(result), std::move(remaining)});
            }
            catch (...)
            {
                promise.set_exception(std::current_exception());
            }
        },
        asio::detached);

    std::thread runner([&]() { io.run(); });
    auto [result, remaining] = wait_for_future(future, std::chrono::seconds(3));

    EXPECT_FALSE(result.found);
    EXPECT_FALSE(remaining);
    EXPECT_GE(first_requests.load(std::memory_order_relaxed), 1);
    EXPECT_GE(second_requests.load(std::memory_order_relaxed), 1);

    io.stop();
    runner.join();
}

TEST(PeerSessionInterface, NodeCacheWalkReachesUntriedFanoutPeer)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .max_entries = 1024,
            .fetch_timeout = std::chrono::milliseconds(30),
            .max_walk_peer_retries = 3,
            .fetch_stale_multiplier = 1,
        });

    auto const root_hash = hash_with_byte(0xA1);
    auto const target_key = hash_with_byte(0xA2);
    auto const ledger_hash = hash_with_byte(0xA3);
    auto const wire = make_leaf_wire(target_key);
    constexpr uint32_t ledger_seq = 4000;

    std::array<std::shared_ptr<FakePeerSession>, 4> fake_peers;
    std::array<std::atomic<int>, 4> requests{};
    for (std::size_t i = 0; i < fake_peers.size(); ++i)
    {
        fake_peers[i] = std::make_shared<FakePeerSession>(
            io,
            std::string(1, static_cast<char>('a' + i)) + "-fake:1",
            true,
            ledger_seq,
            ledger_seq - static_cast<uint32_t>(400 - i * 100),
            ledger_seq + static_cast<uint32_t>(400 - i * 100));
        fake_peers[i]->set_send_get_nodes_handler(
            [&requests, i](
                Hash256 const&,
                int,
                std::vector<SHAMapNodeID> const&,
                std::function<void(boost::system::error_code)>) {
                requests[i].fetch_add(1, std::memory_order_relaxed);
            });
        PeerSetTestAccess::insert_connected_peer(io, peers, fake_peers[i]);
    }

    fake_peers.back()->set_send_get_nodes_handler(
        [cache, root_hash, wire, &requests](
            Hash256 const&,
            int,
            std::vector<SHAMapNodeID> const&,
            std::function<void(boost::system::error_code)> on_error) mutable {
            requests.back().fetch_add(1, std::memory_order_relaxed);
            cache->insert_and_notify(root_hash, wire);
            if (on_error)
                on_error({});
        });

    std::promise<WalkResult> promise;
    auto future = promise.get_future();
    asio::co_spawn(
        io,
        [cache,
         peers,
         root_hash,
         target_key,
         ledger_hash,
         first = fake_peers.front(),
         &promise]() -> asio::awaitable<void> {
            try
            {
                promise.set_value(co_await cache->walk_to(
                    root_hash,
                    target_key,
                    ledger_hash,
                    ledger_seq,
                    1,
                    peers,
                    first));
            }
            catch (...)
            {
                promise.set_exception(std::current_exception());
            }
        },
        asio::detached);

    std::thread runner([&]() { io.run(); });
    auto result = wait_for_future(future, std::chrono::seconds(3));

    EXPECT_TRUE(result.found);
    for (auto const& request_count : requests)
        EXPECT_GE(request_count.load(std::memory_order_relaxed), 1);

    io.stop();
    runner.join();
}

TEST(PeerSessionInterface, NodeCacheFanoutSurvivesOneWriteFailure)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .max_entries = 1024,
            .fetch_timeout = 100ms,
            .max_walk_peer_retries = 0,
            .fetch_stale_multiplier = 1,
        });

    auto const root_hash = hash_with_byte(0xB1);
    auto const target_key = hash_with_byte(0xB2);
    auto const ledger_hash = hash_with_byte(0xB3);
    auto const wire = make_leaf_wire(target_key);
    constexpr uint32_t ledger_seq = 5000;

    auto failing = std::make_shared<FakePeerSession>(
        io, "a-fake-write-failure:1", true, ledger_seq, 4900, 5100);
    auto responding = std::make_shared<FakePeerSession>(
        io, "z-fake-delayed-response:1", true, ledger_seq, 4900, 5100);

    failing->set_send_get_nodes_handler(
        [&io](
            Hash256 const&,
            int,
            std::vector<SHAMapNodeID> const&,
            std::function<void(boost::system::error_code)> on_error) {
            auto timer = std::make_shared<asio::steady_timer>(io, 5ms);
            timer->async_wait([timer, on_error = std::move(on_error)](
                                  boost::system::error_code ec) mutable {
                if (!ec && on_error)
                    on_error(asio::error::connection_reset);
            });
        });
    responding->set_send_get_nodes_handler(
        [&io, cache, root_hash, wire](
            Hash256 const&,
            int,
            std::vector<SHAMapNodeID> const&,
            std::function<void(boost::system::error_code)>) {
            auto timer = std::make_shared<asio::steady_timer>(io, 20ms);
            timer->async_wait(
                [timer, cache, root_hash, wire](boost::system::error_code ec) {
                    if (!ec)
                        cache->insert_and_notify(root_hash, wire);
                });
        });

    PeerSetTestAccess::insert_connected_peer(io, peers, failing);
    PeerSetTestAccess::insert_connected_peer(io, peers, responding);

    std::promise<WalkResult> promise;
    auto future = promise.get_future();
    asio::co_spawn(
        io,
        [cache, peers, root_hash, target_key, ledger_hash, failing, &promise]()
            -> asio::awaitable<void> {
            try
            {
                promise.set_value(co_await cache->walk_to(
                    root_hash,
                    target_key,
                    ledger_hash,
                    ledger_seq,
                    1,
                    peers,
                    failing));
            }
            catch (...)
            {
                promise.set_exception(std::current_exception());
            }
        },
        asio::detached);

    std::thread runner([&]() { io.run(); });
    auto result = wait_for_future(future, std::chrono::seconds(3));

    EXPECT_TRUE(result.found);

    io.stop();
    runner.join();
}

TEST(
    PeerSessionInterface,
    SharedNodeFetchRetainsGenerationPinAfterInitiatingWaiterCancels)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto work = asio::make_work_guard(io);
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 0,
            .archival_range_threshold = 1000,
        });
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .max_entries = 1024,
            .fetch_timeout = 500ms,
            .max_walk_peer_retries = 0,
            .fetch_stale_multiplier = 2,
        });

    constexpr uint32_t ledger_seq = 5500;
    auto const root_hash = hash_with_byte(0xC1);
    auto const target_key = hash_with_byte(0xC2);
    auto const ledger_hash = hash_with_byte(0xC3);
    auto const wire = make_leaf_wire(target_key);

    auto existing = std::make_shared<FakePeerSession>(
        io, "shared-fetch-peer:1", true, ledger_seq, 5400, 5600);
    auto candidate = std::make_shared<FakePeerSession>(
        io, "replacement-peer:1", true, ledger_seq, 5400, 5600);
    auto cancel = std::make_shared<std::atomic<bool>>(false);
    std::promise<void> request_sent;
    auto request_sent_future = request_sent.get_future();
    std::atomic<bool> request_observed{false};

    existing->set_send_get_nodes_handler(
        [&request_sent, &request_observed](
            Hash256 const&,
            int,
            std::vector<SHAMapNodeID> const&,
            std::function<void(boost::system::error_code)>) {
            if (!request_observed.exchange(true))
                request_sent.set_value();
            // The shared operation remains in flight until the test inserts
            // the response after the initiating waiter has cancelled.
        });

    PeerSetTestAccess::insert_connected_peer(io, peers, existing);
    auto pin =
        PeerSetTestAccess::make_peer_pin(io, peers, existing->endpoint());
    ASSERT_TRUE(pin);

    std::promise<WalkResult> leader_done;
    auto leader_future = leader_done.get_future();
    asio::co_spawn(
        io,
        [cache,
         peers,
         root_hash,
         target_key,
         ledger_hash,
         pin = std::move(pin),
         cancel,
         &leader_done]() mutable -> asio::awaitable<void> {
            try
            {
                leader_done.set_value(co_await cache->walk_to(
                    root_hash,
                    target_key,
                    ledger_hash,
                    ledger_seq,
                    1,
                    peers,
                    std::move(pin),
                    cancel));
            }
            catch (...)
            {
                leader_done.set_exception(std::current_exception());
            }
        },
        asio::detached);

    std::thread runner([&]() { io.run(); });
    ASSERT_EQ(request_sent_future.wait_for(2s), std::future_status::ready);

    // Join through the unmanaged compatibility seam so the shared entry's
    // copy is the only PeerSet pin left after the leader returns.
    std::promise<WalkResult> follower_done;
    auto follower_future = follower_done.get_future();
    asio::co_spawn(
        io,
        [cache,
         peers,
         root_hash,
         target_key,
         ledger_hash,
         existing,
         &follower_done]() -> asio::awaitable<void> {
            try
            {
                follower_done.set_value(co_await cache->walk_to(
                    root_hash,
                    target_key,
                    ledger_hash,
                    ledger_seq,
                    1,
                    peers,
                    existing));
            }
            catch (...)
            {
                follower_done.set_exception(std::current_exception());
            }
        },
        asio::detached);

    std::this_thread::sleep_for(20ms);
    cancel->store(true, std::memory_order_relaxed);
    auto leader_result = wait_for_future(leader_future, 2s);
    EXPECT_FALSE(leader_result.found);

    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 1u);
    EXPECT_FALSE(PeerSetTestAccess::admit_connected_peer(
        io, peers, candidate, ledger_seq, {existing->endpoint()}));

    cache->insert_and_notify(root_hash, wire);
    auto follower_result = wait_for_future(follower_future, 2s);
    EXPECT_TRUE(follower_result.found);

    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 0u);
    EXPECT_TRUE(PeerSetTestAccess::admit_connected_peer(
        io, peers, candidate, ledger_seq, {existing->endpoint()}));

    work.reset();
    io.stop();
    runner.join();
}

TEST(
    PeerSessionInterface,
    NodeLeaderCancellationBeforeSendCannotAbandonJoinedFollower)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto work = asio::make_work_guard(io);
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 0,
            .archival_range_threshold = 1000,
        });
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .max_entries = 1024,
            .fetch_timeout = 500ms,
            .max_walk_peer_retries = 0,
            .fetch_stale_multiplier = 2,
        });

    constexpr uint32_t ledger_seq = 5550;
    auto const root_hash = hash_with_byte(0xC4);
    auto const target_key = hash_with_byte(0xC5);
    auto const ledger_hash = hash_with_byte(0xC6);
    auto const wire = make_leaf_wire(target_key);
    auto existing = std::make_shared<FakePeerSession>(
        io, "pre-send-peer:1", true, ledger_seq, 5450, 5650);
    auto cancel = std::make_shared<std::atomic<bool>>(false);

    std::promise<void> slot_installed;
    auto slot_installed_future = slot_installed.get_future();
    std::promise<void> follower_attached;
    auto follower_attached_future = follower_attached.get_future();
    std::promise<void> resume_send;
    auto resume_send_future = resume_send.get_future().share();
    std::promise<void> request_sent;
    auto request_sent_future = request_sent.get_future();
    std::atomic<bool> slot_reported{false};
    std::atomic<bool> follower_reported{false};
    std::atomic<bool> request_reported{false};

    NodeCacheTestAccess::set_node_round_hooks(
        *cache,
        [&]() {
            if (!slot_reported.exchange(true))
            {
                slot_installed.set_value();
                resume_send_future.wait();
            }
        },
        [&]() {
            if (!follower_reported.exchange(true))
                follower_attached.set_value();
        });

    existing->set_send_get_nodes_handler(
        [&request_sent, &request_reported](
            Hash256 const&,
            int,
            std::vector<SHAMapNodeID> const&,
            std::function<void(boost::system::error_code)>) {
            if (!request_reported.exchange(true))
                request_sent.set_value();
        });

    PeerSetTestAccess::insert_connected_peer(io, peers, existing);
    auto pin =
        PeerSetTestAccess::make_peer_pin(io, peers, existing->endpoint());
    ASSERT_TRUE(pin);

    std::promise<WalkResult> leader_done;
    auto leader_future = leader_done.get_future();
    asio::co_spawn(
        io,
        [cache,
         peers,
         root_hash,
         target_key,
         ledger_hash,
         pin = std::move(pin),
         cancel,
         &leader_done]() mutable -> asio::awaitable<void> {
            try
            {
                leader_done.set_value(co_await cache->walk_to(
                    root_hash,
                    target_key,
                    ledger_hash,
                    ledger_seq,
                    1,
                    peers,
                    std::move(pin),
                    cancel));
            }
            catch (...)
            {
                leader_done.set_exception(std::current_exception());
            }
        },
        asio::detached);

    std::thread first_runner([&]() { io.run(); });
    std::thread second_runner([&]() { io.run(); });
    auto stop = [&]() {
        work.reset();
        io.stop();
        first_runner.join();
        second_runner.join();
    };

    auto const slot_status = slot_installed_future.wait_for(2s);
    EXPECT_EQ(slot_status, std::future_status::ready);
    if (slot_status != std::future_status::ready)
    {
        resume_send.set_value();
        stop();
        return;
    }

    std::promise<WalkResult> follower_done;
    auto follower_future = follower_done.get_future();
    asio::co_spawn(
        io,
        [cache,
         peers,
         root_hash,
         target_key,
         ledger_hash,
         existing,
         &follower_done]() -> asio::awaitable<void> {
            try
            {
                follower_done.set_value(co_await cache->walk_to(
                    root_hash,
                    target_key,
                    ledger_hash,
                    ledger_seq,
                    1,
                    peers,
                    existing));
            }
            catch (...)
            {
                follower_done.set_exception(std::current_exception());
            }
        },
        asio::detached);

    auto const follower_status = follower_attached_future.wait_for(2s);
    EXPECT_EQ(follower_status, std::future_status::ready);
    if (follower_status != std::future_status::ready)
    {
        resume_send.set_value();
        stop();
        return;
    }

    cancel->store(true, std::memory_order_relaxed);
    resume_send.set_value();

    auto const send_status = request_sent_future.wait_for(2s);
    EXPECT_EQ(send_status, std::future_status::ready);
    if (send_status != std::future_status::ready)
    {
        stop();
        return;
    }

    auto const leader_status = leader_future.wait_for(2s);
    EXPECT_EQ(leader_status, std::future_status::ready);
    if (leader_status != std::future_status::ready)
    {
        stop();
        return;
    }
    EXPECT_FALSE(leader_future.get().found);

    cache->insert_and_notify(root_hash, wire);
    auto const joined_status = follower_future.wait_for(2s);
    EXPECT_EQ(joined_status, std::future_status::ready);
    if (joined_status == std::future_status::ready)
        EXPECT_TRUE(follower_future.get().found);

    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 0u);
    stop();
}

TEST(
    PeerSessionInterface,
    FreshlyPublishedStaleReplacementCannotBeReplacedBeforeSend)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto work = asio::make_work_guard(io);
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .max_entries = 1024,
            .fetch_timeout = 500ms,
            .max_walk_peer_retries = 0,
            .fetch_stale_multiplier = 1,
        });

    constexpr uint32_t ledger_seq = 5575;
    auto const root_hash = hash_with_byte(0xC7);
    auto const target_key = hash_with_byte(0xC8);
    auto const ledger_hash = hash_with_byte(0xC9);
    auto const wire = make_leaf_wire(target_key);
    auto peer = std::make_shared<FakePeerSession>(
        io, "stale-publication-peer:1", true, ledger_seq, 5475, 5675);

    NodeCacheTestAccess::seed_stale_round(*cache, root_hash);

    std::promise<void> replacement_published;
    auto replacement_published_future = replacement_published.get_future();
    std::promise<void> follower_attached;
    auto follower_attached_future = follower_attached.get_future();
    std::promise<void> resume_sender;
    auto resume_sender_future = resume_sender.get_future().share();
    std::promise<void> request_sent;
    auto request_sent_future = request_sent.get_future();
    std::atomic<bool> publication_reported{false};
    std::atomic<bool> follower_reported{false};
    std::atomic<bool> request_reported{false};
    std::shared_ptr<SerializedTimerSignal> published_signal;

    NodeCacheTestAccess::set_node_round_hooks(
        *cache,
        [&]() {
            if (!publication_reported.exchange(true))
            {
                published_signal =
                    NodeCacheTestAccess::signal(*cache, root_hash);
                replacement_published.set_value();
                resume_sender_future.wait();
            }
        },
        [&]() {
            if (!follower_reported.exchange(true))
                follower_attached.set_value();
        });

    peer->set_send_get_nodes_handler(
        [&](Hash256 const&,
            int,
            std::vector<SHAMapNodeID> const&,
            std::function<void(boost::system::error_code)>) {
            if (!request_reported.exchange(true))
                request_sent.set_value();
        });

    auto spawn_walk = [&](std::promise<WalkResult>& completed) {
        asio::co_spawn(
            io,
            [cache, root_hash, target_key, ledger_hash, peer, &completed]()
                -> asio::awaitable<void> {
                try
                {
                    completed.set_value(co_await cache->walk_to(
                        root_hash,
                        target_key,
                        ledger_hash,
                        ledger_seq,
                        1,
                        nullptr,
                        peer));
                }
                catch (...)
                {
                    completed.set_exception(std::current_exception());
                }
            },
            asio::detached);
    };

    std::promise<WalkResult> leader_done;
    auto leader_future = leader_done.get_future();
    spawn_walk(leader_done);

    std::thread first_runner([&]() { io.run(); });
    std::thread second_runner([&]() { io.run(); });
    auto stop = [&]() {
        work.reset();
        io.stop();
        first_runner.join();
        second_runner.join();
    };

    auto const published_status = replacement_published_future.wait_for(2s);
    EXPECT_EQ(published_status, std::future_status::ready);
    if (published_status != std::future_status::ready)
    {
        resume_sender.set_value();
        stop();
        return;
    }

    std::promise<WalkResult> follower_done;
    auto follower_future = follower_done.get_future();
    spawn_walk(follower_done);

    auto const follower_status = follower_attached_future.wait_for(2s);
    EXPECT_EQ(follower_status, std::future_status::ready);
    if (follower_status != std::future_status::ready)
    {
        resume_sender.set_value();
        stop();
        return;
    }

    EXPECT_EQ(NodeCacheTestAccess::signal(*cache, root_hash), published_signal);

    resume_sender.set_value();
    auto const send_status = request_sent_future.wait_for(2s);
    EXPECT_EQ(send_status, std::future_status::ready);
    if (send_status != std::future_status::ready)
    {
        stop();
        return;
    }

    cache->insert_and_notify(root_hash, wire);
    auto const leader_status = leader_future.wait_for(2s);
    auto const joined_status = follower_future.wait_for(2s);
    EXPECT_EQ(leader_status, std::future_status::ready);
    EXPECT_EQ(joined_status, std::future_status::ready);
    if (leader_status == std::future_status::ready)
        EXPECT_TRUE(leader_future.get().found);
    if (joined_status == std::future_status::ready)
        EXPECT_TRUE(follower_future.get().found);

    stop();
}

TEST(
    PeerSessionInterface,
    SoleCancelledNodeWaiterStillReleasesPinAtRoundDeadline)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto work = asio::make_work_guard(io);
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 0,
            .archival_range_threshold = 1000,
        });
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .max_entries = 1024,
            .fetch_timeout = 60ms,
            .max_walk_peer_retries = 0,
            .fetch_stale_multiplier = 2,
        });

    constexpr uint32_t ledger_seq = 5600;
    auto const root_hash = hash_with_byte(0xD1);
    auto const target_key = hash_with_byte(0xD2);
    auto const ledger_hash = hash_with_byte(0xD3);
    auto existing = std::make_shared<FakePeerSession>(
        io, "sole-cancel-peer:1", true, ledger_seq, 5500, 5700);
    auto candidate = std::make_shared<FakePeerSession>(
        io, "sole-cancel-replacement:1", true, ledger_seq, 5500, 5700);
    auto cancel = std::make_shared<std::atomic<bool>>(false);
    std::promise<void> request_sent;
    auto request_sent_future = request_sent.get_future();

    existing->set_send_get_nodes_handler(
        [&request_sent](
            Hash256 const&,
            int,
            std::vector<SHAMapNodeID> const&,
            std::function<void(boost::system::error_code)>) {
            request_sent.set_value();
        });
    PeerSetTestAccess::insert_connected_peer(io, peers, existing);
    auto pin =
        PeerSetTestAccess::make_peer_pin(io, peers, existing->endpoint());
    ASSERT_TRUE(pin);

    std::promise<WalkResult> done;
    auto done_future = done.get_future();
    asio::co_spawn(
        io,
        [cache,
         peers,
         root_hash,
         target_key,
         ledger_hash,
         pin = std::move(pin),
         cancel,
         &done]() mutable -> asio::awaitable<void> {
            try
            {
                done.set_value(co_await cache->walk_to(
                    root_hash,
                    target_key,
                    ledger_hash,
                    ledger_seq,
                    1,
                    peers,
                    std::move(pin),
                    cancel));
            }
            catch (...)
            {
                done.set_exception(std::current_exception());
            }
        },
        asio::detached);

    std::thread runner([&]() { io.run(); });
    auto const request_status = request_sent_future.wait_for(2s);
    EXPECT_EQ(request_status, std::future_status::ready);
    if (request_status != std::future_status::ready)
    {
        work.reset();
        io.stop();
        runner.join();
        return;
    }
    cancel->store(true, std::memory_order_relaxed);
    EXPECT_FALSE(wait_for_future(done_future, 2s).found);
    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 1u);

    std::this_thread::sleep_for(100ms);
    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 0u);
    EXPECT_TRUE(PeerSetTestAccess::admit_connected_peer(
        io, peers, candidate, ledger_seq, {existing->endpoint()}));

    work.reset();
    io.stop();
    runner.join();
}

TEST(
    PeerSessionInterface,
    NodeDeadlineServantFailureStillRetiresExactRoundAndPin)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto work = asio::make_work_guard(io);
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 0,
            .archival_range_threshold = 1000,
        });
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .max_entries = 1024,
            .fetch_timeout = 1h,
            .max_walk_peer_retries = 0,
            .fetch_stale_multiplier = 2,
        });

    constexpr uint32_t ledger_seq = 5625;
    auto const root_hash = hash_with_byte(0xE1);
    auto const target_key = hash_with_byte(0xE2);
    auto const ledger_hash = hash_with_byte(0xE3);
    auto existing = std::make_shared<FakePeerSession>(
        io, "deadline-fault-peer:1", true, ledger_seq, 5500, 5700);
    auto cancel = std::make_shared<std::atomic<bool>>(false);

    std::promise<void> request_sent;
    auto request_sent_future = request_sent.get_future().share();
    std::promise<void> allow_servant_failure;
    auto allow_servant_failure_future =
        allow_servant_failure.get_future().share();
    std::promise<void> retired;
    auto retired_future = retired.get_future();
    std::atomic<bool> request_reported{false};
    std::atomic<bool> retirement_reported{false};

    NodeCacheTestAccess::set_node_deadline_hooks(
        *cache,
        [request_sent_future, allow_servant_failure_future]() {
            request_sent_future.wait();
            allow_servant_failure_future.wait();
            throw std::runtime_error("forced deadline servant failure");
        },
        {},
        {},
        [&]() {
            if (!retirement_reported.exchange(true))
                retired.set_value();
        });

    existing->set_send_get_nodes_handler(
        [&](Hash256 const&,
            int,
            std::vector<SHAMapNodeID> const&,
            std::function<void(boost::system::error_code)>) {
            cancel->store(true, std::memory_order_relaxed);
            if (!request_reported.exchange(true))
                request_sent.set_value();
        });
    PeerSetTestAccess::insert_connected_peer(io, peers, existing);
    auto pin =
        PeerSetTestAccess::make_peer_pin(io, peers, existing->endpoint());
    ASSERT_TRUE(pin);

    std::promise<WalkResult> done;
    auto done_future = done.get_future();
    asio::co_spawn(
        io,
        [cache,
         peers,
         root_hash,
         target_key,
         ledger_hash,
         pin = std::move(pin),
         cancel,
         &done]() mutable -> asio::awaitable<void> {
            try
            {
                done.set_value(co_await cache->walk_to(
                    root_hash,
                    target_key,
                    ledger_hash,
                    ledger_seq,
                    1,
                    peers,
                    std::move(pin),
                    cancel));
            }
            catch (...)
            {
                done.set_exception(std::current_exception());
            }
        },
        asio::detached);

    std::thread first_runner([&]() { io.run(); });
    std::thread second_runner([&]() { io.run(); });
    auto stop = [&]() {
        work.reset();
        io.stop();
        first_runner.join();
        second_runner.join();
    };

    auto const request_status = request_sent_future.wait_for(2s);
    EXPECT_EQ(request_status, std::future_status::ready);
    if (request_status != std::future_status::ready)
    {
        allow_servant_failure.set_value();
        stop();
        return;
    }

    auto const done_status = done_future.wait_for(2s);
    EXPECT_EQ(done_status, std::future_status::ready);
    if (done_status != std::future_status::ready)
    {
        allow_servant_failure.set_value();
        stop();
        return;
    }
    EXPECT_FALSE(done_future.get().found);
    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 1u);

    allow_servant_failure.set_value();
    auto const retired_status = retired_future.wait_for(2s);
    EXPECT_EQ(retired_status, std::future_status::ready);
    if (retired_status != std::future_status::ready)
    {
        stop();
        return;
    }
    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 0u);

    stop();
}

TEST(
    PeerSessionInterface,
    TimedOutManagedWalkReleasesFullPoolGenerationBeforeReplacementWait)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto work = asio::make_work_guard(io);
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 0,
            .archival_range_threshold = 1000,
        });
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .max_entries = 1024,
            .fetch_timeout = 40ms,
            .max_walk_peer_retries = 1,
            .fetch_stale_multiplier = 2,
            .peer_refresh_timeout_secs = 1,
        });

    constexpr uint32_t ledger_seq = 5650;
    auto const root_hash = hash_with_byte(0xD4);
    auto const target_key = hash_with_byte(0xD5);
    auto const ledger_hash = hash_with_byte(0xD6);
    auto const wire = make_leaf_wire(target_key);
    auto existing = std::make_shared<FakePeerSession>(
        io, "full-pool-old:1", true, ledger_seq, 5500, 5700);
    auto replacement = std::make_shared<FakePeerSession>(
        io, "full-pool-new:1", true, ledger_seq, 5500, 5700);

    existing->set_send_get_nodes_handler(
        [](Hash256 const&,
           int,
           std::vector<SHAMapNodeID> const&,
           std::function<void(boost::system::error_code)>) {
            // Missing history: leave the exact round unanswered.
        });
    replacement->set_send_get_nodes_handler(
        [cache, root_hash, wire](
            Hash256 const&,
            int,
            std::vector<SHAMapNodeID> const&,
            std::function<void(boost::system::error_code)>) {
            cache->insert_and_notify(root_hash, wire);
        });

    PeerSetTestAccess::insert_connected_peer(io, peers, existing);
    auto pin =
        PeerSetTestAccess::make_peer_pin(io, peers, existing->endpoint());
    ASSERT_TRUE(pin);

    std::promise<WalkResult> done;
    auto done_future = done.get_future();
    asio::co_spawn(
        io,
        [cache,
         peers,
         root_hash,
         target_key,
         ledger_hash,
         pin = std::move(pin),
         &done]() mutable -> asio::awaitable<void> {
            try
            {
                done.set_value(co_await cache->walk_to(
                    root_hash,
                    target_key,
                    ledger_hash,
                    ledger_seq,
                    1,
                    peers,
                    std::move(pin)));
            }
            catch (...)
            {
                done.set_exception(std::current_exception());
            }
        },
        asio::detached);

    std::thread runner([&]() { io.run(); });
    std::this_thread::sleep_for(120ms);

    ASSERT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 0u);
    ASSERT_TRUE(PeerSetTestAccess::admit_connected_peer(
        io, peers, replacement, ledger_seq, {existing->endpoint()}));

    auto result = wait_for_future(done_future, 2s);
    EXPECT_TRUE(result.found);
    EXPECT_FALSE(
        PeerSetTestAccess::has_connection(io, peers, existing->endpoint()));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, replacement->endpoint()));
    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 0u);

    work.reset();
    io.stop();
    runner.join();
}

TEST(PeerSessionInterface, ExpiredOldNodeWaitCannotClearReplacementRound)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto work = asio::make_work_guard(io);
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .max_entries = 1024,
            .fetch_timeout = 300ms,
            .max_walk_peer_retries = 0,
            .fetch_stale_multiplier = 1,
        });

    constexpr uint32_t ledger_seq = 5700;
    auto const root_hash = hash_with_byte(0xE1);
    auto const target_key = hash_with_byte(0xE2);
    auto const ledger_hash = hash_with_byte(0xE3);
    auto const wire = make_leaf_wire(target_key);
    auto first = std::make_shared<FakePeerSession>(
        io, "old-round-peer:1", true, ledger_seq, 5600, 5800);
    auto second = std::make_shared<FakePeerSession>(
        io, "replacement-round-peer:1", true, ledger_seq, 5600, 5800);
    std::promise<void> first_sent;
    std::promise<void> second_sent;
    auto first_sent_future = first_sent.get_future();
    auto second_sent_future = second_sent.get_future();
    first->set_send_get_nodes_handler(
        [&first_sent](
            Hash256 const&,
            int,
            std::vector<SHAMapNodeID> const&,
            std::function<void(boost::system::error_code)>) {
            first_sent.set_value();
        });
    second->set_send_get_nodes_handler(
        [&second_sent](
            Hash256 const&,
            int,
            std::vector<SHAMapNodeID> const&,
            std::function<void(boost::system::error_code)>) {
            second_sent.set_value();
        });
    PeerSetTestAccess::insert_connected_peer(io, peers, first);
    PeerSetTestAccess::insert_connected_peer(io, peers, second);

    auto first_pin =
        PeerSetTestAccess::make_peer_pin(io, peers, first->endpoint());
    auto second_pin =
        PeerSetTestAccess::make_peer_pin(io, peers, second->endpoint());
    ASSERT_TRUE(first_pin);
    ASSERT_TRUE(second_pin);

    auto spawn_walk = [&](PeerGenerationPin pin,
                          std::promise<WalkResult>& result) {
        asio::co_spawn(
            io,
            [cache,
             root_hash,
             target_key,
             ledger_hash,
             pin = std::move(pin),
             &result]() mutable -> asio::awaitable<void> {
                try
                {
                    result.set_value(co_await cache->walk_to(
                        root_hash,
                        target_key,
                        ledger_hash,
                        ledger_seq,
                        1,
                        nullptr,
                        std::move(pin)));
                }
                catch (...)
                {
                    result.set_exception(std::current_exception());
                }
            },
            asio::detached);
    };

    std::promise<WalkResult> first_done;
    std::promise<WalkResult> second_done;
    auto first_done_future = first_done.get_future();
    auto second_done_future = second_done.get_future();
    spawn_walk(std::move(first_pin), first_done);

    std::thread runner([&]() { io.run(); });
    ASSERT_EQ(first_sent_future.wait_for(2s), std::future_status::ready);
    auto old_signal = NodeCacheTestAccess::signal(*cache, root_hash);
    ASSERT_TRUE(old_signal);
    NodeCacheTestAccess::make_round_stale(*cache, root_hash);

    spawn_walk(std::move(second_pin), second_done);
    ASSERT_EQ(second_sent_future.wait_for(2s), std::future_status::ready);
    auto replacement_signal = NodeCacheTestAccess::signal(*cache, root_hash);
    ASSERT_TRUE(replacement_signal);
    ASSERT_NE(old_signal, replacement_signal);

    old_signal->expires_at(std::chrono::steady_clock::now());
    EXPECT_FALSE(wait_for_future(first_done_future, 2s).found);
    EXPECT_EQ(
        NodeCacheTestAccess::signal(*cache, root_hash), replacement_signal);

    cache->insert_and_notify(root_hash, wire);
    EXPECT_TRUE(wait_for_future(second_done_future, 2s).found);

    work.reset();
    io.stop();
    runner.join();
}

TEST(PeerSessionInterface, LateHeaderFailureCannotCancelNewRound)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .max_entries = 1024,
            .fetch_timeout = 10ms,
            .header_timeout_floor = 50ms,
        });
    auto work = asio::make_work_guard(io);
    constexpr uint32_t ledger_seq = 6000;

    auto old_round = std::make_shared<FakePeerSession>(
        io, "old-header-round:1", true, ledger_seq, 5900, 6100);
    auto new_round = std::make_shared<FakePeerSession>(
        io, "new-header-round:1", false, ledger_seq, 5900, 6100);

    std::mutex callback_mutex;
    Callback<LedgerHeaderResult> old_callback;
    Callback<LedgerHeaderResult> new_callback;
    std::promise<void> old_sent;
    auto old_sent_future = old_sent.get_future();
    std::promise<void> new_sent;
    auto new_sent_future = new_sent.get_future();
    old_round->set_get_ledger_header_handler(
        [&](uint32_t, Callback<LedgerHeaderResult> callback) {
            std::lock_guard lock(callback_mutex);
            old_callback = std::move(callback);
            old_sent.set_value();
        });
    new_round->set_get_ledger_header_handler(
        [&](uint32_t, Callback<LedgerHeaderResult> callback) {
            std::lock_guard lock(callback_mutex);
            new_callback = std::move(callback);
            new_sent.set_value();
        });

    PeerSetTestAccess::insert_connected_peer(io, peers, old_round);
    PeerSetTestAccess::insert_connected_peer(io, peers, new_round);
    std::promise<bool> owner_promise;
    std::promise<bool> joiner_promise;
    auto owner_future = owner_promise.get_future();
    auto joiner_future = joiner_promise.get_future();
    asio::co_spawn(
        io,
        [cache, peers, old_round, &owner_promise]() -> asio::awaitable<void> {
            try
            {
                (void)co_await cache->get_header(6000, peers, old_round);
                owner_promise.set_value(true);
            }
            catch (...)
            {
                owner_promise.set_exception(std::current_exception());
            }
        },
        asio::detached);
    asio::co_spawn(
        io,
        [cache, peers, &joiner_promise]() -> asio::awaitable<void> {
            try
            {
                (void)co_await cache->get_header(6000, peers, nullptr);
                joiner_promise.set_value(true);
            }
            catch (...)
            {
                joiner_promise.set_exception(std::current_exception());
            }
        },
        asio::detached);

    std::array<std::thread, 4> runners;
    for (auto& runner : runners)
        runner = std::thread([&]() { io.run(); });

    ASSERT_EQ(old_sent_future.wait_for(2s), std::future_status::ready);
    new_round->set_ready(true);
    ASSERT_EQ(new_sent_future.wait_for(2s), std::future_status::ready);

    Callback<LedgerHeaderResult> stale;
    {
        std::lock_guard lock(callback_mutex);
        stale = std::move(old_callback);
    }
    ASSERT_TRUE(stale);
    stale(Error::Timeout, LedgerHeaderResult{});

    std::promise<void> stale_processed;
    auto stale_processed_future = stale_processed.get_future();
    asio::co_spawn(
        io,
        [cache, &stale_processed]() -> asio::awaitable<void> {
            co_await NodeCacheTestAccess::header_barrier(cache);
            stale_processed.set_value();
        },
        asio::detached);
    ASSERT_EQ(stale_processed_future.wait_for(2s), std::future_status::ready);
    EXPECT_EQ(owner_future.wait_for(0ms), std::future_status::timeout);
    EXPECT_EQ(joiner_future.wait_for(0ms), std::future_status::timeout);
    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 1u);

    Callback<LedgerHeaderResult> current;
    {
        std::lock_guard lock(callback_mutex);
        current = std::move(new_callback);
    }
    ASSERT_TRUE(current);
    current(Error::Success, LedgerHeaderResult{});

    EXPECT_TRUE(wait_for_future(owner_future, std::chrono::seconds(2)));
    EXPECT_TRUE(wait_for_future(joiner_future, std::chrono::seconds(2)));
    work.reset();
    io.stop();
    for (auto& runner : runners)
        runner.join();

    io.restart();
    EXPECT_EQ(
        PeerSetTestAccess::ledger_failure_count(
            io, peers, old_round->endpoint()),
        1u);
    EXPECT_EQ(
        PeerSetTestAccess::ledger_failure_count(
            io, peers, new_round->endpoint()),
        0u);
}

TEST(PeerSessionInterface, FastHeaderFailureIsLatchedBeforeOwnerWaits)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .max_entries = 1024,
            .fetch_timeout = 10ms,
            .header_timeout_floor = 500ms,
        });
    auto primary = std::make_shared<FakePeerSession>(
        io, "fast-header-failure:1", true, 6000, 5900, 6100);
    primary->set_invoke_header_handler_inline(true);
    primary->set_get_ledger_header_handler(
        [](uint32_t, Callback<LedgerHeaderResult> callback) {
            callback(Error::Timeout, LedgerHeaderResult{});
        });

    bool timed_out = false;
    auto const started = std::chrono::steady_clock::now();
    asio::co_spawn(
        io,
        [&, cache, peers, primary]() -> asio::awaitable<void> {
            try
            {
                (void)co_await cache->get_header(6000, peers, primary);
            }
            catch (PeerClientException const&)
            {
                timed_out = true;
            }
        },
        asio::detached);

    io.run();

    EXPECT_TRUE(timed_out);
    EXPECT_LT(
        std::chrono::steady_clock::now() - started,
        std::chrono::milliseconds(250));
}

TEST(PeerSessionInterface, HeaderLeaderCancellationDoesNotAbandonFollower)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .max_entries = 1024,
            .fetch_timeout = 10ms,
            .header_timeout_floor = 500ms,
        });
    auto primary = std::make_shared<FakePeerSession>(
        io, "cancelled-header-leader:1", true, 6000, 5900, 6100);
    bool response_delivered = false;
    primary->set_get_ledger_header_handler(
        [&io, &response_delivered](
            uint32_t, Callback<LedgerHeaderResult> callback) {
            auto timer = std::make_shared<asio::steady_timer>(io, 100ms);
            timer->async_wait(
                [timer, &response_delivered, callback = std::move(callback)](
                    boost::system::error_code ec) mutable {
                    if (!ec)
                    {
                        response_delivered = true;
                        callback(Error::Success, LedgerHeaderResult{});
                    }
                });
        });
    auto follower_peer = std::make_shared<FakePeerSession>(
        io, "unused-header-joiner:1", true, 6000, 5900, 6100);
    PeerSetTestAccess::insert_connected_peer(io, peers, follower_peer);
    auto follower_pin =
        PeerSetTestAccess::make_peer_pin(io, peers, follower_peer->endpoint());
    ASSERT_TRUE(follower_pin);

    bool leader_done = false;
    bool cancel_branch_won = false;
    bool leader_cancelled = false;
    bool leader_finished_before_response = false;
    bool follower_done = false;
    bool follower_succeeded = false;

    asio::co_spawn(
        io,
        [&, cache, primary]() -> asio::awaitable<void> {
            auto ex = co_await asio::this_coro::executor;
            auto result =
                co_await asio::experimental::make_parallel_group(
                    asio::co_spawn(
                        ex,
                        cache->get_header(6000, nullptr, primary),
                        asio::deferred),
                    asio::co_spawn(
                        ex,
                        []() -> asio::awaitable<void> {
                            auto ex = co_await asio::this_coro::executor;
                            asio::steady_timer timer(ex, 30ms);
                            boost::system::error_code ec;
                            co_await timer.async_wait(
                                asio::redirect_error(asio::use_awaitable, ec));
                        },
                        asio::deferred))
                    .async_wait(
                        asio::experimental::wait_for_one(),
                        asio::use_awaitable);
            cancel_branch_won = (std::get<0>(result)[0] == 1);
            if (auto const& error = std::get<1>(result))
            {
                try
                {
                    std::rethrow_exception(error);
                }
                catch (boost::system::system_error const& e)
                {
                    leader_cancelled =
                        e.code() == asio::error::operation_aborted;
                }
                catch (...)
                {
                }
            }
            leader_finished_before_response = !response_delivered;
            leader_done = true;
        },
        asio::detached);

    // Drive the leader until its request and shared operation are both
    // suspended, then attach the follower before the cancellation timer fires.
    io.poll();
    io.restart();

    asio::co_spawn(
        io,
        [&, cache, peers, pin = std::move(follower_pin)]() mutable
        -> asio::awaitable<void> {
            try
            {
                (void)co_await cache->get_header(6000, peers, std::move(pin));
                follower_succeeded = true;
            }
            catch (...)
            {
            }
            follower_done = true;
        },
        asio::detached);

    io.poll();
    io.restart();
    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 0u);

    io.run();

    EXPECT_TRUE(leader_done);
    EXPECT_TRUE(cancel_branch_won);
    EXPECT_TRUE(leader_cancelled);
    EXPECT_TRUE(leader_finished_before_response);
    EXPECT_TRUE(follower_done);
    EXPECT_TRUE(follower_succeeded);
}

TEST(PeerSessionInterface, ManagedHeaderSuccessReleasesGenerationPin)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .max_entries = 1024,
            .fetch_timeout = 10ms,
            .header_timeout_floor = 500ms,
        });
    auto primary = std::make_shared<FakePeerSession>(
        io, "managed-header-success:1", true, 6000, 5900, 6100);
    primary->set_get_ledger_header_handler(
        [&io](uint32_t, Callback<LedgerHeaderResult> callback) {
            auto timer = std::make_shared<asio::steady_timer>(io, 10ms);
            timer->async_wait([timer, callback = std::move(callback)](
                                  boost::system::error_code ec) mutable {
                if (!ec)
                    callback(Error::Success, LedgerHeaderResult{});
            });
        });

    PeerSetTestAccess::insert_connected_peer(io, peers, primary);
    auto pin = PeerSetTestAccess::make_peer_pin(io, peers, primary->endpoint());
    ASSERT_TRUE(pin);
    ASSERT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 1u);

    bool completed = false;
    asio::co_spawn(
        io,
        [&, cache, peers, pin = std::move(pin)]() mutable
        -> asio::awaitable<void> {
            (void)co_await cache->get_header(6000, peers, std::move(pin));
            completed = true;
        },
        asio::detached);

    io.run();

    EXPECT_TRUE(completed);
    io.restart();
    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 0u);
}

TEST(PeerSessionInterface, ManagedHeaderPinRefusesForeignSelectionPeerSet)
{
    asio::io_context io;
    auto owner = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto foreign = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto cache = NodeCache::create(io, {});
    auto primary = std::make_shared<FakePeerSession>(
        io, "foreign-pin-owner:1", true, 6000, 5900, 6100);
    std::atomic<unsigned> sends = 0;
    primary->set_get_ledger_header_handler(
        [&sends](uint32_t, Callback<LedgerHeaderResult>) {
            sends.fetch_add(1, std::memory_order_relaxed);
        });

    PeerSetTestAccess::insert_connected_peer(io, owner, primary);
    auto pin = PeerSetTestAccess::make_peer_pin(io, owner, primary->endpoint());
    ASSERT_TRUE(pin);

    std::optional<Error> observed;
    asio::co_spawn(
        io,
        [cache, foreign, pin = std::move(pin), &observed]() mutable
        -> asio::awaitable<void> {
            try
            {
                (void)co_await cache->get_header(6000, foreign, std::move(pin));
            }
            catch (PeerClientException const& e)
            {
                observed = e.error;
            }
        },
        asio::detached);
    io.run();

    ASSERT_TRUE(observed);
    EXPECT_EQ(*observed, Error::BadRequest);
    EXPECT_EQ(sends.load(std::memory_order_relaxed), 0u);
    io.restart();
    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(io, owner), 0u);
    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(io, foreign), 0u);
}

TEST(PeerSessionInterface, ClearReleasesManagedHeaderGenerationPin)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto work = asio::make_work_guard(io);
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .max_entries = 1024,
            .fetch_timeout = 10ms,
            .header_timeout_floor = 5s,
        });
    auto primary = std::make_shared<FakePeerSession>(
        io, "managed-header-clear:1", true, 6000, 5900, 6100);
    std::promise<void> request_started;
    auto request_started_future = request_started.get_future();
    primary->set_get_ledger_header_handler(
        [&request_started](uint32_t, Callback<LedgerHeaderResult> callback) {
            (void)callback;
            request_started.set_value();
        });

    PeerSetTestAccess::insert_connected_peer(io, peers, primary);
    auto pin = PeerSetTestAccess::make_peer_pin(io, peers, primary->endpoint());
    ASSERT_TRUE(pin);

    std::promise<Error> done;
    auto done_future = done.get_future();
    asio::co_spawn(
        io,
        [cache, peers, pin = std::move(pin), &done]() mutable
        -> asio::awaitable<void> {
            try
            {
                (void)co_await cache->get_header(6000, peers, std::move(pin));
                done.set_value(Error::Success);
            }
            catch (PeerClientException const& e)
            {
                done.set_value(e.error);
            }
        },
        asio::detached);

    std::thread runner([&]() { io.run(); });
    ASSERT_EQ(request_started_future.wait_for(2s), std::future_status::ready);
    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 1u);

    std::promise<bool> clear_done;
    auto clear_done_future = clear_done.get_future();
    asio::co_spawn(
        io,
        [cache, &clear_done]() -> asio::awaitable<void> {
            try
            {
                co_await cache->co_clear_headers();
                clear_done.set_value(true);
            }
            catch (...)
            {
                clear_done.set_value(false);
            }
        },
        asio::detached);

    EXPECT_TRUE(wait_for_future(clear_done_future, 2s));
    EXPECT_EQ(wait_for_future(done_future, 2s), Error::Cancelled);
    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 0u);

    work.reset();
    io.stop();
    runner.join();
}

TEST(PeerSessionInterface, CancelledClearWaiterCannotAbandonSharedDrain)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto work = asio::make_work_guard(io);
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .max_entries = 1024,
            .fetch_timeout = 10ms,
            .header_timeout_floor = 5s,
        });
    auto primary = std::make_shared<FakePeerSession>(
        io, "cancelled-clear-waiter:1", true, 6000, 5900, 6100);
    std::promise<void> request_started;
    auto request_started_future = request_started.get_future();
    primary->set_get_ledger_header_handler(
        [&request_started](uint32_t, Callback<LedgerHeaderResult> callback) {
            (void)callback;
            request_started.set_value();
        });

    PeerSetTestAccess::insert_connected_peer(io, peers, primary);
    auto pin = PeerSetTestAccess::make_peer_pin(io, peers, primary->endpoint());
    ASSERT_TRUE(pin);

    asio::co_spawn(
        io,
        [cache,
         peers,
         pin = std::move(pin)]() mutable -> asio::awaitable<void> {
            try
            {
                (void)co_await cache->get_header(6000, peers, std::move(pin));
            }
            catch (...)
            {
            }
        },
        asio::detached);

    std::thread first_runner([&]() { io.run(); });
    std::thread second_runner([&]() { io.run(); });
    std::thread third_runner([&]() { io.run(); });
    ASSERT_EQ(request_started_future.wait_for(2s), std::future_status::ready);

    std::promise<void> peer_strand_entered;
    auto peer_strand_entered_future = peer_strand_entered.get_future();
    std::promise<void> release_peer_strand;
    auto release_peer_strand_future = release_peer_strand.get_future().share();
    PeerSetTestAccess::block_strand_until(
        peers, peer_strand_entered, release_peer_strand_future);
    ASSERT_EQ(
        peer_strand_entered_future.wait_for(2s), std::future_status::ready);

    std::promise<bool> first_waiter_cancelled;
    auto first_waiter_cancelled_future = first_waiter_cancelled.get_future();
    asio::co_spawn(
        io,
        [cache, &first_waiter_cancelled]() -> asio::awaitable<void> {
            auto ex = co_await asio::this_coro::executor;
            auto result =
                co_await asio::experimental::make_parallel_group(
                    asio::co_spawn(
                        ex, cache->co_clear_headers(), asio::deferred),
                    asio::co_spawn(
                        ex,
                        []() -> asio::awaitable<void> {
                            auto ex = co_await asio::this_coro::executor;
                            asio::steady_timer timer(ex, 30ms);
                            boost::system::error_code ec;
                            co_await timer.async_wait(
                                asio::redirect_error(asio::use_awaitable, ec));
                        },
                        asio::deferred))
                    .async_wait(
                        asio::experimental::wait_for_one(),
                        asio::use_awaitable);
            first_waiter_cancelled.set_value(
                std::get<0>(result)[0] == 1 &&
                static_cast<bool>(std::get<1>(result)));
        },
        asio::detached);

    ASSERT_TRUE(wait_for_future(first_waiter_cancelled_future, 2s));

    std::promise<bool> joined_clear_done;
    auto joined_clear_done_future = joined_clear_done.get_future();
    asio::co_spawn(
        io,
        [cache, &joined_clear_done]() -> asio::awaitable<void> {
            try
            {
                co_await cache->co_clear_headers();
                joined_clear_done.set_value(true);
            }
            catch (...)
            {
                joined_clear_done.set_value(false);
            }
        },
        asio::detached);
    EXPECT_EQ(
        joined_clear_done_future.wait_for(50ms), std::future_status::timeout);

    release_peer_strand.set_value();
    EXPECT_TRUE(wait_for_future(joined_clear_done_future, 2s));
    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 0u);

    work.reset();
    io.stop();
    first_runner.join();
    second_runner.join();
    third_runner.join();
}

TEST(PeerSessionInterface, ClearFencesLateHeaderCallbackFromReplacement)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto work = asio::make_work_guard(io);
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .max_entries = 1024,
            .fetch_timeout = 10ms,
            .header_timeout_floor = 5s,
        });
    auto primary = std::make_shared<FakePeerSession>(
        io, "header-clear-replacement:1", true, 6000, 5900, 6100);

    std::mutex callback_mutex;
    Callback<LedgerHeaderResult> old_callback;
    Callback<LedgerHeaderResult> replacement_callback;
    std::promise<void> old_sent;
    auto old_sent_future = old_sent.get_future();
    std::promise<void> replacement_sent;
    auto replacement_sent_future = replacement_sent.get_future();
    int send_count = 0;
    primary->set_get_ledger_header_handler(
        [&](uint32_t, Callback<LedgerHeaderResult> callback) {
            std::lock_guard lock(callback_mutex);
            if (send_count++ == 0)
            {
                old_callback = std::move(callback);
                old_sent.set_value();
            }
            else
            {
                replacement_callback = std::move(callback);
                replacement_sent.set_value();
            }
        });

    PeerSetTestAccess::insert_connected_peer(io, peers, primary);
    auto first_pin =
        PeerSetTestAccess::make_peer_pin(io, peers, primary->endpoint());
    ASSERT_TRUE(first_pin);

    std::promise<bool> first_done;
    auto first_done_future = first_done.get_future();
    asio::co_spawn(
        io,
        [cache, peers, pin = std::move(first_pin), &first_done]() mutable
        -> asio::awaitable<void> {
            try
            {
                (void)co_await cache->get_header(6000, peers, std::move(pin));
                first_done.set_value(false);
            }
            catch (PeerClientException const&)
            {
                first_done.set_value(true);
            }
        },
        asio::detached);

    std::thread runner([&]() { io.run(); });
    ASSERT_EQ(old_sent_future.wait_for(2s), std::future_status::ready);

    std::promise<bool> clear_done;
    auto clear_done_future = clear_done.get_future();
    asio::co_spawn(
        io,
        [cache, &clear_done]() -> asio::awaitable<void> {
            try
            {
                co_await cache->co_clear_headers();
                clear_done.set_value(true);
            }
            catch (...)
            {
                clear_done.set_value(false);
            }
        },
        asio::detached);
    EXPECT_TRUE(wait_for_future(clear_done_future, 2s));
    EXPECT_TRUE(wait_for_future(first_done_future, 2s));

    std::promise<bool> replacement_done;
    auto replacement_done_future = replacement_done.get_future();
    asio::co_spawn(
        io,
        [cache, peers, &replacement_done]() -> asio::awaitable<void> {
            try
            {
                auto selected =
                    co_await peers->co_select_peers_for(6000, 1, {});
                if (selected.empty())
                {
                    replacement_done.set_value(false);
                    co_return;
                }
                (void)co_await cache->get_header(
                    6000, peers, std::move(selected.front()));
                replacement_done.set_value(true);
            }
            catch (...)
            {
                replacement_done.set_value(false);
            }
        },
        asio::detached);
    ASSERT_EQ(replacement_sent_future.wait_for(2s), std::future_status::ready);

    Callback<LedgerHeaderResult> stale;
    {
        std::lock_guard lock(callback_mutex);
        stale = std::move(old_callback);
    }
    ASSERT_TRUE(stale);
    stale(Error::Success, LedgerHeaderResult{});

    std::promise<void> stale_processed;
    auto stale_processed_future = stale_processed.get_future();
    asio::co_spawn(
        io,
        [cache, &stale_processed]() -> asio::awaitable<void> {
            co_await NodeCacheTestAccess::header_barrier(cache);
            stale_processed.set_value();
        },
        asio::detached);
    ASSERT_EQ(stale_processed_future.wait_for(2s), std::future_status::ready);
    EXPECT_EQ(
        replacement_done_future.wait_for(0ms), std::future_status::timeout);

    Callback<LedgerHeaderResult> current;
    {
        std::lock_guard lock(callback_mutex);
        current = std::move(replacement_callback);
    }
    ASSERT_TRUE(current);
    current(Error::Success, LedgerHeaderResult{});
    EXPECT_TRUE(wait_for_future(replacement_done_future, 2s));
    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 0u);

    work.reset();
    io.stop();
    runner.join();
}

TEST(PeerSessionInterface, HeaderStrandPreservesRequestStatusContext)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .max_entries = 1024,
            .fetch_timeout = 10ms,
            .header_timeout_floor = 100ms,
        });
    auto primary = std::make_shared<FakePeerSession>(
        io, "context-header:1", true, 6000, 5900, 6100);
    primary->set_get_ledger_header_handler(
        [&io](uint32_t, Callback<LedgerHeaderResult> callback) {
            auto timer = std::make_shared<asio::steady_timer>(io, 10ms);
            timer->async_wait([timer, callback = std::move(callback)](
                                  boost::system::error_code ec) mutable {
                if (!ec)
                    callback(Error::Success, LedgerHeaderResult{});
            });
        });

    auto request = std::make_shared<catl::core::RequestContext>(
        "request-id", "transaction", std::chrono::steady_clock::now());
    auto request_executor =
        catl::core::ContextExecutor(asio::make_strand(io), request);
    bool completed = false;
    asio::co_spawn(
        request_executor,
        [&, cache, primary]() -> asio::awaitable<void> {
            (void)co_await cache->get_header(6000, nullptr, primary);
            completed = true;
        },
        asio::detached);

    io.run();

    EXPECT_TRUE(completed);
    auto const events = request->drain();
    EXPECT_TRUE(
        std::any_of(events.begin(), events.end(), [](std::string const& event) {
            return event.find("anchor header") != std::string::npos;
        }));
}

TEST(PeerSessionInterface, ArchivalAdmissionRotatesOnlyArchivalPool)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 1,
            .archival_range_threshold = 100,
        });
    constexpr uint32_t target = 500;

    auto hub = std::make_shared<FakePeerSession>(
        io, "hub-kept:1", true, 1000, 900, 950);
    auto failed_archival = std::make_shared<FakePeerSession>(
        io, "archive-failed:1", true, 1000, 400, 900);
    auto replacement = std::make_shared<FakePeerSession>(
        io, "archive-replacement:1", true, 1000, 300, 1000);
    auto extra = std::make_shared<FakePeerSession>(
        io, "archive-extra:1", true, 1000, 200, 1100);

    PeerSetTestAccess::insert_connected_peer(io, peers, hub);
    PeerSetTestAccess::insert_connected_peer(io, peers, failed_archival);
    PeerSetTestAccess::note_ledger_failure(
        io, peers, failed_archival->endpoint(), target);
    PeerSetTestAccess::acquire_wanted_ledger(io, peers, target);

    EXPECT_TRUE(PeerSetTestAccess::admit_connected_peer(
        io, peers, replacement, target));
    EXPECT_TRUE(PeerSetTestAccess::has_connection(io, peers, hub->endpoint()));
    EXPECT_FALSE(PeerSetTestAccess::has_connection(
        io, peers, failed_archival->endpoint()));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, replacement->endpoint()));
    auto counts = PeerSetTestAccess::connected_pool_counts(io, peers);
    EXPECT_EQ(counts.first, 1u);
    EXPECT_EQ(counts.second, 1u);

    // The first replacement is now a usable covering peer, so another
    // simultaneous candidate cannot churn the pool or cross the cap.
    EXPECT_FALSE(
        PeerSetTestAccess::admit_connected_peer(io, peers, extra, target));
    counts = PeerSetTestAccess::connected_pool_counts(io, peers);
    EXPECT_EQ(counts.first, 1u);
    EXPECT_EQ(counts.second, 1u);

    PeerSetTestAccess::release_wanted_ledger(io, peers, target);
}

TEST(PeerSessionInterface, HubAdmissionNeverConsumesArchivalSlot)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 1,
            .archival_range_threshold = 500,
        });
    constexpr uint32_t target = 100;

    auto archival = std::make_shared<FakePeerSession>(
        io, "archive-kept:1", true, 2000, 1000, 2000);
    auto old_hub =
        std::make_shared<FakePeerSession>(io, "hub-old:1", true, 950, 900, 950);
    auto target_hub = std::make_shared<FakePeerSession>(
        io, "hub-target:1", true, 150, 50, 150);

    PeerSetTestAccess::insert_connected_peer(io, peers, archival);
    PeerSetTestAccess::insert_connected_peer(io, peers, old_hub);
    PeerSetTestAccess::acquire_wanted_ledger(io, peers, target);

    EXPECT_TRUE(
        PeerSetTestAccess::admit_connected_peer(io, peers, target_hub, target));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, archival->endpoint()));
    EXPECT_FALSE(
        PeerSetTestAccess::has_connection(io, peers, old_hub->endpoint()));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, target_hub->endpoint()));
    auto counts = PeerSetTestAccess::connected_pool_counts(io, peers);
    EXPECT_EQ(counts.first, 1u);
    EXPECT_EQ(counts.second, 1u);

    PeerSetTestAccess::release_wanted_ledger(io, peers, target);
}

TEST(PeerSessionInterface, UsefulArchivalPeerIsNotRotated)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 1,
            .archival_range_threshold = 100,
        });
    constexpr uint32_t target = 500;

    auto useful = std::make_shared<FakePeerSession>(
        io, "archive-useful:1", true, 1000, 300, 1000);
    auto candidate = std::make_shared<FakePeerSession>(
        io, "archive-unneeded:1", true, 1000, 200, 1100);
    PeerSetTestAccess::insert_connected_peer(io, peers, useful);
    PeerSetTestAccess::acquire_wanted_ledger(io, peers, target);

    EXPECT_FALSE(
        PeerSetTestAccess::admit_connected_peer(io, peers, candidate, target));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, useful->endpoint()));
    EXPECT_FALSE(
        PeerSetTestAccess::has_connection(io, peers, candidate->endpoint()));
    auto counts = PeerSetTestAccess::connected_pool_counts(io, peers);
    EXPECT_EQ(counts.first, 0u);
    EXPECT_EQ(counts.second, 1u);

    PeerSetTestAccess::release_wanted_ledger(io, peers, target);
}

TEST(PeerSessionInterface, UntargetedCandidateCannotRotateFullPool)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 0,
            .archival_range_threshold = 1000,
        });
    constexpr uint32_t target = 100;
    auto existing = std::make_shared<FakePeerSession>(
        io, "existing-hub:1", true, 950, 900, 950);
    auto candidate = std::make_shared<FakePeerSession>(
        io, "ordinary-candidate:1", true, 150, 50, 150);

    PeerSetTestAccess::insert_connected_peer(io, peers, existing);
    PeerSetTestAccess::acquire_wanted_ledger(io, peers, target);

    EXPECT_FALSE(PeerSetTestAccess::admit_connected_peer(io, peers, candidate));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, existing->endpoint()));
    EXPECT_FALSE(
        PeerSetTestAccess::has_connection(io, peers, candidate->endpoint()));

    PeerSetTestAccess::release_wanted_ledger(io, peers, target);
}

TEST(PeerSessionInterface, NonCoveringProbeNeedsKnownBadVictim)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 0,
            .archival_range_threshold = 1000,
        });
    constexpr uint32_t target = 500;
    auto existing = std::make_shared<FakePeerSession>(
        io, "untried-hub:1", true, 950, 900, 950);
    auto candidate = std::make_shared<FakePeerSession>(
        io, "noncovering-candidate:1", true, 1200, 1100, 1200);

    PeerSetTestAccess::insert_connected_peer(io, peers, existing);
    EXPECT_FALSE(
        PeerSetTestAccess::admit_connected_peer(io, peers, candidate, target));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, existing->endpoint()));
}

TEST(PeerSessionInterface, NonCoveringProbeCanReplaceFailedVictim)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 0,
            .archival_range_threshold = 1000,
        });
    constexpr uint32_t target = 500;
    auto failed = std::make_shared<FakePeerSession>(
        io, "failed-hub:1", true, 950, 900, 950);
    auto candidate = std::make_shared<FakePeerSession>(
        io, "untried-candidate:1", true, 1200, 1100, 1200);

    PeerSetTestAccess::insert_connected_peer(io, peers, failed);
    PeerSetTestAccess::note_ledger_failure(
        io, peers, failed->endpoint(), target);
    EXPECT_TRUE(
        PeerSetTestAccess::admit_connected_peer(io, peers, candidate, target));
    EXPECT_FALSE(
        PeerSetTestAccess::has_connection(io, peers, failed->endpoint()));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, candidate->endpoint()));
}

TEST(PeerSessionInterface, ActivePeerGenerationPinPinsUntilFinalShareReleases)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 0,
            .archival_range_threshold = 1000,
        });
    constexpr uint32_t target = 500;
    auto existing = std::make_shared<FakePeerSession>(
        io, "pinned-hub:1", true, 600, 400, 600);
    auto candidate = std::make_shared<FakePeerSession>(
        io, "replacement-hub:1", true, 650, 450, 650);

    ASSERT_EQ(existing->pending_count(), 0u);
    PeerSetTestAccess::insert_connected_peer(io, peers, existing);
    auto pin =
        PeerSetTestAccess::make_peer_pin(io, peers, existing->endpoint());
    ASSERT_TRUE(pin);
    auto shared_operation_pin = pin.share();
    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 1u);

    EXPECT_FALSE(PeerSetTestAccess::admit_connected_peer(
        io, peers, candidate, target, {existing->endpoint()}));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, existing->endpoint()));
    EXPECT_FALSE(
        PeerSetTestAccess::has_connection(io, peers, candidate->endpoint()));

    pin.reset();
    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 1u);
    shared_operation_pin.reset();
    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 0u);
    EXPECT_TRUE(PeerSetTestAccess::admit_connected_peer(
        io, peers, candidate, target, {existing->endpoint()}));
    EXPECT_FALSE(
        PeerSetTestAccess::has_connection(io, peers, existing->endpoint()));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, candidate->endpoint()));
}

TEST(PeerSessionInterface, StalePinReleaseCannotConsumeReplacementGeneration)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto old_session = std::make_shared<FakePeerSession>(
        io, "same-endpoint:1", true, 600, 400, 600);
    auto replacement = std::make_shared<FakePeerSession>(
        io, "same-endpoint:1", true, 700, 500, 700);

    PeerSetTestAccess::insert_connected_peer(io, peers, old_session);
    auto old_pin =
        PeerSetTestAccess::make_peer_pin_for_session(io, peers, old_session);
    ASSERT_TRUE(old_pin);

    // Test-only forced removal models a delayed release arriving after the
    // endpoint has been admitted under a new generation.
    PeerSetTestAccess::remove_peer_for_test(
        io, peers, old_session->endpoint(), false);
    PeerSetTestAccess::insert_connected_peer(io, peers, replacement);

    EXPECT_FALSE(
        PeerSetTestAccess::make_peer_pin_for_session(io, peers, old_session));
    auto replacement_pin =
        PeerSetTestAccess::make_peer_pin_for_session(io, peers, replacement);
    ASSERT_TRUE(replacement_pin);
    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 2u);

    old_pin.reset();
    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 1u);
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, replacement->endpoint()));
    EXPECT_EQ(
        PeerSetTestAccess::live_generation(io, peers, replacement->endpoint()),
        replacement_pin.connection_generation());

    replacement_pin.reset();
    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 0u);
}

TEST(PeerSessionInterface, ExcludedPeerCanBeRotatedAndRemainsDiscoverable)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 0,
            .archival_range_threshold = 1000,
        });
    constexpr uint32_t target = 500;
    auto existing = std::make_shared<FakePeerSession>(
        io, "excluded-hub:1", true, 600, 400, 600);
    auto candidate = std::make_shared<FakePeerSession>(
        io, "replacement-hub:1", true, 650, 450, 650);

    PeerSetTestAccess::insert_connected_peer(io, peers, existing);
    EXPECT_TRUE(PeerSetTestAccess::admit_connected_peer(
        io, peers, candidate, target, {existing->endpoint()}));
    EXPECT_FALSE(
        PeerSetTestAccess::has_connection(io, peers, existing->endpoint()));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, candidate->endpoint()));
    EXPECT_TRUE(
        PeerSetTestAccess::tracker_contains(io, peers, existing->endpoint()));
}

TEST(PeerSessionInterface, RotationPreservesCoverageForOtherWantedLedger)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 2,
            .max_archival_peers = 0,
            .archival_range_threshold = 5000,
        });
    constexpr uint32_t requested = 500;
    constexpr uint32_t other_wanted = 900;
    auto only_other = std::make_shared<FakePeerSession>(
        io, "only-other-ledger:1", true, 950, 850, 950);
    auto replaceable = std::make_shared<FakePeerSession>(
        io, "replaceable-hub:1", true, 2000, 1000, 2000);
    auto candidate = std::make_shared<FakePeerSession>(
        io, "requested-ledger:1", true, 550, 450, 550);

    PeerSetTestAccess::prioritize_ledger(io, peers, other_wanted);
    PeerSetTestAccess::insert_connected_peer(io, peers, only_other);
    PeerSetTestAccess::insert_connected_peer(io, peers, replaceable);

    EXPECT_TRUE(PeerSetTestAccess::admit_connected_peer(
        io, peers, candidate, requested));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, only_other->endpoint()));
    EXPECT_FALSE(
        PeerSetTestAccess::has_connection(io, peers, replaceable->endpoint()));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, candidate->endpoint()));
}

TEST(PeerSessionInterface, RotationPreservesSameSequenceConcurrentWaiter)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 0,
            .archival_range_threshold = 1000,
        });
    constexpr uint32_t target = 500;
    auto useful = std::make_shared<FakePeerSession>(
        io, "same-seq-useful:1", true, 600, 400, 600);
    auto candidate = std::make_shared<FakePeerSession>(
        io, "same-seq-probe:1", true, 900, 700, 900);

    PeerSetTestAccess::insert_connected_peer(io, peers, useful);
    auto other_wait =
        PeerSetTestAccess::acquire_targeted_wanted_ledger(io, peers, target);

    EXPECT_FALSE(PeerSetTestAccess::admit_connected_peer(
        io, peers, candidate, target, {useful->endpoint()}));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, useful->endpoint()));
    EXPECT_FALSE(
        PeerSetTestAccess::has_connection(io, peers, candidate->endpoint()));

    PeerSetTestAccess::release_targeted_wanted_ledger(
        io, peers, std::move(other_wait));
}

TEST(PeerSessionInterface, RotationHonorsOtherWaiterExclusions)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 2,
            .max_archival_peers = 0,
            .archival_range_threshold = 1000,
        });
    constexpr uint32_t target = 500;
    constexpr uint32_t other_target = 900;
    auto useful = std::make_shared<FakePeerSession>(
        io, "other-wait-useful:1", true, 950, 850, 950);
    auto excluded_alternative = std::make_shared<FakePeerSession>(
        io, "other-wait-excluded:1", true, 960, 840, 960);
    auto candidate = std::make_shared<FakePeerSession>(
        io, "target-candidate:1", true, 550, 450, 550);

    PeerSetTestAccess::insert_connected_peer(io, peers, useful);
    PeerSetTestAccess::insert_connected_peer(io, peers, excluded_alternative);
    auto other_wait = PeerSetTestAccess::acquire_targeted_wanted_ledger(
        io, peers, other_target, {excluded_alternative->endpoint()});

    EXPECT_TRUE(PeerSetTestAccess::admit_connected_peer(
        io, peers, candidate, target, {useful->endpoint()}));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, useful->endpoint()));
    EXPECT_FALSE(PeerSetTestAccess::has_connection(
        io, peers, excluded_alternative->endpoint()));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, candidate->endpoint()));

    PeerSetTestAccess::release_targeted_wanted_ledger(
        io, peers, std::move(other_wait));
}

TEST(PeerSessionInterface, QuarantinedCandidateCannotReplaceOnlyCoverage)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 0,
            .archival_range_threshold = 1000,
        });
    constexpr uint32_t target = 500;
    constexpr uint32_t other_target = 550;
    auto useful = std::make_shared<FakePeerSession>(
        io, "quarantine-useful:1", true, 600, 400, 600);
    auto candidate = std::make_shared<FakePeerSession>(
        io, "quarantined-candidate:1", true, 600, 400, 600);

    PeerSetTestAccess::insert_connected_peer(io, peers, useful);
    PeerSetTestAccess::prioritize_ledger(io, peers, other_target);
    PeerSetTestAccess::note_ledger_failure(
        io, peers, candidate->endpoint(), other_target);

    EXPECT_FALSE(PeerSetTestAccess::admit_connected_peer(
        io, peers, candidate, target, {useful->endpoint()}));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, useful->endpoint()));
    EXPECT_FALSE(
        PeerSetTestAccess::has_connection(io, peers, candidate->endpoint()));
}

TEST(PeerSessionInterface, UnavailableCandidateCannotRotatePool)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 0,
            .archival_range_threshold = 1000,
        });
    constexpr uint32_t target = 500;
    auto existing = std::make_shared<FakePeerSession>(
        io, "ready-existing:1", true, 600, 400, 600);
    auto unavailable = std::make_shared<FakePeerSession>(
        io, "unavailable-candidate:1", false, 600, 400, 600);

    PeerSetTestAccess::insert_connected_peer(io, peers, existing);
    EXPECT_FALSE(PeerSetTestAccess::admit_connected_peer(
        io, peers, unavailable, target, {existing->endpoint()}));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, existing->endpoint()));
}

TEST(PeerSessionInterface, ClaimedProbeSurvivesFallbackButNotCancellation)
{
    constexpr uint32_t target = 500;

    asio::io_context fallback_io;
    auto fallback_peers = PeerSet::create(
        fallback_io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 0,
            .archival_range_threshold = 1000,
        });
    auto fallback_existing = std::make_shared<FakePeerSession>(
        fallback_io, "fallback-existing:1", true, 900, 800, 900);
    auto fallback_candidate = std::make_shared<FakePeerSession>(
        fallback_io, "fallback-candidate:1", true, 600, 400, 600);
    PeerSetTestAccess::insert_connected_peer(
        fallback_io, fallback_peers, fallback_existing);

    EXPECT_TRUE(PeerSetTestAccess::admit_connected_peer_after_wait_finishes(
        fallback_io,
        fallback_peers,
        fallback_candidate,
        target,
        {fallback_existing->endpoint()},
        false));

    asio::io_context canceled_io;
    auto canceled_peers = PeerSet::create(
        canceled_io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 0,
            .archival_range_threshold = 1000,
        });
    auto canceled_existing = std::make_shared<FakePeerSession>(
        canceled_io, "canceled-existing:1", true, 900, 800, 900);
    auto canceled_candidate = std::make_shared<FakePeerSession>(
        canceled_io, "canceled-candidate:1", true, 600, 400, 600);
    PeerSetTestAccess::insert_connected_peer(
        canceled_io, canceled_peers, canceled_existing);

    EXPECT_FALSE(PeerSetTestAccess::admit_connected_peer_after_wait_finishes(
        canceled_io,
        canceled_peers,
        canceled_candidate,
        target,
        {canceled_existing->endpoint()},
        true));
    EXPECT_TRUE(PeerSetTestAccess::has_connection(
        canceled_io, canceled_peers, canceled_existing->endpoint()));
}

// ---------------------------------------------------------------------------
// Terminal receipt channel: the peer-client death export and PeerSet's
// generation-gated consumer (v4 design references T72 -> T39). These are
// deterministic single-strand interleavings driven through the test access
// harness — no wall-clock races.
// ---------------------------------------------------------------------------

TEST(PeerTerminalReceipt, CurrentGenerationTerminalRemovesPeer)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{.network_id = 42});

    auto fake = std::make_shared<FakePeerSession>(
        io, "fake-term:1", true, 500, 400, 600);
    auto const key = fake->endpoint();
    auto const gen =
        PeerSetTestAccess::insert_ready_peer_with_generation(io, peers, fake);
    ASSERT_GT(gen, 0u);
    ASSERT_EQ(PeerSetTestAccess::live_generation(io, peers, key), gen);

    PeerSetTestAccess::consume_terminal(
        io,
        peers,
        key,
        gen,
        {boost::asio::error::eof, "packet header read: end of stream"});

    EXPECT_EQ(PeerSetTestAccess::live_generation(io, peers, key), 0u);
    EXPECT_EQ(PeerSetTestAccess::total_disconnects(io, peers), 1u);
    EXPECT_EQ(PeerSetTestAccess::endpoint_failure_count(io, peers, key), 1u);
}

TEST(PeerTerminalReceipt, StaleTerminalCannotRemoveNewerGeneration)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{.network_id = 42});

    // Established instance holds the key.
    auto established = std::make_shared<FakePeerSession>(
        io, "fake-stale:1", true, 500, 400, 600);
    auto const key = established->endpoint();
    auto const live_gen = PeerSetTestAccess::insert_ready_peer_with_generation(
        io, peers, established);

    // A later candidate for the same endpoint died before admission; its
    // terminal record arrives carrying a generation the map never admitted.
    // Before generation gating, this record erased the healthy peer.
    auto const candidate_gen = live_gen + 1;
    PeerSetTestAccess::consume_terminal(
        io,
        peers,
        key,
        candidate_gen,
        {boost::asio::error::connection_refused, "connect: refused"});

    EXPECT_EQ(PeerSetTestAccess::live_generation(io, peers, key), live_gen)
        << "stale/foreign terminal must not remove the live instance";
    EXPECT_EQ(PeerSetTestAccess::total_disconnects(io, peers), 0u);
    EXPECT_EQ(PeerSetTestAccess::endpoint_failure_count(io, peers, key), 0u)
        << "a never-admitted candidate's death must not poison live stats";
    EXPECT_TRUE(PeerSetTestAccess::endpoint_connected_ok(io, peers, key))
        << "the live endpoint must stay selectable";
}

TEST(PeerTerminalReceipt, DuplicateTerminalIsIdempotent)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{.network_id = 42});

    auto fake = std::make_shared<FakePeerSession>(
        io, "fake-dup:1", true, 500, 400, 600);
    auto const key = fake->endpoint();
    auto const gen =
        PeerSetTestAccess::insert_ready_peer_with_generation(io, peers, fake);

    PeerTerminalReason const reason{
        boost::asio::error::connection_reset, "packet payload read: reset"};
    PeerSetTestAccess::consume_terminal(io, peers, key, gen, reason);
    PeerSetTestAccess::consume_terminal(io, peers, key, gen, reason);

    EXPECT_EQ(PeerSetTestAccess::total_disconnects(io, peers), 1u)
        << "replayed terminal record must not double-count";
    EXPECT_EQ(PeerSetTestAccess::endpoint_failure_count(io, peers, key), 1u)
        << "replayed terminal record must not double-record the failure";
}

TEST(PeerTerminalReceipt, EvictionThenLateTerminalDoesNotDoubleCount)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{.network_id = 42});

    auto fake = std::make_shared<FakePeerSession>(
        io, "fake-evict:1", true, 500, 400, 600);
    auto const key = fake->endpoint();
    auto const gen =
        PeerSetTestAccess::insert_ready_peer_with_generation(io, peers, fake);

    // PeerSet-initiated eviction removes the row first...
    PeerSetTestAccess::remove_peer_for_test(io, peers, key, false);
    ASSERT_EQ(PeerSetTestAccess::total_disconnects(io, peers), 1u);

    // ...then the instance's own death record arrives late (a read failure
    // racing the eviction's close). It must record the failure fact once
    // and must not double-count the disconnect.
    PeerSetTestAccess::consume_terminal(
        io,
        peers,
        key,
        gen,
        {boost::asio::error::operation_aborted, "socket closed"});

    EXPECT_EQ(PeerSetTestAccess::total_disconnects(io, peers), 1u);
    EXPECT_EQ(PeerSetTestAccess::live_generation(io, peers, key), 0u);
    EXPECT_EQ(PeerSetTestAccess::endpoint_failure_count(io, peers, key), 0u)
        << "an eviction's late terminal must not add a failure";
}

TEST(PeerTerminalReceipt, InterleavedGenerationsConvergeToNewestSurvivor)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{.network_id = 42});

    // gen1 admitted, dies; gen2 admitted at the same endpoint; gen1's
    // duplicate record and an unknown candidate's record both arrive after.
    auto fake1 = std::make_shared<FakePeerSession>(
        io, "fake-ilv:1", true, 500, 400, 600);
    auto const key = fake1->endpoint();
    auto const gen1 =
        PeerSetTestAccess::insert_ready_peer_with_generation(io, peers, fake1);
    PeerSetTestAccess::consume_terminal(
        io, peers, key, gen1, {boost::asio::error::eof, "eof"});

    auto fake2 = std::make_shared<FakePeerSession>(
        io, "fake-ilv:1", true, 520, 400, 650);
    auto const gen2 =
        PeerSetTestAccess::insert_ready_peer_with_generation(io, peers, fake2);
    ASSERT_GT(gen2, gen1);

    // Late duplicate of gen1's terminal: no row carries that generation
    // any more, so the record has no consuming authority — pure no-op.
    PeerSetTestAccess::consume_terminal(
        io, peers, key, gen1, {boost::asio::error::eof, "eof"});

    EXPECT_EQ(PeerSetTestAccess::live_generation(io, peers, key), gen2)
        << "the newest admitted instance must survive every stale record";
    EXPECT_EQ(PeerSetTestAccess::total_disconnects(io, peers), 1u);
    EXPECT_EQ(PeerSetTestAccess::endpoint_failure_count(io, peers, key), 1u);
}

TEST(PeerTerminalReceipt, HigherCandidateTerminalCannotBlockLowerLiveTerminal)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{.network_id = 42});

    auto live = std::make_shared<FakePeerSession>(
        io, "fake-order:1", true, 500, 400, 600);
    auto const key = live->endpoint();
    auto const live_gen =
        PeerSetTestAccess::insert_ready_peer_with_generation(io, peers, live);

    // A concurrently minted, never-admitted candidate with a HIGHER
    // generation dies first. Terminal delivery is not ordered by
    // generation; this record must be a pure no-op.
    PeerSetTestAccess::consume_terminal(
        io,
        peers,
        key,
        live_gen + 5,
        {boost::asio::error::connection_refused, "connect: refused"});
    ASSERT_EQ(PeerSetTestAccess::live_generation(io, peers, key), live_gen);

    // The live instance's own, LOWER-generation terminal must still have
    // full authority afterward. (A scalar high-water broke exactly here:
    // the dead row would have been stranded in connections_.)
    PeerSetTestAccess::consume_terminal(
        io,
        peers,
        key,
        live_gen,
        {boost::asio::error::eof, "packet header read: end of stream"});

    EXPECT_EQ(PeerSetTestAccess::live_generation(io, peers, key), 0u)
        << "the live instance's genuine terminal must still remove the row";
    EXPECT_EQ(PeerSetTestAccess::total_disconnects(io, peers), 1u);
    EXPECT_EQ(PeerSetTestAccess::endpoint_failure_count(io, peers, key), 1u);
}

TEST(PeerTerminalReceipt, PreAdmissionConnectFailureIsCountedExactlyOnce)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{.network_id = 42});

    // Reserve an ephemeral loopback port, then close it so the connect is
    // deterministically refused.
    boost::asio::ip::tcp::acceptor acceptor(
        io,
        boost::asio::ip::tcp::endpoint(
            boost::asio::ip::make_address("127.0.0.1"), 0));
    auto const port = acceptor.local_endpoint().port();
    acceptor.close();

    auto session =
        PeerSetTestAccess::try_real_connect(io, peers, "127.0.0.1", port);
    EXPECT_EQ(session, nullptr);

    auto const key = "127.0.0.1:" + std::to_string(port);
    EXPECT_EQ(PeerSetTestAccess::endpoint_failure_count(io, peers, key), 1u)
        << "a refused TCP connect completes in the async-connect handler "
           "and never reaches fail_and_close, so exactly one failure is "
           "recorded by the note_connect_failure path alone";
    EXPECT_EQ(PeerSetTestAccess::total_disconnects(io, peers), 0u);
    EXPECT_EQ(PeerSetTestAccess::live_generation(io, peers, key), 0u);
}

TEST(PeerTerminalReceipt, NeverAdmittedTerminalCannotAlterConnectFailureStats)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{.network_id = 42});

    // The legacy connect-failure path records the candidate's failure once.
    auto const key = std::string{"10.0.0.9:51235"};
    PeerSetTestAccess::note_connect_failure_for_test(
        io, peers, key, "tls handshake: timeout");
    ASSERT_EQ(PeerSetTestAccess::endpoint_failure_count(io, peers, key), 1u);

    // The same candidate's terminal record then arrives: never admitted,
    // no row, no authority. It must not double-account the failure or
    // fabricate a disconnect. This pins the ownership rule that fixed
    // blocker 2: pre-admission failure belongs to note_connect_failure,
    // and the terminal channel consumes only exact live matches.
    PeerSetTestAccess::consume_terminal(
        io,
        peers,
        key,
        7,
        {boost::asio::error::timed_out, "tls handshake: timeout"});

    EXPECT_EQ(PeerSetTestAccess::endpoint_failure_count(io, peers, key), 1u)
        << "never-admitted terminal must not double-account the failure";
    EXPECT_EQ(PeerSetTestAccess::total_disconnects(io, peers), 0u);
    EXPECT_EQ(PeerSetTestAccess::live_generation(io, peers, key), 0u);
}
