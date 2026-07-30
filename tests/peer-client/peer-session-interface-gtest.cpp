#include "peer-client-test-access.h"
#include "peer-set-test-access.h"
#include <catl/peer-client/ledger-fetch-coordinator.h>
#include <catl/peer-client/node-cache.h>
#include <catl/peer-client/peer-set.h>

#include <catl/core/context-executor.h>
#include <catl/core/logger.h>
#include <catl/core/request-context.h>

#include "../test-support/deterministic-scheduler.h"
#include "../test-support/io-context-runners.h"

#include "ripple.pb.h"

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/streambuf.hpp>
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
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
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
static_assert(std::is_default_constructible_v<WalkResult>);
static_assert(!std::is_constructible_v<
              WalkResult,
              std::vector<PathNode>,
              std::vector<PlaceholderNode>,
              WalkTerminal>);
static_assert(
    std::is_same_v<
        PeerSession::NodeResponseHandler,
        std::function<void(std::shared_ptr<protocol::TMLedgerData> const&)>>);

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
        , advertised_ledger_range_(
              AdvertisedLedgerRange{first_seq, last_seq}.packed())
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

    AdvertisedLedgerRange
    advertised_ledger_range() const override
    {
        return AdvertisedLedgerRange::from_packed(
            advertised_ledger_range_.load(std::memory_order_acquire));
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

    asio::awaitable<void>
    co_disconnect() override
    {
        auto const call_index =
            co_disconnect_calls.fetch_add(1, std::memory_order_relaxed);
        disconnect();
        auto gate = disconnect_receipt_gates_.empty()
            ? nullptr
            : disconnect_receipt_gates_[std::min<std::size_t>(
                  static_cast<std::size_t>(call_index),
                  disconnect_receipt_gates_.size() - 1)];
        if (gate)
        {
            co_disconnect_waiting.store(true, std::memory_order_release);
            boost::system::error_code ec;
            co_await gate->async_wait(
                asio::redirect_error(asio::use_awaitable, ec));
            co_disconnect_waiting.store(false, std::memory_order_release);
        }
        co_return;
    }

    asio::strand<asio::io_context::executor_type>&
    strand() override
    {
        return strand_;
    }

    std::atomic<std::uint64_t> co_disconnect_calls{0};
    std::atomic<bool> co_disconnect_waiting{false};

    void
    hold_disconnect_receipt()
    {
        disconnect_receipt_gates_.push_back(
            std::make_shared<asio::steady_timer>(
                strand_, asio::steady_timer::time_point::max()));
    }

    void
    release_disconnect_receipt()
    {
        for (auto const& gate : disconnect_receipt_gates_)
        {
            asio::post(strand_, [gate] {
                if (gate)
                    gate->cancel();
            });
        }
    }

    void
    release_disconnect_receipt(std::size_t call_index)
    {
        ASSERT_LT(call_index, disconnect_receipt_gates_.size());
        auto gate = disconnect_receipt_gates_[call_index];
        asio::post(strand_, [gate] {
            if (gate)
                gate->cancel();
        });
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
    deliver_node_response(
        std::shared_ptr<protocol::TMLedgerData> const& message)
    {
        NodeResponseHandler handler;
        {
            std::lock_guard lock(mutex_);
            handler = node_response_handler_;
        }
        if (handler)
            handler(message);
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
        std::uint32_t,
        int type,
        std::vector<SHAMapNodeID> const& node_ids,
        std::function<void(boost::system::error_code)> on_write) override
    {
        SendGetNodesHandler handler;
        {
            std::lock_guard lock(mutex_);
            handler = send_get_nodes_handler_;
        }

        auto completed = std::make_shared<std::atomic<bool>>(false);
        auto complete = [completed, on_write = std::move(on_write)](
                            boost::system::error_code ec) mutable {
            if (!completed->exchange(true) && on_write)
                on_write(ec);
        };
        asio::post(
            strand_,
            [handler = std::move(handler),
             ledger_hash,
             type,
             node_ids,
             complete = std::move(complete)]() mutable {
                if (handler)
                {
                    handler(ledger_hash, type, node_ids, complete);
                    // Test handlers model packet processing, not the socket
                    // completion machinery. Unless a handler synchronously
                    // refuses, returning means its fake write completed.
                    complete({});
                    return;
                }

                complete({});
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

    void
    set_advertised_ledger_range(uint32_t first_seq, uint32_t last_seq)
    {
        advertised_ledger_range_.store(
            AdvertisedLedgerRange{first_seq, last_seq}.packed(),
            std::memory_order_release);
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
    std::atomic<std::uint64_t> advertised_ledger_range_{0};
    std::atomic<size_t> pending_{0};
    std::atomic<bool> node_batch_accepts_{false};
    std::atomic<std::size_t> node_batch_submissions_{0};
    std::atomic<std::size_t> node_batch_node_count_{0};
    std::atomic<int> node_batch_tree_{0};
    std::vector<std::shared_ptr<asio::steady_timer>> disconnect_receipt_gates_;

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
    static NodeResponseObservation
    on_node_response(
        NodeCache& cache,
        std::shared_ptr<protocol::TMLedgerData> const& message)
    {
        return cache.on_node_response(message);
    }

    struct LateSubmissionState
    {
        std::shared_ptr<NodeCache::NodeFetchRound> round;
        std::size_t managed_slot = 0;
        std::size_t raw_slot = 0;
    };

    static void
    register_test_node_operation_locked(
        NodeCache& cache,
        std::shared_ptr<NodeCache::NodeFetchRound> const& round)
    {
        round->servants_done_signal = SerializedTimerSignal::create(
            cache.io_, asio::steady_timer::time_point::max());
        round->operation_id = ++cache.next_node_operation_id_;
        cache.active_node_operations_.emplace(round->operation_id, round);
    }

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
            register_test_node_operation_locked(cache, round);
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
            register_test_node_operation_locked(cache, round);
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
            register_test_node_operation_locked(cache, round);
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
    set_header_request_timeout(NodeCache& cache, std::chrono::seconds timeout)
    {
        cache.header_request_timeout_for_test_ = timeout;
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
    set_before_node_deadline_launch_hook(
        NodeCache& cache,
        std::function<void()> before_launch)
    {
        cache.before_node_deadline_launch_for_test_ = std::move(before_launch);
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

    static std::pair<std::size_t, std::size_t>
    node_operation_census(NodeCache& cache)
    {
        std::lock_guard lock(cache.mutex_);
        std::size_t servants = 0;
        for (auto const& [_, round] : cache.active_node_operations_)
            servants += round->deadline_servants.size();
        return {cache.active_node_operations_.size(), servants};
    }

    static std::size_t
    node_store_row_count(NodeCache& cache)
    {
        std::lock_guard lock(cache.mutex_);
        return cache.store_.size();
    }

    static bool
    node_lifecycle_stopped(NodeCache& cache)
    {
        std::lock_guard lock(cache.mutex_);
        return cache.node_lifecycle_ == NodeCache::NodeLifecycle::stopped;
    }

    static void
    set_node_stop_sealed_hook(NodeCache& cache, std::function<void()> hook)
    {
        cache.node_stop_sealed_hook_for_test_ = std::move(hook);
    }

    static void
    set_node_response_hooks(
        NodeCache& cache,
        std::function<void()> before_prepare,
        std::function<void()> before_commit,
        std::function<void()> after_commit)
    {
        cache.before_node_response_prepare_for_test_ =
            std::move(before_prepare);
        cache.before_node_response_commit_for_test_ = std::move(before_commit);
        cache.after_node_response_commit_for_test_ = std::move(after_commit);
    }

    static void
    set_node_split_admission_hooks(
        NodeCache& cache,
        std::function<void()> before_primary_pin,
        std::function<void()> before_waiter_attach)
    {
        cache.before_node_primary_pin_commit_for_test_ =
            std::move(before_primary_pin);
        cache.before_node_waiter_attach_commit_for_test_ =
            std::move(before_waiter_attach);
    }

    static void
    set_node_stop_driver_hook(NodeCache& cache, std::function<void()> hook)
    {
        cache.node_stop_driver_hook_for_test_ = std::move(hook);
    }

    static void
    set_node_stop_launch_hook(NodeCache& cache, std::function<void()> hook)
    {
        cache.before_node_stop_driver_launch_for_test_ = std::move(hook);
    }

    static void
    set_node_local_pins_destroyed_hook(
        NodeCache& cache,
        std::function<void()> hook)
    {
        cache.after_node_local_pins_destroyed_for_test_ = std::move(hook);
    }

    static void
    retain_node_local_pin(NodeCache& cache, PeerGenerationPin pin)
    {
        std::lock_guard lock(cache.mutex_);
        cache.remember_node_pin_owner_barrier_locked(pin);
        cache.store_[Hash256{}].peer_pins.push_back(std::move(pin));
    }

    static bool
    node_lifecycle_running(NodeCache& cache)
    {
        std::lock_guard lock(cache.mutex_);
        return cache.node_lifecycle_ == NodeCache::NodeLifecycle::running;
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
            5500,
            peer,
            std::move(pin),
            nullptr);
    }

    static bool
    project_raw_round(NodeCache& cache, PeerSession& peer)
    {
        auto round = std::make_shared<NodeCache::NodeFetchRound>();
        round->phase = NodeCache::NodeFetchRound::Phase::response;
        {
            std::lock_guard lock(cache.mutex_);
            auto& entry = cache.store_[Hash256{}];
            entry.round = round;
            entry.signal =
                SerializedTimerSignal::create(cache.io_, std::chrono::hours(1));
            entry.round_opened_at = std::chrono::steady_clock::now();
        }

        auto const submission = cache.send_fetch(
            Hash256{},
            Hash256{},
            1,
            SHAMapNodeID::root(),
            Hash256{},
            1,
            5500,
            peer,
            {},
            round);
        cache.close_raw_submissions(Hash256{}, round);
        return submission == NodeCache::NodeFetchSubmission::raw_projected;
    }

    static bool
    raw_refusal_retired_without_write_evidence(NodeCache& cache)
    {
        std::lock_guard lock(cache.mutex_);
        auto const it = cache.store_.find(Hash256{});
        if (it == cache.store_.end() || !it->second.round)
            return false;
        auto const& round = *it->second.round;
        return !it->second.signal &&
            it->second.last_fetch_at ==
            std::chrono::steady_clock::time_point{} &&
            round.phase == NodeCache::NodeFetchRound::Phase::retired &&
            round.submissions_closed && !round.response_possible &&
            round.dispatch_count == 1 &&
            round.dispatches[0].state ==
            NodeCache::NodeFetchRound::DispatchState::not_written;
    }

    static LateSubmissionState
    seed_late_submission_callbacks(NodeCache& cache)
    {
        auto round = std::make_shared<NodeCache::NodeFetchRound>();
        round->phase = NodeCache::NodeFetchRound::Phase::response;
        {
            std::lock_guard lock(cache.mutex_);
            auto& entry = cache.store_[Hash256{}];
            register_test_node_operation_locked(cache, round);
            entry.round = round;
            entry.signal =
                SerializedTimerSignal::create(cache.io_, std::chrono::hours(1));
        }

        auto managed = cache.register_managed_submission(
            Hash256{}, round, "late-managed:1");
        auto raw =
            cache.register_raw_submission(Hash256{}, round, "late-raw:1");
        if (!managed || !raw)
            throw std::logic_error("failed to seed late submission callbacks");
        return LateSubmissionState{
            .round = std::move(round),
            .managed_slot = *managed,
            .raw_slot = *raw,
        };
    }

    static void
    deliver_late_submission_callbacks(
        NodeCache& cache,
        LateSubmissionState const& state)
    {
        cache.settle_managed_submission(
            Hash256{},
            state.round,
            state.managed_slot,
            NodeCache::ManagedSubmissionOutcome::written);
        cache.settle_raw_submission(Hash256{}, state.round, state.raw_slot, {});
    }

    static bool
    late_submission_callbacks_are_inert(LateSubmissionState const& state)
    {
        return state.round->dispatches[state.managed_slot].state ==
            NodeCache::NodeFetchRound::DispatchState::submitting &&
            state.round->dispatches[state.raw_slot].state ==
            NodeCache::NodeFetchRound::DispatchState::submitting;
    }

    static void
    install_node_response_handler(NodeCache& cache, PeerSession& peer)
    {
        cache.ensure_response_handler(peer);
    }

    static asio::awaitable<std::shared_ptr<std::vector<uint8_t> const>>
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

std::shared_ptr<protocol::TMLedgerData>
make_node_response(
    Hash256 const& ledger_hash,
    SHAMapNodeID const& node_id,
    bool valid_node)
{
    auto message = std::make_shared<protocol::TMLedgerData>();
    message->set_type(protocol::liTX_NODE);
    message->set_ledgerseq(1);
    message->set_ledgerhash(ledger_hash.data(), Hash256::size());

    auto* node = message->add_nodes();
    node->set_nodeid(node_id.to_wire());
    if (valid_node)
    {
        auto const wire = make_leaf_wire(Hash256{});
        node->set_nodedata(wire.data(), wire.size());
    }
    return message;
}

std::shared_ptr<protocol::TMLedgerData>
make_ledger_error_response(
    Hash256 const& ledger_hash,
    std::uint32_t ledger_seq,
    int type,
    protocol::TMReplyError error)
{
    auto message = std::make_shared<protocol::TMLedgerData>();
    message->set_type(static_cast<protocol::TMLedgerInfoType>(type));
    message->set_ledgerseq(ledger_seq);
    message->set_ledgerhash(ledger_hash.data(), Hash256::size());
    message->set_error(error);
    return message;
}

std::vector<std::uint8_t>
serialize(protocol::TMLedgerData const& message)
{
    std::vector<std::uint8_t> payload(message.ByteSizeLong());
    EXPECT_TRUE(message.SerializeToArray(payload.data(), payload.size()));
    return payload;
}

class ScopedErrorLogCapture
{
public:
    ScopedErrorLogCapture()
        : partition_level_("peer-client", LogLevel::WARNING)
        , error_stream_(error_)
    {
    }

    std::string
    error() const
    {
        return error_.str();
    }

private:
    std::ostringstream error_;
    ScopedLogPartitionLevel partition_level_;
    ScopedErrorLogStream error_stream_;
};

template <class T>
T
wait_for_future(std::future<T>& fut, std::chrono::seconds timeout)
{
    auto const status = fut.wait_for(timeout);
    EXPECT_EQ(status, std::future_status::ready);
    return fut.get();
}

template <class Predicate, class Rep, class Period>
bool
wait_until(Predicate&& predicate, std::chrono::duration<Rep, Period> timeout)
{
    auto const deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (predicate())
            return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return predicate();
}

}  // namespace

TEST(PeerSessionInterface, DefaultWalkResultCarriesOnlyInertFailure)
{
    WalkResult result;
    EXPECT_FALSE(result.found());
    EXPECT_TRUE(std::holds_alternative<WalkInternalFailure>(result.terminal()));
    EXPECT_TRUE(result.path().empty());
    EXPECT_TRUE(result.placeholders().empty());
}

TEST(PeerSessionInterface, NodeCacheReportsAcceptedAndIgnoredResponses)
{
    asio::io_context io;
    auto cache = NodeCache::create(io, {});
    auto const ledger_hash = hash_with_byte(0x31);
    auto const node_id = SHAMapNodeID::root();

    auto valid = make_node_response(ledger_hash, node_id, true);
    auto accepted = NodeCacheTestAccess::on_node_response(*cache, valid);
    EXPECT_TRUE(accepted.accepted);
    EXPECT_FALSE(accepted.failure);
    auto duplicate = NodeCacheTestAccess::on_node_response(*cache, valid);
    EXPECT_TRUE(duplicate.accepted)
        << "an idempotent duplicate remains accepted by the cache sink";
    EXPECT_FALSE(duplicate.failure);

    auto invalid = make_node_response(ledger_hash, node_id, false);
    auto ignored = NodeCacheTestAccess::on_node_response(*cache, invalid);
    EXPECT_FALSE(ignored.accepted);
    EXPECT_FALSE(ignored.failure);
    cache->clear_nodes();

    auto stopped_cache = NodeCache::create(io, {});
    auto stopped =
        asio::co_spawn(io, stopped_cache->co_stop_nodes(), asio::use_future);
    io.run();
    ASSERT_EQ(
        stopped.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    ASSERT_NO_THROW(stopped.get());
    auto stopped_observation =
        NodeCacheTestAccess::on_node_response(*stopped_cache, valid);
    EXPECT_FALSE(stopped_observation.accepted);
    EXPECT_FALSE(stopped_observation.failure);
}

TEST(PeerSessionInterface, NodeCachePreservesPartialAcceptanceAndFailure)
{
    asio::io_context io;
    auto const ledger_hash = hash_with_byte(0x33);
    auto const node_id = SHAMapNodeID::root();
    auto two_nodes = make_node_response(ledger_hash, node_id, true);
    *two_nodes->add_nodes() = two_nodes->nodes(0);

    auto fail_before_acceptance = NodeCache::create(io, {});
    NodeCacheTestAccess::set_node_response_hooks(
        *fail_before_acceptance,
        [] { throw std::runtime_error("before first node commit"); },
        {},
        {});
    auto first_failure = NodeCacheTestAccess::on_node_response(
        *fail_before_acceptance, two_nodes);
    EXPECT_FALSE(first_failure.accepted);
    EXPECT_TRUE(first_failure.failure);

    auto accept_then_fail = NodeCache::create(io, {});
    std::size_t preparations = 0;
    NodeCacheTestAccess::set_node_response_hooks(
        *accept_then_fail,
        [&preparations] {
            if (++preparations == 2)
                throw std::runtime_error("after first node commit");
        },
        {},
        {});
    auto partial =
        NodeCacheTestAccess::on_node_response(*accept_then_fail, two_nodes);
    EXPECT_TRUE(partial.accepted);
    EXPECT_TRUE(partial.failure);
    EXPECT_EQ(preparations, 2u);
}

TEST(PeerSessionInterface, PeerClientPreservesBothNodeResponseSinkResults)
{
    struct Case
    {
        bool cache_accepted;
        bool cache_failed;
        bool legacy_matched;
    };

    for (auto const test_case :
         {Case{false, false, false},
          Case{false, false, true},
          Case{true, false, false},
          Case{true, false, true},
          Case{false, true, false},
          Case{false, true, true},
          Case{true, true, false},
          Case{true, true, true}})
    {
        SCOPED_TRACE(
            std::string{"cache="} +
            (test_case.cache_accepted ? "accepted" : "ignored") +
            " failure=" + (test_case.cache_failed ? "present" : "absent") +
            " legacy=" + (test_case.legacy_matched ? "matched" : "absent"));

        asio::io_context io;
        auto client = PeerClientTestAccess::make(io, State::Ready);
        client->set_node_response_observer(
            [accepted = test_case.cache_accepted,
             failed = test_case.cache_failed](
                std::shared_ptr<protocol::TMLedgerData> const&) {
                return NodeResponseObservation{
                    .accepted = accepted,
                    .failure = failed
                        ? std::make_exception_ptr(
                              std::runtime_error("test sink failure"))
                        : std::exception_ptr{}};
            });

        auto const ledger_hash = hash_with_byte(0x32);
        auto const node_id = SHAMapNodeID::root();
        std::size_t legacy_callbacks = 0;
        auto const message = make_node_response(ledger_hash, node_id, true);
        auto const payload = serialize(*message);
        ScopedErrorLogCapture logs;
        asio::post(client->strand(), [&, client, payload] {
            if (test_case.legacy_matched)
            {
                EXPECT_TRUE(PeerClientTestAccess::seed_pending_nodes(
                    *client,
                    ledger_hash,
                    protocol::liTX_NODE,
                    {node_id},
                    [&legacy_callbacks](Error error, LedgerNodesResult result) {
                        EXPECT_EQ(error, Error::Success);
                        EXPECT_TRUE(static_cast<bool>(result));
                        ++legacy_callbacks;
                    }));
            }
            PeerClientTestAccess::dispatch_ledger_data(*client, payload);
        });
        io.run();

        EXPECT_EQ(
            legacy_callbacks, test_case.legacy_matched ? std::size_t{1} : 0u);
        auto const expected = std::string{"cache="} +
            (test_case.cache_accepted ? "accepted" : "ignored") +
            " legacy=" + (test_case.legacy_matched ? "matched" : "absent");
        auto const globally_unowned = !test_case.cache_accepted &&
            !test_case.cache_failed && !test_case.legacy_matched;
        auto const disposition_warning =
            logs.error().find("node response disposition:") !=
            std::string::npos;
        EXPECT_EQ(disposition_warning, globally_unowned);
        auto const failure_diagnostic =
            logs.error().find("node response sink failure:") !=
            std::string::npos;
        EXPECT_EQ(failure_diagnostic, test_case.cache_failed);
        if (globally_unowned || test_case.cache_failed)
            EXPECT_NE(logs.error().find(expected), std::string::npos);
    }
}

TEST(PeerSessionInterface, ThrowingNodeObserverCannotBypassLegacyDispatch)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready);
    client->set_node_response_observer(
        [](std::shared_ptr<protocol::TMLedgerData> const&)
            -> NodeResponseObservation {
            throw std::runtime_error("throwing sink");
        });

    auto const ledger_hash = hash_with_byte(0x34);
    auto const node_id = SHAMapNodeID::root();
    auto const message = make_node_response(ledger_hash, node_id, true);
    auto const payload = serialize(*message);
    std::size_t legacy_callbacks = 0;
    ScopedErrorLogCapture logs;
    asio::post(client->strand(), [&, client, payload] {
        EXPECT_TRUE(PeerClientTestAccess::seed_pending_nodes(
            *client,
            ledger_hash,
            protocol::liTX_NODE,
            {node_id},
            [&legacy_callbacks](Error error, LedgerNodesResult result) {
                EXPECT_EQ(error, Error::Success);
                EXPECT_TRUE(static_cast<bool>(result));
                ++legacy_callbacks;
            }));
        PeerClientTestAccess::dispatch_ledger_data(*client, payload);
    });
    io.run();

    EXPECT_EQ(legacy_callbacks, 1u);
    EXPECT_NE(
        logs.error().find("node response sink failure:"), std::string::npos);
    EXPECT_EQ(
        logs.error().find("node response disposition:"), std::string::npos);
}

TEST(
    PeerSessionInterface,
    UniqueNegativeNodeObservationResolvesExactlyOnePendingAttempt)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready);
    auto const ledger_hash = hash_with_byte(0x35);
    auto const node_id = SHAMapNodeID::root();
    std::vector<Error> completions;
    auto const response = make_ledger_error_response(
        ledger_hash, 1, protocol::liTX_NODE, protocol::reNO_LEDGER);
    auto const payload = serialize(*response);

    asio::post(client->strand(), [&, client, payload] {
        ASSERT_TRUE(PeerClientTestAccess::seed_pending_nodes(
            *client,
            ledger_hash,
            protocol::liTX_NODE,
            {node_id},
            [&completions](Error error, LedgerNodesResult) {
                completions.push_back(error);
            }));
        PeerClientTestAccess::dispatch_ledger_data(*client, payload);
    });
    io.run();

    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions.front(), Error::PeerNoLedger);
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(
    PeerSessionInterface,
    AmbiguousNegativeNodeObservationConsumesNoPendingAttempt)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready);
    auto const ledger_hash = hash_with_byte(0x36);
    auto const first_node = SHAMapNodeID::root();
    auto const second_node = first_node.child(1);
    std::vector<Error> completions;
    auto callback = [&completions](Error error, LedgerNodesResult) {
        completions.push_back(error);
    };
    auto const response = make_ledger_error_response(
        ledger_hash, 1, protocol::liTX_NODE, protocol::reNO_LEDGER);
    auto const payload = serialize(*response);

    asio::post(client->strand(), [&, client, payload, callback] {
        ASSERT_TRUE(PeerClientTestAccess::seed_pending_nodes(
            *client, ledger_hash, protocol::liTX_NODE, {first_node}, callback));
        ASSERT_TRUE(PeerClientTestAccess::seed_pending_nodes(
            *client,
            ledger_hash,
            protocol::liTX_NODE,
            {second_node},
            callback));
        PeerClientTestAccess::dispatch_ledger_data(*client, payload);
        EXPECT_TRUE(completions.empty());
        EXPECT_EQ(client->pending_count(), 2u);
        client->disconnect();
    });
    io.run();

    ASSERT_EQ(completions.size(), 2u);
    EXPECT_TRUE(std::ranges::all_of(
        completions, [](Error error) { return error == Error::Cancelled; }));
}

TEST(
    PeerSessionInterface,
    SequenceAndHashHeaderAttemptsMakeNegativeObservationAmbiguous)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready);
    auto const ledger_hash = hash_with_byte(0x37);
    constexpr std::uint32_t ledger_seq = 77;
    std::vector<Error> completions;
    auto callback = [&completions](Error error, LedgerHeaderResult) {
        completions.push_back(error);
    };
    auto const response = make_ledger_error_response(
        ledger_hash, ledger_seq, protocol::liBASE, protocol::reNO_LEDGER);
    auto const payload = serialize(*response);

    asio::post(client->strand(), [&, client, payload, callback] {
        ASSERT_TRUE(PeerClientTestAccess::seed_pending_header_by_sequence(
            *client, ledger_seq, protocol::liBASE, callback));
        ASSERT_TRUE(PeerClientTestAccess::seed_pending_header_by_hash(
            *client, ledger_hash, protocol::liBASE, callback));
        PeerClientTestAccess::dispatch_ledger_data(*client, payload);
        EXPECT_TRUE(completions.empty());
        EXPECT_EQ(client->pending_count(), 2u);
        client->disconnect();
    });
    io.run();

    ASSERT_EQ(completions.size(), 2u);
    EXPECT_TRUE(std::ranges::all_of(
        completions, [](Error error) { return error == Error::Cancelled; }));
}

TEST(PeerSessionInterface, UnexpectedRelayCookieCarriesNoLocalAuthority)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready);
    auto cache = NodeCache::create(io, {});
    auto const ledger_hash = hash_with_byte(0x38);
    auto const node_id = SHAMapNodeID::root();
    auto response = make_node_response(ledger_hash, node_id, true);
    response->set_requestcookie(1);
    auto const payload = serialize(*response);
    auto const complete_payload =
        serialize(*make_node_response(ledger_hash, node_id, true));
    std::vector<Error> completions;
    client->set_node_response_observer(
        [cache](std::shared_ptr<protocol::TMLedgerData> const& message) {
            return NodeCacheTestAccess::on_node_response(*cache, message);
        });

    asio::post(client->strand(), [&, client, payload, complete_payload] {
        ASSERT_TRUE(PeerClientTestAccess::seed_pending_nodes(
            *client,
            ledger_hash,
            protocol::liTX_NODE,
            {node_id},
            [&completions](Error error, LedgerNodesResult) {
                completions.push_back(error);
            }));
        PeerClientTestAccess::dispatch_ledger_data(*client, payload);
        EXPECT_TRUE(completions.empty());
        EXPECT_EQ(client->pending_count(), 1u);
        EXPECT_EQ(cache->size(), 1u)
            << "the cookie cannot taint independently valid node content";

        PeerClientTestAccess::dispatch_ledger_data(*client, complete_payload);
        ASSERT_EQ(completions.size(), 1u);
        EXPECT_EQ(completions.front(), Error::Success);
        EXPECT_EQ(client->pending_count(), 0u);
    });
    io.run();

    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions.front(), Error::Success);
}

TEST(
    PeerSessionInterface,
    AmbiguousSuccessfulNodeObservationMayEnterCacheButResolvesNoRequest)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready);
    auto const ledger_hash = hash_with_byte(0x39);
    auto const root = SHAMapNodeID::root();
    auto const child = root.child(2);
    auto const response = make_node_response(ledger_hash, child, true);
    auto const payload = serialize(*response);
    std::vector<Error> completions;
    std::size_t cache_observations = 0;
    client->set_node_response_observer(
        [&cache_observations](std::shared_ptr<protocol::TMLedgerData> const&) {
            ++cache_observations;
            return NodeResponseObservation{.accepted = true};
        });
    auto callback = [&completions](Error error, LedgerNodesResult) {
        completions.push_back(error);
    };

    asio::post(client->strand(), [&, client, payload, callback] {
        ASSERT_TRUE(PeerClientTestAccess::seed_pending_nodes(
            *client, ledger_hash, protocol::liTX_NODE, {root}, callback));
        ASSERT_TRUE(PeerClientTestAccess::seed_pending_nodes(
            *client, ledger_hash, protocol::liTX_NODE, {child}, callback));
        PeerClientTestAccess::dispatch_ledger_data(*client, payload);
        EXPECT_EQ(cache_observations, 1u);
        EXPECT_TRUE(completions.empty());
        EXPECT_EQ(client->pending_count(), 2u);
        client->disconnect();
    });
    io.run();

    ASSERT_EQ(completions.size(), 2u);
    EXPECT_TRUE(std::ranges::all_of(
        completions, [](Error error) { return error == Error::Cancelled; }));
}

TEST(
    PeerSessionInterface,
    PartiallyMalformedNodeObservationMayEnterCacheButResolvesNoRequest)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready);
    auto cache = NodeCache::create(io, {});
    auto const ledger_hash = hash_with_byte(0x3a);
    auto const node_id = SHAMapNodeID::root();
    auto partial_response = make_node_response(ledger_hash, node_id, true);
    partial_response->add_nodes()->set_nodedata("malformed-without-node-id");
    auto const partial_payload = serialize(*partial_response);
    auto const complete_payload =
        serialize(*make_node_response(ledger_hash, node_id, true));
    std::vector<Error> completions;
    client->set_node_response_observer(
        [cache](std::shared_ptr<protocol::TMLedgerData> const& message) {
            return NodeCacheTestAccess::on_node_response(*cache, message);
        });

    asio::post(
        client->strand(), [&, client, partial_payload, complete_payload] {
            ASSERT_TRUE(PeerClientTestAccess::seed_pending_nodes(
                *client,
                ledger_hash,
                protocol::liTX_NODE,
                {node_id},
                [&completions](Error error, LedgerNodesResult) {
                    completions.push_back(error);
                }));
            PeerClientTestAccess::dispatch_ledger_data(
                *client, partial_payload);
            EXPECT_EQ(cache->size(), 1u)
                << "the valid content-addressed node remains useful";
            EXPECT_TRUE(completions.empty());
            EXPECT_EQ(client->pending_count(), 1u);

            PeerClientTestAccess::dispatch_ledger_data(
                *client, complete_payload);
            ASSERT_EQ(completions.size(), 1u);
            EXPECT_EQ(completions.front(), Error::Success);
            EXPECT_EQ(client->pending_count(), 0u);
            EXPECT_EQ(cache->size(), 1u)
                << "the malformed node never became a second cache entry";
        });
    io.run();

    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions.front(), Error::Success);
}

TEST(
    PeerSessionInterface,
    RegisteredButUninitiatedNodeAttemptCannotConsumeWireObservation)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready);
    auto const ledger_hash = hash_with_byte(0x3b);
    auto const node_id = SHAMapNodeID::root();
    auto const payload =
        serialize(*make_node_response(ledger_hash, node_id, true));
    std::vector<Error> completions;

    asio::post(client->strand(), [&, client, payload] {
        ASSERT_TRUE(PeerClientTestAccess::seed_pending_nodes(
            *client,
            ledger_hash,
            protocol::liTX_NODE,
            {node_id},
            [&completions](Error error, LedgerNodesResult) {
                completions.push_back(error);
            },
            false));

        PeerClientTestAccess::dispatch_ledger_data(*client, payload);

        EXPECT_TRUE(completions.empty())
            << "registration alone is not evidence that a request started";
        EXPECT_EQ(client->pending_count(), 1u);
        client->disconnect();
    });
    io.run();

    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions.front(), Error::Cancelled);
}

TEST(
    PeerSessionInterface,
    QueuedNodeAttemptCannotConsumeUntilTransportInitiationReturns)
{
    asio::io_context io;
    asio::ssl::context ssl_context(asio::ssl::context::tlsv12);
    auto client = PeerClientTestAccess::make(io, State::Ready);
    PeerClientTestAccess::seed_open_connection(*client, ssl_context);
    auto const ledger_hash = hash_with_byte(0x3d);
    auto const node_id = SHAMapNodeID::root();
    auto const payload =
        serialize(*make_node_response(ledger_hash, node_id, true));
    std::vector<peer_connection::test_write_completion> writes;
    std::vector<Error> completions;
    client->raw_connection().set_write_initiator_for_test(
        [&writes](
            asio::const_buffer,
            peer_connection::test_write_completion completion) {
            writes.push_back(std::move(completion));
        });

    asio::post(client->strand(), [&, client, payload] {
        EXPECT_EQ(
            client->raw_connection().async_send_packet(
                packet_type::ping,
                std::vector<std::uint8_t>(1, 0x3d),
                [](boost::system::error_code) {}),
            peer_connection::SendAdmission::accepted);
        ASSERT_EQ(writes.size(), 1u);

        client->get_tx_nodes(
            ledger_hash,
            {node_id},
            [&completions](Error error, LedgerNodesResult) {
                completions.push_back(error);
            });
        ASSERT_EQ(writes.size(), 1u)
            << "the request is admitted behind the active write but its "
               "transport initiation has not run";

        PeerClientTestAccess::dispatch_ledger_data(*client, payload);
        EXPECT_TRUE(completions.empty());
        EXPECT_EQ(client->pending_count(), 1u);

        auto predecessor = std::move(writes.front());
        predecessor({});
        ASSERT_EQ(writes.size(), 2u)
            << "retiring the predecessor must initiate the queued request";

        PeerClientTestAccess::dispatch_ledger_data(*client, payload);
        ASSERT_EQ(completions.size(), 1u);
        EXPECT_EQ(completions.front(), Error::Success);
        EXPECT_EQ(client->pending_count(), 0u);

        auto request_write = std::move(writes.back());
        request_write({});
    });
    io.run();

    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions.front(), Error::Success);
}

TEST(
    PeerSessionInterface,
    ClosedAdmissionRefusalCannotLeaveOrCorrelatePendingNodeAttempt)
{
    asio::io_context io;
    asio::ssl::context ssl_context(asio::ssl::context::tlsv12);
    auto client = PeerClientTestAccess::make(io, State::Ready);
    PeerClientTestAccess::seed_open_connection(*client, ssl_context);
    auto const ledger_hash = hash_with_byte(0x3e);
    auto const node_id = SHAMapNodeID::root();
    auto const payload =
        serialize(*make_node_response(ledger_hash, node_id, true));
    peer_connection::test_write_completion write;
    std::vector<Error> completions;
    client->raw_connection().set_write_initiator_for_test(
        [&write](
            asio::const_buffer,
            peer_connection::test_write_completion completion) {
            write = std::move(completion);
        });

    asio::post(client->strand(), [&, client, payload] {
        EXPECT_EQ(
            client->raw_connection().async_send_packet(
                packet_type::ping,
                std::vector<std::uint8_t>(1, 0x3e),
                [&, client](boost::system::error_code) {
                    client->get_tx_nodes(
                        ledger_hash,
                        {node_id},
                        [&completions](Error error, LedgerNodesResult) {
                            completions.push_back(error);
                        });
                }),
            peer_connection::SendAdmission::accepted);
        ASSERT_TRUE(write);

        auto fatal = std::move(write);
        fatal(asio::error::connection_reset);

        ASSERT_EQ(completions.size(), 1u);
        EXPECT_EQ(completions.front(), Error::Disconnected);
        EXPECT_EQ(client->pending_count(), 0u);

        PeerClientTestAccess::dispatch_ledger_data(*client, payload);
        EXPECT_EQ(completions.size(), 1u)
            << "a later packet cannot satisfy the refused attempt";
    });
    io.run();

    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions.front(), Error::Disconnected);
}

TEST(
    PeerSessionInterface,
    QueueFullRefusalRetiresPendingNodeAttemptWithoutOutOfRange)
{
    asio::io_context io;
    asio::ssl::context ssl_context(asio::ssl::context::tlsv12);
    auto client = PeerClientTestAccess::make(io, State::Ready);
    PeerClientTestAccess::seed_open_connection(*client, ssl_context);
    auto const ledger_hash = hash_with_byte(0x3f);
    auto const node_id = SHAMapNodeID::root();
    auto const payload =
        serialize(*make_node_response(ledger_hash, node_id, true));
    peer_connection::test_write_completion active_write;
    std::vector<Error> completions;
    client->raw_connection().set_write_initiator_for_test(
        [&active_write](
            asio::const_buffer,
            peer_connection::test_write_completion completion) {
            ASSERT_FALSE(active_write);
            active_write = std::move(completion);
        });

    asio::post(client->strand(), [&, client, payload] {
        EXPECT_EQ(
            client->raw_connection().async_send_packet(
                packet_type::ping,
                std::vector<std::uint8_t>(1, 0x40),
                [](boost::system::error_code) {}),
            peer_connection::SendAdmission::accepted);
        ASSERT_TRUE(active_write);

        for (std::size_t index = 0; index < 256; ++index)
        {
            EXPECT_EQ(
                client->raw_connection().async_send_packet(
                    packet_type::ping,
                    std::vector<std::uint8_t>(1, 0x41),
                    [](boost::system::error_code) {}),
                peer_connection::SendAdmission::accepted);
        }

        client->get_tx_nodes(
            ledger_hash,
            {node_id},
            [&completions](Error error, LedgerNodesResult) {
                completions.push_back(error);
            });

        ASSERT_EQ(completions.size(), 1u);
        EXPECT_EQ(completions.front(), Error::ResourceExhausted);
        EXPECT_EQ(client->pending_count(), 0u);
        PeerClientTestAccess::dispatch_ledger_data(*client, payload);
        EXPECT_EQ(completions.size(), 1u);

        auto physical_receipt = std::move(active_write);
        physical_receipt(asio::error::operation_aborted);
    });
    io.run();

    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions.front(), Error::ResourceExhausted);
}

TEST(
    PeerSessionInterface,
    StaleUniqueNodeClassificationCannotConsumeReplacementIncarnation)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready);
    auto const ledger_hash = hash_with_byte(0x3c);
    auto const node_id = SHAMapNodeID::root();
    auto const response = make_node_response(ledger_hash, node_id, true);
    auto const payload = serialize(*response);
    std::vector<Error> predecessor_completions;
    std::vector<Error> successor_completions;
    ScopedErrorLogCapture logs;

    asio::post(client->strand(), [&, client, payload] {
        ASSERT_TRUE(PeerClientTestAccess::seed_pending_nodes(
            *client,
            ledger_hash,
            protocol::liTX_NODE,
            {node_id},
            [&predecessor_completions](Error error, LedgerNodesResult) {
                predecessor_completions.push_back(error);
            }));
        PeerClientTestAccess::set_before_ledger_attempt_consume(
            *client, [&, client] {
                ASSERT_TRUE(
                    PeerClientTestAccess::replace_pending_nodes_same_key(
                        *client,
                        ledger_hash,
                        protocol::liTX_NODE,
                        {node_id},
                        [&successor_completions](
                            Error error, LedgerNodesResult) {
                            successor_completions.push_back(error);
                        }));
            });

        PeerClientTestAccess::dispatch_ledger_data(*client, payload);

        EXPECT_TRUE(predecessor_completions.empty());
        EXPECT_TRUE(successor_completions.empty())
            << "A's classified packet must not resolve replacement B";
        EXPECT_EQ(client->pending_count(), 1u)
            << "replacement B must remain the sole live attempt";
        EXPECT_NE(
            logs.error().find(
                "stale ledger-attempt correlation refused before node "
                "consumption"),
            std::string::npos);
        client->disconnect();
    });
    io.run();

    EXPECT_TRUE(predecessor_completions.empty());
    ASSERT_EQ(successor_completions.size(), 1u);
    EXPECT_EQ(successor_completions.front(), Error::Cancelled);
}

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
    EXPECT_EQ(after_unpinned.raw_fetches_projected, 0u);
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
    EXPECT_EQ(after_raw.raw_fetches_projected, 1u);
    EXPECT_EQ(after_raw.managed_fetches_reserved, 0u);
    EXPECT_EQ(after_raw.managed_fetches_refused, 0u);
    EXPECT_EQ(raw_writes.load(), 1u);
}

TEST(
    PeerSessionInterface,
    RawCoverageRevocationRetiresWithoutFalseWriteEvidence)
{
    asio::io_context io;
    asio::ssl::context ssl_context(asio::ssl::context::tlsv12);
    auto client = PeerClientTestAccess::make(io, State::Ready);
    PeerClientTestAccess::seed_open_connection(*client, ssl_context);
    PeerClientTestAccess::publish_advertised_ledger_range(
        *client, {.first_seq = 5'400, .last_seq = 5'600});

    std::size_t writes = 0;
    client->raw_connection().set_write_initiator_for_test(
        [&writes](
            asio::const_buffer,
            peer_connection::test_write_completion complete) {
            ++writes;
            complete({});
        });

    // NodeCache's preflight sees coverage, but the peer-owner strand consumes
    // this revocation before the projected write. The completion must retire
    // the exact shared round rather than manufacturing write evidence and
    // waiting for its response deadline.
    asio::post(client->strand(), [client] {
        PeerClientTestAccess::publish_advertised_ledger_range(
            *client, {.first_seq = 6'000, .last_seq = 7'000});
    });

    auto cache = NodeCache::create(io, {});
    ASSERT_TRUE(NodeCacheTestAccess::project_raw_round(*cache, *client));
    auto const projected = cache->stats();
    EXPECT_EQ(projected.raw_fetches_projected, 1u);
    EXPECT_EQ(projected.raw_writes_confirmed, 0u);
    EXPECT_EQ(projected.raw_not_written, 0u);

    io.run();

    EXPECT_EQ(writes, 0u);
    EXPECT_TRUE(NodeCacheTestAccess::raw_refusal_retired_without_write_evidence(
        *cache));
    auto const refused = cache->stats();
    EXPECT_EQ(refused.raw_fetches_projected, 1u);
    EXPECT_EQ(refused.raw_writes_confirmed, 0u);
    EXPECT_EQ(refused.raw_not_written, 1u);
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
    EXPECT_EQ(stats.raw_fetches_projected, 0u);
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
    PeerClientTestAccess::publish_advertised_ledger_range(
        *client, {.first_seq = 5400, .last_seq = 5600});
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
    EXPECT_EQ(cache->stats().raw_fetches_projected, 0u);
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
    PeerClientTestAccess::publish_advertised_ledger_range(
        *client, {.first_seq = 5400, .last_seq = 5600});
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
    auto noncovering = std::make_shared<FakePeerSession>(
        io, "managed-fanout-noncovering:1", true, 9000, 8900, 9100);
    noncovering->set_node_batch_accepts(true);
    PeerSetTestAccess::insert_connected_peer(io, peers, noncovering);

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
    EXPECT_EQ(noncovering->node_batch_submissions(), 0u);
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
    auto noncovering = std::make_shared<FakePeerSession>(
        io, "managed-retry-noncovering:1", true, 9000, 8900, 9100);
    noncovering->set_node_batch_accepts(true);
    PeerSetTestAccess::insert_connected_peer(io, peers, noncovering);

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
    auto exhausted = result.get();
    EXPECT_FALSE(exhausted.found());
    EXPECT_TRUE(
        std::holds_alternative<WalkTransportExhausted>(exhausted.terminal()));
    EXPECT_EQ(cache->stats().managed_fetches_reserved, 3u)
        << "the first two-peer round plus one covered-peer retry must consume "
           "the bounded retry budget";
    std::size_t submissions = 0;
    for (auto const& session : sessions)
        submissions += session->node_batch_submissions();
    EXPECT_GE(submissions, 3u)
        << "coordinator publication repair may retransmit each reserved row";
    EXPECT_EQ(noncovering->node_batch_submissions(), 0u);
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

TEST(PeerSessionInterface, CoveringSelectionNeverReturnsUnknownOrOutOfRangePeer)
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
    auto unknown = std::make_shared<FakePeerSession>(
        io, "fake-unknown-range:1", true, 500, 0, 0);
    auto out_of_range = std::make_shared<FakePeerSession>(
        io, "fake-out-of-range:1", true, 900, 800, 950);
    auto covering = std::make_shared<FakePeerSession>(
        io, "fake-explicit-cover:1", true, 520, 450, 650);
    PeerSetTestAccess::insert_connected_peer(io, peers, unknown);
    PeerSetTestAccess::insert_connected_peer(io, peers, out_of_range);
    PeerSetTestAccess::insert_connected_peer(io, peers, covering);

    auto selected = asio::co_spawn(
        io,
        peers->co_select_covering_peers_for(wanted, 3, {}),
        asio::use_future);
    io.run();
    auto pins = selected.get();
    ASSERT_EQ(pins.size(), 1u);
    EXPECT_EQ(pins.front()->endpoint(), covering->endpoint());

    io.restart();
    std::unordered_set<std::string> excluded{covering->endpoint()};
    auto waited = asio::co_spawn(
        io,
        peers->wait_for_covering_peer(wanted, 0, excluded),
        asio::use_future);
    io.run();
    EXPECT_FALSE(waited.get())
        << "ledger-specific waits must not fall back to merely ready peers";
}

TEST(PeerSessionInterface, NodeDispatchRechecksRangeAfterPreCommitSeam)
{
    asio::io_context io;
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .fetch_timeout = std::chrono::milliseconds(50),
        });
    auto peer = std::make_shared<FakePeerSession>(
        io, "range-changes-before-send:1", true, 5500, 5400, 5600);
    std::atomic<unsigned> sends = 0;
    peer->set_send_get_nodes_handler(
        [&sends](
            Hash256 const&,
            int,
            std::vector<SHAMapNodeID> const&,
            std::function<void(boost::system::error_code)>) {
            sends.fetch_add(1, std::memory_order_relaxed);
        });
    NodeCacheTestAccess::set_node_round_hooks(
        *cache, [peer] { peer->set_advertised_ledger_range(7000, 7100); }, {});

    bool completed = false;
    bool initiated = false;
    std::unordered_set<std::string> attempted;
    asio::co_spawn(
        io,
        [&, cache, peer]() -> asio::awaitable<void> {
            auto wire = co_await NodeCacheTestAccess::ensure_node(
                *cache, *peer, {}, &attempted, &initiated);
            EXPECT_FALSE(wire);
            completed = true;
        },
        asio::detached);
    io.run();

    EXPECT_TRUE(completed);
    EXPECT_FALSE(initiated);
    EXPECT_TRUE(attempted.empty());
    EXPECT_EQ(sends.load(std::memory_order_relaxed), 0u);
    EXPECT_EQ(cache->stats().raw_fetches_projected, 0u);
}

TEST(PeerSessionInterface, NodeWalkSseReportsCoveringPeerSearchAndExhaustion)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_targeted_connect_attempts = 0,
        });
    auto noncovering = std::make_shared<FakePeerSession>(
        io, "fake-sse-noncovering:1", true, 900, 800, 950);
    auto overlapping = std::make_shared<FakePeerSession>(
        io, "fake-sse-overlapping:1", true, 950, 900, 1000);
    auto disjoint = std::make_shared<FakePeerSession>(
        io, "fake-sse-disjoint:1", true, 1300, 1200, 1300);
    auto unknown = std::make_shared<FakePeerSession>(
        io, "fake-sse-unknown:1", true, 1400, 0, 0);
    PeerSetTestAccess::insert_connected_peer(io, peers, noncovering);
    PeerSetTestAccess::insert_connected_peer(io, peers, overlapping);
    PeerSetTestAccess::insert_connected_peer(io, peers, disjoint);
    PeerSetTestAccess::insert_connected_peer(io, peers, unknown);
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .peer_refresh_timeout_secs = 0,
        });

    auto request = std::make_shared<catl::core::RequestContext>(
        "history-search", "transaction", std::chrono::steady_clock::now());
    auto request_executor =
        catl::core::ContextExecutor(asio::make_strand(io), request);
    bool completed = false;
    asio::co_spawn(
        request_executor,
        [&, cache, peers]() -> asio::awaitable<void> {
            auto result = co_await cache->walk_to(
                hash_with_byte(0x81),
                hash_with_byte(0x82),
                hash_with_byte(0x83),
                500,
                1,
                peers,
                PeerGenerationPin{});
            EXPECT_FALSE(result.found());
            auto const* unavailable =
                std::get_if<WalkNoSuitablePeer>(&result.terminal());
            EXPECT_NE(unavailable, nullptr);
            if (unavailable)
            {
                EXPECT_EQ(unavailable->ledger_seq, 500u);
                EXPECT_TRUE(unavailable->coverage.observed);
                EXPECT_EQ(unavailable->coverage.connected_peers, 4u);
                EXPECT_EQ(unavailable->coverage.ready_peers, 4u);
                EXPECT_EQ(unavailable->coverage.advertising_peers, 3u);
                EXPECT_EQ(unavailable->coverage.advertised_ranges.size(), 2u);
                if (unavailable->coverage.advertised_ranges.size() == 2)
                {
                    EXPECT_EQ(
                        unavailable->coverage.advertised_ranges[0],
                        (AdvertisedLedgerRange{
                            .first_seq = 800, .last_seq = 1000}));
                    EXPECT_EQ(
                        unavailable->coverage.advertised_ranges[1],
                        (AdvertisedLedgerRange{
                            .first_seq = 1200, .last_seq = 1300}));
                }
            }
            completed = true;
        },
        asio::detached);

    io.run();

    EXPECT_TRUE(completed);
    auto const events = request->drain();
    EXPECT_TRUE(
        std::any_of(events.begin(), events.end(), [](std::string const& event) {
            return event == "searching for peer advertising ledger 500";
        }));
    EXPECT_TRUE(
        std::any_of(events.begin(), events.end(), [](std::string const& event) {
            return event ==
                "no peer advertising ledger 500; peers=4, advertising=3, "
                "advertised_history=[800-1000, 1200-1300]";
        }));
    EXPECT_TRUE(std::any_of(
        events.begin(),
        events.end(),
        [](std::string const& event) {
            return event ==
                "peer history search ledger 500: candidates=0/0, "
                "in_flight=0, connected=4, remaining=0s";
        }))
        << "the request context must survive the PeerSet strand hop";
    EXPECT_TRUE(
        std::any_of(events.begin(), events.end(), [](std::string const& event) {
            return event ==
                "peer history search ledger 500: no covering peer after 0s; "
                "candidates=0/0";
        }));
}

TEST(PeerSessionInterface, CoveringPeerProgressTicksAgainstAbsoluteDeadline)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_targeted_connect_attempts = 0,
        });
    auto request = std::make_shared<catl::core::RequestContext>(
        "history-progress", "transaction", std::chrono::steady_clock::now());
    auto request_executor =
        catl::core::ContextExecutor(asio::make_strand(io), request);
    bool completed = false;
    asio::co_spawn(
        request_executor,
        [&, peers]() -> asio::awaitable<void> {
            EXPECT_FALSE(co_await peers->wait_for_covering_peer(500, 2));
            completed = true;
        },
        asio::detached);

    io.run();

    EXPECT_TRUE(completed);
    auto const events = request->drain();
    auto contains = [&](std::string_view needle) {
        return std::ranges::any_of(events, [&](std::string const& event) {
            return event.find(needle) != std::string::npos;
        });
    };
    EXPECT_TRUE(contains("remaining=2s"));
    EXPECT_TRUE(contains("remaining=1s"))
        << "progress must follow the absolute deadline every second, "
           "independently of the five-second diagnostic log";
    EXPECT_TRUE(contains("remaining=0s"));
}

TEST(PeerSessionInterface, CoveringPeerProgressCountdownNeverIncreases)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_targeted_connect_attempts = 1,
        });
    auto request = std::make_shared<catl::core::RequestContext>(
        "history-progress", "transaction", std::chrono::steady_clock::now());
    auto request_executor =
        catl::core::ContextExecutor(asio::make_strand(io), request);

    constexpr uint32_t wanted = 500;
    bool completed = false;
    asio::co_spawn(
        request_executor,
        [&, peers]() -> asio::awaitable<void> {
            EXPECT_FALSE(co_await peers->wait_for_covering_peer(wanted, 2));
            completed = true;
        },
        asio::detached);

    asio::steady_timer introduce_candidate(io);
    introduce_candidate.expires_after(500ms);
    introduce_candidate.async_wait([peers](boost::system::error_code ec) {
        if (ec)
            return;
        PeerSetTestAccess::post_note_status(
            peers,
            "late-history.example.com:1",
            {.first_seq = 400, .last_seq = 600, .current_seq = 600});
    });

    io.run();

    EXPECT_TRUE(completed);
    auto const events = request->drain();
    std::vector<int> remaining_values;
    std::optional<std::size_t> first_zero_remaining_index;
    std::optional<std::size_t> terminal_index;
    for (std::size_t event_index = 0; event_index < events.size();
         ++event_index)
    {
        auto const& event = events[event_index];
        if (event.find("peer history search ledger 500: no covering peer") !=
            std::string::npos)
        {
            terminal_index = event_index;
        }
        auto const prefix = event.find("peer history search ledger 500:");
        auto const remaining = event.find("remaining=");
        if (prefix == std::string::npos || remaining == std::string::npos)
            continue;
        auto value_begin = remaining + std::string_view("remaining=").size();
        auto value_end = event.find('s', value_begin);
        ASSERT_NE(value_end, std::string::npos) << event;
        auto const remaining_value =
            std::stoi(event.substr(value_begin, value_end - value_begin));
        remaining_values.push_back(remaining_value);
        if (remaining_value == 0 && !first_zero_remaining_index)
            first_zero_remaining_index = event_index;
    }

    ASSERT_GE(remaining_values.size(), 3u);
    for (std::size_t index = 1; index < remaining_values.size(); ++index)
    {
        EXPECT_LE(remaining_values[index], remaining_values[index - 1])
            << "browser-visible countdown must be one stable request fact; "
               "events were: "
            << testing::PrintToString(remaining_values);
    }
    ASSERT_TRUE(first_zero_remaining_index)
        << "terminal failure must be preceded by a visible zero countdown";
    ASSERT_TRUE(terminal_index);
    EXPECT_LE(*first_zero_remaining_index, *terminal_index)
        << "terminal failure must not publish while visible remaining time is "
           "still positive";
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
                promise.set_value(
                    co_await peers->wait_for_covering_peer(wanted, 2));
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
                promise.set_value(
                    co_await peers->wait_for_covering_peer(wanted, 3));
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

    EXPECT_TRUE(result.found());
    ASSERT_EQ(result.path().size(), 1u);
    auto const& leaf = result.leaf();
    ASSERT_EQ(leaf.leaf_data.size() + 1u, wire.size());
    EXPECT_TRUE(
        std::equal(leaf.leaf_data.begin(), leaf.leaf_data.end(), wire.begin()));

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

    EXPECT_TRUE(result.found());
    ASSERT_EQ(result.path().size(), 1u);

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
                auto remaining =
                    co_await peers->wait_for_covering_peer(ledger_seq, 0);
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

    EXPECT_FALSE(result.found());
    auto const* exhausted =
        std::get_if<WalkTransportExhausted>(&result.terminal());
    ASSERT_NE(exhausted, nullptr);
    EXPECT_TRUE(
        std::ranges::find(exhausted->attempted_peers, first->endpoint()) !=
        exhausted->attempted_peers.end());
    EXPECT_TRUE(
        std::ranges::find(exhausted->attempted_peers, second->endpoint()) !=
        exhausted->attempted_peers.end());
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

    EXPECT_TRUE(result.found());
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
        [](Hash256 const&,
           int,
           std::vector<SHAMapNodeID> const&,
           std::function<void(boost::system::error_code)> on_error) {
            if (on_error)
                on_error(asio::error::connection_reset);
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

    EXPECT_TRUE(result.found());

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
    EXPECT_FALSE(leader_result.found());

    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 1u);
    EXPECT_FALSE(PeerSetTestAccess::admit_connected_peer(
        io, peers, candidate, ledger_seq, {existing->endpoint()}));

    cache->insert_and_notify(root_hash, wire);
    auto follower_result = wait_for_future(follower_future, 2s);
    EXPECT_TRUE(follower_result.found());

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
    EXPECT_FALSE(leader_future.get().found());

    cache->insert_and_notify(root_hash, wire);
    auto const joined_status = follower_future.wait_for(2s);
    EXPECT_EQ(joined_status, std::future_status::ready);
    if (joined_status == std::future_status::ready)
        EXPECT_TRUE(follower_future.get().found());

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
        EXPECT_TRUE(leader_future.get().found());
    if (joined_status == std::future_status::ready)
        EXPECT_TRUE(follower_future.get().found());

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
    EXPECT_FALSE(wait_for_future(done_future, 2s).found());
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
    EXPECT_FALSE(done_future.get().found());
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

TEST(NodeCacheShutdown, StopJoinsExactServantAndRefusesLateMutation)
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
            .fetch_timeout = 1h,
            .max_walk_peer_retries = 0,
        });
    auto session = std::make_shared<FakePeerSession>(
        io, "node-stop-servant:1", true, 5500, 5400, 5600);
    PeerSetTestAccess::insert_connected_peer(io, peers, session);
    auto pin = PeerSetTestAccess::make_peer_pin(io, peers, session->endpoint());
    ASSERT_TRUE(pin);

    std::promise<void> servant_started;
    auto servant_started_future = servant_started.get_future();
    std::promise<void> release_servant;
    auto release_servant_future = release_servant.get_future().share();
    std::promise<void> stop_sealed;
    auto stop_sealed_future = stop_sealed.get_future();
    std::atomic<bool> servant_reported{false};
    std::atomic<bool> seal_reported{false};

    NodeCacheTestAccess::set_node_deadline_hooks(
        *cache,
        [&]() {
            if (!servant_reported.exchange(true))
            {
                servant_started.set_value();
                release_servant_future.wait();
            }
        },
        {},
        {},
        {});
    NodeCacheTestAccess::set_node_stop_sealed_hook(*cache, [&]() {
        if (!seal_reported.exchange(true))
            stop_sealed.set_value();
    });

    bool initiated = false;
    std::unordered_set<std::string> attempted;
    auto request = asio::co_spawn(
        io,
        NodeCacheTestAccess::ensure_node(
            *cache,
            *session,
            std::move(pin),
            &attempted,
            &initiated,
            nullptr,
            nullptr,
            peers),
        asio::use_future);

    std::thread first_runner([&]() { io.run(); });
    std::thread second_runner([&]() { io.run(); });
    auto stop_runners = [&]() {
        work.reset();
        io.stop();
        first_runner.join();
        second_runner.join();
    };

    auto const servant_status = servant_started_future.wait_for(2s);
    EXPECT_EQ(servant_status, std::future_status::ready);
    if (servant_status != std::future_status::ready)
    {
        release_servant.set_value();
        stop_runners();
        return;
    }
    EXPECT_EQ(
        NodeCacheTestAccess::node_operation_census(*cache),
        (std::pair{1u, 1u}));

    auto first_stop =
        asio::co_spawn(io, cache->co_stop_nodes(), asio::use_future);
    auto joined_stop =
        asio::co_spawn(io, cache->co_stop_nodes(), asio::use_future);
    auto const seal_status = stop_sealed_future.wait_for(2s);
    EXPECT_EQ(seal_status, std::future_status::ready);
    if (seal_status != std::future_status::ready)
    {
        release_servant.set_value();
        stop_runners();
        return;
    }

    EXPECT_EQ(first_stop.wait_for(0ms), std::future_status::timeout);
    EXPECT_EQ(joined_stop.wait_for(0ms), std::future_status::timeout);
    EXPECT_FALSE(cache->insert(Hash256{}, {1, 2, 3}));
    cache->insert_and_notify(Hash256{}, {4, 5, 6});
    EXPECT_EQ(cache->size(), 0u);

    release_servant.set_value();
    auto const first_stop_status = first_stop.wait_for(2s);
    auto const joined_stop_status = joined_stop.wait_for(2s);
    EXPECT_EQ(first_stop_status, std::future_status::ready);
    EXPECT_EQ(joined_stop_status, std::future_status::ready);
    if (first_stop_status != std::future_status::ready ||
        joined_stop_status != std::future_status::ready)
    {
        stop_runners();
        return;
    }
    EXPECT_NO_THROW(first_stop.get());
    EXPECT_NO_THROW(joined_stop.get());
    auto const request_status = request.wait_for(2s);
    EXPECT_EQ(request_status, std::future_status::ready);
    if (request_status != std::future_status::ready)
    {
        stop_runners();
        return;
    }
    EXPECT_EQ(request.get(), nullptr);
    EXPECT_TRUE(initiated);
    EXPECT_EQ(
        NodeCacheTestAccess::node_operation_census(*cache),
        (std::pair{0u, 0u}));
    EXPECT_TRUE(NodeCacheTestAccess::node_lifecycle_stopped(*cache));
    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 0u);

    auto later_stop =
        asio::co_spawn(io, cache->co_stop_nodes(), asio::use_future);
    auto const later_stop_status = later_stop.wait_for(2s);
    EXPECT_EQ(later_stop_status, std::future_status::ready);
    if (later_stop_status == std::future_status::ready)
        EXPECT_NO_THROW(later_stop.get());

    stop_runners();
}

TEST(NodeCacheShutdown, DeadlineLaunchFailureRollsBackExactReservation)
{
    asio::io_context io;
    auto cache = NodeCache::create(io, {});
    auto session = std::make_shared<FakePeerSession>(
        io, "node-deadline-launch-failure:1", true, 5500, 5400, 5600);
    NodeCacheTestAccess::set_before_node_deadline_launch_hook(*cache, [] {
        throw std::runtime_error("forced deadline launch refusal");
    });

    bool initiated = false;
    std::unordered_set<std::string> attempted;
    auto request = asio::co_spawn(
        io,
        NodeCacheTestAccess::ensure_node(
            *cache, *session, {}, &attempted, &initiated),
        asio::use_future);
    io.run();

    ASSERT_EQ(
        request.wait_for(std::chrono::milliseconds(0)),
        std::future_status::ready);
    EXPECT_EQ(request.get(), nullptr);
    EXPECT_FALSE(initiated);
    EXPECT_TRUE(attempted.empty());
    EXPECT_EQ(
        NodeCacheTestAccess::node_operation_census(*cache),
        (std::pair{0u, 0u}));

    io.restart();
    auto stopped = asio::co_spawn(io, cache->co_stop_nodes(), asio::use_future);
    io.run();
    ASSERT_EQ(
        stopped.wait_for(std::chrono::milliseconds(0)),
        std::future_status::ready);
    EXPECT_NO_THROW(stopped.get());
}

TEST(NodeCacheShutdown, StopCountsOverlappingAdmissionAndResponseServants)
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
        io, "node-overlapping-deadlines:1", true, 5500, 5400, 5600);
    session->set_node_batch_accepts(true);
    PeerSetTestAccess::insert_connected_peer(io, peers, session);
    auto coordinator = LedgerFetchCoordinator::create(io, peers);
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .fetch_timeout = 1h,
            .managed_admission_timeout = 10ms,
        },
        coordinator);
    auto pin = PeerSetTestAccess::make_peer_pin(io, peers, session->endpoint());
    ASSERT_TRUE(pin);

    std::promise<void> admission_commit_reached;
    auto admission_commit_reached_future =
        admission_commit_reached.get_future();
    std::promise<void> release_admission_servant;
    auto release_admission_servant_future =
        release_admission_servant.get_future().share();
    std::promise<void> stop_sealed;
    auto stop_sealed_future = stop_sealed.get_future();
    std::atomic<bool> admission_reported{false};
    std::atomic<bool> seal_reported{false};
    NodeCacheTestAccess::set_node_deadline_hooks(
        *cache,
        {},
        {},
        [&]() {
            if (!admission_reported.exchange(true))
            {
                admission_commit_reached.set_value();
                release_admission_servant_future.wait();
            }
        },
        {});
    NodeCacheTestAccess::set_node_stop_sealed_hook(*cache, [&]() {
        if (!seal_reported.exchange(true))
            stop_sealed.set_value();
    });

    bool initiated = false;
    std::unordered_set<std::string> attempted;
    auto request = asio::co_spawn(
        io,
        NodeCacheTestAccess::ensure_node(
            *cache,
            *session,
            std::move(pin),
            &attempted,
            &initiated,
            nullptr,
            nullptr,
            peers),
        asio::use_future);

    std::thread first_runner([&]() { io.run(); });
    std::thread second_runner([&]() { io.run(); });
    auto stop_runners = [&]() {
        work.reset();
        io.stop();
        first_runner.join();
        second_runner.join();
    };

    auto const commit_status = admission_commit_reached_future.wait_for(2s);
    EXPECT_EQ(commit_status, std::future_status::ready);
    if (commit_status != std::future_status::ready)
    {
        release_admission_servant.set_value();
        stop_runners();
        return;
    }
    EXPECT_EQ(
        NodeCacheTestAccess::node_operation_census(*cache),
        (std::pair{1u, 2u}));

    auto stopped = asio::co_spawn(io, cache->co_stop_nodes(), asio::use_future);
    auto const seal_status = stop_sealed_future.wait_for(2s);
    EXPECT_EQ(seal_status, std::future_status::ready);
    if (seal_status != std::future_status::ready)
    {
        release_admission_servant.set_value();
        stop_runners();
        return;
    }
    EXPECT_EQ(stopped.wait_for(0ms), std::future_status::timeout);

    release_admission_servant.set_value();
    auto const stopped_status = stopped.wait_for(2s);
    EXPECT_EQ(stopped_status, std::future_status::ready);
    if (stopped_status != std::future_status::ready)
    {
        stop_runners();
        return;
    }
    EXPECT_NO_THROW(stopped.get());
    auto const request_status = request.wait_for(2s);
    EXPECT_EQ(request_status, std::future_status::ready);
    if (request_status == std::future_status::ready)
        EXPECT_EQ(request.get(), nullptr);
    EXPECT_EQ(
        NodeCacheTestAccess::node_operation_census(*cache),
        (std::pair{0u, 0u}));

    stop_runners();
}

TEST(NodeCacheShutdown, ResponsePreparationFailurePreservesExactOperation)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto work = asio::make_work_guard(io);
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .fetch_timeout = 1h,
            .max_walk_peer_retries = 0,
        });
    auto session = std::make_shared<FakePeerSession>(
        io, "node-response-prepare:1", true, 5500, 5400, 5600);

    std::promise<void> servant_started;
    auto servant_started_future = servant_started.get_future();
    std::atomic<bool> servant_reported{false};
    std::atomic<int> failure_phase{1};
    NodeCacheTestAccess::set_node_deadline_hooks(
        *cache,
        [&]() {
            if (!servant_reported.exchange(true))
                servant_started.set_value();
        },
        {},
        {},
        {});
    NodeCacheTestAccess::set_node_response_hooks(
        *cache,
        [&]() {
            if (failure_phase.load() == 1)
                throw std::runtime_error("forced response preparation failure");
        },
        [&]() {
            if (failure_phase.load() == 2)
                throw std::runtime_error("forced final pre-commit failure");
        },
        {});

    bool initiated = false;
    std::unordered_set<std::string> attempted;
    auto request = asio::co_spawn(
        io,
        NodeCacheTestAccess::ensure_node(
            *cache, *session, {}, &attempted, &initiated),
        asio::use_future);

    std::thread first_runner([&]() { io.run(); });
    std::thread second_runner([&]() { io.run(); });
    auto stop_runners = [&]() {
        work.reset();
        io.stop();
        first_runner.join();
        second_runner.join();
    };

    ASSERT_EQ(servant_started_future.wait_for(2s), std::future_status::ready);
    EXPECT_THROW(cache->insert(Hash256{}, {1, 2, 3}), std::runtime_error);
    EXPECT_EQ(
        NodeCacheTestAccess::node_operation_census(*cache),
        (std::pair{1u, 1u}));
    EXPECT_EQ(cache->get(Hash256{}), nullptr);

    failure_phase.store(2);
    EXPECT_THROW(
        cache->insert_and_notify(Hash256{}, {4, 5, 6}), std::runtime_error);
    EXPECT_EQ(
        NodeCacheTestAccess::node_operation_census(*cache),
        (std::pair{1u, 1u}));
    EXPECT_EQ(cache->get(Hash256{}), nullptr);

    failure_phase.store(0);
    auto stopped = asio::co_spawn(io, cache->co_stop_nodes(), asio::use_future);
    ASSERT_EQ(stopped.wait_for(2s), std::future_status::ready);
    EXPECT_NO_THROW(stopped.get());
    ASSERT_EQ(request.wait_for(2s), std::future_status::ready);
    EXPECT_EQ(request.get(), nullptr);
    EXPECT_EQ(
        NodeCacheTestAccess::node_operation_census(*cache),
        (std::pair{0u, 0u}));

    stop_runners();
}

TEST(NodeCacheShutdown, StopWakesServantWhileResponseFrameIsHeld)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto work = asio::make_work_guard(io);
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .fetch_timeout = 1h,
            .max_walk_peer_retries = 0,
        });
    auto session = std::make_shared<FakePeerSession>(
        io, "node-response-held:1", true, 5500, 5400, 5600);

    std::promise<void> servant_started;
    auto servant_started_future = servant_started.get_future();
    std::promise<void> response_committed;
    auto response_committed_future = response_committed.get_future();
    std::promise<void> release_response;
    auto release_response_future = release_response.get_future().share();
    std::atomic<bool> servant_reported{false};
    std::atomic<bool> response_reported{false};
    NodeCacheTestAccess::set_node_deadline_hooks(
        *cache,
        [&]() {
            if (!servant_reported.exchange(true))
                servant_started.set_value();
        },
        {},
        {},
        {});
    NodeCacheTestAccess::set_node_response_hooks(*cache, {}, {}, [&]() {
        if (!response_reported.exchange(true))
        {
            response_committed.set_value();
            release_response_future.wait();
        }
    });

    bool initiated = false;
    std::unordered_set<std::string> attempted;
    auto request = asio::co_spawn(
        io,
        NodeCacheTestAccess::ensure_node(
            *cache, *session, {}, &attempted, &initiated),
        asio::use_future);
    std::thread first_runner([&]() { io.run(); });
    std::thread second_runner([&]() { io.run(); });
    auto stop_runners = [&]() {
        work.reset();
        io.stop();
        first_runner.join();
        second_runner.join();
    };

    ASSERT_EQ(servant_started_future.wait_for(2s), std::future_status::ready);
    std::thread response(
        [&]() { cache->insert_and_notify(Hash256{}, {7, 8, 9}); });
    ASSERT_EQ(
        response_committed_future.wait_for(2s), std::future_status::ready);

    auto stopped = asio::co_spawn(io, cache->co_stop_nodes(), asio::use_future);
    auto const stop_status = stopped.wait_for(2s);
    EXPECT_EQ(stop_status, std::future_status::ready);

    release_response.set_value();
    response.join();
    if (stop_status == std::future_status::ready)
        EXPECT_NO_THROW(stopped.get());
    ASSERT_EQ(request.wait_for(2s), std::future_status::ready);
    (void)request.get();
    EXPECT_EQ(
        NodeCacheTestAccess::node_operation_census(*cache),
        (std::pair{0u, 0u}));

    stop_runners();
}

TEST(NodeCacheShutdown, PostCommitDiagnosticFailureCannotSuppressWakes)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto work = asio::make_work_guard(io);
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .fetch_timeout = 1h,
            .max_walk_peer_retries = 0,
        });
    auto session = std::make_shared<FakePeerSession>(
        io, "node-response-diagnostic:1", true, 5500, 5400, 5600);

    std::promise<void> servant_started;
    auto servant_started_future = servant_started.get_future();
    std::atomic<bool> servant_reported{false};
    std::promise<void> waiter_attached;
    auto waiter_attached_future = waiter_attached.get_future();
    std::atomic<bool> waiter_reported{false};
    NodeCacheTestAccess::set_node_deadline_hooks(
        *cache,
        [&]() {
            if (!servant_reported.exchange(true))
                servant_started.set_value();
        },
        {},
        {},
        {});
    NodeCacheTestAccess::set_node_round_hooks(*cache, {}, [&]() {
        if (!waiter_reported.exchange(true))
            waiter_attached.set_value();
    });
    NodeCacheTestAccess::set_node_response_hooks(*cache, {}, {}, [] {
        throw std::runtime_error("forced post-commit diagnostic failure");
    });

    bool initiated = false;
    std::unordered_set<std::string> attempted;
    auto request = asio::co_spawn(
        io,
        NodeCacheTestAccess::ensure_node(
            *cache, *session, {}, &attempted, &initiated),
        asio::use_future);
    std::thread first_runner([&]() { io.run(); });
    std::thread second_runner([&]() { io.run(); });
    auto stop_runners = [&]() {
        work.reset();
        io.stop();
        first_runner.join();
        second_runner.join();
    };

    ASSERT_EQ(servant_started_future.wait_for(2s), std::future_status::ready);
    ASSERT_EQ(waiter_attached_future.wait_for(2s), std::future_status::ready);
    EXPECT_NO_THROW(cache->insert_and_notify(Hash256{}, {10, 11, 12}));
    EXPECT_TRUE(cache->has(Hash256{}));
    ASSERT_EQ(request.wait_for(2s), std::future_status::ready);
    auto data = request.get();
    if (!data)
    {
        stop_runners();
        FAIL() << "committed response was not projected to its waiter";
        return;
    }
    EXPECT_EQ(*data, (std::vector<uint8_t>{10, 11, 12}));
    EXPECT_TRUE(wait_until(
        [&]() {
            return NodeCacheTestAccess::node_operation_census(*cache) ==
                std::pair<std::size_t, std::size_t>{0, 0};
        },
        2s));

    stop_runners();
}

TEST(NodeCacheShutdown, StopCrossesActualPeerSetStrandAfterLocalPinDestruction)
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
        io, "node-pin-barrier:1", true, 5500, 5400, 5600);
    PeerSetTestAccess::insert_connected_peer(io, peers, session);
    auto pin = PeerSetTestAccess::make_peer_pin(io, peers, session->endpoint());
    ASSERT_TRUE(pin);

    auto cache = NodeCache::create(io, {});
    NodeCacheTestAccess::retain_node_local_pin(*cache, std::move(pin));
    std::promise<void> local_pins_destroyed;
    auto local_pins_destroyed_future = local_pins_destroyed.get_future();
    NodeCacheTestAccess::set_node_local_pins_destroyed_hook(
        *cache, [&]() { local_pins_destroyed.set_value(); });
    auto work = asio::make_work_guard(io);
    std::promise<void> strand_blocked;
    auto strand_blocked_future = strand_blocked.get_future();
    std::promise<void> release_strand;
    auto release_strand_future = release_strand.get_future().share();
    PeerSetTestAccess::post_on_owner_strand(peers, [&]() {
        strand_blocked.set_value();
        release_strand_future.wait();
    });

    std::thread first_runner([&]() { io.run(); });
    std::thread second_runner([&]() { io.run(); });
    auto stop_runners = [&]() {
        work.reset();
        io.stop();
        first_runner.join();
        second_runner.join();
    };
    ASSERT_EQ(strand_blocked_future.wait_for(2s), std::future_status::ready);

    auto stopped = asio::co_spawn(io, cache->co_stop_nodes(), asio::use_future);
    EXPECT_EQ(
        local_pins_destroyed_future.wait_for(2s), std::future_status::ready);
    EXPECT_EQ(stopped.wait_for(50ms), std::future_status::timeout);
    release_strand.set_value();
    ASSERT_EQ(stopped.wait_for(2s), std::future_status::ready);
    EXPECT_NO_THROW(stopped.get());
    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 0u);

    stop_runners();
}

TEST(NodeCacheShutdown, LocalStopDoesNotClaimFinalSharedPinRelease)
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
        io, "node-pin-alias:1", true, 5500, 5400, 5600);
    PeerSetTestAccess::insert_connected_peer(io, peers, session);
    auto pin = PeerSetTestAccess::make_peer_pin(io, peers, session->endpoint());
    ASSERT_TRUE(pin);
    auto coordinator_alias = pin.share();

    auto cache = NodeCache::create(io, {});
    NodeCacheTestAccess::retain_node_local_pin(*cache, std::move(pin));
    auto work = asio::make_work_guard(io);
    std::thread runner([&]() { io.run(); });

    auto stopped = asio::co_spawn(io, cache->co_stop_nodes(), asio::use_future);
    ASSERT_EQ(stopped.wait_for(2s), std::future_status::ready);
    EXPECT_NO_THROW(stopped.get());
    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 1u);

    coordinator_alias.reset();
    auto final_barrier = asio::co_spawn(
        io, peers->co_generation_pin_release_barrier(), asio::use_future);
    ASSERT_EQ(final_barrier.wait_for(2s), std::future_status::ready);
    EXPECT_NO_THROW(final_barrier.get());
    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 0u);

    work.reset();
    io.stop();
    runner.join();
}

TEST(NodeCacheShutdown, DriverFailureIsRetainedForAllJoiners)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto cache = NodeCache::create(io, {});
    std::atomic<unsigned> driver_calls{0};
    NodeCacheTestAccess::set_node_stop_driver_hook(*cache, [&]() {
        ++driver_calls;
        throw std::runtime_error("forced node-stop driver failure");
    });

    auto first = asio::co_spawn(io, cache->co_stop_nodes(), asio::use_future);
    auto second = asio::co_spawn(io, cache->co_stop_nodes(), asio::use_future);
    io.run();

    ASSERT_EQ(first.wait_for(0ms), std::future_status::ready);
    ASSERT_EQ(second.wait_for(0ms), std::future_status::ready);
    EXPECT_THROW(first.get(), std::runtime_error);
    EXPECT_THROW(second.get(), std::runtime_error);
    EXPECT_EQ(driver_calls.load(), 1u);
    EXPECT_FALSE(NodeCacheTestAccess::node_lifecycle_running(*cache));
    EXPECT_FALSE(NodeCacheTestAccess::node_lifecycle_stopped(*cache));

    io.restart();
    auto later = asio::co_spawn(io, cache->co_stop_nodes(), asio::use_future);
    io.run();
    ASSERT_EQ(later.wait_for(0ms), std::future_status::ready);
    EXPECT_THROW(later.get(), std::runtime_error);
    EXPECT_EQ(driver_calls.load(), 1u);
}

TEST(NodeCacheShutdown, SynchronousDriverLaunchFailureIsRetained)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto cache = NodeCache::create(io, {});
    std::atomic<unsigned> launch_calls{0};
    NodeCacheTestAccess::set_node_stop_launch_hook(*cache, [&]() {
        ++launch_calls;
        throw std::runtime_error("forced node-stop launch failure");
    });

    auto first = asio::co_spawn(io, cache->co_stop_nodes(), asio::use_future);
    auto second = asio::co_spawn(io, cache->co_stop_nodes(), asio::use_future);
    io.run();

    ASSERT_EQ(first.wait_for(0ms), std::future_status::ready);
    ASSERT_EQ(second.wait_for(0ms), std::future_status::ready);
    EXPECT_THROW(first.get(), std::runtime_error);
    EXPECT_THROW(second.get(), std::runtime_error);
    EXPECT_EQ(launch_calls.load(), 1u);
    EXPECT_FALSE(NodeCacheTestAccess::node_lifecycle_running(*cache));
    EXPECT_FALSE(NodeCacheTestAccess::node_lifecycle_stopped(*cache));

    io.restart();
    auto later = asio::co_spawn(io, cache->co_stop_nodes(), asio::use_future);
    io.run();
    ASSERT_EQ(later.wait_for(0ms), std::future_status::ready);
    EXPECT_THROW(later.get(), std::runtime_error);
    EXPECT_EQ(launch_calls.load(), 1u);
}

TEST(NodeCacheShutdown, CancellingInitiatorDoesNotCancelSharedDriver)
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
        io, "node-stop-cancel:1", true, 5500, 5400, 5600);
    PeerSetTestAccess::insert_connected_peer(io, peers, session);
    auto pin = PeerSetTestAccess::make_peer_pin(io, peers, session->endpoint());
    ASSERT_TRUE(pin);

    auto cache = NodeCache::create(io, {});
    NodeCacheTestAccess::retain_node_local_pin(*cache, std::move(pin));
    std::promise<void> strand_blocked;
    auto strand_blocked_future = strand_blocked.get_future();
    std::promise<void> release_strand;
    auto release_strand_future = release_strand.get_future().share();
    PeerSetTestAccess::post_on_owner_strand(peers, [&]() {
        strand_blocked.set_value();
        release_strand_future.wait();
    });

    auto work = asio::make_work_guard(io);
    std::thread first_runner([&]() { io.run(); });
    std::thread second_runner([&]() { io.run(); });
    auto stop_runners = [&]() {
        work.reset();
        io.stop();
        first_runner.join();
        second_runner.join();
    };
    ASSERT_EQ(strand_blocked_future.wait_for(2s), std::future_status::ready);

    asio::cancellation_signal cancellation;
    std::promise<std::exception_ptr> initiator_done;
    auto initiator_done_future = initiator_done.get_future();
    std::promise<void> initiator_sealed;
    auto initiator_sealed_future = initiator_sealed.get_future();
    std::atomic<bool> seal_reported{false};
    NodeCacheTestAccess::set_node_stop_sealed_hook(*cache, [&]() {
        if (!seal_reported.exchange(true))
            initiator_sealed.set_value();
    });
    asio::co_spawn(
        io,
        cache->co_stop_nodes(),
        asio::bind_cancellation_slot(
            cancellation.slot(), [&initiator_done](std::exception_ptr failure) {
                initiator_done.set_value(std::move(failure));
            }));
    ASSERT_EQ(initiator_sealed_future.wait_for(2s), std::future_status::ready);
    auto joiner = asio::co_spawn(io, cache->co_stop_nodes(), asio::use_future);

    cancellation.emit(asio::cancellation_type::all);
    ASSERT_EQ(initiator_done_future.wait_for(2s), std::future_status::ready);
    auto initiator_failure = initiator_done_future.get();
    ASSERT_TRUE(initiator_failure);
    try
    {
        std::rethrow_exception(initiator_failure);
        FAIL() << "cancelled initiator unexpectedly succeeded";
    }
    catch (boost::system::system_error const& e)
    {
        EXPECT_EQ(e.code(), asio::error::operation_aborted);
    }
    EXPECT_EQ(joiner.wait_for(0ms), std::future_status::timeout);

    release_strand.set_value();
    ASSERT_EQ(joiner.wait_for(2s), std::future_status::ready);
    EXPECT_NO_THROW(joiner.get());
    auto later = asio::co_spawn(io, cache->co_stop_nodes(), asio::use_future);
    ASSERT_EQ(later.wait_for(2s), std::future_status::ready);
    EXPECT_NO_THROW(later.get());

    stop_runners();
}

TEST(NodeCacheShutdown, PrimaryPinCommitCannotCrossSeal)
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
        io, "node-primary-seal:1", true, 5500, 5400, 5600);
    PeerSetTestAccess::insert_connected_peer(io, peers, session);
    auto pin = PeerSetTestAccess::make_peer_pin(io, peers, session->endpoint());
    ASSERT_TRUE(pin);
    auto cache = NodeCache::create(io, NodeCache::Options{.fetch_timeout = 1h});

    std::promise<void> primary_ready;
    auto primary_ready_future = primary_ready.get_future();
    std::promise<void> release_primary;
    auto release_primary_future = release_primary.get_future().share();
    std::atomic<bool> primary_reported{false};
    NodeCacheTestAccess::set_node_split_admission_hooks(
        *cache,
        [&]() {
            if (!primary_reported.exchange(true))
            {
                primary_ready.set_value();
                release_primary_future.wait();
            }
        },
        {});

    bool initiated = false;
    std::unordered_set<std::string> attempted;
    auto request = asio::co_spawn(
        io,
        NodeCacheTestAccess::ensure_node(
            *cache,
            *session,
            std::move(pin),
            &attempted,
            &initiated,
            nullptr,
            nullptr,
            peers),
        asio::use_future);
    auto work = asio::make_work_guard(io);
    std::thread first_runner([&]() { io.run(); });
    std::thread second_runner([&]() { io.run(); });
    auto stop_runners = [&]() {
        work.reset();
        io.stop();
        first_runner.join();
        second_runner.join();
    };
    ASSERT_EQ(primary_ready_future.wait_for(2s), std::future_status::ready);

    auto stopped = asio::co_spawn(io, cache->co_stop_nodes(), asio::use_future);
    ASSERT_TRUE(wait_until(
        [&]() { return !NodeCacheTestAccess::node_lifecycle_running(*cache); },
        2s));
    release_primary.set_value();

    ASSERT_EQ(stopped.wait_for(2s), std::future_status::ready);
    EXPECT_NO_THROW(stopped.get());
    ASSERT_EQ(request.wait_for(2s), std::future_status::ready);
    EXPECT_EQ(request.get(), nullptr);
    EXPECT_FALSE(initiated);
    EXPECT_TRUE(attempted.empty());
    EXPECT_EQ(cache->size(), 0u);

    stop_runners();
}

TEST(NodeCacheShutdown, WaiterAttachCommitCannotCrossSeal)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto session = std::make_shared<FakePeerSession>(
        io, "node-waiter-seal:1", true, 5500, 5400, 5600);
    auto cache = NodeCache::create(io, NodeCache::Options{.fetch_timeout = 1h});

    std::promise<void> waiter_ready;
    auto waiter_ready_future = waiter_ready.get_future();
    std::promise<void> release_waiter;
    auto release_waiter_future = release_waiter.get_future().share();
    std::atomic<bool> waiter_reported{false};
    NodeCacheTestAccess::set_node_split_admission_hooks(*cache, {}, [&]() {
        if (!waiter_reported.exchange(true))
        {
            waiter_ready.set_value();
            release_waiter_future.wait();
        }
    });

    bool initiated = false;
    std::unordered_set<std::string> attempted;
    auto request = asio::co_spawn(
        io,
        NodeCacheTestAccess::ensure_node(
            *cache, *session, {}, &attempted, &initiated),
        asio::use_future);
    auto work = asio::make_work_guard(io);
    std::thread first_runner([&]() { io.run(); });
    std::thread second_runner([&]() { io.run(); });
    auto stop_runners = [&]() {
        work.reset();
        io.stop();
        first_runner.join();
        second_runner.join();
    };
    ASSERT_EQ(waiter_ready_future.wait_for(2s), std::future_status::ready);

    auto stopped = asio::co_spawn(io, cache->co_stop_nodes(), asio::use_future);
    ASSERT_TRUE(wait_until(
        [&]() { return !NodeCacheTestAccess::node_lifecycle_running(*cache); },
        2s));
    release_waiter.set_value();

    ASSERT_EQ(stopped.wait_for(2s), std::future_status::ready);
    EXPECT_NO_THROW(stopped.get());
    ASSERT_EQ(request.wait_for(2s), std::future_status::ready);
    EXPECT_EQ(request.get(), nullptr);
    EXPECT_TRUE(initiated);
    EXPECT_EQ(cache->size(), 0u);

    stop_runners();
}

TEST(NodeCacheShutdown, LateCallbackClassesCannotRecreateNodeState)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto cache = NodeCache::create(io, {});
    auto session = std::make_shared<FakePeerSession>(
        io, "node-late-callbacks:1", true, 5500, 5400, 5600);
    NodeCacheTestAccess::install_node_response_handler(*cache, *session);
    auto late = NodeCacheTestAccess::seed_late_submission_callbacks(*cache);
    auto const before = cache->stats();

    auto stopped = asio::co_spawn(io, cache->co_stop_nodes(), asio::use_future);
    io.run();
    ASSERT_EQ(stopped.wait_for(0ms), std::future_status::ready);
    EXPECT_NO_THROW(stopped.get());

    NodeCacheTestAccess::deliver_late_submission_callbacks(*cache, late);
    auto message = std::make_shared<protocol::TMLedgerData>();
    message->set_type(protocol::liTX_NODE);
    auto* node = message->add_nodes();
    node->set_nodeid(std::string(33, '\0'));
    auto const wire = make_leaf_wire(hash_with_byte(0xA5));
    node->set_nodedata(wire.data(), wire.size());
    session->deliver_node_response(message);

    auto const after = cache->stats();
    EXPECT_EQ(after.raw_writes_confirmed, before.raw_writes_confirmed);
    EXPECT_EQ(after.raw_not_written, before.raw_not_written);
    EXPECT_EQ(after.managed_writes_confirmed, before.managed_writes_confirmed);
    EXPECT_EQ(after.managed_not_written, before.managed_not_written);
    EXPECT_TRUE(NodeCacheTestAccess::late_submission_callbacks_are_inert(late));
    EXPECT_EQ(NodeCacheTestAccess::node_store_row_count(*cache), 0u);
    EXPECT_EQ(
        NodeCacheTestAccess::node_operation_census(*cache),
        (std::pair{0u, 0u}));
}

TEST(NodeCacheShutdown, ClearFirstKeepsOldServantAndDrainsSuccessor)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto work = asio::make_work_guard(io);
    auto cache = NodeCache::create(io, NodeCache::Options{.fetch_timeout = 1h});
    auto session = std::make_shared<FakePeerSession>(
        io, "node-clear-first:1", true, 5500, 5400, 5600);

    std::promise<void> old_servant_started;
    auto old_servant_started_future = old_servant_started.get_future();
    std::promise<void> release_old_servant;
    auto release_old_servant_future = release_old_servant.get_future().share();
    std::atomic<unsigned> servant_calls{0};
    NodeCacheTestAccess::set_node_deadline_hooks(
        *cache,
        [&]() {
            if (servant_calls.fetch_add(1) == 0)
            {
                old_servant_started.set_value();
                release_old_servant_future.wait();
            }
        },
        {},
        {},
        {});

    bool first_initiated = false;
    bool successor_initiated = false;
    std::unordered_set<std::string> first_attempted;
    std::unordered_set<std::string> successor_attempted;
    auto first_request = asio::co_spawn(
        io,
        NodeCacheTestAccess::ensure_node(
            *cache, *session, {}, &first_attempted, &first_initiated),
        asio::use_future);
    std::thread first_runner([&]() { io.run(); });
    std::thread second_runner([&]() { io.run(); });
    auto stop_runners = [&]() {
        work.reset();
        io.stop();
        first_runner.join();
        second_runner.join();
    };
    ASSERT_EQ(
        old_servant_started_future.wait_for(2s), std::future_status::ready);

    cache->clear_nodes();
    EXPECT_EQ(
        NodeCacheTestAccess::node_operation_census(*cache),
        (std::pair{1u, 1u}));
    auto successor_request = asio::co_spawn(
        io,
        NodeCacheTestAccess::ensure_node(
            *cache, *session, {}, &successor_attempted, &successor_initiated),
        asio::use_future);
    ASSERT_TRUE(wait_until(
        [&]() {
            return NodeCacheTestAccess::node_operation_census(*cache) ==
                std::pair<std::size_t, std::size_t>{2, 2};
        },
        2s));

    auto stopped = asio::co_spawn(io, cache->co_stop_nodes(), asio::use_future);
    EXPECT_EQ(stopped.wait_for(20ms), std::future_status::timeout);
    release_old_servant.set_value();
    ASSERT_EQ(stopped.wait_for(2s), std::future_status::ready);
    EXPECT_NO_THROW(stopped.get());
    ASSERT_EQ(first_request.wait_for(2s), std::future_status::ready);
    ASSERT_EQ(successor_request.wait_for(2s), std::future_status::ready);
    EXPECT_EQ(first_request.get(), nullptr);
    EXPECT_EQ(successor_request.get(), nullptr);
    EXPECT_TRUE(first_initiated);
    EXPECT_TRUE(successor_initiated);
    EXPECT_EQ(
        NodeCacheTestAccess::node_operation_census(*cache),
        (std::pair{0u, 0u}));

    stop_runners();
}

TEST(NodeCacheShutdown, StopFirstMakesCompatibilityClearInert)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto work = asio::make_work_guard(io);
    auto cache = NodeCache::create(io, NodeCache::Options{.fetch_timeout = 1h});
    auto session = std::make_shared<FakePeerSession>(
        io, "node-stop-first-clear:1", true, 5500, 5400, 5600);

    std::promise<void> servant_started;
    auto servant_started_future = servant_started.get_future();
    std::promise<void> release_servant;
    auto release_servant_future = release_servant.get_future().share();
    std::promise<void> stop_sealed;
    auto stop_sealed_future = stop_sealed.get_future();
    std::promise<void> release_seal_hook;
    auto release_seal_hook_future = release_seal_hook.get_future().share();
    std::atomic<bool> servant_reported{false};
    std::atomic<bool> seal_reported{false};
    NodeCacheTestAccess::set_node_deadline_hooks(
        *cache,
        [&]() {
            if (!servant_reported.exchange(true))
            {
                servant_started.set_value();
                release_servant_future.wait();
            }
        },
        {},
        {},
        {});
    NodeCacheTestAccess::set_node_stop_sealed_hook(*cache, [&]() {
        if (!seal_reported.exchange(true))
        {
            stop_sealed.set_value();
            release_seal_hook_future.wait();
        }
    });

    bool initiated = false;
    std::unordered_set<std::string> attempted;
    auto request = asio::co_spawn(
        io,
        NodeCacheTestAccess::ensure_node(
            *cache, *session, {}, &attempted, &initiated),
        asio::use_future);
    std::thread first_runner([&]() { io.run(); });
    std::thread second_runner([&]() { io.run(); });
    auto stop_runners = [&]() {
        work.reset();
        io.stop();
        first_runner.join();
        second_runner.join();
    };
    ASSERT_EQ(servant_started_future.wait_for(2s), std::future_status::ready);

    auto stopped = asio::co_spawn(io, cache->co_stop_nodes(), asio::use_future);
    ASSERT_EQ(stop_sealed_future.wait_for(2s), std::future_status::ready);
    ASSERT_EQ(NodeCacheTestAccess::node_store_row_count(*cache), 1u);
    cache->clear_nodes();
    EXPECT_EQ(NodeCacheTestAccess::node_store_row_count(*cache), 1u);
    EXPECT_EQ(
        NodeCacheTestAccess::node_operation_census(*cache),
        (std::pair{1u, 1u}));

    release_seal_hook.set_value();
    EXPECT_EQ(stopped.wait_for(20ms), std::future_status::timeout);
    release_servant.set_value();
    ASSERT_EQ(stopped.wait_for(2s), std::future_status::ready);
    EXPECT_NO_THROW(stopped.get());
    ASSERT_EQ(request.wait_for(2s), std::future_status::ready);
    EXPECT_EQ(request.get(), nullptr);
    EXPECT_EQ(cache->size(), 0u);

    stop_runners();
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
    EXPECT_TRUE(result.found());
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
    EXPECT_FALSE(wait_for_future(first_done_future, 2s).found());
    EXPECT_EQ(
        NodeCacheTestAccess::signal(*cache, root_hash), replacement_signal);

    cache->insert_and_notify(root_hash, wire);
    EXPECT_TRUE(wait_for_future(second_done_future, 2s).found());

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
    static constexpr uint32_t ledger_seq = 6000;

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
        0u)
        << "neither aggregate expiry nor a callback from a stale round may "
           "manufacture peer-failure evidence";
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

TEST(
    PeerSessionInterface,
    PeerLocalCoverageRevocationDoesNotMintLedgerFailureEvidence)
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
            .fetch_timeout = std::chrono::milliseconds(10),
            .peer_refresh_timeout_secs = 0,
            .header_timeout_floor = std::chrono::milliseconds(20),
        });
    constexpr std::uint32_t ledger_seq = 6000;
    auto primary = std::make_shared<FakePeerSession>(
        io, "coverage-revoked:1", true, ledger_seq, 5900, 6100);
    primary->set_invoke_header_handler_inline(true);
    primary->set_get_ledger_header_handler(
        [](uint32_t, Callback<LedgerHeaderResult> callback) {
            callback(Error::PeerCoverageChanged, LedgerHeaderResult{});
        });
    PeerSetTestAccess::insert_connected_peer(io, peers, primary);

    bool terminal = false;
    asio::co_spawn(
        io,
        [&, cache, peers, primary]() -> asio::awaitable<void> {
            try
            {
                (void)co_await cache->get_header(
                    ledger_seq, peers, std::move(primary));
            }
            catch (PeerClientException const&)
            {
                terminal = true;
            }
        },
        asio::detached);
    io.run_for(std::chrono::milliseconds(250));
    io.restart();

    EXPECT_TRUE(terminal);
    EXPECT_EQ(
        PeerSetTestAccess::ledger_failure_count(
            io, peers, "coverage-revoked:1"),
        0u)
        << "one peer's local coverage revocation cannot become service-failure "
           "history";
}

TEST(
    PeerSessionInterface,
    HeaderDeadlineBeforeTransportInitiationDoesNotBlamePeer)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    asio::ssl::context ssl_context(asio::ssl::context::tlsv12);
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
            .peer_refresh_timeout_secs = 0,
            .header_timeout_floor = 100ms,
        });
    NodeCacheTestAccess::set_header_request_timeout(*cache, 0s);

    constexpr std::uint32_t ledger_seq = 6000;
    std::string const endpoint = "queued-header-timeout:1";
    auto client = PeerClientTestAccess::make(io, State::Ready);
    PeerClientTestAccess::set_endpoint(*client, endpoint);
    PeerClientTestAccess::seed_open_connection(*client, ssl_context);

    std::vector<peer_connection::test_write_completion> writes;
    client->raw_connection().set_write_initiator_for_test(
        [&writes](
            asio::const_buffer,
            peer_connection::test_write_completion complete) {
            writes.push_back(std::move(complete));
        });
    PeerSetTestAccess::insert_connected_peer(io, peers, client);

    asio::post(client->strand(), [client] {
        EXPECT_EQ(
            client->raw_connection().async_send_packet(
                packet_type::ping,
                std::vector<std::uint8_t>(1, 0x65),
                [](boost::system::error_code) {}),
            peer_connection::SendAdmission::accepted);
    });
    io.run();
    io.restart();
    ASSERT_EQ(writes.size(), 1u);

    auto terminal = asio::co_spawn(
        io,
        [cache, peers, client]() -> asio::awaitable<bool> {
            try
            {
                (void)co_await cache->get_header(ledger_seq, peers, client);
                co_return false;
            }
            catch (PeerClientException const&)
            {
                co_return true;
            }
        },
        asio::use_future);
    io.run_for(250ms);
    io.restart();

    ASSERT_EQ(terminal.wait_for(0ms), std::future_status::ready);
    EXPECT_TRUE(terminal.get());
    EXPECT_EQ(writes.size(), 1u)
        << "the header request must still be queued behind the predecessor";
    EXPECT_EQ(PeerSetTestAccess::ledger_failure_count(io, peers, endpoint), 0u)
        << "T0204 must classify a deadline before S0065 as local "
           "SendNotInitiated, not peer failure";

    auto predecessor = std::move(writes.front());
    asio::post(
        client->strand(),
        [predecessor = std::move(predecessor)]() mutable { predecessor({}); });
    io.run();
    io.restart();
    ASSERT_EQ(writes.size(), 2u);

    auto late_header_write = std::move(writes.back());
    asio::post(
        client->strand(),
        [late_header_write = std::move(late_header_write)]() mutable {
            late_header_write({});
        });
    io.run();
}

TEST(
    PeerSessionInterface,
    HeaderDeadlineAfterTransportInitiationBlamesExactPeerOnce)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    asio::ssl::context ssl_context(asio::ssl::context::tlsv12);
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
            .peer_refresh_timeout_secs = 0,
            .header_timeout_floor = 100ms,
        });
    NodeCacheTestAccess::set_header_request_timeout(*cache, 0s);

    constexpr std::uint32_t ledger_seq = 6000;
    std::string const endpoint = "initiated-header-timeout:1";
    auto client = PeerClientTestAccess::make(io, State::Ready);
    PeerClientTestAccess::set_endpoint(*client, endpoint);
    PeerClientTestAccess::seed_open_connection(*client, ssl_context);

    std::vector<peer_connection::test_write_completion> writes;
    client->raw_connection().set_write_initiator_for_test(
        [&writes](
            asio::const_buffer,
            peer_connection::test_write_completion complete) {
            writes.push_back(std::move(complete));
        });
    PeerSetTestAccess::insert_connected_peer(io, peers, client);

    auto terminal = asio::co_spawn(
        io,
        [cache, peers, client]() -> asio::awaitable<bool> {
            try
            {
                (void)co_await cache->get_header(ledger_seq, peers, client);
                co_return false;
            }
            catch (PeerClientException const&)
            {
                co_return true;
            }
        },
        asio::use_future);
    io.run_for(250ms);
    io.restart();

    ASSERT_EQ(terminal.wait_for(0ms), std::future_status::ready);
    EXPECT_TRUE(terminal.get());
    ASSERT_EQ(writes.size(), 1u)
        << "the exact header write must have crossed transport initiation";
    EXPECT_EQ(PeerSetTestAccess::ledger_failure_count(io, peers, endpoint), 1u)
        << "a post-S0065 response timeout carries peer-failure evidence";

    auto header_write = std::move(writes.front());
    asio::post(
        client->strand(), [header_write = std::move(header_write)]() mutable {
            header_write({});
        });
    io.run();
    io.restart();
    EXPECT_EQ(PeerSetTestAccess::ledger_failure_count(io, peers, endpoint), 1u)
        << "late physical completion cannot double-retire the terminal";
}

TEST(PeerSessionInterface, HeaderRoundExpiryDoesNotInventPeerFailure)
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
            .peer_refresh_timeout_secs = 0,
            .header_timeout_floor = 20ms,
        });

    constexpr std::uint32_t ledger_seq = 6000;
    std::string const endpoint = "unsettled-header-round:1";
    auto peer = std::make_shared<FakePeerSession>(
        io, endpoint, true, ledger_seq, 5900, 6100);
    Callback<LedgerHeaderResult> withheld;
    peer->set_invoke_header_handler_inline(true);
    peer->set_get_ledger_header_handler(
        [&withheld](std::uint32_t, Callback<LedgerHeaderResult> callback) {
            withheld = std::move(callback);
        });
    PeerSetTestAccess::insert_connected_peer(io, peers, peer);

    auto terminal = asio::co_spawn(
        io,
        [cache, peers, peer]() -> asio::awaitable<bool> {
            try
            {
                (void)co_await cache->get_header(ledger_seq, peers, peer);
                co_return false;
            }
            catch (PeerClientException const&)
            {
                co_return true;
            }
        },
        asio::use_future);
    io.run_for(250ms);
    io.restart();

    ASSERT_EQ(terminal.wait_for(0ms), std::future_status::ready);
    EXPECT_TRUE(terminal.get());
    ASSERT_TRUE(withheld);
    EXPECT_EQ(PeerSetTestAccess::ledger_failure_count(io, peers, endpoint), 0u)
        << "aggregate S0066 round expiry is a local bound, not an endpoint "
           "terminal";

    withheld(Error::Timeout, LedgerHeaderResult{});
    io.run();
    io.restart();
    EXPECT_EQ(PeerSetTestAccess::ledger_failure_count(io, peers, endpoint), 0u)
        << "a late stale terminal cannot retroactively blame the endpoint";
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

TEST(PeerSessionInterface, HeaderDispatchRejectsNonCoveringCallerPin)
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
            .fetch_timeout = std::chrono::milliseconds(10),
            .peer_refresh_timeout_secs = 0,
            .header_timeout_floor = std::chrono::milliseconds(50),
        });
    constexpr uint32_t ledger_seq = 6000;

    auto out_of_range = std::make_shared<FakePeerSession>(
        io, "out-of-range-header:1", true, 7000, 6900, 7100);
    auto unknown_range = std::make_shared<FakePeerSession>(
        io, "unknown-range-header:1", true, 0, 0, 0);
    auto covering = std::make_shared<FakePeerSession>(
        io, "covering-header:1", true, ledger_seq, 5900, 6100);

    std::atomic<unsigned> out_of_range_sends = 0;
    std::atomic<unsigned> unknown_range_sends = 0;
    std::atomic<unsigned> covering_sends = 0;
    out_of_range->set_get_ledger_header_handler(
        [&out_of_range_sends](uint32_t, Callback<LedgerHeaderResult>) {
            out_of_range_sends.fetch_add(1, std::memory_order_relaxed);
        });
    unknown_range->set_get_ledger_header_handler(
        [&unknown_range_sends](uint32_t, Callback<LedgerHeaderResult>) {
            unknown_range_sends.fetch_add(1, std::memory_order_relaxed);
        });
    covering->set_invoke_header_handler_inline(true);
    covering->set_get_ledger_header_handler(
        [&covering_sends](uint32_t, Callback<LedgerHeaderResult> callback) {
            covering_sends.fetch_add(1, std::memory_order_relaxed);
            callback(Error::Success, LedgerHeaderResult{});
        });

    PeerSetTestAccess::insert_connected_peer(io, peers, out_of_range);
    PeerSetTestAccess::insert_connected_peer(io, peers, unknown_range);
    PeerSetTestAccess::insert_connected_peer(io, peers, covering);
    auto pin =
        PeerSetTestAccess::make_peer_pin(io, peers, out_of_range->endpoint());
    ASSERT_TRUE(pin);

    bool completed = false;
    asio::co_spawn(
        io,
        [&, cache, peers, pin = std::move(pin)]() mutable
        -> asio::awaitable<void> {
            (void)co_await cache->get_header(ledger_seq, peers, std::move(pin));
            completed = true;
        },
        asio::detached);
    io.run();

    EXPECT_TRUE(completed);
    EXPECT_EQ(out_of_range_sends.load(std::memory_order_relaxed), 0u);
    EXPECT_EQ(unknown_range_sends.load(std::memory_order_relaxed), 0u);
    EXPECT_EQ(covering_sends.load(std::memory_order_relaxed), 1u);
}

TEST(PeerSessionInterface, HeaderDispatchFailsWithoutAdvertisedCoverage)
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
            .fetch_timeout = std::chrono::milliseconds(10),
            .peer_refresh_timeout_secs = 0,
            .header_timeout_floor = std::chrono::milliseconds(50),
        });
    static constexpr uint32_t ledger_seq = 6000;
    auto out_of_range = std::make_shared<FakePeerSession>(
        io, "only-noncovering-header:1", true, 7000, 6900, 7100);
    std::atomic<unsigned> sends = 0;
    out_of_range->set_get_ledger_header_handler(
        [&sends](uint32_t, Callback<LedgerHeaderResult>) {
            sends.fetch_add(1, std::memory_order_relaxed);
        });

    PeerSetTestAccess::insert_connected_peer(io, peers, out_of_range);
    auto pin =
        PeerSetTestAccess::make_peer_pin(io, peers, out_of_range->endpoint());
    ASSERT_TRUE(pin);

    std::optional<Error> observed;
    asio::co_spawn(
        io,
        [cache, peers, pin = std::move(pin), &observed]() mutable
        -> asio::awaitable<void> {
            try
            {
                (void)co_await cache->get_header(
                    ledger_seq, peers, std::move(pin));
            }
            catch (PeerClientException const& e)
            {
                observed = e.error;
            }
        },
        asio::detached);
    io.run();

    ASSERT_TRUE(observed);
    EXPECT_EQ(*observed, Error::Timeout);
    EXPECT_EQ(sends.load(std::memory_order_relaxed), 0u);
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
                    co_await peers->co_select_covering_peers_for(6000, 1, {});
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

TEST(PeerSessionInterface, NonCoveringProbeCannotReplaceUntriedVictim)
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

TEST(PeerSessionInterface, NonCoveringProbeCannotSpendTargetedAdmission)
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
    EXPECT_FALSE(
        PeerSetTestAccess::admit_connected_peer(io, peers, candidate, target));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, failed->endpoint()));
    EXPECT_FALSE(
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

TEST(PeerSessionInterface, ClaimedProbeSurvivesNormalCloseButNotCancellation)
{
    constexpr uint32_t target = 500;

    asio::io_context completed_io;
    auto completed_peers = PeerSet::create(
        completed_io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 0,
            .archival_range_threshold = 1000,
        });
    auto completed_existing = std::make_shared<FakePeerSession>(
        completed_io, "completed-existing:1", true, 900, 800, 900);
    auto completed_candidate = std::make_shared<FakePeerSession>(
        completed_io, "completed-candidate:1", true, 600, 400, 600);
    PeerSetTestAccess::insert_connected_peer(
        completed_io, completed_peers, completed_existing);

    EXPECT_TRUE(PeerSetTestAccess::admit_connected_peer_after_wait_finishes(
        completed_io,
        completed_peers,
        completed_candidate,
        target,
        {completed_existing->endpoint()},
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

TEST(PeerSetShutdown, IdleAndRepeatedStopsComplete)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{.network_id = 42});

    auto first = asio::co_spawn(io, peers->co_stop(), asio::use_future);
    auto joined = asio::co_spawn(io, peers->co_stop(), asio::use_future);
    io.run();
    EXPECT_NO_THROW(first.get());
    EXPECT_NO_THROW(joined.get());

    io.restart();
    auto second = asio::co_spawn(io, peers->co_stop(), asio::use_future);
    io.run();
    EXPECT_NO_THROW(second.get());
}

TEST(PeerSetShutdown, AdmittedSessionIsReceiptedlyClosed)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{.network_id = 42});
    auto session = std::make_shared<FakePeerSession>(
        io, "shutdown-peer:1", true, 500, 400, 600);
    PeerSetTestAccess::insert_ready_peer_with_generation(io, peers, session);

    auto stopped = asio::co_spawn(io, peers->co_stop(), asio::use_future);
    io.run();
    EXPECT_NO_THROW(stopped.get());

    EXPECT_EQ(session->co_disconnect_calls.load(), 1u);
    EXPECT_FALSE(session->is_ready());

    io.restart();
    EXPECT_EQ(
        PeerSetTestAccess::live_generation(io, peers, session->endpoint()), 0u);
}

TEST(PeerSetShutdown, HeldSessionReceiptDominatesTerminalPeerSetState)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{.network_id = 42});
    auto session = std::make_shared<FakePeerSession>(
        io, "held-shutdown-peer:1", true, 500, 400, 600);
    session->hold_disconnect_receipt();
    PeerSetTestAccess::insert_ready_peer_with_generation(io, peers, session);

    auto stopped = asio::co_spawn(io, peers->co_stop(), asio::use_future);
    auto joined = asio::co_spawn(io, peers->co_stop(), asio::use_future);
    io.run_for(10ms);

    EXPECT_TRUE(session->co_disconnect_waiting.load(std::memory_order_acquire));
    EXPECT_GE(session->co_disconnect_calls.load(), 1u);
    EXPECT_EQ(stopped.wait_for(0ms), std::future_status::timeout);
    EXPECT_EQ(joined.wait_for(0ms), std::future_status::timeout);

    io.restart();
    EXPECT_NE(
        PeerSetTestAccess::live_generation(io, peers, session->endpoint()), 0u)
        << "PeerSet may not erase the generation before its physical receipt";

    session->release_disconnect_receipt();
    io.restart();
    io.run();

    EXPECT_NO_THROW(stopped.get());
    EXPECT_NO_THROW(joined.get());
    EXPECT_FALSE(
        session->co_disconnect_waiting.load(std::memory_order_acquire));
    io.restart();
    EXPECT_EQ(
        PeerSetTestAccess::live_generation(io, peers, session->endpoint()), 0u);

    io.restart();
    auto late = asio::co_spawn(io, peers->co_stop(), asio::use_future);
    io.run();
    EXPECT_NO_THROW(late.get());
}

TEST(
    PeerSetShutdown,
    ActiveConnectCancellationWaitsForExactPhysicalDrainReceipt)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    constexpr std::size_t servant_count = 2;
    std::array<std::shared_ptr<asio::ip::tcp::acceptor>, servant_count>
        acceptors;
    std::array<std::shared_ptr<asio::ip::tcp::socket>, servant_count> accepted;
    std::array<std::string, servant_count> keys;
    for (std::size_t index = 0; index < servant_count; ++index)
    {
        acceptors[index] = std::make_shared<asio::ip::tcp::acceptor>(
            io,
            asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
        accepted[index] = std::make_shared<asio::ip::tcp::socket>(io);
        acceptors[index]->async_accept(
            *accepted[index],
            [socket = accepted[index]](boost::system::error_code) {});
        keys[index] = "127.0.0.1:" +
            std::to_string(acceptors[index]->local_endpoint().port());
    }

    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .endpoint_cache_path = ":memory:",
            .max_in_flight_connects = servant_count,
            .max_in_flight_crawls = 0,
        });
    std::size_t servant_receipts = 0;
    PeerSetTestAccess::set_connect_completion_diagnostic_hook(
        peers, [&servant_receipts] { ++servant_receipts; });
    for (auto const& key : keys)
        PeerSetTestAccess::push_pending_connect(io, peers, key);
    PeerSetTestAccess::pump_connects(io, peers);

    std::array<std::shared_ptr<PeerClient>, servant_count> clients;
    for (std::size_t attempt = 0; attempt < 100; ++attempt)
    {
        io.run_for(1ms);
        io.restart();
        for (std::size_t index = 0; index < servant_count; ++index)
        {
            if (!clients[index])
            {
                clients[index] = PeerSetTestAccess::connect_servant_client(
                    io, peers, keys[index]);
            }
        }
        if (std::ranges::all_of(clients, [](auto const& client) {
                return static_cast<bool>(client);
            }))
        {
            break;
        }
    }
    ASSERT_TRUE(std::ranges::all_of(
        clients, [](auto const& client) { return static_cast<bool>(client); }));
    for (std::size_t index = 0; index < servant_count; ++index)
    {
        ASSERT_NE(clients[index]->state(), State::Disconnected);
        ASSERT_TRUE(
            PeerSetTestAccess::in_flight_contains(io, peers, keys[index]));
        ASSERT_FALSE(PeerSetTestAccess::has_failed(io, peers, keys[index]));
        ASSERT_EQ(
            PeerSetTestAccess::endpoint_failure_count(io, peers, keys[index]),
            0u);
        ASSERT_EQ(
            PeerSetTestAccess::cached_endpoint_failure_count(
                io, peers, keys[index]),
            0u);
    }

    std::array<peer_connection::test_write_completion, servant_count>
        held_completions;
    for (std::size_t index = 0; index < servant_count; ++index)
    {
        asio::post(
            clients[index]->strand(),
            [client = clients[index], &held_completions, index] {
                client->raw_connection().set_write_initiator_for_test(
                    [&held_completions, index](
                        asio::const_buffer,
                        peer_connection::test_write_completion completion) {
                        held_completions[index] = std::move(completion);
                    });
                client->raw_connection().async_send_packet(
                    packet_type::ping,
                    std::vector<std::uint8_t>(64, 0xC7),
                    [](boost::system::error_code) {});
            });
    }
    for (std::size_t attempt = 0; attempt < 100; ++attempt)
    {
        io.run_for(1ms);
        io.restart();
        if (std::ranges::all_of(held_completions, [](auto const& completion) {
                return static_cast<bool>(completion);
            }))
        {
            break;
        }
    }
    ASSERT_TRUE(std::ranges::all_of(
        held_completions,
        [](auto const& completion) { return static_cast<bool>(completion); }));

    auto stopped = asio::co_spawn(io, peers->co_stop(), asio::use_future);
    io.run_for(20ms);
    io.restart();

    for (std::size_t index = 0; index < servant_count; ++index)
    {
        EXPECT_EQ(clients[index]->state(), State::Disconnected)
            << "stop must actively cancel every admitted connect servant";
        EXPECT_TRUE(
            PeerSetTestAccess::in_flight_contains(io, peers, keys[index]))
            << "cancellation intent cannot retire a servant census row";
        EXPECT_GE(
            clients[index]
                ->raw_connection()
                .transport_operation_count_for_test(),
            1u);
        EXPECT_FALSE(clients[index]
                         ->raw_connection()
                         .physical_transport_drained_for_test());
        EXPECT_FALSE(PeerSetTestAccess::has_failed(io, peers, keys[index]))
            << "owner-issued shutdown is not peer failure evidence";
        EXPECT_EQ(
            PeerSetTestAccess::endpoint_failure_count(io, peers, keys[index]),
            0u);
        EXPECT_EQ(
            PeerSetTestAccess::cached_endpoint_failure_count(
                io, peers, keys[index]),
            0u)
            << "owner-issued shutdown must not poison restart ranking";
    }
    EXPECT_EQ(stopped.wait_for(0ms), std::future_status::timeout)
        << "PeerSet terminality must wait for the exact physical receipt";

    // Keep the predecessor executable after the deliberately red assertion:
    // the manual disconnect is cleanup only. The production successor must
    // make it redundant.
    for (auto const& client : clients)
    {
        if (client->state() != State::Disconnected)
            client->disconnect();
    }
    auto first_completion = std::move(held_completions[0]);
    asio::post(
        clients[0]->strand(),
        [completion = std::move(first_completion)]() mutable {
            completion(asio::error::operation_aborted);
        });
    for (std::size_t attempt = 0; attempt < 100 &&
         PeerSetTestAccess::in_flight_contains(io, peers, keys[0]);
         ++attempt)
    {
        io.run_for(1ms);
        io.restart();
    }

    EXPECT_FALSE(PeerSetTestAccess::in_flight_contains(io, peers, keys[0]));
    EXPECT_TRUE(PeerSetTestAccess::in_flight_contains(io, peers, keys[1]))
        << "one exact receipt cannot retire an unrelated servant row";
    EXPECT_EQ(servant_receipts, 1u);
    EXPECT_EQ(stopped.wait_for(0ms), std::future_status::timeout)
        << "one remaining exact servant keeps terminality unavailable";

    auto second_completion = std::move(held_completions[1]);
    asio::post(
        clients[1]->strand(),
        [completion = std::move(second_completion)]() mutable {
            completion(asio::error::operation_aborted);
        });
    io.run_for(1s);

    ASSERT_EQ(stopped.wait_for(0ms), std::future_status::ready);
    EXPECT_NO_THROW(stopped.get());
    io.restart();
    EXPECT_FALSE(PeerSetTestAccess::in_flight_contains(io, peers, keys[1]));
    EXPECT_EQ(servant_receipts, servant_count)
        << "each admitted connect servant retires exactly once";

    auto late = asio::co_spawn(io, peers->co_stop(), asio::use_future);
    io.run_for(1s);
    ASSERT_EQ(late.wait_for(0ms), std::future_status::ready);
    EXPECT_NO_THROW(late.get())
        << "the terminal PeerSet fact must be replayable to later joiners";
    EXPECT_EQ(servant_receipts, servant_count);
}

//@@start active-crawl-cancellation-before-deadline
TEST(PeerSetShutdown, ActiveCrawlCancellationWaitsForExactPhysicalReceipt)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto acceptor = std::make_shared<asio::ip::tcp::acceptor>(
        io, asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
    auto accepted = std::make_shared<asio::ip::tcp::socket>(io);
    auto request = std::make_shared<asio::streambuf>();
    auto close_probe = std::make_shared<std::array<char, 1>>();
    auto request_seen = std::make_shared<std::promise<void>>();
    auto request_seen_future = request_seen->get_future();
    auto client_closed = std::make_shared<std::promise<void>>();
    auto client_closed_future = client_closed->get_future();

    acceptor->async_accept(
        *accepted,
        [accepted, request, close_probe, request_seen, client_closed](
            boost::system::error_code accept_error) {
            if (accept_error)
            {
                auto failure = std::make_exception_ptr(
                    boost::system::system_error(accept_error));
                request_seen->set_exception(failure);
                client_closed->set_exception(std::move(failure));
                return;
            }
            asio::async_read_until(
                *accepted,
                *request,
                "\r\n\r\n",
                [accepted, close_probe, request_seen, client_closed](
                    boost::system::error_code read_error, std::size_t) {
                    if (read_error)
                    {
                        auto failure = std::make_exception_ptr(
                            boost::system::system_error(read_error));
                        request_seen->set_exception(failure);
                        client_closed->set_exception(std::move(failure));
                        return;
                    }
                    request_seen->set_value();
                    accepted->async_read_some(
                        asio::buffer(*close_probe),
                        [accepted, close_probe, client_closed](
                            boost::system::error_code, std::size_t) {
                            client_closed->set_value();
                        });
                });
        });

    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .endpoint_cache_path = ":memory:",
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 1,
        });
    auto const key =
        "127.0.0.1:" + std::to_string(acceptor->local_endpoint().port());
    auto withheld_receipt = std::make_shared<std::function<void()>>();
    auto receipt_withheld = std::make_shared<std::atomic_bool>(false);
    std::size_t servant_receipts = 0;
    std::size_t https_fallbacks = 0;
    PeerSetTestAccess::set_before_crawl_https_fallback(
        io, peers, [&https_fallbacks] { ++https_fallbacks; });
    PeerSetTestAccess::set_defer_crawl_servant_receipt(
        io,
        peers,
        [withheld_receipt, receipt_withheld](
            std::string const&, std::function<void()> receipt) {
            *withheld_receipt = std::move(receipt);
            receipt_withheld->store(true, std::memory_order_release);
        });
    PeerSetTestAccess::set_crawl_completion_diagnostic_hook(
        io, peers, [&servant_receipts] { ++servant_receipts; });
    PeerSetTestAccess::queue_crawl(io, peers, key);
    PeerSetTestAccess::pump_crawls(io, peers);

    for (std::size_t attempt = 0; attempt < 100 &&
         request_seen_future.wait_for(0ms) != std::future_status::ready;
         ++attempt)
    {
        io.run_for(1ms);
        io.restart();
    }
    ASSERT_EQ(request_seen_future.wait_for(0ms), std::future_status::ready);
    ASSERT_NO_THROW(request_seen_future.get());

    auto stopped = asio::co_spawn(io, peers->co_stop(), asio::use_future);
    io.run_for(20ms);
    io.restart();

    ASSERT_EQ(client_closed_future.wait_for(0ms), std::future_status::ready)
        << "stop must cancel an admitted crawl instead of waiting for its "
           "configured read deadline";
    EXPECT_NO_THROW(client_closed_future.get());
    EXPECT_TRUE(receipt_withheld->load(std::memory_order_acquire));
    EXPECT_TRUE(
        PeerSetTestAccess::crawl_stop_cancellation_requested(io, peers, key));
    EXPECT_TRUE(PeerSetTestAccess::crawl_in_flight_contains(io, peers, key))
        << "cancellation intent cannot retire the crawl servant row";
    EXPECT_EQ(stopped.wait_for(0ms), std::future_status::timeout)
        << "PeerSet terminality must wait for the exact crawl receipt";
    EXPECT_EQ(servant_receipts, 0u);
    EXPECT_EQ(https_fallbacks, 0u)
        << "owner-issued stop must not mint an HTTPS fallback after the "
           "cancelled HTTP attempt";

    boost::system::error_code ignored;
    acceptor->close(ignored);
    accepted->close(ignored);

    ASSERT_TRUE(static_cast<bool>(*withheld_receipt));
    auto exact_receipt = std::move(*withheld_receipt);
    auto duplicate_receipt = exact_receipt;
    PeerSetTestAccess::post_on_owner_strand(
        peers, [exact_receipt = std::move(exact_receipt)]() mutable {
            exact_receipt();
        });
    io.run_for(1s);

    ASSERT_EQ(stopped.wait_for(0ms), std::future_status::ready);
    EXPECT_NO_THROW(stopped.get());
    io.restart();
    EXPECT_FALSE(PeerSetTestAccess::crawl_in_flight_contains(io, peers, key));
    EXPECT_EQ(servant_receipts, 1u);

    PeerSetTestAccess::post_on_owner_strand(
        peers, [duplicate_receipt = std::move(duplicate_receipt)]() mutable {
            duplicate_receipt();
        });
    io.run_for(10ms);
    io.restart();
    EXPECT_FALSE(PeerSetTestAccess::crawl_in_flight_contains(io, peers, key));
    EXPECT_EQ(servant_receipts, 1u)
        << "a duplicate deferred receipt must lose the once-only authority";

    auto late = asio::co_spawn(io, peers->co_stop(), asio::use_future);
    io.run_for(1s);
    ASSERT_EQ(late.wait_for(0ms), std::future_status::ready);
    EXPECT_NO_THROW(late.get());
    EXPECT_EQ(servant_receipts, 1u);
}
//@@end active-crawl-cancellation-before-deadline

//@@start crawl-stop-seal-before-fallback
TEST(
    PeerSetShutdown,
    PublicStopSealPreventsQueuedHttpFailureFromAdmittingHttpsFallback)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto acceptor = std::make_shared<asio::ip::tcp::acceptor>(
        io, asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
    auto http = std::make_shared<asio::ip::tcp::socket>(io);
    auto possible_https = std::make_shared<asio::ip::tcp::socket>(io);
    auto request = std::make_shared<asio::streambuf>();
    auto fallback_boundary = std::make_shared<std::promise<void>>();
    auto fallback_boundary_ready = fallback_boundary->get_future();
    auto release_boundary = std::make_shared<std::promise<void>>();
    auto release_boundary_ready = release_boundary->get_future().share();
    auto seal_boundary = std::make_shared<std::promise<void>>();
    auto seal_boundary_ready = seal_boundary->get_future();
    auto release_seal = std::make_shared<std::promise<void>>();
    auto release_seal_ready = release_seal->get_future().share();
    auto https_boundary = std::make_shared<std::promise<void>>();
    auto https_boundary_ready = https_boundary->get_future();
    auto https_accepted = std::make_shared<std::atomic_bool>(false);

    acceptor->async_accept(
        *http,
        [acceptor, http, possible_https, request, https_accepted](
            boost::system::error_code accept_error) {
            if (accept_error)
                return;
            asio::async_read_until(
                *http,
                *request,
                "\r\n\r\n",
                [acceptor, http, possible_https, https_accepted](
                    boost::system::error_code read_error, std::size_t) {
                    if (read_error)
                        return;
                    acceptor->async_accept(
                        *possible_https,
                        [possible_https, https_accepted](
                            boost::system::error_code https_error) {
                            if (!https_error)
                                https_accepted->store(
                                    true, std::memory_order_release);
                        });
                    boost::system::error_code ignored;
                    http->shutdown(
                        asio::ip::tcp::socket::shutdown_both, ignored);
                    http->close(ignored);
                });
        });

    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .endpoint_cache_path = ":memory:",
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 1,
        });
    auto const key =
        "127.0.0.1:" + std::to_string(acceptor->local_endpoint().port());
    std::size_t https_fallbacks = 0;
    PeerSetTestAccess::set_before_crawl_fallback_stop_check(
        io, peers, [fallback_boundary, release_boundary_ready] {
            fallback_boundary->set_value();
            release_boundary_ready.wait();
        });
    PeerSetTestAccess::set_before_crawl_https_fallback(
        io, peers, [&https_fallbacks, https_boundary] {
            ++https_fallbacks;
            https_boundary->set_value();
        });
    PeerSetTestAccess::set_after_crawl_ingress_seal(
        io, peers, [seal_boundary, release_seal_ready] {
            seal_boundary->set_value();
            release_seal_ready.wait();
        });
    PeerSetTestAccess::queue_crawl(io, peers, key);
    PeerSetTestAccess::pump_crawls(io, peers);

    bool boundary_reached = false;
    bool seal_observer_reached = false;
    bool stop_fact_preceded_seal = false;
    bool stop_completed = false;
    {
        catl::test::IoContextRunners runners(io, 2);
        boundary_reached =
            fallback_boundary_ready.wait_for(1s) == std::future_status::ready;

        std::future<void> stopped;
        if (boundary_reached)
        {
            stopped = asio::co_spawn(io, peers->co_stop(), asio::use_future);
            seal_observer_reached =
                seal_boundary_ready.wait_for(1s) == std::future_status::ready;
            if (seal_observer_reached)
            {
                stop_fact_preceded_seal =
                    PeerSetTestAccess::crawl_stop_fact_published(peers);
            }
        }

        release_boundary->set_value();
        if (seal_observer_reached && !stop_fact_preceded_seal)
            (void)https_boundary_ready.wait_for(1s);
        release_seal->set_value();
        if (boundary_reached)
        {
            stop_completed = stopped.wait_for(1s) == std::future_status::ready;
            if (stop_completed)
                EXPECT_NO_THROW(stopped.get());
        }
    }

    EXPECT_TRUE(boundary_reached)
        << "the control must hold the real HTTP-to-HTTPS decision boundary";
    EXPECT_TRUE(seal_observer_reached)
        << "the control must observe the public ingress seal";
    EXPECT_TRUE(stop_fact_preceded_seal)
        << "public co_stop must publish the sticky fact before the ingress "
           "seal becomes observable";
    EXPECT_TRUE(stop_completed);
    EXPECT_EQ(https_fallbacks, 0u);
    EXPECT_FALSE(https_accepted->load(std::memory_order_acquire))
        << "a queued HTTP terminal after the public seal must not admit a "
           "second transport";
}
//@@end crawl-stop-seal-before-fallback

TEST(PeerSetShutdown, PostReadyPeerFailureIsClassifiedBeforeServantRetirement)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto acceptor = std::make_shared<asio::ip::tcp::acceptor>(
        io, asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
    auto accepted = std::make_shared<asio::ip::tcp::socket>(io);
    acceptor->async_accept(*accepted, [accepted](boost::system::error_code) {});
    auto const key =
        "127.0.0.1:" + std::to_string(acceptor->local_endpoint().port());

    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .endpoint_cache_path = ":memory:",
            .max_in_flight_connects = 1,
            .max_in_flight_crawls = 0,
        });
    PeerSetTestAccess::set_after_connect_ready_before_admission(
        peers,
        [](std::shared_ptr<PeerClient> const& client) -> asio::awaitable<void> {
            co_await asio::co_spawn(
                client->strand(),
                [client]() -> asio::awaitable<void> {
                    PeerClientTestAccess::fail_initial_connect_on_strand(
                        *client);
                    co_return;
                },
                asio::use_awaitable);
        });
    PeerSetTestAccess::push_pending_connect(io, peers, key);
    PeerSetTestAccess::pump_connects(io, peers);

    std::shared_ptr<PeerClient> client;
    for (std::size_t attempt = 0; attempt < 100; ++attempt)
    {
        io.run_for(1ms);
        io.restart();
        client = PeerSetTestAccess::connect_servant_client(io, peers, key);
        if (client)
            break;
    }
    ASSERT_TRUE(client);
    ASSERT_NE(client->state(), State::Disconnected);

    asio::post(client->strand(), [client] {
        PeerClientTestAccess::become_ready_on_strand(*client);
    });
    io.run_for(1s);
    io.restart();

    auto const initial_terminal =
        PeerClientTestAccess::initial_connect_terminal(*client);
    ASSERT_TRUE(initial_terminal);
    EXPECT_EQ(
        initial_terminal->kind,
        catl::peer_client::detail::ConnectAttemptTerminalKind::ready)
        << "the immutable initial result remains Ready";
    EXPECT_EQ(client->terminal_cause(), PeerLogicalTerminalCause::peer_failure);
    EXPECT_TRUE(PeerSetTestAccess::has_failed(io, peers, key));
    EXPECT_EQ(PeerSetTestAccess::endpoint_failure_count(io, peers, key), 1u);
    EXPECT_EQ(
        PeerSetTestAccess::cached_endpoint_failure_count(io, peers, key), 1u);
    EXPECT_FALSE(PeerSetTestAccess::in_flight_contains(io, peers, key));

    auto stopped = asio::co_spawn(io, peers->co_stop(), asio::use_future);
    io.run_for(1s);
    io.restart();
    ASSERT_EQ(stopped.wait_for(0ms), std::future_status::ready);
    EXPECT_NO_THROW(stopped.get());
    EXPECT_EQ(PeerSetTestAccess::endpoint_failure_count(io, peers, key), 1u)
        << "later shutdown cannot classify the same servant twice";
}

TEST(PeerSetEndpointIdentity, PureIpv6RemainsOneKeyThroughConnectFailure)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto acceptor = std::make_shared<asio::ip::tcp::acceptor>(
        io, asio::ip::tcp::endpoint(asio::ip::make_address("::1"), 0));
    auto accepted = std::make_shared<asio::ip::tcp::socket>(io);
    acceptor->async_accept(*accepted, [accepted](boost::system::error_code) {});
    auto const port = acceptor->local_endpoint().port();
    auto const key = "[::1]:" + std::to_string(port);
    auto const split_key = "::1:" + std::to_string(port);
    EXPECT_EQ(EndpointTracker::normalize_endpoint_identity("[::1]", port), key);
    EXPECT_FALSE(EndpointTracker::normalize_endpoint_identity("", port));
    EXPECT_FALSE(EndpointTracker::normalize_endpoint_identity("::1", 0));
    EXPECT_FALSE(EndpointTracker::normalize_endpoint_identity("[[::1]]", port));
    EXPECT_FALSE(EndpointTracker::normalize_endpoint_identity("::g", port));
    EXPECT_FALSE(
        EndpointTracker::normalize_endpoint_identity("bad/host", port));
    EXPECT_FALSE(
        EndpointTracker::normalize_endpoint_identity("::ffff:not-an-ip", port));
    EXPECT_FALSE(EndpointTracker::normalize_endpoint_identity(
        "[history.example.com]", port));
    EXPECT_FALSE(EndpointTracker::normalize_endpoint_identity(
        "[history.example.com]:" + std::to_string(port)));
    EXPECT_EQ(
        EndpointTracker::normalize_endpoint_identity(
            "[::ffff:192.0.2.10]", port),
        "192.0.2.10:" + std::to_string(port));
    EXPECT_FALSE(
        EndpointTracker::normalize_endpoint_identity("bad..host", port));
    EXPECT_FALSE(
        EndpointTracker::normalize_endpoint_identity("-bad.example", port));
    EXPECT_FALSE(
        EndpointTracker::normalize_endpoint_identity("bad-.example", port));
    EXPECT_FALSE(
        EndpointTracker::normalize_endpoint_identity("bad_host", port));
    EXPECT_EQ(
        EndpointTracker::normalize_endpoint_identity(
            "History.Example.COM.", port),
        "history.example.com:" + std::to_string(port));

    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .endpoint_cache_path = ":memory:",
            .max_in_flight_connects = 1,
            .max_in_flight_crawls = 0,
        });
    peers->start_connect("::1", port);

    std::shared_ptr<PeerClient> client;
    for (std::size_t attempt = 0; attempt < 100 && !client; ++attempt)
    {
        io.run_for(1ms);
        io.restart();
        client = PeerSetTestAccess::connect_servant_client(io, peers, key);
    }
    ASSERT_TRUE(client)
        << "the bracketed queue/census identity must survive host/port "
           "reconstruction";
    EXPECT_FALSE(PeerSetTestAccess::in_flight_contains(io, peers, split_key));

    peers->start_connect("[::1]", port);
    peers->start_connect("", port);
    peers->start_connect("::1", 0);
    peers->start_connect("[[::1]]", port);
    peers->start_connect("::g", port);
    peers->start_connect("bad/host", port);
    PeerSetTestAccess::queue_connect(
        io, peers, "[::g]:" + std::to_string(port));
    PeerSetTestAccess::queue_connect(
        io, peers, "bad/host:" + std::to_string(port));
    io.run_for(10ms);
    io.restart();
    EXPECT_EQ(PeerSetTestAccess::snapshot(io, peers).queued_connects, 0u)
        << "equivalent bracket syntax deduplicates while invalid identities "
           "never enter the queue";

    asio::post(client->strand(), [client] {
        PeerClientTestAccess::fail_initial_connect_on_strand(*client);
    });
    io.run_for(1s);
    io.restart();

    EXPECT_TRUE(PeerSetTestAccess::has_failed(io, peers, key));
    EXPECT_FALSE(PeerSetTestAccess::has_failed(io, peers, split_key));
    EXPECT_EQ(PeerSetTestAccess::endpoint_failure_count(io, peers, key), 1u);
    EXPECT_EQ(
        PeerSetTestAccess::cached_endpoint_failure_count(io, peers, key), 1u);
    EXPECT_FALSE(PeerSetTestAccess::in_flight_contains(io, peers, key));
}

TEST(PeerSetShutdown, OwnerStopWinsInPostReadyPreAdmissionWindow)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto acceptor = std::make_shared<asio::ip::tcp::acceptor>(
        io, asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
    auto accepted = std::make_shared<asio::ip::tcp::socket>(io);
    acceptor->async_accept(*accepted, [accepted](boost::system::error_code) {});
    auto const key =
        "127.0.0.1:" + std::to_string(acceptor->local_endpoint().port());

    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .endpoint_cache_path = ":memory:",
            .max_in_flight_connects = 1,
            .max_in_flight_crawls = 0,
        });
    auto admission_gate = std::make_shared<asio::steady_timer>(
        io, asio::steady_timer::time_point::max());
    std::atomic<bool> gap_reached{false};
    PeerSetTestAccess::set_after_connect_ready_before_admission(
        peers,
        [admission_gate, &gap_reached](
            std::shared_ptr<PeerClient> const&) -> asio::awaitable<void> {
            gap_reached.store(true, std::memory_order_release);
            boost::system::error_code ec;
            co_await admission_gate->async_wait(
                asio::redirect_error(asio::use_awaitable, ec));
        });
    PeerSetTestAccess::push_pending_connect(io, peers, key);
    PeerSetTestAccess::pump_connects(io, peers);

    std::shared_ptr<PeerClient> client;
    for (std::size_t attempt = 0; attempt < 100; ++attempt)
    {
        io.run_for(1ms);
        io.restart();
        client = PeerSetTestAccess::connect_servant_client(io, peers, key);
        if (client)
            break;
    }
    ASSERT_TRUE(client);
    ASSERT_NE(client->state(), State::Disconnected);

    asio::post(client->strand(), [client] {
        PeerClientTestAccess::become_ready_on_strand(*client);
    });
    for (std::size_t attempt = 0;
         attempt < 100 && !gap_reached.load(std::memory_order_acquire);
         ++attempt)
    {
        io.run_for(1ms);
        io.restart();
    }
    ASSERT_TRUE(gap_reached.load(std::memory_order_acquire));
    ASSERT_TRUE(PeerSetTestAccess::in_flight_contains(io, peers, key));

    auto stopped = asio::co_spawn(io, peers->co_stop(), asio::use_future);
    for (std::size_t attempt = 0;
         attempt < 100 && !PeerSetTestAccess::shutdown_ingress_sealed(peers);
         ++attempt)
    {
        io.run_for(1ms);
        io.restart();
    }
    ASSERT_TRUE(PeerSetTestAccess::shutdown_ingress_sealed(peers));
    EXPECT_EQ(
        client->terminal_cause(), PeerLogicalTerminalCause::owner_disconnect);
    EXPECT_EQ(stopped.wait_for(0ms), std::future_status::timeout);
    EXPECT_TRUE(PeerSetTestAccess::in_flight_contains(io, peers, key));

    admission_gate->cancel();
    io.run_for(1s);
    io.restart();

    ASSERT_EQ(stopped.wait_for(0ms), std::future_status::ready);
    EXPECT_NO_THROW(stopped.get());
    EXPECT_FALSE(PeerSetTestAccess::has_failed(io, peers, key));
    EXPECT_EQ(PeerSetTestAccess::endpoint_failure_count(io, peers, key), 0u);
    EXPECT_EQ(
        PeerSetTestAccess::cached_endpoint_failure_count(io, peers, key), 0u);
    EXPECT_FALSE(PeerSetTestAccess::in_flight_contains(io, peers, key));
}

TEST(PeerSetShutdown, PeerFailureCauseSurvivesOwnerStopBeforeServantCatch)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto acceptor = std::make_shared<asio::ip::tcp::acceptor>(
        io, asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
    auto accepted = std::make_shared<asio::ip::tcp::socket>(io);
    acceptor->async_accept(*accepted, [accepted](boost::system::error_code) {});
    auto const key =
        "127.0.0.1:" + std::to_string(acceptor->local_endpoint().port());

    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .endpoint_cache_path = ":memory:",
            .max_in_flight_connects = 1,
            .max_in_flight_crawls = 0,
        });
    PeerSetTestAccess::push_pending_connect(io, peers, key);
    PeerSetTestAccess::pump_connects(io, peers);

    std::shared_ptr<PeerClient> client;
    for (std::size_t attempt = 0; attempt < 100 && !client; ++attempt)
    {
        io.run_for(1ms);
        io.restart();
        client = PeerSetTestAccess::connect_servant_client(io, peers, key);
    }
    ASSERT_TRUE(client);
    ASSERT_TRUE(PeerClientTestAccess::hold_initial_connect_terminal_publication(
        *client));

    asio::post(client->strand(), [client] {
        PeerClientTestAccess::fail_initial_connect_on_strand(*client);
    });
    io.run_for(100ms);
    io.restart();

    auto const terminal =
        PeerClientTestAccess::initial_connect_terminal(*client);
    ASSERT_TRUE(terminal);
    EXPECT_EQ(
        terminal->kind,
        catl::peer_client::detail::ConnectAttemptTerminalKind::peer_failure);
    EXPECT_EQ(client->terminal_cause(), PeerLogicalTerminalCause::peer_failure);

    auto stopped = asio::co_spawn(io, peers->co_stop(), asio::use_future);
    io.run_for(100ms);
    io.restart();

    EXPECT_TRUE(PeerSetTestAccess::shutdown_ingress_sealed(peers));
    EXPECT_TRUE(PeerSetTestAccess::connect_servant_stop_cancellation_requested(
        io, peers, key));
    EXPECT_EQ(client->state(), State::Disconnected)
        << "the public stop path must project the stale mutable state before "
           "the retained peer-failure result reaches classification";
    EXPECT_TRUE(PeerSetTestAccess::in_flight_contains(io, peers, key));
    EXPECT_EQ(stopped.wait_for(0ms), std::future_status::timeout);

    ASSERT_TRUE(
        PeerClientTestAccess::release_initial_connect_terminal_publication(
            *client));
    io.run_for(1s);
    io.restart();

    ASSERT_EQ(stopped.wait_for(0ms), std::future_status::ready);
    EXPECT_NO_THROW(stopped.get());
    EXPECT_TRUE(PeerSetTestAccess::has_failed(io, peers, key))
        << "the public stop seal and request bit cannot suppress an earlier "
           "peer failure";
    EXPECT_EQ(PeerSetTestAccess::endpoint_failure_count(io, peers, key), 1u);
    EXPECT_EQ(
        PeerSetTestAccess::cached_endpoint_failure_count(io, peers, key), 1u);
    EXPECT_FALSE(PeerSetTestAccess::in_flight_contains(io, peers, key));
}

TEST(PeerSetShutdown, OwnerDisconnectWinsBeforeConnectDeadline)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto acceptor = std::make_shared<asio::ip::tcp::acceptor>(
        io, asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
    auto accepted = std::make_shared<asio::ip::tcp::socket>(io);
    acceptor->async_accept(*accepted, [accepted](boost::system::error_code) {});
    auto const key =
        "127.0.0.1:" + std::to_string(acceptor->local_endpoint().port());

    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .endpoint_cache_path = ":memory:",
            .max_in_flight_connects = 1,
            .max_in_flight_crawls = 0,
        });
    PeerSetTestAccess::push_pending_connect(io, peers, key);
    PeerSetTestAccess::pump_connects(io, peers);

    std::shared_ptr<PeerClient> client;
    for (std::size_t attempt = 0; attempt < 100 && !client; ++attempt)
    {
        io.run_for(1ms);
        io.restart();
        client = PeerSetTestAccess::connect_servant_client(io, peers, key);
    }
    ASSERT_TRUE(client);
    ASSERT_TRUE(PeerClientTestAccess::hold_initial_connect_terminal_publication(
        *client));

    auto stopped = asio::co_spawn(io, peers->co_stop(), asio::use_future);
    io.run_for(100ms);
    io.restart();

    ASSERT_TRUE(PeerSetTestAccess::shutdown_ingress_sealed(peers));
    ASSERT_TRUE(PeerSetTestAccess::connect_servant_stop_cancellation_requested(
        io, peers, key));
    ASSERT_EQ(
        client->terminal_cause(), PeerLogicalTerminalCause::owner_disconnect);
    auto const owner_terminal =
        PeerClientTestAccess::initial_connect_terminal(*client);
    ASSERT_TRUE(owner_terminal);
    EXPECT_EQ(
        owner_terminal->kind,
        catl::peer_client::detail::ConnectAttemptTerminalKind::
            owner_disconnect);
    EXPECT_FALSE(
        PeerClientTestAccess::commit_initial_connect_deadline(*client, 10)
            .terminal_committed)
        << "a later deadline cannot overtake the owner terminal commit";

    ASSERT_TRUE(
        PeerClientTestAccess::release_initial_connect_terminal_publication(
            *client));
    io.run_for(1s);
    io.restart();

    ASSERT_EQ(stopped.wait_for(0ms), std::future_status::ready);
    EXPECT_NO_THROW(stopped.get());
    EXPECT_FALSE(PeerSetTestAccess::has_failed(io, peers, key));
    EXPECT_EQ(PeerSetTestAccess::endpoint_failure_count(io, peers, key), 0u);
    EXPECT_EQ(
        PeerSetTestAccess::cached_endpoint_failure_count(io, peers, key), 0u);
    EXPECT_FALSE(PeerSetTestAccess::in_flight_contains(io, peers, key));
}

TEST(PeerSetShutdown, ConnectDeadlineWinsBeforeOwnerDisconnect)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto acceptor = std::make_shared<asio::ip::tcp::acceptor>(
        io, asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
    auto accepted = std::make_shared<asio::ip::tcp::socket>(io);
    acceptor->async_accept(*accepted, [accepted](boost::system::error_code) {});
    auto const key =
        "127.0.0.1:" + std::to_string(acceptor->local_endpoint().port());

    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .endpoint_cache_path = ":memory:",
            .max_in_flight_connects = 1,
            .max_in_flight_crawls = 0,
        });
    PeerSetTestAccess::push_pending_connect(io, peers, key);
    PeerSetTestAccess::pump_connects(io, peers);

    std::shared_ptr<PeerClient> client;
    for (std::size_t attempt = 0; attempt < 100 && !client; ++attempt)
    {
        io.run_for(1ms);
        io.restart();
        client = PeerSetTestAccess::connect_servant_client(io, peers, key);
    }
    ASSERT_TRUE(client);
    ASSERT_TRUE(PeerClientTestAccess::hold_initial_connect_terminal_publication(
        *client));
    ASSERT_TRUE(
        PeerClientTestAccess::commit_initial_connect_deadline(*client, 10)
            .terminal_committed);

    auto stopped = asio::co_spawn(io, peers->co_stop(), asio::use_future);
    io.run_for(100ms);
    io.restart();

    ASSERT_TRUE(PeerSetTestAccess::shutdown_ingress_sealed(peers));
    ASSERT_TRUE(PeerSetTestAccess::connect_servant_stop_cancellation_requested(
        io, peers, key));
    EXPECT_EQ(
        client->terminal_cause(), PeerLogicalTerminalCause::owner_disconnect);
    auto const deadline_terminal =
        PeerClientTestAccess::initial_connect_terminal(*client);
    ASSERT_TRUE(deadline_terminal);
    EXPECT_EQ(
        deadline_terminal->kind,
        catl::peer_client::detail::ConnectAttemptTerminalKind::deadline)
        << "owner cleanup cannot rewrite an earlier deadline";
    EXPECT_EQ(stopped.wait_for(0ms), std::future_status::timeout);

    ASSERT_TRUE(
        PeerClientTestAccess::release_initial_connect_terminal_publication(
            *client));
    io.run_for(1s);
    io.restart();

    ASSERT_EQ(stopped.wait_for(0ms), std::future_status::ready);
    EXPECT_NO_THROW(stopped.get());
    EXPECT_TRUE(PeerSetTestAccess::has_failed(io, peers, key));
    EXPECT_EQ(PeerSetTestAccess::endpoint_failure_count(io, peers, key), 1u);
    EXPECT_EQ(
        PeerSetTestAccess::cached_endpoint_failure_count(io, peers, key), 1u);
    EXPECT_FALSE(PeerSetTestAccess::in_flight_contains(io, peers, key));
}

TEST(
    PeerSetShutdown,
    PumpConnectFailurePathJoinsInstalledPhysicalReceiptBeforeRetirement)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto acceptor = std::make_shared<asio::ip::tcp::acceptor>(
        io, asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
    auto accepted = std::make_shared<asio::ip::tcp::socket>(io);
    acceptor->async_accept(*accepted, [accepted](boost::system::error_code) {});
    auto const key =
        "127.0.0.1:" + std::to_string(acceptor->local_endpoint().port());

    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .endpoint_cache_path = ":memory:",
            .max_in_flight_connects = 1,
            .max_in_flight_crawls = 0,
        });
    bool classification_reached = false;
    std::size_t servant_receipts = 0;
    PeerSetTestAccess::set_connect_failure_classification_hook(
        peers, [&classification_reached] {
            classification_reached = true;
            throw std::runtime_error("escaped connect classification");
        });
    PeerSetTestAccess::set_connect_completion_diagnostic_hook(
        peers, [&servant_receipts] { ++servant_receipts; });
    PeerSetTestAccess::push_pending_connect(io, peers, key);
    PeerSetTestAccess::pump_connects(io, peers);

    std::shared_ptr<PeerClient> client;
    for (std::size_t attempt = 0; attempt < 100 && !client; ++attempt)
    {
        io.run_for(1ms);
        io.restart();
        client = PeerSetTestAccess::connect_servant_client(io, peers, key);
    }
    ASSERT_TRUE(client);

    peer_connection::test_write_completion held_completion;
    asio::post(client->strand(), [client, &held_completion] {
        client->raw_connection().set_write_initiator_for_test(
            [&held_completion](
                asio::const_buffer,
                peer_connection::test_write_completion completion) {
                held_completion = std::move(completion);
            });
        client->raw_connection().async_send_packet(
            packet_type::ping,
            std::vector<std::uint8_t>(64, 0xC8),
            [](boost::system::error_code) {});
        PeerClientTestAccess::fail_initial_connect_on_strand(*client);
    });
    for (std::size_t attempt = 0;
         attempt < 100 && (!held_completion || !classification_reached);
         ++attempt)
    {
        io.run_for(1ms);
        io.restart();
    }

    ASSERT_TRUE(held_completion);
    ASSERT_TRUE(classification_reached);
    EXPECT_TRUE(PeerSetTestAccess::in_flight_contains(io, peers, key))
        << "the real pump catch must retain S0037 until T0183 joins";
    EXPECT_GE(
        client->raw_connection().transport_operation_count_for_test(), 1u);
    EXPECT_FALSE(
        client->raw_connection().physical_transport_drained_for_test());
    EXPECT_EQ(servant_receipts, 0u);

    auto completion = std::move(held_completion);
    asio::post(
        client->strand(), [completion = std::move(completion)]() mutable {
            completion(asio::error::operation_aborted);
        });
    io.run_for(1s);
    io.restart();

    EXPECT_TRUE(client->raw_connection().physical_transport_drained_for_test());
    EXPECT_FALSE(PeerSetTestAccess::in_flight_contains(io, peers, key));
    EXPECT_EQ(servant_receipts, 1u)
        << "the production servant funnel retires the exact row once";
}

TEST(
    PeerSetShutdownDeterministic,
    ReturnedReceiptCannotEraseDifferentSessionAtSameGeneration)
{
    catl::test::DeterministicScheduler scheduler;
    auto& io = scheduler.io();
    auto peers = PeerSet::create(io, PeerSetOptions{.network_id = 42});

    auto incumbent = std::make_shared<FakePeerSession>(
        io, "guarded-shutdown-peer:1", true, 500, 400, 600);
    auto replacement = std::make_shared<FakePeerSession>(
        io, "guarded-shutdown-peer:1", true, 500, 400, 600);
    incumbent->hold_disconnect_receipt();
    replacement->hold_disconnect_receipt();
    auto const generation =
        PeerSetTestAccess::insert_ready_peer_with_generation(
            io, peers, incumbent);

    bool stopped = false;
    std::exception_ptr stop_failure;
    asio::co_spawn(io, peers->co_stop(), [&](std::exception_ptr failure) {
        stop_failure = std::move(failure);
        stopped = true;
    });
    scheduler.run_until_idle();

    ASSERT_TRUE(
        incumbent->co_disconnect_waiting.load(std::memory_order_acquire));
    EXPECT_FALSE(stopped);

    // Production admission is sealed here. This adversarial overwrite exists
    // only to make the exact post-await session predicate falsifiable.
    PeerSetTestAccess::overwrite_connection_row_for_guard_control(
        io, peers, replacement, generation);
    incumbent->release_disconnect_receipt();
    scheduler.run_until_idle();

    EXPECT_FALSE(stopped)
        << "the incumbent receipt cannot retire a different exact session";
    EXPECT_TRUE(
        replacement->co_disconnect_waiting.load(std::memory_order_acquire));
    EXPECT_EQ(
        PeerSetTestAccess::live_generation(io, peers, replacement->endpoint()),
        generation);

    replacement->release_disconnect_receipt();
    scheduler.run_until_idle();

    EXPECT_TRUE(stopped);
    EXPECT_EQ(stop_failure, nullptr);
    EXPECT_EQ(
        PeerSetTestAccess::live_generation(io, peers, replacement->endpoint()),
        0u);
}

TEST(
    PeerSetShutdownDeterministic,
    ReturnedReceiptCannotEraseDifferentGenerationOfSameSession)
{
    catl::test::DeterministicScheduler scheduler;
    auto& io = scheduler.io();
    auto peers = PeerSet::create(io, PeerSetOptions{.network_id = 42});

    auto session = std::make_shared<FakePeerSession>(
        io, "generation-guarded-shutdown-peer:1", true, 500, 400, 600);
    session->hold_disconnect_receipt();
    session->hold_disconnect_receipt();
    auto const generation =
        PeerSetTestAccess::insert_ready_peer_with_generation(
            io, peers, session);

    bool stopped = false;
    std::exception_ptr stop_failure;
    asio::co_spawn(io, peers->co_stop(), [&](std::exception_ptr failure) {
        stop_failure = std::move(failure);
        stopped = true;
    });
    scheduler.run_until_idle();

    ASSERT_TRUE(session->co_disconnect_waiting.load(std::memory_order_acquire));
    ASSERT_EQ(session->co_disconnect_calls.load(), 1u);

    // The same object under a different generation makes the generation guard,
    // rather than the exact-session guard, solely responsible for refusing the
    // first returned receipt.
    PeerSetTestAccess::overwrite_connection_row_for_guard_control(
        io, peers, session, generation + 1);
    session->release_disconnect_receipt(0);
    scheduler.run_until_idle();

    EXPECT_FALSE(stopped)
        << "the predecessor receipt cannot retire a successor generation";
    EXPECT_TRUE(session->co_disconnect_waiting.load(std::memory_order_acquire));
    EXPECT_EQ(session->co_disconnect_calls.load(), 2u);
    EXPECT_EQ(
        PeerSetTestAccess::live_generation(io, peers, session->endpoint()),
        generation + 1);

    session->release_disconnect_receipt(1);
    scheduler.run_until_idle();

    EXPECT_TRUE(stopped);
    EXPECT_EQ(stop_failure, nullptr);
    EXPECT_EQ(
        PeerSetTestAccess::live_generation(io, peers, session->endpoint()), 0u);
}

TEST(PeerSetShutdown, CallerCancellationCannotAbandonPhysicalReceipt)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{.network_id = 42});
    auto session = std::make_shared<FakePeerSession>(
        io, "cancelled-shutdown-peer:1", true, 500, 400, 600);
    session->hold_disconnect_receipt();
    PeerSetTestAccess::insert_ready_peer_with_generation(io, peers, session);

    asio::cancellation_signal cancellation;
    std::promise<std::exception_ptr> done;
    auto done_future = done.get_future();
    asio::co_spawn(
        io,
        peers->co_stop(),
        asio::bind_cancellation_slot(
            cancellation.slot(), [&done](std::exception_ptr failure) {
                done.set_value(std::move(failure));
            }));
    io.run_for(10ms);

    ASSERT_TRUE(session->co_disconnect_waiting.load(std::memory_order_acquire));
    cancellation.emit(asio::cancellation_type::all);
    io.restart();
    io.poll();

    EXPECT_EQ(done_future.wait_for(0ms), std::future_status::timeout)
        << "caller cancellation must not abandon the only stop driver";

    session->release_disconnect_receipt();
    io.restart();
    io.run();

    ASSERT_EQ(done_future.wait_for(0ms), std::future_status::ready);
    EXPECT_EQ(done_future.get(), nullptr);
    io.restart();
    EXPECT_EQ(
        PeerSetTestAccess::live_generation(io, peers, session->endpoint()), 0u);
}

TEST(PeerSetShutdown, GenerationPinDelaysPhysicalClose)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{.network_id = 42});
    auto session = std::make_shared<FakePeerSession>(
        io, "pinned-shutdown-peer:1", true, 500, 400, 600);
    PeerSetTestAccess::insert_ready_peer_with_generation(io, peers, session);
    auto pin = PeerSetTestAccess::make_peer_pin(io, peers, session->endpoint());
    ASSERT_TRUE(pin);

    auto stopped = asio::co_spawn(io, peers->co_stop(), asio::use_future);
    io.run_for(10ms);

    EXPECT_EQ(stopped.wait_for(0ms), std::future_status::timeout);
    EXPECT_EQ(session->co_disconnect_calls.load(), 0u)
        << "a predecessor-owned pin is an eviction veto";

    pin = {};
    io.restart();
    io.run();
    EXPECT_NO_THROW(stopped.get());
    EXPECT_EQ(session->co_disconnect_calls.load(), 1u);
}

TEST(PeerSetShutdown, ExpiredPinWithoutReleasePostCannotWedgeStop)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{.network_id = 42});
    auto handle = PeerSetTestAccess::insert_unnotified_peer_pin(
        io, peers, "unnotified-pin:1", 1);
    ASSERT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 1u);

    auto stopped = asio::co_spawn(io, peers->co_stop(), asio::use_future);
    io.run_for(5ms);
    EXPECT_EQ(stopped.wait_for(0ms), std::future_status::timeout);

    // Deliberately expire the weak authority without posting the normal
    // release callback. The finite re-prune is the recovery path under test.
    handle.reset();
    io.restart();
    io.run_for(250ms);

    ASSERT_EQ(stopped.wait_for(0ms), std::future_status::ready);
    EXPECT_NO_THROW(stopped.get());
    io.restart();
    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(io, peers), 0u);
}

TEST(PeerSetShutdown, ConnectBodyFailureReceiptsBeforeThrowingDiagnostic)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{.network_id = 42});

    auto const result =
        PeerSetTestAccess::complete_connect_servant_after_body_failure(
            io, peers, "connect-receipt.test:51235");

    EXPECT_TRUE(result.throwing_diagnostic_ran);
    EXPECT_TRUE(result.exact_row_consumed);
    EXPECT_TRUE(result.stop_waiter_woke);
}

TEST(
    PeerSetShutdown,
    EscapedConnectBodyFailureJoinsInstalledPhysicalReceiptBeforeRetirement)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{.network_id = 42});
    auto session = std::make_shared<FakePeerSession>(
        io, "connect-body-failure:51235", false, 0, 0, 0);
    session->hold_disconnect_receipt();

    std::size_t servant_receipts = 0;
    PeerSetTestAccess::set_connect_completion_diagnostic_hook(
        peers, [&servant_receipts] { ++servant_receipts; });
    auto finished =
        PeerSetTestAccess::finish_installed_connect_servant_after_body_failure(
            io, peers, session->endpoint(), session);

    io.run_for(20ms);
    io.restart();

    EXPECT_TRUE(session->co_disconnect_waiting.load(std::memory_order_acquire));
    EXPECT_TRUE(
        PeerSetTestAccess::in_flight_contains(io, peers, session->endpoint()))
        << "an escaped body failure cannot consume S0037 before physical drain";
    EXPECT_EQ(finished.wait_for(0ms), std::future_status::timeout);
    EXPECT_EQ(servant_receipts, 0u);

    session->release_disconnect_receipt();
    io.restart();
    io.run_for(1s);

    ASSERT_EQ(finished.wait_for(0ms), std::future_status::ready);
    EXPECT_NO_THROW(finished.get());
    io.restart();
    EXPECT_FALSE(
        PeerSetTestAccess::in_flight_contains(io, peers, session->endpoint()));
    EXPECT_EQ(servant_receipts, 1u);
}

TEST(PeerSetShutdown, CrawlBodyFailureReceiptsBeforeThrowingDiagnostic)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{.network_id = 42});

    auto const result =
        PeerSetTestAccess::complete_crawl_servant_after_body_failure(
            io, peers, "crawl-receipt.test:51235");

    EXPECT_TRUE(result.throwing_diagnostic_ran);
    EXPECT_TRUE(result.exact_row_consumed);
    EXPECT_TRUE(result.stop_waiter_woke);
}

TEST(PeerSetShutdown, ConnectLaunchFailureConsumesCommittedCensusRow)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{.network_id = 42});

    auto const result = PeerSetTestAccess::fail_connect_servant_launch(
        io, peers, "127.0.0.1:51235");

    EXPECT_TRUE(result.exact_row_consumed);
    EXPECT_TRUE(result.stop_waiter_woke);
}

TEST(PeerSetShutdown, CrawlLaunchFailureConsumesCommittedCensusRow)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{.network_id = 42});

    auto const result = PeerSetTestAccess::fail_crawl_servant_launch(
        io, peers, "127.0.0.1:51235");

    EXPECT_TRUE(result.exact_row_consumed);
    EXPECT_TRUE(result.stop_waiter_woke);
}

//@@start crawl-completion-setup-failure
TEST(PeerSetShutdown, CrawlCompletionSetupFailureConsumesCommittedCensusRow)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    asio::ip::tcp::acceptor unavailable(
        io, asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
    auto const key =
        "127.0.0.1:" + std::to_string(unavailable.local_endpoint().port());
    boost::system::error_code ignored;
    unavailable.close(ignored);

    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .endpoint_cache_path = ":memory:",
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 1,
        });
    std::size_t servant_receipts = 0;
    PeerSetTestAccess::set_crawl_completion_diagnostic_hook(
        io, peers, [&servant_receipts] { ++servant_receipts; });
    PeerSetTestAccess::fail_next_crawl_completion_setup(io, peers);
    PeerSetTestAccess::queue_crawl(io, peers, key);
    PeerSetTestAccess::pump_crawls(io, peers);

    auto stopped = asio::co_spawn(io, peers->co_stop(), asio::use_future);
    io.run_for(1s);

    ASSERT_EQ(stopped.wait_for(0ms), std::future_status::ready)
        << "completion-token setup refusal must directly consume the exact "
           "admitted crawl row";
    EXPECT_NO_THROW(stopped.get());
    io.restart();
    EXPECT_FALSE(PeerSetTestAccess::crawl_in_flight_contains(io, peers, key));
    EXPECT_EQ(servant_receipts, 1u);
}
//@@end crawl-completion-setup-failure
