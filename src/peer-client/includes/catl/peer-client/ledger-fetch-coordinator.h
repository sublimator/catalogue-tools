#pragma once

#include "node-transport.h"
#include "peer-generation-pin.h"

#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

class LedgerFetchCoordinatorTestAccess;  // forward decl for test friend

namespace catl::peer_client {

class PeerSet;

/**
 * Coordinator-owned sender half of the NodeBatch protocol — awaiting-bind slice.
 *
 * This actor exists because production has no executor able to own a replayable
 * sender row. `NodeCache` serialises with a mutex plus a header-state strand, and
 * a mutex cannot own an asynchronous row lifecycle. `PeerClient` owns the
 * RECEIVER side, so sender authority must not be minted there.
 *
 * Scope of THIS slice: bounded ingress reservation, coordinator-strand row
 * commit, exact-pin seal through PeerSet, and a bounded bind retry. It
 * deliberately stops before publication. The point is to close the one liveness
 * gap PeerSet refused to own: when a seal projection fails, or its completion is
 * lost, somebody must retry, and that somebody is here. Nothing in this class
 * claims node arrival, retry selection across peers, proof membership, C09
 * membership, or generation closure.
 *
 * ## The fence is LOGICAL, and is not quiescence
 *
 * Read this before using `fence()` for anything that needs a drain.
 *
 * `fence()` seals ingress and terminalizes every row. It does NOT wait for
 * outstanding seal projections, and therefore does not wait for the pin shares
 * those projections carry. That is deliberate: PeerSet does not promise one
 * completion per accepted projection — a lost completion post is the exact
 * failure this actor recovers from — so a fence that waited on one would inherit
 * the liveness bug it exists to fix.
 *
 * The consequence is stated rather than implied: after `Lifecycle::fenced`,
 * `outstanding_bind_projections()` may be nonzero, and each one may still deliver
 * a completion carrying a pin share. **A fenced coordinator must not be treated as
 * quiescent, and `fenced` may not satisfy a cutover or shutdown-quiescence
 * requirement.** Proving quiescence needs an owned receipt/cancellation path
 * against PeerSet, which this slice does not have. The counter is public
 * precisely so the missing work is countable instead of assumed away.
 *
 * ## Design points earlier rejected versions got wrong
 *
 *  * A synchronous return claims ONLY that a projection was queued. It never
 *    claims an attempt ID, a row, or a completed drain.
 *  * ONE conserved projection covers granted ingress, committed rows, and the
 *    ingress seal, in a single atomic word. Two counters could not be read
 *    consistently across the permit-to-row transfer, and a separate `fencing`
 *    flag left a window where a caller admitted before the seal could still take
 *    a permit after the fence had been reported.
 *  * Every row keeps its ORIGINAL intent and ORIGINAL pin for life; each bind
 *    hands PeerSet a fresh `share()`.
 *  * Callbacks re-find their row by `(attempt, bind_revision)` and never capture
 *    a row reference. An event must match the open revision AND find the bind
 *    open, so a retry cannot be completed by the previous bind's callback.
 *  * Every accepted permit either commits a row or is released. All fallible
 *    preparation — intent construction, timer allocation, timer arming, bind
 *    initiation — is contained, because an escaping exception on the strand would
 *    both leak the permit and take the handler out with it.
 *  * Retry is a DIRECT owner-strand call, not a post. A posted retry can fail to
 *    queue and orphan a row in `awaiting_bind` with nothing in flight.
 *
 * Back-pressure, stated because it looks like a leak and is not: a `publishable`
 * row holds its work unit until publication consumes it, and publication is a
 * later slice. So after `kMaxWork` successful binds this class refuses ingress.
 * That is an unconsumed outbox applying back-pressure, and a control asserts it.
 */
class LedgerFetchCoordinator
    : public std::enable_shared_from_this<LedgerFetchCoordinator>
{
public:
    /// Hard cap on live work: granted-but-uncommitted ingress plus committed
    /// rows, as ONE budget. Row storage is reserved to this exactly once.
    static constexpr std::size_t kMaxWork = 64;

    /// Bounded fence waiters. Beyond this a caller is refused with a disposition
    /// rather than silently dropped.
    static constexpr std::size_t kMaxFenceWaiters = 8;

    /// Bind bounds. Retry is limited by an attempt budget AND an absolute
    /// deadline; exhausting either terminalizes identically.
    static constexpr std::uint32_t kMaxBindAttempts = 3;
    static constexpr std::chrono::milliseconds kBindDeadline{250};
    static constexpr std::chrono::milliseconds kAbsoluteBindDeadline{2000};

    /// Why an ingress reservation was refused. This is the ONLY thing a caller
    /// learns synchronously — never an attempt ID, never a row.
    enum class Ingress : std::uint8_t {
        reserved,
        ingress_sealed,
        work_budget_exhausted,
        node_count_invalid,
        unpinned_peer,
        projection_failed,
    };

    /// What `fence()` learned synchronously. Same discipline as `Ingress`: it can
    /// only report whether the projection was queued, because everything else is
    /// the strand's to decide.
    enum class Fence : std::uint8_t {
        drain_queued,
        /// The drain could not be queued. Ingress STAYS sealed — un-sealing would
        /// resurrect an actor whose callers have already been refused — so this is
        /// reported instead of hidden, and `fence()` is idempotent: call it again.
        projection_failed,
    };

    /// Exactly-once disposition handed to every fence waiter. A waiter that
    /// cannot be registered still learns why, rather than waiting forever.
    enum class FenceOutcome : std::uint8_t {
        fenced,
        waiter_capacity_exhausted,
    };

    using FenceHandler = std::function<void(FenceOutcome)>;

    /// Where one transport attempt's bind has got to.
    enum class Phase : std::uint8_t {
        awaiting_bind,  ///< committed; a bind is open or about to be opened
        publishable,    ///< sealed command held; publication is a later slice
        terminal,       ///< bind bounds exhausted, refused, or fenced
    };

    enum class Lifecycle : std::uint8_t { running, fencing, fenced };

    /// Returns nullptr when no sender generation can be minted. A coordinator
    /// without a distinct generation could alias rows retained by an earlier
    /// incarnation, so it must not exist at all.
    static std::shared_ptr<LedgerFetchCoordinator>
    create(boost::asio::io_context& io, std::shared_ptr<PeerSet> peers);

    LedgerFetchCoordinator(LedgerFetchCoordinator const&) = delete;
    LedgerFetchCoordinator&
    operator=(LedgerFetchCoordinator const&) = delete;

    /// Compatibility ingress: submit against an ALREADY selected physical pin.
    ///
    /// The name is deliberate. Choosing the peer is not yet this actor's
    /// authority, and a caller that supplies the pin acquires no retry or
    /// transport authority by doing so.
    ///
    /// `Ingress::reserved` means a work permit was taken and a row commit was
    /// queued onto the coordinator strand. It does NOT mean a row exists.
    [[nodiscard]] Ingress
    submit_node_batch_on_pin(
        Hash256 ledger_hash,
        NodeTree tree,
        std::vector<SHAMapNodeID> node_ids,
        PeerGenerationPin pin);

    /// Seal ingress and terminalize every row. A LOGICAL fence — see the class
    /// comment; this is not quiescence and must not be used as if it were.
    ///
    /// `on_fenced` is invoked exactly once with a `FenceOutcome`, on the
    /// coordinator strand, through the one callback funnel. Multiple callers each
    /// get their own disposition; none overwrites another.
    [[nodiscard]] Fence
    fence(FenceHandler on_fenced = FenceHandler{});

    /// Reported, not authoritative: the seal bit inside `work_` is what actually
    /// gates ingress, so there is one authority rather than two.
    Lifecycle
    lifecycle() const noexcept
    {
        if (fenced_.load(std::memory_order_acquire))
            return Lifecycle::fenced;
        return ingress_sealed() ? Lifecycle::fencing : Lifecycle::running;
    }

    bool
    ingress_sealed() const noexcept
    {
        return (work_.load(std::memory_order_acquire) & kIngressSealedBit) != 0;
    }

    /// The conserved projection: granted ingress plus committed rows.
    std::size_t
    work_outstanding() const noexcept
    {
        return static_cast<std::size_t>(
            work_.load(std::memory_order_acquire) & kWorkMask);
    }

    /// Seal projections accepted by PeerSet whose completion has not been seen.
    ///
    /// This is the residue that makes `fence()` a logical fence rather than a
    /// drain. Nonzero after `Lifecycle::fenced` means completions — and the pin
    /// shares they carry — may still land. Public so the gap is countable.
    std::size_t
    outstanding_bind_projections() const noexcept
    {
        auto const projected = binds_projected_.load(std::memory_order_acquire);
        auto const completed = binds_completed_.load(std::memory_order_acquire);
        return projected >= completed ? projected - completed : 0;
    }

    SenderGenerationId
    sender_generation() const noexcept
    {
        return sender_generation_;
    }

private:
    friend class ::LedgerFetchCoordinatorTestAccess;

    /// Top bit of `work_` seals ingress. Sealing and reserving therefore contend
    /// on ONE word: a caller cannot pass an unsealed check and then reserve
    /// against a seal that has since landed.
    static constexpr std::uint64_t kIngressSealedBit = 1ull << 63;
    static constexpr std::uint64_t kWorkMask = kIngressSealedBit - 1;

    LedgerFetchCoordinator(
        boost::asio::io_context& io,
        std::shared_ptr<PeerSet> peers,
        SenderGenerationId sender_generation);

    /// Never reused, and once exhausted it STAYS exhausted. CAS below the
    /// ceiling, never fetch_add: an unconditional increment walks past the
    /// ceiling and wraps through zero, which would hand out the no-generation
    /// sentinel and then re-issue every value it had already issued.
    static std::optional<SenderGenerationId>
    allocate_sender_generation() noexcept;

    static std::atomic<std::uint64_t> next_sender_generation_;

    struct OutboxRow
    {
        OutboxRow(NodeBatchIntent intent_, PeerGenerationPin pin_) noexcept
            : intent(std::move(intent_)), pin(std::move(pin_))
        {
        }

        OutboxRow(OutboxRow const&) = delete;
        OutboxRow&
        operator=(OutboxRow const&) = delete;
        OutboxRow(OutboxRow&&) noexcept = default;
        OutboxRow&
        operator=(OutboxRow&&) noexcept = default;

        NodeBatchIntent intent;  ///< original, retained for life
        PeerGenerationPin pin;   ///< original; each bind gets a share()
        std::optional<NodeBatchCommand> sealed;
        Phase phase = Phase::awaiting_bind;

        /// Monotonic per bind. Together with `bind_open` this is the whole
        /// exactly-once mechanism: an event must match the current revision AND
        /// find the bind still open to commit anything.
        std::uint32_t bind_revision = 0;
        std::uint32_t bind_attempts = 0;
        bool bind_open = false;
        /// Snapshotted per row so the bound is a property of the row rather than
        /// of whatever the actor's setting happens to be when it is consulted.
        std::uint32_t max_bind_attempts = kMaxBindAttempts;

        std::shared_ptr<boost::asio::steady_timer> deadline;
        std::chrono::steady_clock::time_point absolute_deadline{};
        bool retired = false;
    };

    OutboxRow*
    find_row(TransportAttemptId attempt) noexcept;

    void
    commit_row_on_strand(
        Hash256 ledger_hash,
        NodeTree tree,
        std::vector<SHAMapNodeID> node_ids,
        PeerGenerationPin pin);

    void
    begin_bind_on_strand(TransportAttemptId attempt);

    /// Commit one bind outcome. Returns false when the event is stale — the row
    /// is gone, the revision has moved on, or the bind is already closed — in
    /// which case it must be, and is, inert.
    bool
    settle_bind_on_strand(
        TransportAttemptId attempt,
        std::uint32_t revision,
        std::optional<NodeBatchCommand> sealed,
        bool timed_out);

    void
    terminalize_on_strand(OutboxRow& row, char const* why);

    /// Is either bind bound spent? The single spelling of that question.
    static bool
    bind_bounds_spent(OutboxRow const& row) noexcept;

    /// Non-throwing timer cancellation. A throwing cancel inside a commit section
    /// would abandon the transition half-applied.
    static void
    cancel_quietly(OutboxRow& row) noexcept;

    void
    begin_fence_on_strand(FenceHandler on_fenced);

    void
    release_work() noexcept;

    void
    check_fenced_on_strand();

    /// The ONE place a caller-supplied fence disposition is delivered.
    void
    deliver_fence_outcome(FenceHandler const& handler, FenceOutcome outcome) noexcept;

    boost::asio::strand<boost::asio::io_context::executor_type> strand_;
    std::shared_ptr<PeerSet> peers_;
    SenderGenerationId sender_generation_;

    // Strand-owned. No other executor may read or write these.
    std::uint64_t next_attempt_ = 0;
    std::vector<OutboxRow> rows_;
    std::vector<FenceHandler> fence_waiters_;
    std::uint64_t completions_seen_ = 0;
    std::uint64_t completions_sealed_ = 0;

    /// Bounds as members so a control can force each one independently. They
    /// start at the constants above and production never changes them.
    std::uint32_t max_bind_attempts_ = kMaxBindAttempts;
    std::chrono::milliseconds bind_deadline_ = kBindDeadline;
    std::chrono::milliseconds absolute_bind_deadline_ = kAbsoluteBindDeadline;

    /// Default-inert failpoints. Narrow on purpose: each makes one otherwise
    /// unforceable path deterministic without adding a production branch.
    bool fail_ingress_projection_for_test_ = false;
    bool fail_row_commit_for_test_ = false;
    bool fail_bind_initiation_for_test_ = false;
    bool fail_fence_projection_for_test_ = false;
    std::uint32_t drop_bind_completion_revision_ = 0;

    /// Granted ingress + committed rows, plus the seal bit. Reserved before the
    /// projection is queued, released on refusal or row retirement, never
    /// counted twice.
    std::atomic<std::uint64_t> work_{0};
    std::atomic<bool> fenced_{false};

    /// Atomic so the residue is readable off-strand by whoever has to decide that
    /// this actor is NOT quiescent.
    std::atomic<std::uint64_t> binds_projected_{0};
    std::atomic<std::uint64_t> binds_completed_{0};
};

}  // namespace catl::peer_client
