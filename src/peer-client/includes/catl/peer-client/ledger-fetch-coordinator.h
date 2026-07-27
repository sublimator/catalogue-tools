#pragma once

#include "node-transport.h"
#include "peer-generation-pin.h"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <vector>

class LedgerFetchCoordinatorTestAccess;  // forward decl for test friend

namespace catl::peer_client {

class PeerSet;

/**
 * Coordinator-owned sender half of the NodeBatch protocol: ingress through to
 * the acknowledgement that retires the receiver's exact retained row.
 *
 * This actor exists because production has no executor able to own a replayable
 * sender row. `NodeCache` serialises with a mutex plus a header-state strand,
 * and a mutex cannot own an asynchronous row lifecycle. `PeerClient` owns the
 * RECEIVER side, so sender authority must not be minted there.
 *
 * Scope of THIS slice: bounded ingress reservation, coordinator-strand row
 * commit, exact-pin seal through PeerSet, bounded bind retry, publication of
 * the sealed command on the retained pin, and consumption of the exact receipt
 * through to its acknowledgement. Nothing in this class claims node arrival,
 * retry selection across peers, proof membership, C09 membership, or generation
 * closure.
 *
 * The row is OWNED from ingress to acknowledgement. An accepted publication may
 * already have created a replayable receiver row, and this row is the only
 * authority able to mint that row's exact acknowledgement — so it is retained
 * through an unknown disposition rather than erased, and a fence waits on it.
 *
 * ## The fence is LOGICAL, and is not quiescence
 *
 * Read this before using `fence()` for anything that needs a drain.
 *
 * `fence()` seals ingress and terminalizes every row THAT OWES NOTHING. It does
 * NOT wait for outstanding seal projections: PeerSet does not promise one
 * completion per accepted projection — a lost completion post is the exact
 * failure this actor recovers from — so a fence that waited on one would
 * inherit the liveness bug it exists to fix.
 *
 * It DOES wait on rows that owe the receiver an acknowledgement, and that is
 * not the same class of promise. The receiver retains those rows, and only this
 * coordinator's identity can retire them, so a fence that erased them would
 * report a drain it had not performed and leave receiver state nobody could
 * repair. Such rows keep their work unit, and that held unit IS the
 * backpressure.
 *
 * Retry — of publication and of acknowledgement — is deferred on a timer the
 * row OWNS: never a direct recursion, never a naked post. A `false` from
 * PeerClient usually means its bounded ingress is momentarily full, and
 * recursing spent the whole attempt budget before that strand could drain a
 * single item.
 *
 * The consequence is stated rather than implied: after `Lifecycle::fenced`,
 * `outstanding_bind_projections()` may be nonzero, meaning bind completion work
 * is still unobserved. **A fenced coordinator must not be treated as quiescent,
 * and `fenced` may not satisfy a cutover or shutdown-quiescence requirement.**
 * Proving quiescence needs an owned receipt/cancellation path against PeerSet,
 * which this slice does not have. The counter is public precisely so the
 * missing work is countable instead of assumed away.
 *
 * The residue is NOT a count of physical pin shares. PeerSet consumes the share
 * in its own owner-strand handler and releases it there, before the completion
 * is posted, so a completion carries a command and no retention.
 *
 * ## Design points earlier rejected versions got wrong
 *
 *  * A synchronous return claims ONLY that a projection was queued. It never
 *    claims an attempt ID, a row, or a completed drain.
 *  * ONE conserved projection covers granted ingress, committed rows, and the
 *    ingress seal, in a single atomic word. Two counters could not be read
 *    consistently across the permit-to-row transfer, and a separate `fencing`
 *    flag left a window where a caller admitted before the seal could still
 * take a permit after the fence had been reported.
 *  * Every row keeps its ORIGINAL intent and ORIGINAL pin for life; each bind
 *    hands PeerSet a fresh `share()`.
 *  * Callbacks re-find their row by `(attempt, bind_revision)` and never
 * capture a row reference. An event must match the open revision AND find the
 * bind open, so a retry cannot be completed by the previous bind's callback.
 *  * Every accepted permit either commits a row or is released. All fallible
 *    preparation — intent construction, timer allocation, timer arming, bind
 *    initiation — is contained, because an escaping exception on the strand
 * would both leak the permit and take the handler out with it.
 *  * BIND retry is a direct owner-strand call, not a post: a posted retry can
 * fail to queue and orphan a row in `awaiting_bind` with nothing in flight.
 *    PUBLICATION and ACKNOWLEDGEMENT retry is the opposite — deferred on a
 * timer the row OWNS — because there the failure is usually the receiver's
 * bounded ingress being momentarily full, and an immediate retry spends the
 * whole budget without ever yielding to it. Different failure, different
 * remedy.
 *
 * Back-pressure, stated because it looks like a leak and is not: a row holds
 * its work unit until its receipt obligation is settled. So `kMaxWork` rows
 * awaiting receipts or acknowledgements refuse further ingress, and a fence
 * waits on them. Both are asserted by controls rather than left to be
 * discovered.
 */
class LedgerFetchCoordinator
    : public std::enable_shared_from_this<LedgerFetchCoordinator>
{
public:
    /// Hard cap on live work: granted-but-uncommitted ingress plus committed
    /// rows, as ONE budget. Row storage is reserved to this exactly once.
    static constexpr std::size_t kMaxWork = 64;

    /// Bounded fence waiters. Beyond this a caller is refused with a
    /// disposition rather than silently dropped.
    static constexpr std::size_t kMaxFenceWaiters = 8;

    /// Absorbing ceiling for the unobserved-projection residue. Reaching it
    /// means something is losing completions at scale; saturating there is a
    /// bounded, visible failure, whereas wrapping would silently report
    /// "nothing outstanding" after enough losses.
    static constexpr std::uint64_t kMaxOutstandingProjections =
        std::numeric_limits<std::uint64_t>::max() - 1;

    /// Publication bounds. A receipt is NOT guaranteed by an accepted
    /// publication: PeerClient's duplicate-waiter path returns without invoking
    /// the handler at all, and tells the sender to replay once the original
    /// terminal receipt is available. So publication needs its own deadline for
    /// exactly the reason the bind did, and its recovery is REPLAY of the same
    /// command — never a new attempt, because a second identity for one payload
    /// is what the receiver's identity_conflict refusal exists to catch.
    static constexpr std::uint32_t kMaxPublishAttempts = 3;
    static constexpr std::chrono::milliseconds kPublishDeadline{250};

    /// Acknowledgement bounds. `acknowledge_node_batch_receipt` returns false
    /// when the receiver's bounded ingress is momentarily full, which is a
    /// retry input rather than a terminal fact.
    static constexpr std::uint32_t kMaxAckAttempts = 3;

    /// Backoff between publication or acknowledgement attempts.
    ///
    /// Load-bearing, not politeness. A `false` from PeerClient often means its
    /// bounded ingress is momentarily full, and retrying by direct recursion
    /// burned the entire attempt budget before that strand could drain a single
    /// item — so the bound was spent without the receiver ever getting a chance
    /// to accept. Retry yields.
    static constexpr std::chrono::milliseconds kRetryBackoff{5};

    /// Backoff between REPAIR rounds for a row whose disposition never settled.
    ///
    /// Repair rounds are UNBOUNDED, and that is deliberate. The receiver keeps
    /// its retained row until its incarnation dies, so the duty to retire it
    /// does not expire — giving up would erase the only authority able to do
    /// it, which is the defect this whole correction exists to fix. What IS
    /// bounded is the number of such rows: each holds a work unit out of
    /// kMaxWork, so ingress refuses and a fence waits. Back-pressure is the
    /// bound, not a countdown.
    static constexpr std::chrono::milliseconds kRepairBackoff{250};

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

    /// The one terminal transport fact projected back to an accepted ingress
    /// caller. This does not claim node arrival.
    enum class SubmissionOutcome : std::uint8_t {
        /// A receiver receipt proves the packet left its write queue.
        written,
        /// The coordinator proved that no packet was written for this ingress.
        not_written,
        /// A publication may have reached the receiver, but its bounded
        /// disposition did not settle. The sender row remains owned and
        /// repairs.
        indeterminate,
    };

    /// Delivered at most once, on the coordinator strand, for every
    /// `Ingress::reserved` whose executor continues to make progress. Handler
    /// failure is contained and cannot weaken coordinator accounting.
    using SubmissionOutcomeHandler = std::function<void(SubmissionOutcome)>;

    /// What `fence()` learned synchronously. Same discipline as `Ingress`: it
    /// can only report whether the projection was queued, because everything
    /// else is the strand's to decide.
    ///
    /// The refusals are load-bearing: `drain_queued` is the ONLY value that
    /// promises the handler will be called. Anything else means it will not be,
    /// so a caller can never be left waiting on a notification that cannot
    /// come.
    enum class Fence : std::uint8_t {
        drain_queued,
        /// The drain could not be queued. Ingress STAYS sealed — un-sealing
        /// would resurrect an actor whose callers have already been refused —
        /// so this is reported instead of hidden, and `fence()` is idempotent:
        /// call it again.
        projection_failed,
        /// The bounded fence projection budget is spent. Refused BEFORE
        /// anything is posted, which is the point: the cap has to bound the
        /// public boundary, not just the retained waiter list, or a caller can
        /// queue unbounded work onto the io_context and the bound is
        /// decorative.
        ///
        /// Safe to refuse, because ingress is already sealed and the
        /// projections already queued will terminalize every row. The fence
        /// WILL happen; this caller just does not get a notification unless it
        /// calls again.
        capacity_exhausted,
    };

    /// Called exactly once, on the coordinator strand, through the one callback
    /// funnel — and only when `fence()` returned `drain_queued`. There is no
    /// outcome parameter because there is only one outcome: every other
    /// disposition is reported synchronously by `fence()` itself.
    using FenceHandler = std::function<void()>;

    /// Where one transport attempt has got to.
    enum class Phase : std::uint8_t {
        awaiting_bind,     ///< committed; a bind is open or about to be opened
        publishable,       ///< sealed command held, not yet published
        awaiting_receipt,  ///< publication accepted; a receipt may or may not
                           ///< come
        acknowledging,     ///< receipt retained, NO LONGER PROJECTING, ack in
                           ///< flight
        /// Accepted publication whose disposition could not be settled within
        /// its bounds. The row is RETAINED, identity and all, because an
        /// accepted publication may already have created a replayable receiver
        /// row and this row is the only authority that can mint its exact
        /// acknowledgement.
        ///
        /// Erasing it was the defect: the receiver keeps its row while the
        /// sender discards the one thing able to retire it. The canonical cut
        /// is explicit — sender-owned outbox stays owned until the receipt is
        /// consumed, T62 removes S40/S41 only once S42/S44 are empty, T69
        /// drains rather than erases. A scalar diagnostic is not replacement
        /// authority.
        unsettled,
        terminal,  ///< bounds exhausted before publication, refused, acked, or
                   ///< fenced
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
        std::uint32_t ledger_seq,
        NodeTree tree,
        std::vector<SHAMapNodeID> node_ids,
        PeerGenerationPin pin,
        SubmissionOutcomeHandler on_outcome = {});

    /// Seal ingress and terminalize every row. A LOGICAL fence — see the class
    /// comment; this is not quiescence and must not be used as if it were.
    ///
    /// `on_fenced` is invoked exactly once with a `FenceOutcome`, on the
    /// coordinator strand, through the one callback funnel. Multiple callers
    /// each get their own disposition; none overwrites another.
    [[nodiscard]] Fence
    fence(FenceHandler on_fenced = FenceHandler{});

    /// Await the logical fence. This still does not claim that bind projection
    /// completions have all crossed back from PeerSet; use the shutdown
    /// reconciliation below only after its documented causal barriers.
    boost::asio::awaitable<void>
    co_fence();

    /// Retire shutdown-only bind-completion residue after the caller has:
    ///
    /// 1. stopped all NodeCache admissions and drained its servants;
    /// 2. awaited co_fence();
    /// 3. crossed PeerSet's generation-pin release barrier twice.
    ///
    /// The second PeerSet pass is required because consuming a seal projection
    /// may enqueue its pin release behind the first pass. Entering this method
    /// then forms the coordinator-side barrier: every deliverable completion
    /// caused by those projections is ahead of this coroutine on the strand.
    ///
    /// Returns the number of projections whose completion was provably lost.
    /// Any live work, receipt obligation, retained row, broken accounting, or
    /// unfenced lifecycle fails closed.
    boost::asio::awaitable<std::size_t>
    co_reconcile_shutdown_projection_residue();

    /// Reported, not authoritative: the seal bit inside `work_` is what
    /// actually gates ingress, so there is one authority rather than two.
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
    /// drain. Nonzero after `Lifecycle::fenced` means bind completion work is
    /// still unobserved.
    ///
    /// It is NOT a count of physical pin shares: PeerSet consumes the share in
    /// its own owner-strand handler and releases it there, before the
    /// completion is posted, so the completion carries a command and no
    /// retention. The residue is unobserved completion/receipt work.
    ///
    /// ONE value, not a projected/completed pair. A pair cannot be read
    /// consistently — read P=5, C=4; another projection completes to P=6, C=5;
    /// read C=5 and report zero while the original projection is still open —
    /// and a pair whose halves can be incremented independently saturates to a
    /// false zero the moment they disagree.
    std::size_t
    outstanding_bind_projections() const noexcept
    {
        // Saturation is ABSORBING here, not merely marked. The previous version
        // set a flag at the ceiling and returned without counting the new
        // projection, so the next observation decremented from the ceiling and
        // the value could drain all the way to zero while uncounted work
        // remained — the exact false zero the ceiling exists to prevent,
        // reached by a different route.
        //@@mutation-site projection-saturation-absorbing-merely-recorded
        if (projection_residue_saturated_.load(std::memory_order_acquire))
        {
            return static_cast<std::size_t>(kMaxOutstandingProjections);
        }
        return static_cast<std::size_t>(
            //@@mutation-end projection-saturation-absorbing-merely-recorded
            outstanding_projections_.load(std::memory_order_acquire));
    }

    /// Has ANY conserved quantity been violated — a projection observed that
    /// was never opened, a fence permit released that was never taken, or a
    /// counter driven to saturation?
    ///
    /// Public and sticky rather than log-only. A log line does not fail a test:
    /// a control passed over three real `fence permit underflow` errors because
    /// the release path logged and returned, and the assertions could not see
    /// it. Every control now asserts this is false, so an invariant breach
    /// cannot be walked past again.
    bool
    accounting_broken() const noexcept
    {
        return accounting_broken_.load(std::memory_order_acquire);
    }

    /// Rows that still owe the receiver an exact acknowledgement, or owe
    /// themselves the disposition of a publication the receiver may have
    /// accepted.
    ///
    /// A PROJECTION of owned rows, recomputed and STORED on the strand at every
    /// transition — never incremented. It therefore cannot wrap, and cannot be
    /// non-zero without a nameable retained row behind it, which is the whole
    /// difference from the counter this replaces: that one was bumped in the
    /// two paths that also destroyed the row, so the moment it became non-zero
    /// the identity it referred to was already gone. A count of things nobody
    /// can name is not authority.
    ///
    /// Bounded by kMaxWork, because each such row holds a work unit.
    std::size_t
    receipt_obligations() const noexcept
    {
        return receipt_obligations_.load(std::memory_order_acquire);
    }

    SenderGenerationId
    sender_generation() const noexcept
    {
        return sender_generation_;
    }

private:
    friend class ::LedgerFetchCoordinatorTestAccess;

    /// Top bit of `work_` seals ingress. Sealing and reserving therefore
    /// contend on ONE word: a caller cannot pass an unsealed check and then
    /// reserve against a seal that has since landed.
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
        OutboxRow(
            NodeBatchIntent intent_,
            PeerGenerationPin pin_,
            SubmissionOutcomeHandler on_outcome_) noexcept
            : intent(std::move(intent_))
            , pin(std::move(pin_))
            , on_outcome(std::move(on_outcome_))
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
        //@@authority state=prv.S0019 status=provisional scope=field
        // bind=catl::peer_client::LedgerFetchCoordinator::OutboxRow::on_outcome@13ca41889e935acb
        // owner=unresolved owner_hypothesis=ledger_fetch_coordinator
        // observed_home=ledger_fetch_coordinator
        // label=one_shot_submission_observer_until_publication_disposition
        SubmissionOutcomeHandler on_outcome;
        bool outcome_delivered = false;
        std::optional<NodeBatchCommand> sealed;
        Phase phase = Phase::awaiting_bind;

        /// Monotonic per bind. Together with `bind_open` this is the whole
        /// exactly-once mechanism: an event must match the current revision AND
        /// find the bind still open to commit anything.
        std::uint32_t bind_revision = 0;
        std::uint32_t bind_attempts = 0;
        bool bind_open = false;
        /// Snapshotted per row so the bound is a property of the row rather
        /// than of whatever the actor's setting happens to be when it is
        /// consulted.
        std::uint32_t max_bind_attempts = kMaxBindAttempts;

        /// Publication, same exactly-once shape as the bind: an event must
        /// match the open revision AND find publication still open to commit
        /// anything.
        std::uint32_t publish_revision = 0;
        std::uint32_t publish_attempts = 0;
        bool publish_open = false;
        std::uint32_t max_publish_attempts = kMaxPublishAttempts;

        /// STICKY: a projection of this identity may have reached the receiver,
        /// so a replayable receiver row may exist that only this row can
        /// retire.
        ///
        /// Separate from `publish_attempts` because that counts attempts in the
        /// CURRENT bounded round and is reset per round — and overloading it
        /// with this fact was a real defect: a repair round reset it to zero,
        /// `begin_publish_on_strand` read zero as "never published", and under
        /// a fence the first repair tick terminalized the very row being
        /// retained.
        ///
        /// Set the moment a publication is ACCEPTED. Cleared only by proof that
        /// nothing is owed: an exact terminal receipt naming no receiver row,
        /// or a successful acknowledgement. A later refused round is not proof
        /// of anything and must not clear it. A generation-close witness would
        /// also qualify and does not exist until the C11 slice.
        bool may_have_reached_receiver = false;

        /// The exact terminal receipt, retained. A receipt whose
        /// receiver_commit_sequence is zero identifies NO retained receiver row
        /// — PeerClient refuses to accept an acknowledgement for one — so it is
        /// a terminal fact with nothing to retire.
        std::optional<NodeBatchWriteReceipt> receipt;
        std::uint32_t ack_attempts = 0;

        /// Which deferred retry this row owns, if any. A SEPARATE timer from
        /// `deadline` on purpose: multiplexing one timer between "how long
        /// until I give up waiting" and "how long until I try again" makes a
        /// stale firing indistinguishable from a live one, and cancellation
        /// ambiguous. Allocated during row commit alongside `deadline`, never
        /// lazily. A lazy allocation that failed called mark_unsettled, which
        /// scheduled a retry, which failed again — unbounded synchronous
        /// recursion on a persistent allocation failure.
        enum class Retry : std::uint8_t { none, publish, acknowledge, repair };
        /// Which deferred attempt this row owns. Identity-bearing in the sense
        /// that matters: a fired handler re-checks it, so a cancelled retry
        /// whose handler was already queued cannot act on a row that has moved
        /// on.
        //@@authority state=ledger_fetch_coordinator.row_retry_schedule
        // bind=catl::peer_client::LedgerFetchCoordinator::OutboxRow::retry_kind@9c2b2fcf9be96802
        // owner=ledger_fetch_coordinator
        Retry retry_kind = Retry::none;
        std::shared_ptr<boost::asio::steady_timer> retry;

        std::shared_ptr<boost::asio::steady_timer> deadline;
        std::chrono::steady_clock::time_point absolute_deadline{};
        bool retired = false;

        /// Does this row still owe the receiver an exact acknowledgement, or
        /// owe itself the truth about a publication the receiver may have
        /// accepted?
        ///
        /// This is what a fence must NOT erase and must wait on.
        /// Derived from the STICKY fact, not from transient execution phase.
        ///
        /// Phase-derived was wrong and reported false-empty: initial
        /// publication enters `awaiting_receipt` without a refresh, and a
        /// repair round passes through `publishable` — so a public diagnostic
        /// could read zero while the historical obligation was live.
        bool
        owes_receipt_settlement() const noexcept
        {
            //@@mutation-site obligation-derived-from-sticky-fact
            return !retired && may_have_reached_receiver;
            //@@mutation-end obligation-derived-from-sticky-fact
        }
    };

    OutboxRow*
    find_row(TransportAttemptId attempt) noexcept;

    void
    commit_row_on_strand(
        Hash256 ledger_hash,
        std::uint32_t ledger_seq,
        NodeTree tree,
        std::vector<SHAMapNodeID> node_ids,
        PeerGenerationPin pin,
        SubmissionOutcomeHandler on_outcome);

    void
    begin_bind_on_strand(TransportAttemptId attempt);

    /// Publish, or REPLAY, the row's sealed command on its retained pin.
    void
    begin_publish_on_strand(TransportAttemptId attempt);

    /// Commit one publication outcome. Returns false when the event is stale.
    bool
    settle_publication_on_strand(
        TransportAttemptId attempt,
        std::uint32_t revision,
        std::optional<NodeBatchWriteReceipt> receipt,
        bool timed_out);

    /// Retire the receiver's retained row for this receipt. Called ONLY after
    /// the row has stopped projecting the command.
    void
    acknowledge_on_strand(TransportAttemptId attempt);

    /// Arm this row's OWNED retry timer. Yields to the receiver between
    /// attempts, and is cancelled explicitly by terminalization and by
    /// settlement.
    void
    schedule_retry_on_strand(TransportAttemptId attempt, OutboxRow::Retry kind);

    /// Retain a row whose accepted publication has no settled disposition. It
    /// keeps its identity, its pin and its work unit, so a fence backpressures
    /// on it instead of reporting a false-empty drain. `schedule_repair =
    /// false` when the caller IS a failed scheduling attempt. Otherwise
    /// retaining a row would schedule a repair, whose failure would retain it
    /// again, forever, on the stack.
    void
    mark_unsettled_on_strand(
        OutboxRow& row,
        char const* why,
        bool schedule_repair = true);

    /// One repair round for a retained row: re-acknowledge if the receipt is in
    /// hand, otherwise replay publication to learn a disposition we never got.
    void
    repair_unsettled_on_strand(TransportAttemptId attempt);

    /// Recompute and store the receipt-obligation projection from owned rows.
    void
    refresh_receipt_obligations_on_strand() noexcept;

    static void
    cancel_retry_quietly(OutboxRow& row) noexcept;

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

    static void
    deliver_submission_outcome_noexcept(
        SubmissionOutcomeHandler& handler,
        SubmissionOutcome outcome) noexcept;

    void
    deliver_submission_outcome_on_strand(
        OutboxRow& row,
        SubmissionOutcome outcome) noexcept;

    /// Is either bind bound spent? The single spelling of that question.
    static bool
    bind_bounds_spent(OutboxRow const& row) noexcept;

    /// Non-throwing timer cancellation. A throwing cancel inside a commit
    /// section would abandon the transition half-applied.
    static void
    cancel_quietly(OutboxRow& row) noexcept;

    void
    begin_fence_on_strand(FenceHandler on_fenced);

    void
    release_work() noexcept;

    void
    check_fenced_on_strand();

    /// One projection accepted by PeerSet. Counted only when PeerSet actually
    /// accepted it, so a refusal or a throw before that point counts nothing.
    void
    note_projection_opened() noexcept;

    /// One projection observed. Never saturates: observing one that was never
    /// counted is an accounting violation and is recorded as one.
    void
    note_projection_observed() noexcept;

    /// Take one bounded fence projection permit. False when the budget is
    /// spent.
    bool
    acquire_fence_permit() noexcept;

    void
    release_fence_permit() noexcept;

    /// The ONE place a caller-supplied fence notification is delivered.
    void
    //@@mutation-site fence-disposition-contained-funnel-b
    deliver_fence_notification(FenceHandler const& handler) noexcept;
    //@@mutation-end fence-disposition-contained-funnel-b

    boost::asio::strand<boost::asio::io_context::executor_type> strand_;
    std::shared_ptr<PeerSet> peers_;
    SenderGenerationId sender_generation_;

    // Strand-owned. No other executor may read or write these.
    std::uint64_t next_attempt_ = 0;

    /// The retained sender outbox. Durable and identity-bearing: each row names
    /// the exact attempt and sender generation it owes, which is what lets it
    /// mint the receiver's exact acknowledgement. A fence must not erase a row
    /// that still owes one — the receiver keeps its side, and only this
    /// identity retires it.
    //@@authority state=ledger_fetch_coordinator.sender_outbox
    // bind=catl::peer_client::LedgerFetchCoordinator::rows_@723e122d06318285
    // owner=ledger_fetch_coordinator
    std::vector<OutboxRow> rows_;
    /// Double-buffered on purpose. Delivery swaps these two — `swap` cannot
    /// allocate and cannot throw — so nothing fallible happens after the
    /// terminal fence commit. The previous version cleared and RESERVED after
    /// storing `fenced_`, which is an allocation past the point of no return:
    /// had it thrown, every detached waiter would have been destroyed without
    /// notification and the exception would have escaped the strand handler.
    std::vector<FenceHandler> fence_waiters_;
    std::vector<FenceHandler> fence_delivery_;
    std::uint64_t completions_seen_ = 0;
    std::uint64_t completions_sealed_ = 0;

    /// Stored, never incremented. See receipt_obligations().
    std::atomic<std::size_t> receipt_obligations_{0};

    /// Bounds as members so a control can force each one independently. They
    /// start at the constants above and production never changes them.
    std::uint32_t max_publish_attempts_ = kMaxPublishAttempts;
    std::chrono::milliseconds publish_deadline_ = kPublishDeadline;
    std::uint32_t max_ack_attempts_ = kMaxAckAttempts;
    std::chrono::milliseconds retry_backoff_ = kRetryBackoff;
    std::chrono::milliseconds repair_backoff_ = kRepairBackoff;
    std::uint32_t max_bind_attempts_ = kMaxBindAttempts;
    std::chrono::milliseconds bind_deadline_ = kBindDeadline;
    std::chrono::milliseconds absolute_bind_deadline_ = kAbsoluteBindDeadline;

    /// Default-inert failpoints. Narrow on purpose: each makes one otherwise
    /// unforceable path deterministic without adding a production branch.
    bool fail_ingress_projection_for_test_ = false;
    bool fail_row_commit_for_test_ = false;
    bool fail_bind_initiation_for_test_ = false;
    bool fail_fence_projection_for_test_ = false;
    bool fail_publish_initiation_for_test_ = false;
    /// Refuse to arm the retry timer, to prove a scheduling failure leaves one
    /// named owned obligation and does not recurse.
    bool fail_retry_arm_for_test_ = false;

    /// Force the acknowledgement to be observed as NOT queued.
    ///
    /// A sender-side failpoint is faithful HERE, unlike the lost-completion
    /// case where one was not. `acknowledge_node_batch_receipt` returning false
    /// is the entire observable: there is no callback and no receiver state
    /// change to simulate away, so a coordinator cannot distinguish this from
    /// the receiver refusing on its own bound. The lost completion was
    /// different — simulating it on this side still ran our own callback, which
    /// settled our accounting and made the residue lie.
    ///
    /// It cannot be forced through the real bound either: the receiver's
    /// node-batch ingress projection gates publication and acknowledgement
    /// alike, so saturating it refuses the WRITE and the row never reaches an
    /// ack at all. That bound is exercised against the real receiver on the
    /// publication path, where it is reachable.
    ///
    /// A COUNT, so "refused twice, then accepted" is expressible — which is
    /// what proves the retry yields rather than burning its budget in one
    /// strand pass.
    std::uint32_t fail_next_acknowledgements_for_test_ = 0;

    /// Granted ingress + committed rows, plus the seal bit. Reserved before the
    /// projection is queued, released on refusal or row retirement, never
    /// counted twice.
    std::atomic<std::uint64_t> work_{0};
    std::atomic<bool> fenced_{false};

    /// ONE value, atomic so the residue is readable off-strand by whoever has
    /// to decide that this actor is NOT quiescent. Incremented only when
    /// PeerSet has actually accepted a projection; decremented only when one is
    /// observed.
    ///
    /// Lost completions accumulate monotonically and are never reclaimed, so
    /// this can only grow over a long-lived process. A wrapping counter would
    /// eventually report a false zero, which is the one answer this diagnostic
    /// must never give.
    ///
    /// Read through `outstanding_bind_projections()`, never directly: past the
    /// ceiling this value is meaningless and the saturation flag is
    /// authoritative.
    std::atomic<std::uint64_t> outstanding_projections_{0};

    /// Absorbing. Once set, the public residue reports the ceiling forever and
    /// no observation can drain it. A flag that merely RECORDED reaching the
    /// ceiling was not absorbing at all: opens past it were uncounted while
    /// observations still decremented, so the residue drained back toward zero
    /// with real work outstanding.
    std::atomic<bool> projection_residue_saturated_{false};

    /// Sticky, and shared by every conservation invariant in this class. Set on
    /// an observation with nothing open, a permit released that was never
    /// taken, or a counter reaching its absorbing ceiling.
    std::atomic<bool> accounting_broken_{false};

    /// Queued fence projections plus retained fence waiters, as ONE budget
    /// bounded by kMaxFenceWaiters. Taken before the post and released when the
    /// caller's disposition is settled, so the bound applies to the PUBLIC
    /// boundary rather than only to whatever survived to the strand.
    std::atomic<std::uint64_t> fence_permits_{0};
};

}  // namespace catl::peer_client
