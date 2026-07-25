#include <catl/peer-client/ledger-fetch-coordinator.h>
#include <catl/peer-client/peer-set.h>

#include <catl/core/logger.h>

#include <boost/asio/post.hpp>

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace catl::peer_client {

namespace asio = boost::asio;

static LogPartition log_("fetch-coord", LogLevel::INHERIT);

std::atomic<std::uint64_t> LedgerFetchCoordinator::next_sender_generation_{0};

//@@authority transition=ledger_fetch_coordinator.mint_sender_generation
// owner=ledger_fetch_coordinator commit=atomic
std::optional<SenderGenerationId>
LedgerFetchCoordinator::allocate_sender_generation() noexcept
{
    // Process-common-fate, never reused, and once exhausted it stays exhausted.
    //
    // A CAS below the ceiling, NOT fetch_add. An unconditional increment keeps
    // walking after the first refusal and wraps through zero, which would hand
    // out the no-generation sentinel and then re-issue every value it had already
    // issued — the worst possible failure for an identity whose only job is to be
    // distinct across incarnations.
    auto current = next_sender_generation_.load(std::memory_order_relaxed);
    for (;;)
    {
        if (current == std::numeric_limits<std::uint64_t>::max())
        {
            return std::nullopt;
        }
        if (next_sender_generation_.compare_exchange_weak(
                current,
                current + 1,
                std::memory_order_acq_rel,
                std::memory_order_relaxed))
        {
            return SenderGenerationId{current + 1};
        }
    }
}

// The ack must be ONE line: the resolver matches reason="..." on the same line as
// the marker, so a wrapped continuation reads as a missing reason.
//@@audit-ack unannotated_authority_candidate reason="factory; the only authority it exercises is the sender-generation mint, which carries its own directive at the mint site" ref=2026-07-25-206113a-coordinator-review-reject
std::shared_ptr<LedgerFetchCoordinator>
LedgerFetchCoordinator::create(
    asio::io_context& io,
    std::shared_ptr<PeerSet> peers)
{
    if (!peers)
    {
        return nullptr;
    }
    auto generation = allocate_sender_generation();
    if (!generation)
    {
        PLOGE(log_, "sender generation exhausted; refusing to construct");
        return nullptr;
    }
    return std::shared_ptr<LedgerFetchCoordinator>(
        new LedgerFetchCoordinator(io, std::move(peers), *generation));
}

LedgerFetchCoordinator::LedgerFetchCoordinator(
    asio::io_context& io,
    std::shared_ptr<PeerSet> peers,
    SenderGenerationId sender_generation)
    : strand_(asio::make_strand(io))
    , peers_(std::move(peers))
    , sender_generation_(sender_generation)
{
    // Allocated once for the life of the actor, so appending a row or a fence
    // waiter later cannot allocate and therefore cannot throw.
    rows_.reserve(kMaxWork);
    // Both fence buffers, so delivery can swap between them forever without ever
    // allocating again — including after the terminal fence commit, where an
    // allocation that threw would strand every detached waiter.
    fence_waiters_.reserve(kMaxFenceWaiters);
    fence_delivery_.reserve(kMaxFenceWaiters);
}

//@@authority transition=ledger_fetch_coordinator.reserve_ingress
// owner=ledger_fetch_coordinator commit=atomic
LedgerFetchCoordinator::Ingress
LedgerFetchCoordinator::submit_node_batch_on_pin(
    Hash256 ledger_hash,
    NodeTree tree,
    std::vector<SHAMapNodeID> node_ids,
    PeerGenerationPin pin)
{
    if (!pin)
    {
        return Ingress::unpinned_peer;
    }
    // A pure function of the arguments, decided before any permit exists, so a
    // refusal here leaves nothing to reconcile.
    if (node_ids.empty() || node_ids.size() > kMaxNodeBatchRequestNodes)
    {
        return Ingress::node_count_invalid;
    }

    // The seal bit and the count live in ONE word, so admission and sealing
    // contend rather than race: a caller cannot observe "not sealed", have the
    // seal land, and still reserve a permit that outlives the reported fence.
    auto current = work_.load(std::memory_order_relaxed);
    for (;;)
    {
        if ((current & kIngressSealedBit) != 0)
        {
            return Ingress::ingress_sealed;
        }
        if ((current & kWorkMask) >= kMaxWork)
        {
            return Ingress::work_budget_exhausted;
        }
        if (work_.compare_exchange_weak(
                current,
                current + 1,
                std::memory_order_acq_rel,
                std::memory_order_relaxed))
        {
            break;
        }
    }

    try
    {
        if (fail_ingress_projection_for_test_)
        {
            throw std::runtime_error("ingress projection failpoint");
        }
        // No attempt ID is minted here. The strand owns attempt identity, and a
        // synchronous return that named one would be claiming a row that does
        // not exist yet.
        asio::post(
            strand_,
            [self = shared_from_this(),
             ledger_hash = std::move(ledger_hash),
             tree,
             node_ids = std::move(node_ids),
             pin = std::move(pin)]() mutable {
                self->commit_row_on_strand(
                    std::move(ledger_hash),
                    tree,
                    std::move(node_ids),
                    std::move(pin));
            });
    }
    catch (...)
    {
        // The permit must not survive a projection that never queued.
        release_work();
        return Ingress::projection_failed;
    }
    return Ingress::reserved;
}

LedgerFetchCoordinator::OutboxRow*
LedgerFetchCoordinator::find_row(TransportAttemptId attempt) noexcept
{
    auto found = std::find_if(
        rows_.begin(), rows_.end(), [attempt](OutboxRow const& row) {
            return !row.retired && row.intent.attempt_id() == attempt;
        });
    return found == rows_.end() ? nullptr : &*found;
}

bool
LedgerFetchCoordinator::bind_bounds_spent(OutboxRow const& row) noexcept
{
    // ONE place, deliberately. This was written out twice — once before opening a
    // bind and once when deciding whether to retry — and a probe that removed the
    // absolute-deadline term from one copy changed no observable behaviour,
    // because the other copy still caught it. Two spellings of one bound means
    // neither is really the bound.
    return row.bind_attempts >= row.max_bind_attempts ||
        std::chrono::steady_clock::now() >= row.absolute_deadline;
}

//@@audit-ack unannotated_authority_candidate reason="internal sub-operation dominated exclusively by the annotated settle_bind and terminalize_row commit transitions; it does mutate owned timer state, so it is not inert, but it has no reachable caller outside those two" ref=2026-07-25-b074165-coordinator-review-final-closure
void
LedgerFetchCoordinator::cancel_quietly(OutboxRow& row) noexcept
{
    if (!row.deadline)
    {
        return;
    }
    // Contained, deliberately. A cancel that threw inside a commit section would
    // abandon the transition half-applied — the row already advanced, its timer
    // still armed.
    //
    // This build defines BOOST_ASIO_NO_DEPRECATED, so the `cancel(error_code&)`
    // overload does not exist and plain `cancel()` routes its error code through
    // throw_error. Containment here is therefore the only way to make cancellation
    // non-throwing at this boundary; it is not a preference.
    try
    {
        row.deadline->cancel();
    }
    catch (std::exception const& e)
    {
        PLOGE(log_, "timer cancel threw: ", e.what());
    }
    catch (...)
    {
        PLOGE(log_, "timer cancel threw a non-exception");
    }
}

//@@authority transition=ledger_fetch_coordinator.commit_sender_row
// owner=ledger_fetch_coordinator commit=strand
void
LedgerFetchCoordinator::commit_row_on_strand(
    Hash256 ledger_hash,
    NodeTree tree,
    std::vector<SHAMapNodeID> node_ids,
    PeerGenerationPin pin)
{
    if (ingress_sealed())
    {
        // The permit was granted before the seal landed. Releasing it here is
        // what lets the fence complete without waiting on a caller.
        release_work();
        check_fenced_on_strand();
        return;
    }
    if (next_attempt_ == std::numeric_limits<std::uint64_t>::max())
    {
        // Refuse at the ceiling with no partial row. A wrapped attempt ID could
        // be matched by a late event belonging to a retired attempt.
        PLOGE(log_, "attempt id exhausted; refusing without mutation");
        release_work();
        check_fenced_on_strand();
        return;
    }

    TransportAttemptId attempt;
    try
    {
        if (fail_row_commit_for_test_)
        {
            throw std::runtime_error("row commit failpoint");
        }
        // Every fallible step is inside this try: reclaiming retired slots, the
        // intent (which owns the node-ID vector), and the timer. The permit is
        // already granted, so the only acceptable outcomes are a committed row or
        // a released permit — and an exception escaping a strand handler would
        // give neither.
        std::erase_if(rows_, [](OutboxRow const& row) { return row.retired; });
        NodeBatchIntent intent{
            TransportAttemptId{next_attempt_ + 1},
            sender_generation_,
            std::move(ledger_hash),
            tree,
            std::move(node_ids)};
        auto timer = std::make_shared<asio::steady_timer>(strand_);

        // The append cannot allocate: rows_ was reserved once to kMaxWork, and
        // every live row holds a permit, so at most kMaxWork - 1 remain here.
        static_assert(std::is_nothrow_move_constructible_v<OutboxRow>);
        attempt = intent.attempt_id();
        rows_.emplace_back(std::move(intent), std::move(pin));
        auto& row = rows_.back();
        row.deadline = std::move(timer);
        row.max_bind_attempts = max_bind_attempts_;
        row.absolute_deadline =
            std::chrono::steady_clock::now() + absolute_bind_deadline_;
        next_attempt_ = attempt.value();
    }
    catch (...)
    {
        PLOGE(log_, "row commit failed; releasing the accepted ingress permit");
        release_work();
        check_fenced_on_strand();
        return;
    }

    begin_bind_on_strand(attempt);
}

//@@authority transition=ledger_fetch_coordinator.begin_bind
// owner=ledger_fetch_coordinator commit=strand
void
LedgerFetchCoordinator::begin_bind_on_strand(TransportAttemptId attempt)
{
    auto* row = find_row(attempt);
    if (row == nullptr || row->phase != Phase::awaiting_bind || row->bind_open)
    {
        return;
    }
    if (ingress_sealed())
    {
        // Nothing to gain from opening a bind that the fence is about to retire.
        terminalize_on_strand(*row, "ingress sealed");
        return;
    }
    if (bind_bounds_spent(*row))
    {
        terminalize_on_strand(*row, "bind bounds exhausted");
        return;
    }

    ++row->bind_revision;
    ++row->bind_attempts;
    row->bind_open = true;
    auto const revision = row->bind_revision;

    bool projected = false;
    try
    {
        if (fail_bind_initiation_for_test_)
        {
            throw std::runtime_error("bind initiation failpoint");
        }
        // The deadline is the recovery path for a LOST completion — the exact
        // liveness gap PeerSet refused to own. Armed BEFORE the projection, so a
        // projection that never completes is still bounded.
        row->deadline->expires_after(bind_deadline_);
        row->deadline->async_wait(
            [self = shared_from_this(), attempt, revision](
                boost::system::error_code const& ec) {
                if (ec == asio::error::operation_aborted)
                {
                    return;
                }
                self->settle_bind_on_strand(
                    attempt, revision, std::nullopt, /*timed_out=*/true);
            });

        projected = peers_->seal_node_batch_command(
            row->intent,       // copied: the row keeps the original for its life
            row->pin.share(),  // and keeps the retention it owns
            strand_,
            [self = shared_from_this(), attempt, revision](
                std::optional<NodeBatchCommand> sealed) {
                self->note_projection_observed();
                ++self->completions_seen_;
                if (sealed)
                {
                    ++self->completions_sealed_;
                }
                self->settle_bind_on_strand(
                    attempt, revision, std::move(sealed), /*timed_out=*/false);
                // The settle above may have been inert (a timeout won this race).
                // The fence still has to be re-checked, or a fence concurrent
                // with a losing completion would never report.
                self->check_fenced_on_strand();
            });
        if (projected)
        {
            // Counted ONLY once PeerSet has accepted it. Counting before the call
            // — or settling a counter in the catch below — makes the residue lie:
            // the failpoint throws before the projection exists, so an
            // unconditional decrement produced observed-without-opened, and a
            // relation that can go negative was being clamped to a false zero
            // that masked later real work.
            //
            // Safe to count after the call: the completion is posted to this
            // strand and we are on it, so it cannot run before we return.
            note_projection_opened();
        }
    }
    catch (...)
    {
        // Initiation threw after the revision was opened. Falling through to the
        // ordinary settle path is what guarantees an accepted permit can never sit
        // in awaiting_bind with nothing in flight.
        //
        // Nothing is decremented here. Either PeerSet accepted the projection
        // before the throw — in which case its completion will account for it — or
        // it did not, and there is nothing to account for.
        PLOGE(log_, "bind initiation threw; settling as a failed bind");
    }

    // `projected` is false on every throwing path as well as on refusal, so ONE
    // condition covers both. A separate `initiation_failed` flag was here and was
    // provably redundant: deleting it changed nothing a control could see.
    if (!projected)
    {
        // A refused or failed projection is a coordinator retry input, not a
        // caller error.
        settle_bind_on_strand(attempt, revision, std::nullopt, false);
    }
}

//@@authority transition=ledger_fetch_coordinator.settle_bind
// owner=ledger_fetch_coordinator commit=strand
bool
LedgerFetchCoordinator::settle_bind_on_strand(
    TransportAttemptId attempt,
    std::uint32_t revision,
    std::optional<NodeBatchCommand> sealed,
    bool timed_out)
{
    auto* row = find_row(attempt);
    if (row == nullptr)
    {
        return false;
    }
    // Exactly-once between completion and timeout: whichever event matches the
    // open revision commits, and closing the bind makes the loser inert. A
    // completion from revision N therefore cannot settle revision N + 1.
    if (!row->bind_open || row->bind_revision != revision)
    {
        return false;
    }
    row->bind_open = false;
    cancel_quietly(*row);  // part of THIS commit, not a later cleanup

    if (sealed && !ingress_sealed())
    {
        row->sealed = std::move(sealed);
        row->phase = Phase::publishable;
        // Publication is the next slice. The row, its original intent and its
        // original pin stay held, and so does its work unit.
        return true;
    }
    if (sealed)
    {
        // Sealed, but the fence has already sealed ingress. Publication will
        // never happen, so the command is dropped rather than retained.
        terminalize_on_strand(*row, "sealed after ingress seal");
        return true;
    }
    if (bind_bounds_spent(*row))
    {
        terminalize_on_strand(
            *row, timed_out ? "bind deadline exhausted" : "bind refused");
        return true;
    }

    // A DIRECT call, not a post. A posted retry can fail to queue, and that
    // leaves the row in awaiting_bind with nothing in flight and no owner — an
    // orphan the deadline cannot rescue because the deadline was just cancelled.
    // Recursion is bounded by the attempt budget, which is what makes retry
    // finite in the first place.
    begin_bind_on_strand(attempt);
    return true;
}

//@@authority transition=ledger_fetch_coordinator.terminalize_row
// owner=ledger_fetch_coordinator commit=strand
void
LedgerFetchCoordinator::terminalize_on_strand(OutboxRow& row, char const* why)
{
    PLOGD(
        log_,
        "terminal attempt=",
        row.intent.attempt_id().value(),
        " after ",
        row.bind_attempts,
        " bind attempt(s): ",
        why);
    row.phase = Phase::terminal;
    row.bind_open = false;
    row.sealed.reset();
    cancel_quietly(row);
    row.pin.reset();  // the retention this row owned, released here and nowhere else
    row.retired = true;
    release_work();
    check_fenced_on_strand();
}

//@@authority transition=ledger_fetch_coordinator.release_work_unit
// owner=ledger_fetch_coordinator commit=atomic
void
LedgerFetchCoordinator::release_work() noexcept
{
    auto current = work_.load(std::memory_order_relaxed);
    for (;;)
    {
        if ((current & kWorkMask) == 0)
        {
            // Underflow means one unit was released twice, which is the
            // conservation failure this single projection exists to prevent.
            //
            // The public contract says ANY conserved quantity, and the work
            // projection is one of them. This was log-only while the fence permit
            // and projection counters were sticky — an inconsistency that would
            // have let a control pass over a work-unit breach exactly the way one
            // passed over three permit underflows.
            accounting_broken_.store(true, std::memory_order_release);
            PLOGE(log_, "work projection underflow; a permit was released twice");
            return;
        }
        if (work_.compare_exchange_weak(
                current,
                current - 1,
                std::memory_order_acq_rel,
                std::memory_order_relaxed))
        {
            return;
        }
    }
}

/// Deliver one fence notification. Containment lives HERE, not at the call sites,
/// so ordering and exception behaviour are properties of one function.
///
/// Delivered inline rather than posted, deliberately. This runs only after the
/// fence has fully committed — `fenced_` stored, waiters detached — so there is no
/// subsequent transition for a re-entrant caller to corrupt, and a posted
/// notification could be lost to an allocation failure at exactly the moment a
/// caller waiting on shutdown most needs it. What must not happen is an escaping
/// exception, and that is contained below.
//@@callback-funnel
void
LedgerFetchCoordinator::deliver_fence_notification(
    FenceHandler const& handler) noexcept
{
    if (!handler)
    {
        return;
    }
    try
    {
        handler();
    }
    catch (std::exception const& e)
    {
        PLOGE(log_, "fence handler threw: ", e.what());
    }
    catch (...)
    {
        PLOGE(log_, "fence handler threw a non-exception");
    }
}

//@@authority transition=ledger_fetch_coordinator.open_bind_projection
// owner=ledger_fetch_coordinator commit=atomic
void
LedgerFetchCoordinator::note_projection_opened() noexcept
{
    // CAS to an ABSORBING ceiling, not fetch_add. Lost completions are never
    // reclaimed, so this only grows over a long-lived process; a wrapping counter
    // would eventually report zero outstanding while work was permanently
    // unobserved, which is the single answer this diagnostic must never give.
    auto current = outstanding_projections_.load(std::memory_order_relaxed);
    for (;;)
    {
        if (current >= kMaxOutstandingProjections)
        {
            // ABSORBING, not merely recorded. This branch used to return without
            // counting the projection while observations kept decrementing, so the
            // residue drained back toward zero with uncounted work outstanding.
            // The flag now makes the public residue permanently nonzero.
            projection_residue_saturated_.store(true, std::memory_order_release);
            accounting_broken_.store(true, std::memory_order_release);
            PLOGE(
                log_,
                "outstanding projection count saturated; completions are being "
                "lost at scale");
            return;
        }
        if (outstanding_projections_.compare_exchange_weak(
                current,
                current + 1,
                std::memory_order_acq_rel,
                std::memory_order_relaxed))
        {
            return;
        }
    }
}

//@@authority transition=ledger_fetch_coordinator.observe_bind_projection
// owner=ledger_fetch_coordinator commit=atomic
void
LedgerFetchCoordinator::note_projection_observed() noexcept
{
    auto current = outstanding_projections_.load(std::memory_order_relaxed);
    for (;;)
    {
        if (current == 0)
        {
            // A completion for a projection that was never counted. Recorded, not
            // clamped: clamping produced a residue that read zero while real work
            // was still open, which is indistinguishable from healthy.
            //
            // Silent once saturated, because past the ceiling this counter is
            // already meaningless and every further observation would log the same
            // violation. Note it does NOT return early on saturation: absorbing is
            // spelled ONCE, in outstanding_bind_projections(). Guarding it here too
            // made each guard individually sufficient, so deleting either one alone
            // changed nothing a control could see and both mutations survived —
            // the same defect as a bound written out twice.
            if (!projection_residue_saturated_.load(std::memory_order_acquire))
            {
                accounting_broken_.store(true, std::memory_order_release);
                PLOGE(
                    log_,
                    "projection accounting violated: observed a completion with "
                    "no open projection");
            }
            return;
        }
        if (outstanding_projections_.compare_exchange_weak(
                current,
                current - 1,
                std::memory_order_acq_rel,
                std::memory_order_relaxed))
        {
            return;
        }
    }
}

//@@authority transition=ledger_fetch_coordinator.acquire_fence_permit
// owner=ledger_fetch_coordinator commit=atomic
bool
LedgerFetchCoordinator::acquire_fence_permit() noexcept
{
    // Bounds the PUBLIC boundary, not merely the retained waiter list. A permit is
    // held from before the post until the caller's disposition is settled, so
    // queued-but-unrun projections and retained waiters share one budget and
    // neither can grow without the other shrinking.
    auto current = fence_permits_.load(std::memory_order_relaxed);
    for (;;)
    {
        if (current >= kMaxFenceWaiters)
        {
            return false;
        }
        if (fence_permits_.compare_exchange_weak(
                current,
                current + 1,
                std::memory_order_acq_rel,
                std::memory_order_relaxed))
        {
            return true;
        }
    }
}

//@@authority transition=ledger_fetch_coordinator.release_fence_permit
// owner=ledger_fetch_coordinator commit=atomic
void
LedgerFetchCoordinator::release_fence_permit() noexcept
{
    auto current = fence_permits_.load(std::memory_order_relaxed);
    for (;;)
    {
        if (current == 0)
        {
            // Sticky, not log-only. A control passed over three of these because
            // this path logged and returned: the waiters were delivered, the
            // assertions were satisfied, and the invariant breach was visible
            // only to whoever read the test output.
            accounting_broken_.store(true, std::memory_order_release);
            PLOGE(log_, "fence permit underflow; one was released twice");
            return;
        }
        if (fence_permits_.compare_exchange_weak(
                current,
                current - 1,
                std::memory_order_acq_rel,
                std::memory_order_relaxed))
        {
            return;
        }
    }
}

//@@authority transition=ledger_fetch_coordinator.seal_ingress
// owner=ledger_fetch_coordinator commit=atomic
LedgerFetchCoordinator::Fence
LedgerFetchCoordinator::fence(FenceHandler on_fenced)
{
    // Seal FIRST and unconditionally. It is idempotent, allocates nothing, and is
    // the part that must happen even if everything below is refused.
    work_.fetch_or(kIngressSealedBit, std::memory_order_acq_rel);

    if (!acquire_fence_permit())
    {
        // Refused BEFORE posting: the cap has to bound what the public API can
        // queue onto the io_context, or a caller can enqueue unbounded lambdas and
        // the bound only ever applied to what survived to the strand.
        //
        // Safe: ingress is sealed and kMaxFenceWaiters projections are already
        // queued, so rows WILL be terminalized. This caller simply gets no
        // notification unless it calls again.
        return Fence::capacity_exhausted;
    }
    try
    {
        if (fail_fence_projection_for_test_)
        {
            throw std::runtime_error("fence projection failpoint");
        }
        asio::post(
            strand_,
            [self = shared_from_this(),
             on_fenced = std::move(on_fenced)]() mutable {
                self->begin_fence_on_strand(std::move(on_fenced));
            });
    }
    catch (...)
    {
        // Ingress stays sealed. Un-sealing would resurrect an actor whose callers
        // have already been refused, so the honest move is to REPORT that no
        // owner-strand fence is running: this call is idempotent, and a retry
        // re-posts the drain rather than leaving the actor wedged with ingress
        // closed and nobody terminalizing rows.
        PLOGE(log_, "fence projection failed; ingress sealed, retry fence()");
        release_fence_permit();  // nothing was queued, so nothing will settle it
        return Fence::projection_failed;
    }
    return Fence::drain_queued;
}

//@@authority transition=ledger_fetch_coordinator.register_fence_waiter
// owner=ledger_fetch_coordinator commit=strand
void
LedgerFetchCoordinator::begin_fence_on_strand(FenceHandler on_fenced)
{
    if (fenced_.load(std::memory_order_acquire))
    {
        // Already fenced; this caller is still owed exactly one notification.
        deliver_fence_notification(on_fenced);
        release_fence_permit();
        return;
    }
    if (on_fenced)
    {
        // Cannot allocate and cannot overflow: capacity was reserved once in the
        // constructor, and a permit is held for every retained waiter, so the
        // permit bound IS the size bound. Each waiter is retained separately, so a
        // second caller never overwrites the first.
        fence_waiters_.push_back(std::move(on_fenced));
    }
    else
    {
        // No handler, nothing to retain: settle the permit now.
        release_fence_permit();
    }
    // Terminalize eagerly rather than waiting on outstanding seal completions.
    // PeerSet does not promise a completion per accepted projection, so a fence
    // that waited for one would inherit the liveness bug this actor exists to
    // fix. This is why the fence is LOGICAL: see outstanding_bind_projections().
    for (auto& row : rows_)
    {
        if (!row.retired)
        {
            terminalize_on_strand(row, "fencing");
        }
    }
    check_fenced_on_strand();
}

//@@authority transition=ledger_fetch_coordinator.commit_fence
// owner=ledger_fetch_coordinator commit=strand
void
LedgerFetchCoordinator::check_fenced_on_strand()
{
    if (!ingress_sealed() || fenced_.load(std::memory_order_acquire))
    {
        return;
    }
    // The fence is reached when every work unit is accounted for. Outstanding
    // seal projections are deliberately NOT part of this condition, and that is
    // the whole reason this state is named `fenced` rather than `stopped`.
    if (work_outstanding() != 0)
    {
        return;
    }
    auto const live = std::any_of(
        rows_.begin(), rows_.end(), [](OutboxRow const& row) {
            return !row.retired;
        });
    if (live)
    {
        PLOGE(log_, "work projection reached zero with a live row remaining");
        return;
    }

    fenced_.store(true, std::memory_order_release);

    // Detach by SWAP, so nothing fallible happens after the terminal commit.
    // `vector::swap` exchanges buffers and cannot allocate or throw; the previous
    // version cleared and then RESERVED here, an allocation past the point of no
    // return which, on throwing, would have destroyed every detached waiter
    // without notifying it AND escaped this strand handler.
    static_assert(
        noexcept(fence_waiters_.swap(fence_delivery_)),
        "detaching waiters after the fence commit must not be able to throw");
    fence_waiters_.swap(fence_delivery_);
    for (auto const& waiter : fence_delivery_)
    {
        deliver_fence_notification(waiter);
        release_fence_permit();
    }
    // Destroys the handlers and keeps the capacity, so the buffers stay allocated
    // for the life of the actor and this path never allocates again.
    fence_delivery_.clear();
}

}  // namespace catl::peer_client
