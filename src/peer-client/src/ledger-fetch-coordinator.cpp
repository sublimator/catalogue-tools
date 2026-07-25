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
    fence_waiters_.reserve(kMaxFenceWaiters);
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
    if (row->bind_attempts >= max_bind_attempts_ ||
        std::chrono::steady_clock::now() >= row->absolute_deadline)
    {
        terminalize_on_strand(*row, "bind bounds exhausted");
        return;
    }

    ++row->bind_revision;
    ++row->bind_attempts;
    row->bind_open = true;
    auto const revision = row->bind_revision;

    bool projected = false;
    bool initiation_failed = false;
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

        binds_projected_.fetch_add(1, std::memory_order_acq_rel);
        projected = peers_->seal_node_batch_command(
            row->intent,       // copied: the row keeps the original for its life
            row->pin.share(),  // and keeps the retention it owns
            strand_,
            [self = shared_from_this(), attempt, revision](
                std::optional<NodeBatchCommand> sealed) {
                self->binds_completed_.fetch_add(1, std::memory_order_acq_rel);
                ++self->completions_seen_;
                if (sealed)
                {
                    ++self->completions_sealed_;
                }
                if (revision == self->drop_bind_completion_revision_)
                {
                    // Failpoint: behave exactly as a lost completion post does —
                    // the capability is dropped and the row is left for its
                    // deadline to recover.
                    self->check_fenced_on_strand();
                    return;
                }
                self->settle_bind_on_strand(
                    attempt, revision, std::move(sealed), /*timed_out=*/false);
                // The settle above may have been inert (a timeout won this race).
                // The fence still has to be re-checked, or a fence concurrent
                // with a losing completion would never report.
                self->check_fenced_on_strand();
            });
        if (!projected)
        {
            binds_completed_.fetch_add(1, std::memory_order_acq_rel);
        }
    }
    catch (...)
    {
        // Initiation threw after the revision was opened. Falling through to the
        // ordinary settle path is what guarantees an accepted permit can never sit
        // in awaiting_bind with nothing in flight.
        PLOGE(log_, "bind initiation threw; settling as a failed bind");
        binds_completed_.fetch_add(1, std::memory_order_acq_rel);
        initiation_failed = true;
    }

    if (initiation_failed || !projected)
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
    if (row->bind_attempts >= max_bind_attempts_ ||
        std::chrono::steady_clock::now() >= row->absolute_deadline)
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

/// Deliver one fence disposition. Containment lives HERE, not at the call sites,
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
LedgerFetchCoordinator::deliver_fence_outcome(
    FenceHandler const& handler,
    FenceOutcome outcome) noexcept
{
    if (!handler)
    {
        return;
    }
    try
    {
        handler(outcome);
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

//@@authority transition=ledger_fetch_coordinator.seal_ingress
// owner=ledger_fetch_coordinator commit=atomic
LedgerFetchCoordinator::Fence
LedgerFetchCoordinator::fence(FenceHandler on_fenced)
{
    work_.fetch_or(kIngressSealedBit, std::memory_order_acq_rel);
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
        return Fence::projection_failed;
    }
    return Fence::drain_queued;
}

void
LedgerFetchCoordinator::begin_fence_on_strand(FenceHandler on_fenced)
{
    if (fenced_.load(std::memory_order_acquire))
    {
        // Already fenced; this caller is still owed exactly one disposition.
        deliver_fence_outcome(on_fenced, FenceOutcome::fenced);
        return;
    }
    if (on_fenced)
    {
        if (fence_waiters_.size() >= kMaxFenceWaiters)
        {
            // Bounded, and refused with a reason. Silently dropping a waiter
            // would leave a caller waiting on a notification that can never come.
            deliver_fence_outcome(
                on_fenced, FenceOutcome::waiter_capacity_exhausted);
        }
        else
        {
            // Cannot allocate: reserved once in the constructor. Each waiter is
            // retained separately, so a second caller never overwrites the first.
            fence_waiters_.push_back(std::move(on_fenced));
        }
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
    // Detach the waiters BEFORE delivering, so the transition is complete and a
    // re-entrant caller cannot be handed any of them twice.
    auto waiters = std::move(fence_waiters_);
    fence_waiters_.clear();
    fence_waiters_.reserve(kMaxFenceWaiters);
    for (auto const& waiter : waiters)
    {
        deliver_fence_outcome(waiter, FenceOutcome::fenced);
    }
}

}  // namespace catl::peer_client
