#include <catl/peer-client/ledger-fetch-coordinator.h>
#include <catl/peer-client/peer-set.h>

#include <catl/core/logger.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

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
// bind=catl::peer_client::allocate_sender_generation@dc4f050892bfb475
// owner=ledger_fetch_coordinator commit=atomic
std::optional<SenderGenerationId>
LedgerFetchCoordinator::allocate_sender_generation() noexcept
{
    // Process-common-fate, never reused, and once exhausted it stays exhausted.
    //
    // A CAS below the ceiling, NOT fetch_add. An unconditional increment keeps
    // walking after the first refusal and wraps through zero, which would hand
    // out the no-generation sentinel and then re-issue every value it had
    // already issued — the worst possible failure for an identity whose only
    // job is to be distinct across incarnations.
    //@@mutation-site generation-allocation-cases-below-ceiling
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
    //@@mutation-end generation-allocation-cases-below-ceiling
}

// The ack must be ONE line: the resolver matches reason="..." on the same line
// as the marker, so a wrapped continuation reads as a missing reason.
//@@audit-ack unannotated_authority_candidate reason="factory; the only
// authority it exercises is the sender-generation mint, which carries its own
// directive at the mint site" ref=2026-07-25-206113a-coordinator-review-reject
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
    // Both fence buffers, so delivery can swap between them forever without
    // ever allocating again — including after the terminal fence commit, where
    // an allocation that threw would strand every detached waiter.
    fence_waiters_.reserve(kMaxFenceWaiters);
    fence_delivery_.reserve(kMaxFenceWaiters);
}

//@@authority transition=ledger_fetch_coordinator.reserve_ingress
// bind=catl::peer_client::submit_node_batch_on_pin@6d8e36c291566c87
// owner=ledger_fetch_coordinator commit=atomic
LedgerFetchCoordinator::Ingress
LedgerFetchCoordinator::submit_node_batch_on_pin(
    Hash256 ledger_hash,
    std::uint32_t ledger_seq,
    NodeTree tree,
    std::vector<SHAMapNodeID> node_ids,
    PeerGenerationPin pin,
    SubmissionOutcomeHandler on_outcome)
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
        //@@mutation-site ingress-refuses-once-sealed
        if ((current & kIngressSealedBit) != 0)
        {
            return Ingress::ingress_sealed;
        }
        //@@mutation-end ingress-refuses-once-sealed
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
             ledger_seq,
             tree,
             node_ids = std::move(node_ids),
             pin = std::move(pin),
             on_outcome = std::move(on_outcome)]() mutable {
                self->commit_row_on_strand(
                    std::move(ledger_hash),
                    ledger_seq,
                    tree,
                    std::move(node_ids),
                    std::move(pin),
                    std::move(on_outcome));
            });
    }
    catch (...)
    {
        //@@mutation-site conserved-permit-released-when-projection
        // The permit must not survive a projection that never queued.
        release_work();
        return Ingress::projection_failed;
        //@@mutation-end conserved-permit-released-when-projection
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
    // ONE place, deliberately. This was written out twice — once before opening
    // a bind and once when deciding whether to retry — and a probe that removed
    // the absolute-deadline term from one copy changed no observable behaviour,
    // because the other copy still caught it. Two spellings of one bound means
    // neither is really the bound.
    //@@mutation-site absolute-deadline-bounds-retry-independently
    return row.bind_attempts >= row.max_bind_attempts ||
        std::chrono::steady_clock::now() >= row.absolute_deadline;
    //@@mutation-end absolute-deadline-bounds-retry-independently
}

//@@audit-ack unannotated_authority_candidate reason="internal sub-operation
// dominated exclusively by the annotated settle_bind and terminalize_row commit
// transitions; it does mutate owned timer state, so it is not inert, but it has
// no reachable caller outside those two"
// ref=2026-07-25-b074165-coordinator-review-final-closure
void
LedgerFetchCoordinator::cancel_quietly(OutboxRow& row) noexcept
{
    if (!row.deadline)
    {
        return;
    }
    // Contained, deliberately. A cancel that threw inside a commit section
    // would abandon the transition half-applied — the row already advanced, its
    // timer still armed.
    //
    // This build defines BOOST_ASIO_NO_DEPRECATED, so the `cancel(error_code&)`
    // overload does not exist and plain `cancel()` routes its error code
    // through throw_error. Containment here is therefore the only way to make
    // cancellation non-throwing at this boundary; it is not a preference.
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
// bind=catl::peer_client::commit_row_on_strand@59b9f100d71287e7
// owner=ledger_fetch_coordinator commit=strand
void
LedgerFetchCoordinator::commit_row_on_strand(
    Hash256 ledger_hash,
    std::uint32_t ledger_seq,
    NodeTree tree,
    std::vector<SHAMapNodeID> node_ids,
    PeerGenerationPin pin,
    SubmissionOutcomeHandler on_outcome)
{
    if (ingress_sealed())
    {
        // The permit was granted before the seal landed. Releasing it here is
        // what lets the fence complete without waiting on a caller.
        deliver_submission_outcome_noexcept(
            on_outcome, SubmissionOutcome::not_written);
        release_work();
        check_fenced_on_strand();
        return;
    }
    //@@mutation-site attempt-id-ceiling-refuses-without
    if (next_attempt_ == std::numeric_limits<std::uint64_t>::max())
    //@@mutation-end attempt-id-ceiling-refuses-without
    {
        // Refuse at the ceiling with no partial row. A wrapped attempt ID could
        // be matched by a late event belonging to a retired attempt.
        PLOGE(log_, "attempt id exhausted; refusing without mutation");
        deliver_submission_outcome_noexcept(
            on_outcome, SubmissionOutcome::not_written);
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
        // already granted, so the only acceptable outcomes are a committed row
        // or a released permit — and an exception escaping a strand handler
        // would give neither.
        std::erase_if(rows_, [](OutboxRow const& row) { return row.retired; });
        NodeBatchIntent intent{
            TransportAttemptId{next_attempt_ + 1},
            sender_generation_,
            std::move(ledger_hash),
            ledger_seq,
            tree,
            std::move(node_ids)};
        auto timer = std::make_shared<asio::steady_timer>(strand_);
        // Both timers up front, in the fallible prepare. Allocating the retry
        // timer lazily meant a failure called mark_unsettled, which scheduled a
        // retry, which failed again — unbounded synchronous recursion under a
        // persistent allocation failure.
        auto retry_timer = std::make_shared<asio::steady_timer>(strand_);

        // The append cannot allocate: rows_ was reserved once to kMaxWork, and
        // every live row holds a permit, so at most kMaxWork - 1 remain here.
        static_assert(std::is_nothrow_move_constructible_v<OutboxRow>);
        attempt = intent.attempt_id();
        rows_.emplace_back(
            std::move(intent), std::move(pin), std::move(on_outcome));
        auto& row = rows_.back();
        row.deadline = std::move(timer);
        row.retry = std::move(retry_timer);
        row.max_bind_attempts = max_bind_attempts_;
        //@@mutation-site publication-budget-property-row
        row.max_publish_attempts = max_publish_attempts_;
        //@@mutation-end publication-budget-property-row
        row.absolute_deadline =
            std::chrono::steady_clock::now() + absolute_bind_deadline_;
        next_attempt_ = attempt.value();
    }
    catch (...)
    {
        //@@mutation-site failed-row-commit-releases-permit
        PLOGE(log_, "row commit failed; releasing the accepted ingress permit");
        deliver_submission_outcome_noexcept(
            on_outcome, SubmissionOutcome::not_written);
        release_work();
        //@@mutation-end failed-row-commit-releases-permit
        check_fenced_on_strand();
        return;
    }

    begin_bind_on_strand(attempt);
}

//@@authority transition=ledger_fetch_coordinator.begin_bind
// bind=catl::peer_client::begin_bind_on_strand@ca0fc5d4853038aa
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
        // Nothing to gain from opening a bind that the fence is about to
        // retire.
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
        // liveness gap PeerSet refused to own. Armed BEFORE the projection, so
        // a projection that never completes is still bounded.
        //@@mutation-site bind-deadline-what-recovers-lost
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
        //@@mutation-end bind-deadline-what-recovers-lost

        projected = peers_->seal_node_batch_command(
            row->intent,  // copied: the row keeps the original for its life
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
                // The settle above may have been inert (a timeout won this
                // race). The fence still has to be re-checked, or a fence
                // concurrent with a losing completion would never report.
                self->check_fenced_on_strand();
            });
        if (projected)
        {
            // Counted ONLY once PeerSet has accepted it. Counting before the
            // call — or settling a counter in the catch below — makes the
            // residue lie: the failpoint throws before the projection exists,
            // so an unconditional decrement produced observed-without-opened,
            // and a relation that can go negative was being clamped to a false
            // zero that masked later real work.
            //
            // Safe to count after the call: the completion is posted to this
            // strand and we are on it, so it cannot run before we return.
            note_projection_opened();
        }
    }
    catch (...)
    {
        // Initiation threw after the revision was opened. Falling through to
        // the ordinary settle path is what guarantees an accepted permit can
        // never sit in awaiting_bind with nothing in flight.
        //
        // Nothing is decremented here. Either PeerSet accepted the projection
        // before the throw — in which case its completion will account for it —
        // or it did not, and there is nothing to account for.
        //@@mutation-site projection-settled-only-if-actually
        PLOGE(log_, "bind initiation threw; settling as a failed bind");
    }
    //@@mutation-end projection-settled-only-if-actually

    // `projected` is false on every throwing path as well as on refusal, so ONE
    // condition covers both. A separate `initiation_failed` flag was here and
    // was provably redundant: deleting it changed nothing a control could see.
    //@@mutation-site bind-initiation-throws-orphan-row
    if (!projected)
    {
        // A refused or failed projection is a coordinator retry input, not a
        // caller error.
        settle_bind_on_strand(attempt, revision, std::nullopt, false);
    }
    //@@mutation-end bind-initiation-throws-orphan-row
}

//@@authority transition=ledger_fetch_coordinator.settle_bind
// bind=catl::peer_client::settle_bind_on_strand@be442fb60dc4a6a9
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
    //@@mutation-site exactly-once-event-match-open
    if (!row->bind_open || row->bind_revision != revision)
    {
        return false;
    }
    //@@mutation-end exactly-once-event-match-open
    row->bind_open = false;
    cancel_quietly(*row);  // part of THIS commit, not a later cleanup

    if (sealed)
    {
        row->sealed = std::move(sealed);
        row->phase = Phase::publishable;
        // Straight into publication. The row, its original intent and its
        // original pin stay held throughout, and so does its work unit.
        //
        // No ingress check here, deliberately. There WAS one — `sealed &&
        // !ingress_sealed()` with a separate terminalize branch — and adding
        // the publication gate below made it redundant: a probe that deleted it
        // changed nothing observable, because begin_publish_on_strand refuses a
        // fenced row one step later and terminalizes it identically. That
        // regressed a previously-killed mutation to SURVIVED, which is the
        // "bound spelled twice" defect arriving in a new place. "No publication
        // after the fence" is now spelled once, where publication happens.
        begin_publish_on_strand(attempt);
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
    // orphan the deadline cannot rescue because the deadline was just
    // cancelled. Recursion is bounded by the attempt budget, which is what
    // makes retry finite in the first place.
    begin_bind_on_strand(attempt);
    return true;
}

//@@authority transition=ledger_fetch_coordinator.publish_command
// bind=catl::peer_client::begin_publish_on_strand@36aca9cf9aea234f
// owner=ledger_fetch_coordinator commit=strand
void
LedgerFetchCoordinator::begin_publish_on_strand(TransportAttemptId attempt)
{
    auto* row = find_row(attempt);
    if (row == nullptr || row->phase != Phase::publishable || row->publish_open)
    {
        return;
    }
    //@@mutation-site command-sealed-after-fence-never
    if (ingress_sealed() && !row->may_have_reached_receiver)
    {
        //@@mutation-end command-sealed-after-fence-never
        // Never published, so the receiver holds nothing for this attempt and
        // there is nothing to repair. Safe to terminalize.
        //
        // The qualifier reads the STICKY fact, not `publish_attempts`. A repair
        // round resets that counter, so keying on it meant the first repair
        // tick under a fence saw "never published" and terminalized the very
        // row being retained. Replaying to learn a disposition is DRAINING an
        // accepted obligation, not admitting new work — T69 drains rather than
        // erases.
        terminalize_on_strand(*row, "ingress sealed before any publication");
        return;
    }
    if (!row->sealed)
    {
        terminalize_on_strand(*row, "publishable without a sealed command");
        return;
    }
    if (row->publish_attempts >= row->max_publish_attempts)
    {
        // Exhausted with at least one accepted publication behind it, so the
        // disposition is UNKNOWN rather than known-failed. Retain the identity.
        mark_unsettled_on_strand(*row, "publication attempts exhausted");
        return;
    }

    auto* session = row->pin.get();
    if (session == nullptr)
    {
        // No session to publish on. If nothing ever reached the receiver it
        // holds nothing; if something may have, we can no longer learn or
        // repair it here, so the obligation is retained rather than dropped.
        //
        // Keys on the STICKY fact, never on `publish_attempts`. This branch was
        // the last instance of the authority bug that was fixed at the
        // ingress-seal gate above: a repair round resets the per-round counter
        // to zero, so reading zero as "never published" would terminalize a row
        // whose accepted projection is still unresolved. Ordinary sequencing
        // makes the combination hard to reach today — repair checks the pin
        // immediately before resetting the counter and calls this synchronously
        // — but C11's generation-close and session-invalidation transition is
        // exactly what makes a session die between those two points, and it
        // would have inherited the bug.
        //@@mutation-site null-session-branch-keys-sticky
        if (!row->may_have_reached_receiver)
        {
            terminalize_on_strand(
                *row, "no session and nothing ever published");
            //@@mutation-end null-session-branch-keys-sticky
        }
        else
        {
            mark_unsettled_on_strand(
                *row, "no session with an unresolved publication");
        }
        return;
    }

    ++row->publish_revision;
    ++row->publish_attempts;
    row->publish_open = true;
    row->phase = Phase::awaiting_receipt;
    refresh_receipt_obligations_on_strand();
    auto const revision = row->publish_revision;

    bool accepted = false;
    try
    {
        if (fail_publish_initiation_for_test_)
        {
            throw std::runtime_error("publish initiation failpoint");
        }
        // Armed BEFORE publishing, and it is not belt-and-braces. An accepted
        // publication does NOT promise a receipt: PeerClient's duplicate-waiter
        // capacity path returns without ever invoking the handler and expects
        // the sender to replay. Without this deadline such a row waits forever.
        row->deadline->expires_after(publish_deadline_);
        row->deadline->async_wait(
            [self = shared_from_this(), attempt, revision](
                boost::system::error_code const& ec) {
                if (ec == asio::error::operation_aborted)
                {
                    return;
                }
                self->settle_publication_on_strand(
                    attempt, revision, std::nullopt, /*timed_out=*/true);
            });

        // REPLAY, not re-mint: the same sealed command, same attempt identity.
        // A second identity for one payload is exactly what the receiver's
        // identity_conflict refusal exists to catch.
        //@@authority channel=prv.C0003 status=provisional scope=statement
        // bind=submit_node_batch@8c216cd03b7675ba
        // endpoints=ledger_fetch_coordinator,peer_client
        // channel_semantics=command_receipt calls=submit_node_batch
        // label=candidate_exact_node_batch_write_command_and_receipt_channel
        accepted = session->submit_node_batch(
            *row->sealed,
            [self = shared_from_this(), attempt, revision](
                NodeBatchWriteReceipt receipt) {
                // Delivered on the RECEIVER's strand. Hop to ours before
                // touching any row: the receipt is the receiver's fact, the row
                // is ours.
                asio::post(
                    self->strand_,
                    [self, attempt, revision, receipt]() mutable {
                        self->settle_publication_on_strand(
                            attempt, revision, std::move(receipt), false);
                    });
            });  //@@authority-target ids=prv.C0003
    }
    catch (...)
    {
        PLOGE(
            log_,
            "publish initiation threw; settling as a refused publication");
    }

    if (accepted)
    {
        // STICKY from here. A projection of this identity has reached the
        // receiver, so a replayable row may exist that only this row can
        // retire.
        row->may_have_reached_receiver = true;
        refresh_receipt_obligations_on_strand();
    }

    if (!accepted)
    {
        // Refused or threw. Bounded replay, or terminal, under coordinator
        // bounds.
        settle_publication_on_strand(attempt, revision, std::nullopt, false);
    }
}

//@@authority transition=ledger_fetch_coordinator.settle_publication
// bind=catl::peer_client::settle_publication_on_strand@614c9e537cb57e19
// owner=ledger_fetch_coordinator commit=strand
bool
LedgerFetchCoordinator::settle_publication_on_strand(
    TransportAttemptId attempt,
    std::uint32_t revision,
    std::optional<NodeBatchWriteReceipt> receipt,
    bool timed_out)
{
    auto* row = find_row(attempt);
    if (row == nullptr)
    {
        return false;
    }
    if (!row->publish_open || row->publish_revision != revision)
    {
        return false;
    }
    row->publish_open = false;
    cancel_quietly(*row);

    if (receipt)
    {
        // THE ordering rule from the canonical cut: stop projecting the command
        // BEFORE consuming its receipt and posting the acknowledgement. Every
        // permitted pre-ack replay is therefore already consumed, and a
        // post-ack replay is forbidden at its source — here — rather than
        // defended against by an unbounded tombstone set on the receiver.
        //@@mutation-site row-stops-projecting-before-acknowledgement
        row->phase = Phase::acknowledging;
        row->receipt = std::move(receipt);
        //@@mutation-end row-stops-projecting-before-acknowledgement
        deliver_submission_outcome_on_strand(
            *row,
            row->receipt->outcome() == NodeBatchWriteOutcome::written
                ? SubmissionOutcome::written
                : SubmissionOutcome::not_written);

        if (row->receipt->receiver_commit_sequence() == 0)
        {
            // PROOF that nothing is owed: an exact terminal receipt naming no
            // retained receiver row. This is one of only two things that may
            // clear the sticky fact — the other is a successful
            // acknowledgement. A refused round is not proof of anything.
            //@@mutation-site receipt-naming-retained-row-never
            row->may_have_reached_receiver = false;
            terminalize_on_strand(
                *row, "terminal receipt with no retained row");
            return true;
            //@@mutation-end receipt-naming-retained-row-never
        }
        acknowledge_on_strand(attempt);
        return true;
    }

    if (row->publish_attempts >= row->max_publish_attempts)
    {
        if (timed_out)
        {
            // An accepted publication with no receipt inside the deadline. We
            // do not know whether the receiver retained a row, so the
            // obligation is retained rather than erased.
            //@@mutation-site unknown-publication-disposition-retains-row
            mark_unsettled_on_strand(
                *row, "no receipt within the publication deadline");
            //@@mutation-end unknown-publication-disposition-retains-row
        }
        //@@mutation-site refused-round-erase-earlier-accepted
        else if (row->may_have_reached_receiver)
        //@@mutation-end refused-round-erase-earlier-accepted
        {
            // Refused THIS round, but an earlier projection reached the
            // receiver. A refusal is not proof that nothing is owed, so the
            // obligation stays.
            mark_unsettled_on_strand(
                *row, "round refused with an earlier accepted projection");
        }
        else
        {
            // Never accepted by the receiver, so it holds nothing for this
            // identity.
            terminalize_on_strand(*row, "publication refused, never accepted");
        }
        return true;
    }

    // Back to publishable, then retry on this row's OWNED timer.
    //
    // NOT a direct call. A `false` from PeerClient usually means its bounded
    // ingress is momentarily full, and recursing immediately burned the whole
    // attempt budget before that strand could drain one item — the bound was
    // spent without the receiver ever having a chance to accept. Not a naked
    // post either: the timer is owned by the row, so terminalization and
    // fencing cancel it explicitly rather than hoping a queued lambda finds
    // nothing to do.
    //@@mutation-site publication-retry-yields-instead-recursing
    row->phase = Phase::publishable;
    schedule_retry_on_strand(attempt, OutboxRow::Retry::publish);
    //@@mutation-end publication-retry-yields-instead-recursing
    return true;
}

//@@authority transition=ledger_fetch_coordinator.acknowledge_receipt
// bind=catl::peer_client::acknowledge_on_strand@47ce7f9ef93212af
// owner=ledger_fetch_coordinator commit=strand
void
LedgerFetchCoordinator::acknowledge_on_strand(TransportAttemptId attempt)
{
    auto* row = find_row(attempt);
    if (row == nullptr || row->phase != Phase::acknowledging || !row->receipt)
    {
        return;
    }

    auto* session = row->pin.get();
    if (session == nullptr)
    {
        // The receiver row outlives our retention and we can no longer reach
        // it. Retained, not counted-and-erased: the row IS the authority, and a
        // scalar bumped while destroying the identity it refers to is not a
        // substitute.
        mark_unsettled_on_strand(*row, "pin gone before acknowledgement");
        return;
    }

    ++row->ack_attempts;
    bool queued = false;
    try
    {
        // Minted here and nowhere else: NodeBatchReceiptAck names this
        // coordinator as its only friend, and it retires the exact receiver row
        // this exact receipt identifies.
        if (fail_next_acknowledgements_for_test_ > 0)
        {
            --fail_next_acknowledgements_for_test_;
            queued = false;
        }
        else
        {
            queued = session->acknowledge_node_batch_receipt(
                NodeBatchReceiptAck{*row->receipt});
        }
    }
    catch (...)
    {
        PLOGE(log_, "acknowledgement threw; treated as not queued");
    }

    if (queued)
    {
        // PROOF that nothing is owed: the receiver accepted the exact
        // acknowledgement for the exact row this receipt names.
        row->may_have_reached_receiver = false;
        terminalize_on_strand(*row, "acknowledged");
        return;
    }
    if (row->ack_attempts >= max_ack_attempts_)
    {
        // Bounded, and the row is KEPT. The receiver holds a retained row that
        // only this identity can retire, so discarding it here would leave the
        // receiver permanently unable to be repaired — which is exactly what
        // the canonical cut refuses to defend against with a tombstone set.
        PLOGE(
            log_,
            "acknowledgement refused ",
            row->ack_attempts,
            " time(s); retaining the sender row for attempt ",
            attempt.value());
        //@@mutation-site unretirable-receiver-row-retained-erased
        mark_unsettled_on_strand(*row, "acknowledgement attempts exhausted");
        //@@mutation-end unretirable-receiver-row-retained-erased
        return;
    }
    // Deferred on the row's OWNED timer, yielding to the receiver. A false
    // return usually means its bounded ingress is momentarily full, and
    // recursing here spent all three attempts before that strand could drain a
    // single item.
    //@@mutation-site acknowledgement-retry-yields-instead-recursing
    schedule_retry_on_strand(attempt, OutboxRow::Retry::acknowledge);
    //@@mutation-end acknowledgement-retry-yields-instead-recursing
}

//@@audit-ack unannotated_authority_candidate reason="cancels the retry schedule
// this row owns, so it is NOT inert, but it is reached only from the annotated
// terminalize_row and retain_unsettled_row commits and is dominated by them"
// ref=2026-07-26-0832-annotations-are-in-scope-for-current-slice
void
LedgerFetchCoordinator::cancel_retry_quietly(OutboxRow& row) noexcept
{
    row.retry_kind = OutboxRow::Retry::none;
    if (!row.retry)
    {
        return;
    }
    // Contained for the same reason as the deadline cancel: this build defines
    // BOOST_ASIO_NO_DEPRECATED, so plain cancel() routes its error through
    // throw_error, and a throw inside a commit section abandons the transition.
    try
    {
        row.retry->cancel();
    }
    catch (std::exception const& e)
    {
        PLOGE(log_, "retry cancel threw: ", e.what());
    }
    catch (...)
    {
        PLOGE(log_, "retry cancel threw a non-exception");
    }
}

//@@authority transition=ledger_fetch_coordinator.schedule_retry
// bind=catl::peer_client::schedule_retry_on_strand@6a6991755ceb8a90
// owner=ledger_fetch_coordinator commit=strand
void
LedgerFetchCoordinator::schedule_retry_on_strand(
    TransportAttemptId attempt,
    OutboxRow::Retry kind)
{
    auto* row = find_row(attempt);
    if (row == nullptr || kind == OutboxRow::Retry::none)
    {
        return;
    }
    if (!row->retry)
    {
        // Allocated during commit preparation, so this is an invariant breach
        // rather than an allocation failure. Retain WITHOUT scheduling.
        accounting_broken_.store(true, std::memory_order_release);
        PLOGE(log_, "row has no retry timer; retaining without scheduling");
        mark_unsettled_on_strand(
            *row, "no retry timer", /*schedule_repair=*/false);
        return;
    }

    row->retry_kind = kind;
    try
    {
        if (fail_retry_arm_for_test_)
        {
            throw std::runtime_error("retry arm failpoint");
        }
        row->retry->expires_after(
            kind == OutboxRow::Retry::repair ? repair_backoff_
                                             : retry_backoff_);
        row->retry->async_wait([self = shared_from_this(), attempt, kind](
                                   boost::system::error_code const& ec) {
            if (ec == asio::error::operation_aborted)
            {
                return;
            }
            auto* row = self->find_row(attempt);
            // The kind check is what makes a stale firing inert: a cancelled
            // retry whose handler was already queued finds a row that either
            // moved on or now owns a different retry.
            if (row == nullptr || row->retry_kind != kind)
            {
                return;
            }
            row->retry_kind = OutboxRow::Retry::none;
            if (kind == OutboxRow::Retry::publish)
            {
                self->begin_publish_on_strand(attempt);
            }
            else if (kind == OutboxRow::Retry::repair)
            {
                self->repair_unsettled_on_strand(attempt);
            }
            else
            {
                self->acknowledge_on_strand(attempt);
            }
        });
    }
    catch (...)
    {
        // schedule_repair=false is load-bearing. mark_unsettled used to
        // schedule a repair unconditionally, so an arm failure retained the
        // row, which armed a retry, which failed again — unbounded synchronous
        // recursion on a persistent failure. The row stays owned and named;
        // nothing reschedules it.
        PLOGE(
            log_, "cannot arm the retry timer; retaining without rescheduling");
        //@@mutation-site scheduling-failure-reschedule-itself
        mark_unsettled_on_strand(
            *row, "retry timer would not arm", /*schedule_repair=*/false);
        //@@mutation-end scheduling-failure-reschedule-itself
    }
}

//@@authority transition=ledger_fetch_coordinator.retain_unsettled_row
// bind=catl::peer_client::mark_unsettled_on_strand@4e198aa3515f20da
// owner=ledger_fetch_coordinator commit=strand
void
LedgerFetchCoordinator::mark_unsettled_on_strand(
    OutboxRow& row,
    char const* why,
    bool schedule_repair)
{
    PLOGE(
        log_,
        "attempt ",
        row.intent.attempt_id().value(),
        " retained UNSETTLED: ",
        why,
        "; the receiver may hold a row only this identity can retire");
    row.phase = Phase::unsettled;
    row.publish_open = false;
    deliver_submission_outcome_on_strand(
        row,
        row.may_have_reached_receiver ? SubmissionOutcome::indeterminate
                                      : SubmissionOutcome::not_written);
    cancel_quietly(row);
    cancel_retry_quietly(row);
    // NOT a dead end. Before this, exactly one code path wrote `unsettled` and
    // none read it: the row held its work unit forever and the fence could
    // never complete, which trades "erases the repair authority" for "wedges
    // the actor" — arguably the worse bug. A repair round is scheduled on the
    // row's own timer.
    if (schedule_repair)
    {
        schedule_retry_on_strand(
            row.intent.attempt_id(), OutboxRow::Retry::repair);
    }
    // Deliberately NOT released: not the pin, not the work unit, not the row.
    // The work unit is what makes a fence backpressure on this obligation
    // instead of reporting a false-empty drain, and the pin is what a later
    // repair would need.
    refresh_receipt_obligations_on_strand();
    check_fenced_on_strand();
}

//@@audit-ack unannotated_authority_candidate reason="recomputes a DERIVED
// diagnostic projection from owned rows and stores it; it mutates no owned
// state of its own and is reached only from annotated commit transitions"
// ref=2026-07-26-0832-annotations-are-in-scope-for-current-slice
void
LedgerFetchCoordinator::refresh_receipt_obligations_on_strand() noexcept
{
    // STORED, never incremented, so it cannot wrap and cannot be non-zero
    // without a nameable row behind it.
    std::size_t obligations = 0;
    for (auto const& row : rows_)
    {
        if (row.owes_receipt_settlement())
        {
            ++obligations;
        }
    }
    receipt_obligations_.store(obligations, std::memory_order_release);
}

//@@authority transition=ledger_fetch_coordinator.repair_unsettled_row
// bind=catl::peer_client::repair_unsettled_on_strand@4b11c689adb951dd
// owner=ledger_fetch_coordinator commit=strand
void
LedgerFetchCoordinator::repair_unsettled_on_strand(TransportAttemptId attempt)
{
    auto* row = find_row(attempt);
    if (row == nullptr || row->phase != Phase::unsettled)
    {
        return;
    }
    if (row->pin.get() == nullptr)
    {
        // NOT "the receiver incarnation is gone". PeerGenerationPin::State owns
        // a PeerSessionPtr and neither disconnect nor PeerSet removal nulls it,
        // so this condition means only that OUR retention was dropped — which
        // nothing in production does while a row is live. Claiming it proved
        // the receiver row was moot was fiction dressed as a terminal witness.
        //
        // A real terminal witness is a generation close, which does not exist
        // until the C11 slice. Until then the obligation is retained and
        // reported, not released.
        PLOGE(
            log_,
            "attempt ",
            attempt.value(),
            " cannot be repaired: this row holds no session. Retained; release "
            "awaits a real generation-close witness");
        return;
    }

    // Attempt counters reset per round, which is what makes rounds unbounded
    // while each round stays bounded. Per-round counters only. The sticky fact
    // survives every round, which is the whole point: resetting it made
    // begin_publish read "never published".
    if (row->receipt && row->receipt->receiver_commit_sequence() != 0)
    {
        row->ack_attempts = 0;
        row->phase = Phase::acknowledging;
        refresh_receipt_obligations_on_strand();
        acknowledge_on_strand(attempt);
        return;
    }

    // No receipt: replay the same sealed command to learn the disposition. If
    // the receiver retained a row for this identity it replays its terminal
    // receipt, which is exactly the repair the canonical cut describes.
    if (!row->sealed)
    {
        terminalize_on_strand(*row, "unsettled with no command to replay");
        return;
    }
    row->publish_attempts = 0;
    row->phase = Phase::publishable;
    refresh_receipt_obligations_on_strand();
    begin_publish_on_strand(attempt);
}

//@@authority transition=prv.T0053 status=provisional scope=function
// bind=catl::peer_client::deliver_submission_outcome_noexcept@d79177834044413d
// owner=unresolved owner_hypothesis=ledger_fetch_coordinator commit=strand
// touches=prv.S0019 label=deliver_submission_observer_exception_contained
void
LedgerFetchCoordinator::deliver_submission_outcome_noexcept(
    SubmissionOutcomeHandler& handler,
    SubmissionOutcome outcome) noexcept
{
    auto delivery = std::move(handler);
    handler = {};
    if (!delivery)
        return;
    try
    {
        delivery(outcome);
    }
    catch (std::exception const& e)
    {
        PLOGE(log_, "submission outcome handler threw: ", e.what());
    }
    catch (...)
    {
        PLOGE(log_, "submission outcome handler threw a non-exception");
    }
}

//@@authority transition=prv.T0054 status=provisional scope=function
// bind=catl::peer_client::deliver_submission_outcome_on_strand@b17133adbb84c92c
// owner=unresolved owner_hypothesis=ledger_fetch_coordinator commit=strand
// touches=prv.S0019 label=consume_submission_observer_exactly_once
void
LedgerFetchCoordinator::deliver_submission_outcome_on_strand(
    OutboxRow& row,
    SubmissionOutcome outcome) noexcept
{
    if (row.outcome_delivered)
        return;
    row.outcome_delivered = true;
    deliver_submission_outcome_noexcept(row.on_outcome, outcome);
}

//@@authority transition=ledger_fetch_coordinator.terminalize_row
// bind=catl::peer_client::terminalize_on_strand@576477a17202fcc9
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
    if (!row.outcome_delivered)
    {
        auto const outcome = row.receipt &&
                row.receipt->outcome() == NodeBatchWriteOutcome::written
            ? SubmissionOutcome::written
            : (row.may_have_reached_receiver ? SubmissionOutcome::indeterminate
                                             : SubmissionOutcome::not_written);
        deliver_submission_outcome_on_strand(row, outcome);
    }
    row.phase = Phase::terminal;
    row.bind_open = false;
    row.publish_open = false;
    row.sealed.reset();
    cancel_quietly(row);
    cancel_retry_quietly(
        row);  // both timers, explicitly, or a retry outlives the row
    //@@mutation-site terminal-row-releases-retention-owned
    row.pin.reset();  // the retention this row owned, released here and nowhere
                      // else
    //@@mutation-end terminal-row-releases-retention-owned
    row.retired = true;
    release_work();
    refresh_receipt_obligations_on_strand();
    check_fenced_on_strand();
}

//@@authority transition=ledger_fetch_coordinator.release_work_unit
// bind=catl::peer_client::release_work@e07911105bcfb257
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
            // projection is one of them. This was log-only while the fence
            // permit and projection counters were sticky — an inconsistency
            // that would have let a control pass over a work-unit breach
            // exactly the way one passed over three permit underflows.
            accounting_broken_.store(true, std::memory_order_release);
            PLOGE(
                log_, "work projection underflow; a permit was released twice");
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

/// Deliver one fence notification. Containment lives HERE, not at the call
/// sites, so ordering and exception behaviour are properties of one function.
///
/// Delivered inline rather than posted, deliberately. This runs only after the
/// fence has fully committed — `fenced_` stored, waiters detached — so there is
/// no subsequent transition for a re-entrant caller to corrupt, and a posted
/// notification could be lost to an allocation failure at exactly the moment a
/// caller waiting on shutdown most needs it. What must not happen is an
/// escaping exception, and that is contained below.
//@@callback-funnel
void
LedgerFetchCoordinator::deliver_fence_notification(
    //@@mutation-site fence-disposition-contained-funnel-a
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
    //@@mutation-end fence-disposition-contained-funnel-a
}

//@@authority transition=ledger_fetch_coordinator.open_bind_projection
// bind=catl::peer_client::note_projection_opened@50022b04943c3de2
// owner=ledger_fetch_coordinator commit=atomic
void
LedgerFetchCoordinator::note_projection_opened() noexcept
{
    // CAS to an ABSORBING ceiling, not fetch_add. Lost completions are never
    // reclaimed, so this only grows over a long-lived process; a wrapping
    // counter would eventually report zero outstanding while work was
    // permanently unobserved, which is the single answer this diagnostic must
    // never give.
    auto current = outstanding_projections_.load(std::memory_order_relaxed);
    for (;;)
    {
        if (current >= kMaxOutstandingProjections)
        {
            // ABSORBING, not merely recorded. This branch used to return
            // without counting the projection while observations kept
            // decrementing, so the residue drained back toward zero with
            // uncounted work outstanding. The flag now makes the public residue
            // permanently nonzero.
            projection_residue_saturated_.store(
                true, std::memory_order_release);
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
// bind=catl::peer_client::note_projection_observed@e956cad960e1efa9
// owner=ledger_fetch_coordinator commit=atomic
void
LedgerFetchCoordinator::note_projection_observed() noexcept
{
    auto current = outstanding_projections_.load(std::memory_order_relaxed);
    for (;;)
    {
        if (current == 0)
        {
            // A completion for a projection that was never counted. Recorded,
            // not clamped: clamping produced a residue that read zero while
            // real work was still open, which is indistinguishable from
            // healthy.
            //
            // Silent once saturated, because past the ceiling this counter is
            // already meaningless and every further observation would log the
            // same violation. Note it does NOT return early on saturation:
            // absorbing is spelled ONCE, in outstanding_bind_projections().
            // Guarding it here too made each guard individually sufficient, so
            // deleting either one alone changed nothing a control could see and
            // both mutations survived — the same defect as a bound written out
            // twice.
            if (!projection_residue_saturated_.load(std::memory_order_acquire))
            {
                accounting_broken_.store(true, std::memory_order_release);
                PLOGE(
                    log_,
                    "projection accounting violated: observed a completion "
                    "with "
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
// bind=catl::peer_client::acquire_fence_permit@e66f1190eb6f810a
// owner=ledger_fetch_coordinator commit=atomic
bool
LedgerFetchCoordinator::acquire_fence_permit() noexcept
{
    // Bounds the PUBLIC boundary, not merely the retained waiter list. A permit
    // is held from before the post until the caller's disposition is settled,
    // so queued-but-unrun projections and retained waiters share one budget and
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
// bind=catl::peer_client::release_fence_permit@7d1eb63fce0f3f07
// owner=ledger_fetch_coordinator commit=atomic
void
LedgerFetchCoordinator::release_fence_permit() noexcept
{
    auto current = fence_permits_.load(std::memory_order_relaxed);
    for (;;)
    {
        if (current == 0)
        {
            // Sticky, not log-only. A control passed over three of these
            // because this path logged and returned: the waiters were
            // delivered, the assertions were satisfied, and the invariant
            // breach was visible only to whoever read the test output.
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
// bind=catl::peer_client::fence@a6c50cd459267e35
// owner=ledger_fetch_coordinator commit=atomic
LedgerFetchCoordinator::Fence
LedgerFetchCoordinator::fence(FenceHandler on_fenced)
{
    // Seal FIRST and unconditionally. It is idempotent, allocates nothing, and
    // is the part that must happen even if everything below is refused.
    work_.fetch_or(kIngressSealedBit, std::memory_order_acq_rel);

    //@@mutation-site fence-budget-bounds-public-boundary
    if (!acquire_fence_permit())
    //@@mutation-end fence-budget-bounds-public-boundary
    {
        // Refused BEFORE posting: the cap has to bound what the public API can
        // queue onto the io_context, or a caller can enqueue unbounded lambdas
        // and the bound only ever applied to what survived to the strand.
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
        // Ingress stays sealed. Un-sealing would resurrect an actor whose
        // callers have already been refused, so the honest move is to REPORT
        // that no owner-strand fence is running: this call is idempotent, and a
        // retry re-posts the drain rather than leaving the actor wedged with
        // ingress closed and nobody terminalizing rows.
        PLOGE(log_, "fence projection failed; ingress sealed, retry fence()");
        release_fence_permit();  // nothing was queued, so nothing will settle
                                 // it
        //@@mutation-site fence-projection-cannot-queued-reported
        return Fence::projection_failed;
        //@@mutation-end fence-projection-cannot-queued-reported
    }
    return Fence::drain_queued;
}

asio::awaitable<void>
LedgerFetchCoordinator::co_fence()
{
    auto caller_executor = co_await asio::this_coro::executor;
    auto self = shared_from_this();

    for (;;)
    {
        if (lifecycle() == Lifecycle::fenced)
            co_return;

        auto signal = std::make_shared<asio::steady_timer>(
            caller_executor, asio::steady_timer::time_point::max());
        auto disposition = fence([caller_executor, signal]() noexcept {
            try
            {
                asio::post(caller_executor, [signal]() noexcept {
                    try
                    {
                        signal->cancel();
                    }
                    catch (...)
                    {
                    }
                });
            }
            catch (...)
            {
            }
        });

        if (disposition == Fence::drain_queued)
        {
            boost::system::error_code ec;
            co_await signal->async_wait(
                asio::redirect_error(asio::use_awaitable, ec));
            if (lifecycle() == Lifecycle::fenced)
                co_return;
        }
        else
        {
            // Capacity exhaustion proves another queued fence projection owns
            // the notification budget and will perform the drain. Projection
            // failure leaves ingress sealed and fence() explicitly retryable.
            signal->expires_after(std::chrono::milliseconds(10));
            boost::system::error_code ec;
            co_await signal->async_wait(
                asio::redirect_error(asio::use_awaitable, ec));
        }
    }
}

asio::awaitable<std::size_t>
LedgerFetchCoordinator::co_reconcile_shutdown_projection_residue()
{
    auto self = shared_from_this();
    co_return co_await asio::co_spawn(
        strand_,
        [self]() -> asio::awaitable<std::size_t> {
            if (self->lifecycle() != Lifecycle::fenced ||
                self->work_outstanding() != 0 ||
                self->receipt_obligations() != 0 || self->accounting_broken())
            {
                throw std::logic_error(
                    "coordinator shutdown reconciliation before clean fence");
            }

            auto const live = std::any_of(
                self->rows_.begin(),
                self->rows_.end(),
                [](OutboxRow const& row) { return !row.retired; });
            if (live)
            {
                throw std::logic_error(
                    "coordinator shutdown reconciliation with live row");
            }

            auto const residue = self->outstanding_bind_projections();
            self->outstanding_projections_.store(0, std::memory_order_release);
            co_return residue;
        },
        asio::use_awaitable);
}

//@@authority transition=ledger_fetch_coordinator.register_fence_waiter
// bind=catl::peer_client::begin_fence_on_strand@8bc178949b03a157
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
        // Cannot allocate and cannot overflow: capacity was reserved once in
        // the constructor, and a permit is held for every retained waiter, so
        // the permit bound IS the size bound. Each waiter is retained
        // separately, so a second caller never overwrites the first.
        //@@mutation-site one-fence-caller-never-overwrites
        fence_waiters_.push_back(std::move(on_fenced));
        //@@mutation-end one-fence-caller-never-overwrites
    }
    else
    {
        // No handler, nothing to retain: settle the permit now.
        release_fence_permit();
    }
    // Terminalize eagerly rather than waiting on outstanding seal completions.
    // PeerSet does not promise a completion per accepted projection, so a fence
    // that waited for one would inherit the liveness bug this actor exists to
    // fix. This is why the fence is LOGICAL: see
    // outstanding_bind_projections().
    for (auto& row : rows_)
    {
        if (row.retired)
        {
            continue;
        }
        //@@mutation-site fence-retains-owing-row
        if (row.owes_receipt_settlement())
        {
            // RETAINED. An accepted publication may already have created a
            // replayable receiver row, and this row is the only authority able
            // to mint its exact acknowledgement. Erasing it here left the
            // receiver holding state nobody could retire — the defect this
            // correction fixes.
            //
            // Its work unit stays held, so check_fenced_on_strand backpressures
            // on the obligation rather than reporting a drain that has not
            // happened. Replay and acknowledgement continue to run while
            // fencing: sealing ingress stops NEW work, and draining an accepted
            // obligation is not new work.
            continue;
        }
        //@@mutation-end fence-retains-owing-row
        terminalize_on_strand(row, "fencing");
    }
    refresh_receipt_obligations_on_strand();
    check_fenced_on_strand();
}

//@@authority transition=ledger_fetch_coordinator.commit_fence
// bind=catl::peer_client::check_fenced_on_strand@a9368b09dd247809
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
    //
    // Rows owing a receipt settlement hold their work units, so this single
    // condition also makes the fence backpressure on them. That is intended: a
    // fence that completed while the receiver still held a row only this
    // coordinator could retire would be reporting a drain it had not performed.
    if (work_outstanding() != 0)
    {
        if (receipt_obligations() != 0)
        {
            PLOGD(
                log_,
                "fence waiting on ",
                receipt_obligations(),
                " retained receipt obligation(s)");
        }
        return;
    }
    auto const live =
        std::any_of(rows_.begin(), rows_.end(), [](OutboxRow const& row) {
            return !row.retired;
        });
    if (live)
    {
        PLOGE(log_, "work projection reached zero with a live row remaining");
        return;
    }

    if (before_fence_terminal_commit_for_test_)
        before_fence_terminal_commit_for_test_();
    fenced_.store(true, std::memory_order_release);

    // Detach by SWAP, so nothing fallible happens after the terminal commit.
    // `vector::swap` exchanges buffers and cannot allocate or throw; the
    // previous version cleared and then RESERVED here, an allocation past the
    // point of no return which, on throwing, would have destroyed every
    // detached waiter without notifying it AND escaped this strand handler.
    static_assert(
        noexcept(fence_waiters_.swap(fence_delivery_)),
        "detaching waiters after the fence commit must not be able to throw");
    fence_waiters_.swap(fence_delivery_);
    for (auto const& waiter : fence_delivery_)
    {
        deliver_fence_notification(waiter);
        release_fence_permit();
    }
    // Destroys the handlers and keeps the capacity, so the buffers stay
    // allocated for the life of the actor and this path never allocates again.
    fence_delivery_.clear();
}

}  // namespace catl::peer_client
