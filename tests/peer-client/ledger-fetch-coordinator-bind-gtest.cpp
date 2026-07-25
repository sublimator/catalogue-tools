/// Controls for the LedgerFetchCoordinator awaiting-bind slice:
///
///     bounded ingress reservation
///       -> coordinator-strand row commit (awaiting_bind)
///       -> PeerSet exact-pin seal
///       -> publishable | bounded bind retry | terminal refusal
///
/// The reason this actor exists is the liveness gap PeerSet deliberately refused
/// to own: a seal projection that fails, or whose completion is lost, leaves the
/// sender with nothing to retry from. So the load-bearing controls here are the
/// ones that force those two failures and require recovery.
///
/// No sleeps and no wall-clock waits. Interleavings are forced by stepping the
/// io_context ONE handler at a time with poll_one(), which is why the deadline
/// bound is set to 0ms where a timeout has to win a race.
///
/// Single-threaded throughout, which is what makes the direct (non-hopping) test
/// access sound: the strand only ever runs on the test thread.

#include "ledger-fetch-coordinator-test-access.h"
#include "node-batch-test-session.h"
#include "peer-set-test-access.h"

#include <catl/peer-client/ledger-fetch-coordinator.h>
#include <catl/peer-client/peer-set.h>

#include <gtest/gtest.h>

#include <boost/asio/io_context.hpp>

#include <chrono>
#include <limits>
#include <stdexcept>
#include <memory>
#include <string>
#include <vector>

using namespace catl::peer_client;
using Access = LedgerFetchCoordinatorTestAccess;
using Ingress = LedgerFetchCoordinator::Ingress;
using Phase = LedgerFetchCoordinator::Phase;
using Lifecycle = LedgerFetchCoordinator::Lifecycle;
using Fence = LedgerFetchCoordinator::Fence;
using FenceOutcome = LedgerFetchCoordinator::FenceOutcome;
namespace asio = boost::asio;

namespace {

struct CoordFixture
{
    asio::io_context io;
    std::shared_ptr<PeerSet> peers;
    std::shared_ptr<NodeBatchTestSession> session;
    std::shared_ptr<LedgerFetchCoordinator> coord;
    std::uint64_t generation = 0;

    explicit CoordFixture(std::string endpoint = "203.0.113.23:51235")
    {
        peers = PeerSet::create(
            io,
            PeerSetOptions{
                .network_id = 0,
                .max_in_flight_connects = 0,
                .max_in_flight_crawls = 0,
            });
        session = std::make_shared<NodeBatchTestSession>(std::move(endpoint));
        session->attach_strand(io);
        generation = PeerSetTestAccess::insert_ready_peer_with_generation(
            io, peers, session);
        coord = LedgerFetchCoordinator::create(io, peers);
    }

    PeerGenerationPin
    pin()
    {
        return PeerSetTestAccess::make_peer_pin_for_session(io, peers, session);
    }

    /// Distinctive, non-default payload. A default-valued one cannot detect
    /// substitution: every field has to differ from what a careless bind or a
    /// re-minted intent would produce.
    static Hash256
    ledger_hash()
    {
        Hash256 h{};
        h.data()[0] = 0x5A;
        h.data()[31] = 0xA5;
        return h;
    }

    static std::vector<SHAMapNodeID>
    nodes()
    {
        SHAMapNodeID a{};
        a.depth = 9;
        a.id.data()[0] = 0x31;
        SHAMapNodeID b{};
        b.depth = 17;
        b.id.data()[0] = 0x42;
        return {a, b};
    }

    Ingress
    submit()
    {
        return coord->submit_node_batch_on_pin(
            ledger_hash(), NodeTree::account_state, nodes(), pin());
    }

    /// Run exactly one ready handler. Returns how many ran (0 or 1).
    std::size_t
    step()
    {
        io.restart();
        return io.poll_one();
    }

    /// Drive to quiescence, then leave the context RUNNABLE. poll() returning 0
    /// leaves an io_context stopped, and PeerSetTestAccess::run_on_strand
    /// busy-waits while polling — hand it a stopped context and it deadlocks.
    void
    settle()
    {
        for (int i = 0; i < 256; ++i)
        {
            io.restart();
            if (io.poll() == 0)
            {
                break;
            }
        }
        io.restart();
    }
};

}  // namespace

/// 1. Ingress-projection failure releases the ONE conserved permit.
///
/// The permit is taken before the projection is queued, so a projection that
/// never queues is exactly the case where the single conserved value can leak.
TEST(LedgerFetchCoordinatorBind, IngressProjectionFailureReleasesThePermit)
{
    CoordFixture f;
    ASSERT_TRUE(f.coord != nullptr);
    Access::set_ingress_projection_failure(f.coord, true);

    EXPECT_EQ(f.submit(), Ingress::projection_failed);
    EXPECT_EQ(f.coord->work_outstanding(), 0u)
        << "a permit taken for a projection that never queued must be released";
    f.settle();
    EXPECT_EQ(Access::row_count(f.coord), 0u)
        << "and nothing may have been committed";

    // Disarmed, the same call is accepted — so the refusal above was the
    // failpoint, not a broken fixture.
    Access::set_ingress_projection_failure(f.coord, false);
    EXPECT_EQ(f.submit(), Ingress::reserved);
    EXPECT_EQ(f.coord->work_outstanding(), 1u);
}

/// 2. Refusals decided from the arguments alone take no permit at all.
TEST(LedgerFetchCoordinatorBind, ArgumentRefusalsTakeNoPermit)
{
    CoordFixture f;
    EXPECT_EQ(
        f.coord->submit_node_batch_on_pin(
            CoordFixture::ledger_hash(),
            NodeTree::account_state,
            CoordFixture::nodes(),
            PeerGenerationPin{}),
        Ingress::unpinned_peer);
    EXPECT_EQ(
        f.coord->submit_node_batch_on_pin(
            CoordFixture::ledger_hash(),
            NodeTree::account_state,
            {},
            f.pin()),
        Ingress::node_count_invalid);
    EXPECT_EQ(
        f.coord->submit_node_batch_on_pin(
            CoordFixture::ledger_hash(),
            NodeTree::account_state,
            std::vector<SHAMapNodeID>(kMaxNodeBatchRequestNodes + 1),
            f.pin()),
        Ingress::node_count_invalid);
    EXPECT_EQ(f.coord->work_outstanding(), 0u);
    EXPECT_EQ(Access::row_count(f.coord), 0u);
}

/// 3. An exact pin reaches `publishable`, and the row still holds the ORIGINAL
///    intent and the ORIGINAL pin afterwards.
///
/// Every field is compared, not just the generation. Keeping only one field made
/// "seals the original intent" unfalsifiable in the PeerSet suite, and the same
/// trap applies to the row.
TEST(LedgerFetchCoordinatorBind, ExactBindReachesPublishableRetainingIntentAndPin)
{
    CoordFixture f;
    ASSERT_EQ(f.submit(), Ingress::reserved);
    f.settle();

    ASSERT_EQ(Access::live_row_count(f.coord), 1u);
    auto const attempt = Access::attempt_at(f.coord, 0);
    ASSERT_TRUE(attempt.has_value());
    auto* row = Access::find_any(f.coord, *attempt);
    ASSERT_TRUE(row != nullptr);

    EXPECT_EQ(row->phase, Phase::publishable);
    EXPECT_FALSE(row->bind_open);
    EXPECT_EQ(row->bind_attempts, 1u);
    EXPECT_EQ(row->bind_revision, 1u);
    ASSERT_TRUE(row->sealed.has_value());

    // The sealed command carries the row's own intent, unaltered.
    EXPECT_EQ(row->sealed->attempt_id(), row->intent.attempt_id());
    EXPECT_EQ(row->sealed->sender_generation(), f.coord->sender_generation());
    EXPECT_EQ(row->sealed->ledger_hash(), CoordFixture::ledger_hash());
    EXPECT_EQ(row->sealed->tree(), NodeTree::account_state);
    ASSERT_EQ(row->sealed->node_ids().size(), 2u);
    EXPECT_EQ(row->sealed->node_ids()[0].depth, 9);
    EXPECT_EQ(row->sealed->node_ids()[1].depth, 17);
    EXPECT_EQ(row->sealed->payload_digest(), row->intent.payload_digest());
    EXPECT_EQ(row->sealed->peer_generation().value(), f.generation);

    // And the row kept the retention it owns: the bind consumed only its share.
    EXPECT_TRUE(static_cast<bool>(row->pin));
    EXPECT_EQ(row->pin.connection_generation(), f.generation);
    EXPECT_EQ(f.coord->work_outstanding(), 1u)
        << "a publishable row holds its work unit until publication consumes it";
}

/// 4. An immediately refused projection is retried under coordinator authority,
///    bounded by the attempt budget, and terminalizes without spinning.
TEST(LedgerFetchCoordinatorBind, RefusedProjectionRetriesThenTerminalizes)
{
    CoordFixture f;
    Access::set_max_bind_attempts(f.coord, 2);
    PeerSetTestAccess::set_seal_projection_failure(f.peers, true);

    ASSERT_EQ(f.submit(), Ingress::reserved);
    f.settle();

    auto const attempt = Access::attempt_at(f.coord, 0);
    ASSERT_TRUE(attempt.has_value());
    auto* row = Access::find_any(f.coord, *attempt);
    ASSERT_TRUE(row != nullptr);

    EXPECT_EQ(row->phase, Phase::terminal);
    EXPECT_EQ(row->bind_attempts, 2u)
        << "exactly the budget, so the retry neither stopped early nor spun";
    EXPECT_FALSE(row->sealed.has_value());
    EXPECT_EQ(Access::completions_seen(f.coord), 0u)
        << "a refused projection completes nothing; the retry is ours alone";

    // Terminal releases: the row's own retention and its work unit.
    EXPECT_FALSE(static_cast<bool>(row->pin));
    EXPECT_EQ(f.coord->work_outstanding(), 0u);
    EXPECT_EQ(Access::live_row_count(f.coord), 0u);

    // And it has stopped: more polling produces no further attempts.
    f.settle();
    EXPECT_EQ(Access::find_any(f.coord, *attempt)->bind_attempts, 2u);
}

/// 5. THE control this whole slice exists for: a LOST completion is recovered by
///    the coordinator's bind deadline.
///
/// PeerSet drops the capability when it cannot post a completion, so from the
/// sender's side the bind simply never answers. Without the deadline the row
/// would sit in awaiting_bind forever.
TEST(LedgerFetchCoordinatorBind, LostCompletionIsRecoveredByTheBindDeadline)
{
    CoordFixture f;
    Access::set_bind_deadline(f.coord, std::chrono::milliseconds{0});
    Access::set_drop_bind_completion_revision(f.coord, 1);

    ASSERT_EQ(f.submit(), Ingress::reserved);
    f.settle();

    auto const attempt = Access::attempt_at(f.coord, 0);
    ASSERT_TRUE(attempt.has_value());
    auto* row = Access::find_any(f.coord, *attempt);
    ASSERT_TRUE(row != nullptr);

    EXPECT_EQ(Access::completions_seen(f.coord), 2u)
        << "revision 1's completion arrived and was dropped; revision 2 answered";
    EXPECT_EQ(row->bind_revision, 2u);
    EXPECT_EQ(row->bind_attempts, 2u);
    EXPECT_EQ(row->phase, Phase::publishable)
        << "the deadline had to re-bind, or this row is still awaiting_bind";
    ASSERT_TRUE(row->sealed.has_value());
    EXPECT_EQ(row->sealed->attempt_id(), row->intent.attempt_id())
        << "recovery must re-bind the SAME attempt, not mint a new one";
}

/// 6. Completion before timeout: the ordinary path settles once and the deadline
///    never contributes.
TEST(LedgerFetchCoordinatorBind, CompletionBeforeTimeoutSettlesOnce)
{
    CoordFixture f;  // default 250ms deadline; the completion is immediate
    ASSERT_EQ(f.submit(), Ingress::reserved);
    f.settle();

    auto const attempt = Access::attempt_at(f.coord, 0);
    ASSERT_TRUE(attempt.has_value());
    auto* row = Access::find_any(f.coord, *attempt);
    ASSERT_TRUE(row != nullptr);
    EXPECT_EQ(row->phase, Phase::publishable);
    EXPECT_EQ(row->bind_attempts, 1u);
    EXPECT_EQ(Access::completions_seen(f.coord), 1u);

    // The armed timer was cancelled as part of the settling commit, so further
    // polling cannot produce a second outcome.
    f.settle();
    EXPECT_EQ(row->bind_attempts, 1u);
    EXPECT_EQ(row->phase, Phase::publishable);
}

/// 7. Timeout before completion: the timeout commits, and the sealed completion
///    that arrives afterwards is inert.
///
/// The timeout is driven through the settle path directly, because a 0ms deadline
/// does NOT win this race — measured, not assumed: asio queues an expired timer's
/// handler when it next examines the timer queue, while the seal's two strand
/// posts are already in the handler queue ahead of it, so the completion runs
/// first every time. Calling settle with `timed_out` is the timer handler's exact
/// body with its exact arguments, so this forces the interleaving rather than
/// approximating it. That the timer really does fire and really does re-bind is
/// proven separately, by LostCompletionIsRecoveredByTheBindDeadline.
///
/// Non-vacuous by construction: `completions_sealed` proves PeerSet really did
/// produce a command for this bind. Without the bind_open/revision guard the row
/// would end publishable instead of terminal.
TEST(LedgerFetchCoordinatorBind, TimeoutBeforeCompletionMakesTheSealInert)
{
    CoordFixture f;
    Access::set_max_bind_attempts(f.coord, 1);

    ASSERT_EQ(f.submit(), Ingress::reserved);
    ASSERT_EQ(f.step(), 1u);  // commit + open bind; completion still queued

    auto const attempt = Access::attempt_at(f.coord, 0);
    ASSERT_TRUE(attempt.has_value());
    auto* row = Access::find_any(f.coord, *attempt);
    ASSERT_TRUE(row != nullptr);
    ASSERT_TRUE(row->bind_open);
    ASSERT_EQ(Access::completions_seen(f.coord), 0u);

    // The deadline commits first, and the attempt budget is spent.
    ASSERT_TRUE(Access::settle_bind(f.coord, *attempt, row->bind_revision, true));
    ASSERT_EQ(row->phase, Phase::terminal);

    // Now let the seal completion arrive.
    f.settle();
    EXPECT_EQ(Access::completions_sealed(f.coord), 1u)
        << "PeerSet must actually have sealed a command, or nothing was refused";
    EXPECT_EQ(row->phase, Phase::terminal)
        << "the later seal must not resurrect a terminalized row";
    EXPECT_FALSE(row->sealed.has_value());
    EXPECT_EQ(row->bind_attempts, 1u);
    EXPECT_EQ(f.coord->work_outstanding(), 0u);
}

/// 8. A stale or future revision token cannot settle the open bind, and the
///    matching one can.
///
/// Driven through the settle path directly, which is exactly what a late callback
/// does, so the interleaving is forced rather than raced for.
TEST(LedgerFetchCoordinatorBind, OnlyTheOpenRevisionMaySettleABind)
{
    CoordFixture f;
    Access::set_max_bind_attempts(f.coord, 4);
    ASSERT_EQ(f.submit(), Ingress::reserved);

    // One handler: the row commit, which opens revision 1 and projects the seal.
    // The completion is queued behind two strand hops and has NOT run.
    ASSERT_EQ(f.step(), 1u);
    auto const attempt = Access::attempt_at(f.coord, 0);
    ASSERT_TRUE(attempt.has_value());
    auto* row = Access::find_any(f.coord, *attempt);
    ASSERT_TRUE(row != nullptr);
    ASSERT_TRUE(row->bind_open);
    ASSERT_EQ(row->bind_revision, 1u);
    ASSERT_EQ(Access::completions_seen(f.coord), 0u);

    // A completion from a revision that has already closed, and one from a
    // revision never opened, are both inert.
    EXPECT_FALSE(Access::settle_bind(f.coord, *attempt, 0, false));
    EXPECT_FALSE(Access::settle_bind(f.coord, *attempt, 2, false));
    EXPECT_TRUE(row->bind_open) << "a stale token must not close the open bind";
    EXPECT_EQ(row->bind_attempts, 1u);

    // The matching token does settle — so the refusals above were the guard.
    EXPECT_TRUE(Access::settle_bind(f.coord, *attempt, 1, true));

    // Retry is a DIRECT owner-strand call, so revision 2 is ALREADY open by the
    // time settle returns. There is deliberately no window in which this row sits
    // in awaiting_bind with nothing in flight — a posted retry that failed to
    // queue would have created exactly that orphan.
    EXPECT_EQ(row->bind_revision, 2u);
    EXPECT_TRUE(row->bind_open);
    EXPECT_EQ(row->bind_attempts, 2u);

    // And the real completion, now carrying a closed revision, changes nothing.
    f.settle();
    EXPECT_GE(Access::completions_seen(f.coord), 1u);
    EXPECT_NE(row->bind_revision, 1u)
        << "the forced timeout retried, so a new revision must be in play";
}

/// 9. The absolute deadline terminalizes a row while its attempt budget is
///    nowhere near spent — so the two bounds are genuinely independent.
TEST(LedgerFetchCoordinatorBind, AbsoluteDeadlineTerminalizesWithBudgetRemaining)
{
    CoordFixture f;
    Access::set_max_bind_attempts(f.coord, 1000);
    // The bind must be REAL and its outcome must arrive in a LATER handler, so
    // the deadline can be retired in between. A permanently refused projection
    // would not do: retry is a direct call, so a refusal loop spends the whole
    // 1000-attempt budget inside the first handler and the absolute deadline
    // never gets a turn — which is how this control first failed.
    Access::set_bind_deadline(f.coord, std::chrono::milliseconds{0});
    Access::set_drop_bind_completion_revision(f.coord, 1);

    ASSERT_EQ(f.submit(), Ingress::reserved);
    ASSERT_EQ(f.step(), 1u);  // commit + one real bind opened
    auto const attempt = Access::attempt_at(f.coord, 0);
    ASSERT_TRUE(attempt.has_value());
    ASSERT_EQ(Access::find_any(f.coord, *attempt)->bind_attempts, 1u);

    // Retire the absolute bound only. The attempt budget is untouched.
    ASSERT_TRUE(Access::expire_absolute_deadline(f.coord, *attempt));
    f.settle();

    auto* row = Access::find_any(f.coord, *attempt);
    ASSERT_TRUE(row != nullptr);
    EXPECT_EQ(row->phase, Phase::terminal);
    EXPECT_EQ(row->bind_attempts, 1u)
        << "with 1000 attempts available, only the absolute deadline can have "
           "terminalized this row";
    EXPECT_EQ(f.coord->work_outstanding(), 0u);
}

/// 10. Fencing during QUEUED ingress reaches `fenced` exactly once.
TEST(LedgerFetchCoordinatorBind, FenceDuringQueuedIngressCompletesExactlyOnce)
{
    CoordFixture f;
    ASSERT_EQ(f.submit(), Ingress::reserved);
    ASSERT_EQ(f.coord->work_outstanding(), 1u);

    // Nothing has run: the permit exists, the row does not.
    ASSERT_EQ(Access::row_count(f.coord), 0u);

    int fenced = 0;
    ASSERT_EQ(
        f.coord->fence([&fenced](FenceOutcome o) {
            EXPECT_EQ(o, FenceOutcome::fenced);
            ++fenced;
        }),
        Fence::drain_queued);
    EXPECT_EQ(f.coord->lifecycle(), Lifecycle::fencing);
    f.settle();

    EXPECT_EQ(fenced, 1);
    EXPECT_EQ(f.coord->lifecycle(), Lifecycle::fenced);
    EXPECT_EQ(f.coord->work_outstanding(), 0u)
        << "the permit granted before the seal must be released, not waited on";
    EXPECT_EQ(Access::live_row_count(f.coord), 0u);

    // Ingress is sealed afterwards, and a later fence owes its own single call.
    EXPECT_EQ(f.submit(), Ingress::ingress_sealed);
    int again = 0;
    ASSERT_EQ(
        f.coord->fence([&again](FenceOutcome o) {
            EXPECT_EQ(o, FenceOutcome::fenced);
            ++again;
        }),
        Fence::drain_queued);
    f.settle();
    EXPECT_EQ(again, 1);
    EXPECT_EQ(fenced, 1) << "the first waiter must not be notified twice";
}

/// 11. Fencing during an OPEN bind completes exactly once, without waiting on a
///     completion PeerSet never promised — and the residue is VISIBLE.
TEST(LedgerFetchCoordinatorBind, FenceDuringOpenBindCompletesExactlyOnce)
{
    CoordFixture f;
    ASSERT_EQ(f.submit(), Ingress::reserved);
    ASSERT_EQ(f.step(), 1u);  // commit + open bind; completion still queued

    auto const attempt = Access::attempt_at(f.coord, 0);
    ASSERT_TRUE(attempt.has_value());
    ASSERT_TRUE(Access::find_any(f.coord, *attempt)->bind_open);
    ASSERT_EQ(Access::completions_seen(f.coord), 0u);

    int fenced = 0;
    ASSERT_EQ(
        f.coord->fence([&fenced](FenceOutcome o) {
            EXPECT_EQ(o, FenceOutcome::fenced);
            ++fenced;
        }),
        Fence::drain_queued);

    // Step until the fence is reported. Stepping rather than settling is what
    // makes the claim observable, but the claim itself is NOT a queue order: it is
    // that `fenced` is reached while the seal completion has still not run.
    // Asserting which handler runs second would be brittle and would prove less.
    for (int i = 0; i < 8 && f.coord->lifecycle() != Lifecycle::fenced; ++i)
    {
        f.step();
    }
    EXPECT_EQ(fenced, 1);
    EXPECT_EQ(f.coord->lifecycle(), Lifecycle::fenced);
    EXPECT_EQ(Access::completions_seen(f.coord), 0u);
    EXPECT_EQ(f.coord->outstanding_bind_projections(), 1u)
        << "the residue that makes this a LOGICAL fence must be countable at the "
           "moment the fence is reported, not merely documented";

    f.settle();
    auto* row = Access::find_any(f.coord, *attempt);
    ASSERT_TRUE(row != nullptr);
    EXPECT_EQ(row->phase, Phase::terminal);
    EXPECT_FALSE(row->sealed.has_value())
        << "a command sealed after the fence must be dropped, not retained";
    EXPECT_FALSE(static_cast<bool>(row->pin));
    EXPECT_EQ(f.coord->work_outstanding(), 0u);

    // The completion arrived AFTER the fence was reported and was inert. That is
    // the point, and it is also exactly why `fenced` is not quiescence.
    EXPECT_EQ(Access::completions_seen(f.coord), 1u);
    EXPECT_EQ(f.coord->outstanding_bind_projections(), 0u);
    EXPECT_EQ(fenced, 1);
}

/// 12. The attempt-ID ceiling refuses without wrapping and without a partial row.
TEST(LedgerFetchCoordinatorBind, AttemptIdCeilingRefusesWithoutMutation)
{
    CoordFixture f;
    Access::set_next_attempt(f.coord, std::numeric_limits<std::uint64_t>::max());

    // The synchronous return only ever claimed reserved INGRESS, so it is still
    // `reserved` here: the refusal belongs to the strand, which owns attempts.
    ASSERT_EQ(f.submit(), Ingress::reserved);
    f.settle();

    EXPECT_EQ(Access::row_count(f.coord), 0u) << "no partial row";
    EXPECT_EQ(
        Access::next_attempt(f.coord),
        std::numeric_limits<std::uint64_t>::max())
        << "and no wrap";
    EXPECT_EQ(f.coord->work_outstanding(), 0u) << "and no leaked permit";
}

/// 13. The conserved projection really does bound live work: publishable rows
///     hold their unit until publication, so ingress refuses at the cap.
///
/// This documents the back-pressure of an unconsumed outbox rather than leaving
/// it to be discovered as a leak.
TEST(LedgerFetchCoordinatorBind, PublishableRowsHoldTheirWorkUnitUntilTheCap)
{
    CoordFixture f;
    for (std::size_t i = 0; i < LedgerFetchCoordinator::kMaxWork; ++i)
    {
        ASSERT_EQ(f.submit(), Ingress::reserved) << "at i=" << i;
        f.settle();
    }
    EXPECT_EQ(f.coord->work_outstanding(), LedgerFetchCoordinator::kMaxWork);
    EXPECT_EQ(Access::live_row_count(f.coord), LedgerFetchCoordinator::kMaxWork);
    EXPECT_EQ(f.submit(), Ingress::work_budget_exhausted);
    EXPECT_EQ(f.coord->work_outstanding(), LedgerFetchCoordinator::kMaxWork)
        << "a refused reservation must not have taken a unit";
}

/// 14. A throwing fence handler is contained at the funnel.
///
/// std::function is not noexcept and nothing forbids a caller's handler from
/// throwing. If it escaped it would propagate out of a strand handler and out of
/// the io_context, taking the fence with it.
TEST(LedgerFetchCoordinatorBind, ThrowingFenceHandlerIsContainedAtTheFunnel)
{
    CoordFixture f;
    ASSERT_EQ(f.submit(), Ingress::reserved);
    f.settle();

    bool ran = false;
    ASSERT_EQ(
        f.coord->fence([&ran](FenceOutcome) {
            ran = true;
            throw std::runtime_error("fence handler throws");
        }),
        Fence::drain_queued);
    EXPECT_NO_THROW(f.settle());
    EXPECT_TRUE(ran);
    EXPECT_EQ(f.coord->lifecycle(), Lifecycle::fenced)
        << "the fence must have committed before the handler was delivered, so a "
           "throwing consumer cannot leave the actor mid-fence";

    // And the actor is still usable: the throw did not poison the strand. A later
    // fence still owes exactly one delivery.
    int again = 0;
    ASSERT_EQ(
        f.coord->fence([&again](FenceOutcome) { ++again; }),
        Fence::drain_queued);
    f.settle();
    EXPECT_EQ(again, 1);
}

/// 15. Distinct coordinators receive distinct sender generations, so a restarted
///     one cannot alias rows retained by its predecessor.
TEST(LedgerFetchCoordinatorBind, SenderGenerationsAreNeverReused)
{
    CoordFixture f;
    auto second = LedgerFetchCoordinator::create(f.io, f.peers);
    ASSERT_TRUE(second != nullptr);
    EXPECT_NE(
        f.coord->sender_generation().value(),
        second->sender_generation().value());
    EXPECT_NE(f.coord->sender_generation().value(), 0u)
        << "zero is the no-generation sentinel and must never be minted";
}

// ---------------------------------------------------------------------------
// Controls for the six blockers raised against the first cut of this slice.
// Each one exists because a reviewer named a specific way the code was wrong,
// not because the shape looked untested.
// ---------------------------------------------------------------------------

/// 16. The sender-generation allocator stays exhausted, and never wraps.
///
/// The first cut used fetch_add and refused only the single call that landed on
/// the ceiling. The very next call wrapped through zero, handing out the
/// no-generation sentinel and then RE-ISSUING every value it had already issued —
/// for an identity whose only job is to be distinct across incarnations.
///
/// A repeated-after-ceiling control, because one refusal proves nothing.
TEST(LedgerFetchCoordinatorBind, SenderGenerationStaysExhaustedAtTheCeiling)
{
    auto const saved = Access::next_sender_generation();
    constexpr auto kMax = std::numeric_limits<std::uint64_t>::max();

    // One below the ceiling: exactly one more value is issuable, and it is the
    // ceiling value itself.
    Access::set_next_sender_generation(kMax - 1);
    auto const last = Access::allocate_sender_generation();
    ASSERT_TRUE(last.has_value());
    EXPECT_EQ(last->value(), kMax);

    // Every subsequent call refuses. Ten of them, because the wrap only showed
    // up on the call AFTER the first refusal.
    for (int i = 0; i < 10; ++i)
    {
        auto const refused = Access::allocate_sender_generation();
        EXPECT_FALSE(refused.has_value()) << "at i=" << i;
    }
    EXPECT_EQ(Access::next_sender_generation(), kMax)
        << "the counter must not walk past the ceiling, or it will wrap";

    // A coordinator cannot be constructed without an identity.
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{});
    EXPECT_TRUE(LedgerFetchCoordinator::create(io, peers) == nullptr);

    Access::set_next_sender_generation(saved);
}

/// 17. Zero is never issued. It is the no-generation sentinel, and the wrap bug
///     produced it directly.
TEST(LedgerFetchCoordinatorBind, SenderGenerationIsNeverZero)
{
    auto const saved = Access::next_sender_generation();
    Access::set_next_sender_generation(0);
    auto const first = Access::allocate_sender_generation();
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->value(), 1u) << "the first issued value is 1, never 0";
    Access::set_next_sender_generation(saved);
}

/// 18. A row commit that throws AFTER the permit was granted releases it.
///
/// Intent construction and timer allocation both allocate. An escaping exception
/// here would leak the conserved unit and take the strand handler with it.
TEST(LedgerFetchCoordinatorBind, RowCommitFailureReleasesTheGrantedPermit)
{
    CoordFixture f;
    Access::set_row_commit_failure(f.coord, true);

    ASSERT_EQ(f.submit(), Ingress::reserved)
        << "the synchronous return only ever claimed reserved ingress";
    EXPECT_EQ(f.coord->work_outstanding(), 1u);

    EXPECT_NO_THROW(f.settle())
        << "an exception must not escape the strand handler";
    EXPECT_EQ(f.coord->work_outstanding(), 0u)
        << "commit-or-release: a failed commit must release the permit";
    EXPECT_EQ(Access::row_count(f.coord), 0u) << "and leave no partial row";

    // Disarmed, the same submission commits — so the release above was the
    // failpoint, not a fixture that never got started.
    Access::set_row_commit_failure(f.coord, false);
    ASSERT_EQ(f.submit(), Ingress::reserved);
    f.settle();
    EXPECT_EQ(Access::live_row_count(f.coord), 1u);
}

/// 19. A row commit failure while fencing still completes the fence.
///
/// Otherwise a permit granted just before the seal, whose commit then fails,
/// would hold the fence open forever.
TEST(LedgerFetchCoordinatorBind, RowCommitFailureWhileFencingStillCompletes)
{
    CoordFixture f;
    Access::set_row_commit_failure(f.coord, true);
    ASSERT_EQ(f.submit(), Ingress::reserved);

    int fenced = 0;
    ASSERT_EQ(
        f.coord->fence([&fenced](FenceOutcome o) {
            EXPECT_EQ(o, FenceOutcome::fenced);
            ++fenced;
        }),
        Fence::drain_queued);
    EXPECT_NO_THROW(f.settle());
    EXPECT_EQ(fenced, 1);
    EXPECT_EQ(f.coord->lifecycle(), Lifecycle::fenced);
    EXPECT_EQ(f.coord->work_outstanding(), 0u);
}

/// 20. Bind initiation that throws does NOT orphan the row.
///
/// The revision is already open at that point. The first cut would have left the
/// row in awaiting_bind with nothing in flight and its deadline unarmed — a state
/// no timer and no completion could ever rescue.
TEST(LedgerFetchCoordinatorBind, BindInitiationFailureDoesNotOrphanTheRow)
{
    CoordFixture f;
    Access::set_max_bind_attempts(f.coord, 2);
    Access::set_bind_initiation_failure(f.coord, true);

    ASSERT_EQ(f.submit(), Ingress::reserved);
    EXPECT_NO_THROW(f.settle());

    auto const attempt = Access::attempt_at(f.coord, 0);
    ASSERT_TRUE(attempt.has_value());
    auto* row = Access::find_any(f.coord, *attempt);
    ASSERT_TRUE(row != nullptr);

    EXPECT_FALSE(row->bind_open)
        << "an open bind with nothing in flight is the orphan this control exists "
           "to forbid";
    EXPECT_EQ(row->phase, Phase::terminal)
        << "initiation failure is a retry input, and the budget was 2";
    EXPECT_EQ(row->bind_attempts, 2u);
    EXPECT_EQ(f.coord->work_outstanding(), 0u);
    EXPECT_EQ(f.coord->outstanding_bind_projections(), 0u)
        << "a projection that never reached PeerSet must not be counted as "
           "outstanding, or the residue count would never return to zero";
}

/// 21. A fence projection that cannot be queued is REPORTED, and recoverable.
///
/// Ingress is sealed before the post. If the post fails silently, the actor is
/// wedged: no new work admitted, and no owner-strand fence to terminalize rows.
TEST(LedgerFetchCoordinatorBind, FenceProjectionFailureIsObservableAndRecoverable)
{
    CoordFixture f;
    ASSERT_EQ(f.submit(), Ingress::reserved);
    f.settle();
    ASSERT_EQ(Access::live_row_count(f.coord), 1u);

    Access::set_fence_projection_failure(f.coord, true);
    int fenced = 0;
    EXPECT_EQ(
        f.coord->fence([&fenced](FenceOutcome) { ++fenced; }),
        Fence::projection_failed)
        << "a caller told nothing about this would be wedged with no way to know";

    f.settle();
    EXPECT_EQ(fenced, 0) << "no fence ran, so no waiter may be notified";
    EXPECT_EQ(f.coord->lifecycle(), Lifecycle::fencing)
        << "ingress stays sealed: un-sealing would resurrect an actor whose "
           "callers have already been refused";
    EXPECT_EQ(f.submit(), Ingress::ingress_sealed);
    EXPECT_EQ(Access::live_row_count(f.coord), 1u)
        << "and no row was terminalized, which is exactly the wedged state";

    // Recovery: fence() is idempotent, so a retry re-posts the drain.
    Access::set_fence_projection_failure(f.coord, false);
    EXPECT_EQ(
        f.coord->fence([&fenced](FenceOutcome o) {
            EXPECT_EQ(o, FenceOutcome::fenced);
            ++fenced;
        }),
        Fence::drain_queued);
    f.settle();
    EXPECT_EQ(fenced, 1);
    EXPECT_EQ(f.coord->lifecycle(), Lifecycle::fenced);
    EXPECT_EQ(Access::live_row_count(f.coord), 0u);
}

/// 22. Several fence callers each get their own disposition. None is overwritten.
///
/// The first cut assigned into a single member, so the second caller silently
/// erased the first — who then waited forever on a notification that was gone.
TEST(LedgerFetchCoordinatorBind, EveryFenceWaiterGetsItsOwnDisposition)
{
    CoordFixture f;
    ASSERT_EQ(f.submit(), Ingress::reserved);
    ASSERT_EQ(f.coord->work_outstanding(), 1u) << "uncommitted, so the fence waits";

    int first = 0;
    int second = 0;
    int third = 0;

    // Waiters only COEXIST while the fence cannot complete, so a permit is left
    // uncommitted. Three plain fence() calls do not test the overwrite at all: the
    // first body completes the fence and the other two are answered on arrival, so
    // each still gets exactly one disposition even if the container is clobbered on
    // every insert. A probe that cleared the vector before each push changed
    // nothing in that version — the control was vacuous and said so.
    ASSERT_EQ(f.coord->fence(), Fence::drain_queued);
    Access::begin_fence(f.coord, [&first](FenceOutcome o) {
        EXPECT_EQ(o, FenceOutcome::fenced);
        ++first;
    });
    Access::begin_fence(f.coord, [&second](FenceOutcome o) {
        EXPECT_EQ(o, FenceOutcome::fenced);
        ++second;
    });
    Access::begin_fence(f.coord, [&third](FenceOutcome o) {
        EXPECT_EQ(o, FenceOutcome::fenced);
        ++third;
    });
    ASSERT_EQ(first + second + third, 0) << "all three are retained, not answered";

    f.settle();
    EXPECT_EQ(first, 1) << "the earliest waiter must not have been overwritten";
    EXPECT_EQ(second, 1);
    EXPECT_EQ(third, 1);
    EXPECT_EQ(f.coord->lifecycle(), Lifecycle::fenced);
}

/// 23. Fence waiters are BOUNDED, and the caller past the bound is told so rather
///     than left waiting on a notification that can never arrive.
///
/// Waiters only accumulate while the fence CANNOT complete — that is, while a
/// granted ingress permit is still uncommitted. Going through `fence()` nine times
/// does not produce that state in a single-threaded harness: the first body
/// terminalizes every row, work reaches zero, the fence completes, and the other
/// eight are answered immediately. I wrote that version first and it reported nine
/// acceptances and zero refusals — the cap was unreachable, which would have made
/// `waiter_capacity_exhausted` decoration.
///
/// The reachable production route is a submit on ANOTHER thread: it CASes a permit
/// before the seal lands, and its commit post arrives after the fence bodies. This
/// control forces exactly that ordering by running the fence bodies directly while
/// a submitted permit is still queued.
TEST(LedgerFetchCoordinatorBind, FenceWaiterCapacityRefusesWithADisposition)
{
    CoordFixture f;
    ASSERT_EQ(f.submit(), Ingress::reserved);
    ASSERT_EQ(f.coord->work_outstanding(), 1u) << "uncommitted, so the fence waits";
    ASSERT_EQ(Access::row_count(f.coord), 0u);

    int accepted = 0;
    int refused = 0;
    auto handler = [&accepted, &refused](FenceOutcome o) {
        if (o == FenceOutcome::fenced)
        {
            ++accepted;
        }
        else
        {
            ++refused;
        }
    };

    // Seal ingress the way fence() does, then run one body past the cap.
    ASSERT_EQ(f.coord->fence(), Fence::drain_queued);
    for (std::size_t i = 0; i < LedgerFetchCoordinator::kMaxFenceWaiters + 1; ++i)
    {
        Access::begin_fence(f.coord, handler);
    }
    EXPECT_EQ(refused, 1) << "the caller past the bound must learn why, at once";
    EXPECT_EQ(accepted, 0) << "and the ones within it are still waiting";

    // The queued commit releases the permit, and every retained waiter is served.
    f.settle();
    EXPECT_EQ(f.coord->lifecycle(), Lifecycle::fenced);
    EXPECT_EQ(accepted, static_cast<int>(LedgerFetchCoordinator::kMaxFenceWaiters));
    EXPECT_EQ(
        accepted + refused,
        static_cast<int>(LedgerFetchCoordinator::kMaxFenceWaiters + 1))
        << "every caller is owed exactly one disposition, bounded or not";
}

/// 24. A caller who fences an ALREADY fenced coordinator is answered, not queued.
TEST(LedgerFetchCoordinatorBind, FencingAnAlreadyFencedCoordinatorAnswersAtOnce)
{
    CoordFixture f;
    ASSERT_EQ(f.coord->fence(), Fence::drain_queued);
    f.settle();
    ASSERT_EQ(f.coord->lifecycle(), Lifecycle::fenced);

    int late = 0;
    ASSERT_EQ(
        f.coord->fence([&late](FenceOutcome o) {
            EXPECT_EQ(o, FenceOutcome::fenced);
            ++late;
        }),
        Fence::drain_queued);
    f.settle();
    EXPECT_EQ(late, 1);
}

/// 25. A command sealed while ingress is already sealed is DROPPED.
///
/// This branch is only reachable when the completion arrives BEFORE the fence
/// body — the row is still live and ingress is already sealed. Control 11 arranges
/// the opposite order, so it never entered here: a probe that removed the
/// `!ingress_sealed()` term changed nothing it could see. Reaching the branch means
/// letting PeerSet's seal handler run first, so the completion is queued ahead of
/// the fence body.
TEST(LedgerFetchCoordinatorBind, SealArrivingAfterIngressSealIsDropped)
{
    CoordFixture f;
    ASSERT_EQ(f.submit(), Ingress::reserved);
    ASSERT_EQ(f.step(), 1u);  // commit + bind opened; seal queued on PeerSet
    ASSERT_EQ(f.step(), 1u);  // PeerSet seals; the completion is now queued on us
    auto const attempt = Access::attempt_at(f.coord, 0);
    ASSERT_TRUE(attempt.has_value());
    ASSERT_EQ(Access::completions_seen(f.coord), 0u) << "queued, not yet run";
    ASSERT_TRUE(Access::find_any(f.coord, *attempt)->bind_open);

    // Seal ingress with the completion already queued ahead of the fence body.
    int fenced = 0;
    ASSERT_EQ(
        f.coord->fence([&fenced](FenceOutcome) { ++fenced; }),
        Fence::drain_queued);

    // ONE handler: the completion. Asserting here rather than after settling is
    // the whole point. The fence body terminalizes any live row, so by the end
    // the row is terminal and unsealed whether or not the completion dropped the
    // command — a probe of the guard changed nothing an end-state assertion could
    // see, and reported this control as vacuous. The difference exists only in
    // this window.
    ASSERT_EQ(f.step(), 1u);
    ASSERT_EQ(Access::completions_sealed(f.coord), 1u)
        << "PeerSet did produce a command, or nothing was dropped";
    auto* row = Access::find_any(f.coord, *attempt);
    ASSERT_TRUE(row != nullptr);
    EXPECT_EQ(row->phase, Phase::terminal)
        << "a command that can never be published must terminalize the row AS IT "
           "ARRIVES, not wait for the fence body to tidy up";
    EXPECT_FALSE(row->sealed.has_value())
        << "and must not be retained even briefly past the seal";
    EXPECT_EQ(f.coord->work_outstanding(), 0u)
        << "a retained command holds its work unit, which is what would hang the "
           "fence if a later row were still to arrive";

    f.settle();
    EXPECT_EQ(fenced, 1);
    EXPECT_EQ(f.coord->lifecycle(), Lifecycle::fenced);
}
