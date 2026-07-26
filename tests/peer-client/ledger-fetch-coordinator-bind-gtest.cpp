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
#include "peer-client-test-access.h"
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

    /// EVERY control checks the conservation invariant, without having to remember
    /// to. Three retained fence waiters were once delivered while holding no
    /// permits: the release path logged an underflow and returned, the assertions
    /// were all satisfied, and the breach was visible only to whoever read the
    /// test output. A per-test assertion would have to be added by hand to each new
    /// control; a destructor cannot be forgotten.
    ///
    /// EXPECT rather than ASSERT: ASSERT_* expands to `return`, which is
    /// ill-formed in a destructor.
    ~CoordFixture()
    {
        EXPECT_FALSE(coord && coord->accounting_broken())
            << "a conserved quantity was violated during this control";
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

    // A successful bind now flows straight into publication, so the row is past
    // `publishable` by the time this observes it. The fake accepts and never
    // answers, which is PeerClient's real duplicate-waiter behaviour, so it parks
    // in awaiting_receipt with its deadline armed.
    EXPECT_EQ(row->phase, Phase::awaiting_receipt);
    EXPECT_EQ(row->publish_attempts, 1u);
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

/// 5. THE control this whole slice exists for: a LOST completion post is
///    recovered by the coordinator's bind deadline.
///
/// Forced at the REAL boundary, in PeerSet's `deliver_seal_result`, which drops the
/// capability and posts nothing. That distinction is the whole control: the
/// previous version used a coordinator-side failpoint that swallowed the callback
/// AFTER it had already run, so the coordinator's own accounting settled and the
/// residue returned to zero. A real loss never invokes the callback at all, so the
/// projection stays permanently unobserved — and claiming the sender-side
/// simulation reproduced it was simply false.
TEST(LedgerFetchCoordinatorBind, LostCompletionPostIsRecoveredByTheBindDeadline)
{
    CoordFixture f;
    Access::set_bind_deadline(f.coord, std::chrono::milliseconds{0});
    // Exactly ONE completion is lost. No disarming, so no race with the retry.
    PeerSetTestAccess::drop_next_seal_completions(f.peers, 1);

    ASSERT_EQ(f.submit(), Ingress::reserved);
    f.settle();

    auto const attempt = Access::attempt_at(f.coord, 0);
    ASSERT_TRUE(attempt.has_value());
    EXPECT_EQ(Access::completions_seen(f.coord), 1u)
        << "the LOST completion never reaches the consumer; the retry's does";

    auto* row = Access::find_any(f.coord, *attempt);
    ASSERT_TRUE(row != nullptr);
    EXPECT_EQ(row->phase, Phase::awaiting_receipt)
        << "the deadline had to re-bind and the retry had to publish, or this row "
           "is still awaiting_bind";
    EXPECT_GE(row->bind_attempts, 2u);
    ASSERT_TRUE(row->sealed.has_value()) << "dereferenced below";
    EXPECT_EQ(row->sealed->attempt_id(), row->intent.attempt_id())
        << "recovery must re-bind the SAME attempt, not mint a new one";

    // The lost projection stays countable forever. That is the honest residue and
    // the reason `fenced` is not quiescence.
    EXPECT_EQ(f.coord->outstanding_bind_projections(), 1u)
        << "the retry was observed; the LOST one never will be";
    EXPECT_FALSE(f.coord->accounting_broken());
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
    EXPECT_EQ(row->phase, Phase::awaiting_receipt);
    EXPECT_EQ(row->bind_attempts, 1u);
    EXPECT_EQ(Access::completions_seen(f.coord), 1u);

    // The armed timer was cancelled as part of the settling commit, so further
    // polling cannot produce a second outcome.
    f.settle();
    EXPECT_EQ(row->bind_attempts, 1u);
    EXPECT_EQ(row->phase, Phase::awaiting_receipt);
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
    PeerSetTestAccess::drop_next_seal_completions(f.peers, 1);

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
        f.coord->fence([&fenced]() { ++fenced; }),
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
        f.coord->fence([&again]() { ++again; }),
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
        f.coord->fence([&fenced]() { ++fenced; }),
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
    // Publication refused, so the row terminalizes and owes the receiver nothing.
    // With publication ACCEPTED and unanswered the row would owe a receipt
    // settlement and the fence would correctly wait on it — which is the subject of
    // its own controls, not of this one.
    f.session->publish_accepts = false;
    Access::set_retry_backoff(f.coord, std::chrono::milliseconds{0});
    ASSERT_EQ(f.submit(), Ingress::reserved);
    f.settle();

    bool ran = false;
    ASSERT_EQ(
        f.coord->fence([&ran]() {
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
        f.coord->fence([&again]() { ++again; }),
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
        f.coord->fence([&fenced]() { ++fenced; }),
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
    // This control needs a row that is LIVE but owes the receiver nothing, so the
    // fence has something to terminalize and its own backpressure does not enter
    // into it. Publication refused, with a backoff long enough that the retry stays
    // pending: the row sits in `publishable`, which owes no receipt settlement.
    f.session->publish_accepts = false;
    Access::set_retry_backoff(f.coord, std::chrono::milliseconds{60000});
    ASSERT_EQ(f.submit(), Ingress::reserved);
    f.settle();
    ASSERT_EQ(Access::live_row_count(f.coord), 1u);

    Access::set_fence_projection_failure(f.coord, true);
    int fenced = 0;
    EXPECT_EQ(
        f.coord->fence([&fenced]() { ++fenced; }),
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
        f.coord->fence([&fenced]() { ++fenced; }),
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

    // Waiters only COEXIST while the fence cannot complete, and a single-threaded
    // caller cannot produce that: submit() posts its commit BEFORE any fence()
    // does, so the commit releases the last work unit and the fence completes in
    // its first body, leaving the rest to be answered on arrival. Each then gets
    // exactly one notification even if the container is clobbered on every insert,
    // which is why a probe that cleared the vector before each push changed nothing
    // and this control reported itself vacuous.
    //
    // In production the state comes from a submit on another thread. Running the
    // bodies directly forces that ordering. The PUBLIC bound on how many
    // projections can be queued is a different property, tested through fence()
    // itself in FenceProjectionBudgetBoundsThePublicBoundary.
    ASSERT_EQ(f.coord->fence(), Fence::drain_queued);
    ASSERT_TRUE(Access::begin_fence(f.coord, [&first]() { ++first; }));
    ASSERT_TRUE(Access::begin_fence(f.coord, [&second]() { ++second; }));
    ASSERT_TRUE(Access::begin_fence(f.coord, [&third]() { ++third; }));
    ASSERT_EQ(first + second + third, 0) << "all three are retained, not answered";

    f.settle();
    EXPECT_EQ(first, 1) << "the earliest waiter must not have been overwritten";
    EXPECT_EQ(second, 1);
    EXPECT_EQ(third, 1);
    EXPECT_EQ(f.coord->lifecycle(), Lifecycle::fenced);
    EXPECT_FALSE(f.coord->accounting_broken())
        << "three waiters were delivered while holding no permits, and the "
           "underflow was only a log line; this assertion is what stops a control "
           "passing over an invariant breach";
}

/// 23. The fence budget bounds the PUBLIC boundary, not just retained waiters.
///
/// The previous version applied the cap only when a fence body reached the strand,
/// so `fence()` could post arbitrarily many lambdas onto the io_context first and
/// the bound was decorative — and the control proved it via TestAccess, bypassing
/// the very API that was unbounded. A permit is now taken synchronously, before
/// anything is posted, so this control goes through `fence()` with the strand not
/// yet run at all.
TEST(LedgerFetchCoordinatorBind, FenceProjectionBudgetBoundsThePublicBoundary)
{
    CoordFixture f;
    int notified = 0;
    int refused_never_called = 0;

    // Nothing is polled in this block: every permit below is taken by the public
    // call itself, which is the property under test.
    for (std::size_t i = 0; i < LedgerFetchCoordinator::kMaxFenceWaiters; ++i)
    {
        ASSERT_EQ(
            f.coord->fence([&notified]() { ++notified; }),
            Fence::drain_queued)
            << "at i=" << i;
    }
    EXPECT_EQ(
        f.coord->fence([&refused_never_called]() { ++refused_never_called; }),
        Fence::capacity_exhausted)
        << "one past the budget must be refused BEFORE anything is queued";
    EXPECT_EQ(notified, 0) << "and none of this has run yet";
    EXPECT_EQ(f.coord->lifecycle(), Lifecycle::fencing)
        << "asserted BEFORE anything that can poll — see below";

    // Ingress is sealed even for the refused caller: sealing happens first and is
    // idempotent, so a refusal cannot leave the actor admitting new work.
    //
    // NOTE this call is not passive. submit() mints a pin through
    // PeerSetTestAccess, whose run_on_strand busy-waits while POLLING the
    // io_context, so it runs queued fence bodies as a side effect. The lifecycle
    // assertion above was originally after this line and failed reporting `fenced`
    // — the assertion had advanced the very state it was checking.
    EXPECT_EQ(f.submit(), Ingress::ingress_sealed);

    f.settle();
    EXPECT_EQ(f.coord->lifecycle(), Lifecycle::fenced);
    EXPECT_EQ(notified, static_cast<int>(LedgerFetchCoordinator::kMaxFenceWaiters))
        << "every accepted caller is notified exactly once";
    EXPECT_EQ(refused_never_called, 0)
        << "a caller told capacity_exhausted must NEVER be called back; that is "
           "what makes the synchronous refusal safe";

    // The budget is a live bound, not a one-shot: permits are released with their
    // dispositions, so a later caller is accepted again.
    int late = 0;
    EXPECT_EQ(f.coord->fence([&late]() { ++late; }), Fence::drain_queued);
    f.settle();
    EXPECT_EQ(late, 1);
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
        f.coord->fence([&late]() { ++late; }),
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
        f.coord->fence([&fenced]() { ++fenced; }),
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

/// 26. Projection accounting cannot be masked by failed initiations.
///
/// THE control Codex asked for, and it fails on the previous pin. The bind
/// initiation failpoint throws BEFORE PeerSet accepts anything, yet the catch
/// decremented a completion counter anyway. Two failed attempts gave
/// completed=2, projected=0, and the residue — computed as a saturating
/// difference — reported zero. A later REAL open projection then stayed masked
/// until projected caught up, which is the worst possible failure for a number
/// whose only job is to say "work is still outstanding".
///
/// So: exhaust initiation failures first, disarm, open a real projection, and
/// require it to be reported.
TEST(LedgerFetchCoordinatorBind, FailedInitiationsCannotMaskALaterOpenProjection)
{
    CoordFixture f;
    Access::set_max_bind_attempts(f.coord, 2);
    Access::set_bind_initiation_failure(f.coord, true);

    ASSERT_EQ(f.submit(), Ingress::reserved);
    f.settle();

    // Two attempts, both failing before PeerSet saw anything.
    auto const first = Access::attempt_at(f.coord, 0);
    ASSERT_TRUE(first.has_value());
    ASSERT_EQ(Access::find_any(f.coord, *first)->bind_attempts, 2u);
    EXPECT_EQ(f.coord->outstanding_bind_projections(), 0u)
        << "nothing was ever accepted, so nothing is outstanding";
    EXPECT_FALSE(f.coord->accounting_broken())
        << "and no violation: a projection that never opened must not be settled";

    // Now a REAL projection, left pending by not letting its completion run.
    Access::set_bind_initiation_failure(f.coord, false);
    ASSERT_EQ(f.submit(), Ingress::reserved);
    ASSERT_EQ(f.step(), 1u);  // commit + open bind; completion still queued

    EXPECT_EQ(f.coord->outstanding_bind_projections(), 1u)
        << "a real open projection MUST be visible; on the previous pin the "
           "earlier failed initiations had pushed the count negative and it "
           "reported zero here";
    EXPECT_FALSE(f.coord->accounting_broken());

    f.settle();
    EXPECT_EQ(f.coord->outstanding_bind_projections(), 0u)
        << "and returns to zero once observed";
    EXPECT_FALSE(f.coord->accounting_broken());
}

/// 27. A refused projection is not counted as outstanding either.
TEST(LedgerFetchCoordinatorBind, RefusedProjectionsAreNotCountedAsOutstanding)
{
    CoordFixture f;
    Access::set_max_bind_attempts(f.coord, 2);
    PeerSetTestAccess::set_seal_projection_failure(f.peers, true);

    ASSERT_EQ(f.submit(), Ingress::reserved);
    f.settle();
    EXPECT_EQ(f.coord->outstanding_bind_projections(), 0u)
        << "PeerSet returned false, so it holds no projection to complete";
    EXPECT_FALSE(f.coord->accounting_broken());
}

/// 28. The fence delivery buffers never reallocate, so nothing fallible happens
///     after the terminal fence commit.
///
/// The previous version cleared and RESERVED after storing `fenced_` — an
/// allocation past the point of no return. Had it thrown, every detached waiter
/// would have been destroyed without notification and the exception would have
/// escaped the strand handler. Delivery now swaps two preallocated buffers, which
/// `static_assert(noexcept(...))` proves cannot throw; this control proves the
/// capacity is genuinely stable across repeated fence cycles, so the swap is never
/// standing in for a hidden reallocation.
TEST(LedgerFetchCoordinatorBind, FenceDeliveryBuffersNeverReallocate)
{
    CoordFixture f;
    auto const capacity_before = Access::fence_buffer_capacities(f.coord);
    EXPECT_GE(capacity_before.first, LedgerFetchCoordinator::kMaxFenceWaiters);
    EXPECT_GE(capacity_before.second, LedgerFetchCoordinator::kMaxFenceWaiters);

    // Fill the budget, complete the fence, then do it again. If either buffer
    // reallocated, the capacity would move.
    int notified = 0;
    for (std::size_t i = 0; i < LedgerFetchCoordinator::kMaxFenceWaiters; ++i)
    {
        ASSERT_EQ(
            f.coord->fence([&notified]() { ++notified; }), Fence::drain_queued);
    }
    f.settle();
    ASSERT_EQ(notified, static_cast<int>(LedgerFetchCoordinator::kMaxFenceWaiters));

    for (std::size_t i = 0; i < LedgerFetchCoordinator::kMaxFenceWaiters; ++i)
    {
        ASSERT_EQ(
            f.coord->fence([&notified]() { ++notified; }), Fence::drain_queued);
    }
    f.settle();
    EXPECT_EQ(notified, static_cast<int>(LedgerFetchCoordinator::kMaxFenceWaiters) * 2);

    auto const capacity_after = Access::fence_buffer_capacities(f.coord);
    EXPECT_EQ(capacity_after, capacity_before)
        << "a changed capacity means an allocation happened on a path that must "
           "never allocate";
}

/// 29. The unobserved-projection residue saturates ABSORBINGLY.
///
/// The previous version was not absorbing and mutation 14 was not evidence for it:
/// that mutation exercises underflow, a different branch. At the ceiling,
/// `note_projection_opened` marked accounting broken and returned WITHOUT counting
/// the new projection, while `note_projection_observed` still decremented from the
/// ceiling — so the residue drained back down, and with enough observations would
/// have reported zero while uncounted work was outstanding. A false zero reached
/// from the opposite direction to the wrap it was written to prevent.
///
/// Driven directly to the ceiling through the seam, exactly like the
/// sender-generation ceiling control, because reaching it honestly needs 2^64
/// lost completions.
TEST(LedgerFetchCoordinatorBind, ProjectionResidueSaturatesAbsorbingly)
{
    CoordFixture f;
    constexpr auto kCeiling = LedgerFetchCoordinator::kMaxOutstandingProjections;

    // One below: a normal open still counts, and nothing is broken yet.
    Access::set_outstanding_projections(f.coord, kCeiling - 1);
    Access::note_projection_opened(f.coord);
    EXPECT_EQ(f.coord->outstanding_bind_projections(), kCeiling);
    EXPECT_FALSE(f.coord->accounting_broken());

    // At the ceiling: the next open cannot be counted, so it saturates.
    Access::note_projection_opened(f.coord);
    EXPECT_TRUE(f.coord->accounting_broken())
        << "an uncountable projection is an accounting failure, not a no-op";
    EXPECT_EQ(f.coord->outstanding_bind_projections(), kCeiling);

    // THE point: observations must not drain it. Ten of them, because the old
    // behaviour decremented once per observation and would have looked fine after
    // one.
    for (int i = 0; i < 10; ++i)
    {
        Access::note_projection_observed(f.coord);
        ASSERT_EQ(f.coord->outstanding_bind_projections(), kCeiling)
            << "residue drained at observation " << i
            << "; saturation must be absorbing, not merely recorded";
    }
    EXPECT_NE(f.coord->outstanding_bind_projections(), 0u)
        << "a saturated residue must never report zero — that is the one answer "
           "this diagnostic exists to make impossible";

    // The fixture destructor asserts !accounting_broken(), which this control
    // deliberately violates, so clear the expectation it is checking.
    Access::clear_accounting_broken(f.coord);
}

// ---------------------------------------------------------------------------
// Publication and receipt disposition.
//
// These publish across the EXACT production boundary: the pinned session is a
// real PeerClient, and the receipts are real receipts it minted. That is not
// thoroughness for its own sake — `NodeBatchWriteReceipt`'s only friend is
// PeerClient, deliberately, and unlike every sibling capability not even
// PeerClientTestAccess can mint one. A fake session therefore CANNOT deliver a
// receipt, and the alternative would have been widening that friendship to a test
// seam, which the canonical cut lists among its refusals.
//
// One thing these controls have to align by hand, and it is the documented open
// boundary rather than a fixture convenience: PeerSet mints the peer generation
// and PeerClient holds its own `connection_generation_`, and publication is
// refused unless they are equal. The canonical cut names exactly this as "an
// unnamed PeerSet-to-PeerClient construction boundary" and says not to rename the
// bare token into apparent exactness. So the fixture makes them equal explicitly
// and says why, rather than implying the production path already guarantees it.
// ---------------------------------------------------------------------------

namespace {

struct PublishFixture
{
    asio::io_context io;
    std::shared_ptr<PeerSet> peers;
    std::shared_ptr<PeerClient> client;
    std::shared_ptr<LedgerFetchCoordinator> coord;
    std::uint64_t generation = 0;

    /// `client_generation` is what makes or breaks publication: PeerClient refuses
    /// any command whose peer generation is not its own. Default 1 matches what a
    /// fresh PeerSet mints; pass anything else to exercise the refusal.
    /// A connection is required for the write to be ADMITTED rather than refused.
    /// Without one the receiver answers `disconnected` with commit sequence zero,
    /// which never reaches the acknowledgement path at all — so the controls that
    /// matter most would have been exercising nothing.
    std::optional<asio::ssl::context> ssl;

    explicit PublishFixture(
        std::uint64_t client_generation = 1,
        bool with_connection = false)
    {
        peers = PeerSet::create(
            io,
            PeerSetOptions{
                .network_id = 0,
                .max_in_flight_connects = 0,
                .max_in_flight_crawls = 0,
            });
        // A fresh PeerSet mints generation 1 first, so the client is built to
        // match. See the note above: this equality is the open construction
        // boundary, made explicit here.
        client = PeerClientTestAccess::make(io, State::Ready, client_generation);
        if (with_connection)
        {
            ssl.emplace(asio::ssl::context::tlsv12_client);
            PeerClientTestAccess::seed_open_connection(*client, *ssl);
        }
        generation =
            PeerSetTestAccess::insert_ready_peer_with_generation(io, peers, client);
        coord = LedgerFetchCoordinator::create(io, peers);
    }

    Ingress
    submit()
    {
        return coord->submit_node_batch_on_pin(
            CoordFixture::ledger_hash(),
            NodeTree::account_state,
            CoordFixture::nodes(),
            PeerSetTestAccess::make_peer_pin_for_session(io, peers, client));
    }

    /// Run exactly one ready handler, so a control can observe a row mid-flight.
    std::size_t
    step()
    {
        io.restart();
        return io.poll_one();
    }

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

    ~PublishFixture()
    {
        EXPECT_FALSE(coord && coord->accounting_broken())
            << "a conserved quantity was violated during this control";
    }
};

}  // namespace

/// 30. The generations agree, so publication is accepted and a real receipt comes
///     back to the coordinator strand.
TEST(LedgerFetchCoordinatorPublish, AlignedGenerationPublishesAndReceiptReturns)
{
    PublishFixture f;
    ASSERT_EQ(f.generation, 1u) << "the fixture's whole premise";
    ASSERT_EQ(f.submit(), Ingress::reserved);
    f.settle();

    auto const attempt = Access::attempt_at(f.coord, 0);
    ASSERT_TRUE(attempt.has_value());
    auto* row = Access::find_any(f.coord, *attempt);
    ASSERT_TRUE(row != nullptr);

    EXPECT_GE(row->publish_attempts, 1u) << "publication was actually attempted";
    ASSERT_TRUE(row->receipt.has_value())
        << "a real PeerClient must have produced a terminal receipt";
    EXPECT_EQ(row->receipt->attempt_id(), row->intent.attempt_id())
        << "the receipt names OUR attempt, not one derived downstream";
    EXPECT_EQ(row->receipt->sender_generation(), f.coord->sender_generation());
    EXPECT_EQ(row->receipt->peer_generation().value(), f.generation);
    EXPECT_EQ(row->receipt->payload_digest(), row->intent.payload_digest());
    EXPECT_FALSE(row->publish_open) << "the disposition is committed exactly once";
}

/// 31. A receipt naming no retained receiver row is NOT acknowledged.
///
/// PeerClient rejects any ack whose receiver_commit_sequence is zero, and an
/// admission refusal carries exactly that. Attempting one would be a sender
/// claiming to retire receiver state that never existed.
TEST(LedgerFetchCoordinatorPublish, ReceiptWithoutARetainedRowIsNotAcknowledged)
{
    // A client whose connection generation is NOT the one PeerSet minted. It is
    // still Ready, so PeerSet pins it — a non-ready session cannot be pinned at
    // all, which is why that route to a refusal does not exist. The receiver then
    // refuses on exact generation, producing a terminal receipt with sequence zero
    // and nothing to retire.
    //
    // This is the unnamed PeerSet-to-PeerClient construction boundary being
    // ENFORCED rather than assumed: nothing in the production path guarantees the
    // two generations agree, and when they do not, publication is refused exactly.
    PublishFixture f{99};
    ASSERT_NE(f.generation, 99u) << "the mismatch is the premise";
    ASSERT_EQ(f.submit(), Ingress::reserved);
    f.settle();

    auto const attempt = Access::attempt_at(f.coord, 0);
    ASSERT_TRUE(attempt.has_value());
    auto* row = Access::find_any(f.coord, *attempt);
    ASSERT_TRUE(row != nullptr);

    ASSERT_TRUE(row->receipt.has_value());
    EXPECT_EQ(row->receipt->receiver_commit_sequence(), 0u)
        << "an admission refusal names no retained row";
    EXPECT_EQ(row->receipt->outcome(), NodeBatchWriteOutcome::generation_mismatch)
        << "refused on exact generation by the receiver that owns that fact";
    EXPECT_EQ(row->ack_attempts, 0u)
        << "nothing may be acknowledged for a row that was never retained";
    EXPECT_EQ(row->phase, Phase::terminal);
    EXPECT_EQ(f.coord->receipt_obligations(), 0u)
        << "and no obligation is retained, because the receiver holds nothing";
    EXPECT_EQ(f.coord->work_outstanding(), 0u);
}

/// 32. The row stops projecting BEFORE the acknowledgement — the canonical
///     ordering rule, asserted rather than asserted-about.
///
/// "One coordinator executor must stop projecting a command before consuming its
/// receipt and posting the acknowledgement... a post-ack replay is forbidden at
/// its source." Source-forbidden means the row is not in a publishable or
/// awaiting_receipt phase at the moment the ack is minted, so no code path can
/// republish it.
TEST(LedgerFetchCoordinatorPublish, ProjectionStopsBeforeAcknowledgement)
{
    PublishFixture f;
    ASSERT_EQ(f.submit(), Ingress::reserved);
    f.settle();

    auto const attempt = Access::attempt_at(f.coord, 0);
    ASSERT_TRUE(attempt.has_value());
    auto* row = Access::find_any(f.coord, *attempt);
    ASSERT_TRUE(row != nullptr);

    // Whatever the outcome, publication is closed and the sealed command has been
    // released, so there is nothing left to project.
    EXPECT_FALSE(row->publish_open);
    EXPECT_NE(row->phase, Phase::publishable);
    EXPECT_NE(row->phase, Phase::awaiting_receipt);
    EXPECT_FALSE(row->sealed.has_value())
        << "a retained command is a republication waiting to happen; terminal rows "
           "must not hold one";
}

/// 33. Publication refused at initiation is a bounded coordinator retry.
TEST(LedgerFetchCoordinatorPublish, RefusedPublicationRetriesThenTerminalizes)
{
    PublishFixture f;
    Access::set_publish_initiation_failure(f.coord, true);
    Access::set_max_publish_attempts(f.coord, 2);
    // Retry is deferred on an owned timer now, so a poll-driven control needs a
    // zero backoff for it to fire at all.
    Access::set_retry_backoff(f.coord, std::chrono::milliseconds{0});

    ASSERT_EQ(f.submit(), Ingress::reserved);
    EXPECT_NO_THROW(f.settle());

    auto const attempt = Access::attempt_at(f.coord, 0);
    ASSERT_TRUE(attempt.has_value());
    auto* row = Access::find_any(f.coord, *attempt);
    ASSERT_TRUE(row != nullptr);

    EXPECT_EQ(row->publish_attempts, 2u) << "exactly the budget, no spinning";
    EXPECT_EQ(row->phase, Phase::terminal);
    EXPECT_FALSE(row->receipt.has_value());
    EXPECT_FALSE(static_cast<bool>(row->pin)) << "terminal releases the retention";
    EXPECT_EQ(f.coord->work_outstanding(), 0u);
}

/// 34. THE control for this slice: an ADMITTED write yields a receipt naming a
///     retained receiver row, and the coordinator retires it with an ack.
///
/// Everything before this exercised refusals, whose receipts carry commit sequence
/// zero and are terminal with nothing to retire — so none of them reached
/// `acknowledge_on_strand` at all. Without a real connection the receiver answers
/// `disconnected` and the acknowledgement path, which is the entire point of the
/// slice, would have been dead code behind green tests.
TEST(LedgerFetchCoordinatorPublish, AdmittedWriteIsAcknowledgedAndRetiresTheRow)
{
    PublishFixture f{1, /*with_connection=*/true};
    ASSERT_EQ(f.submit(), Ingress::reserved);
    f.settle();

    auto const attempt = Access::attempt_at(f.coord, 0);
    ASSERT_TRUE(attempt.has_value());
    auto* row = Access::find_any(f.coord, *attempt);
    ASSERT_TRUE(row != nullptr);
    ASSERT_TRUE(row->receipt.has_value());

    EXPECT_NE(row->receipt->receiver_commit_sequence(), 0u)
        << "an admitted write must name a RETAINED receiver row, or there is "
           "nothing for an acknowledgement to retire";
    EXPECT_NE(row->receipt->receiver_instance_id(), 0u);
    EXPECT_EQ(row->ack_attempts, 1u)
        << "exactly one acknowledgement, accepted first time";
    EXPECT_EQ(row->phase, Phase::terminal);
    EXPECT_EQ(f.coord->receipt_obligations(), 0u)
        << "the receiver row was retired, so nothing is still owed";

    // Terminal releases everything the row owned.
    EXPECT_FALSE(static_cast<bool>(row->pin));
    EXPECT_FALSE(row->sealed.has_value());
    EXPECT_EQ(f.coord->work_outstanding(), 0u);
    EXPECT_FALSE(f.coord->accounting_broken());
}

/// 35. An acknowledgement that cannot be queued is retried, and its exhaustion
///     leaves the receiver row COUNTABLE rather than silently orphaned.
TEST(LedgerFetchCoordinatorPublish, ExhaustedAcknowledgementCountsTheOrphanedRow)
{
    PublishFixture f{1, /*with_connection=*/true};
    Access::set_max_ack_attempts(f.coord, 2);
    // Zero backoff, because the acknowledgement retry is now deferred on the row's
    // owned timer and a poll-driven control advances no clock.
    Access::set_retry_backoff(f.coord, std::chrono::milliseconds{0});
    // Forced on OUR side, deliberately. The receiver's ingress projection gates
    // publication and acknowledgement with one bound, so saturating it refuses the
    // write and the row never reaches an acknowledgement to refuse. And a false
    // return is the whole observable here — no callback, no receiver state change —
    // so this is faithful in a way the sender-side lost-completion simulation was
    // not.
    Access::fail_next_acknowledgements(f.coord, 99);

    ASSERT_EQ(f.submit(), Ingress::reserved);
    f.settle();

    auto const attempt = Access::attempt_at(f.coord, 0);
    ASSERT_TRUE(attempt.has_value());
    auto* row = Access::find_any(f.coord, *attempt);
    ASSERT_TRUE(row != nullptr);

    // RETAINED, not counted-and-erased. The previous version bumped a scalar in
    // the same breath as destroying the row, so the moment it read non-zero the
    // identity it referred to was already gone — a count of things nobody can name.
    EXPECT_EQ(row->phase, Phase::unsettled)
        << "the receiver holds a row only this identity can retire, so this row "
           "must survive";
    EXPECT_FALSE(row->retired);
    EXPECT_EQ(f.coord->receipt_obligations(), 1u)
        << "and the obligation is visible, backed by that row";
    EXPECT_TRUE(static_cast<bool>(row->pin))
        << "a later repair needs the retention, so it is not released";
    EXPECT_EQ(f.coord->work_outstanding(), 1u)
        << "the work unit is held, which is what makes a fence backpressure "
           "instead of reporting a drain it has not performed";
}

/// 36. The acknowledgement path REFUSES to run on a row that is still projecting.
///
/// Control 32 asserts the end state, and by then the row is terminal whichever
/// order things happened in — a probe that deleted the phase advance was caught
/// only incidentally, by other controls whose acks then never fired. This asserts
/// the guard itself: driven directly with the row mid-publication, nothing is
/// acknowledged. That is what "forbidden at its source" has to mean to be a
/// property of the code rather than of the sequence a test happened to produce.
TEST(LedgerFetchCoordinatorPublish, AcknowledgementRefusesWhileStillProjecting)
{
    PublishFixture f{1, /*with_connection=*/true};
    ASSERT_EQ(f.submit(), Ingress::reserved);

    // Step until the row is mid-publication. Stepping until the PROPERTY holds
    // rather than counting handlers: the seal takes two strand hops before
    // publication even begins, so a fixed count is both brittle and less
    // informative than the condition it is standing in for.
    // Access::Row, not the coordinator's private nested type: the row is private
    // and only the declared test seam may name it.
    Access::Row* row = nullptr;
    std::optional<TransportAttemptId> attempt;
    for (int i = 0; i < 16; ++i)
    {
        if (f.step() == 0)
        {
            break;
        }
        attempt = Access::attempt_at(f.coord, 0);
        if (!attempt)
        {
            continue;
        }
        row = Access::find_any(f.coord, *attempt);
        if (row != nullptr && row->phase == Phase::awaiting_receipt)
        {
            break;
        }
    }
    ASSERT_TRUE(attempt.has_value());
    ASSERT_TRUE(row != nullptr);
    ASSERT_EQ(row->phase, Phase::awaiting_receipt) << "still projecting";
    ASSERT_TRUE(row->publish_open);

    // Force the acknowledgement path against a row that has not stopped
    // projecting. It must decline.
    Access::acknowledge(f.coord, *attempt);
    EXPECT_EQ(row->ack_attempts, 0u)
        << "acknowledging a row that could still republish is exactly the "
           "post-ack replay the canonical cut forbids at its source";
    EXPECT_FALSE(row->receipt.has_value());

    // And the ordinary path still completes, so the refusal above is the guard
    // rather than a broken fixture.
    f.settle();
    EXPECT_EQ(row->ack_attempts, 1u);
    EXPECT_EQ(row->phase, Phase::terminal);
}

// ---------------------------------------------------------------------------
// Corrections to publication/receipt ownership. Each control below exists
// because a specific defect was named, not because a shape looked untested.
// ---------------------------------------------------------------------------

/// 37. A refused PUBLICATION yields to the receiver and a later attempt wins.
///
/// Against the REAL receiver bound: its node-batch ingress projection is bounded
/// before the strand runs, so filling it makes `submit_node_batch` return false.
/// That is exactly the transient condition the old direct recursion mishandled —
/// it burned the whole attempt budget before the receiver could drain one item.
TEST(LedgerFetchCoordinatorPublish, RefusedPublicationYieldsThenSucceeds)
{
    PublishFixture f{1, /*with_connection=*/true};
    Access::set_max_publish_attempts(f.coord, 6);
    Access::set_retry_backoff(f.coord, std::chrono::milliseconds{0});

    // Saturate BEFORE submitting, so the receiver refuses admission on its own
    // bound rather than on anything simulated here.
    PeerClientTestAccess::seed_node_batch_ingress_saturated(*f.client);
    ASSERT_EQ(f.submit(), Ingress::reserved);

    // STEPPED, not settled. Settling to quiescence would run every retry the
    // backoff allows and prove nothing about separation — the property is that one
    // strand pass makes ONE attempt, which is what direct recursion violated.
    Access::Row* row = nullptr;
    std::optional<TransportAttemptId> attempt;
    for (int i = 0; i < 64; ++i)
    {
        if (f.step() == 0)
        {
            break;
        }
        attempt = Access::attempt_at(f.coord, 0);
        if (!attempt)
        {
            continue;
        }
        row = Access::find_any(f.coord, *attempt);
        if (row != nullptr && row->publish_attempts >= 1)
        {
            break;
        }
    }
    ASSERT_TRUE(row != nullptr);
    ASSERT_EQ(row->publish_attempts, 1u)
        << "ONE attempt per strand pass. Direct recursion would already have spent "
           "the whole budget here, which is the defect being corrected";
    EXPECT_FALSE(row->receipt.has_value()) << "refused by the real receiver bound";

    // Drain the receiver, exactly as its own strand does, and let the owned retry
    // timer fire.
    PeerClientTestAccess::drain_node_batch_ingress(*f.client);
    f.settle();

    ASSERT_TRUE(row->receipt.has_value())
        << "a later attempt reached the receiver, which is what yielding buys";
    EXPECT_NE(row->receipt->receiver_commit_sequence(), 0u)
        << "and was admitted to a retained row";
    EXPECT_EQ(row->phase, Phase::terminal) << "then acknowledged and retired";
    EXPECT_EQ(f.coord->receipt_obligations(), 0u);
    EXPECT_EQ(f.coord->work_outstanding(), 0u);
    EXPECT_FALSE(f.coord->accounting_broken());
}

/// 38. A refused ACKNOWLEDGEMENT yields between attempts.
///
/// Forced with a counted failpoint rather than the real bound, and the reason is
/// worth stating: the receiver's ingress projection gates publication and
/// acknowledgement with ONE bound, so saturating it refuses the write and no row
/// ever reaches an acknowledgement. Control 37 exercises that same real bound where
/// it IS reachable. Here the observable is only a `false` return — no callback, no
/// receiver state change — so a counted refusal is faithful.
///
/// The property under test is that attempts are SEPARATED, not merely bounded: each
/// retry is a distinct strand handler on an owned timer, so one step advances the
/// count by one. Direct recursion advanced it to the budget in a single step.
TEST(LedgerFetchCoordinatorPublish, RefusedAcknowledgementYieldsBetweenAttempts)
{
    PublishFixture f{1, /*with_connection=*/true};
    Access::set_max_ack_attempts(f.coord, 5);
    Access::set_retry_backoff(f.coord, std::chrono::milliseconds{0});
    Access::fail_next_acknowledgements(f.coord, 2);

    ASSERT_EQ(f.submit(), Ingress::reserved);

    // Step to the first refused acknowledgement.
    Access::Row* row = nullptr;
    std::optional<TransportAttemptId> attempt;
    for (int i = 0; i < 64; ++i)
    {
        if (f.step() == 0)
        {
            break;
        }
        attempt = Access::attempt_at(f.coord, 0);
        if (!attempt)
        {
            continue;
        }
        row = Access::find_any(f.coord, *attempt);
        if (row != nullptr && row->ack_attempts >= 1)
        {
            break;
        }
    }
    ASSERT_TRUE(row != nullptr);
    ASSERT_EQ(row->ack_attempts, 1u)
        << "ONE attempt per strand pass. Direct recursion would already have "
           "spent the whole budget here, which is the defect being corrected";
    EXPECT_EQ(row->phase, Phase::acknowledging) << "still owed, not terminalized";

    f.settle();
    EXPECT_EQ(row->ack_attempts, 3u) << "two refused, the third accepted";
    EXPECT_EQ(row->phase, Phase::terminal);
    EXPECT_EQ(f.coord->receipt_obligations(), 0u);
    EXPECT_FALSE(f.coord->accounting_broken());
}

/// 39. A fence BACKPRESSURES on a retained receipt obligation instead of
///     reporting a drain it has not performed.
///
/// The defect: the fence terminalized every live row. A row whose publication had
/// been accepted may already have created a replayable receiver row, so erasing it
/// discarded the only authority able to mint that row's exact acknowledgement — the
/// receiver retains state nobody can retire. T62 removes S40/S41 only once S42/S44
/// are empty; T69 drains rather than erases.
TEST(LedgerFetchCoordinatorPublish, FenceWaitsOnARetainedReceiptObligation)
{
    PublishFixture f{1, /*with_connection=*/true};
    Access::set_retry_backoff(f.coord, std::chrono::milliseconds{0});
    Access::set_max_ack_attempts(f.coord, 1);
    Access::fail_next_acknowledgements(f.coord, 99);

    ASSERT_EQ(f.submit(), Ingress::reserved);
    f.settle();

    auto const attempt = Access::attempt_at(f.coord, 0);
    ASSERT_TRUE(attempt.has_value());
    auto* row = Access::find_any(f.coord, *attempt);
    ASSERT_TRUE(row != nullptr);
    ASSERT_EQ(row->phase, Phase::unsettled) << "the obligation is retained";
    ASSERT_EQ(f.coord->receipt_obligations(), 1u);

    int fenced = 0;
    ASSERT_EQ(f.coord->fence([&fenced]() { ++fenced; }), Fence::drain_queued);
    f.settle();

    EXPECT_EQ(fenced, 0)
        << "a fence completing here would report a drain it has not performed, "
           "while the receiver still holds a row only this identity can retire";
    EXPECT_EQ(f.coord->lifecycle(), Lifecycle::fencing);
    EXPECT_EQ(row->phase, Phase::unsettled);
    EXPECT_FALSE(row->retired) << "erasing this was the defect";
    EXPECT_EQ(row->intent.attempt_id(), *attempt)
        << "identity-bearing: the row still names the attempt it owes";
    EXPECT_TRUE(static_cast<bool>(row->pin));
    EXPECT_EQ(f.coord->work_outstanding(), 1u)
        << "the held work unit is the mechanism of the backpressure";
    EXPECT_FALSE(f.coord->accounting_broken());
}

/// 40. The obligation projection is derived from owned rows, so it cannot be
///     non-zero without a row behind it, and cannot wrap.
///
/// It replaces a counter that was incremented in the same two paths that destroyed
/// the row — so at the moment it read non-zero, the identity it referred to was
/// already gone. A count of things nobody can name is not authority.
TEST(LedgerFetchCoordinatorPublish, ObligationCountIsBackedByNameableRows)
{
    PublishFixture f{1, /*with_connection=*/true};
    Access::set_retry_backoff(f.coord, std::chrono::milliseconds{0});
    Access::set_max_ack_attempts(f.coord, 1);
    // Counted refusal, for the reason given on control 38: the receiver's one
    // ingress bound gates publication too, so saturating it never reaches an ack.
    Access::fail_next_acknowledgements(f.coord, 99);

    EXPECT_EQ(f.coord->receipt_obligations(), 0u);
    ASSERT_EQ(f.submit(), Ingress::reserved);
    f.settle();

    auto const obligations = f.coord->receipt_obligations();
    ASSERT_EQ(obligations, 1u);

    // Every counted obligation is a row that can be named and inspected.
    std::size_t owing = 0;
    for (std::size_t i = 0; i < Access::row_count(f.coord); ++i)
    {
        auto const at = Access::attempt_at(f.coord, i);
        ASSERT_TRUE(at.has_value());
        auto* row = Access::find_any(f.coord, *at);
        ASSERT_TRUE(row != nullptr);
        if (row->phase == Phase::awaiting_receipt ||
            row->phase == Phase::acknowledging || row->phase == Phase::unsettled)
        {
            ++owing;
            EXPECT_NE(row->intent.attempt_id().value(), 0u)
                << "an obligation must name the attempt it owes";
        }
    }
    EXPECT_EQ(owing, obligations)
        << "the projection must equal the rows it claims to be counting";

    // Letting the acknowledgement succeed retires it, and the projection returns to
    // zero without ever having been decremented — it is recomputed from the rows.
    Access::fail_next_acknowledgements(f.coord, 0);
    Access::set_max_ack_attempts(f.coord, 8);
    auto* owed = Access::find_any(f.coord, *Access::attempt_at(f.coord, 0));
    ASSERT_TRUE(owed != nullptr);
    Access::set_phase_acknowledging(f.coord, *Access::attempt_at(f.coord, 0));
    Access::acknowledge(f.coord, *Access::attempt_at(f.coord, 0));
    f.settle();
    EXPECT_EQ(f.coord->receipt_obligations(), 0u)
        << "recomputed from rows, so retiring the row retires the count";
}

/// 41. An accepted publication with NO receipt inside its deadline retains the row.
///
/// The deadline path reaches `unsettled` by a different route than the
/// acknowledgement path, and control 39 only covers the latter — a probe that
/// swapped this branch back to `terminalize` changed nothing control 39 could see.
///
/// The fake session accepts publication and never answers, which is exactly
/// PeerClient's duplicate-waiter behaviour: admitted, no receipt, replay expected.
/// So the disposition is genuinely UNKNOWN — the receiver may hold a row — and
/// erasing the sender identity here is the defect.
TEST(LedgerFetchCoordinatorBind, UnknownPublicationDispositionRetainsTheRow)
{
    CoordFixture f;
    ASSERT_TRUE(f.session->publish_accepts) << "accepted, and never answered";
    Access::set_publish_deadline(f.coord, std::chrono::milliseconds{0});
    Access::set_max_publish_attempts(f.coord, 1);
    Access::set_retry_backoff(f.coord, std::chrono::milliseconds{0});

    ASSERT_EQ(f.submit(), Ingress::reserved);
    f.settle();

    auto const attempt = Access::attempt_at(f.coord, 0);
    ASSERT_TRUE(attempt.has_value());
    auto* row = Access::find_any(f.coord, *attempt);
    ASSERT_TRUE(row != nullptr);

    EXPECT_GE(f.session->publish_calls, 1u) << "the publication was accepted";
    EXPECT_FALSE(row->receipt.has_value()) << "and never answered";
    EXPECT_EQ(row->phase, Phase::unsettled)
        << "an UNKNOWN disposition must retain the row: the receiver may hold a "
           "replayable row only this identity can retire";
    EXPECT_FALSE(row->retired);
    EXPECT_TRUE(static_cast<bool>(row->pin)) << "and the retention a repair needs";
    EXPECT_EQ(f.coord->receipt_obligations(), 1u);
    EXPECT_EQ(f.coord->work_outstanding(), 1u)
        << "held, so a fence backpressures rather than reporting a false drain";
    EXPECT_FALSE(f.coord->accounting_broken());
}
