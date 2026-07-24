// Negative controls for the composed-review's four bad sequences
// FIRST, then the full three-phase round trip with loss/replay at
// every phase.

#include "coordinator-seams.h"
#include "operation-ledger.h"

#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

#include <optional>
#include <utility>

using namespace catl::provider_lab;
namespace asio = boost::asio;

namespace {

PartitionScope const kScope{1, 1};
ProviderEpochId const kEpoch = 5;

struct SeamsFixture : ::testing::Test
{
    DeterministicScheduler sched;
    DeterministicScheduler::strand_type strand = sched.make_strand();
    InterestRegistry interests{strand, kScope};
    ParticipantCellRegistry cells{strand, kScope};
    CoordinatorInterestSeams seams{interests, cells};
    OperationLedger ledger{strand, kScope};

    template <typename Fn>
    void
    run(Fn fn)
    {
        asio::post(strand, std::move(fn));
        sched.run_until_idle();
    }

    struct Sponsored
    {
        InterestId interest;
        CellRevision revision;
        GrantId grant;
    };

    /// Open the cell FIRST (mints the revision), then register the
    /// interest with that exact expectation — the composed order.
    Sponsored
    sponsored_interest(
        InterestOperationId operation = 100,
        InterestChildId child = 7)
    {
        auto open = cells.open_cell(/*interest*/ next_interest_guess(),
                                    10, operation, kEpoch);
        EXPECT_EQ(open.refusal, ParticipantCellRegistry::Refusal::ok);
        auto r = interests.register_interest(
            operation, child, 10, 900,
            ParticipantDestination{kScope, kEpoch, open.revision});
        EXPECT_EQ(r.refusal, InterestRegistry::Refusal::ok);
        auto g = cells.issue_grant(r.interest, open.revision);
        EXPECT_EQ(g.refusal, ParticipantCellRegistry::Refusal::ok);
        return {r.interest, open.revision, g.grant};
    }

    // The cell is keyed by InterestId and must be opened before
    // registration; predict the next mint (registry starts at 1).
    InterestId
    next_interest_guess()
    {
        return ++registered_;
    }

    InterestId registered_ = 0;
};

using SR = CoordinatorInterestSeams::Refusal;
using IR = InterestRegistry::Refusal;
using IJ = InterestRegistry::JoinRefusal;
using CR = ParticipantCellRegistry::Refusal;
using G = ParticipantCellRegistry::GrantState;
using AO = OperationLedger::AckOutcome;

}  // namespace

// Bad sequence 1 (review F1): a delayed command minted for a retired
// cell instance must not acquire authority over the reopened one.
TEST_F(SeamsFixture, BadSequence1_StaleRevokeAfterReopenRefuses)
{
    run([&] {
        auto s = sponsored_interest();
        ASSERT_EQ(seams.sponsor_use(s.interest, s.revision, s.grant),
                  SR::ok);
        auto d = interests.begin_detach(s.interest);
        ASSERT_EQ(d.refusal, IR::ok);
        auto stale = *d.command;  // the delayed copy, genuinely minted

        // Drive instance R1 to full retirement through the seams.
        ASSERT_EQ(seams.finish_use(s.interest, s.revision, s.grant),
                  SR::ok);
        auto revoke = cells.revoke_cell(stale);
        ASSERT_EQ(revoke.refusal, CR::ok);
        auto consumed = interests.consume_revoke_receipt(*revoke.receipt);
        ASSERT_EQ(consumed.refusal, IR::ok);
        ASSERT_EQ(cells.retire_cell(*consumed.consumed), CR::ok);

        // Reopen the SAME interest key as instance R2.
        auto reopen = cells.open_cell(s.interest, 10, 100, kEpoch);
        ASSERT_EQ(reopen.refusal, CR::ok);
        ASSERT_GT(reopen.revision, s.revision);

        // The delayed R1 command arrives.
        EXPECT_EQ(cells.revoke_cell(stale).refusal, CR::wrong_binding)
            << "a stale command has no authority over a fresh instance";
        EXPECT_EQ(cells.issue_grant(s.interest, reopen.revision).refusal,
                  CR::ok)
            << "R2 remains fully active";
    });
}

// Bad sequence 2 (review F1): registry instances with colliding local
// counters must refuse each other's artifacts on scope binding.
TEST_F(SeamsFixture, BadSequence2_CrossPartitionArtifactsRefuse)
{
    // Partition B, its own strand and stack, identical local values.
    DeterministicScheduler s2;
    auto strand2 = s2.make_strand();
    PartitionScope const scope_b{1, 2};
    InterestRegistry interests_b{strand2, scope_b};
    ParticipantCellRegistry cells_b{strand2, scope_b};
    std::optional<RevokeCommand> cmd_b;
    std::optional<ParticipantRevokedReceipt> receipt_b;
    asio::post(strand2, [&] {
        auto open = cells_b.open_cell(1, 10, 100, kEpoch);
        ASSERT_EQ(open.refusal, CR::ok);
        auto r = interests_b.register_interest(
            100, 7, 10, 900,
            ParticipantDestination{scope_b, kEpoch, open.revision});
        ASSERT_EQ(r.refusal, IR::ok);
        auto d = interests_b.begin_detach(r.interest);
        ASSERT_EQ(d.refusal, IR::ok);
        cmd_b = d.command;
        auto rv = cells_b.revoke_cell(*cmd_b);
        ASSERT_EQ(rv.refusal, CR::ok);
        receipt_b = rv.receipt;
    });
    s2.run_until_idle();

    run([&] {
        auto s = sponsored_interest();
        ASSERT_EQ(s.interest, 1u)
            << "the collision is real: both partitions minted "
               "interest 1 with identical local counters";
        ASSERT_EQ(interests.begin_detach(s.interest).refusal, IR::ok);

        // B's command against A's cell; B's receipt against A's
        // interest: refused on the scope carried in the binding.
        EXPECT_EQ(cells.revoke_cell(*cmd_b).refusal, CR::wrong_binding);
        EXPECT_EQ(
            interests.consume_revoke_receipt(*receipt_b).refusal,
            IR::wrong_binding);

        // B's terminal-phase artifacts are equally inert here.
        EXPECT_EQ(
            ledger
                .consume_terminal_ack(
                    TerminalAckTestMint::mint(scope_b, 1, 100, 7), 9, 4)
                .outcome,
            AO::wrong_scope);
        EXPECT_EQ(
            interests.retire_terminal(
                TerminalConsumedTestMint::mint(scope_b, 1, 100, 7)),
            IR::not_detach_pending)
            << "not terminal-ready, and the scope would refuse anyway";
    });
}

// Bad sequence 3 (review F2): neither outbox can be destroyed by its
// own outgoing message. (The wrong-type forms no longer compile; the
// runtime bindings are exercised in the round trip below and the
// component suites.) Here: the interest cannot be removed before the
// ledger consumed its ack, and the cell cannot be removed before the
// interest consumed its receipt.
TEST_F(SeamsFixture, BadSequence3_ProducerHomesSurviveUntilConsumption)
{
    run([&] {
        auto s = sponsored_interest();
        ASSERT_EQ(seams.sponsor_use(s.interest, s.revision, s.grant),
                  SR::ok);
        ASSERT_EQ(seams.finish_use(s.interest, s.revision, s.grant),
                  SR::ok);
        auto d = interests.begin_detach(s.interest);
        ASSERT_EQ(d.refusal, IR::ok);

        auto revoke = cells.revoke_cell(*d.command);
        ASSERT_EQ(revoke.refusal, CR::ok);

        // The cell still holds its receipt home: the interest has not
        // consumed. No witness exists, so retirement is impossible —
        // and the replay home still serves the receipt.
        auto replay = cells.revoke_cell(*d.command);
        ASSERT_EQ(replay.refusal, CR::ok);
        EXPECT_TRUE(*replay.receipt == *revoke.receipt);

        auto consumed = interests.consume_revoke_receipt(*revoke.receipt);
        ASSERT_EQ(consumed.refusal, IR::ok);
        ASSERT_EQ(
            interests.consume_unsubscribe_receipt(
                UnsubscribeReceiptTestMint::mint(s.interest, 900)),
            IR::ok);

        auto join = interests.terminal_join(s.interest);
        ASSERT_EQ(join.refusal, IJ::ok);

        // The interest is terminal-ready but its ack is unconsumed:
        // the row and its replay home MUST survive. retire_terminal
        // does not accept the ack's type; the only witness minter is
        // the ledger, which has not consumed.
        EXPECT_TRUE(interests.interest_present(s.interest).value_or(false));
        auto rejoin = interests.terminal_join(s.interest);
        ASSERT_EQ(rejoin.refusal, IJ::ok);
        EXPECT_TRUE(*rejoin.ack == *join.ack)
            << "the terminal witness survives until downstream "
               "consumption, not merely until production";
    });
}

// Bad sequence 4 (review F3): the split spend/register orders are
// inexpressible — the seam validates both homes and commits both or
// neither, and no raw-id debt verbs exist.
TEST_F(SeamsFixture, BadSequence4_SplitSpendRegisterIsInexpressible)
{
    run([&] {
        // Interest-side refusal: detached interest, healthy grant.
        auto a = sponsored_interest(100, 7);
        auto g2 = cells.issue_grant(a.interest, a.revision);
        ASSERT_EQ(g2.refusal, CR::ok);
        ASSERT_EQ(interests.begin_detach(a.interest).refusal, IR::ok);
        EXPECT_EQ(seams.sponsor_use(a.interest, a.revision, g2.grant),
                  SR::interest_refuses);
        EXPECT_EQ(cells.grant_state(a.interest, g2.grant), G::unspent)
            << "no half-commit: the grant did not spend";
        EXPECT_EQ(interests.use_open(a.interest, g2.grant),
                  std::optional<bool>{false})
            << "no half-commit: no interest debt registered";

        // Cell-side refusal: active interest, foreign revision.
        auto b = sponsored_interest(101, 8);
        EXPECT_EQ(seams.sponsor_use(b.interest, b.revision + 1, b.grant),
                  SR::cell_refuses);
        EXPECT_EQ(interests.use_open(b.interest, b.grant),
                  std::optional<bool>{false});

        // Bad order A from the review is now impossible by
        // construction: a spent grant IS a registered debt, so the
        // join is gated until the seam finishes it.
        ASSERT_EQ(seams.sponsor_use(b.interest, b.revision, b.grant),
                  SR::ok);
        auto d = interests.begin_detach(b.interest);
        ASSERT_EQ(d.refusal, IR::ok);
        auto revoke = cells.revoke_cell(*d.command);
        ASSERT_EQ(revoke.refusal, CR::ok);
        ASSERT_EQ(
            interests.consume_revoke_receipt(*revoke.receipt).refusal,
            IR::ok);
        ASSERT_EQ(
            interests.consume_unsubscribe_receipt(
                UnsubscribeReceiptTestMint::mint(b.interest, 900)),
            IR::ok);
        EXPECT_EQ(interests.terminal_join(b.interest).refusal,
                  IJ::obligations_open)
            << "the child cannot become terminal while the participant "
               "owns an open use";

        // Duplicate seam commits are typed on the refusing side.
        EXPECT_EQ(seams.sponsor_use(b.interest, b.revision, b.grant),
                  SR::cell_refuses)
            << "a spent grant cannot respend";
        ASSERT_EQ(seams.finish_use(b.interest, b.revision, b.grant),
                  SR::ok);
        EXPECT_EQ(seams.finish_use(b.interest, b.revision, b.grant),
                  SR::cell_refuses)
            << "a finished use cannot re-finish";
        EXPECT_EQ(interests.terminal_join(b.interest).refusal, IJ::ok);
    });
}

// The full three-phase composed round trip, losing every artifact
// once and recovering it from its retained home.
TEST_F(SeamsFixture, ThreePhaseRoundTripSurvivesLossAtEveryPhase)
{
    run([&] {
        // The ledger owns the original child; the interest retains it.
        auto root = ledger.open_root();
        ASSERT_TRUE(root.has_value());
        auto [reg_refusal, child] = ledger.register_child(*root);
        ASSERT_EQ(reg_refusal, OperationLedger::Refusal::ok);
        ASSERT_EQ(ledger.publish(child, /*dest*/ 9, /*work*/ 4),
                  OperationLedger::Refusal::ok);

        auto open = cells.open_cell(1, 10, root->id(), kEpoch);
        ASSERT_EQ(open.refusal, CR::ok);
        auto r = interests.register_interest(
            root->id(), child.id(), 10, 900,
            ParticipantDestination{kScope, kEpoch, open.revision});
        ASSERT_EQ(r.refusal, IR::ok);
        auto g = cells.issue_grant(r.interest, open.revision);
        ASSERT_EQ(g.refusal, CR::ok);
        ASSERT_EQ(seams.sponsor_use(r.interest, open.revision, g.grant),
                  SR::ok);
        ASSERT_EQ(seams.finish_use(r.interest, open.revision, g.grant),
                  SR::ok);

        // Phase 1: the command. Lose it; replay from the outbox.
        ASSERT_EQ(interests.begin_detach(r.interest).refusal, IR::ok);
        auto cmd = interests.replay_revoke_command(r.interest);
        ASSERT_TRUE(cmd.has_value());

        // Phase 2: the receipt. Lose it; replay via the same command.
        ASSERT_EQ(cells.revoke_cell(*cmd).refusal, CR::ok);
        auto receipt = cells.revoke_cell(*cmd).receipt;
        ASSERT_TRUE(receipt.has_value());

        // Phase 3: the consumption witness. Lose the returned copy;
        // recover via replay getter AND via exact re-consumption.
        ASSERT_EQ(interests.consume_revoke_receipt(*receipt).refusal,
                  IR::ok);
        auto witness = interests.replay_revoke_consumed(r.interest);
        ASSERT_TRUE(witness.has_value());
        auto reconsume = interests.consume_revoke_receipt(*receipt);
        EXPECT_EQ(reconsume.refusal, IR::already_acked);
        EXPECT_TRUE(*reconsume.consumed == *witness);

        // Terminal handoff, same three phases.
        ASSERT_EQ(
            interests.consume_unsubscribe_receipt(
                UnsubscribeReceiptTestMint::mint(r.interest, 900)),
            IR::ok);
        ASSERT_EQ(interests.terminal_join(r.interest).refusal, IJ::ok);
        auto ack = interests.terminal_join(r.interest).ack;  // replay
        ASSERT_TRUE(ack.has_value());

        auto consumed = ledger.consume_terminal_ack(*ack, 9, 4);
        ASSERT_EQ(consumed.outcome, AO::removed);
        ASSERT_TRUE(consumed.witness.has_value());
        EXPECT_EQ(ledger.child_count(root->id()), 0u)
            << "the ledger's exactly-once gate ran";

        // Lose the terminal witness; recover from BOTH replay homes.
        auto replayed = ledger.consume_terminal_ack(*ack, 9, 4);
        EXPECT_EQ(replayed.outcome, AO::removed);
        EXPECT_TRUE(*replayed.witness == *consumed.witness);
        auto from_home = ledger.replay_terminal_consumed(r.interest);
        ASSERT_TRUE(from_home.has_value());
        EXPECT_TRUE(*from_home == *consumed.witness);

        // Retire both producers with their downstream witnesses.
        ASSERT_EQ(interests.retire_terminal(*consumed.witness), IR::ok);
        ASSERT_EQ(cells.retire_cell(*witness), CR::ok);
        EXPECT_FALSE(interests.interest_present(r.interest).value_or(true));
        EXPECT_FALSE(cells.cell_present(r.interest).value_or(true));
    });
}

TEST_F(SeamsFixture, OffStrandSeamCommitsAreRefused)
{
    Sponsored s{};
    run([&] { s = sponsored_interest(); });

    bool checked = false;
    asio::post(sched.io(), [&] {
        EXPECT_EQ(seams.sponsor_use(s.interest, s.revision, s.grant),
                  SR::off_strand);
        EXPECT_EQ(seams.finish_use(s.interest, s.revision, s.grant),
                  SR::off_strand);
        checked = true;
    });
    sched.run_until_idle();
    EXPECT_TRUE(checked);

    run([&] {
        EXPECT_EQ(seams.sponsor_use(s.interest, s.revision, s.grant),
                  SR::ok)
            << "the off-strand refusals committed nothing";
    });
}
