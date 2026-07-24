// Controls for the aggregate coordinator façade: the composed-review
// bad sequences lead, then creation/commit exception-atomicity, the
// closed ledger terminal variant, the witness release protocol, and
// the full three-phase round trip with loss at every phase.

#include "coordinator-seams.h"

#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

#include <limits>
#include <optional>
#include <stdexcept>
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
    // The façade OWNS its components: one (strand, scope) constructs
    // everything, so mismatched joins are unrepresentable — there is
    // no constructor accepting foreign registries.
    CoordinatorInterestSeams seams{strand, kScope};
    InterestRegistry& interests = seams.interests();
    ParticipantCellRegistry& cells = seams.cells();
    OperationLedger& ledger = seams.ledger();

    template <typename Fn>
    void
    run(Fn fn)
    {
        asio::post(strand, std::move(fn));
        sched.run_until_idle();
    }

    struct Pair
    {
        InterestId interest;
        CellRevision revision;
        GrantId grant;
    };

    /// The atomic pair mint plus one issued grant.
    Pair
    opened_pair(
        InterestOperationId operation = 100,
        InterestChildId child = 7)
    {
        auto o = seams.open_interest_with_cell(
            operation, child, 10, 900, kEpoch);
        EXPECT_EQ(o.refusal, CoordinatorInterestSeams::Refusal::ok);
        auto g = cells.issue_grant(o.interest, o.revision);
        EXPECT_EQ(g.refusal, ParticipantCellRegistry::Refusal::ok);
        return {o.interest, o.revision, g.grant};
    }
};

using SR = CoordinatorInterestSeams::Refusal;
using IR = InterestRegistry::Refusal;
using IJ = InterestRegistry::JoinRefusal;
using CR = ParticipantCellRegistry::Refusal;
using G = ParticipantCellRegistry::GrantState;
using AO = OperationLedger::AckOutcome;

}  // namespace

// Review F2: interest/cell creation is ONE transition — both minted
// with the exact shared binding, or neither. No guessed ids.
TEST_F(SeamsFixture, PairCreationIsAtomicAndExactlyBound)
{
    run([&] {
        auto o = seams.open_interest_with_cell(100, 7, 10, 900, kEpoch);
        ASSERT_EQ(o.refusal, SR::ok);
        EXPECT_TRUE(interests.interest_present(o.interest).value_or(false));
        EXPECT_EQ(cells.cell_revision(o.interest), o.revision)
            << "the pair shares one identity, minted together";

        // Failpoint between prepare and commit: NEITHER home appears,
        // and neither counter moves — the next open mints the same
        // ids the failed one would have.
        seams.test_hooks.open_pre_commit = [] {
            throw std::runtime_error{"injected"};
        };
        EXPECT_THROW(
            seams.open_interest_with_cell(101, 8, 10, 901, kEpoch),
            std::runtime_error);
        seams.test_hooks.open_pre_commit = nullptr;
        auto retry = seams.open_interest_with_cell(101, 8, 10, 901, kEpoch);
        ASSERT_EQ(retry.refusal, SR::ok);
        EXPECT_EQ(retry.interest, o.interest + 1)
            << "no orphan interest: the failed attempt minted nothing";
        EXPECT_EQ(retry.revision, o.revision + 1)
            << "no orphan cell: the failed attempt minted nothing";
    });
}

TEST(SeamsEdge, PairCreationCeilingsRefuseTyped)
{
    // Interest-id ceiling.
    {
        DeterministicScheduler sched;
        auto strand = sched.make_strand();
        CoordinatorInterestSeams::Seeds seeds;
        seeds.first_interest = std::numeric_limits<InterestId>::max();
        CoordinatorInterestSeams s{strand, kScope, seeds};
        asio::post(strand, [&] {
            EXPECT_EQ(
                s.open_interest_with_cell(1, 2, 3, 4, kEpoch).refusal,
                SR::interest_id_exhausted);
            EXPECT_FALSE(s.cells().cell_present(0).value_or(true))
                << "neither home minted; zero reserved";
            EXPECT_EQ(
                s.open_interest_with_cell(1, 2, 3, 4, kEpoch).refusal,
                SR::interest_id_exhausted)
                << "the counter did not move";
        });
        sched.run_until_idle();
    }
    // Revision ceiling.
    {
        DeterministicScheduler sched;
        auto strand = sched.make_strand();
        CoordinatorInterestSeams::Seeds seeds;
        seeds.first_revision = std::numeric_limits<CellRevision>::max();
        CoordinatorInterestSeams s{strand, kScope, seeds};
        asio::post(strand, [&] {
            EXPECT_EQ(
                s.open_interest_with_cell(1, 2, 3, 4, kEpoch).refusal,
                SR::revision_exhausted);
            EXPECT_FALSE(
                s.interests().interest_present(1).value_or(true))
                << "the interest was not minted commandless either";
        });
        sched.run_until_idle();
    }
}

// Review F1 bad sequence 1: a delayed command minted for a retired
// cell instance must not acquire authority over the reopened one.
TEST_F(SeamsFixture, BadSequence1_StaleRevokeAfterReopenRefuses)
{
    run([&] {
        auto p = opened_pair();
        ASSERT_EQ(seams.sponsor_use(p.interest, p.grant), SR::ok);
        auto d = interests.begin_detach(p.interest);
        ASSERT_EQ(d.refusal, IR::ok);
        auto stale = *d.command;  // the delayed copy, genuinely minted

        // Drive instance R1 to full retirement through the seams.
        ASSERT_EQ(seams.finish_use(p.interest, p.grant), SR::ok);
        auto revoke = cells.revoke_cell(stale);
        ASSERT_EQ(revoke.refusal, CR::ok);
        auto consumed = interests.consume_revoke_receipt(*revoke.receipt);
        ASSERT_EQ(consumed.refusal, IR::ok);
        ASSERT_EQ(cells.retire_cell(*consumed.consumed), CR::ok);

        // Reopen the SAME interest key as instance R2 (component test
        // seam: the composed reopen path is a later increment).
        auto reopen = ParticipantCellTestAccess::open(
            cells, p.interest, 10, 100, kEpoch);
        ASSERT_TRUE(reopen.ok);
        ASSERT_GT(reopen.revision, p.revision);

        // The delayed R1 command arrives.
        EXPECT_EQ(cells.revoke_cell(stale).refusal, CR::wrong_binding)
            << "a stale command has no authority over a fresh instance";
        EXPECT_EQ(cells.issue_grant(p.interest, reopen.revision).refusal,
                  CR::ok)
            << "R2 remains fully active";

        // And the interest still expects R1: the seam's shared-binding
        // revalidation refuses the mismatched pair.
        auto g2 = cells.issue_grant(p.interest, reopen.revision);
        ASSERT_EQ(g2.refusal, CR::ok);
        EXPECT_EQ(seams.sponsor_use(p.interest, g2.grant),
                  SR::binding_mismatch)
            << "expected R1 vs actual R2 cannot pass the seam";
        EXPECT_EQ(cells.grant_state(p.interest, g2.grant), G::unspent);
        EXPECT_EQ(interests.use_open(p.interest, g2.grant),
                  std::optional<bool>{false});
    });
}

// Review F1 bad sequence 2: identical local counters across partitions;
// every artifact refused on scope alone, present or absent ids alike.
TEST_F(SeamsFixture, BadSequence2_CrossPartitionArtifactsRefuse)
{
    DeterministicScheduler s2;
    auto strand2 = s2.make_strand();
    PartitionScope const scope_b{1, 2};
    CoordinatorInterestSeams b{strand2, scope_b};
    std::optional<RevokeCommand> cmd_b;
    std::optional<ParticipantRevokedReceipt> receipt_b;
    asio::post(strand2, [&] {
        auto o = b.open_interest_with_cell(100, 7, 10, 900, kEpoch);
        ASSERT_EQ(o.refusal, SR::ok);
        auto d = b.interests().begin_detach(o.interest);
        ASSERT_EQ(d.refusal, IR::ok);
        cmd_b = d.command;
        auto rv = b.cells().revoke_cell(*cmd_b);
        ASSERT_EQ(rv.refusal, CR::ok);
        receipt_b = rv.receipt;
    });
    s2.run_until_idle();

    run([&] {
        auto p = opened_pair();
        ASSERT_EQ(p.interest, 1u)
            << "the collision is real: both partitions minted "
               "interest 1 with identical local counters";
        ASSERT_EQ(interests.begin_detach(p.interest).refusal, IR::ok);

        // B's artifacts against A: refused scope-first, with the SAME
        // refusal whether A has the id (1, present) or not (999).
        EXPECT_EQ(cells.revoke_cell(*cmd_b).refusal, CR::wrong_scope);
        EXPECT_EQ(
            interests.consume_revoke_receipt(*receipt_b).refusal,
            IR::wrong_scope);
        EXPECT_EQ(
            interests
                .consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                    scope_b, kEpoch, 999, 1, 1))
                .refusal,
            IR::wrong_scope)
            << "absent id: identical refusal, no presence leak";
        EXPECT_EQ(
            ledger
                .consume_terminal_ack(
                    TerminalAckTestMint::mint(scope_b, 1, 100, 7), 9, 4)
                .outcome,
            AO::wrong_scope);
        EXPECT_EQ(
            seams.retire_interest_terminal(
                TerminalConsumedTestMint::mint(scope_b, 1, 100, 7)),
            SR::wrong_scope)
            << "scope-first at the seam too: no phase leak";
    });
}

// Review F2 (round 3): neither producer home can be destroyed before
// downstream consumption — and now retirement itself is the façade's
// atomic dual-erase.
TEST_F(SeamsFixture, BadSequence3_ProducerHomesSurviveUntilConsumption)
{
    run([&] {
        auto p = opened_pair();
        ASSERT_EQ(seams.sponsor_use(p.interest, p.grant), SR::ok);
        ASSERT_EQ(seams.finish_use(p.interest, p.grant), SR::ok);
        auto d = interests.begin_detach(p.interest);
        ASSERT_EQ(d.refusal, IR::ok);

        auto revoke = cells.revoke_cell(*d.command);
        ASSERT_EQ(revoke.refusal, CR::ok);

        // The cell still holds its receipt home: the interest has not
        // consumed. The replay home still serves the receipt.
        auto replay = cells.revoke_cell(*d.command);
        ASSERT_EQ(replay.refusal, CR::ok);
        EXPECT_TRUE(*replay.receipt == *revoke.receipt);

        auto consumed = interests.consume_revoke_receipt(*revoke.receipt);
        ASSERT_EQ(consumed.refusal, IR::ok);
        ASSERT_EQ(
            interests.consume_unsubscribe_receipt(
                UnsubscribeReceiptTestMint::mint(kScope, p.interest, 900)),
            IR::ok);

        auto join = interests.terminal_join(p.interest);
        ASSERT_EQ(join.refusal, IJ::ok);

        // Terminal-ready with the ack unconsumed: the row and its
        // replay survive. A witness the ledger never minted cannot
        // retire it — the seam validates BOTH retained homes.
        EXPECT_TRUE(
            interests.interest_present(p.interest).value_or(false));
        EXPECT_EQ(
            seams.retire_interest_terminal(TerminalConsumedTestMint::mint(
                kScope, p.interest, join.ack->operation(),
                join.ack->child())),
            SR::wrong_witness)
            << "no ledger home: the ledger has not consumed the ack, "
               "so retirement is impossible";
        auto rejoin = interests.terminal_join(p.interest);
        ASSERT_EQ(rejoin.refusal, IJ::ok);
        EXPECT_TRUE(*rejoin.ack == *join.ack)
            << "the terminal witness survives until downstream "
               "consumption, not merely until production";
    });
}

// Review F3 (round 3): split spend/register orders are inexpressible;
// round 4 adds exception-atomicity at the sponsor failpoint.
TEST_F(SeamsFixture, BadSequence4_SplitSpendRegisterIsInexpressible)
{
    run([&] {
        // Interest-side refusal: detached interest, healthy grant.
        auto a = opened_pair(100, 7);
        auto g2 = cells.issue_grant(a.interest, a.revision);
        ASSERT_EQ(g2.refusal, CR::ok);
        ASSERT_EQ(interests.begin_detach(a.interest).refusal, IR::ok);
        EXPECT_EQ(seams.sponsor_use(a.interest, g2.grant),
                  SR::interest_refuses);
        EXPECT_EQ(cells.grant_state(a.interest, g2.grant), G::unspent)
            << "no half-commit: the grant did not spend";
        EXPECT_EQ(interests.use_open(a.interest, g2.grant),
                  std::optional<bool>{false})
            << "no half-commit: no interest debt registered";

        // Exception-atomicity: a throw at the sponsor failpoint leaves
        // both homes pre-state; the retried commit then succeeds.
        auto b2 = opened_pair(101, 8);
        seams.test_hooks.sponsor_pre_commit = [] {
            throw std::runtime_error{"injected"};
        };
        EXPECT_THROW(seams.sponsor_use(b2.interest, b2.grant),
                     std::runtime_error);
        seams.test_hooks.sponsor_pre_commit = nullptr;
        EXPECT_EQ(cells.grant_state(b2.interest, b2.grant), G::unspent);
        EXPECT_EQ(interests.use_open(b2.interest, b2.grant),
                  std::optional<bool>{false});
        ASSERT_EQ(seams.sponsor_use(b2.interest, b2.grant), SR::ok);

        // A spent grant IS a registered debt: the join stays gated
        // until the seam finishes it (review's bad order A).
        auto d = interests.begin_detach(b2.interest);
        ASSERT_EQ(d.refusal, IR::ok);
        auto revoke = cells.revoke_cell(*d.command);
        ASSERT_EQ(revoke.refusal, CR::ok);
        ASSERT_EQ(
            interests.consume_revoke_receipt(*revoke.receipt).refusal,
            IR::ok);
        ASSERT_EQ(
            interests.consume_unsubscribe_receipt(
                UnsubscribeReceiptTestMint::mint(kScope, b2.interest, 900)),
            IR::ok);
        EXPECT_EQ(interests.terminal_join(b2.interest).refusal,
                  IJ::obligations_open)
            << "the child cannot become terminal while the participant "
               "owns an open use";

        EXPECT_EQ(seams.sponsor_use(b2.interest, b2.grant),
                  SR::cell_refuses)
            << "a spent grant cannot respend";
        ASSERT_EQ(seams.finish_use(b2.interest, b2.grant), SR::ok);
        EXPECT_EQ(seams.finish_use(b2.interest, b2.grant),
                  SR::cell_refuses)
            << "a finished use cannot re-finish";
        EXPECT_EQ(interests.terminal_join(b2.interest).refusal, IJ::ok);
    });
}

// Review F4: the ledger's terminal variant is closed — exercised at
// the composed level (component orders live in the ledger suite).
TEST_F(SeamsFixture, RawGateCannotDestroyOrStealTheInterestWitness)
{
    run([&] {
        auto root = [&] {
            auto [r, lease] = ledger.open_root();
            EXPECT_EQ(r, OperationLedger::Refusal::ok);
            return std::move(lease);
        }();
        auto [cr, child] = ledger.register_child(root);
        ASSERT_EQ(cr, OperationLedger::Refusal::ok);

        auto o = seams.open_interest_with_cell(
            root.id(), child.id(), 10, 900, kEpoch);
        ASSERT_EQ(o.refusal, SR::ok);
        ASSERT_EQ(
            ledger.publish_for_interest(child, 9, 4, o.interest),
            OperationLedger::Refusal::ok);

        // Raw-first: the old gate refuses the new variant outright.
        EXPECT_EQ(
            ledger.ack_terminal(child.id(), 9, 4), AO::interest_bound);
        EXPECT_EQ(ledger.child_count(root.id()), 1u)
            << "nothing destroyed; the terminal obligation survives";

        // Wrong-interest-first: another interest's ack cannot steal it.
        EXPECT_EQ(
            ledger
                .consume_terminal_ack(
                    TerminalAckTestMint::mint(
                        kScope, o.interest + 500, root.id(), child.id()),
                    9, 4)
                .outcome,
            AO::wrong_binding);
        EXPECT_EQ(ledger.child_count(root.id()), 1u);
    });
}

// The full three-phase composed round trip, losing every artifact once
// and recovering it from its retained home — ending in the façade's
// atomic retirement that RELEASES the ledger witness home (review F6's
// named release protocol).
TEST_F(SeamsFixture, ThreePhaseRoundTripEndsWithWitnessRelease)
{
    run([&] {
        auto root = [&] {
            auto [r, lease] = ledger.open_root();
            EXPECT_EQ(r, OperationLedger::Refusal::ok);
            return std::move(lease);
        }();
        auto [cr, child] = ledger.register_child(root);
        ASSERT_EQ(cr, OperationLedger::Refusal::ok);

        auto o = seams.open_interest_with_cell(
            root.id(), child.id(), 10, 900, kEpoch);
        ASSERT_EQ(o.refusal, SR::ok);
        ASSERT_EQ(
            ledger.publish_for_interest(child, 9, 4, o.interest),
            OperationLedger::Refusal::ok);
        auto g = cells.issue_grant(o.interest, o.revision);
        ASSERT_EQ(g.refusal, CR::ok);
        ASSERT_EQ(seams.sponsor_use(o.interest, g.grant), SR::ok);
        ASSERT_EQ(seams.finish_use(o.interest, g.grant), SR::ok);

        // Phase 1: the command. Lose it; replay from the outbox.
        ASSERT_EQ(interests.begin_detach(o.interest).refusal, IR::ok);
        auto cmd = interests.replay_revoke_command(o.interest);
        ASSERT_TRUE(cmd.has_value());

        // Phase 2: the receipt. Lose it; replay via the same command.
        ASSERT_EQ(cells.revoke_cell(*cmd).refusal, CR::ok);
        auto receipt = cells.revoke_cell(*cmd).receipt;
        ASSERT_TRUE(receipt.has_value());

        // Phase 3: the consumption witness. Lose the returned copy;
        // recover via the replay getter AND exact re-consumption.
        ASSERT_EQ(interests.consume_revoke_receipt(*receipt).refusal,
                  IR::ok);
        auto witness = interests.replay_revoke_consumed(o.interest);
        ASSERT_TRUE(witness.has_value());
        auto reconsume = interests.consume_revoke_receipt(*receipt);
        EXPECT_EQ(reconsume.refusal, IR::already_acked);
        EXPECT_TRUE(*reconsume.consumed == *witness);

        // Terminal handoff, same three phases.
        ASSERT_EQ(
            interests.consume_unsubscribe_receipt(
                UnsubscribeReceiptTestMint::mint(kScope, o.interest, 900)),
            IR::ok);
        ASSERT_EQ(interests.terminal_join(o.interest).refusal, IJ::ok);
        auto ack = interests.terminal_join(o.interest).ack;  // replay
        ASSERT_TRUE(ack.has_value());

        auto consumed = ledger.consume_terminal_ack(*ack, 9, 4);
        ASSERT_EQ(consumed.outcome, AO::removed);
        ASSERT_TRUE(consumed.witness.has_value());
        EXPECT_EQ(ledger.child_count(root.id()), 0u);

        // Lose the terminal witness; recover from BOTH replay homes.
        auto replayed = ledger.consume_terminal_ack(*ack, 9, 4);
        EXPECT_EQ(replayed.outcome, AO::removed);
        EXPECT_TRUE(*replayed.witness == *consumed.witness);
        auto from_home = ledger.replay_terminal_consumed(o.interest);
        ASSERT_TRUE(from_home.has_value());
        EXPECT_TRUE(*from_home == *consumed.witness);

        // The façade's atomic retirement: interest row erased AND the
        // ledger's witness home RELEASED, exactly once, one commit.
        ASSERT_EQ(seams.retire_interest_terminal(*consumed.witness),
                  SR::ok);
        EXPECT_FALSE(
            interests.interest_present(o.interest).value_or(true));
        EXPECT_FALSE(
            ledger.replay_terminal_consumed(o.interest).has_value())
            << "the witness home is released at retirement: rows are "
               "bounded by live terminal-consumed interests";
        EXPECT_EQ(seams.retire_interest_terminal(*consumed.witness),
                  SR::unknown_interest)
            << "exactly once, typed on the second attempt";

        // And the participant side retires on its own witness.
        ASSERT_EQ(cells.retire_cell(*witness), CR::ok);
        EXPECT_FALSE(cells.cell_present(o.interest).value_or(true));
    });
}

TEST_F(SeamsFixture, OffStrandSeamCommitsAreRefused)
{
    Pair p{};
    run([&] { p = opened_pair(); });

    bool checked = false;
    asio::post(sched.io(), [&] {
        EXPECT_EQ(
            seams.open_interest_with_cell(1, 2, 3, 4, kEpoch).refusal,
            SR::off_strand);
        EXPECT_EQ(seams.sponsor_use(p.interest, p.grant), SR::off_strand);
        EXPECT_EQ(seams.finish_use(p.interest, p.grant), SR::off_strand);
        EXPECT_EQ(
            seams.retire_interest_terminal(TerminalConsumedTestMint::mint(
                kScope, p.interest, 100, 7)),
            SR::off_strand);
        checked = true;
    });
    sched.run_until_idle();
    EXPECT_TRUE(checked);

    run([&] {
        EXPECT_EQ(seams.sponsor_use(p.interest, p.grant), SR::ok)
            << "the off-strand refusals committed nothing";
    });
}
