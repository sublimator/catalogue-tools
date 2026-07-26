// Controls for the aggregate coordinator façade: the composed-review
// bad sequences lead, then triple-bind creation (interest, cell, and
// the reserved ledger child), commit exception-atomicity, the closed
// ledger terminal variant, and the total retirement that reconciles
// and releases EVERY remaining home.

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

    /// Register a fresh ledger child (its root lease is dropped:
    /// destructors are passive, the row stays visible, and close is
    /// never requested here).
    ChildLease
    make_child(OperationId* operation_out = nullptr)
    {
        auto root_result = ledger.open_root();
        EXPECT_EQ(root_result.refusal, OperationLedger::Refusal::ok);
        if (operation_out)
            *operation_out = root_result.lease.id();
        auto [r, child] = ledger.register_child(root_result.lease);
        EXPECT_EQ(r, OperationLedger::Refusal::ok);
        return std::move(child);
    }

    struct Triple
    {
        InterestId interest = 0;
        CellRevision revision = 0;
        GrantId grant = 0;
        OperationId operation = 0;
        ChildLease child;
    };

    /// The atomic triple mint plus one issued grant.
    Triple
    opened_triple()
    {
        Triple t;
        t.child = make_child(&t.operation);
        auto o = seams.open_interest_with_cell(t.child, 10, 900, kEpoch);
        EXPECT_EQ(o.refusal, CoordinatorInterestSeams::Refusal::ok);
        t.interest = o.interest;
        t.revision = o.revision;
        auto g = cells.issue_grant(o.interest, o.revision);
        EXPECT_EQ(g.refusal, ParticipantCellRegistry::Refusal::ok);
        t.grant = g.grant;
        return t;
    }
};

using SR = CoordinatorInterestSeams::Refusal;
using IR = InterestRegistry::Refusal;
using IJ = InterestRegistry::JoinRefusal;
using CR = ParticipantCellRegistry::Refusal;
using G = ParticipantCellRegistry::GrantState;
using AO = OperationLedger::AckOutcome;
using LR = OperationLedger::Refusal;

}  // namespace

// Review F2 (round 4): the pair mint is a TRIPLE bind — the ledger
// child is the third home, reserved atomically, never caller-selected.
TEST_F(SeamsFixture, TripleMintBindsAndReservesTheExactChild)
{
    run([&] {
        // A nonexistent child cannot anchor an interest.
        EXPECT_EQ(
            seams.open_interest_with_cell(ChildLease{}, 10, 900, kEpoch)
                .refusal,
            SR::ledger_refuses);

        // An already-published child cannot either.
        auto published = make_child();
        ASSERT_EQ(ledger.publish(published, 9, 4), LR::ok);
        EXPECT_EQ(
            seams.open_interest_with_cell(published, 10, 900, kEpoch)
                .refusal,
            SR::ledger_refuses);

        // A fresh child binds: the operation is DERIVED, the child is
        // reserved for the minted InterestId.
        OperationId operation = 0;
        auto child = make_child(&operation);
        auto o = seams.open_interest_with_cell(child, 10, 900, kEpoch);
        ASSERT_EQ(o.refusal, SR::ok);
        EXPECT_EQ(cells.cell_revision(o.interest), o.revision);

        // The SAME child cannot be claimed twice — and the refusal
        // changes no home (the next mint ids are unmoved).
        auto second = seams.open_interest_with_cell(child, 11, 901, kEpoch);
        EXPECT_EQ(second.refusal, SR::child_already_claimed);
        EXPECT_FALSE(
            interests.interest_present(o.interest + 1).value_or(true));

        // The store verb refuses the reserved child: the variant was
        // chosen at reservation, not by publication convention.
        EXPECT_EQ(ledger.publish(child, 9, 4), LR::interest_reserved);

        // The seam publication derives the retained InterestId; the
        // consume gate then accepts exactly that interest's ack.
        ASSERT_EQ(seams.publish_for_interest(child, 9, 4), LR::ok);
        EXPECT_EQ(
            ledger
                .consume_terminal_ack(
                    TerminalAckTestMint::mint(
                        kScope, o.interest + 500, operation, child.id()),
                    9, 4)
                .outcome,
            AO::wrong_binding)
            << "another interest cannot steal the reserved child";
    });
}

TEST_F(SeamsFixture, TripleMintIsExceptionAtomicAtTheFailpoint)
{
    run([&] {
        auto first = opened_triple();

        // Failpoint between prepare and commit: NO home appears — no
        // interest, no cell, no reservation — and no counter moves.
        auto child = make_child();
        seams.test_hooks.open_pre_commit = [] {
            throw std::runtime_error{"injected"};
        };
        EXPECT_THROW(
            seams.open_interest_with_cell(child, 10, 901, kEpoch),
            std::runtime_error);
        seams.test_hooks.open_pre_commit = nullptr;

        // The SAME child retries successfully: its reservation was not
        // half-applied; the minted ids are exactly the ones the failed
        // attempt would have received.
        auto retry = seams.open_interest_with_cell(child, 10, 901, kEpoch);
        ASSERT_EQ(retry.refusal, SR::ok);
        EXPECT_EQ(retry.interest, first.interest + 1)
            << "no orphan interest: the failed attempt minted nothing";
        EXPECT_EQ(retry.revision, first.revision + 1)
            << "no orphan cell: the failed attempt minted nothing";
    });
}

TEST(SeamsEdge, TripleMintCeilingsRefuseTyped)
{
    // Interest-id ceiling.
    {
        DeterministicScheduler sched;
        auto strand = sched.make_strand();
        CoordinatorInterestSeams::Seeds seeds;
        seeds.first_interest = std::numeric_limits<InterestId>::max();
        CoordinatorInterestSeams s{strand, kScope, seeds};
        asio::post(strand, [&] {
            auto rr = s.ledger().open_root();
            ASSERT_EQ(rr.refusal, OperationLedger::Refusal::ok);
            auto [cr, child] = s.ledger().register_child(rr.lease);
            ASSERT_EQ(cr, OperationLedger::Refusal::ok);
            EXPECT_EQ(
                s.open_interest_with_cell(child, 3, 4, kEpoch).refusal,
                SR::interest_id_exhausted);
            EXPECT_FALSE(s.cells().cell_present(0).value_or(true))
                << "neither home minted; zero reserved";
            EXPECT_EQ(s.ledger().publish(child, 9, 4),
                      OperationLedger::Refusal::ok)
                << "the child was NOT left reserved by the refusal";
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
            auto rr = s.ledger().open_root();
            ASSERT_EQ(rr.refusal, OperationLedger::Refusal::ok);
            auto [cr, child] = s.ledger().register_child(rr.lease);
            ASSERT_EQ(cr, OperationLedger::Refusal::ok);
            EXPECT_EQ(
                s.open_interest_with_cell(child, 3, 4, kEpoch).refusal,
                SR::revision_exhausted);
            EXPECT_FALSE(
                s.interests().interest_present(1).value_or(true))
                << "the interest was not minted cellless either";
        });
        sched.run_until_idle();
    }
}

// Review F1 (round 3) bad sequence 1: a delayed command minted for a
// retired cell instance must not acquire authority over the reopened
// one — and the seam's shared-binding revalidation refuses the pair.
TEST_F(SeamsFixture, BadSequence1_StaleRevokeAfterReopenRefuses)
{
    run([&] {
        auto t = opened_triple();
        ASSERT_EQ(seams.sponsor_use(t.interest, t.grant), SR::ok);
        auto d = interests.begin_detach(t.interest);
        ASSERT_EQ(d.refusal, IR::ok);
        auto stale = *d.command;  // the delayed copy, genuinely minted

        // Drive instance R1's cell to retirement (component seam: the
        // composed re-participation path is a later increment).
        ASSERT_EQ(seams.finish_use(t.interest, t.grant), SR::ok);
        auto revoke = cells.revoke_cell(stale);
        ASSERT_EQ(revoke.refusal, CR::ok);
        auto consumed = interests.consume_revoke_receipt(*revoke.receipt);
        ASSERT_EQ(consumed.refusal, IR::ok);
        ASSERT_EQ(
            ParticipantCellTestAccess::retire(cells, *consumed.consumed),
            CR::ok);

        // Reopen the SAME interest key as instance R2.
        auto reopen = ParticipantCellTestAccess::open(
            cells, t.interest, 10, t.operation, kEpoch);
        ASSERT_TRUE(reopen.ok);
        ASSERT_GT(reopen.revision, t.revision);

        // The delayed R1 command arrives.
        EXPECT_EQ(cells.revoke_cell(stale).refusal, CR::wrong_binding)
            << "a stale command has no authority over a fresh instance";
        EXPECT_EQ(cells.issue_grant(t.interest, reopen.revision).refusal,
                  CR::ok)
            << "R2 remains fully active";

        // And the interest still expects R1: the seam's shared-binding
        // revalidation refuses the mismatched pair.
        auto g2 = cells.issue_grant(t.interest, reopen.revision);
        ASSERT_EQ(g2.refusal, CR::ok);
        EXPECT_EQ(seams.sponsor_use(t.interest, g2.grant),
                  SR::binding_mismatch)
            << "expected R1 vs actual R2 cannot pass the seam";
        EXPECT_EQ(cells.grant_state(t.interest, g2.grant), G::unspent);
        EXPECT_EQ(interests.use_open(t.interest, g2.grant),
                  std::optional<bool>{false});
    });
}

// Review F1 (round 3) bad sequence 2: identical local counters across
// partitions; every artifact refused on scope alone, present or
// absent ids alike.
TEST_F(SeamsFixture, BadSequence2_CrossPartitionArtifactsRefuse)
{
    DeterministicScheduler s2;
    auto strand2 = s2.make_strand();
    PartitionScope const scope_b{1, 2};
    CoordinatorInterestSeams b{strand2, scope_b};
    std::optional<RevokeCommand> cmd_b;
    std::optional<ParticipantRevokedReceipt> receipt_b;
    ChildLease spare_b;  // unclaimed child from B's ledger incarnation
    asio::post(strand2, [&] {
        auto rr = b.ledger().open_root();
        ASSERT_EQ(rr.refusal, OperationLedger::Refusal::ok);
        auto [cr, child_b] = b.ledger().register_child(rr.lease);
        ASSERT_EQ(cr, OperationLedger::Refusal::ok);
        auto [cr2, spare] = b.ledger().register_child(rr.lease);
        ASSERT_EQ(cr2, OperationLedger::Refusal::ok);
        spare_b = std::move(spare);
        auto o = b.open_interest_with_cell(child_b, 10, 900, kEpoch);
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
        auto t = opened_triple();
        ASSERT_EQ(t.interest, 1u)
            << "the collision is real: both partitions minted "
               "interest 1 with identical local counters";
        ASSERT_EQ(interests.begin_detach(t.interest).refusal, IR::ok);

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
                ChildTerminalWitnessTestMint::mint(scope_b, 1, 100, 7)),
            SR::wrong_scope)
            << "scope-first at the seam too: no phase leak";

        // A child LEASE from B's ledger incarnation cannot anchor an
        // interest here, even though its numeric id may collide: the
        // lease is bound to its minting incarnation, not to an id.
        EXPECT_EQ(
            seams.open_interest_with_cell(spare_b, 10, 902, kEpoch)
                .refusal,
            SR::ledger_refuses)
            << "foreign leases resolve to nothing before any lookup";
    });
}

// Review F2 (round 3): neither producer home can be destroyed before
// downstream consumption.
TEST_F(SeamsFixture, BadSequence3_ProducerHomesSurviveUntilConsumption)
{
    run([&] {
        auto t = opened_triple();
        ASSERT_EQ(seams.sponsor_use(t.interest, t.grant), SR::ok);
        ASSERT_EQ(seams.finish_use(t.interest, t.grant), SR::ok);
        auto d = interests.begin_detach(t.interest);
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
                UnsubscribeReceiptTestMint::mint(kScope, t.interest, 900)),
            IR::ok);

        auto join = interests.terminal_join(t.interest);
        ASSERT_EQ(join.refusal, IJ::ok);

        // Terminal-ready with the ack unconsumed: the row and its
        // replay survive. A witness the ledger never minted cannot
        // retire it — the seam validates EVERY retained home.
        EXPECT_TRUE(
            interests.interest_present(t.interest).value_or(false));
        EXPECT_EQ(
            seams.retire_interest_terminal(ChildTerminalWitnessTestMint::mint(
                kScope, t.interest, join.ack->operation(),
                join.ack->child())),
            SR::wrong_witness)
            << "no ledger home: the ledger has not consumed the ack, "
               "so retirement is impossible";
        auto rejoin = interests.terminal_join(t.interest);
        ASSERT_EQ(rejoin.refusal, IJ::ok);
        EXPECT_TRUE(*rejoin.ack == *join.ack)
            << "the terminal witness survives until downstream "
               "consumption, not merely until production";
    });
}

// Review F3 (round 3): split spend/register orders are inexpressible;
// exception-atomicity proven at the sponsor failpoint.
TEST_F(SeamsFixture, BadSequence4_SplitSpendRegisterIsInexpressible)
{
    run([&] {
        // Interest-side refusal: detached interest, healthy grant.
        auto a = opened_triple();
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
        auto b2 = opened_triple();
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

// Review F4 (round 3): the ledger's terminal variant is closed —
// composed order; component orders live in the ledger suite.
TEST_F(SeamsFixture, RawGateCannotDestroyOrStealTheInterestWitness)
{
    run([&] {
        auto t = opened_triple();
        ASSERT_EQ(seams.publish_for_interest(t.child, 9, 4), LR::ok);

        // Raw-first: the old gate refuses the new variant outright.
        EXPECT_EQ(
            ledger.ack_terminal(t.child.id(), 9, 4), AO::interest_bound);
        EXPECT_EQ(ledger.child_count(t.operation), 1u)
            << "nothing destroyed; the terminal obligation survives";

        // Wrong-interest-first: another interest's ack cannot steal it.
        EXPECT_EQ(
            ledger
                .consume_terminal_ack(
                    TerminalAckTestMint::mint(
                        kScope, t.interest + 500, t.operation,
                        t.child.id()),
                    9, 4)
                .outcome,
            AO::wrong_binding);
        EXPECT_EQ(ledger.child_count(t.operation), 1u);
    });
}

// The full three-phase composed round trip, losing every artifact once
// — including EVERY copy of the participant witness — and ending in
// the aggregate retirement that reconciles retained homes only and
// releases all three, or none at the failpoint (review F1, round 4).
TEST_F(SeamsFixture, RoundTripRetirementReleasesEveryHomeOrNone)
{
    run([&] {
        auto t = opened_triple();
        ASSERT_EQ(seams.publish_for_interest(t.child, 9, 4), LR::ok);
        ASSERT_EQ(seams.sponsor_use(t.interest, t.grant), SR::ok);
        ASSERT_EQ(seams.finish_use(t.interest, t.grant), SR::ok);

        // Phase 1: the command. Lose it; replay from the outbox.
        ASSERT_EQ(interests.begin_detach(t.interest).refusal, IR::ok);
        auto cmd = interests.replay_revoke_command(t.interest);
        ASSERT_TRUE(cmd.has_value());

        // Phase 2: the receipt. Lose it; replay via the same command.
        ASSERT_EQ(cells.revoke_cell(*cmd).refusal, CR::ok);
        auto receipt = cells.revoke_cell(*cmd).receipt;
        ASSERT_TRUE(receipt.has_value());

        // Phase 3: consume — and DROP every returned copy of the
        // participant witness immediately. Final retirement must
        // derive it from the RETAINED homes alone.
        ASSERT_EQ(interests.consume_revoke_receipt(*receipt).refusal,
                  IR::ok);
        EXPECT_TRUE(
            interests.replay_revoke_consumed(t.interest).has_value())
            << "the retained home exists; no caller copy is held";

        // Terminal handoff.
        ASSERT_EQ(
            interests.consume_unsubscribe_receipt(
                UnsubscribeReceiptTestMint::mint(kScope, t.interest, 900)),
            IR::ok);
        ASSERT_EQ(interests.terminal_join(t.interest).refusal, IJ::ok);
        auto ack = interests.terminal_join(t.interest).ack;  // replay
        ASSERT_TRUE(ack.has_value());

        auto consumed = ledger.consume_terminal_ack(*ack, 9, 4);
        ASSERT_EQ(consumed.outcome, AO::removed);
        ASSERT_TRUE(consumed.witness.has_value());
        EXPECT_EQ(consumed.witness->disposition(),
                  TerminalDisposition::terminal_ack_consumed)
            << "the ordinary path records genuine ack consumption";
        EXPECT_EQ(ledger.child_count(t.operation), 0u);

        // Injected failure at the retirement failpoint: EVERY home
        // survives — interest, cell, and ledger witness.
        seams.test_hooks.retire_pre_commit = [] {
            throw std::runtime_error{"injected"};
        };
        EXPECT_THROW(seams.retire_interest_terminal(*consumed.witness),
                     std::runtime_error);
        seams.test_hooks.retire_pre_commit = nullptr;
        EXPECT_TRUE(
            interests.interest_present(t.interest).value_or(false));
        EXPECT_TRUE(cells.cell_present(t.interest).value_or(false));
        EXPECT_TRUE(
            ledger.replay_child_terminal_witness(t.interest).has_value())
            << "all three or none: the failed retirement released "
               "nothing";

        // The real retirement: one commit reconciles and releases the
        // participant cell, the interest row, and the ledger witness.
        ASSERT_EQ(seams.retire_interest_terminal(*consumed.witness),
                  SR::ok);
        EXPECT_FALSE(
            interests.interest_present(t.interest).value_or(true));
        EXPECT_FALSE(cells.cell_present(t.interest).value_or(true))
            << "the participant cell is released by the aggregate, "
               "never by a lucky caller copy";
        EXPECT_FALSE(
            ledger.replay_child_terminal_witness(t.interest).has_value())
            << "the ledger witness home is released in the same commit";
        EXPECT_EQ(seams.retire_interest_terminal(*consumed.witness),
                  SR::unknown_interest)
            << "exactly once, typed on the second attempt";
    });
}

// Self-review catch: the never-published path is the OTHER old gate.
// An interest-reserved child that fails enqueue must still yield the
// terminal witness, or the interest is stranded terminal-ready with
// nothing to consume.
TEST_F(SeamsFixture, NeverPublishedReservedChildStillYieldsTheWitness)
{
    run([&] {
        auto t = opened_triple();

        // Enqueue failure: the child never publishes.
        ASSERT_EQ(ledger.mark_never_published(t.child), LR::ok);
        EXPECT_EQ(
            ledger.ack_never_published(t.child.id()), AO::removed);
        auto witness = ledger.replay_child_terminal_witness(t.interest);
        ASSERT_TRUE(witness.has_value())
            << "the never-published removal minted a retained witness";
        EXPECT_EQ(witness->child(), t.child.id());
        EXPECT_EQ(witness->disposition(),
                  TerminalDisposition::never_published)
            << "the witness records what HAPPENED: terminality "
               "without consuming an acknowledgement (the interest's "
               "later join may still mint one into its outbox)";

        // Presenting a terminal ack for this child must NOT report an
        // acknowledgement as consumed — none was ever produced.
        auto probe = ledger.consume_terminal_ack(
            TerminalAckTestMint::mint(
                kScope, t.interest, t.operation, t.child.id()),
            9, 4);
        EXPECT_EQ(probe.outcome, AO::unpublished)
            << "two terminal histories are not laundered into one";
        EXPECT_FALSE(probe.witness.has_value());
        EXPECT_EQ(
            ledger.replay_child_terminal_witness(t.interest)->disposition(),
            TerminalDisposition::never_published)
            << "replay preserves the honest disposition";

        // The interest drains normally and retires on that witness.
        auto d = interests.begin_detach(t.interest);
        ASSERT_EQ(d.refusal, IR::ok);
        auto revoke = cells.revoke_cell(*d.command);
        ASSERT_EQ(revoke.refusal, CR::ok);
        ASSERT_EQ(
            interests.consume_revoke_receipt(*revoke.receipt).refusal,
            IR::ok);
        ASSERT_EQ(
            interests.consume_unsubscribe_receipt(
                UnsubscribeReceiptTestMint::mint(kScope, t.interest, 900)),
            IR::ok);
        ASSERT_EQ(interests.terminal_join(t.interest).refusal, IJ::ok);
        ASSERT_EQ(seams.retire_interest_terminal(*witness), SR::ok);
        EXPECT_FALSE(
            interests.interest_present(t.interest).value_or(true));
        EXPECT_FALSE(cells.cell_present(t.interest).value_or(true));
        EXPECT_FALSE(
            ledger.replay_child_terminal_witness(t.interest).has_value());
    });
}

TEST_F(SeamsFixture, OffStrandSeamCommitsAreRefused)
{
    Triple t;
    run([&] { t = opened_triple(); });

    bool checked = false;
    asio::post(sched.io(), [&] {
        EXPECT_EQ(
            seams.open_interest_with_cell(ChildLease{}, 3, 4, kEpoch)
                .refusal,
            SR::off_strand);
        EXPECT_EQ(seams.sponsor_use(t.interest, t.grant), SR::off_strand);
        EXPECT_EQ(seams.finish_use(t.interest, t.grant), SR::off_strand);
        EXPECT_EQ(
            seams.publish_for_interest(t.child, 9, 4), LR::off_strand);
        EXPECT_EQ(
            seams.retire_interest_terminal(ChildTerminalWitnessTestMint::mint(
                kScope, t.interest, 100, 7)),
            SR::off_strand);
        checked = true;
    });
    sched.run_until_idle();
    EXPECT_TRUE(checked);

    run([&] {
        EXPECT_EQ(seams.sponsor_use(t.interest, t.grant), SR::ok)
            << "the off-strand refusals committed nothing";
    });
}
