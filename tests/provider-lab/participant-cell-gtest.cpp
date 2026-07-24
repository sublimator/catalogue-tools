#include "participant-cell.h"

#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

#include <limits>
#include <utility>

using namespace catl::provider_lab;
namespace asio = boost::asio;

namespace {

struct CellFixture : ::testing::Test
{
    DeterministicScheduler sched;
    DeterministicScheduler::strand_type strand = sched.make_strand();
    ParticipantCellRegistry cells{strand};

    template <typename Fn>
    void
    run(Fn fn)
    {
        asio::post(strand, std::move(fn));
        sched.run_until_idle();
    }

    /// Open a cell for interest 1 on fetch 10 / operation 100 and
    /// return its revision.
    CellRevision
    opened_cell(InterestId interest = 1)
    {
        auto r = cells.open_cell(interest, 10, 100);
        EXPECT_EQ(r.refusal, ParticipantCellRegistry::Refusal::ok);
        return r.revision;
    }

    /// The sender-owned revoke command as 3c would mint it for this
    /// cell's binding (test-only mint: RevokeCommand is sole-minted
    /// by the interest registry in production).
    RevokeCommand
    command_for(InterestId interest, RevokeCommandId id = 77)
    {
        return RevokeCommandTestMint::mint(id, interest, 10, 100);
    }
};

using R = ParticipantCellRegistry::Refusal;
using G = ParticipantCellRegistry::GrantState;

}  // namespace

TEST_F(CellFixture, GrantLifecycleSpendsExactlyOnce)
{
    run([&] {
        auto rev = opened_cell();
        auto g = cells.issue_grant(1, rev);
        ASSERT_EQ(g.refusal, R::ok);
        EXPECT_EQ(cells.grant_state(1, g.grant), G::unspent);

        ASSERT_EQ(cells.spend_grant(1, rev, g.grant), R::ok);
        EXPECT_EQ(cells.grant_state(1, g.grant), G::use_open);
        EXPECT_EQ(cells.spend_grant(1, rev, g.grant), R::already_spent)
            << "a spent grant cannot respend";

        ASSERT_EQ(cells.finish_use(1, rev, g.grant), R::ok);
        EXPECT_EQ(cells.grant_state(1, g.grant), G::finished);
        EXPECT_EQ(cells.spend_grant(1, rev, g.grant), R::already_finished);
        EXPECT_EQ(cells.finish_use(1, rev, g.grant), R::already_finished);

        // Finish without spend is typed too.
        auto g2 = cells.issue_grant(1, rev);
        EXPECT_EQ(cells.finish_use(1, rev, g2.grant), R::not_spent);
    });
}

TEST_F(CellFixture, RevisionMismatchRefusesBeforeAnyGrantRead)
{
    run([&] {
        auto rev_a = opened_cell(1);
        auto rev_b = opened_cell(2);
        ASSERT_NE(rev_a, rev_b)
            << "one global mint: no two cell instances share a revision";

        auto g = cells.issue_grant(1, rev_a);
        ASSERT_EQ(g.refusal, R::ok);

        // Cross-cell revision confusion: cell 2's revision presented
        // against cell 1 refuses even though the grant id is valid —
        // identity dominates reads of reclaimable state.
        EXPECT_EQ(
            cells.spend_grant(1, rev_b, g.grant), R::revision_mismatch);
        EXPECT_EQ(
            cells.finish_use(1, rev_b, g.grant), R::revision_mismatch);
        EXPECT_EQ(cells.issue_grant(1, rev_b).refusal,
                  R::revision_mismatch);
        EXPECT_EQ(cells.grant_state(1, g.grant), G::unspent)
            << "nothing moved under the mismatched revision";
    });
}

TEST_F(CellFixture, RevokeKillsUnspentGrantsAtomicallySparesOpenUses)
{
    run([&] {
        auto rev = opened_cell();
        auto spent = cells.issue_grant(1, rev);
        auto unspent = cells.issue_grant(1, rev);
        ASSERT_EQ(cells.spend_grant(1, rev, spent.grant), R::ok);

        auto revoke = cells.revoke_cell(command_for(1));
        ASSERT_EQ(revoke.refusal, R::ok);
        ASSERT_TRUE(revoke.receipt.has_value());

        EXPECT_EQ(cells.grant_state(1, unspent.grant), G::revoked_unspent)
            << "revoke kills every unspent grant in the same commit";
        EXPECT_EQ(cells.grant_state(1, spent.grant), G::use_open)
            << "a submitted use retains the original cell until finish";
        EXPECT_EQ(
            cells.spend_grant(1, rev, unspent.grant), R::grant_revoked);
        EXPECT_EQ(cells.issue_grant(1, rev).refusal, R::cell_revoked)
            << "no new authority from a revoked cell";
        ASSERT_EQ(cells.finish_use(1, rev, spent.grant), R::ok)
            << "in-flight work drains under retained authority";
    });
}

TEST_F(CellFixture, DetachThenFinishAndFinishThenDetachBothConverge)
{
    for (bool detach_first : {true, false})
    {
        DeterministicScheduler s2;
        auto strand2 = s2.make_strand();
        ParticipantCellRegistry c2{strand2};
        asio::post(strand2, [&] {
            auto open = c2.open_cell(1, 10, 100);
            ASSERT_EQ(open.refusal, R::ok);
            auto g = c2.issue_grant(1, open.revision);
            ASSERT_EQ(c2.spend_grant(1, open.revision, g.grant), R::ok);

            auto const cmd = RevokeCommandTestMint::mint(77, 1, 10, 100);
            std::optional<ParticipantRevokedReceipt> receipt;
            if (detach_first)
            {
                auto r = c2.revoke_cell(cmd);
                ASSERT_EQ(r.refusal, R::ok);
                receipt = r.receipt;
                EXPECT_EQ(c2.retire_cell(*receipt), R::uses_open)
                    << "drain gates retirement while the use is open";
                ASSERT_EQ(
                    c2.finish_use(1, open.revision, g.grant), R::ok);
            }
            else
            {
                ASSERT_EQ(
                    c2.finish_use(1, open.revision, g.grant), R::ok);
                auto r = c2.revoke_cell(cmd);
                ASSERT_EQ(r.refusal, R::ok);
                receipt = r.receipt;
            }
            EXPECT_TRUE(c2.cell_present(1).value_or(false));
            ASSERT_EQ(c2.retire_cell(*receipt), R::ok);
            EXPECT_FALSE(c2.cell_present(1).value_or(true));
            EXPECT_EQ(c2.retire_cell(*receipt), R::unknown_cell)
                << "removed exactly once, typed on the second attempt";
        });
        s2.run_until_idle();
    }
}

TEST_F(CellFixture, RevokeAuthenticatesDestinationAndReplaysIdentically)
{
    run([&] {
        auto rev = opened_cell();

        // A command addressed to a different fetch or operation must
        // refuse BEFORE any state changes: a receipt authenticates
        // where the command was addressed.
        EXPECT_EQ(
            cells.revoke_cell(RevokeCommandTestMint::mint(77, 1, 11, 100))
                .refusal,
            R::wrong_binding);
        EXPECT_EQ(
            cells.revoke_cell(RevokeCommandTestMint::mint(77, 1, 10, 101))
                .refusal,
            R::wrong_binding);
        EXPECT_EQ(cells.issue_grant(1, rev).refusal, R::ok)
            << "the misaddressed command changed nothing";

        auto first = cells.revoke_cell(command_for(1));
        ASSERT_EQ(first.refusal, R::ok);
        EXPECT_EQ(first.receipt->interest(), 1u);
        EXPECT_EQ(first.receipt->command(), 77u);
        EXPECT_EQ(first.receipt->revision(), rev);

        // The caller loses the receipt; replaying the identical
        // command re-derives the IDENTICAL retained witness.
        auto replay = cells.revoke_cell(command_for(1));
        ASSERT_EQ(replay.refusal, R::ok);
        EXPECT_TRUE(*replay.receipt == *first.receipt);

        // A DIFFERENT command against the revoked cell never mints a
        // second receipt.
        EXPECT_EQ(
            cells.revoke_cell(command_for(1, 78)).refusal,
            R::wrong_binding);
    });
}

TEST_F(CellFixture, RetireRequiresPossessionOfTheMatchingWitness)
{
    run([&] {
        auto rev = opened_cell();
        auto pre = ParticipantReceiptTestMint::mint(1, 77, rev);
        EXPECT_EQ(cells.retire_cell(pre), R::not_revoked)
            << "an active cell cannot be retired, witness or not";

        auto revoke = cells.revoke_cell(command_for(1));
        ASSERT_EQ(revoke.refusal, R::ok);

        // A receipt with any mismatched binding cannot retire the
        // replay home.
        EXPECT_EQ(
            cells.retire_cell(ParticipantReceiptTestMint::mint(1, 78, rev)),
            R::wrong_binding);
        EXPECT_EQ(
            cells.retire_cell(
                ParticipantReceiptTestMint::mint(1, 77, rev + 1)),
            R::wrong_binding);
        ASSERT_EQ(cells.retire_cell(*revoke.receipt), R::ok);
    });
}

TEST_F(CellFixture, ReopenAfterRetireMintsFreshRevisionStaleRefuses)
{
    run([&] {
        auto rev1 = opened_cell();
        auto g1 = cells.issue_grant(1, rev1);
        ASSERT_EQ(g1.refusal, R::ok);
        EXPECT_EQ(cells.open_cell(1, 10, 100).refusal, R::already_open);

        auto revoke = cells.revoke_cell(command_for(1));
        ASSERT_EQ(cells.retire_cell(*revoke.receipt), R::ok);

        auto reopen = cells.open_cell(1, 10, 100);
        ASSERT_EQ(reopen.refusal, R::ok);
        EXPECT_GT(reopen.revision, rev1)
            << "reopen is a NEW cell instance, never a resurrection";

        // Stale authority from the retired instance refuses against
        // the new one: revision first, so the dead grant id is never
        // even consulted.
        EXPECT_EQ(
            cells.spend_grant(1, rev1, g1.grant), R::revision_mismatch);
        EXPECT_EQ(cells.grant_state(1, g1.grant), std::nullopt)
            << "the retired instance's grants have no home here";
    });
}

TEST_F(CellFixture, MintCeilingsFailClosedWithNothingMutated)
{
    // Revision ceiling: open refuses, mints nothing, and a second
    // attempt refuses identically (the counter did not move).
    {
        DeterministicScheduler s2;
        auto strand2 = s2.make_strand();
        ParticipantCellRegistry c2{
            strand2, std::numeric_limits<CellRevision>::max(), 1};
        asio::post(strand2, [&] {
            EXPECT_EQ(c2.open_cell(1, 10, 100).refusal,
                      R::revision_exhausted);
            EXPECT_FALSE(c2.cell_present(1).value_or(true));
            EXPECT_EQ(c2.open_cell(1, 10, 100).refusal,
                      R::revision_exhausted);
        });
        s2.run_until_idle();
    }
    // Grant ceiling: issue refuses with the cell intact and no grant
    // minted; zero stays reserved for both mints.
    {
        DeterministicScheduler s2;
        auto strand2 = s2.make_strand();
        ParticipantCellRegistry c2{
            strand2, 1, std::numeric_limits<GrantId>::max()};
        asio::post(strand2, [&] {
            auto open = c2.open_cell(1, 10, 100);
            ASSERT_EQ(open.refusal, R::ok);
            auto g = c2.issue_grant(1, open.revision);
            EXPECT_EQ(g.refusal, R::grant_id_exhausted);
            EXPECT_EQ(g.grant, 0u);
            EXPECT_EQ(c2.grant_state(1, 0), std::nullopt)
                << "zero reserved; nothing minted";
            EXPECT_EQ(c2.issue_grant(1, open.revision).refusal,
                      R::grant_id_exhausted);
        });
        s2.run_until_idle();
    }
}

TEST_F(CellFixture, ComposedSeamRoundTripWithInterestRegistry)
{
    // The full 3c<->3d seam, no test mints anywhere: the interest
    // registry commits the owned revoke command; the participant cell
    // consumes it and is the sole production minter of the receipt;
    // the interest registry consumes only that bound receipt.
    InterestRegistry reg{strand};
    run([&] {
        auto r = reg.register_interest(100, 7, 10, 900);
        ASSERT_EQ(r.refusal, InterestRegistry::Refusal::ok);
        auto open = cells.open_cell(r.interest, 10, 100);
        ASSERT_EQ(open.refusal, R::ok);
        auto g = cells.issue_grant(r.interest, open.revision);
        ASSERT_EQ(
            cells.spend_grant(r.interest, open.revision, g.grant), R::ok);
        ASSERT_EQ(reg.add_submitted_use(r.interest, g.grant),
                  InterestRegistry::Refusal::ok);

        // Detach: 3c commits the command; 3d consumes it and mints.
        auto d = reg.begin_detach(r.interest);
        ASSERT_EQ(d.refusal, InterestRegistry::Refusal::ok);
        auto revoke = cells.revoke_cell(*d.command);
        ASSERT_EQ(revoke.refusal, R::ok);
        ASSERT_EQ(reg.consume_revoke_receipt(*revoke.receipt),
                  InterestRegistry::Refusal::ok)
            << "the production-minted receipt satisfies 3c's binding";

        // Drain both sides through the owned verbs.
        ASSERT_EQ(
            cells.finish_use(r.interest, open.revision, g.grant), R::ok);
        ASSERT_EQ(reg.use_terminal(r.interest, g.grant),
                  InterestRegistry::Refusal::ok);
        ASSERT_EQ(
            reg.consume_unsubscribe_receipt(
                UnsubscribeReceiptTestMint::mint(r.interest, 900)),
            InterestRegistry::Refusal::ok);

        auto join = reg.terminal_join(r.interest);
        ASSERT_EQ(join.refusal, InterestRegistry::JoinRefusal::ok);
        ASSERT_EQ(cells.retire_cell(*revoke.receipt), R::ok);
        ASSERT_EQ(reg.retire_terminal(*join.ack),
                  InterestRegistry::Refusal::ok);
        EXPECT_FALSE(cells.cell_present(r.interest).value_or(true));
        EXPECT_FALSE(reg.interest_present(r.interest).value_or(true));
    });
}

TEST_F(CellFixture, OffStrandTouchesAreRefusedBeforeAnyRead)
{
    std::optional<ParticipantRevokedReceipt> receipt;
    run([&] {
        opened_cell();
        auto r = cells.revoke_cell(command_for(1));
        ASSERT_EQ(r.refusal, R::ok);
        receipt = r.receipt;
    });

    bool checked = false;
    asio::post(sched.io(), [&] {
        EXPECT_EQ(cells.open_cell(2, 10, 100).refusal, R::off_strand);
        EXPECT_EQ(cells.issue_grant(1, 1).refusal, R::off_strand);
        EXPECT_EQ(cells.spend_grant(1, 1, 1), R::off_strand);
        EXPECT_EQ(cells.finish_use(1, 1, 1), R::off_strand);
        EXPECT_EQ(cells.revoke_cell(command_for(1)).refusal,
                  R::off_strand);
        EXPECT_EQ(cells.retire_cell(*receipt), R::off_strand)
            << "even the genuine witness cannot retire off-strand";
        EXPECT_FALSE(cells.cell_revision(1).has_value());
        EXPECT_FALSE(cells.cell_present(1).has_value());
        checked = true;
    });
    sched.run_until_idle();
    EXPECT_TRUE(checked);

    // The refusals read nothing and consumed nothing: the same
    // witness still retires on the owner strand.
    run([&] { EXPECT_EQ(cells.retire_cell(*receipt), R::ok); });
}
