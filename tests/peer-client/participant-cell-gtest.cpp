#include "participant-cell.h"

#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

#include <limits>
#include <optional>
#include <utility>

using namespace catl::provider_lab;
namespace asio = boost::asio;

namespace {

PartitionScope const kScope{1, 1};
PartitionScope const kForeign{1, 2};
ProviderEpochId const kEpoch = 5;

struct CellFixture : ::testing::Test
{
    DeterministicScheduler sched;
    DeterministicScheduler::strand_type strand = sched.make_strand();
    ParticipantCellRegistry cells{strand, kScope};

    template <typename Fn>
    void
    run(Fn fn)
    {
        asio::post(strand, std::move(fn));
        sched.run_until_idle();
    }

    /// Component-test cell creation (production cells exist only as
    /// half of the façade's atomic interest/cell pair).
    CellRevision
    opened_cell(InterestId interest = 1)
    {
        auto r = ParticipantCellTestAccess::open(
            cells, interest, 10, 100, kEpoch);
        EXPECT_TRUE(r.ok);
        return r.revision;
    }

    /// A revoke command as 3c would mint it, bound to the COMPLETE
    /// destination of this fixture's cell instance (test-only mint).
    static RevokeCommand
    command_for(
        InterestId interest,
        CellRevision revision,
        RevokeCommandId id = 77,
        PartitionScope scope = kScope)
    {
        return RevokeCommandTestMint::mint(
            id, interest, 10, 100,
            ParticipantDestination{scope, kEpoch, revision});
    }

    /// The interest's third-phase witness matching a genuine receipt.
    static ParticipantReceiptConsumed
    consumed_for(ParticipantRevokedReceipt const& receipt)
    {
        return ParticipantConsumedTestMint::mint(
            receipt.scope(), receipt.epoch(), receipt.interest(),
            receipt.command(), receipt.revision());
    }
};

using R = ParticipantCellRegistry::Refusal;
using G = ParticipantCellRegistry::GrantState;

}  // namespace

TEST_F(CellFixture, IssueRequiresTheCurrentRevisionAndAnActiveCell)
{
    run([&] {
        auto rev_a = opened_cell(1);
        auto rev_b = opened_cell(2);
        ASSERT_NE(rev_a, rev_b)
            << "revisions are unique within this registry instance";
        EXPECT_FALSE(
            ParticipantCellTestAccess::open(cells, 1, 10, 100, kEpoch).ok)
            << "one cell per interest key";

        auto g = cells.issue_grant(1, rev_a);
        ASSERT_EQ(g.refusal, R::ok);
        EXPECT_EQ(cells.grant_state(1, g.grant), G::unspent);

        // Cross-cell revision confusion refuses before any grant read.
        EXPECT_EQ(cells.issue_grant(1, rev_b).refusal,
                  R::revision_mismatch);

        auto revoke = cells.revoke_cell(command_for(1, rev_a));
        ASSERT_EQ(revoke.refusal, R::ok);
        EXPECT_EQ(cells.issue_grant(1, rev_a).refusal, R::cell_revoked)
            << "no new authority from a revoked cell";
    });
}

TEST_F(CellFixture, ForeignScopeRefusesBeforeAnyLookup)
{
    run([&] {
        auto rev = opened_cell(1);

        // Foreign-scope command: SAME refusal for a present and an
        // absent interest id — presence is never leaked before
        // authentication.
        EXPECT_EQ(
            cells.revoke_cell(command_for(1, rev, 77, kForeign)).refusal,
            R::wrong_scope);
        EXPECT_EQ(
            cells.revoke_cell(command_for(999, rev, 77, kForeign))
                .refusal,
            R::wrong_scope)
            << "absent id: identical refusal, no unknown_cell leak";

        // Foreign-scope witness: same discipline on the retire path.
        EXPECT_EQ(
            ParticipantCellTestAccess::retire(cells,ParticipantConsumedTestMint::mint(
                kForeign, kEpoch, 1, 77, rev)),
            R::wrong_scope);
        EXPECT_EQ(
            ParticipantCellTestAccess::retire(cells,ParticipantConsumedTestMint::mint(
                kForeign, kEpoch, 999, 77, rev)),
            R::wrong_scope);

        EXPECT_EQ(cells.issue_grant(1, rev).refusal, R::ok)
            << "the foreign artifacts changed nothing";
    });
}

TEST_F(CellFixture, RevokeKillsUnspentGrantsAtomically)
{
    run([&] {
        auto rev = opened_cell();
        auto g1 = cells.issue_grant(1, rev);
        auto g2 = cells.issue_grant(1, rev);
        ASSERT_EQ(g1.refusal, R::ok);
        ASSERT_EQ(g2.refusal, R::ok);

        auto revoke = cells.revoke_cell(command_for(1, rev));
        ASSERT_EQ(revoke.refusal, R::ok);
        EXPECT_EQ(cells.grant_state(1, g1.grant), G::revoked_unspent);
        EXPECT_EQ(cells.grant_state(1, g2.grant), G::revoked_unspent)
            << "every unspent grant dies in the same commit";
    });
}

TEST_F(CellFixture, RevokeAuthenticatesTheCompleteDestination)
{
    run([&] {
        auto rev = opened_cell();

        // Epoch, revision, fetch, operation: each mismatch alone
        // refuses (scope has its own scope-first control above).
        EXPECT_EQ(
            cells
                .revoke_cell(RevokeCommandTestMint::mint(
                    77, 1, 10, 100,
                    ParticipantDestination{kScope, kEpoch + 1, rev}))
                .refusal,
            R::wrong_binding);
        EXPECT_EQ(
            cells.revoke_cell(command_for(1, rev + 1)).refusal,
            R::wrong_binding)
            << "wrong target revision refuses (the ABA gate)";
        EXPECT_EQ(
            cells
                .revoke_cell(RevokeCommandTestMint::mint(
                    77, 1, 11, 100,
                    ParticipantDestination{kScope, kEpoch, rev}))
                .refusal,
            R::wrong_binding);
        EXPECT_EQ(
            cells
                .revoke_cell(RevokeCommandTestMint::mint(
                    77, 1, 10, 101,
                    ParticipantDestination{kScope, kEpoch, rev}))
                .refusal,
            R::wrong_binding);
        EXPECT_EQ(cells.issue_grant(1, rev).refusal, R::ok)
            << "the misaddressed commands changed nothing";

        auto first = cells.revoke_cell(command_for(1, rev));
        ASSERT_EQ(first.refusal, R::ok);
        EXPECT_EQ(first.receipt->scope(), kScope);
        EXPECT_EQ(first.receipt->epoch(), kEpoch);
        EXPECT_EQ(first.receipt->revision(), rev);

        // Identical replay re-derives the IDENTICAL retained receipt;
        // a different command never mints a second one.
        auto replay = cells.revoke_cell(command_for(1, rev));
        ASSERT_EQ(replay.refusal, R::ok);
        EXPECT_TRUE(*replay.receipt == *first.receipt);
        EXPECT_EQ(
            cells.revoke_cell(command_for(1, rev, 78)).refusal,
            R::wrong_binding);
    });
}

TEST_F(CellFixture, StaleCommandFromRetiredInstanceCannotTouchReopened)
{
    run([&] {
        // Instance R1: revoke, consume (witness), retire.
        auto rev1 = opened_cell();
        auto stale = command_for(1, rev1);  // the delayed copy
        auto revoke = cells.revoke_cell(stale);
        ASSERT_EQ(revoke.refusal, R::ok);
        ASSERT_EQ(ParticipantCellTestAccess::retire(cells,consumed_for(*revoke.receipt)), R::ok);

        // Instance R2: same interest key, same fetch/operation/epoch.
        auto reopen = ParticipantCellTestAccess::open(
            cells, 1, 10, 100, kEpoch);
        ASSERT_TRUE(reopen.ok);
        ASSERT_GT(reopen.revision, rev1);

        // The delayed R1 command arrives: it must NOT acquire
        // authority over the fresh instance.
        EXPECT_EQ(cells.revoke_cell(stale).refusal, R::wrong_binding)
            << "a stale command from a retired instance is refused by "
               "its revision binding";
        EXPECT_EQ(cells.issue_grant(1, reopen.revision).refusal, R::ok)
            << "R2 remains fully active";
    });
}

TEST_F(CellFixture, RetireConsumesTheWitnessNotTheCellsOwnReceipt)
{
    run([&] {
        auto rev = opened_cell();
        EXPECT_EQ(
            ParticipantCellTestAccess::retire(cells,ParticipantConsumedTestMint::mint(
                kScope, kEpoch, 1, 77, rev)),
            R::not_revoked)
            << "an active cell cannot be retired, witness or not";

        auto revoke = cells.revoke_cell(command_for(1, rev));
        ASSERT_EQ(revoke.refusal, R::ok);

        // NOTE: presenting the cell's own outgoing receipt does not
        // compile — retire_cell only accepts the interest-minted
        // ParticipantReceiptConsumed. Runtime binding is checked too:
        EXPECT_EQ(
            ParticipantCellTestAccess::retire(cells,ParticipantConsumedTestMint::mint(
                kScope, kEpoch, 1, 78, rev)),
            R::wrong_binding);
        EXPECT_EQ(
            ParticipantCellTestAccess::retire(cells,ParticipantConsumedTestMint::mint(
                kScope, kEpoch + 1, 1, 77, rev)),
            R::wrong_binding);

        // The receipt replay home survives until the matching witness
        // arrives: replay still works pre-retirement.
        auto replay = cells.revoke_cell(command_for(1, rev));
        ASSERT_EQ(replay.refusal, R::ok);
        ASSERT_TRUE(replay.receipt.has_value());

        ASSERT_EQ(ParticipantCellTestAccess::retire(cells,consumed_for(*replay.receipt)), R::ok);
        EXPECT_FALSE(cells.cell_present(1).value_or(true));
        EXPECT_EQ(
            ParticipantCellTestAccess::retire(cells,consumed_for(*replay.receipt)),
            R::unknown_cell)
            << "removed exactly once, typed on the second attempt";
    });
}

TEST_F(CellFixture, MintCeilingsFailClosedWithNothingMutated)
{
    // Revision ceiling: creation refuses (typed at the façade seam;
    // the component test access reports the same fail-closed refusal).
    {
        DeterministicScheduler s2;
        auto strand2 = s2.make_strand();
        ParticipantCellRegistry c2{
            strand2, kScope, std::numeric_limits<CellRevision>::max(), 1};
        asio::post(strand2, [&] {
            EXPECT_FALSE(
                ParticipantCellTestAccess::open(c2, 1, 10, 100, kEpoch).ok);
            EXPECT_FALSE(c2.cell_present(1).value_or(true));
            EXPECT_FALSE(
                ParticipantCellTestAccess::open(c2, 1, 10, 100, kEpoch).ok)
                << "the counter did not move";
        });
        s2.run_until_idle();
    }
    // Grant ceiling: issue refuses with the cell intact and no grant
    // minted; zero stays reserved for both mints.
    {
        DeterministicScheduler s2;
        auto strand2 = s2.make_strand();
        ParticipantCellRegistry c2{
            strand2, kScope, 1, std::numeric_limits<GrantId>::max()};
        asio::post(strand2, [&] {
            auto open = ParticipantCellTestAccess::open(
                c2, 1, 10, 100, kEpoch);
            ASSERT_TRUE(open.ok);
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

TEST_F(CellFixture, OffStrandTouchesAreRefusedBeforeAnyRead)
{
    std::optional<ParticipantReceiptConsumed> witness;
    run([&] {
        auto rev = opened_cell();
        auto r = cells.revoke_cell(command_for(1, rev));
        ASSERT_EQ(r.refusal, R::ok);
        witness = consumed_for(*r.receipt);
    });

    bool checked = false;
    asio::post(sched.io(), [&] {
        EXPECT_FALSE(
            ParticipantCellTestAccess::open(cells, 2, 10, 100, kEpoch).ok);
        EXPECT_EQ(cells.issue_grant(1, 1).refusal, R::off_strand);
        EXPECT_EQ(
            cells.revoke_cell(command_for(1, 1)).refusal, R::off_strand)
            << "off_strand dominates even the scope gate";
        EXPECT_EQ(ParticipantCellTestAccess::retire(cells,*witness), R::off_strand)
            << "even the matching witness cannot retire off-strand";
        EXPECT_FALSE(cells.cell_revision(1).has_value());
        EXPECT_FALSE(cells.cell_present(1).has_value());
        checked = true;
    });
    sched.run_until_idle();
    EXPECT_TRUE(checked);

    // The refusals read nothing and consumed nothing: the same
    // witness still retires on the owner strand.
    run([&] { EXPECT_EQ(ParticipantCellTestAccess::retire(cells,*witness), R::ok); });
}
