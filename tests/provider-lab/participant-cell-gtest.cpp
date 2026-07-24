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

    CellRevision
    opened_cell(InterestId interest = 1)
    {
        auto r = cells.open_cell(interest, 10, 100, kEpoch);
        EXPECT_EQ(r.refusal, ParticipantCellRegistry::Refusal::ok);
        return r.revision;
    }

    /// A revoke command as 3c would mint it, bound to the COMPLETE
    /// destination of this fixture's cell instance (test-only mint).
    RevokeCommand
    command_for(
        InterestId interest,
        CellRevision revision,
        RevokeCommandId id = 77)
    {
        return RevokeCommandTestMint::mint(
            id, interest, 10, 100,
            ParticipantDestination{kScope, kEpoch, revision});
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
        EXPECT_EQ(cells.open_cell(1, 10, 100, kEpoch).refusal,
                  R::already_open);

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

        // Scope, epoch, revision, fetch, operation: each mismatch
        // alone refuses BEFORE any state changes.
        EXPECT_EQ(
            cells
                .revoke_cell(RevokeCommandTestMint::mint(
                    77, 1, 10, 100,
                    ParticipantDestination{PartitionScope{1, 2}, kEpoch,
                                           rev}))
                .refusal,
            R::wrong_binding);
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
        ASSERT_EQ(cells.retire_cell(consumed_for(*revoke.receipt)), R::ok);

        // Instance R2: same interest, same fetch/operation/epoch.
        auto reopen = cells.open_cell(1, 10, 100, kEpoch);
        ASSERT_EQ(reopen.refusal, R::ok);
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

TEST_F(CellFixture, CrossRegistryArtifactsRefuseDespiteCollidingCounters)
{
    // Two instances with identical LOCAL counters but different
    // partition scopes: everything each mints must be refused by the
    // other on scope alone.
    DeterministicScheduler s2;
    auto strand2 = s2.make_strand();
    ParticipantCellRegistry other{strand2, PartitionScope{1, 2}};
    CellRevision rev_b = 0;
    asio::post(strand2, [&] {
        auto r = other.open_cell(1, 10, 100, kEpoch);
        ASSERT_EQ(r.refusal, R::ok);
        rev_b = r.revision;
    });
    s2.run_until_idle();

    run([&] {
        auto rev_a = opened_cell(1);
        ASSERT_EQ(rev_a, rev_b)
            << "the collision is real: both instances minted the same "
               "local revision";

        // A command destined for partition {1,2} presented to {1,1}.
        auto foreign = RevokeCommandTestMint::mint(
            77, 1, 10, 100,
            ParticipantDestination{PartitionScope{1, 2}, kEpoch, rev_b});
        EXPECT_EQ(cells.revoke_cell(foreign).refusal, R::wrong_binding);

        // A consumption witness minted against {1,2}'s receipt cannot
        // retire {1,1}'s cell either.
        auto own = cells.revoke_cell(command_for(1, rev_a));
        ASSERT_EQ(own.refusal, R::ok);
        EXPECT_EQ(
            cells.retire_cell(ParticipantConsumedTestMint::mint(
                PartitionScope{1, 2}, kEpoch, 1, 77, rev_b)),
            R::wrong_binding);
    });
}

TEST_F(CellFixture, RetireConsumesTheWitnessNotTheCellsOwnReceipt)
{
    run([&] {
        auto rev = opened_cell();
        EXPECT_EQ(
            cells.retire_cell(ParticipantConsumedTestMint::mint(
                kScope, kEpoch, 1, 77, rev)),
            R::not_revoked)
            << "an active cell cannot be retired, witness or not";

        auto revoke = cells.revoke_cell(command_for(1, rev));
        ASSERT_EQ(revoke.refusal, R::ok);

        // NOTE: presenting the cell's own outgoing receipt does not
        // compile — retire_cell only accepts the interest-minted
        // ParticipantReceiptConsumed. Runtime binding is checked too:
        EXPECT_EQ(
            cells.retire_cell(ParticipantConsumedTestMint::mint(
                kScope, kEpoch, 1, 78, rev)),
            R::wrong_binding);
        EXPECT_EQ(
            cells.retire_cell(ParticipantConsumedTestMint::mint(
                kScope, kEpoch + 1, 1, 77, rev)),
            R::wrong_binding);

        // The receipt replay home survives until the matching witness
        // arrives: replay still works pre-retirement.
        auto replay = cells.revoke_cell(command_for(1, rev));
        ASSERT_EQ(replay.refusal, R::ok);
        ASSERT_TRUE(replay.receipt.has_value());

        ASSERT_EQ(cells.retire_cell(consumed_for(*replay.receipt)), R::ok);
        EXPECT_FALSE(cells.cell_present(1).value_or(true));
        EXPECT_EQ(
            cells.retire_cell(consumed_for(*replay.receipt)),
            R::unknown_cell)
            << "removed exactly once, typed on the second attempt";
    });
}

TEST_F(CellFixture, MintCeilingsFailClosedWithNothingMutated)
{
    {
        DeterministicScheduler s2;
        auto strand2 = s2.make_strand();
        ParticipantCellRegistry c2{
            strand2, kScope, std::numeric_limits<CellRevision>::max(), 1};
        asio::post(strand2, [&] {
            EXPECT_EQ(c2.open_cell(1, 10, 100, kEpoch).refusal,
                      R::revision_exhausted);
            EXPECT_FALSE(c2.cell_present(1).value_or(true));
            EXPECT_EQ(c2.open_cell(1, 10, 100, kEpoch).refusal,
                      R::revision_exhausted);
        });
        s2.run_until_idle();
    }
    {
        DeterministicScheduler s2;
        auto strand2 = s2.make_strand();
        ParticipantCellRegistry c2{
            strand2, kScope, 1, std::numeric_limits<GrantId>::max()};
        asio::post(strand2, [&] {
            auto open = c2.open_cell(1, 10, 100, kEpoch);
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
        EXPECT_EQ(cells.open_cell(2, 10, 100, kEpoch).refusal,
                  R::off_strand);
        EXPECT_EQ(cells.issue_grant(1, 1).refusal, R::off_strand);
        EXPECT_EQ(
            cells.revoke_cell(command_for(1, 1)).refusal, R::off_strand);
        EXPECT_EQ(cells.retire_cell(*witness), R::off_strand)
            << "even the matching witness cannot retire off-strand";
        EXPECT_FALSE(cells.cell_revision(1).has_value());
        EXPECT_FALSE(cells.cell_present(1).has_value());
        checked = true;
    });
    sched.run_until_idle();
    EXPECT_TRUE(checked);

    // The refusals read nothing and consumed nothing: the same
    // witness still retires on the owner strand.
    run([&] { EXPECT_EQ(cells.retire_cell(*witness), R::ok); });
}
