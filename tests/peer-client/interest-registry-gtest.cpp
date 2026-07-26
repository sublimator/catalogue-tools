#include "interest-registry.h"

#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

#include <limits>
#include <utility>

using namespace catl::provider_lab;
namespace asio = boost::asio;

namespace {

PartitionScope const kScope{1, 1};
PartitionScope const kForeign{1, 2};
ProviderEpochId const kEpoch = 5;
ParticipantRevision const kRevision = 3;
ParticipantDestination const kDest{kScope, kEpoch, kRevision};

struct RegistryFixture : ::testing::Test
{
    DeterministicScheduler sched;
    DeterministicScheduler::strand_type strand = sched.make_strand();
    InterestRegistry reg{strand, kScope};

    template <typename Fn>
    void
    run(Fn fn)
    {
        asio::post(strand, std::move(fn));
        sched.run_until_idle();
    }

    /// Component-test row creation (production rows are minted only
    /// by the façade's atomic open_interest_with_cell).
    InterestId
    created(
        InterestOperationId operation = 100,
        InterestChildId child = 7,
        SubscriptionId subscription = 900)
    {
        auto r = InterestRegistryTestAccess::create(
            reg, operation, child, 1, subscription, kDest);
        EXPECT_TRUE(r.ok);
        return r.interest;
    }

    /// Drive one interest to detach_pending with revoke and
    /// unsubscribe consumed through the owned seams; returns its id.
    InterestId
    drained_interest()
    {
        auto const id = created();
        auto d = reg.begin_detach(id);
        EXPECT_EQ(d.refusal, InterestRegistry::Refusal::ok);
        EXPECT_EQ(
            reg.consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                                           kScope, kEpoch, id,
                                           d.command->command(),
                                           kRevision))
                .refusal,
            InterestRegistry::Refusal::ok);
        EXPECT_EQ(
            reg.consume_unsubscribe_receipt(
                UnsubscribeReceiptTestMint::mint(kScope, id, 900)),
            InterestRegistry::Refusal::ok);
        return id;
    }
};

using R = InterestRegistry::Refusal;
using J = InterestRegistry::JoinRefusal;

}  // namespace

TEST_F(RegistryFixture, DetachCommitsAFullyBoundReplayableRevokeCommand)
{
    run([&] {
        auto const id = created();

        auto d = reg.begin_detach(id);
        ASSERT_EQ(d.refusal, R::ok)
            << "initiation is a fact with a home, not an arrow";
        auto const& cmd = *d.command;
        EXPECT_EQ(cmd.interest(), id);
        EXPECT_EQ(cmd.fetch(), 1u);
        EXPECT_EQ(cmd.operation(), 100u);
        EXPECT_GT(cmd.command(), 0u);
        EXPECT_EQ(cmd.destination(), kDest)
            << "the command carries the COMPLETE destination binding";

        // The projection is replayable, identically, until receipted.
        auto replay = reg.replay_revoke_command(id);
        ASSERT_TRUE(replay.has_value());
        EXPECT_TRUE(*replay == cmd);

        // Receipt consumption retires the command replay home.
        ASSERT_EQ(
            reg.consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                                           kScope, kEpoch, id,
                                           cmd.command(), kRevision))
                .refusal,
            R::ok);
        EXPECT_FALSE(reg.replay_revoke_command(id).has_value())
            << "a consumed receipt retires the replay home";
    });
}

TEST_F(RegistryFixture, ForeignScopeRefusesBeforeAnyLookup)
{
    run([&] {
        auto const present = created();
        ASSERT_EQ(reg.begin_detach(present).refusal, R::ok);
        auto const absent = present + 1000;

        // A foreign-scope receipt gets the SAME refusal whether its
        // interest id exists here or not: presence is never leaked
        // before authentication.
        EXPECT_EQ(
            reg.consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                                           kForeign, kEpoch, present, 1,
                                           kRevision))
                .refusal,
            R::wrong_scope);
        EXPECT_EQ(
            reg.consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                                           kForeign, kEpoch, absent, 1,
                                           kRevision))
                .refusal,
            R::wrong_scope)
            << "absent id: identical refusal, no unknown_interest leak";

        EXPECT_EQ(
            reg.consume_unsubscribe_receipt(
                UnsubscribeReceiptTestMint::mint(kForeign, present, 900)),
            R::wrong_scope);
        EXPECT_EQ(
            reg.consume_unsubscribe_receipt(
                UnsubscribeReceiptTestMint::mint(kForeign, absent, 900)),
            R::wrong_scope);
    });
}

TEST_F(RegistryFixture, RevocationValidatesTheCompleteBinding)
{
    run([&] {
        auto const iid = created();
        auto d = reg.begin_detach(iid);
        ASSERT_EQ(d.refusal, R::ok);
        auto const id = d.command->command();

        // Wrong command, wrong revision, wrong epoch: each refuses
        // BEFORE consumption. The wrong-revision case is the review's
        // named control — the receipt's revision is actually read.
        EXPECT_EQ(
            reg.consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                                           kScope, kEpoch, iid, id + 1,
                                           kRevision))
                .refusal,
            R::wrong_binding);
        EXPECT_EQ(
            reg.consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                                           kScope, kEpoch, iid, id,
                                           kRevision + 1))
                .refusal,
            R::wrong_binding)
            << "right command, wrong revision: refused pre-consumption";
        EXPECT_EQ(
            reg.consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                                           kScope, kEpoch + 1, iid, id,
                                           kRevision))
                .refusal,
            R::wrong_binding);

        auto first = reg.consume_revoke_receipt(
            ParticipantReceiptTestMint::mint(
                kScope, kEpoch, iid, id, kRevision));
        ASSERT_EQ(first.refusal, R::ok);
        ASSERT_TRUE(first.consumed.has_value());

        // Exact replay: already_acked with the IDENTICAL witness.
        auto again = reg.consume_revoke_receipt(
            ParticipantReceiptTestMint::mint(
                kScope, kEpoch, iid, id, kRevision));
        EXPECT_EQ(again.refusal, R::already_acked);
        EXPECT_TRUE(*again.consumed == *first.consumed);

        // Wrong revision AFTER consumption: still wrong_binding,
        // never laundered as already_acked.
        EXPECT_EQ(
            reg.consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                                           kScope, kEpoch, iid, id,
                                           kRevision + 1))
                .refusal,
            R::wrong_binding);

        // The witness home survives loss of every returned copy.
        auto replay = reg.replay_revoke_consumed(iid);
        ASSERT_TRUE(replay.has_value());
        EXPECT_TRUE(*replay == *first.consumed);

        // Unsubscribe: binding to the RETAINED subscription identity.
        EXPECT_EQ(
            reg.consume_unsubscribe_receipt(
                UnsubscribeReceiptTestMint::mint(kScope, iid, 901)),
            R::wrong_binding);
        EXPECT_EQ(
            reg.consume_unsubscribe_receipt(
                UnsubscribeReceiptTestMint::mint(kScope, iid, 900)),
            R::ok);
    });
}

TEST_F(RegistryFixture, TerminalJoinRetainsTheReplayableAck)
{
    run([&] {
        auto id = drained_interest();

        auto first = reg.terminal_join(id);
        ASSERT_EQ(first.refusal, J::ok);
        ASSERT_TRUE(first.ack.has_value());
        EXPECT_EQ(first.ack->child(), 7u);
        EXPECT_EQ(first.ack->scope(), kScope);
        EXPECT_TRUE(reg.interest_present(id).value_or(false))
            << "the join does not erase: the witness has a home";

        // Losing the ack is safe: the identical binding replays. The
        // row is removed ONLY by the façade's retire_interest_terminal
        // (no public retirement verb exists on this registry).
        auto replay = reg.terminal_join(id);
        ASSERT_EQ(replay.refusal, J::ok);
        EXPECT_TRUE(*replay.ack == *first.ack);
        EXPECT_TRUE(reg.interest_present(id).value_or(false));
    });
}

TEST_F(RegistryFixture, SoleInterestDetachBothOrdersDrainsUnderAuthority)
{
    for (bool detach_first : {true, false})
    {
        DeterministicScheduler s2;
        auto strand2 = s2.make_strand();
        InterestRegistry r2{strand2, kScope};
        asio::post(strand2, [&] {
            auto cr = InterestRegistryTestAccess::create(
                r2, 100, 7, 1, 900, kDest);
            ASSERT_TRUE(cr.ok);
            auto const id = cr.interest;
            ASSERT_EQ(r2.add_delivery_obligation(id, 600), R::ok);

            std::optional<RevokeCommand> cmd;
            if (detach_first)
            {
                auto d = r2.begin_detach(id);
                ASSERT_EQ(d.refusal, R::ok);
                cmd = d.command;
                ASSERT_EQ(
                    DeliveryAckTestSeam::close(r2, id, 600), R::ok);
            }
            else
            {
                ASSERT_EQ(
                    DeliveryAckTestSeam::close(r2, id, 600), R::ok);
                auto d = r2.begin_detach(id);
                ASSERT_EQ(d.refusal, R::ok);
                cmd = d.command;
            }
            EXPECT_TRUE(r2.interest_present(id).value_or(false));
            EXPECT_EQ(r2.sponsors_of(1)->size(), 0u);
            EXPECT_EQ(r2.draining_of(1)->size(), 1u)
                << "zero active sponsors is draining under retained "
                   "authority, never ownerless";

            ASSERT_EQ(
                r2.consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                                              kScope, kEpoch, id,
                                              cmd->command(), kRevision))
                    .refusal,
                R::ok);
            ASSERT_EQ(
                r2.consume_unsubscribe_receipt(
                    UnsubscribeReceiptTestMint::mint(kScope, id, 900)),
                R::ok);

            auto join = r2.terminal_join(id);
            ASSERT_EQ(join.refusal, J::ok);
            EXPECT_EQ(join.ack->operation(), 100u);
            EXPECT_EQ(join.ack->child(), 7u);
            EXPECT_TRUE(r2.interest_present(id).value_or(false))
                << "terminal-ready, awaiting the façade's retirement";
        });
        s2.run_until_idle();
    }
}

TEST_F(RegistryFixture, DuplicateDeliveryObligationsAreTypedNoNewDebt)
{
    run([&] {
        auto const id = created();
        ASSERT_EQ(reg.add_delivery_obligation(id, 600), R::ok);
        EXPECT_EQ(reg.add_delivery_obligation(id, 600),
                  R::already_registered)
            << "a duplicate must never report a fresh commit";

        auto d = reg.begin_detach(id);
        ASSERT_EQ(d.refusal, R::ok);
        ASSERT_EQ(
            reg.consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                                           kScope, kEpoch, id,
                                           d.command->command(),
                                           kRevision))
                .refusal,
            R::ok);
        ASSERT_EQ(
            reg.consume_unsubscribe_receipt(
                UnsubscribeReceiptTestMint::mint(kScope, id, 900)),
            R::ok);

        // The duplicate created no second debt: one closure clears
        // the ONE obligation and the join opens.
        EXPECT_EQ(reg.terminal_join(id).refusal, J::obligations_open);
        ASSERT_EQ(DeliveryAckTestSeam::close(reg, id, 600), R::ok);
        EXPECT_EQ(DeliveryAckTestSeam::close(reg, id, 600),
                  R::unknown_delivery)
            << "the single debt closed once";
        EXPECT_EQ(reg.terminal_join(id).refusal, J::ok);
    });
}

TEST_F(RegistryFixture, DetachSealsAndJoinGatesEveryObligationClass)
{
    run([&] {
        auto const id = created();
        ASSERT_EQ(reg.add_delivery_obligation(id, 600), R::ok);
        EXPECT_EQ(reg.terminal_join(id).refusal, J::not_detach_pending);

        auto d = reg.begin_detach(id);
        ASSERT_EQ(d.refusal, R::ok);
        EXPECT_EQ(reg.begin_detach(id).refusal, R::not_active)
            << "detach is not repeatable, and the refusal is typed";
        EXPECT_EQ(reg.add_delivery_obligation(id, 601), R::not_active)
            << "detach seals new sponsorship";

        // Each obligation class left open alone blocks the join.
        EXPECT_EQ(reg.terminal_join(id).refusal, J::obligations_open);
        ASSERT_EQ(DeliveryAckTestSeam::close(reg, id, 600), R::ok);
        EXPECT_EQ(reg.terminal_join(id).refusal, J::obligations_open)
            << "revoke consumption still outstanding";
        ASSERT_EQ(
            reg.consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                                           kScope, kEpoch, id,
                                           d.command->command(),
                                           kRevision))
                .refusal,
            R::ok);
        EXPECT_EQ(reg.terminal_join(id).refusal, J::obligations_open)
            << "unsubscribe consumption still outstanding";
        ASSERT_EQ(
            reg.consume_unsubscribe_receipt(
                UnsubscribeReceiptTestMint::mint(kScope, id, 900)),
            R::ok);
        EXPECT_EQ(reg.terminal_join(id).refusal, J::ok);
    });
}

TEST_F(RegistryFixture, NoRootAdoptionIsStructural)
{
    run([&] {
        auto const first = created(100, 7, 900);
        auto const second = created(101, 8, 901);
        EXPECT_GT(second, first);

        ASSERT_EQ(reg.begin_detach(first).refusal, R::ok);
        auto sponsors = reg.sponsors_of(1);
        ASSERT_EQ(sponsors->size(), 1u);
        EXPECT_EQ(sponsors->front(), second)
            << "shared work is owned by remaining actives; no adopted "
               "root exists";
        EXPECT_EQ(reg.draining_of(1)->size(), 1u);
    });
}

TEST(RegistryEdge, RevokeCommandCeilingRefusesWithoutSealingTheInterest)
{
    DeterministicScheduler sched;
    auto strand = sched.make_strand();
    InterestRegistry reg{
        strand, kScope, 1, std::numeric_limits<RevokeCommandId>::max()};
    asio::post(strand, [&] {
        using R = InterestRegistry::Refusal;
        auto cr = InterestRegistryTestAccess::create(
            reg, 100, 7, 1, 900, kDest);
        ASSERT_TRUE(cr.ok);
        auto d = reg.begin_detach(cr.interest);
        EXPECT_EQ(d.refusal, R::revoke_command_id_exhausted)
            << "the wrap class, refused at the command minter too";
        EXPECT_FALSE(d.command.has_value());
        EXPECT_FALSE(reg.replay_revoke_command(cr.interest).has_value())
            << "no command committed";
        EXPECT_EQ(reg.add_delivery_obligation(cr.interest, 600), R::ok)
            << "seal-plus-commit is one atom: the refused mint left "
               "the interest fully ACTIVE, not sealed commandless";
        EXPECT_EQ(reg.begin_detach(cr.interest).refusal,
                  R::revoke_command_id_exhausted);
    });
    sched.run_until_idle();
}

TEST_F(RegistryFixture, OffStrandTouchesAreRefusedBeforeAnyRead)
{
    run([&] {
        auto id = drained_interest();
        ASSERT_EQ(reg.terminal_join(id).refusal, J::ok);
    });

    bool checked = false;
    asio::post(sched.io(), [&] {
        EXPECT_EQ(reg.begin_detach(1).refusal, R::off_strand);
        EXPECT_FALSE(reg.replay_revoke_command(1).has_value());
        EXPECT_FALSE(reg.replay_revoke_consumed(1).has_value());
        EXPECT_EQ(reg.add_delivery_obligation(1, 1), R::off_strand);
        EXPECT_EQ(reg.terminal_join(1).refusal, J::off_strand);
        EXPECT_EQ(
            reg.consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                                           kScope, kEpoch, 1, 1,
                                           kRevision))
                .refusal,
            R::off_strand)
            << "off_strand dominates even the scope gate";
        EXPECT_FALSE(reg.sponsors_of(1).has_value());
        EXPECT_FALSE(reg.interest_present(1).has_value());
        EXPECT_FALSE(
            InterestRegistryTestAccess::create(reg, 1, 2, 3, 4, kDest).ok)
            << "even the test seam refuses off-strand";
        checked = true;
    });
    sched.run_until_idle();
    EXPECT_TRUE(checked);
}
