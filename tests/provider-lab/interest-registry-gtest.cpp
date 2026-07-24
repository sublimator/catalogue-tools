#include "interest-registry.h"

#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

#include <limits>
#include <utility>

using namespace catl::provider_lab;
namespace asio = boost::asio;

namespace {

PartitionScope const kScope{1, 1};
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

    /// Drive one interest to detach_pending with revoke and
    /// unsubscribe consumed through the owned seams; returns its id.
    InterestId
    drained_interest()
    {
        auto r = reg.register_interest(100, 7, 1, /*subscription*/ 900,
                                       kDest);
        EXPECT_EQ(r.refusal, InterestRegistry::Refusal::ok);
        auto d = reg.begin_detach(r.interest);
        EXPECT_EQ(d.refusal, InterestRegistry::Refusal::ok);
        EXPECT_EQ(
            reg.consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                                           kScope, kEpoch, r.interest,
                                           d.command->command(),
                                           kRevision))
                .refusal,
            InterestRegistry::Refusal::ok);
        EXPECT_EQ(
            reg.consume_unsubscribe_receipt(
                UnsubscribeReceiptTestMint::mint(r.interest, 900)),
            InterestRegistry::Refusal::ok);
        return r.interest;
    }
};

using R = InterestRegistry::Refusal;
using J = InterestRegistry::JoinRefusal;

}  // namespace

TEST_F(RegistryFixture, DetachCommitsAFullyBoundReplayableRevokeCommand)
{
    run([&] {
        auto r = reg.register_interest(100, 7, 1, 900, kDest);
        ASSERT_EQ(r.refusal, R::ok);

        auto d = reg.begin_detach(r.interest);
        ASSERT_EQ(d.refusal, R::ok)
            << "initiation is a fact with a home, not an arrow";
        auto const& cmd = *d.command;
        EXPECT_EQ(cmd.interest(), r.interest);
        EXPECT_EQ(cmd.fetch(), 1u);
        EXPECT_EQ(cmd.operation(), 100u);
        EXPECT_GT(cmd.command(), 0u);
        EXPECT_EQ(cmd.destination(), kDest)
            << "the command carries the COMPLETE destination binding";

        // The projection is replayable, identically, until receipted.
        auto replay = reg.replay_revoke_command(r.interest);
        ASSERT_TRUE(replay.has_value());
        EXPECT_TRUE(*replay == cmd);

        // Receipt consumption retires the command replay home.
        ASSERT_EQ(
            reg.consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                                           kScope, kEpoch, r.interest,
                                           cmd.command(), kRevision))
                .refusal,
            R::ok);
        EXPECT_FALSE(reg.replay_revoke_command(r.interest).has_value())
            << "a consumed receipt retires the replay home";
    });
}

TEST_F(RegistryFixture, RevocationValidatesTheCompleteBinding)
{
    run([&] {
        auto r = reg.register_interest(100, 7, 1, 900, kDest);
        auto d = reg.begin_detach(r.interest);
        ASSERT_EQ(d.refusal, R::ok);
        auto const id = d.command->command();

        // Wrong command, wrong revision, wrong epoch, wrong scope:
        // each refuses BEFORE consumption. The wrong-revision case is
        // the review's named control — consume_revoke_receipt must
        // actually read receipt.revision().
        EXPECT_EQ(
            reg.consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                                           kScope, kEpoch, r.interest,
                                           id + 1, kRevision))
                .refusal,
            R::wrong_binding);
        EXPECT_EQ(
            reg.consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                                           kScope, kEpoch, r.interest, id,
                                           kRevision + 1))
                .refusal,
            R::wrong_binding)
            << "right command, wrong revision: refused pre-consumption";
        EXPECT_EQ(
            reg.consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                                           kScope, kEpoch + 1, r.interest,
                                           id, kRevision))
                .refusal,
            R::wrong_binding);
        EXPECT_EQ(
            reg.consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                                           PartitionScope{1, 2}, kEpoch,
                                           r.interest, id, kRevision))
                .refusal,
            R::wrong_binding)
            << "an artifact from another partition instance refuses";

        auto first = reg.consume_revoke_receipt(
            ParticipantReceiptTestMint::mint(
                kScope, kEpoch, r.interest, id, kRevision));
        ASSERT_EQ(first.refusal, R::ok);
        ASSERT_TRUE(first.consumed.has_value());

        // Exact replay: already_acked with the IDENTICAL witness.
        auto again = reg.consume_revoke_receipt(
            ParticipantReceiptTestMint::mint(
                kScope, kEpoch, r.interest, id, kRevision));
        EXPECT_EQ(again.refusal, R::already_acked);
        EXPECT_TRUE(*again.consumed == *first.consumed);

        // Wrong revision AFTER consumption: still wrong_binding,
        // never laundered as already_acked.
        EXPECT_EQ(
            reg.consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                                           kScope, kEpoch, r.interest, id,
                                           kRevision + 1))
                .refusal,
            R::wrong_binding);

        // The witness home survives loss of every returned copy.
        auto replay = reg.replay_revoke_consumed(r.interest);
        ASSERT_TRUE(replay.has_value());
        EXPECT_TRUE(*replay == *first.consumed);

        // Unsubscribe: binding to the RETAINED subscription identity.
        EXPECT_EQ(
            reg.consume_unsubscribe_receipt(
                UnsubscribeReceiptTestMint::mint(r.interest, 901)),
            R::wrong_binding);
        EXPECT_EQ(
            reg.consume_unsubscribe_receipt(
                UnsubscribeReceiptTestMint::mint(r.interest, 900)),
            R::ok);
    });
}

TEST_F(RegistryFixture, TerminalAckCannotRetireOnlyTheConsumedWitnessCan)
{
    run([&] {
        auto id = drained_interest();

        auto first = reg.terminal_join(id);
        ASSERT_EQ(first.refusal, J::ok);
        ASSERT_TRUE(first.ack.has_value());
        EXPECT_EQ(first.ack->child(), 7u);
        EXPECT_EQ(first.ack->scope(), kScope);
        EXPECT_TRUE(reg.interest_present(id).value_or(false))
            << "the join no longer erases: the witness has a home";

        // Losing the ack is safe: the identical binding replays.
        auto replay = reg.terminal_join(id);
        ASSERT_EQ(replay.refusal, J::ok);
        EXPECT_TRUE(*replay.ack == *first.ack);

        // The interest's own outgoing ack is NOT retirement evidence —
        // retire_terminal does not even accept its type. A consumption
        // witness with a mismatched binding refuses.
        EXPECT_EQ(
            reg.retire_terminal(TerminalConsumedTestMint::mint(
                kScope, id, first.ack->operation(), 8)),
            R::wrong_binding);
        EXPECT_EQ(
            reg.retire_terminal(TerminalConsumedTestMint::mint(
                PartitionScope{1, 2}, id, first.ack->operation(), 7)),
            R::wrong_binding);

        // Only the matching downstream witness removes, exactly once.
        auto witness = TerminalConsumedTestMint::mint(
            kScope, id, first.ack->operation(), first.ack->child());
        ASSERT_EQ(reg.retire_terminal(witness), R::ok);
        EXPECT_FALSE(reg.interest_present(id).value_or(true));
        EXPECT_EQ(reg.retire_terminal(witness), R::unknown_interest)
            << "removed exactly once, typed on the second attempt";
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
            auto r = r2.register_interest(100, 7, 1, 900, kDest);
            ASSERT_EQ(r.refusal, R::ok);
            auto const id = r.interest;
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
                    UnsubscribeReceiptTestMint::mint(id, 900)),
                R::ok);

            auto join = r2.terminal_join(id);
            ASSERT_EQ(join.refusal, J::ok);
            EXPECT_EQ(join.ack->operation(), 100u);
            EXPECT_EQ(join.ack->child(), 7u);
            ASSERT_EQ(
                r2.retire_terminal(TerminalConsumedTestMint::mint(
                    kScope, id, 100, 7)),
                R::ok);
            EXPECT_FALSE(r2.interest_present(id).value_or(true));
        });
        s2.run_until_idle();
    }
}

TEST_F(RegistryFixture, DuplicateDeliveryObligationsAreTypedNoNewDebt)
{
    run([&] {
        auto r = reg.register_interest(100, 7, 1, 900, kDest);
        ASSERT_EQ(reg.add_delivery_obligation(r.interest, 600), R::ok);
        EXPECT_EQ(reg.add_delivery_obligation(r.interest, 600),
                  R::already_registered)
            << "a duplicate must never report a fresh commit";

        auto d = reg.begin_detach(r.interest);
        ASSERT_EQ(d.refusal, R::ok);
        ASSERT_EQ(
            reg.consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                                           kScope, kEpoch, r.interest,
                                           d.command->command(),
                                           kRevision))
                .refusal,
            R::ok);
        ASSERT_EQ(
            reg.consume_unsubscribe_receipt(
                UnsubscribeReceiptTestMint::mint(r.interest, 900)),
            R::ok);

        // The duplicate created no second debt: one closure clears
        // the ONE obligation and the join opens.
        EXPECT_EQ(reg.terminal_join(r.interest).refusal,
                  J::obligations_open);
        ASSERT_EQ(DeliveryAckTestSeam::close(reg, r.interest, 600), R::ok);
        EXPECT_EQ(DeliveryAckTestSeam::close(reg, r.interest, 600),
                  R::unknown_delivery)
            << "the single debt closed once";
        EXPECT_EQ(reg.terminal_join(r.interest).refusal, J::ok);
    });
}

TEST_F(RegistryFixture, DetachSealsAndJoinGatesEveryObligationClass)
{
    run([&] {
        auto r = reg.register_interest(100, 7, 1, 900, kDest);
        ASSERT_EQ(reg.add_delivery_obligation(r.interest, 600), R::ok);
        EXPECT_EQ(reg.terminal_join(r.interest).refusal,
                  J::not_detach_pending);

        auto d = reg.begin_detach(r.interest);
        ASSERT_EQ(d.refusal, R::ok);
        EXPECT_EQ(reg.begin_detach(r.interest).refusal, R::not_active)
            << "detach is not repeatable, and the refusal is typed";
        EXPECT_EQ(reg.add_delivery_obligation(r.interest, 601),
                  R::not_active)
            << "detach seals new sponsorship";

        // Each obligation class left open alone blocks the join.
        EXPECT_EQ(reg.terminal_join(r.interest).refusal,
                  J::obligations_open);
        ASSERT_EQ(DeliveryAckTestSeam::close(reg, r.interest, 600), R::ok);
        EXPECT_EQ(reg.terminal_join(r.interest).refusal,
                  J::obligations_open)
            << "revoke consumption still outstanding";
        ASSERT_EQ(
            reg.consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                                           kScope, kEpoch, r.interest,
                                           d.command->command(),
                                           kRevision))
                .refusal,
            R::ok);
        EXPECT_EQ(reg.terminal_join(r.interest).refusal,
                  J::obligations_open)
            << "unsubscribe consumption still outstanding";
        ASSERT_EQ(
            reg.consume_unsubscribe_receipt(
                UnsubscribeReceiptTestMint::mint(r.interest, 900)),
            R::ok);
        EXPECT_EQ(reg.terminal_join(r.interest).refusal, J::ok);
    });
}

TEST_F(RegistryFixture, NoRootAdoptionIsStructural)
{
    run([&] {
        auto first = reg.register_interest(100, 7, 1, 900, kDest);
        auto second = reg.register_interest(101, 8, 1, 901, kDest);
        ASSERT_EQ(first.refusal, R::ok);
        ASSERT_EQ(second.refusal, R::ok);
        EXPECT_GT(second.interest, first.interest);

        ASSERT_EQ(reg.begin_detach(first.interest).refusal, R::ok);
        auto sponsors = reg.sponsors_of(1);
        ASSERT_EQ(sponsors->size(), 1u);
        EXPECT_EQ(sponsors->front(), second.interest)
            << "shared work is owned by remaining actives; no adopted "
               "root exists";
        EXPECT_EQ(reg.draining_of(1)->size(), 1u);
    });
}

TEST(RegistryEdge, InterestIdCeilingFailsClosedWithNothingMutated)
{
    DeterministicScheduler sched;
    auto strand = sched.make_strand();
    InterestRegistry reg{
        strand, kScope, std::numeric_limits<InterestId>::max()};
    asio::post(strand, [&] {
        auto r = reg.register_interest(1, 2, 3, 4, kDest);
        EXPECT_EQ(r.refusal, InterestRegistry::Refusal::interest_id_exhausted)
            << "the wrap class, refused at this habitat too";
        EXPECT_EQ(r.interest, 0u);
        EXPECT_FALSE(reg.interest_present(0).value_or(true))
            << "zero stays reserved; nothing minted, nothing mutated";
        EXPECT_EQ(
            reg.register_interest(1, 2, 3, 4, kDest).refusal,
            InterestRegistry::Refusal::interest_id_exhausted);
    });
    sched.run_until_idle();
}

TEST(RegistryEdge, RevokeCommandCeilingRefusesWithoutSealingTheInterest)
{
    DeterministicScheduler sched;
    auto strand = sched.make_strand();
    InterestRegistry reg{
        strand, kScope, 1, std::numeric_limits<RevokeCommandId>::max()};
    asio::post(strand, [&] {
        using R = InterestRegistry::Refusal;
        auto r = reg.register_interest(100, 7, 1, 900, kDest);
        ASSERT_EQ(r.refusal, R::ok);
        auto d = reg.begin_detach(r.interest);
        EXPECT_EQ(d.refusal, R::revoke_command_id_exhausted)
            << "the wrap class, refused at the command minter too";
        EXPECT_FALSE(d.command.has_value());
        EXPECT_FALSE(reg.replay_revoke_command(r.interest).has_value())
            << "no command committed";
        EXPECT_EQ(reg.add_delivery_obligation(r.interest, 600), R::ok)
            << "seal-plus-commit is one atom: the refused mint left "
               "the interest fully ACTIVE, not sealed commandless";
        EXPECT_EQ(reg.begin_detach(r.interest).refusal,
                  R::revoke_command_id_exhausted);
    });
    sched.run_until_idle();
}

TEST_F(RegistryFixture, OffStrandTouchesAreRefusedBeforeAnyRead)
{
    // Prepare a terminal-ready interest and a matching consumption
    // witness on the owner strand first.
    std::optional<InterestTerminalConsumed> witness;
    run([&] {
        auto id = drained_interest();
        auto join = reg.terminal_join(id);
        ASSERT_EQ(join.refusal, J::ok);
        witness = TerminalConsumedTestMint::mint(
            kScope, id, join.ack->operation(), join.ack->child());
    });

    bool checked = false;
    asio::post(sched.io(), [&] {
        EXPECT_EQ(
            reg.register_interest(1, 2, 3, 4, kDest).refusal,
            R::off_strand);
        EXPECT_EQ(reg.begin_detach(1).refusal, R::off_strand);
        EXPECT_FALSE(reg.replay_revoke_command(1).has_value());
        EXPECT_FALSE(reg.replay_revoke_consumed(1).has_value());
        EXPECT_EQ(reg.add_delivery_obligation(1, 1), R::off_strand);
        EXPECT_EQ(reg.terminal_join(1).refusal, J::off_strand);
        EXPECT_EQ(reg.retire_terminal(*witness), R::off_strand)
            << "even a matching witness cannot retire off-strand";
        EXPECT_FALSE(reg.sponsors_of(1).has_value());
        EXPECT_FALSE(reg.interest_present(1).has_value());
        checked = true;
    });
    sched.run_until_idle();
    EXPECT_TRUE(checked);

    // Back on the owner strand the SAME witness still retires: the
    // refusal above read nothing and consumed nothing.
    run([&] { EXPECT_EQ(reg.retire_terminal(*witness), R::ok); });
}
