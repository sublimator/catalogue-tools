#include "interest-registry.h"

#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

#include <limits>
#include <utility>

using namespace catl::provider_lab;
namespace asio = boost::asio;

namespace {

struct RegistryFixture : ::testing::Test
{
    DeterministicScheduler sched;
    DeterministicScheduler::strand_type strand = sched.make_strand();
    InterestRegistry reg{strand};

    template <typename Fn>
    void
    run(Fn fn)
    {
        asio::post(strand, std::move(fn));
        sched.run_until_idle();
    }

    /// Drive one interest to detach_pending with obligations closed
    /// through the OWNED seams, returning its id.
    InterestId
    drained_interest()
    {
        auto r = reg.register_interest(100, 7, 1, /*subscription*/ 900);
        EXPECT_EQ(r.refusal, InterestRegistry::Refusal::ok);
        auto d = reg.begin_detach(r.interest);
        EXPECT_EQ(d.refusal, InterestRegistry::Refusal::ok);
        EXPECT_EQ(
            reg.consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                r.interest, d.command->command(), /*revision*/ 3)),
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

TEST_F(RegistryFixture, DetachCommitsAnOwnedReplayableRevokeCommand)
{
    run([&] {
        auto r = reg.register_interest(100, 7, 1, 900);
        ASSERT_EQ(r.refusal, R::ok);

        auto d = reg.begin_detach(r.interest);
        ASSERT_EQ(d.refusal, R::ok)
            << "initiation is a fact with a home, not an arrow";
        auto const& cmd = *d.command;
        EXPECT_EQ(cmd.interest(), r.interest);
        EXPECT_EQ(cmd.fetch(), 1u);
        EXPECT_EQ(cmd.operation(), 100u);
        EXPECT_GT(cmd.command(), 0u);

        // The projection is replayable, identically, until receipted.
        auto replay = reg.replay_revoke_command(r.interest);
        ASSERT_TRUE(replay.has_value());
        EXPECT_TRUE(*replay == cmd);

        // Receipt consumption retires the outbox.
        ASSERT_EQ(
            reg.consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                r.interest, cmd.command(), 3)),
            R::ok);
        EXPECT_FALSE(reg.replay_revoke_command(r.interest).has_value())
            << "a consumed receipt retires the replay home";
    });
}

TEST_F(RegistryFixture, RevocationCompletesOnlyByBoundReceipt)
{
    run([&] {
        auto r = reg.register_interest(100, 7, 1, 900);
        auto d = reg.begin_detach(r.interest);
        ASSERT_EQ(d.refusal, R::ok);
        auto const id = d.command->command();

        // A receipt bound to a DIFFERENT command cannot complete it.
        EXPECT_EQ(
            reg.consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                r.interest, id + 1, 3)),
            R::wrong_binding)
            << "no raw assertion path exists; the binding is checked";

        ASSERT_EQ(
            reg.consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                r.interest, id, 3)),
            R::ok);
        EXPECT_EQ(
            reg.consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                r.interest, id, 3)),
            R::already_acked)
            << "exact replay after consumption is a typed no-op";
        EXPECT_EQ(
            reg.consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                r.interest, id + 1, 3)),
            R::wrong_binding)
            << "a mismatched receipt post-consumption must not launder "
               "as already_acked: binding dominates the done state";

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

TEST_F(RegistryFixture, TerminalWitnessSurvivesLossAndRetiresOnce)
{
    run([&] {
        auto id = drained_interest();

        // First join transitions to terminal_ready and mints the ack.
        auto first = reg.terminal_join(id);
        ASSERT_EQ(first.refusal, J::ok);
        ASSERT_TRUE(first.ack.has_value());
        EXPECT_EQ(first.ack->child(), 7u);
        EXPECT_TRUE(reg.interest_present(id).value_or(false))
            << "the join no longer erases: the witness has a home";

        // The caller LOSES the ack; asking again replays the
        // IDENTICAL binding — the witness survives the fact.
        auto replay = reg.terminal_join(id);
        ASSERT_EQ(replay.refusal, J::ok);
        EXPECT_TRUE(*replay.ack == *first.ack);

        // Retirement consumes a genuinely minted matching ack, once.
        ASSERT_EQ(reg.retire_terminal(*replay.ack), R::ok);
        EXPECT_FALSE(reg.interest_present(id).value_or(true));
        EXPECT_EQ(reg.retire_terminal(*first.ack), R::unknown_interest)
            << "removed exactly once, typed on the second attempt";
    });
}

TEST_F(RegistryFixture, SoleInterestDetachBothOrdersThroughOwnedSeams)
{
    for (bool detach_first : {true, false})
    {
        DeterministicScheduler s2;
        auto strand2 = s2.make_strand();
        InterestRegistry r2{strand2};
        asio::post(strand2, [&] {
            auto r = r2.register_interest(100, 7, 1, 900);
            ASSERT_EQ(r.refusal, R::ok);
            auto const id = r.interest;
            ASSERT_EQ(r2.add_submitted_use(id, 500), R::ok);

            std::optional<RevokeCommand> cmd;
            if (detach_first)
            {
                auto d = r2.begin_detach(id);
                ASSERT_EQ(d.refusal, R::ok);
                cmd = d.command;
                ASSERT_EQ(r2.use_terminal(id, 500), R::ok);
            }
            else
            {
                ASSERT_EQ(r2.use_terminal(id, 500), R::ok);
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
                    id, cmd->command(), 3)),
                R::ok);
            ASSERT_EQ(
                r2.consume_unsubscribe_receipt(
                    UnsubscribeReceiptTestMint::mint(id, 900)),
                R::ok);

            auto join = r2.terminal_join(id);
            ASSERT_EQ(join.refusal, J::ok);
            EXPECT_EQ(join.ack->operation(), 100u);
            EXPECT_EQ(join.ack->child(), 7u);
            ASSERT_EQ(r2.retire_terminal(*join.ack), R::ok);
            EXPECT_FALSE(r2.interest_present(id).value_or(true));
        });
        s2.run_until_idle();
    }
}

TEST_F(RegistryFixture, DuplicateObligationsAreTypedAndCreateNoDebt)
{
    run([&] {
        auto r = reg.register_interest(100, 7, 1, 900);
        ASSERT_EQ(reg.add_submitted_use(r.interest, 500), R::ok);
        EXPECT_EQ(reg.add_submitted_use(r.interest, 500),
                  R::already_registered)
            << "a duplicate must never report a fresh commit";
        ASSERT_EQ(reg.add_delivery_obligation(r.interest, 600), R::ok);
        EXPECT_EQ(reg.add_delivery_obligation(r.interest, 600),
                  R::already_registered);

        // One terminal event closes exactly one debt: the duplicate
        // did not create a second, and the join is gated on the ONE.
        auto d = reg.begin_detach(r.interest);
        ASSERT_EQ(d.refusal, R::ok);
        ASSERT_EQ(
            reg.consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                r.interest, d.command->command(), 3)),
            R::ok);
        ASSERT_EQ(
            reg.consume_unsubscribe_receipt(
                UnsubscribeReceiptTestMint::mint(r.interest, 900)),
            R::ok);
        EXPECT_EQ(reg.terminal_join(r.interest).refusal,
                  J::obligations_open);
        ASSERT_EQ(reg.use_terminal(r.interest, 500), R::ok);
        EXPECT_EQ(reg.use_terminal(r.interest, 500), R::unknown_use)
            << "the single debt closed once";
        EXPECT_EQ(reg.terminal_join(r.interest).refusal,
                  J::obligations_open);
        ASSERT_EQ(reg.ack_delivery(r.interest, 600), R::ok);
        EXPECT_EQ(reg.terminal_join(r.interest).refusal, J::ok);
    });
}

TEST_F(RegistryFixture, DetachSealsAndJoinGatesEveryObligation)
{
    run([&] {
        auto r = reg.register_interest(100, 7, 1, 900);
        ASSERT_EQ(reg.add_submitted_use(r.interest, 500), R::ok);
        EXPECT_EQ(reg.terminal_join(r.interest).refusal,
                  J::not_detach_pending);

        auto d = reg.begin_detach(r.interest);
        ASSERT_EQ(d.refusal, R::ok);
        EXPECT_EQ(reg.begin_detach(r.interest).refusal, R::not_active)
            << "detach is not repeatable, and the refusal is typed";
        EXPECT_EQ(reg.add_submitted_use(r.interest, 501), R::not_active)
            << "detach seals new sponsorship";

        // Each obligation left open alone blocks the join.
        EXPECT_EQ(reg.terminal_join(r.interest).refusal,
                  J::obligations_open);
        ASSERT_EQ(reg.use_terminal(r.interest, 500), R::ok);
        EXPECT_EQ(reg.terminal_join(r.interest).refusal,
                  J::obligations_open);
        ASSERT_EQ(
            reg.consume_revoke_receipt(ParticipantReceiptTestMint::mint(
                r.interest, d.command->command(), 3)),
            R::ok);
        EXPECT_EQ(reg.terminal_join(r.interest).refusal,
                  J::obligations_open);
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
        auto first = reg.register_interest(100, 7, 1, 900);
        auto second = reg.register_interest(101, 8, 1, 901);
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
        strand, std::numeric_limits<InterestId>::max()};
    asio::post(strand, [&] {
        auto r = reg.register_interest(1, 2, 3, 4);
        EXPECT_EQ(r.refusal, InterestRegistry::Refusal::interest_id_exhausted)
            << "the wrap class, refused at its fourth known habitat";
        EXPECT_EQ(r.interest, 0u);
        EXPECT_FALSE(reg.interest_present(0).value_or(true))
            << "zero stays reserved; nothing minted, nothing mutated";
        // A later attempt refuses identically: the counter did not move.
        EXPECT_EQ(
            reg.register_interest(1, 2, 3, 4).refusal,
            InterestRegistry::Refusal::interest_id_exhausted);
    });
    sched.run_until_idle();
}

TEST(RegistryEdge, RevokeCommandCeilingRefusesWithoutSealingTheInterest)
{
    DeterministicScheduler sched;
    auto strand = sched.make_strand();
    InterestRegistry reg{
        strand, 1, std::numeric_limits<RevokeCommandId>::max()};
    asio::post(strand, [&] {
        using R = InterestRegistry::Refusal;
        auto r = reg.register_interest(100, 7, 1, 900);
        ASSERT_EQ(r.refusal, R::ok);
        auto d = reg.begin_detach(r.interest);
        EXPECT_EQ(d.refusal, R::revoke_command_id_exhausted)
            << "the wrap class, refused at the command minter too";
        EXPECT_FALSE(d.command.has_value());
        EXPECT_FALSE(reg.replay_revoke_command(r.interest).has_value())
            << "no command committed";
        EXPECT_EQ(reg.add_submitted_use(r.interest, 500), R::ok)
            << "seal-plus-commit is one atom: the refused mint left "
               "the interest fully ACTIVE, not sealed commandless";
        EXPECT_EQ(reg.begin_detach(r.interest).refusal,
                  R::revoke_command_id_exhausted);
    });
    sched.run_until_idle();
}

TEST_F(RegistryFixture, OffStrandTouchesAreRefusedBeforeAnyRead)
{
    // Mint a genuine terminal ack on the owner strand first: retire is
    // only callable with a minted ack (construction is compile-closed).
    std::optional<InterestTerminalAck> ack;
    run([&] {
        auto id = drained_interest();
        auto join = reg.terminal_join(id);
        ASSERT_EQ(join.refusal, J::ok);
        ack = join.ack;
    });

    bool checked = false;
    asio::post(sched.io(), [&] {
        EXPECT_EQ(reg.register_interest(1, 2, 3, 4).refusal, R::off_strand);
        EXPECT_EQ(reg.begin_detach(1).refusal, R::off_strand);
        EXPECT_FALSE(reg.replay_revoke_command(1).has_value());
        EXPECT_EQ(reg.use_terminal(1, 1), R::off_strand);
        EXPECT_EQ(reg.ack_delivery(1, 1), R::off_strand);
        EXPECT_EQ(reg.terminal_join(1).refusal, J::off_strand);
        EXPECT_EQ(reg.retire_terminal(*ack), R::off_strand)
            << "even a genuinely minted ack cannot retire off-strand";
        EXPECT_FALSE(reg.sponsors_of(1).has_value());
        EXPECT_FALSE(reg.interest_present(1).has_value());
        checked = true;
    });
    sched.run_until_idle();
    EXPECT_TRUE(checked);

    // Back on the owner strand the SAME ack still retires: the refusal
    // above read nothing and consumed nothing.
    run([&] { EXPECT_EQ(reg.retire_terminal(*ack), R::ok); });
}
