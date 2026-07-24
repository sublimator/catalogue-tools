#include "c10-inbound-generation-fence.h"

#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

#include <array>
#include <functional>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

using namespace catl::provider_lab;
namespace asio = boost::asio;

namespace {

C09Destination const kDestination{
    .provider = 1,
    .provider_epoch = 2,
    .partition = 3,
    .peer_set_incarnation = 4,
    .receiver_generation = 5};

using Lifecycle = C10PeerClientGenerationLifecycle;
using Refusal = Lifecycle::Refusal;
using Mode = Lifecycle::Mode;
using C09Refusal = C09PeerClientTerminalOutbox::Refusal;

C09MemberIdentity
member(std::uint64_t n)
{
    return {
        kDestination,
        6,
        7,
        C09MemberLineage{10 + n, 20 + n, 30 + n, 40 + n, 50 + n}};
}

C10InboundFenceCandidate
fence(C10FenceId id, std::vector<C09MemberIdentity> expected = {})
{
    return {kDestination, 6, 7, id, std::move(expected)};
}

struct LifecycleFixture : ::testing::Test
{
    DeterministicScheduler sched;
    DeterministicScheduler::strand_type strand = sched.make_strand();
    Lifecycle lifecycle{strand, kDestination, 6, 7};

    template <typename Fn>
    void
    run(Fn fn)
    {
        asio::post(strand, std::move(fn));
        sched.run_until_idle();
    }

    void
    expect_invariant()
    {
        EXPECT_EQ(lifecycle.invariant_holds(), true);
    }
};

}  // namespace

TEST(C10LifecycleTypes, AggregateCannotBeCopiedOrMoved)
{
    static_assert(!std::is_copy_constructible_v<Lifecycle>);
    static_assert(!std::is_copy_assignable_v<Lifecycle>);
    static_assert(!std::is_move_constructible_v<Lifecycle>);
    static_assert(!std::is_move_assignable_v<Lifecycle>);
    SUCCEED();
}

TEST(C10LifecycleTypes, InvalidRawTestScopeCannotCreateAnOpenRoot)
{
    DeterministicScheduler sched;
    auto strand = sched.make_strand();
    EXPECT_THROW(
        Lifecycle(strand, C09Destination{}, 6, 7), std::invalid_argument);
    EXPECT_THROW(Lifecycle(strand, kDestination, 0, 7), std::invalid_argument);
    EXPECT_THROW(Lifecycle(strand, kDestination, 6, 0), std::invalid_argument);
}

TEST_F(LifecycleFixture, OffStrandOperationsRefuseWithoutReadingState)
{
    EXPECT_EQ(lifecycle.accept(member(1)).refusal, Refusal::off_strand);
    EXPECT_EQ(lifecycle.install_fence(fence(1)).refusal, Refusal::off_strand);
    EXPECT_FALSE(lifecycle.mode().has_value());
    EXPECT_FALSE(lifecycle.fence_id().has_value());
    EXPECT_FALSE(lifecycle.frozen_snapshot().has_value());
    EXPECT_FALSE(lifecycle.member_count().has_value());
    EXPECT_FALSE(lifecycle.invariant_holds().has_value());

    run([&] {
        EXPECT_EQ(lifecycle.mode(), Mode::open);
        EXPECT_EQ(lifecycle.member_count(), 0u);
        expect_invariant();
    });
}

TEST_F(LifecycleFixture, OpenAcceptReservesExactlyOneMemberCommandAndAttempt)
{
    run([&] {
        auto const identity = member(1);
        auto accepted = lifecycle.accept(identity);
        ASSERT_EQ(accepted.refusal, Refusal::ok);
        ASSERT_TRUE(accepted.member);
        EXPECT_EQ(*accepted.member, identity);
        EXPECT_EQ(lifecycle.member_count(), 1u);
        EXPECT_EQ(lifecycle.attempt_count(), 1u);
        EXPECT_EQ(lifecycle.live_attempt_count(), 1u);

        auto reservation = lifecycle.reservation(identity);
        ASSERT_EQ(reservation.refusal, Refusal::ok);
        ASSERT_TRUE(reservation.reservation);
        EXPECT_EQ(reservation.reservation->command, 1u);
        EXPECT_EQ(reservation.reservation->attempt, 1u);

        auto duplicate = lifecycle.accept(identity);
        EXPECT_EQ(duplicate.refusal, Refusal::already_open);
        EXPECT_EQ(lifecycle.member_count(), 1u);
        EXPECT_EQ(lifecycle.attempt_count(), 1u);
        EXPECT_EQ(
            lifecycle.reservation(identity).reservation,
            reservation.reservation);
        expect_invariant();
    });
}

TEST_F(LifecycleFixture, InvalidAndForeignIdentityChangeNothing)
{
    run([&] {
        auto invalid = member(1);
        invalid.lineage.operation = 0;
        EXPECT_EQ(
            lifecycle.accept(invalid).refusal,
            Refusal::invalid_member_identity);

        auto foreign = member(1);
        ++foreign.destination.receiver_generation;
        EXPECT_EQ(lifecycle.accept(foreign).refusal, Refusal::foreign_member);

        EXPECT_EQ(lifecycle.member_count(), 0u);
        EXPECT_EQ(lifecycle.attempt_count(), 0u);
        EXPECT_EQ(lifecycle.mode(), Mode::open);
        expect_invariant();
    });
}

TEST_F(LifecycleFixture, OnePipelineSlotCannotBeRebound)
{
    run([&] {
        auto const original = member(1);
        auto rebound = original;
        ++rebound.lineage.operation;

        ASSERT_EQ(lifecycle.accept(original).refusal, Refusal::ok);
        EXPECT_EQ(
            lifecycle.accept(rebound).refusal,
            Refusal::member_identity_conflict);
        EXPECT_EQ(lifecycle.member_count(), 1u);
        EXPECT_EQ(lifecycle.attempt_count(), 1u);

        DeterministicScheduler other_sched;
        auto other_strand = other_sched.make_strand();
        Lifecycle other{other_strand, kDestination, 6, 7};
        asio::post(other_strand, [&] {
            EXPECT_EQ(
                other.install_fence(fence(101, {original, rebound})).refusal,
                Refusal::snapshot_identity_conflict);
            EXPECT_EQ(other.mode(), Mode::open);
            EXPECT_EQ(other.member_count(), 0u);
            EXPECT_EQ(other.invariant_holds(), true);
        });
        other_sched.run_until_idle();
        expect_invariant();
    });
}

TEST_F(LifecycleFixture, FenceFreezesCompleteSnapshotAndControlsLaterAdmission)
{
    run([&] {
        auto const a = member(1);
        auto const b = member(2);
        auto const c = member(3);
        ASSERT_EQ(lifecycle.accept(a).refusal, Refusal::ok);

        auto installed = lifecycle.install_fence(fence(101, {b, a}));
        EXPECT_EQ(installed.refusal, Refusal::ok);
        EXPECT_EQ(installed.fence, 101u);
        EXPECT_EQ(lifecycle.mode(), Mode::fenced);
        EXPECT_EQ(lifecycle.fence_id(), 101u);

        ASSERT_EQ(lifecycle.accept(b).refusal, Refusal::already_open);
        EXPECT_EQ(lifecycle.accept(c).refusal, Refusal::member_not_expected);
        EXPECT_EQ(lifecycle.member_count(), 2u);
        EXPECT_EQ(lifecycle.attempt_count(), 2u);
        EXPECT_EQ(lifecycle.live_attempt_count(), 1u);
        EXPECT_EQ(lifecycle.outbox_count(), 1u);
        auto replayed = lifecycle.replay(b);
        ASSERT_EQ(replayed.refusal, C09Refusal::ok);
        ASSERT_TRUE(replayed.command);
        EXPECT_EQ(
            replayed.command->outcome(), C09TerminalOutcome::never_published);
        expect_invariant();
    });
}

TEST_F(LifecycleFixture, IncompleteFenceLeavesGenerationOpenAndRetryable)
{
    run([&] {
        auto const a = member(1);
        auto const b = member(2);
        ASSERT_EQ(lifecycle.accept(a).refusal, Refusal::ok);

        EXPECT_EQ(
            lifecycle.install_fence(fence(101, {b})).refusal,
            Refusal::incomplete_snapshot);
        EXPECT_EQ(lifecycle.mode(), Mode::open);
        EXPECT_FALSE(lifecycle.fence_id().has_value());
        EXPECT_EQ(lifecycle.frozen_snapshot()->size(), 0u);
        EXPECT_EQ(lifecycle.accept(b).refusal, Refusal::ok);

        EXPECT_EQ(
            lifecycle.install_fence(fence(101, {a, b})).refusal, Refusal::ok);
        EXPECT_EQ(lifecycle.mode(), Mode::fenced);
        expect_invariant();
    });
}

TEST_F(LifecycleFixture, FenceSnapshotValidationIsFailClosed)
{
    run([&] {
        auto const a = member(1);

        EXPECT_EQ(
            lifecycle.install_fence(fence(0, {a})).refusal,
            Refusal::invalid_fence);
        EXPECT_EQ(
            lifecycle.install_fence(fence(101, {a, a})).refusal,
            Refusal::duplicate_snapshot_member);

        auto foreign = a;
        ++foreign.peer_client_incarnation;
        EXPECT_EQ(
            lifecycle.install_fence(fence(101, {foreign})).refusal,
            Refusal::foreign_fence);

        auto wrong_scope = fence(101, {a});
        ++wrong_scope.sender_generation;
        EXPECT_EQ(
            lifecycle.install_fence(wrong_scope).refusal,
            Refusal::foreign_fence);

        EXPECT_EQ(lifecycle.mode(), Mode::open);
        EXPECT_EQ(lifecycle.member_count(), 0u);
        expect_invariant();
    });
}

TEST_F(LifecycleFixture, FenceReplayIsExactAndSnapshotNeverGrows)
{
    run([&] {
        auto const a = member(1);
        auto const b = member(2);
        ASSERT_EQ(
            lifecycle.install_fence(fence(101, {b, a})).refusal, Refusal::ok);
        auto const frozen = lifecycle.frozen_snapshot();

        EXPECT_EQ(
            lifecycle.install_fence(fence(101, {a, b})).refusal,
            Refusal::already_fenced);
        EXPECT_EQ(lifecycle.frozen_snapshot(), frozen);

        EXPECT_EQ(
            lifecycle.install_fence(fence(101, {a})).refusal,
            Refusal::fence_conflict);
        EXPECT_EQ(
            lifecycle.install_fence(fence(102, {a, b})).refusal,
            Refusal::generation_already_fenced);

        auto wrong_receiver = fence(101, {a, b});
        ++wrong_receiver.peer_client_incarnation;
        EXPECT_EQ(
            lifecycle.install_fence(wrong_receiver).refusal,
            Refusal::foreign_fence);
        EXPECT_EQ(lifecycle.frozen_snapshot(), frozen);
        expect_invariant();
    });
}

TEST(C10LifecycleInterleaving, FenceOvertakesQueuedAcceptOnlyWhenAuthorized)
{
    for (bool included : {false, true})
    {
        DeterministicScheduler sched;
        auto strand = sched.make_strand();
        Lifecycle lifecycle{strand, kDestination, 6, 7};
        auto const identity = member(included ? 10 : 20);
        std::optional<Lifecycle::AcceptResult> delayed_accept;
        auto expect_invariant = [&] {
            EXPECT_EQ(lifecycle.invariant_holds(), true);
        };

        // The accept intent exists first, but its owner-strand commit is
        // deliberately posted after the fence commit. This is the relevant
        // cross-strand overtake; both commits themselves remain serialized.
        std::function<void()> queued_accept = [&] {
            delayed_accept = lifecycle.accept(identity);
            expect_invariant();
        };
        asio::post(strand, [&] {
            auto candidate = fence(
                included ? 210 : 220,
                included ? std::vector<C09MemberIdentity>{identity}
                         : std::vector<C09MemberIdentity>{});
            EXPECT_EQ(lifecycle.install_fence(candidate).refusal, Refusal::ok);
            expect_invariant();
        });
        asio::post(strand, std::move(queued_accept));
        sched.run_until_idle();

        ASSERT_TRUE(delayed_accept);
        EXPECT_EQ(
            delayed_accept->refusal,
            included ? Refusal::already_open : Refusal::member_not_expected);
        asio::post(strand, [&] {
            EXPECT_EQ(lifecycle.member_count(), included ? 1u : 0u);
            expect_invariant();
        });
        sched.run_until_idle();
    }
}

TEST_F(LifecycleFixture, MissingAcceptanceStillGetsOwnedTerminalSettlementPath)
{
    run([&] {
        auto const identity = member(1);
        ASSERT_EQ(
            lifecycle.install_fence(fence(101, {identity})).refusal,
            Refusal::ok);
        EXPECT_EQ(lifecycle.member_count(), 1u);
        EXPECT_EQ(lifecycle.attempt_count(), 1u);
        EXPECT_EQ(lifecycle.live_attempt_count(), 0u);
        EXPECT_EQ(lifecycle.outbox_count(), 1u);

        auto replayed = lifecycle.replay(identity);
        ASSERT_EQ(replayed.refusal, C09Refusal::ok);
        ASSERT_TRUE(replayed.command);
        EXPECT_EQ(
            replayed.command->outcome(), C09TerminalOutcome::never_published);
        EXPECT_EQ(replayed.command->identity().command, 1u);
        EXPECT_EQ(replayed.command->identity().transport_attempt, 1u);

        auto receipt = C09TerminalReceiptTestMint::mint(
            replayed.command->identity(), replayed.command->outcome(), 1);
        EXPECT_EQ(lifecycle.consume_receipt(receipt), C09Refusal::ok);
        EXPECT_EQ(lifecycle.outbox_count(), 0u);
        EXPECT_EQ(lifecycle.attempt_count(), 0u);
        auto settled =
            lifecycle.settlement(identity, receipt.identity().command);
        ASSERT_EQ(settled.refusal, C09Refusal::ok);
        EXPECT_EQ(settled.receipt, receipt);
        expect_invariant();
    });
}

TEST_F(LifecycleFixture, FrozenSlotRejectsRebindingBeforeDelayedAccept)
{
    run([&] {
        auto const original = member(1);
        auto rebound = original;
        ++rebound.lineage.operation;
        ASSERT_EQ(
            lifecycle.install_fence(fence(101, {original})).refusal,
            Refusal::ok);

        EXPECT_EQ(
            lifecycle.accept(rebound).refusal,
            Refusal::member_identity_conflict);
        EXPECT_EQ(lifecycle.accept(original).refusal, Refusal::already_open);
        EXPECT_EQ(lifecycle.member_count(), 1u);
        EXPECT_EQ(lifecycle.attempt_count(), 1u);
        auto reservation = lifecycle.reservation(original);
        ASSERT_TRUE(reservation.reservation);
        EXPECT_EQ(reservation.reservation->command, 1u);
        EXPECT_FALSE(reservation.reservation->attempt);
        expect_invariant();
    });
}

TEST_F(LifecycleFixture, C09OpenFailureRollsBackIdsAndPreservesGate)
{
    run([&] {
        auto const identity = member(1);
        ASSERT_EQ(
            C10LifecycleTestSeed::fail_c09_at(
                lifecycle,
                C09PeerClientTerminalOutbox::TestFailpoint::
                    open_after_attempt_reserve),
            C09Refusal::ok);
        EXPECT_THROW(lifecycle.accept(identity), std::runtime_error);
        EXPECT_EQ(lifecycle.mode(), Mode::open);
        EXPECT_EQ(lifecycle.member_count(), 0u);
        EXPECT_EQ(lifecycle.attempt_count(), 0u);
        expect_invariant();

        ASSERT_EQ(
            C10LifecycleTestSeed::fail_c09_at(
                lifecycle, C09PeerClientTerminalOutbox::TestFailpoint::none),
            C09Refusal::ok);
        ASSERT_EQ(lifecycle.accept(identity).refusal, Refusal::ok);
        auto reservation = lifecycle.reservation(identity);
        ASSERT_TRUE(reservation.reservation);
        EXPECT_EQ(reservation.reservation->command, 1u);
        EXPECT_EQ(reservation.reservation->attempt, 1u);
        expect_invariant();
    });
}

TEST_F(LifecycleFixture, FenceFailureLeavesByteEquivalentOpenState)
{
    run([&] {
        auto const a = member(1);
        auto const b = member(2);
        ASSERT_EQ(lifecycle.accept(a).refusal, Refusal::ok);
        auto const before_snapshot = lifecycle.frozen_snapshot();

        ASSERT_EQ(
            C10LifecycleTestSeed::fail_at(
                lifecycle, Lifecycle::TestFailpoint::fence_pre_commit),
            Refusal::ok);
        EXPECT_THROW(
            lifecycle.install_fence(fence(101, {a, b})), std::runtime_error);
        EXPECT_EQ(lifecycle.mode(), Mode::open);
        EXPECT_FALSE(lifecycle.fence_id().has_value());
        EXPECT_EQ(lifecycle.frozen_snapshot(), before_snapshot);
        EXPECT_EQ(lifecycle.member_count(), 1u);
        EXPECT_EQ(lifecycle.attempt_count(), 1u);
        EXPECT_EQ(lifecycle.outbox_count(), 0u);
        EXPECT_EQ(lifecycle.accept(b).refusal, Refusal::ok);
        auto b_reservation = lifecycle.reservation(b);
        ASSERT_TRUE(b_reservation.reservation);
        EXPECT_EQ(b_reservation.reservation->command, 2u);
        EXPECT_EQ(b_reservation.reservation->attempt, 2u);
        expect_invariant();

        ASSERT_EQ(
            C10LifecycleTestSeed::fail_at(
                lifecycle, Lifecycle::TestFailpoint::none),
            Refusal::ok);
        EXPECT_EQ(
            lifecycle.install_fence(fence(101, {a, b})).refusal, Refusal::ok);
        expect_invariant();
    });
}

TEST_F(LifecycleFixture, C09FencePreparationFailureLeavesNoPartialAuthority)
{
    run([&] {
        auto const a = member(1);
        auto const b = member(2);
        ASSERT_EQ(
            C10LifecycleTestSeed::fail_c09_at(
                lifecycle,
                C09PeerClientTerminalOutbox::TestFailpoint::
                    fence_prepare_after_member),
            C09Refusal::ok);
        EXPECT_THROW(
            lifecycle.install_fence(fence(101, {a, b})), std::runtime_error);
        EXPECT_EQ(lifecycle.mode(), Mode::open);
        EXPECT_EQ(lifecycle.member_count(), 0u);
        EXPECT_EQ(lifecycle.attempt_count(), 0u);
        EXPECT_EQ(lifecycle.outbox_count(), 0u);
        expect_invariant();

        ASSERT_EQ(
            C10LifecycleTestSeed::fail_c09_at(
                lifecycle, C09PeerClientTerminalOutbox::TestFailpoint::none),
            C09Refusal::ok);
        ASSERT_EQ(
            lifecycle.install_fence(fence(101, {a, b})).refusal, Refusal::ok);
        auto a_reservation = lifecycle.reservation(a);
        auto b_reservation = lifecycle.reservation(b);
        ASSERT_TRUE(a_reservation.reservation);
        ASSERT_TRUE(b_reservation.reservation);
        EXPECT_EQ(a_reservation.reservation->command, 1u);
        EXPECT_EQ(b_reservation.reservation->command, 2u);
        expect_invariant();
    });
}

TEST_F(LifecycleFixture, C09FenceBatchCeilingRefusesWithoutPartialIds)
{
    run([&] {
        auto const max = std::numeric_limits<std::uint64_t>::max();
        ASSERT_EQ(
            C10LifecycleTestSeed::next_c09_ids(lifecycle, max - 1, max - 1),
            C09Refusal::ok);
        auto const a = member(1);
        auto const b = member(2);

        EXPECT_EQ(
            lifecycle.install_fence(fence(101, {a, b})).refusal,
            Refusal::id_exhausted);
        EXPECT_EQ(lifecycle.mode(), Mode::open);
        EXPECT_EQ(lifecycle.member_count(), 0u);
        EXPECT_EQ(lifecycle.attempt_count(), 0u);
        EXPECT_EQ(lifecycle.outbox_count(), 0u);
        expect_invariant();

        ASSERT_EQ(
            lifecycle.install_fence(fence(101, {a})).refusal, Refusal::ok);
        auto reservation = lifecycle.reservation(a);
        ASSERT_TRUE(reservation.reservation);
        EXPECT_EQ(reservation.reservation->command, max);
        EXPECT_FALSE(reservation.reservation->attempt);
        auto replayed = lifecycle.replay(a);
        ASSERT_TRUE(replayed.command);
        EXPECT_EQ(replayed.command->identity().transport_attempt, max);
        expect_invariant();
    });
}

TEST_F(LifecycleFixture, FenceDoesNotStopAcceptedTransportLifecycle)
{
    run([&] {
        auto const identity = member(1);
        ASSERT_EQ(lifecycle.accept(identity).refusal, Refusal::ok);
        ASSERT_EQ(
            lifecycle.install_fence(fence(101, {identity})).refusal,
            Refusal::ok);

        auto registered = lifecycle.register_attempt(identity, 0xCA7);
        ASSERT_EQ(registered.refusal, C09Refusal::ok);
        ASSERT_TRUE(registered.lease.valid());
        EXPECT_EQ(lifecycle.publish_attempt(registered.lease), C09Refusal::ok);
        auto completed = lifecycle.complete(registered.lease);
        EXPECT_EQ(completed.refusal, C09Refusal::ok);
        ASSERT_TRUE(completed.command);
        EXPECT_EQ(
            completed.command->outcome(),
            C09TerminalOutcome::transport_complete);
        expect_invariant();
    });
}

TEST_F(LifecycleFixture, FenceAllowsRetryAndExactReceiptSettlement)
{
    run([&] {
        auto const identity = member(1);
        ASSERT_EQ(lifecycle.accept(identity).refusal, Refusal::ok);
        ASSERT_EQ(
            lifecycle.install_fence(fence(101, {identity})).refusal,
            Refusal::ok);

        auto first = lifecycle.register_attempt(identity, 0xCA7);
        ASSERT_EQ(first.refusal, C09Refusal::ok);
        ASSERT_EQ(lifecycle.publish_attempt(first.lease), C09Refusal::ok);
        ASSERT_EQ(lifecycle.commit_retry_terminal(first.lease), C09Refusal::ok);

        auto final = lifecycle.register_attempt(identity, 0xCA8);
        ASSERT_EQ(final.refusal, C09Refusal::ok);
        ASSERT_EQ(lifecycle.publish_attempt(final.lease), C09Refusal::ok);
        auto completed = lifecycle.complete(final.lease);
        ASSERT_EQ(completed.refusal, C09Refusal::ok);
        ASSERT_TRUE(completed.command);
        EXPECT_EQ(lifecycle.retry_history_count(), 1u);
        EXPECT_EQ(lifecycle.outbox_count(), 1u);

        auto receipt = C09TerminalReceiptTestMint::mint(
            completed.command->identity(), completed.command->outcome(), 1);
        EXPECT_EQ(lifecycle.consume_receipt(receipt), C09Refusal::ok);
        EXPECT_EQ(lifecycle.outbox_count(), 0u);
        auto settled =
            lifecycle.settlement(identity, receipt.identity().command);
        ASSERT_EQ(settled.refusal, C09Refusal::ok);
        EXPECT_EQ(settled.receipt, receipt);
        expect_invariant();
    });
}

TEST_F(LifecycleFixture, FencePreservesLiveLeaseAndCommittedTerminalAuthority)
{
    run([&] {
        auto const live_member = member(1);
        auto const missing_member = member(2);
        auto const terminal_member = member(3);

        ASSERT_EQ(lifecycle.accept(live_member).refusal, Refusal::ok);
        auto live = lifecycle.register_attempt(live_member, 0xCA7);
        ASSERT_EQ(live.refusal, C09Refusal::ok);
        ASSERT_EQ(lifecycle.publish_attempt(live.lease), C09Refusal::ok);

        ASSERT_EQ(lifecycle.accept(terminal_member).refusal, Refusal::ok);
        auto terminal = lifecycle.register_attempt(terminal_member, 0xCA8);
        ASSERT_EQ(terminal.refusal, C09Refusal::ok);
        ASSERT_EQ(lifecycle.publish_attempt(terminal.lease), C09Refusal::ok);
        auto committed = lifecycle.complete(terminal.lease);
        ASSERT_EQ(committed.refusal, C09Refusal::ok);
        ASSERT_TRUE(committed.command);

        ASSERT_EQ(
            lifecycle
                .install_fence(
                    fence(101, {live_member, missing_member, terminal_member}))
                .refusal,
            Refusal::ok);
        EXPECT_EQ(
            lifecycle.attempt_state(live.lease).state,
            C09PeerClientTerminalOutbox::AttemptState::attempt_committed);
        auto replayed_terminal = lifecycle.replay(terminal_member);
        ASSERT_EQ(replayed_terminal.refusal, C09Refusal::ok);
        EXPECT_EQ(replayed_terminal.command, committed.command);

        auto completed_live = lifecycle.complete(live.lease);
        ASSERT_EQ(completed_live.refusal, C09Refusal::ok);
        ASSERT_TRUE(completed_live.command);

        auto terminal_receipt = C09TerminalReceiptTestMint::mint(
            committed.command->identity(), committed.command->outcome(), 1);
        auto live_receipt = C09TerminalReceiptTestMint::mint(
            completed_live.command->identity(),
            completed_live.command->outcome(),
            2);
        EXPECT_EQ(lifecycle.consume_receipt(terminal_receipt), C09Refusal::ok);
        EXPECT_EQ(lifecycle.consume_receipt(live_receipt), C09Refusal::ok);
        EXPECT_EQ(lifecycle.outbox_count(), 1u)
            << "only the fence-synthesized missing member remains";
        EXPECT_EQ(
            lifecycle
                .settlement(
                    terminal_member, terminal_receipt.identity().command)
                .receipt,
            terminal_receipt);
        EXPECT_EQ(
            lifecycle.settlement(live_member, live_receipt.identity().command)
                .receipt,
            live_receipt);
        expect_invariant();
    });
}
