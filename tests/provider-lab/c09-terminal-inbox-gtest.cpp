#include "c09-terminal-inbox.h"

#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

#include <limits>
#include <optional>
#include <stdexcept>
#include <utility>

using namespace catl::provider_lab;
namespace asio = boost::asio;

namespace catl::provider_lab {

/// Explicit test-only malformed/conflicting command seam. Production command
/// construction remains private to the PeerClient outbox.
class C09TerminalCommandTestMint
{
public:
    static C09TerminalCommand
    mint(C09CommandIdentity identity, C09TerminalOutcome outcome)
    {
        return C09TerminalCommand{std::move(identity), outcome};
    }
};

}  // namespace catl::provider_lab

namespace {

C09Destination const kDestination{
    .provider = 1,
    .provider_epoch = 2,
    .partition = 3,
    .peer_set_incarnation = 4,
    .receiver_generation = 5};

C09MemberLineage const kLineage{
    .operation = 11,
    .interest = 12,
    .use = 13,
    .pipeline = 14,
    .channel_generation = 15};

using Inbox = C09PeerSetTerminalInbox;
using Outbox = C09PeerClientTerminalOutbox;
using DispatchCase = Inbox::Case;
using RegisterRefusal = Inbox::RegisterRefusal;
using SenderRefusal = Outbox::Refusal;

struct ReceiverFixture : ::testing::Test
{
    DeterministicScheduler sched;
    DeterministicScheduler::strand_type sender_strand = sched.make_strand();
    DeterministicScheduler::strand_type receiver_strand = sched.make_strand();
    Outbox sender{sender_strand, kDestination, 6, 7};
    Inbox receiver{receiver_strand, kDestination};

    template <typename Fn>
    void
    on_sender(Fn fn)
    {
        asio::post(sender_strand, std::move(fn));
        sched.run_until_idle();
    }

    template <typename Fn>
    void
    on_receiver(Fn fn)
    {
        asio::post(receiver_strand, std::move(fn));
        sched.run_until_idle();
    }

    C09MemberIdentity
    open(C09MemberLineage lineage = kLineage)
    {
        std::optional<C09MemberIdentity> member;
        on_sender([&] {
            auto result = sender.open_member(lineage);
            ASSERT_EQ(result.refusal, SenderRefusal::ok);
            ASSERT_TRUE(result.member);
            member = *result.member;
        });
        if (!member)
            throw std::logic_error{"C09 test member setup failed"};
        return *member;
    }

    void
    register_receiver(C09MemberIdentity const& member)
    {
        on_receiver([&] {
            EXPECT_EQ(
                C09ReceiverTestRegistration::register_member(receiver, member),
                RegisterRefusal::ok);
        });
    }

    C09TerminalCommand
    complete(C09MemberIdentity const& member, C09PayloadDigest payload = 99)
    {
        std::optional<C09TerminalCommand> command;
        on_sender([&] {
            auto attempt = sender.register_attempt(member, payload);
            ASSERT_EQ(attempt.refusal, SenderRefusal::ok);
            ASSERT_TRUE(attempt.lease.valid());
            ASSERT_EQ(sender.publish_attempt(attempt.lease), SenderRefusal::ok);
            auto result = sender.complete(attempt.lease);
            ASSERT_EQ(result.refusal, SenderRefusal::ok);
            ASSERT_TRUE(result.command);
            command = *result.command;
        });
        if (!command)
            throw std::logic_error{"C09 test command setup failed"};
        return *command;
    }

    Inbox::DispatchResult
    dispatch(C09TerminalCommand const& command)
    {
        Inbox::DispatchResult result;
        on_receiver([&] { result = receiver.dispatch(command); });
        return result;
    }
};

}  // namespace

TEST(C09ReceiverTypes, AuthorityArtifactsAndAggregateAreNotRawOrCopyable)
{
    static_assert(!std::is_default_constructible_v<C09TerminalCommand>);
    static_assert(!std::is_default_constructible_v<C09TerminalReceipt>);
    static_assert(!std::is_copy_constructible_v<Inbox>);
    static_assert(!std::is_copy_assignable_v<Inbox>);
    static_assert(!std::is_move_constructible_v<Inbox>);
    static_assert(!std::is_move_assignable_v<Inbox>);
    SUCCEED();
}

TEST_F(ReceiverFixture, OffStrandRefusesBeforeReadingOrMutating)
{
    C09MemberIdentity const member{kDestination, 6, 7, kLineage};
    auto const command = C09TerminalCommandTestMint::mint(
        {member, 1, 1, 1}, C09TerminalOutcome::transport_complete);

    EXPECT_EQ(
        C09ReceiverTestRegistration::register_member(receiver, member),
        RegisterRefusal::off_strand);
    EXPECT_EQ(receiver.dispatch(command).result, DispatchCase::off_strand);
    EXPECT_FALSE(receiver.member_count());
    EXPECT_FALSE(receiver.inbox_count());
    EXPECT_FALSE(receiver.reserved_sequence_permits());

    on_receiver([&] {
        EXPECT_EQ(receiver.member_count(), 0u);
        EXPECT_EQ(receiver.inbox_count(), 0u);
        EXPECT_EQ(receiver.reserved_sequence_permits(), 0u);
    });
}

TEST_F(ReceiverFixture, RegistrationIsIdempotentButRejectsKeyRebinding)
{
    auto member = open();
    register_receiver(member);

    on_receiver([&] {
        EXPECT_EQ(
            C09ReceiverTestRegistration::register_member(receiver, member),
            RegisterRefusal::already_registered);

        auto rebound = member;
        ++rebound.lineage.operation;
        EXPECT_EQ(
            C09ReceiverTestRegistration::register_member(receiver, rebound),
            RegisterRefusal::member_binding_conflict);

        auto wrong_destination = member;
        ++wrong_destination.destination.provider_epoch;
        EXPECT_EQ(
            C09ReceiverTestRegistration::register_member(
                receiver, wrong_destination),
            RegisterRefusal::wrong_destination);

        auto invalid = member;
        invalid.lineage.operation = 0;
        EXPECT_EQ(
            C09ReceiverTestRegistration::register_member(receiver, invalid),
            RegisterRefusal::invalid_identity);
        EXPECT_EQ(receiver.member_count(), 1u);
        EXPECT_EQ(receiver.reserved_sequence_permits(), 1u);
    });
}

TEST_F(ReceiverFixture, FirstCommitReplayAndSenderSettlementCompose)
{
    auto member = open();
    register_receiver(member);
    auto command = complete(member);

    auto first = dispatch(command);
    ASSERT_EQ(first.result, DispatchCase::first_commit);
    ASSERT_TRUE(first.receipt);
    EXPECT_EQ(first.receipt->identity(), command.identity());
    EXPECT_EQ(first.receipt->outcome(), command.outcome());
    EXPECT_EQ(first.receipt->receiver_commit_sequence(), 1u);

    auto replay = dispatch(command);
    ASSERT_EQ(replay.result, DispatchCase::exact_replay);
    ASSERT_TRUE(replay.receipt);
    EXPECT_EQ(*replay.receipt, *first.receipt);

    on_receiver([&] {
        EXPECT_EQ(receiver.member_count(), 1u);
        EXPECT_EQ(receiver.inbox_count(), 1u);
        EXPECT_EQ(receiver.reserved_sequence_permits(), 0u);
        auto state = receiver.member_state(member);
        ASSERT_EQ(state.refusal, RegisterRefusal::ok);
        EXPECT_EQ(state.state, Inbox::MemberState::terminal_received);
    });
    on_sender([&] {
        EXPECT_EQ(sender.consume_receipt(*first.receipt), SenderRefusal::ok);
        EXPECT_EQ(
            sender.consume_receipt(*first.receipt),
            SenderRefusal::already_settled);
        EXPECT_EQ(sender.outbox_count(), 0u);
    });
}

TEST_F(ReceiverFixture, DestinationAndIdentityChecksDominateLookup)
{
    auto member = open();
    register_receiver(member);
    auto command = complete(member);

    auto wrong_destination = command.identity();
    ++wrong_destination.member.destination.receiver_generation;
    wrong_destination.command = 0;
    EXPECT_EQ(
        dispatch(C09TerminalCommandTestMint::mint(
                     wrong_destination, command.outcome()))
            .result,
        DispatchCase::wrong_destination);

    auto invalid = command.identity();
    invalid.command = 0;
    EXPECT_EQ(
        dispatch(C09TerminalCommandTestMint::mint(invalid, command.outcome()))
            .result,
        DispatchCase::invalid_identity);

    auto unknown = command.identity();
    ++unknown.member.lineage.pipeline;
    EXPECT_EQ(
        dispatch(C09TerminalCommandTestMint::mint(unknown, command.outcome()))
            .result,
        DispatchCase::unknown_member);

    EXPECT_EQ(
        dispatch(C09TerminalCommandTestMint::mint(
                     command.identity(), static_cast<C09TerminalOutcome>(255)))
            .result,
        DispatchCase::invalid_identity);
}

TEST_F(ReceiverFixture, ConflictingCommandKeyCannotChangeRetainedTruth)
{
    auto member = open();
    register_receiver(member);
    auto command = complete(member);
    auto first = dispatch(command);
    ASSERT_EQ(first.result, DispatchCase::first_commit);

    auto changed = command.identity();
    ++changed.payload;
    auto conflict =
        C09TerminalCommandTestMint::mint(changed, command.outcome());
    EXPECT_EQ(dispatch(conflict).result, DispatchCase::command_reuse_conflict);

    auto changed_outcome = C09TerminalCommandTestMint::mint(
        command.identity(), C09TerminalOutcome::cancelled);
    EXPECT_EQ(
        dispatch(changed_outcome).result, DispatchCase::command_reuse_conflict);

    auto replay = dispatch(command);
    ASSERT_EQ(replay.result, DispatchCase::exact_replay);
    EXPECT_EQ(replay.receipt, first.receipt);
}

TEST_F(ReceiverFixture, ASecondCommandCannotReterminalizeOneMember)
{
    auto member = open();
    register_receiver(member);
    auto first_command = complete(member);
    ASSERT_EQ(dispatch(first_command).result, DispatchCase::first_commit);

    auto second_identity = first_command.identity();
    ++second_identity.command;
    ++second_identity.transport_attempt;
    auto second = C09TerminalCommandTestMint::mint(
        second_identity, C09TerminalOutcome::rejected);
    EXPECT_EQ(dispatch(second).result, DispatchCase::member_terminal_conflict);
    on_receiver([&] { EXPECT_EQ(receiver.inbox_count(), 1u); });
}

TEST_F(ReceiverFixture, MemberAndCommandKeysAuthenticateTheirFullBindings)
{
    auto first_member = open(kLineage);
    auto second_member = open(C09MemberLineage{21, 22, 23, 24, 25});
    register_receiver(first_member);
    register_receiver(second_member);
    auto first_command = complete(first_member);
    ASSERT_EQ(dispatch(first_command).result, DispatchCase::first_commit);

    auto rebound_member = first_command.identity();
    ++rebound_member.member.lineage.operation;
    ++rebound_member.command;
    EXPECT_EQ(
        dispatch(C09TerminalCommandTestMint::mint(
                     rebound_member, first_command.outcome()))
            .result,
        DispatchCase::member_binding_conflict);

    auto reused_command = first_command.identity();
    reused_command.member = second_member;
    EXPECT_EQ(
        dispatch(C09TerminalCommandTestMint::mint(
                     reused_command, first_command.outcome()))
            .result,
        DispatchCase::command_reuse_conflict);

    on_receiver([&] {
        auto second_state = receiver.member_state(second_member);
        ASSERT_TRUE(second_state.state);
        EXPECT_EQ(*second_state.state, Inbox::MemberState::awaiting_terminal);
        EXPECT_EQ(receiver.reserved_sequence_permits(), 1u);
    });
}

TEST_F(ReceiverFixture, ReceiverSequencePermitIsReservedAtRegistration)
{
    auto first_member = open();
    on_receiver([&] {
        EXPECT_EQ(
            C09ReceiverTestSeed::next_sequence(
                receiver,
                std::numeric_limits<C09ReceiverCommitSequence>::max() - 1),
            RegisterRefusal::ok);
        EXPECT_EQ(
            C09ReceiverTestRegistration::register_member(
                receiver, first_member),
            RegisterRefusal::ok);
    });

    auto first_command = complete(first_member);
    auto committed = dispatch(first_command);
    ASSERT_EQ(committed.result, DispatchCase::first_commit);
    ASSERT_TRUE(committed.receipt);
    EXPECT_EQ(
        committed.receipt->receiver_commit_sequence(),
        std::numeric_limits<C09ReceiverCommitSequence>::max());
    EXPECT_EQ(dispatch(first_command).result, DispatchCase::exact_replay);

    auto second_member = open(C09MemberLineage{21, 22, 23, 24, 25});
    on_receiver([&] {
        EXPECT_EQ(
            C09ReceiverTestRegistration::register_member(
                receiver, second_member),
            RegisterRefusal::receiver_sequence_exhausted);
        EXPECT_EQ(receiver.member_count(), 1u);
    });
}

TEST_F(ReceiverFixture, PreCommitFailureLeavesPermitAndSequenceReusable)
{
    auto member = open();
    register_receiver(member);
    auto command = complete(member);

    on_receiver([&] {
        EXPECT_EQ(
            C09ReceiverTestSeed::fail_at(
                receiver, Inbox::TestFailpoint::dispatch_pre_commit),
            RegisterRefusal::ok);
        EXPECT_THROW((void)receiver.dispatch(command), std::runtime_error);
        EXPECT_EQ(receiver.inbox_count(), 0u);
        EXPECT_EQ(receiver.reserved_sequence_permits(), 1u);
        auto state = receiver.member_state(member);
        ASSERT_TRUE(state.state);
        EXPECT_EQ(*state.state, Inbox::MemberState::awaiting_terminal);

        EXPECT_EQ(
            C09ReceiverTestSeed::fail_at(receiver, Inbox::TestFailpoint::none),
            RegisterRefusal::ok);
        auto committed = receiver.dispatch(command);
        ASSERT_EQ(committed.result, DispatchCase::first_commit);
        ASSERT_TRUE(committed.receipt);
        EXPECT_EQ(committed.receipt->receiver_commit_sequence(), 1u);
    });
}

TEST_F(ReceiverFixture, IndependentMembersCommitIndependentReceipts)
{
    auto first_member = open(kLineage);
    auto second_member = open(C09MemberLineage{21, 22, 23, 24, 25});
    register_receiver(first_member);
    register_receiver(second_member);
    auto first_command = complete(first_member, 101);
    auto second_command = complete(second_member, 102);
    auto second = dispatch(second_command);
    auto first = dispatch(first_command);

    ASSERT_EQ(first.result, DispatchCase::first_commit);
    ASSERT_EQ(second.result, DispatchCase::first_commit);
    ASSERT_TRUE(first.receipt);
    ASSERT_TRUE(second.receipt);
    EXPECT_EQ(second.receipt->receiver_commit_sequence(), 1u);
    EXPECT_EQ(first.receipt->receiver_commit_sequence(), 2u);
    auto late_replay = dispatch(second_command);
    ASSERT_EQ(late_replay.result, DispatchCase::exact_replay);
    EXPECT_EQ(late_replay.receipt, second.receipt);
    on_receiver([&] {
        EXPECT_EQ(receiver.member_count(), 2u);
        EXPECT_EQ(receiver.inbox_count(), 2u);
        EXPECT_EQ(receiver.reserved_sequence_permits(), 0u);
    });
}
