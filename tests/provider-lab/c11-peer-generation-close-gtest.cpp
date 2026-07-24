#include "c11-peer-generation-close.h"

#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

#include <limits>
#include <optional>
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

C11FenceBinding const kFenceBinding{
    .fence_sequence = 102,
    .command = 103,
    .payload = 104,
    .outcome = C11C10Outcome::installed,
    .receiver_commit_sequence = 105,
    .receipt_permit = 106};

C09MemberIdentity
member(std::uint64_t n)
{
    return {
        kDestination,
        6,
        7,
        C09MemberLineage{10 + n, 20 + n, 30 + n, 40 + n, 50 + n}};
}

using Sender = C11PeerClientGenerationAggregate;
using Receiver = C11PeerSetGenerationAggregate;
using SenderRefusal = Sender::Refusal;
using ReceiverCase = Receiver::Case;

struct CloseFixture : ::testing::Test
{
    DeterministicScheduler sched;
    DeterministicScheduler::strand_type sender_strand = sched.make_strand();
    DeterministicScheduler::strand_type receiver_strand = sched.make_strand();
    Sender sender{sender_strand, kDestination, 6, 7, 8};
    Receiver receiver{receiver_strand, kDestination, 6, 7, 8};

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

    void
    install(std::vector<C09MemberIdentity> const& members)
    {
        on_sender([&] {
            for (auto const& identity : members)
                ASSERT_EQ(
                    C11PeerClientTestAccess::accept(sender, identity).refusal,
                    C10PeerClientGenerationLifecycle::Refusal::ok);
            ASSERT_EQ(
                C11PeerClientTestAccess::install_fence(
                    sender, {kDestination, 6, 7, 101, members})
                    .refusal,
                C10PeerClientGenerationLifecycle::Refusal::ok);
            C11PeerClientTestAccess::bind_fence(sender, kFenceBinding);
        });
        on_receiver([&] {
            ASSERT_EQ(
                C11PeerSetTestAccess::install_fence(
                    receiver, 101, kFenceBinding, members),
                Receiver::InstallRefusal::ok);
        });
    }

    C09TerminalCommand
    terminal(C09MemberIdentity const& identity, C09PayloadDigest payload)
    {
        std::optional<C09TerminalCommand> command;
        on_sender([&] {
            auto attempt = C11PeerClientTestAccess::register_attempt(
                sender, identity, payload);
            ASSERT_EQ(
                attempt.refusal, C09PeerClientTerminalOutbox::Refusal::ok);
            ASSERT_EQ(
                C11PeerClientTestAccess::publish_attempt(sender, attempt.lease),
                C09PeerClientTerminalOutbox::Refusal::ok);
            auto complete =
                C11PeerClientTestAccess::complete(sender, attempt.lease);
            ASSERT_EQ(
                complete.refusal, C09PeerClientTerminalOutbox::Refusal::ok);
            ASSERT_TRUE(complete.command);
            command = *complete.command;
        });
        if (!command)
            throw std::logic_error{"terminal setup failed"};
        return *command;
    }

    void
    deliver_and_settle(C09TerminalCommand const& command)
    {
        std::optional<C09TerminalReceipt> receipt;
        on_receiver([&] {
            auto result = receiver.dispatch_terminal(command);
            ASSERT_EQ(
                result.result, C09PeerSetTerminalInbox::Case::first_commit);
            ASSERT_TRUE(result.receipt);
            receipt = *result.receipt;
        });
        on_sender([&] {
            ASSERT_TRUE(receipt);
            EXPECT_EQ(
                C11PeerClientTestAccess::consume_terminal_receipt(
                    sender, *receipt),
                C09PeerClientTerminalOutbox::Refusal::ok);
        });
    }

    C11PeerGenerationCloseCommand
    ready_close()
    {
        std::optional<C11PeerGenerationCloseCommand> command;
        on_sender([&] {
            auto barrier = sender.commit_projection_barrier();
            ASSERT_EQ(barrier.refusal, SenderRefusal::ok);
            auto close = sender.begin_close();
            ASSERT_EQ(close.refusal, SenderRefusal::ok);
            ASSERT_TRUE(close.command);
            command = *close.command;
        });
        if (!command)
            throw std::logic_error{"close setup failed"};
        return *command;
    }

    std::pair<C11PeerGenerationCloseCommand, C11PeerGenerationCloseReceipt>
    honest_exchange(
        C09Destination destination,
        C11FenceBinding binding,
        std::vector<C09MemberIdentity> members,
        C11CommandId prior_close_command = 0)
    {
        auto sender_owner = sched.make_strand();
        auto receiver_owner = sched.make_strand();
        Sender other_sender{sender_owner, destination, 6, 7, 8};
        Receiver other_receiver{receiver_owner, destination, 6, 7, 8};

        auto run = [&](auto const& owner, auto fn) {
            asio::post(owner, std::move(fn));
            sched.run_until_idle();
        };
        auto require = [](bool condition, char const* what) {
            if (!condition)
                throw std::logic_error{what};
        };

        run(sender_owner, [&] {
            for (auto const& identity : members)
                require(
                    C11PeerClientTestAccess::accept(other_sender, identity)
                            .refusal ==
                        C10PeerClientGenerationLifecycle::Refusal::ok,
                    "alternate C10 accept failed");
            require(
                C11PeerClientTestAccess::install_fence(
                    other_sender, {destination, 6, 7, 101, members})
                        .refusal ==
                    C10PeerClientGenerationLifecycle::Refusal::ok,
                "alternate C10 fence failed");
            C11PeerClientTestAccess::bind_fence(other_sender, binding);
        });
        run(receiver_owner, [&] {
            require(
                C11PeerSetTestAccess::install_fence(
                    other_receiver, 101, binding, members) ==
                    Receiver::InstallRefusal::ok,
                "alternate receiver fence failed");
        });

        for (std::size_t index = 0; index != members.size(); ++index)
        {
            std::optional<C09TerminalCommand> terminal_command;
            run(sender_owner, [&] {
                auto attempt = C11PeerClientTestAccess::register_attempt(
                    other_sender, members[index], 900 + index);
                require(
                    attempt.refusal == C09PeerClientTerminalOutbox::Refusal::ok,
                    "alternate attempt failed");
                require(
                    C11PeerClientTestAccess::publish_attempt(
                        other_sender, attempt.lease) ==
                        C09PeerClientTerminalOutbox::Refusal::ok,
                    "alternate publish failed");
                auto completed = C11PeerClientTestAccess::complete(
                    other_sender, attempt.lease);
                require(
                    completed.refusal ==
                            C09PeerClientTerminalOutbox::Refusal::ok &&
                        completed.command.has_value(),
                    "alternate terminal failed");
                terminal_command = completed.command;
            });

            std::optional<C09TerminalReceipt> terminal_receipt;
            run(receiver_owner, [&] {
                auto received =
                    other_receiver.dispatch_terminal(*terminal_command);
                require(
                    received.result ==
                            C09PeerSetTerminalInbox::Case::first_commit &&
                        received.receipt.has_value(),
                    "alternate terminal receive failed");
                terminal_receipt = received.receipt;
            });
            run(sender_owner, [&] {
                require(
                    C11PeerClientTestAccess::consume_terminal_receipt(
                        other_sender, *terminal_receipt) ==
                        C09PeerClientTerminalOutbox::Refusal::ok,
                    "alternate terminal settlement failed");
            });
        }

        std::optional<C11PeerGenerationCloseCommand> close_command;
        run(sender_owner, [&] {
            require(
                other_sender.commit_projection_barrier().refusal ==
                    SenderRefusal::ok,
                "alternate barrier failed");
            if (prior_close_command != 0)
                C11PeerClientTestAccess::next_close_ids(
                    other_sender, prior_close_command, 0);
            auto begun = other_sender.begin_close();
            require(
                begun.refusal == SenderRefusal::ok && begun.command.has_value(),
                "alternate close failed");
            close_command = begun.command;
        });

        std::optional<C11PeerGenerationCloseReceipt> close_receipt;
        run(receiver_owner, [&] {
            auto received = other_receiver.dispatch_close(*close_command);
            require(
                received.result == ReceiverCase::first_commit &&
                    received.receipt.has_value(),
                "alternate close receive failed");
            close_receipt = received.receipt;
        });
        return {*close_command, *close_receipt};
    }
};

}  // namespace

TEST(C11Types, AuthorityArtifactsAndAggregatesAreNotRawOrMovable)
{
    static_assert(
        !std::is_default_constructible_v<C11PeerGenerationCloseCommand>);
    static_assert(
        !std::is_default_constructible_v<C11PeerGenerationCloseReceipt>);
    static_assert(!std::is_copy_constructible_v<Sender>);
    static_assert(!std::is_move_constructible_v<Sender>);
    static_assert(!std::is_copy_constructible_v<Receiver>);
    static_assert(!std::is_move_constructible_v<Receiver>);
    SUCCEED();
}

TEST_F(CloseFixture, OffStrandRefusesBeforeReadingState)
{
    EXPECT_EQ(
        sender.commit_projection_barrier().refusal, SenderRefusal::off_strand);
    EXPECT_EQ(sender.begin_close().refusal, SenderRefusal::off_strand);
    EXPECT_FALSE(sender.mode());
    EXPECT_FALSE(sender.has_generation_detail());
    EXPECT_FALSE(receiver.closure_pending());
    EXPECT_FALSE(receiver.c09_retained_after_close());
}

TEST_F(CloseFixture, CloseRequiresExactSettlementAndProjectionBarrier)
{
    auto const a = member(1);
    install({a});
    auto command = terminal(a, 201);

    on_sender([&] {
        EXPECT_EQ(
            sender.commit_projection_barrier().refusal,
            SenderRefusal::settlement_incomplete);
        EXPECT_EQ(
            sender.begin_close().refusal,
            SenderRefusal::projection_barrier_missing);
    });

    deliver_and_settle(command);
    on_sender([&] {
        EXPECT_EQ(
            sender.begin_close().refusal,
            SenderRefusal::projection_barrier_missing);
        EXPECT_EQ(
            sender.commit_projection_barrier().refusal, SenderRefusal::ok);
        EXPECT_EQ(sender.begin_close().refusal, SenderRefusal::ok);
    });
}

TEST_F(CloseFixture, SettledGenerationStillWaitsForExactAsyncSource)
{
    auto const a = member(1);
    install({a});
    deliver_and_settle(terminal(a, 201));

    C11AsyncSourceLease delayed;
    on_sender([&] {
        auto opened = sender.open_async_source();
        ASSERT_EQ(opened.refusal, SenderRefusal::ok);
        delayed = std::move(opened.lease);

        EXPECT_EQ(
            sender.commit_projection_barrier().refusal,
            SenderRefusal::async_source_pending);

        C09AttemptLease retired_attempt;
        auto dispatched = sender.dispatch_async_attempt(
            std::move(delayed),
            retired_attempt,
            Sender::AsyncAttemptAction::state);
        EXPECT_EQ(dispatched.dispatch, Sender::AsyncDispatchCase::forwarded);
        EXPECT_EQ(
            dispatched.refusal,
            C09PeerClientTerminalOutbox::Refusal::invalid_lease);
        EXPECT_FALSE(delayed.valid());
        EXPECT_EQ(
            sender.commit_projection_barrier().refusal, SenderRefusal::ok);
    });
}

TEST_F(CloseFixture, FenceBindingCannotPrecedeOrRewriteTheC10Commit)
{
    on_sender([&] {
        EXPECT_THROW(
            C11PeerClientTestAccess::bind_fence(sender, kFenceBinding),
            std::logic_error);
    });

    auto const a = member(1);
    install({a});
    on_sender([&] {
        EXPECT_NO_THROW(
            C11PeerClientTestAccess::bind_fence(sender, kFenceBinding));
        auto changed = kFenceBinding;
        ++changed.receipt_permit;
        EXPECT_THROW(
            C11PeerClientTestAccess::bind_fence(sender, changed),
            std::logic_error);
    });
}

TEST_F(CloseFixture, FirstCommitReplaysAndRetainsReceiverC09State)
{
    auto const a = member(1);
    auto const b = member(2);
    install({b, a});
    auto const terminal_a = terminal(a, 201);
    auto const terminal_b = terminal(b, 202);
    deliver_and_settle(terminal_a);
    deliver_and_settle(terminal_b);
    auto command = ready_close();

    std::optional<C11PeerGenerationCloseReceipt> receipt;
    std::optional<C11PeerClientGenerationAggregate::BeginCloseReceipt>
        begin_receipt;
    std::optional<C11PeerClientGenerationAggregate::C10ClosedHighWater>
        high_water;
    on_sender([&] {
        begin_receipt = sender.begin_close_receipt();
        high_water = sender.c10_closed_high_water();
        ASSERT_TRUE(begin_receipt);
        ASSERT_TRUE(high_water);
        EXPECT_EQ(begin_receipt->command, command.identity().command);
        EXPECT_EQ(high_water->fence, command.identity().fence);
    });
    on_receiver([&] {
        auto first = receiver.dispatch_close(command);
        ASSERT_EQ(first.result, ReceiverCase::first_commit);
        ASSERT_TRUE(first.receipt);
        receipt = *first.receipt;
        EXPECT_EQ(receiver.closure_pending(), true);
        EXPECT_EQ(receiver.c09_retained_after_close(), true);
        EXPECT_EQ(receiver.terminal_member_count(), 2u);
        EXPECT_EQ(receiver.terminal_inbox_count(), 2u);

        auto replay = receiver.dispatch_close(command);
        EXPECT_EQ(replay.result, ReceiverCase::exact_replay);
        EXPECT_EQ(replay.receipt, receipt);
        EXPECT_EQ(receiver.terminal_member_count(), 2u);
        EXPECT_EQ(receiver.terminal_inbox_count(), 2u);

        auto late_c09 = receiver.dispatch_terminal(terminal_a);
        EXPECT_EQ(late_c09.result, C09PeerSetTerminalInbox::Case::exact_replay)
            << "T162 retains the C09 inbox for delayed terminal projections";
    });

    on_sender([&] {
        ASSERT_TRUE(receipt);
        EXPECT_EQ(sender.consume_close_receipt(*receipt), SenderRefusal::ok);
        EXPECT_EQ(sender.mode(), Sender::Mode::closed);
        EXPECT_EQ(sender.has_generation_detail(), false);
        ASSERT_TRUE(sender.closed_witness());
        EXPECT_EQ(
            sender.closed_witness()->receiver_commit_sequence,
            receipt->receiver_commit_sequence());
        EXPECT_EQ(
            sender.consume_close_receipt(*receipt),
            SenderRefusal::close_already_consumed);
        EXPECT_EQ(sender.begin_close_receipt(), begin_receipt)
            << "T164 retains the T116 receipt required by later T72";
        EXPECT_EQ(sender.c10_closed_high_water(), high_water)
            << "T164 retains the explicit predecessor high-water";
    });

    on_receiver([&] {
        EXPECT_EQ(receiver.c09_retained_after_close(), true)
            << "T162 is closure-pending, never receiver compaction";
    });
}

TEST_F(CloseFixture, AsyncLeaseBlocksBarrierAndLateActionIsEpochSafe)
{
    auto const a = member(1);
    install({a});

    C11AsyncSourceLease async;
    C09AttemptLease attempt_lease;
    std::optional<C09TerminalCommand> terminal_command;
    on_sender([&] {
        auto opened = sender.open_async_source();
        ASSERT_EQ(opened.refusal, SenderRefusal::ok);
        async = std::move(opened.lease);

        auto attempt =
            C11PeerClientTestAccess::register_attempt(sender, a, 201);
        ASSERT_EQ(attempt.refusal, C09PeerClientTerminalOutbox::Refusal::ok);
        attempt_lease = std::move(attempt.lease);
        ASSERT_EQ(
            C11PeerClientTestAccess::publish_attempt(sender, attempt_lease),
            C09PeerClientTerminalOutbox::Refusal::ok);
    });

    on_sender([&] {
        EXPECT_EQ(
            sender.commit_projection_barrier().refusal,
            SenderRefusal::async_source_pending);

        ASSERT_EQ(
            C11PeerClientTestAccess::fail_c09_at(
                sender,
                C09PeerClientTerminalOutbox::TestFailpoint::
                    terminal_pre_commit),
            C09PeerClientTerminalOutbox::Refusal::ok);
        EXPECT_THROW(
            sender.dispatch_async_attempt(
                std::move(async),
                attempt_lease,
                Sender::AsyncAttemptAction::complete),
            std::runtime_error);
        EXPECT_TRUE(async.valid())
            << "a failed callback must leave its source pending";
        EXPECT_EQ(
            sender.commit_projection_barrier().refusal,
            SenderRefusal::async_source_pending);
        ASSERT_EQ(
            C11PeerClientTestAccess::fail_c09_at(
                sender, C09PeerClientTerminalOutbox::TestFailpoint::none),
            C09PeerClientTerminalOutbox::Refusal::ok);

        auto delayed = sender.dispatch_async_attempt(
            std::move(async),
            attempt_lease,
            Sender::AsyncAttemptAction::complete);
        EXPECT_EQ(delayed.dispatch, Sender::AsyncDispatchCase::forwarded);
        ASSERT_TRUE(delayed.refusal);
        EXPECT_EQ(*delayed.refusal, C09PeerClientTerminalOutbox::Refusal::ok);
        terminal_command = delayed.command;
        EXPECT_FALSE(async.valid());
    });
    ASSERT_TRUE(terminal_command);
    deliver_and_settle(*terminal_command);

    on_sender([&] {
        auto barrier = sender.commit_projection_barrier();
        ASSERT_EQ(barrier.refusal, SenderRefusal::ok);
        EXPECT_EQ(barrier.final_projection_sequence, 1u);
        EXPECT_EQ(
            sender.open_async_source().refusal,
            SenderRefusal::projection_barrier_committed);
    });

    auto command = ready_close();
    std::optional<C11PeerGenerationCloseReceipt> receipt;
    on_receiver([&] {
        auto closed = receiver.dispatch_close(command);
        ASSERT_EQ(closed.result, ReceiverCase::first_commit);
        receipt = closed.receipt;
    });
    on_sender([&] {
        ASSERT_TRUE(receipt);
        ASSERT_EQ(sender.consume_close_receipt(*receipt), SenderRefusal::ok);

        auto delayed = sender.dispatch_async_attempt(
            std::move(async),
            attempt_lease,
            Sender::AsyncAttemptAction::complete);
        EXPECT_EQ(
            delayed.dispatch, Sender::AsyncDispatchCase::generation_retired);
        EXPECT_FALSE(delayed.command)
            << "retired detail was classified without dereferencing C10";
    });
}

TEST_F(CloseFixture, AsyncDispatcherOwnsEveryLeaseBearingC09Action)
{
    auto const a = member(1);
    auto const b = member(2);
    auto const c = member(3);
    auto const d = member(4);
    auto const e = member(5);
    install({a, b, c, d, e});

    on_sender([&] {
        auto open_attempt = [&](C09MemberIdentity const& identity) {
            auto opened = C11PeerClientTestAccess::register_attempt(
                sender, identity, identity.lineage.operation);
            EXPECT_EQ(opened.refusal, C09PeerClientTerminalOutbox::Refusal::ok);
            return std::move(opened.lease);
        };
        auto dispatch = [&](C09AttemptLease const& attempt,
                            Sender::AsyncAttemptAction action) {
            auto source = sender.open_async_source();
            EXPECT_EQ(source.refusal, SenderRefusal::ok);
            auto result = sender.dispatch_async_attempt(
                std::move(source.lease), attempt, action);
            EXPECT_EQ(result.dispatch, Sender::AsyncDispatchCase::forwarded);
            EXPECT_TRUE(result.refusal);
            EXPECT_FALSE(source.lease.valid());
            EXPECT_EQ(
                sender
                    .dispatch_async_attempt(
                        std::move(source.lease), attempt, action)
                    .dispatch,
                Sender::AsyncDispatchCase::invalid_source)
                << "one source can execute its callback only once";
            return result;
        };

        auto retry = open_attempt(a);
        C11AsyncSourceLease missing_source;
        EXPECT_EQ(
            sender
                .dispatch_async_attempt(
                    std::move(missing_source),
                    retry,
                    Sender::AsyncAttemptAction::publish)
                .dispatch,
            Sender::AsyncDispatchCase::invalid_source)
            << "a C09 action cannot execute without a pending source";
        EXPECT_EQ(
            dispatch(retry, Sender::AsyncAttemptAction::publish).refusal,
            C09PeerClientTerminalOutbox::Refusal::ok);
        EXPECT_EQ(
            dispatch(retry, Sender::AsyncAttemptAction::retry_terminal).refusal,
            C09PeerClientTerminalOutbox::Refusal::ok);

        auto complete = open_attempt(b);
        EXPECT_EQ(
            dispatch(complete, Sender::AsyncAttemptAction::publish).refusal,
            C09PeerClientTerminalOutbox::Refusal::ok);
        auto completed =
            dispatch(complete, Sender::AsyncAttemptAction::complete);
        EXPECT_EQ(completed.refusal, C09PeerClientTerminalOutbox::Refusal::ok);
        ASSERT_TRUE(completed.command);
        auto replay = dispatch(complete, Sender::AsyncAttemptAction::replay);
        EXPECT_EQ(replay.refusal, C09PeerClientTerminalOutbox::Refusal::ok);
        EXPECT_EQ(replay.command, completed.command);
        auto state = dispatch(complete, Sender::AsyncAttemptAction::state);
        EXPECT_EQ(state.refusal, C09PeerClientTerminalOutbox::Refusal::ok);
        EXPECT_EQ(
            state.state, C09PeerClientTerminalOutbox::AttemptState::terminal);

        auto rejected = open_attempt(c);
        EXPECT_EQ(
            dispatch(rejected, Sender::AsyncAttemptAction::publish).refusal,
            C09PeerClientTerminalOutbox::Refusal::ok);
        EXPECT_EQ(
            dispatch(rejected, Sender::AsyncAttemptAction::reject).refusal,
            C09PeerClientTerminalOutbox::Refusal::ok);

        auto cancelled = open_attempt(d);
        EXPECT_EQ(
            dispatch(cancelled, Sender::AsyncAttemptAction::cancel).refusal,
            C09PeerClientTerminalOutbox::Refusal::ok);

        auto never_published = C11PeerClientTestAccess::register_attempt(
            sender, e, e.lineage.operation + 100);
        ASSERT_EQ(
            never_published.refusal, C09PeerClientTerminalOutbox::Refusal::ok);
        EXPECT_EQ(
            dispatch(
                never_published.lease,
                Sender::AsyncAttemptAction::seal_never_published)
                .refusal,
            C09PeerClientTerminalOutbox::Refusal::ok);
    });
}

TEST_F(CloseFixture, EmptyGenerationStillClosesExactly)
{
    install({});
    auto command = ready_close();
    EXPECT_EQ(command.identity().settlement.settled_members.count, 0u);
    EXPECT_NE(command.identity().settlement.settled_members.digest, 0u);
    EXPECT_EQ(command.identity().settlement.final_sequence, 0u);

    std::optional<C11PeerGenerationCloseReceipt> receipt;
    on_receiver([&] {
        auto first = receiver.dispatch_close(command);
        ASSERT_EQ(first.result, ReceiverCase::first_commit);
        ASSERT_TRUE(first.receipt);
        receipt = first.receipt;
        EXPECT_EQ(receiver.terminal_member_count(), 0u);
        EXPECT_EQ(receiver.terminal_inbox_count(), 0u);
        EXPECT_EQ(receiver.c09_retained_after_close(), true);
    });
    on_sender([&] {
        ASSERT_TRUE(receipt);
        EXPECT_EQ(sender.consume_close_receipt(*receipt), SenderRefusal::ok);
        EXPECT_EQ(sender.has_generation_detail(), false);
    });
}

TEST_F(CloseFixture, SenderOutboxReplaysUntilExactReceipt)
{
    auto const a = member(1);
    install({a});
    deliver_and_settle(terminal(a, 201));
    auto command = ready_close();

    auto foreign_destination = kDestination;
    ++foreign_destination.peer_set_incarnation;
    auto foreign = honest_exchange(foreign_destination, kFenceBinding, {});
    auto conflicting_binding = kFenceBinding;
    ++conflicting_binding.payload;
    auto conflicting = honest_exchange(kDestination, conflicting_binding, {});

    on_sender([&] {
        auto replay = sender.replay_close();
        ASSERT_EQ(replay.refusal, SenderRefusal::ok);
        EXPECT_EQ(replay.command, command);

        EXPECT_EQ(
            sender.consume_close_receipt(foreign.second),
            SenderRefusal::receipt_scope_mismatch);
        EXPECT_EQ(sender.replay_close().command, command);

        EXPECT_EQ(
            sender.consume_close_receipt(conflicting.second),
            SenderRefusal::receipt_binding_mismatch);
        EXPECT_EQ(sender.replay_close().command, command);
        EXPECT_EQ(sender.has_generation_detail(), true);
    });
}

TEST_F(CloseFixture, ReceiverPartitionsReplayConflictAndWrongReceiver)
{
    auto const a = member(1);
    install({a});
    deliver_and_settle(terminal(a, 201));
    auto command = ready_close();

    auto conflicting_binding = kFenceBinding;
    ++conflicting_binding.payload;
    auto conflicting = honest_exchange(kDestination, conflicting_binding, {});
    auto later = honest_exchange(
        kDestination, conflicting_binding, {}, /*prior_close_command=*/1);
    auto wrong_destination = kDestination;
    ++wrong_destination.peer_set_incarnation;
    auto wrong = honest_exchange(wrong_destination, kFenceBinding, {});

    on_receiver([&] {
        ASSERT_EQ(
            receiver.dispatch_close(command).result,
            ReceiverCase::first_commit);

        EXPECT_EQ(
            receiver.dispatch_close(conflicting.first).result,
            ReceiverCase::command_reuse_conflict);

        EXPECT_EQ(
            receiver.dispatch_close(later.first).result,
            ReceiverCase::command_reuse_conflict)
            << "T165 needs the later T39 high-water; an open C11 inbox is "
               "still T166 conflict";

        EXPECT_EQ(
            receiver.dispatch_close(wrong.first).result,
            ReceiverCase::wrong_receiver)
            << "destination routing dominates malformed identity";
    });
}

TEST_F(CloseFixture, ReceiverRefusesBeforeAllTerminalMembersArrive)
{
    auto const a = member(1);
    auto const b = member(2);
    auto honest = honest_exchange(kDestination, kFenceBinding, {a, b});
    install({a, b});
    deliver_and_settle(terminal(a, 201));

    on_receiver([&] {
        EXPECT_EQ(
            receiver.dispatch_close(honest.first).result,
            ReceiverCase::settlement_mismatch);
        EXPECT_EQ(receiver.closure_pending(), false);
        EXPECT_EQ(receiver.terminal_member_count(), 2u);
        EXPECT_EQ(receiver.terminal_inbox_count(), 1u);
    });
}

TEST_F(CloseFixture, ReceiverRefusesSameCardinalityDifferentMemberSet)
{
    auto const local = member(1);
    auto const foreign = member(2);
    auto foreign_exchange =
        honest_exchange(kDestination, kFenceBinding, {foreign});

    install({local});
    deliver_and_settle(terminal(local, 201));

    ASSERT_EQ(
        foreign_exchange.first.identity().settlement.settled_members.count, 1u);
    ASSERT_NE(
        foreign_exchange.first.identity().settlement.settled_members.digest,
        c11_detail::member_set_digest({local}));
    on_receiver([&] {
        EXPECT_EQ(receiver.terminal_member_count(), 1u);
        EXPECT_EQ(receiver.terminal_inbox_count(), 1u);
        EXPECT_EQ(
            receiver.dispatch_close(foreign_exchange.first).result,
            ReceiverCase::settlement_mismatch);
        EXPECT_EQ(receiver.closure_pending(), false);
    });
}

TEST_F(CloseFixture, ReducedMemberKeyCannotBeReboundDuringInstall)
{
    auto a = member(1);
    auto rebound = a;
    ++rebound.lineage.operation;

    on_receiver([&] {
        EXPECT_EQ(
            C11PeerSetTestAccess::install_fence(
                receiver, 101, kFenceBinding, {a, rebound}),
            Receiver::InstallRefusal::member_binding_conflict);
        EXPECT_EQ(receiver.terminal_member_count(), 0u);
        EXPECT_EQ(receiver.terminal_inbox_count(), 0u);
        EXPECT_EQ(receiver.closure_pending(), false);
    });
}

TEST_F(CloseFixture, ZeroChannelGenerationCannotEnterTheExactSet)
{
    auto invalid = member(1);
    invalid.lineage.channel_generation = 0;

    on_sender([&] {
        EXPECT_EQ(
            C11PeerClientTestAccess::accept(sender, invalid).refusal,
            C10PeerClientGenerationLifecycle::Refusal::invalid_member_identity);
    });
    on_receiver([&] {
        EXPECT_EQ(
            C11PeerSetTestAccess::install_fence(
                receiver, 101, kFenceBinding, {invalid}),
            Receiver::InstallRefusal::invalid_binding);
        EXPECT_EQ(receiver.terminal_member_count(), 0u);
    });
}

TEST_F(CloseFixture, ReceiverInstallFailureIsAtomicAndRetryable)
{
    auto const a = member(1);
    auto const b = member(2);
    on_receiver([&] {
        C11PeerSetTestAccess::fail_at(
            receiver, Receiver::TestFailpoint::install_pre_commit);
        EXPECT_THROW(
            (void)C11PeerSetTestAccess::install_fence(
                receiver, 101, kFenceBinding, {a, b}),
            std::runtime_error);
        EXPECT_EQ(receiver.terminal_member_count(), 0u);
        EXPECT_EQ(receiver.terminal_inbox_count(), 0u);

        C11PeerSetTestAccess::fail_at(receiver, Receiver::TestFailpoint::none);
        EXPECT_EQ(
            C11PeerSetTestAccess::install_fence(
                receiver, 101, kFenceBinding, {a, b}),
            Receiver::InstallRefusal::ok);
        EXPECT_EQ(receiver.terminal_member_count(), 2u);
    });
}

TEST_F(CloseFixture, IdentifierCeilingsRefuseWithoutConsumingState)
{
    auto const a = member(1);
    install({a});
    deliver_and_settle(terminal(a, 201));

    on_sender([&] {
        ASSERT_EQ(
            sender.commit_projection_barrier().refusal, SenderRefusal::ok);
        C11PeerClientTestAccess::next_close_ids(
            sender, std::numeric_limits<C11CommandId>::max(), 0);
        EXPECT_EQ(sender.begin_close().refusal, SenderRefusal::id_exhausted);
        EXPECT_NE(sender.replay_close().refusal, SenderRefusal::ok);
        C11PeerClientTestAccess::next_close_ids(sender, 0, 0);
    });
    auto command = ready_close();

    on_receiver([&] {
        C11PeerSetTestAccess::next_close_receipt_sequence(
            receiver, std::numeric_limits<C11ReceiverCommitSequence>::max());
        EXPECT_EQ(
            receiver.dispatch_close(command).result,
            ReceiverCase::receiver_sequence_exhausted);
        EXPECT_EQ(receiver.closure_pending(), false);
        EXPECT_EQ(receiver.c09_retained_after_close(), false);
    });
}

TEST_F(CloseFixture, FailpointsLeaveBothOwnersRetryable)
{
    auto const a = member(1);
    install({a});
    deliver_and_settle(terminal(a, 201));

    on_sender([&] {
        C11PeerClientTestAccess::fail_at(
            sender, Sender::TestFailpoint::projection_pre_commit);
        EXPECT_THROW(
            (void)sender.commit_projection_barrier(), std::runtime_error);
        EXPECT_EQ(
            sender.begin_close().refusal,
            SenderRefusal::projection_barrier_missing);
        C11PeerClientTestAccess::fail_at(sender, Sender::TestFailpoint::none);
        ASSERT_EQ(
            sender.commit_projection_barrier().refusal, SenderRefusal::ok);

        C11PeerClientTestAccess::fail_at(
            sender, Sender::TestFailpoint::close_pre_commit);
        EXPECT_THROW((void)sender.begin_close(), std::runtime_error);
        EXPECT_NE(sender.replay_close().refusal, SenderRefusal::ok);
        C11PeerClientTestAccess::fail_at(sender, Sender::TestFailpoint::none);
    });
    auto command = ready_close();

    on_receiver([&] {
        C11PeerSetTestAccess::fail_at(
            receiver, Receiver::TestFailpoint::close_pre_commit);
        EXPECT_THROW(
            (void)receiver.dispatch_close(command), std::runtime_error);
        EXPECT_EQ(receiver.closure_pending(), false);
        EXPECT_EQ(receiver.c09_retained_after_close(), false);
        C11PeerSetTestAccess::fail_at(receiver, Receiver::TestFailpoint::none);
    });

    std::optional<C11PeerGenerationCloseReceipt> receipt;
    on_receiver([&] {
        auto committed = receiver.dispatch_close(command);
        ASSERT_EQ(committed.result, ReceiverCase::first_commit);
        receipt = committed.receipt;
    });
    on_sender([&] {
        C11PeerClientTestAccess::fail_at(
            sender, Sender::TestFailpoint::receipt_pre_commit);
        ASSERT_TRUE(receipt);
        EXPECT_THROW(
            (void)sender.consume_close_receipt(*receipt), std::runtime_error);
        EXPECT_EQ(sender.has_generation_detail(), true);
        EXPECT_EQ(sender.replay_close().command, command);
        C11PeerClientTestAccess::fail_at(sender, Sender::TestFailpoint::none);
        EXPECT_EQ(sender.consume_close_receipt(*receipt), SenderRefusal::ok);
        EXPECT_EQ(sender.has_generation_detail(), false);
    });
}
