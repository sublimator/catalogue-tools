#include "c09-transport-terminal.h"

#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

#include <array>
#include <functional>
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

C09MemberLineage const kLineage{
    .operation = 11,
    .interest = 12,
    .use = 13,
    .pipeline = 14,
    .channel_generation = 15};

using Outbox = C09PeerClientTerminalOutbox;
using Refusal = Outbox::Refusal;
using AttemptState = Outbox::AttemptState;

struct SenderFixture : ::testing::Test
{
    DeterministicScheduler sched;
    DeterministicScheduler::strand_type strand = sched.make_strand();
    Outbox sender{strand, kDestination, 6, 7};

    template <typename Fn>
    void
    run(Fn fn)
    {
        asio::post(strand, std::move(fn));
        sched.run_until_idle();
    }

    C09MemberIdentity
    opened_member(C09MemberLineage lineage = kLineage)
    {
        auto result = sender.open_member(lineage);
        EXPECT_EQ(result.refusal, Refusal::ok);
        EXPECT_TRUE(result.member.has_value());
        return *result.member;
    }

    C09AttemptLease
    registered(
        C09MemberIdentity const& member,
        C09PayloadDigest payload = 0xCA7)
    {
        auto result = sender.register_attempt(member, payload);
        EXPECT_EQ(result.refusal, Refusal::ok);
        EXPECT_TRUE(result.lease.valid());
        return std::move(result.lease);
    }
};

}  // namespace

TEST(C09SenderTypes, AuthorityArtifactsCannotBeRawConstructed)
{
    static_assert(!std::is_default_constructible_v<C09TerminalCommand>);
    static_assert(!std::is_default_constructible_v<C09TerminalReceipt>);
    static_assert(!std::is_constructible_v<
                  C09TerminalCommand,
                  C09CommandIdentity,
                  C09TerminalOutcome>);
    static_assert(!std::is_constructible_v<
                  C09TerminalReceipt,
                  C09CommandIdentity,
                  C09TerminalOutcome,
                  C09ReceiverCommitSequence>);
    static_assert(!std::is_copy_constructible_v<Outbox>);
    static_assert(!std::is_copy_assignable_v<Outbox>);
    static_assert(!std::is_move_constructible_v<Outbox>);
    static_assert(!std::is_move_assignable_v<Outbox>);
    SUCCEED();
}

TEST_F(SenderFixture, OffStrandRefusesBeforeReadingOrMutating)
{
    EXPECT_EQ(sender.open_member(kLineage).refusal, Refusal::off_strand);
    EXPECT_FALSE(sender.attempt_count().has_value());
    EXPECT_FALSE(sender.outbox_count().has_value());
    EXPECT_FALSE(sender.live_attempt_count().has_value());
    EXPECT_FALSE(sender.retry_history_count().has_value());
    EXPECT_EQ(
        sender.attempt_state(C09AttemptLease{}).refusal, Refusal::off_strand);

    run([&] {
        EXPECT_EQ(
            sender.attempt_state(C09AttemptLease{}).refusal,
            Refusal::invalid_lease);
        auto member = opened_member();
        auto lease = registered(member);
        EXPECT_EQ(sender.attempt_count(), 1u);
        EXPECT_EQ(sender.outbox_count(), 0u);
        EXPECT_EQ(
            sender.attempt_state(lease).state, AttemptState::unpublished_open);
    });
}

TEST_F(SenderFixture, InvalidOrUnopenedMemberCannotMintAnAttempt)
{
    run([&] {
        auto invalid = sender.open_member({});
        EXPECT_EQ(invalid.refusal, Refusal::invalid_identity);

        C09MemberIdentity forged{
            kDestination, 6, 7, C09MemberLineage{21, 22, 23, 24, 25}};
        auto attempt = sender.register_attempt(forged, 1);
        EXPECT_EQ(attempt.refusal, Refusal::unknown_member);
        EXPECT_EQ(sender.attempt_count(), 0u);
    });
}

TEST_F(SenderFixture, DuplicateOpenAndConcurrentAttemptAreTypedRefusals)
{
    run([&] {
        auto first = sender.open_member(kLineage);
        ASSERT_EQ(first.refusal, Refusal::ok);
        ASSERT_TRUE(first.member);

        auto duplicate = sender.open_member(kLineage);
        EXPECT_EQ(duplicate.refusal, Refusal::already_open);
        ASSERT_TRUE(duplicate.member);
        EXPECT_EQ(*duplicate.member, *first.member);
        EXPECT_EQ(sender.attempt_count(), 1u);
        EXPECT_EQ(sender.live_attempt_count(), 1u)
            << "the reserved first-attempt permit is drain-visible";

        auto active = registered(*first.member);
        auto competing = sender.register_attempt(*first.member, 99);
        EXPECT_EQ(competing.refusal, Refusal::member_attempt_active);
        EXPECT_EQ(sender.attempt_count(), 1u);
        EXPECT_EQ(sender.commit_retry_terminal(active), Refusal::ok);
        EXPECT_EQ(sender.attempt_count(), 2u);
        EXPECT_EQ(sender.live_attempt_count(), 1u)
            << "the reserved successor permit is drain-visible";
        EXPECT_EQ(sender.retry_history_count(), 1u);
    });
}

TEST_F(SenderFixture, PublishedRetryCannotBeLaunderedIntoNeverPublished)
{
    run([&] {
        auto member = opened_member();
        auto first = registered(member, 101);
        ASSERT_EQ(sender.publish_attempt(first), Refusal::ok);
        ASSERT_EQ(sender.commit_retry_terminal(first), Refusal::ok);
        EXPECT_EQ(sender.attempt_count(), 2u);
        EXPECT_EQ(sender.live_attempt_count(), 1u);
        EXPECT_EQ(
            sender.attempt_state(first).state,
            AttemptState::retry_terminal_published);
        EXPECT_EQ(sender.publish_attempt(first), Refusal::attempt_retired);
        EXPECT_EQ(sender.complete(first).refusal, Refusal::attempt_retired);
        EXPECT_EQ(sender.cancel(first).refusal, Refusal::attempt_retired);
        EXPECT_EQ(sender.retry_history_count(), 1u)
            << "late callbacks cannot erase the owned retry tombstone";

        auto final_attempt = registered(member, 102);
        auto false_absence = sender.seal_never_published(final_attempt);
        EXPECT_EQ(
            false_absence.refusal, Refusal::never_published_after_attempt);
        EXPECT_FALSE(false_absence.command);
        EXPECT_EQ(sender.outbox_count(), 0u);

        auto truthful = sender.cancel(final_attempt);
        ASSERT_EQ(truthful.refusal, Refusal::ok);
        ASSERT_TRUE(truthful.command);
        EXPECT_EQ(truthful.command->outcome(), C09TerminalOutcome::cancelled);
        EXPECT_EQ(truthful.command->identity().transport_attempt, 2u);

        auto after_export = sender.register_attempt(member, 103);
        EXPECT_EQ(after_export.refusal, Refusal::member_sealed);
        EXPECT_EQ(sender.outbox_count(), 1u);
    });
}

TEST_F(SenderFixture, UnpublishedLocalRetryPreservesTruthfulAbsence)
{
    run([&] {
        auto member = opened_member();
        auto first = registered(member, 101);
        ASSERT_EQ(sender.commit_retry_terminal(first), Refusal::ok);
        EXPECT_EQ(
            sender.attempt_state(first).state,
            AttemptState::retry_terminal_unpublished);

        auto final_attempt = registered(member, 102);
        auto absent = sender.seal_never_published(final_attempt);
        ASSERT_EQ(absent.refusal, Refusal::ok);
        ASSERT_TRUE(absent.command);
        EXPECT_EQ(
            absent.command->outcome(), C09TerminalOutcome::never_published);
        EXPECT_EQ(absent.command->identity().transport_attempt, 2u);
    });
}

TEST_F(SenderFixture, RetryTerminalAndExportHaveOneSerializedWinner)
{
    for (bool retry_wins : {true, false})
    {
        run([&] {
            auto const delta = retry_wins ? 1u : 2u;
            auto member = opened_member(C09MemberLineage{
                100u + delta,
                200u + delta,
                300u + delta,
                400u + delta,
                500u + delta});
            auto attempt = registered(member);
            ASSERT_EQ(sender.publish_attempt(attempt), Refusal::ok);

            if (retry_wins)
            {
                EXPECT_EQ(sender.commit_retry_terminal(attempt), Refusal::ok);
                EXPECT_EQ(
                    sender.complete(attempt).refusal, Refusal::attempt_retired);
                EXPECT_EQ(sender.retry_history_count(), 1u);
            }
            else
            {
                EXPECT_EQ(sender.complete(attempt).refusal, Refusal::ok);
                EXPECT_EQ(
                    sender.commit_retry_terminal(attempt),
                    Refusal::attempt_already_terminal);
                EXPECT_EQ(
                    sender.register_attempt(member, 2).refusal,
                    Refusal::member_sealed);
            }
        });
    }
}

TEST_F(SenderFixture, FourOutcomeDomainHasOneOwnedSelectionPathEach)
{
    run([&] {
        auto member = opened_member(C09MemberLineage{11, 12, 13, 14, 15});

        auto complete = registered(member, 101);
        ASSERT_EQ(sender.publish_attempt(complete), Refusal::ok);
        auto completed = sender.complete(complete);
        ASSERT_EQ(completed.refusal, Refusal::ok);
        ASSERT_TRUE(completed.command);
        EXPECT_EQ(
            completed.command->outcome(),
            C09TerminalOutcome::transport_complete);

        member = opened_member(C09MemberLineage{21, 22, 23, 24, 25});
        auto reject = registered(member, 102);
        ASSERT_EQ(sender.publish_attempt(reject), Refusal::ok);
        auto rejected = sender.reject(reject);
        ASSERT_EQ(rejected.refusal, Refusal::ok);
        ASSERT_TRUE(rejected.command);
        EXPECT_EQ(rejected.command->outcome(), C09TerminalOutcome::rejected);

        member = opened_member(C09MemberLineage{31, 32, 33, 34, 35});
        auto cancel = registered(member, 103);
        ASSERT_EQ(sender.publish_attempt(cancel), Refusal::ok);
        auto cancelled = sender.cancel(cancel);
        ASSERT_EQ(cancelled.refusal, Refusal::ok);
        ASSERT_TRUE(cancelled.command);
        EXPECT_EQ(cancelled.command->outcome(), C09TerminalOutcome::cancelled);

        member = opened_member(C09MemberLineage{41, 42, 43, 44, 45});
        auto absent = registered(member, 104);
        auto never = sender.cancel(absent);
        ASSERT_EQ(never.refusal, Refusal::ok);
        ASSERT_TRUE(never.command);
        EXPECT_EQ(
            never.command->outcome(), C09TerminalOutcome::never_published);

        EXPECT_EQ(sender.outbox_count(), 4u);
        EXPECT_EQ(sender.attempt_count(), 4u);
    });
}

TEST_F(SenderFixture, PublishedOutcomesRefuseWithoutAttemptCommit)
{
    run([&] {
        auto member = opened_member();
        auto lease = registered(member);

        EXPECT_EQ(
            sender.complete(lease).refusal, Refusal::attempt_not_committed);
        EXPECT_EQ(sender.reject(lease).refusal, Refusal::attempt_not_committed);
        EXPECT_EQ(sender.outbox_count(), 0u);
        EXPECT_EQ(
            sender.attempt_state(lease).state, AttemptState::unpublished_open);
    });
}

TEST_F(SenderFixture, PublicationAndNeverPublishedAreAnAtomicEitherOr)
{
    for (bool publish_first : {true, false})
    {
        auto member = C09MemberIdentity{};
        auto lease = C09AttemptLease{};
        std::size_t baseline_outbox = 0;
        run([&] {
            baseline_outbox = sender.outbox_count().value();
            std::uint64_t const delta = publish_first ? 1u : 0u;
            member = opened_member(C09MemberLineage{
                100u + delta,
                200u + delta,
                300u + delta,
                400u + delta,
                500u + delta});
            lease = registered(member);
        });

        std::optional<Refusal> publish_result;
        std::optional<Outbox::TerminalResult> absence_result;
        auto publish = [&] { publish_result = sender.publish_attempt(lease); };
        auto seal = [&] {
            absence_result = sender.seal_never_published(lease);
        };
        if (publish_first)
        {
            asio::post(strand, publish);
            asio::post(strand, seal);
        }
        else
        {
            asio::post(strand, seal);
            asio::post(strand, publish);
        }
        sched.run_until_idle();

        run([&] {
            ASSERT_TRUE(publish_result);
            ASSERT_TRUE(absence_result);
            if (publish_first)
            {
                EXPECT_EQ(*publish_result, Refusal::ok);
                EXPECT_EQ(
                    absence_result->refusal,
                    Refusal::never_published_after_attempt);
                EXPECT_EQ(sender.outbox_count(), 0u);
                auto completed = sender.complete(lease);
                ASSERT_EQ(completed.refusal, Refusal::ok);
                EXPECT_EQ(
                    completed.command->outcome(),
                    C09TerminalOutcome::transport_complete);
            }
            else
            {
                EXPECT_EQ(absence_result->refusal, Refusal::ok);
                EXPECT_EQ(*publish_result, Refusal::attempt_already_terminal);
                ASSERT_TRUE(absence_result->command);
                EXPECT_EQ(
                    absence_result->command->outcome(),
                    C09TerminalOutcome::never_published);
            }
            EXPECT_EQ(sender.outbox_count(), baseline_outbox + 1);
        });
    }
}

TEST_F(SenderFixture, FirstPublishedTerminalWinnerCannotBeRewritten)
{
    using Select =
        std::function<Outbox::TerminalResult(C09AttemptLease const&)>;
    std::array<std::pair<C09TerminalOutcome, Select>, 3> selectors{{
        {C09TerminalOutcome::transport_complete,
         [&](auto const& lease) { return sender.complete(lease); }},
        {C09TerminalOutcome::rejected,
         [&](auto const& lease) { return sender.reject(lease); }},
        {C09TerminalOutcome::cancelled,
         [&](auto const& lease) { return sender.cancel(lease); }},
    }};

    for (std::size_t first = 0; first < selectors.size(); ++first)
    {
        for (std::size_t second = 0; second < selectors.size(); ++second)
        {
            if (first == second)
                continue;
            run([&] {
                auto lineage = C09MemberLineage{
                    1000 + first * 10 + second,
                    2000 + first * 10 + second,
                    3000 + first * 10 + second,
                    4000 + first * 10 + second,
                    5000 + first * 10 + second};
                auto member = opened_member(lineage);
                auto lease = registered(member);
                ASSERT_EQ(sender.publish_attempt(lease), Refusal::ok);

                auto winner = selectors[first].second(lease);
                ASSERT_EQ(winner.refusal, Refusal::ok);
                ASSERT_TRUE(winner.command);
                auto loser = selectors[second].second(lease);
                EXPECT_EQ(loser.refusal, Refusal::attempt_already_terminal);
                ASSERT_TRUE(loser.command);
                EXPECT_EQ(*loser.command, *winner.command);
                EXPECT_EQ(winner.command->outcome(), selectors[first].first);
            });
        }
    }
}

TEST_F(SenderFixture, ReplayProjectsTheSameOwnedOutboxRow)
{
    run([&] {
        auto member = opened_member();
        auto lease = registered(member);
        ASSERT_EQ(sender.publish_attempt(lease), Refusal::ok);
        auto terminal = sender.complete(lease);
        ASSERT_EQ(terminal.refusal, Refusal::ok);
        ASSERT_TRUE(terminal.command);

        auto replay1 = sender.replay(lease);
        auto replay2 = sender.replay(lease);
        ASSERT_EQ(replay1.refusal, Refusal::ok);
        ASSERT_EQ(replay2.refusal, Refusal::ok);
        ASSERT_TRUE(replay1.command);
        ASSERT_TRUE(replay2.command);
        EXPECT_EQ(*replay1.command, *terminal.command);
        EXPECT_EQ(*replay2.command, *terminal.command);
        EXPECT_EQ(sender.outbox_count(), 1u);
    });
}

TEST_F(SenderFixture, OnlyTheExactReceiverReceiptConsumesTheOutbox)
{
    run([&] {
        auto member = opened_member();
        auto lease = registered(member);
        ASSERT_EQ(sender.publish_attempt(lease), Refusal::ok);
        auto terminal = sender.reject(lease);
        ASSERT_EQ(terminal.refusal, Refusal::ok);
        ASSERT_TRUE(terminal.command);

        auto identity = terminal.command->identity();
        std::vector<std::function<void(C09CommandIdentity&)>> mutations{
            [](auto& i) { ++i.member.destination.provider; },
            [](auto& i) { ++i.member.destination.provider_epoch; },
            [](auto& i) { ++i.member.destination.partition; },
            [](auto& i) { ++i.member.destination.peer_set_incarnation; },
            [](auto& i) { ++i.member.destination.receiver_generation; },
            [](auto& i) { ++i.member.peer_client_incarnation; },
            [](auto& i) { ++i.member.sender_generation; },
            [](auto& i) { ++i.member.lineage.operation; },
            [](auto& i) { ++i.member.lineage.interest; },
            [](auto& i) { ++i.member.lineage.use; },
            [](auto& i) { ++i.member.lineage.pipeline; },
            [](auto& i) { ++i.member.lineage.channel_generation; },
            [](auto& i) { ++i.transport_attempt; },
            [](auto& i) { ++i.command; },
            [](auto& i) { ++i.payload; },
        };
        for (auto const& mutate : mutations)
        {
            auto wrong_identity = identity;
            mutate(wrong_identity);
            auto wrong_binding = C09TerminalReceiptTestMint::mint(
                wrong_identity, terminal.command->outcome(), 1);
            EXPECT_NE(sender.consume_receipt(wrong_binding), Refusal::ok);
            EXPECT_EQ(sender.outbox_count(), 1u)
                << "every identity field is authenticated before release";
        }

        auto wrong_outcome = C09TerminalReceiptTestMint::mint(
            identity, C09TerminalOutcome::cancelled, 1);
        EXPECT_EQ(
            sender.consume_receipt(wrong_outcome),
            Refusal::receipt_binding_mismatch);

        auto zero_sequence = C09TerminalReceiptTestMint::mint(
            identity, terminal.command->outcome(), 0);
        EXPECT_EQ(
            sender.consume_receipt(zero_sequence),
            Refusal::receiver_sequence_invalid);
        EXPECT_EQ(sender.outbox_count(), 1u);

        auto exact = C09TerminalReceiptTestMint::mint(
            identity, terminal.command->outcome(), 9);
        EXPECT_EQ(sender.consume_receipt(exact), Refusal::ok);
        EXPECT_EQ(sender.outbox_count(), 0u);
        EXPECT_EQ(sender.attempt_count(), 0u);
        auto settlement = sender.settlement(member, identity.command);
        ASSERT_EQ(settlement.refusal, Refusal::ok);
        ASSERT_TRUE(settlement.receipt);
        EXPECT_EQ(*settlement.receipt, exact)
            << "the sender retains the complete receipt identity";
        EXPECT_EQ(sender.consume_receipt(exact), Refusal::unknown_attempt)
            << "a duplicate receipt cannot consume a later row";
        auto replay_after_receipt = sender.replay(lease);
        EXPECT_EQ(replay_after_receipt.refusal, Refusal::unknown_attempt);
        EXPECT_FALSE(replay_after_receipt.command);
        EXPECT_EQ(sender.outbox_count(), 0u)
            << "replay cannot recreate a consumed outbox row";
        EXPECT_EQ(
            sender.register_attempt(member, 2).refusal, Refusal::member_sealed)
            << "receipt consumption does not reopen the member";
    });
}

TEST_F(SenderFixture, ReceiptScopeIsAuthenticatedBeforeAttemptLookup)
{
    run([&] {
        auto member = opened_member();
        auto lease = registered(member);
        ASSERT_EQ(sender.publish_attempt(lease), Refusal::ok);
        auto terminal = sender.complete(lease);
        ASSERT_EQ(terminal.refusal, Refusal::ok);
        ASSERT_TRUE(terminal.command);

        auto foreign = terminal.command->identity();
        ++foreign.member.destination.receiver_generation;
        ++foreign.transport_attempt;
        auto receipt = C09TerminalReceiptTestMint::mint(
            foreign, terminal.command->outcome(), 1);
        EXPECT_EQ(
            sender.consume_receipt(receipt), Refusal::receipt_scope_mismatch);
        EXPECT_EQ(sender.outbox_count(), 1u);
    });
}

TEST(C09Sender, IdentifierCeilingsFailClosedWithoutWrapping)
{
    DeterministicScheduler sched;
    auto strand = sched.make_strand();
    Outbox sender{strand, kDestination, 6, 7};

    asio::post(strand, [&] {
        ASSERT_EQ(
            C09SenderTestSeed::next_ids(
                sender,
                std::numeric_limits<C09TransportAttemptId>::max() - 1,
                std::numeric_limits<C09CommandId>::max() - 1),
            Refusal::ok);
        auto member = sender.open_member(kLineage);
        ASSERT_EQ(member.refusal, Refusal::ok);
        auto no_command_left =
            sender.open_member(C09MemberLineage{21, 22, 23, 24, 25});
        EXPECT_EQ(no_command_left.refusal, Refusal::id_exhausted)
            << "a member is never admitted without its terminal command";

        auto attempt = sender.register_attempt(*member.member, 1);
        ASSERT_EQ(attempt.refusal, Refusal::ok);
        EXPECT_EQ(
            attempt.lease.id(),
            std::numeric_limits<C09TransportAttemptId>::max());
        EXPECT_EQ(
            C09SenderTestSeed::next_ids(sender, 0, 0),
            Refusal::member_attempt_active);
        ASSERT_EQ(sender.publish_attempt(attempt.lease), Refusal::ok);
        auto terminal = sender.complete(attempt.lease);
        ASSERT_EQ(terminal.refusal, Refusal::ok);
        ASSERT_TRUE(terminal.command);
        EXPECT_EQ(
            terminal.command->identity().command,
            std::numeric_limits<C09CommandId>::max());
    });
    sched.run_until_idle();

    DeterministicScheduler other_sched;
    auto other_strand = other_sched.make_strand();
    Outbox exhausted{other_strand, kDestination, 6, 7};
    asio::post(other_strand, [&] {
        ASSERT_EQ(
            C09SenderTestSeed::next_ids(
                exhausted,
                std::numeric_limits<C09TransportAttemptId>::max(),
                0),
            Refusal::ok);
        auto member = exhausted.open_member(kLineage);
        EXPECT_EQ(member.refusal, Refusal::id_exhausted);
        EXPECT_FALSE(member.member);
        EXPECT_EQ(exhausted.attempt_count(), 0u);
        EXPECT_EQ(
            C09SenderTestSeed::next_ids(exhausted, 0, 0),
            Refusal::seed_not_pristine)
            << "the ceiling seam cannot rewind an issued namespace";
    });
    other_sched.run_until_idle();

    DeterministicScheduler command_sched;
    auto command_strand = command_sched.make_strand();
    Outbox no_commands{command_strand, kDestination, 6, 7};
    asio::post(command_strand, [&] {
        ASSERT_EQ(
            C09SenderTestSeed::next_ids(
                no_commands, 0, std::numeric_limits<C09CommandId>::max()),
            Refusal::ok);
        EXPECT_EQ(
            no_commands.open_member(kLineage).refusal, Refusal::id_exhausted);
        EXPECT_EQ(no_commands.attempt_count(), 0u);
    });
    command_sched.run_until_idle();

    DeterministicScheduler retry_sched;
    auto retry_strand = retry_sched.make_strand();
    Outbox no_successor{retry_strand, kDestination, 6, 7};
    asio::post(retry_strand, [&] {
        ASSERT_EQ(
            C09SenderTestSeed::next_ids(
                no_successor,
                std::numeric_limits<C09TransportAttemptId>::max() - 1,
                0),
            Refusal::ok);
        auto member = no_successor.open_member(kLineage);
        ASSERT_EQ(member.refusal, Refusal::ok);
        auto attempt = no_successor.register_attempt(*member.member, 1);
        ASSERT_EQ(attempt.refusal, Refusal::ok);
        EXPECT_EQ(
            no_successor.commit_retry_terminal(attempt.lease),
            Refusal::id_exhausted);
        EXPECT_EQ(
            no_successor.attempt_state(attempt.lease).state,
            AttemptState::unpublished_open)
            << "retry refusal leaves the old attempt terminal-capable";
        auto terminal = no_successor.cancel(attempt.lease);
        EXPECT_EQ(terminal.refusal, Refusal::ok);
        ASSERT_TRUE(terminal.command);
        EXPECT_EQ(
            terminal.command->outcome(), C09TerminalOutcome::never_published);
    });
    retry_sched.run_until_idle();
}

TEST(C09Sender, CommitFailpointsLeaveNoPartialAuthority)
{
    DeterministicScheduler sched;
    auto strand = sched.make_strand();
    Outbox sender{strand, kDestination, 6, 7};

    asio::post(strand, [&] {
        ASSERT_EQ(
            C09SenderTestSeed::fail_at(
                sender, Outbox::TestFailpoint::open_after_attempt_reserve),
            Refusal::ok);
        EXPECT_THROW((void)sender.open_member(kLineage), std::runtime_error);
        EXPECT_EQ(sender.attempt_count(), 0u)
            << "failed member insertion rolls back its reserved attempt";

        ASSERT_EQ(
            C09SenderTestSeed::fail_at(sender, Outbox::TestFailpoint::none),
            Refusal::ok);
        auto member = sender.open_member(kLineage);
        ASSERT_EQ(member.refusal, Refusal::ok);
        ASSERT_EQ(
            C09SenderTestSeed::fail_at(
                sender, Outbox::TestFailpoint::register_pre_commit),
            Refusal::ok);
        EXPECT_THROW(
            (void)sender.register_attempt(*member.member, 1),
            std::runtime_error);
        EXPECT_EQ(sender.attempt_count(), 1u);
        EXPECT_EQ(sender.live_attempt_count(), 1u);

        ASSERT_EQ(
            C09SenderTestSeed::fail_at(sender, Outbox::TestFailpoint::none),
            Refusal::ok);
        auto attempt = sender.register_attempt(*member.member, 1);
        ASSERT_EQ(attempt.refusal, Refusal::ok);
        EXPECT_EQ(attempt.lease.id(), 1u)
            << "failed registration does not lose the reserved identifier";
        ASSERT_EQ(sender.publish_attempt(attempt.lease), Refusal::ok);

        ASSERT_EQ(
            C09SenderTestSeed::fail_at(
                sender, Outbox::TestFailpoint::terminal_pre_commit),
            Refusal::ok);
        EXPECT_THROW((void)sender.complete(attempt.lease), std::runtime_error);
        EXPECT_EQ(
            sender.attempt_state(attempt.lease).state,
            AttemptState::attempt_committed);
        EXPECT_EQ(sender.outbox_count(), 0u);

        ASSERT_EQ(
            C09SenderTestSeed::fail_at(sender, Outbox::TestFailpoint::none),
            Refusal::ok);
        auto terminal = sender.complete(attempt.lease);
        ASSERT_EQ(terminal.refusal, Refusal::ok);
        ASSERT_TRUE(terminal.command);
        EXPECT_EQ(terminal.command->identity().command, 1u)
            << "failed terminal commit consumes no identifier";

        auto receipt = C09TerminalReceiptTestMint::mint(
            terminal.command->identity(), terminal.command->outcome(), 9);
        ASSERT_EQ(
            C09SenderTestSeed::fail_at(
                sender, Outbox::TestFailpoint::receipt_pre_commit),
            Refusal::ok);
        EXPECT_THROW((void)sender.consume_receipt(receipt), std::runtime_error);
        EXPECT_EQ(sender.outbox_count(), 1u);
        EXPECT_EQ(sender.attempt_count(), 1u);

        ASSERT_EQ(
            C09SenderTestSeed::fail_at(sender, Outbox::TestFailpoint::none),
            Refusal::ok);
        EXPECT_EQ(sender.consume_receipt(receipt), Refusal::ok);
        EXPECT_EQ(sender.outbox_count(), 0u);
    });
    sched.run_until_idle();
}

TEST(C09Sender, LeaseFromAnotherSenderCannotAddressACollidingAttempt)
{
    DeterministicScheduler sched;
    auto strand_a = sched.make_strand();
    auto strand_b = sched.make_strand();
    Outbox a{strand_a, kDestination, 6, 7};
    Outbox b{strand_b, kDestination, 6, 7};
    C09AttemptLease lease_a;
    C09AttemptLease lease_b;

    asio::post(strand_a, [&] {
        auto member = a.open_member(kLineage);
        ASSERT_EQ(member.refusal, Refusal::ok);
        lease_a = std::move(a.register_attempt(*member.member, 1).lease);
    });
    asio::post(strand_b, [&] {
        auto member = b.open_member(kLineage);
        ASSERT_EQ(member.refusal, Refusal::ok);
        lease_b = std::move(b.register_attempt(*member.member, 2).lease);
    });
    sched.run_until_idle();

    asio::post(strand_a, [&] {
        EXPECT_EQ(a.publish_attempt(lease_b), Refusal::wrong_sender);
        EXPECT_EQ(a.attempt_state(lease_b).refusal, Refusal::wrong_sender);
        EXPECT_EQ(
            a.attempt_state(lease_a).state, AttemptState::unpublished_open);
    });
    asio::post(strand_b, [&] {
        EXPECT_EQ(b.publish_attempt(lease_a), Refusal::wrong_sender);
        EXPECT_EQ(b.attempt_state(lease_a).refusal, Refusal::wrong_sender);
        EXPECT_EQ(
            b.attempt_state(lease_b).state, AttemptState::unpublished_open);
    });
    sched.run_until_idle();
}
