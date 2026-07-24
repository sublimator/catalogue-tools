#include "c09-transport-terminal.h"

#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

#include <array>
#include <functional>
#include <optional>
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
    SUCCEED();
}

TEST_F(SenderFixture, OffStrandRefusesBeforeReadingOrMutating)
{
    EXPECT_EQ(sender.open_member(kLineage).refusal, Refusal::off_strand);
    EXPECT_EQ(sender.attempt_count(), 0u);
    EXPECT_EQ(sender.outbox_count(), 0u);

    run([&] {
        auto member = opened_member();
        auto lease = registered(member);
        EXPECT_EQ(sender.attempt_count(), 1u);
        EXPECT_EQ(sender.outbox_count(), 0u);
        EXPECT_EQ(sender.attempt_state(lease), AttemptState::unpublished_open);
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

TEST_F(SenderFixture, FourOutcomeDomainHasOneOwnedSelectionPathEach)
{
    run([&] {
        auto member = opened_member();

        auto complete = registered(member, 101);
        ASSERT_EQ(sender.publish_attempt(complete), Refusal::ok);
        auto completed = sender.complete(complete);
        ASSERT_EQ(completed.refusal, Refusal::ok);
        ASSERT_TRUE(completed.command);
        EXPECT_EQ(
            completed.command->outcome(),
            C09TerminalOutcome::transport_complete);

        auto reject = registered(member, 102);
        ASSERT_EQ(sender.publish_attempt(reject), Refusal::ok);
        auto rejected = sender.reject(reject);
        ASSERT_EQ(rejected.refusal, Refusal::ok);
        ASSERT_TRUE(rejected.command);
        EXPECT_EQ(rejected.command->outcome(), C09TerminalOutcome::rejected);

        auto cancel = registered(member, 103);
        ASSERT_EQ(sender.publish_attempt(cancel), Refusal::ok);
        auto cancelled = sender.cancel(cancel);
        ASSERT_EQ(cancelled.refusal, Refusal::ok);
        ASSERT_TRUE(cancelled.command);
        EXPECT_EQ(cancelled.command->outcome(), C09TerminalOutcome::cancelled);

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
        EXPECT_EQ(sender.attempt_state(lease), AttemptState::unpublished_open);
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
            baseline_outbox = sender.outbox_count();
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
        EXPECT_EQ(sender.consume_receipt(exact), Refusal::unknown_attempt)
            << "a duplicate receipt cannot consume a later row";
    });
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
        EXPECT_EQ(a.attempt_state(lease_a), AttemptState::unpublished_open);
    });
    asio::post(strand_b, [&] {
        EXPECT_EQ(b.publish_attempt(lease_a), Refusal::wrong_sender);
        EXPECT_EQ(b.attempt_state(lease_b), AttemptState::unpublished_open);
    });
    sched.run_until_idle();
}
