#include <catl/peer-client/history-fetch.h>
#include <catl/peer-client/peer-client-coro.h>
#include <catl/peer-client/peer-set.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>

using namespace catl::peer_client;
using namespace std::chrono_literals;

namespace {

template <class T>
T
run(boost::asio::io_context& io, boost::asio::awaitable<T> operation)
{
    auto result = boost::asio::co_spawn(
        io, std::move(operation), boost::asio::use_future);
    while (result.wait_for(1ms) != std::future_status::ready)
    {
        io.poll();
        io.restart();
    }
    io.restart();
    return result.get();
}

struct Participant
{
    std::shared_ptr<catl::core::RequestScope> scope;
    LedgerDemandRef demand;
    HistoryAcquisitionRef acquisition;
    HistoryInterestRef interest;
};

struct Fixture : ::testing::Test
{
    Fixture()
        : peers(PeerSet::create(
              io,
              PeerSetOptions{
                  .max_targeted_connect_attempts = 3,
                  .max_active_ledger_demands = 32,
                  .max_acquisitions_per_demand = 8,
                  .max_history_promotions_per_demand = 1,
                  .max_history_blind_uses_per_demand = 1}))
        , now(catl::core::RequestScope::Clock::now())
        , fetch(SharedHistoryFetch::create(HistoryFetchId{7}, 3))
    {
    }

    HistoryAuthorityTarget
    target(std::string endpoint = "peer.example:51235") const
    {
        return HistoryAuthorityTarget{
            .endpoint = std::move(endpoint), .generation = 11, .use = 13};
    }

    HistoryParticipantPlan
    viable(
        catl::core::RequestScope::Clock::time_point at,
        std::string endpoint = "peer.example:51235",
        HistoryAuthorityAction action =
            HistoryAuthorityAction::create_generation) const
    {
        return HistoryParticipantPlan{
            .next_action = HistoryPlannedAction{
                .action = action,
                .target = target(std::move(endpoint)),
                .not_before = at}};
    }

    Participant
    participant(
        catl::core::RequestScope::Clock::time_point deadline,
        HistoryParticipantPlan plan)
    {
        Participant result;
        result.scope = catl::core::RequestScope::create(deadline);
        EXPECT_TRUE(result.scope);
        result.demand =
            run(io,
                peers->co_get_or_create_ledger_demand(
                    result.scope,
                    LedgerDemandOptions{
                        .ledger_seq = 50,
                        .fallback_at = now + 1s,
                        .remaining_new_dials = 3,
                        .remaining_promotions = 1,
                        .remaining_blind_uses = 1}));
        result.acquisition =
            run(io,
                peers->co_open_history_acquisition(
                    result.demand,
                    HistoryAcquisitionOptions{
                        .fetch = HistoryFetchId{7},
                        .fetch_epoch = 3,
                        .request_kind = HistoryRequestKind::ledger_header}));
        result.interest = fetch->register_interest(
            peers,
            result.scope,
            result.demand,
            result.acquisition,
            std::move(plan));
        EXPECT_TRUE(result.interest);
        return result;
    }

    boost::asio::io_context io;
    std::shared_ptr<PeerSet> peers;
    catl::core::RequestScope::Clock::time_point now;
    std::shared_ptr<SharedHistoryFetch> fetch;
};

}  // namespace

TEST_F(Fixture, EarliestDeadlineWinsAndEqualDeadlineTicketsRotate)
{
    auto late = participant(now + 20s, viable(now, "late"));
    auto early = participant(now + 10s, viable(now, "early"));
    auto decision = fetch->select_sponsor(now);
    ASSERT_TRUE(decision.sponsor);
    EXPECT_EQ(decision.sponsor->interest(), early.interest.interest());

    auto equal_fetch = SharedHistoryFetch::create(HistoryFetchId{8}, 3);
    ASSERT_TRUE(equal_fetch);
    auto deadline = now + 30s;
    auto first_scope = catl::core::RequestScope::create(deadline);
    auto second_scope = catl::core::RequestScope::create(deadline);
    auto first_demand =
        run(io,
            peers->co_get_or_create_ledger_demand(
                first_scope,
                LedgerDemandOptions{
                    .ledger_seq = 51,
                    .fallback_at = now + 1s,
                    .remaining_new_dials = 1}));
    auto second_demand =
        run(io,
            peers->co_get_or_create_ledger_demand(
                second_scope,
                LedgerDemandOptions{
                    .ledger_seq = 51,
                    .fallback_at = now + 1s,
                    .remaining_new_dials = 1}));
    auto open = [&](LedgerDemandRef demand) {
        return run(
            io,
            peers->co_open_history_acquisition(
                demand,
                HistoryAcquisitionOptions{
                    .fetch = HistoryFetchId{8},
                    .fetch_epoch = 3,
                    .request_kind = HistoryRequestKind::ledger_header}));
    };
    auto first_acquisition = open(first_demand);
    auto second_acquisition = open(second_demand);
    auto first = equal_fetch->register_interest(
        peers,
        first_scope,
        first_demand,
        first_acquisition,
        viable(now, "first"));
    auto second = equal_fetch->register_interest(
        peers,
        second_scope,
        second_demand,
        second_acquisition,
        viable(now, "second"));

    auto selected = equal_fetch->select_sponsor(now);
    ASSERT_TRUE(selected.sponsor);
    EXPECT_EQ(selected.sponsor->interest(), first.interest());
    ASSERT_TRUE(equal_fetch->mint_grant(*selected.sponsor, now));
    selected = equal_fetch->select_sponsor(now);
    ASSERT_TRUE(selected.sponsor);
    EXPECT_EQ(selected.sponsor->interest(), second.interest());
}

TEST_F(Fixture, NonviableParticipantIsSkippedAndFutureWakeIsAggregated)
{
    auto passive = participant(
        now + 10s,
        HistoryParticipantPlan{
            .local_terminal = HistoryAcquisitionTerminal::exhausted});
    auto later = participant(now + 20s, viable(now + 4s, "later"));
    auto earlier = participant(now + 30s, viable(now + 2s, "earlier"));

    auto before = fetch->select_sponsor(now);
    EXPECT_FALSE(before.sponsor);
    ASSERT_TRUE(before.aggregate_wake);
    EXPECT_EQ(*before.aggregate_wake, now + 2s);
    EXPECT_EQ(before.live_participants, 3u);
    EXPECT_EQ(before.viable_participants, 2u);

    auto ready = fetch->select_sponsor(now + 2s);
    ASSERT_TRUE(ready.sponsor);
    EXPECT_EQ(ready.sponsor->interest(), earlier.interest.interest());
    EXPECT_NE(ready.sponsor->interest(), passive.interest.interest());
    EXPECT_NE(ready.sponsor->interest(), later.interest.interest());
}

TEST_F(Fixture, ActionAtAbsoluteDeadlineIsNotFutureViability)
{
    auto const deadline = now + 10s;
    auto passive = participant(deadline, {});
    EXPECT_FALSE(
        fetch->revise_interest(passive.interest, viable(deadline, "too-late")));

    auto separate = SharedHistoryFetch::create(HistoryFetchId{7}, 3);
    ASSERT_TRUE(separate);
    EXPECT_THROW(
        separate->register_interest(
            peers,
            passive.scope,
            passive.demand,
            passive.acquisition,
            viable(deadline, "too-late")),
        PeerClientException);

    EXPECT_FALSE(fetch->select_sponsor(now).sponsor);
    EXPECT_TRUE(fetch->terminalize_if_no_viable_action(now));
}

TEST_F(Fixture, RevisionUsesThePlannerTimestampAtAnExpiredWakeBoundary)
{
    auto passive = participant(now + 10s, {});
    auto const planning_now = now - 2s;
    auto const wake = HistoryParticipantPlan{
        .wait_until = HistoryPlannedWake{
            .kind = HistoryWakeSourceKind::global_token_refill,
            .at = now - 1s,
            .token = 1}};

    // The same inert plan is expired against a fresh clock sample but was a
    // valid future wake in the snapshot from which the planner derived it.
    EXPECT_FALSE(fetch->revise_interest(passive.interest, wake));
    auto revised = fetch->revise_interest(passive.interest, wake, planning_now);
    ASSERT_TRUE(revised);
    EXPECT_GT(
        revised->participant_revision(),
        passive.interest.participant_revision());
}

TEST_F(Fixture, ExhaustionCancellationAndDetachCannotCancelViableSibling)
{
    auto first = participant(now + 10s, viable(now, "first"));
    auto second = participant(now + 20s, viable(now, "second"));
    auto revised = fetch->revise_interest(
        first.interest,
        HistoryParticipantPlan{
            .local_terminal = HistoryAcquisitionTerminal::exhausted});
    ASSERT_TRUE(revised);
    first.interest = *revised;
    ASSERT_TRUE(first.scope->cancel(now + 1ms));
    auto detached = fetch->detach(first.interest);
    EXPECT_TRUE(detached.accepted);
    EXPECT_FALSE(detached.terminalized_fetch);

    auto decision = fetch->select_sponsor(now + 2ms);
    ASSERT_TRUE(decision.sponsor);
    EXPECT_EQ(decision.sponsor->interest(), second.interest.interest());
    EXPECT_FALSE(fetch->snapshot().terminal);
}

TEST_F(Fixture, ActiveSponsorDetachPromotesViableSibling)
{
    auto sponsor = participant(now + 10s, viable(now, "sponsor"));
    auto sibling = participant(now + 20s, viable(now, "sibling"));
    auto decision = fetch->select_sponsor(now);
    ASSERT_TRUE(decision.sponsor);
    ASSERT_EQ(decision.sponsor->interest(), sponsor.interest.interest());

    auto detached = fetch->detach(sponsor.interest);
    EXPECT_TRUE(detached.accepted);
    EXPECT_FALSE(detached.terminalized_fetch);
    decision = fetch->select_sponsor(now + 1ms);
    ASSERT_TRUE(decision.sponsor);
    EXPECT_EQ(decision.sponsor->interest(), sibling.interest.interest());
    EXPECT_FALSE(fetch->snapshot().terminal);
}

TEST_F(Fixture, ExactActionAndTargetMustMatchBeforeGrantCanSpend)
{
    auto participant_row = participant(now + 10s, viable(now));
    auto decision = fetch->select_sponsor(now);
    ASSERT_TRUE(decision.sponsor);
    auto grant = fetch->mint_grant(*decision.sponsor, now);
    ASSERT_TRUE(grant);

    auto wrong = target("other.example:51235");
    EXPECT_FALSE(fetch->try_spend(
        grant, HistoryAuthorityAction::create_generation, wrong, now));
    wrong = grant.target();
    ++wrong.generation;
    EXPECT_FALSE(fetch->try_spend(
        grant, HistoryAuthorityAction::create_generation, wrong, now));
    wrong = grant.target();
    ++wrong.use;
    EXPECT_FALSE(fetch->try_spend(
        grant, HistoryAuthorityAction::create_generation, wrong, now));
    wrong = grant.target();
    wrong.pool = "other-pool";
    EXPECT_FALSE(fetch->try_spend(
        grant, HistoryAuthorityAction::create_generation, wrong, now));
    EXPECT_FALSE(fetch->try_spend(
        grant, HistoryAuthorityAction::promote, grant.target(), now));
    EXPECT_EQ(fetch->grant_state(grant), HistoryGrantState::active);

    auto spent =
        fetch->try_spend(grant, grant.action(), grant.target(), now + 1ms);
    ASSERT_TRUE(spent);
    EXPECT_TRUE(spent->request_commit);
    EXPECT_EQ(fetch->grant_state(grant), HistoryGrantState::spent);
    EXPECT_FALSE(
        fetch->try_spend(grant, grant.action(), grant.target(), now + 2ms));
    EXPECT_EQ(
        participant_row.scope->snapshot(now + 2ms).last_accepted_commit,
        spent->request_commit);
}

TEST_F(Fixture, GrantCannotSpendBeforeItsExactPlannedActionTime)
{
    auto const ready_at = now + 2s;
    auto participant_row = participant(now + 10s, viable(ready_at));
    EXPECT_FALSE(fetch->select_sponsor(now).sponsor);
    auto decision = fetch->select_sponsor(ready_at);
    ASSERT_TRUE(decision.sponsor);
    auto grant = fetch->mint_grant(*decision.sponsor, ready_at);
    ASSERT_TRUE(grant);
    EXPECT_EQ(grant.not_before(), ready_at);
    EXPECT_FALSE(fetch->try_spend(
        grant, grant.action(), grant.target(), ready_at - 1ms));
    EXPECT_EQ(fetch->grant_state(grant), HistoryGrantState::active);
    EXPECT_TRUE(
        fetch->try_spend(grant, grant.action(), grant.target(), ready_at));
    EXPECT_TRUE(fetch->acknowledge_work(grant, HistoryWorkReceipt::closed));
    (void)participant_row;
}

TEST_F(Fixture, RevokeFirstKillsGrantWithoutRequestCommit)
{
    auto participant_row = participant(now + 10s, viable(now));
    auto decision = fetch->select_sponsor(now);
    ASSERT_TRUE(decision.sponsor);
    auto grant = fetch->mint_grant(*decision.sponsor, now);
    ASSERT_TRUE(grant);

    auto detached = fetch->detach(participant_row.interest);
    EXPECT_TRUE(detached.accepted);
    EXPECT_TRUE(detached.acknowledged);
    EXPECT_EQ(fetch->grant_state(grant), HistoryGrantState::revoked);
    EXPECT_FALSE(
        fetch->try_spend(grant, grant.action(), grant.target(), now + 1ms));
    EXPECT_FALSE(
        participant_row.scope->snapshot(now + 1ms).last_accepted_commit);
}

TEST_F(Fixture, SpendFirstIsNeverRefundedAndDetachWaitsForExactReceipt)
{
    auto participant_row = participant(now + 10s, viable(now));
    auto decision = fetch->select_sponsor(now);
    ASSERT_TRUE(decision.sponsor);
    auto grant = fetch->mint_grant(*decision.sponsor, now);
    ASSERT_TRUE(grant);
    ASSERT_TRUE(
        fetch->try_spend(grant, grant.action(), grant.target(), now + 1ms));

    auto detached = fetch->detach(participant_row.interest);
    EXPECT_TRUE(detached.accepted);
    EXPECT_FALSE(detached.acknowledged);
    EXPECT_EQ(detached.pending_work, 1u);
    EXPECT_FALSE(detached.terminalized_fetch);
    EXPECT_FALSE(fetch->snapshot().terminal);
    EXPECT_EQ(fetch->grant_state(grant), HistoryGrantState::spent);
    EXPECT_FALSE(
        fetch->acknowledge_work(grant, HistoryWorkReceipt::service_owned));
    EXPECT_TRUE(fetch->acknowledge_work(grant, HistoryWorkReceipt::closed));
    EXPECT_FALSE(fetch->acknowledge_work(grant, HistoryWorkReceipt::closed));
    EXPECT_EQ(fetch->snapshot().spent_unacknowledged, 0u);
    EXPECT_EQ(
        fetch->snapshot().terminal, SharedHistoryFetchTerminal::no_interests);
    EXPECT_EQ(fetch->grant_state(grant), HistoryGrantState::spent);
}

TEST_F(Fixture, ConcurrentSpendAndRevokeHaveOneExplicitWinner)
{
    auto participant_row = participant(now + 10s, viable(now));
    auto decision = fetch->select_sponsor(now);
    ASSERT_TRUE(decision.sponsor);
    auto grant = fetch->mint_grant(*decision.sponsor, now);
    ASSERT_TRUE(grant);

    std::atomic<bool> start{false};
    std::optional<HistoryGrantSpendReceipt> spent;
    HistoryDetachReceipt detached;
    std::thread spender([&] {
        while (!start.load(std::memory_order_acquire))
        {
        }
        spent =
            fetch->try_spend(grant, grant.action(), grant.target(), now + 1ms);
    });
    std::thread revoker([&] {
        while (!start.load(std::memory_order_acquire))
        {
        }
        detached = fetch->detach(participant_row.interest);
    });
    start.store(true, std::memory_order_release);
    spender.join();
    revoker.join();

    ASSERT_TRUE(detached.accepted);
    if (spent)
    {
        EXPECT_EQ(detached.pending_work, 1u);
        EXPECT_EQ(fetch->grant_state(grant), HistoryGrantState::spent);
        EXPECT_TRUE(fetch->acknowledge_work(grant, HistoryWorkReceipt::closed));
    }
    else
    {
        EXPECT_EQ(detached.pending_work, 0u);
        EXPECT_EQ(fetch->grant_state(grant), HistoryGrantState::revoked);
    }
    EXPECT_EQ(
        fetch->snapshot().terminal, SharedHistoryFetchTerminal::no_interests);
    EXPECT_EQ(fetch->snapshot().publication_revision, 1u);
}

TEST_F(Fixture, RevisionRevokesOldGrantAndStaleRefCannotTouchSuccessor)
{
    auto participant_row = participant(now + 10s, viable(now, "old"));
    auto decision = fetch->select_sponsor(now);
    ASSERT_TRUE(decision.sponsor);
    auto old_grant = fetch->mint_grant(*decision.sponsor, now);
    ASSERT_TRUE(old_grant);

    auto successor =
        fetch->revise_interest(participant_row.interest, viable(now, "new"));
    ASSERT_TRUE(successor);
    EXPECT_GT(
        successor->participant_revision(),
        participant_row.interest.participant_revision());
    EXPECT_EQ(successor->deadline(), participant_row.interest.deadline());
    EXPECT_EQ(fetch->grant_state(old_grant), HistoryGrantState::revoked);
    EXPECT_FALSE(
        fetch->revise_interest(participant_row.interest, viable(now, "stale")));
    EXPECT_FALSE(fetch->detach(participant_row.interest).accepted);
    EXPECT_FALSE(fetch->try_spend(
        old_grant, old_grant.action(), old_grant.target(), now + 1ms));

    participant_row.interest = *successor;
    decision = fetch->select_sponsor(now);
    ASSERT_TRUE(decision.sponsor);
    EXPECT_EQ(
        decision.sponsor->participant_revision(),
        successor->participant_revision());
}

TEST_F(Fixture, LastInterestDetachPublishesOneTerminalAndRefusesReopen)
{
    auto only = participant(now + 10s, viable(now));
    auto receipt = fetch->detach(only.interest);
    EXPECT_TRUE(receipt.accepted);
    EXPECT_TRUE(receipt.terminalized_fetch);
    auto snapshot = fetch->snapshot();
    EXPECT_EQ(snapshot.terminal, SharedHistoryFetchTerminal::no_interests);
    EXPECT_EQ(snapshot.publication_revision, 1u);
    EXPECT_FALSE(
        fetch->publish_terminal(SharedHistoryFetchTerminal::completed));
    EXPECT_THROW(
        fetch->register_interest(
            peers, only.scope, only.demand, only.acquisition, viable(now)),
        PeerClientException);
}

TEST_F(Fixture, TerminalPublicationIsStickyAndRevokesEveryUnspentGrant)
{
    auto first = participant(now + 10s, viable(now, "first"));
    auto second = participant(now + 20s, viable(now, "second"));
    auto decision = fetch->select_sponsor(now);
    ASSERT_TRUE(decision.sponsor);
    auto grant = fetch->mint_grant(*decision.sponsor, now);
    ASSERT_TRUE(grant);

    EXPECT_FALSE(
        fetch->publish_terminal(static_cast<SharedHistoryFetchTerminal>(255)));
    EXPECT_FALSE(fetch->snapshot().terminal);
    EXPECT_TRUE(fetch->publish_terminal(SharedHistoryFetchTerminal::completed));
    EXPECT_FALSE(fetch->publish_terminal(SharedHistoryFetchTerminal::shutdown));
    auto snapshot = fetch->snapshot();
    EXPECT_EQ(snapshot.terminal, SharedHistoryFetchTerminal::completed);
    EXPECT_EQ(snapshot.publication_revision, 1u);
    EXPECT_EQ(snapshot.active_interests, 0u);
    EXPECT_EQ(fetch->grant_state(grant), HistoryGrantState::revoked);
    EXPECT_FALSE(fetch->select_sponsor(now).sponsor);
    (void)first;
    (void)second;
}

TEST_F(Fixture, EpochReplacementRevokesOldGrantBeforeSuccessorCanOpen)
{
    auto old = participant(now + 10s, viable(now, "old"));
    auto decision = fetch->select_sponsor(now);
    ASSERT_TRUE(decision.sponsor);
    auto old_grant = fetch->mint_grant(*decision.sponsor, now);
    ASSERT_TRUE(old_grant);
    ASSERT_TRUE(fetch->publish_terminal(SharedHistoryFetchTerminal::replaced));
    EXPECT_EQ(fetch->grant_state(old_grant), HistoryGrantState::revoked);

    auto successor_fetch = SharedHistoryFetch::create(HistoryFetchId{7}, 4);
    ASSERT_TRUE(successor_fetch);
    auto successor_acquisition =
        run(io,
            peers->co_open_history_acquisition(
                old.demand,
                HistoryAcquisitionOptions{
                    .fetch = HistoryFetchId{7},
                    .fetch_epoch = 4,
                    .request_kind = HistoryRequestKind::ledger_header}));
    auto successor = successor_fetch->register_interest(
        peers,
        old.scope,
        old.demand,
        successor_acquisition,
        viable(now, "new"));
    EXPECT_EQ(successor.fetch_epoch(), 4u);
    EXPECT_FALSE(successor_fetch->try_spend(
        old_grant, old_grant.action(), old_grant.target(), now + 1ms));
    decision = successor_fetch->select_sponsor(now + 1ms);
    ASSERT_TRUE(decision.sponsor);
    auto new_grant = successor_fetch->mint_grant(*decision.sponsor, now + 1ms);
    ASSERT_TRUE(new_grant);
    EXPECT_EQ(new_grant.fetch_epoch(), 4u);
}

TEST_F(Fixture, SuccessPublicationAndRequestCancelRemainSeparatelySticky)
{
    auto participant_row = participant(now + 10s, viable(now));
    auto decision = fetch->select_sponsor(now);
    ASSERT_TRUE(decision.sponsor);
    auto grant = fetch->mint_grant(*decision.sponsor, now);
    ASSERT_TRUE(grant);

    std::atomic<bool> start{false};
    bool published = false;
    bool cancelled = false;
    std::thread publisher([&] {
        while (!start.load(std::memory_order_acquire))
        {
        }
        published =
            fetch->publish_terminal(SharedHistoryFetchTerminal::completed);
    });
    std::thread canceller([&] {
        while (!start.load(std::memory_order_acquire))
        {
        }
        cancelled = participant_row.scope->cancel(now + 1ms);
    });
    start.store(true, std::memory_order_release);
    publisher.join();
    canceller.join();

    EXPECT_TRUE(published);
    EXPECT_TRUE(cancelled);
    EXPECT_EQ(
        fetch->snapshot().terminal, SharedHistoryFetchTerminal::completed);
    EXPECT_EQ(fetch->grant_state(grant), HistoryGrantState::revoked);
    EXPECT_FALSE(
        fetch->try_spend(grant, grant.action(), grant.target(), now + 2ms));
}

TEST_F(Fixture, ForeignFetchRefusesBeforeCollidingLocalIdsCanMutate)
{
    auto local = participant(now + 10s, viable(now, "local"));
    auto foreign_fetch = SharedHistoryFetch::create(HistoryFetchId{7}, 3);
    ASSERT_TRUE(foreign_fetch);

    EXPECT_FALSE(foreign_fetch->detach(local.interest).accepted);
    EXPECT_FALSE(
        foreign_fetch->revise_interest(local.interest, viable(now, "x")));
    EXPECT_EQ(foreign_fetch->snapshot().interests, 0u);

    auto decision = fetch->select_sponsor(now);
    ASSERT_TRUE(decision.sponsor);
    auto local_grant = fetch->mint_grant(*decision.sponsor, now);
    ASSERT_TRUE(local_grant);
    EXPECT_FALSE(foreign_fetch->try_spend(
        local_grant, local_grant.action(), local_grant.target(), now));
    EXPECT_EQ(fetch->grant_state(local_grant), HistoryGrantState::active);
}

TEST_F(Fixture, ParticipantOwnerIsAuthenticatedBeforeCollidingDemandIds)
{
    auto other = PeerSet::create(
        io,
        PeerSetOptions{
            .max_active_ledger_demands = 32, .max_acquisitions_per_demand = 8});
    ASSERT_TRUE(other);
    auto scope = catl::core::RequestScope::create(now + 10s);
    auto open = [&](std::shared_ptr<PeerSet> const& owner) {
        auto demand =
            run(io,
                owner->co_get_or_create_ledger_demand(
                    scope,
                    LedgerDemandOptions{
                        .ledger_seq = 50, .remaining_new_dials = 1}));
        auto acquisition =
            run(io,
                owner->co_open_history_acquisition(
                    demand,
                    HistoryAcquisitionOptions{
                        .fetch = HistoryFetchId{7},
                        .fetch_epoch = 3,
                        .request_kind = HistoryRequestKind::ledger_header}));
        return std::pair{demand, acquisition};
    };
    auto [local_demand, local_acquisition] = open(peers);
    auto [other_demand, other_acquisition] = open(other);
    ASSERT_EQ(local_demand.id(), other_demand.id());
    ASSERT_EQ(
        local_acquisition.context().acquisition,
        other_acquisition.context().acquisition);

    EXPECT_THROW(
        fetch->register_interest(
            peers, scope, local_demand, other_acquisition, viable(now)),
        PeerClientException);
    EXPECT_THROW(
        fetch->register_interest(
            other, scope, local_demand, local_acquisition, viable(now)),
        PeerClientException);
    EXPECT_EQ(fetch->snapshot().interests, 0u);

    auto accepted = fetch->register_interest(
        peers, scope, local_demand, local_acquisition, viable(now));
    EXPECT_TRUE(accepted);
    EXPECT_NE(accepted.participant_owner_instance(), 0u);
}

TEST_F(Fixture, NoViableTerminalWaitsForOutstandingWorkAndFutureActions)
{
    auto future = participant(now + 20s, viable(now + 4s, "future"));
    EXPECT_FALSE(fetch->terminalize_if_no_viable_action(now));
    EXPECT_FALSE(fetch->snapshot().terminal);

    auto revised = fetch->revise_interest(
        future.interest,
        HistoryParticipantPlan{
            .local_terminal = HistoryAcquisitionTerminal::exhausted});
    ASSERT_TRUE(revised);
    future.interest = *revised;
    EXPECT_TRUE(fetch->terminalize_if_no_viable_action(now));
    EXPECT_EQ(
        fetch->snapshot().terminal,
        SharedHistoryFetchTerminal::no_viable_participant);
}

TEST_F(Fixture, RequestCancellationMakesEveryUnspentGrantInert)
{
    auto participant_row = participant(now + 10s, viable(now));
    auto decision = fetch->select_sponsor(now);
    ASSERT_TRUE(decision.sponsor);
    auto grant = fetch->mint_grant(*decision.sponsor, now);
    ASSERT_TRUE(grant);

    ASSERT_TRUE(participant_row.scope->cancel(now + 1ms));
    EXPECT_FALSE(fetch->select_sponsor(now + 2ms).sponsor);
    EXPECT_FALSE(fetch->mint_grant(participant_row.interest, now + 2ms));
    EXPECT_FALSE(
        fetch->try_spend(grant, grant.action(), grant.target(), now + 2ms));
    EXPECT_FALSE(
        participant_row.scope->snapshot(now + 2ms).last_accepted_commit);
}

TEST_F(Fixture, RequestCancelAndGrantSpendShareOneCommitWinner)
{
    auto participant_row = participant(now + 10s, viable(now));
    auto decision = fetch->select_sponsor(now);
    ASSERT_TRUE(decision.sponsor);
    auto grant = fetch->mint_grant(*decision.sponsor, now);
    ASSERT_TRUE(grant);

    std::atomic<bool> start{false};
    std::optional<HistoryGrantSpendReceipt> spent;
    bool cancel_won = false;
    std::thread spender([&] {
        while (!start.load(std::memory_order_acquire))
        {
        }
        spent =
            fetch->try_spend(grant, grant.action(), grant.target(), now + 1ms);
    });
    std::thread canceller([&] {
        while (!start.load(std::memory_order_acquire))
        {
        }
        cancel_won = participant_row.scope->cancel(now + 1ms);
    });
    start.store(true, std::memory_order_release);
    spender.join();
    canceller.join();

    EXPECT_TRUE(cancel_won);
    if (spent)
    {
        EXPECT_EQ(
            participant_row.scope->snapshot(now + 2ms).last_accepted_commit,
            spent->request_commit);
        EXPECT_TRUE(fetch->acknowledge_work(grant, HistoryWorkReceipt::closed));
    }
    else
    {
        EXPECT_FALSE(
            participant_row.scope->snapshot(now + 2ms).last_accepted_commit);
        EXPECT_NE(fetch->grant_state(grant), HistoryGrantState::spent);
    }
}

TEST_F(Fixture, DuplicateAcquisitionAndCapacityLimitsFailClosed)
{
    EXPECT_FALSE(SharedHistoryFetch::create(
        HistoryFetchId{7},
        3,
        SharedHistoryFetch::Limits{
            .max_interests = 513, .max_grants_per_interest = 1}));
    EXPECT_FALSE(SharedHistoryFetch::create(
        HistoryFetchId{7},
        3,
        SharedHistoryFetch::Limits{
            .max_interests = 1, .max_grants_per_interest = 33}));

    auto first = participant(now + 10s, viable(now, "first"));
    EXPECT_THROW(
        fetch->register_interest(
            peers,
            first.scope,
            first.demand,
            first.acquisition,
            viable(now, "dup")),
        PeerClientException);
    EXPECT_EQ(fetch->snapshot().interests, 1u);

    auto second = participant(now + 20s, viable(now, "second"));
    auto limited = SharedHistoryFetch::create(
        HistoryFetchId{7},
        3,
        SharedHistoryFetch::Limits{
            .max_interests = 1, .max_grants_per_interest = 1});
    ASSERT_TRUE(limited);
    auto limited_first = limited->register_interest(
        peers,
        first.scope,
        first.demand,
        first.acquisition,
        viable(now, "first"));
    EXPECT_THROW(
        limited->register_interest(
            peers,
            second.scope,
            second.demand,
            second.acquisition,
            viable(now, "second")),
        PeerClientException);
    EXPECT_EQ(limited->snapshot().interests, 1u);

    auto decision = limited->select_sponsor(now);
    ASSERT_TRUE(decision.sponsor);
    auto only_grant = limited->mint_grant(*decision.sponsor, now);
    ASSERT_TRUE(only_grant);
    EXPECT_FALSE(limited->mint_grant(limited_first, now));
    EXPECT_EQ(limited->snapshot().active_grants, 1u);
    EXPECT_EQ(limited->grant_state(only_grant), HistoryGrantState::active);
}

TEST_F(Fixture, AdoptionRequiresAnotherSpentExactAttachGrant)
{
    auto source = participant(now + 10s, viable(now, "shared"));
    auto adopter = participant(
        now + 20s,
        viable(now, "shared", HistoryAuthorityAction::attach_or_adopt));
    auto decision = fetch->select_sponsor(now);
    ASSERT_TRUE(decision.sponsor);
    ASSERT_EQ(decision.sponsor->interest(), source.interest.interest());
    auto source_grant = fetch->mint_grant(*decision.sponsor, now);
    ASSERT_TRUE(source_grant);
    ASSERT_TRUE(fetch->try_spend(
        source_grant, source_grant.action(), source_grant.target(), now + 1ms));

    auto source_successor = fetch->revise_interest(
        source.interest,
        HistoryParticipantPlan{
            .local_terminal = HistoryAcquisitionTerminal::exhausted});
    ASSERT_TRUE(source_successor);
    source.interest = *source_successor;
    decision = fetch->select_sponsor(now + 2ms);
    ASSERT_TRUE(decision.sponsor);
    ASSERT_EQ(decision.sponsor->interest(), adopter.interest.interest());
    auto adopter_grant = fetch->mint_grant(*decision.sponsor, now + 2ms);
    ASSERT_TRUE(adopter_grant);

    EXPECT_FALSE(
        fetch->acknowledge_work(source_grant, HistoryWorkReceipt::adopted));
    EXPECT_FALSE(fetch->acknowledge_work(
        source_grant, HistoryWorkReceipt::adopted, adopter_grant));
    ASSERT_TRUE(fetch->try_spend(
        adopter_grant,
        adopter_grant.action(),
        adopter_grant.target(),
        now + 3ms));
    EXPECT_TRUE(fetch->acknowledge_work(
        source_grant, HistoryWorkReceipt::adopted, adopter_grant));
    EXPECT_EQ(fetch->snapshot().spent_unacknowledged, 1u);

    EXPECT_TRUE(fetch->detach(source.interest).acknowledged);
    auto adopter_detach = fetch->detach(adopter.interest);
    EXPECT_FALSE(adopter_detach.acknowledged);
    EXPECT_EQ(adopter_detach.pending_work, 1u);
    EXPECT_TRUE(
        fetch->acknowledge_work(adopter_grant, HistoryWorkReceipt::closed));
    EXPECT_EQ(fetch->snapshot().spent_unacknowledged, 0u);
}

TEST_F(Fixture, OneAdopterGrantCannotAcknowledgeTwoPhysicalOwners)
{
    auto first = participant(now + 10s, viable(now, "shared"));
    auto second = participant(now + 11s, viable(now, "shared"));
    auto adopter = participant(
        now + 20s,
        viable(now, "shared", HistoryAuthorityAction::attach_or_adopt));

    auto spend_selected = [&]() {
        auto decision = fetch->select_sponsor(now);
        EXPECT_TRUE(decision.sponsor);
        auto grant = fetch->mint_grant(*decision.sponsor, now);
        EXPECT_TRUE(grant);
        EXPECT_TRUE(
            fetch->try_spend(grant, grant.action(), grant.target(), now + 1ms));
        return grant;
    };

    auto first_grant = spend_selected();
    auto first_revised = fetch->revise_interest(first.interest, {});
    ASSERT_TRUE(first_revised);
    first.interest = *first_revised;

    auto second_grant = spend_selected();
    auto second_revised = fetch->revise_interest(second.interest, {});
    ASSERT_TRUE(second_revised);
    second.interest = *second_revised;

    auto adopter_grant = spend_selected();
    ASSERT_EQ(adopter_grant.action(), HistoryAuthorityAction::attach_or_adopt);
    EXPECT_TRUE(fetch->acknowledge_work(
        first_grant, HistoryWorkReceipt::adopted, adopter_grant));
    EXPECT_FALSE(fetch->acknowledge_work(
        second_grant, HistoryWorkReceipt::adopted, adopter_grant));
    EXPECT_TRUE(
        fetch->acknowledge_work(second_grant, HistoryWorkReceipt::closed));
    EXPECT_TRUE(
        fetch->acknowledge_work(adopter_grant, HistoryWorkReceipt::closed));
    EXPECT_EQ(fetch->snapshot().spent_unacknowledged, 0u);
}

TEST_F(Fixture, ExactAdopterAndCloseReceiptHaveOneWinner)
{
    auto source = participant(now + 10s, viable(now, "shared"));
    auto adopter = participant(
        now + 20s,
        viable(now, "shared", HistoryAuthorityAction::attach_or_adopt));
    auto decision = fetch->select_sponsor(now);
    ASSERT_TRUE(decision.sponsor);
    auto source_grant = fetch->mint_grant(*decision.sponsor, now);
    ASSERT_TRUE(source_grant);
    ASSERT_TRUE(fetch->try_spend(
        source_grant, source_grant.action(), source_grant.target(), now + 1ms));
    auto source_revised = fetch->revise_interest(source.interest, {});
    ASSERT_TRUE(source_revised);

    decision = fetch->select_sponsor(now + 2ms);
    ASSERT_TRUE(decision.sponsor);
    ASSERT_EQ(decision.sponsor->interest(), adopter.interest.interest());
    auto adopter_grant = fetch->mint_grant(*decision.sponsor, now + 2ms);
    ASSERT_TRUE(adopter_grant);
    ASSERT_TRUE(fetch->try_spend(
        adopter_grant,
        adopter_grant.action(),
        adopter_grant.target(),
        now + 3ms));

    std::atomic<bool> start{false};
    bool adopted = false;
    bool closed = false;
    std::thread adoption([&] {
        while (!start.load(std::memory_order_acquire))
        {
        }
        adopted = fetch->acknowledge_work(
            source_grant, HistoryWorkReceipt::adopted, adopter_grant);
    });
    std::thread closure([&] {
        while (!start.load(std::memory_order_acquire))
        {
        }
        closed =
            fetch->acknowledge_work(source_grant, HistoryWorkReceipt::closed);
    });
    start.store(true, std::memory_order_release);
    adoption.join();
    closure.join();

    EXPECT_NE(adopted, closed);
    EXPECT_EQ(fetch->snapshot().spent_unacknowledged, 1u);
    EXPECT_TRUE(
        fetch->acknowledge_work(adopter_grant, HistoryWorkReceipt::closed));
    EXPECT_EQ(fetch->snapshot().spent_unacknowledged, 0u);
}
