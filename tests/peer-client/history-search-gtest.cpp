#include <catl/peer-client/history-search.h>
#include <catl/peer-client/peer-client-coro.h>
#include <catl/peer-client/peer-set.h>

#include <catl/core/context-executor.h>
#include <catl/core/request-context.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

#include <gtest/gtest.h>

#include "peer-set-test-access.h"
#include "node-batch-test-session.h"

#include <chrono>
#include <future>
#include <string>
#include <thread>
#include <type_traits>

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

HistoryBudgetGrantRef
budget(std::uint64_t id)
{
    return HistoryBudgetGrantRef{
        .owner_instance = 1, .token = id, .revision = 1};
}

struct PlannerFixture : ::testing::Test
{
    PlannerFixture()
    {
        snapshot.control = HistoryControlRevision{
            .peer_set_instance = 1,
            .pool_revision = 1,
            .demand = LedgerDemandId{1},
            .acquisition = AcquisitionId{2},
            .fetch = HistoryFetchId{3},
            .fetch_epoch = 4,
            .interest = HistoryInterestId{5},
            .participant_revision = 6,
            .request_scope = catl::core::RequestScopeId{7}};
        snapshot.absolute_deadline = now + 30s;
        snapshot.fallback_at = now + 5s;
        snapshot.initial_parent_dial_budget = 6;
        snapshot.remaining_parent_dials = 6;
        snapshot.parent_dial_grant = budget(10);
        snapshot.global_dial_grant = budget(11);
        snapshot.parent_blind_grant = budget(12);
        snapshot.global_blind_grant = budget(13);
        snapshot.physical = HistoryPhysicalCensus{
            .max_admitted = 4, .max_transient = 3, .drain_reserve = 2};
    }

    HistorySearchCandidate
    candidate(
        std::string endpoint,
        HistorySourceLane source,
        HistoryGenerationDisposition disposition,
        std::uint64_t ticket) const
    {
        auto const generation =
            disposition == HistoryGenerationDisposition::absent ? 0 : ticket;
        return HistorySearchCandidate{
            .target =
                HistoryAuthorityTarget{
                    .endpoint = std::move(endpoint),
                    .generation = generation,
                    .use = generation == 0 ? 0 : generation + 100},
            .source = source,
            .disposition = disposition,
            .fair_ticket = ticket,
            .eligible = true};
    }

    HistoryPlannedAction const&
    action(HistoryParticipantPlan const& plan) const
    {
        EXPECT_TRUE(plan.next_action);
        EXPECT_FALSE(plan.wait_until);
        EXPECT_FALSE(plan.local_terminal);
        return *plan.next_action;
    }

    std::chrono::steady_clock::time_point now =
        std::chrono::steady_clock::now();
    HistorySearchSnapshot snapshot;
};

TEST_F(PlannerFixture, KnownCoveredUsePrecedesEveryOtherAction)
{
    auto create = candidate(
        "configured:1",
        HistorySourceLane::configured,
        HistoryGenerationDisposition::absent,
        1);
    auto attach = candidate(
        "queued:1",
        HistorySourceLane::configured,
        HistoryGenerationDisposition::queued_or_connecting,
        2);
    attach.attachable = true;
    auto known = candidate(
        "ready:1",
        HistorySourceLane::direct_verified,
        HistoryGenerationDisposition::admitted,
        3);
    known.advertises_ledger = true;
    snapshot.candidates = {create, attach, known};

    auto const plan = plan_history_interest(snapshot, now);
    EXPECT_EQ(action(plan).action, HistoryAuthorityAction::submit_known_use);
    EXPECT_EQ(action(plan).target.endpoint, "ready:1");
}

TEST_F(PlannerFixture, ProposalNeedsNoAuthorityButFinalBindingDoes)
{
    snapshot.control = {};
    auto known = candidate(
        "ready:1",
        HistorySourceLane::direct_verified,
        HistoryGenerationDisposition::admitted,
        1);
    known.advertises_ledger = true;
    snapshot.candidates = {known};

    EXPECT_EQ(
        action(plan_history_interest(snapshot, now)).action,
        HistoryAuthorityAction::submit_known_use);
    auto final = plan_history_acquisition(snapshot, {}, now);
    auto const* terminal = std::get_if<HistoryTerminalPlan>(&final);
    ASSERT_NE(terminal, nullptr);
    EXPECT_EQ(terminal->reason, HistorySearchTerminalReason::invalid_snapshot);
}

TEST_F(PlannerFixture, AttachmentPrecedesBlindUseAndCreation)
{
    snapshot.fallback_at = now;
    auto attach = candidate(
        "joining:1",
        HistorySourceLane::independently_discovered,
        HistoryGenerationDisposition::queued_or_connecting,
        1);
    attach.attachable = true;
    auto blind = candidate(
        "blind:1",
        HistorySourceLane::direct_verified,
        HistoryGenerationDisposition::admitted,
        2);
    blind.blind_eligible = true;
    snapshot.candidates = {
        blind,
        candidate(
            "new:1",
            HistorySourceLane::configured,
            HistoryGenerationDisposition::absent,
            3),
        attach};

    auto const plan = plan_history_interest(snapshot, now);
    EXPECT_EQ(action(plan).action, HistoryAuthorityAction::attach_or_adopt);
    EXPECT_EQ(action(plan).target.endpoint, "joining:1");
}

TEST_F(PlannerFixture, FreshAttachmentNeedsProbeButSponsoredGenerationDoesNot)
{
    auto attach = candidate(
        "joining:1",
        HistorySourceLane::independently_discovered,
        HistoryGenerationDisposition::queued_or_connecting,
        1);
    attach.attachable = true;
    snapshot.candidates = {attach};
    snapshot.remaining_parent_dials = 0;
    snapshot.parent_dial_grant.reset();

    auto exhausted = plan_history_interest(snapshot, now);
    EXPECT_FALSE(exhausted.next_action);
    EXPECT_EQ(exhausted.local_terminal, HistoryAcquisitionTerminal::exhausted);

    snapshot.candidates.front().demand_sponsored = true;
    auto sponsored = plan_history_interest(snapshot, now);
    EXPECT_EQ(
        action(sponsored).action, HistoryAuthorityAction::attach_or_adopt);
    EXPECT_EQ(action(sponsored).target.endpoint, "joining:1");
}

TEST_F(PlannerFixture, BlindUseRequiresFallbackAndBothExactBudgetGrants)
{
    auto blind = candidate(
        "blind:1",
        HistorySourceLane::direct_verified,
        HistoryGenerationDisposition::admitted,
        1);
    blind.blind_eligible = true;
    snapshot.candidates = {blind};

    EXPECT_TRUE(
        plan_history_interest(snapshot, now).local_terminal.has_value());
    snapshot.fallback_at = now;
    EXPECT_EQ(
        action(plan_history_interest(snapshot, now)).action,
        HistoryAuthorityAction::submit_blind_use);
    snapshot.global_blind_grant.reset();
    EXPECT_TRUE(
        plan_history_interest(snapshot, now).local_terminal.has_value());
}

TEST_F(PlannerFixture, LaneFairnessGivesEveryStrongerLaneAnOpportunity)
{
    snapshot.source_attempts = {1, 0, 0, 0, 1};
    snapshot.crawl_only_probes = 1;
    snapshot.candidates = {
        candidate(
            "crawl:1",
            HistorySourceLane::crawl_only,
            HistoryGenerationDisposition::absent,
            1),
        candidate(
            "configured:1",
            HistorySourceLane::configured,
            HistoryGenerationDisposition::absent,
            2),
        candidate(
            "persistent:1",
            HistorySourceLane::persistent_success,
            HistoryGenerationDisposition::absent,
            3),
        candidate(
            "direct:1",
            HistorySourceLane::direct_verified,
            HistoryGenerationDisposition::absent,
            4),
        candidate(
            "independent:1",
            HistorySourceLane::independently_discovered,
            HistoryGenerationDisposition::absent,
            5)};

    auto const plan = plan_history_interest(snapshot, now);
    EXPECT_EQ(action(plan).target.endpoint, "persistent:1");
}

TEST_F(PlannerFixture, CrawlOnlyQuotaCannotBorrowUnusedParentBudget)
{
    snapshot.crawl_only_probes = 2;  // floor(6 / 3)
    snapshot.source_attempts = {9, 9, 9, 9, 0};
    snapshot.candidates = {
        candidate(
            "crawl:1",
            HistorySourceLane::crawl_only,
            HistoryGenerationDisposition::absent,
            1),
        candidate(
            "independent:1",
            HistorySourceLane::independently_discovered,
            HistoryGenerationDisposition::absent,
            2)};

    auto const plan = plan_history_interest(snapshot, now);
    EXPECT_EQ(action(plan).target.endpoint, "independent:1");

    snapshot.candidates.resize(1);
    EXPECT_TRUE(
        plan_history_interest(snapshot, now).local_terminal.has_value());
}

TEST_F(PlannerFixture, OneTokenDemandChoosesNonCrawlCandidate)
{
    snapshot.initial_parent_dial_budget = 1;
    snapshot.remaining_parent_dials = 1;
    snapshot.source_attempts = {20, 0, 0, 0, 0};
    snapshot.candidates = {
        candidate(
            "crawl:1",
            HistorySourceLane::crawl_only,
            HistoryGenerationDisposition::absent,
            1),
        candidate(
            "configured:1",
            HistorySourceLane::configured,
            HistoryGenerationDisposition::absent,
            2)};

    EXPECT_EQ(
        action(plan_history_interest(snapshot, now)).target.endpoint,
        "configured:1");
}

TEST_F(PlannerFixture, SaturatedCrawlBatchCannotHideConfiguredCandidate)
{
    snapshot.initial_parent_dial_budget = 1;
    snapshot.remaining_parent_dials = 1;
    snapshot.candidates.reserve(kMaxHistorySearchCandidates);
    for (std::size_t i = 1; i < kMaxHistorySearchCandidates; ++i)
    {
        snapshot.candidates.push_back(candidate(
            "crawl:" + std::to_string(i),
            HistorySourceLane::crawl_only,
            HistoryGenerationDisposition::absent,
            i));
    }
    snapshot.candidates.push_back(candidate(
        "configured:1",
        HistorySourceLane::configured,
        HistoryGenerationDisposition::absent,
        kMaxHistorySearchCandidates));

    EXPECT_EQ(
        action(plan_history_interest(snapshot, now)).target.endpoint,
        "configured:1");
}

TEST_F(PlannerFixture, CandidateOverflowFailsClosed)
{
    snapshot.candidates.assign(
        kMaxHistorySearchCandidates + 1,
        candidate(
            "crawl:1",
            HistorySourceLane::crawl_only,
            HistoryGenerationDisposition::absent,
            1));
    auto const plan = plan_history_interest(snapshot, now);
    ASSERT_TRUE(plan.local_terminal);
    EXPECT_EQ(*plan.local_terminal, HistoryAcquisitionTerminal::exhausted);
}

TEST_F(PlannerFixture, PhysicalCapRequiresExactBoundedWake)
{
    snapshot.physical = HistoryPhysicalCensus{
        .queued_or_connecting = 1,
        .ready_transient = 1,
        .admitted = 2,
        .draining = 1,
        .max_admitted = 2,
        .max_transient = 2,
        .drain_reserve = 1};
    snapshot.candidates = {candidate(
        "new:1",
        HistorySourceLane::configured,
        HistoryGenerationDisposition::absent,
        1)};
    EXPECT_TRUE(
        plan_history_interest(snapshot, now).local_terminal.has_value());

    snapshot.wakes.push_back(HistoryPlannedWake{
        .kind = HistoryWakeSourceKind::physical_slot_release,
        .at = now + 2s,
        .endpoint = "closing:1",
        .generation = 9});
    auto const plan = plan_history_interest(snapshot, now);
    ASSERT_TRUE(plan.wait_until);
    EXPECT_EQ(
        plan.wait_until->kind, HistoryWakeSourceKind::physical_slot_release);
    EXPECT_EQ(plan.wait_until->generation, 9u);
}

TEST_F(PlannerFixture, UnnamedOrLateCapacityWakeIsNotProgress)
{
    snapshot.physical = HistoryPhysicalCensus{
        .queued_or_connecting = 3,
        .max_admitted = 4,
        .max_transient = 3,
        .drain_reserve = 1};
    snapshot.candidates = {candidate(
        "new:1",
        HistorySourceLane::configured,
        HistoryGenerationDisposition::absent,
        1)};
    snapshot.wakes = {
        HistoryPlannedWake{
            .kind = HistoryWakeSourceKind::physical_slot_release,
            .at = now + 1s},
        HistoryPlannedWake{
            .kind = HistoryWakeSourceKind::physical_watchdog,
            .at = snapshot.absolute_deadline + 1s,
            .endpoint = "late:1",
            .generation = 7}};

    EXPECT_TRUE(
        plan_history_interest(snapshot, now).local_terminal.has_value());
}

TEST_F(PlannerFixture, EarliestExactWakeWinsDeterministically)
{
    snapshot.remaining_parent_dials = 1;
    snapshot.global_dial_grant.reset();
    snapshot.candidates = {candidate(
        "retry:1",
        HistorySourceLane::configured,
        HistoryGenerationDisposition::absent,
        1)};
    snapshot.candidates.front().retry_not_before = now + 2s;
    snapshot.wakes = {
        HistoryPlannedWake{
            .kind = HistoryWakeSourceKind::global_token_refill,
            .at = now + 3s,
            .token = 40},
        HistoryPlannedWake{
            .kind = HistoryWakeSourceKind::candidate_retry_expiry,
            .at = now + 2s,
            .endpoint = "retry:1"}};

    auto const plan = plan_history_interest(snapshot, now);
    ASSERT_TRUE(plan.wait_until);
    EXPECT_EQ(plan.wait_until->at, now + 2s);
    EXPECT_EQ(plan.wait_until->endpoint, "retry:1");
}

TEST_F(PlannerFixture, RetryPastDeadlineDoesNotPoisonUsablePeer)
{
    auto deferred = candidate(
        "retry:1",
        HistorySourceLane::configured,
        HistoryGenerationDisposition::absent,
        1);
    deferred.retry_not_before = snapshot.absolute_deadline + 1s;
    auto known = candidate(
        "ready:1",
        HistorySourceLane::direct_verified,
        HistoryGenerationDisposition::admitted,
        2);
    known.advertises_ledger = true;
    snapshot.candidates = {deferred, known};

    auto const plan = plan_history_interest(snapshot, now);
    EXPECT_EQ(action(plan).action, HistoryAuthorityAction::submit_known_use);
    EXPECT_EQ(action(plan).target.endpoint, "ready:1");
}

TEST_F(PlannerFixture, ExactWakeCannotOutliveExhaustedParentAuthority)
{
    snapshot.remaining_parent_dials = 0;
    snapshot.parent_dial_grant.reset();
    snapshot.global_dial_grant.reset();
    snapshot.candidates = {candidate(
        "new:1",
        HistorySourceLane::configured,
        HistoryGenerationDisposition::absent,
        1)};
    snapshot.wakes = {
        HistoryPlannedWake{
            .kind = HistoryWakeSourceKind::global_token_refill,
            .at = now + 1s,
            .token = 41},
        HistoryPlannedWake{
            .kind = HistoryWakeSourceKind::physical_slot_release,
            .at = now + 1s,
            .endpoint = "closing:1",
            .generation = 8}};

    EXPECT_TRUE(
        plan_history_interest(snapshot, now).local_terminal.has_value());
}

TEST_F(PlannerFixture, DrainingMayExceedReserveWithoutBreakingCensus)
{
    HistoryPhysicalCensus census{
        .queued_or_connecting = 1,
        .admitted = 1,
        .draining = 3,
        .max_admitted = 2,
        .max_transient = 2,
        .drain_reserve = 1};
    EXPECT_TRUE(census.valid());
    EXPECT_FALSE(census.can_create());
    EXPECT_EQ(census.total(), census.max_physical());
}

TEST_F(PlannerFixture, DeadlineHasNoCompletionGrace)
{
    snapshot.candidates = {candidate(
        "configured:1",
        HistorySourceLane::configured,
        HistoryGenerationDisposition::absent,
        1)};
    auto const plan =
        plan_history_interest(snapshot, snapshot.absolute_deadline);
    ASSERT_TRUE(plan.local_terminal);
    EXPECT_EQ(
        *plan.local_terminal, HistoryAcquisitionTerminal::deadline_elapsed);
}

struct GrantFixture : PlannerFixture
{
    GrantFixture()
        : peers(PeerSet::create(io))
        , scope(catl::core::RequestScope::create(snapshot.absolute_deadline))
        , fetch(SharedHistoryFetch::create(HistoryFetchId{3}, 4))
    {
        auto known = candidate(
            "ready:1",
            HistorySourceLane::direct_verified,
            HistoryGenerationDisposition::admitted,
            9);
        known.advertises_ledger = true;
        snapshot.candidates = {known};
        demand =
            run(io,
                peers->co_get_or_create_ledger_demand(
                    scope,
                    LedgerDemandOptions{
                        .ledger_seq = 50,
                        .fallback_at = snapshot.fallback_at,
                        .remaining_new_dials = 3,
                        .remaining_blind_uses = 1}));
        acquisition =
            run(io,
                peers->co_open_history_acquisition(
                    demand,
                    HistoryAcquisitionOptions{
                        .fetch = HistoryFetchId{3},
                        .fetch_epoch = 4,
                        .request_kind = HistoryRequestKind::ledger_header}));
        interest = fetch->register_interest(
            peers,
            scope,
            demand,
            acquisition,
            plan_history_interest(snapshot, now));
        snapshot.control.peer_set_instance =
            interest.participant_owner_instance();
        snapshot.control.demand = demand.id();
        snapshot.control.acquisition = acquisition.context().acquisition;
        snapshot.control.fetch = interest.fetch();
        snapshot.control.fetch_epoch = interest.fetch_epoch();
        snapshot.control.interest = interest.interest();
        snapshot.control.participant_revision = interest.participant_revision();
        snapshot.control.request_scope = interest.request_scope();
        grant = fetch->mint_grant(interest, now);
    }

    boost::asio::io_context io;
    std::shared_ptr<PeerSet> peers;
    std::shared_ptr<catl::core::RequestScope> scope;
    std::shared_ptr<SharedHistoryFetch> fetch;
    LedgerDemandRef demand;
    HistoryAcquisitionRef acquisition;
    HistoryInterestRef interest;
    HistoryAuthorityGrantRef grant;
};

TEST_F(GrantFixture, ExactGrantBindsToInertKnownUsePlan)
{
    ASSERT_TRUE(grant);
    auto plan = plan_history_acquisition(snapshot, grant, now);
    auto const* known = std::get_if<HistorySubmitKnownUsePlan>(&plan);
    ASSERT_NE(known, nullptr);
    EXPECT_EQ(known->participant_grant.grant(), grant.grant());
    EXPECT_EQ(known->target.endpoint, "ready:1");
    EXPECT_EQ(known->control, snapshot.control);
    static_assert(!std::is_pointer_v<decltype(known->target)>);
}

TEST_F(GrantFixture, ChangedTargetRefusesWithoutRetargetingGrant)
{
    snapshot.candidates.front().target.endpoint = "replacement:1";
    auto plan = plan_history_acquisition(snapshot, grant, now);
    auto const* terminal = std::get_if<HistoryTerminalPlan>(&plan);
    ASSERT_NE(terminal, nullptr);
    EXPECT_EQ(terminal->reason, HistorySearchTerminalReason::control_changed);
}

TEST(SharedHistoryFetchWait, ExactWaitPreventsFalseNoViableTerminal)
{
    boost::asio::io_context io;
    auto peers = PeerSet::create(io);
    auto const now = std::chrono::steady_clock::now();
    auto scope = catl::core::RequestScope::create(now + 20s);
    auto demand = run(
        io,
        peers->co_get_or_create_ledger_demand(
            scope,
            LedgerDemandOptions{.ledger_seq = 50, .remaining_new_dials = 1}));
    auto acquisition =
        run(io,
            peers->co_open_history_acquisition(
                demand,
                HistoryAcquisitionOptions{
                    .fetch = HistoryFetchId{8}, .fetch_epoch = 2}));
    auto fetch = SharedHistoryFetch::create(HistoryFetchId{8}, 2);
    auto interest = fetch->register_interest(
        peers,
        scope,
        demand,
        acquisition,
        HistoryParticipantPlan{
            .wait_until = HistoryPlannedWake{
                .kind = HistoryWakeSourceKind::global_token_refill,
                .at = now + 5s,
                .token = 11}});
    ASSERT_TRUE(interest);
    auto const sponsor = fetch->select_sponsor(now);
    EXPECT_FALSE(sponsor.sponsor);
    ASSERT_TRUE(sponsor.aggregate_wake);
    EXPECT_EQ(*sponsor.aggregate_wake, now + 5s);
    EXPECT_FALSE(fetch->terminalize_if_no_viable_action(now));
    EXPECT_FALSE(fetch->snapshot().terminal);
}

TEST(PeerSetHistorySearch, SnapshotKeepsEvidenceLanesAndHintsDistinct)
{
    boost::asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0});
    auto const now = std::chrono::steady_clock::now();
    auto scope = catl::core::RequestScope::create(now + 30s);
    auto demand =
        run(io,
            peers->co_get_or_create_ledger_demand(
                scope,
                LedgerDemandOptions{
                    .ledger_seq = 50,
                    .fallback_at = now + 10s,
                    .remaining_new_dials = 3,
                    .remaining_blind_uses = 1}));
    auto acquisition =
        run(io,
            peers->co_open_history_acquisition(
                demand,
                HistoryAcquisitionOptions{
                    .fetch = HistoryFetchId{20}, .fetch_epoch = 2}));

    PeerSetTestAccess::install_history_source(
        io,
        peers,
        "persistent.example.com:51235",
        true,
        false,
        false,
        false,
        PeerStatus{.first_seq = 1, .last_seq = 2});
    PeerSetTestAccess::install_history_source(
        io, peers, "independent.example.com:51235", false, true, false, false);
    PeerSetTestAccess::install_crawl_history_candidates(io, peers, 1, 40, 60);

    auto snapshot = run(io, peers->co_history_search_snapshot(acquisition));
    EXPECT_FALSE(static_cast<bool>(snapshot.control));
    ASSERT_TRUE(snapshot.parent_dial_grant);
    ASSERT_TRUE(snapshot.global_dial_grant);
    EXPECT_EQ(snapshot.remaining_parent_dials, 3u);
    EXPECT_TRUE(snapshot.physical.valid());

    auto find = [&snapshot](std::string const& endpoint) {
        return std::find_if(
            snapshot.candidates.begin(),
            snapshot.candidates.end(),
            [&endpoint](auto const& candidate) {
                return candidate.target.endpoint == endpoint;
            });
    };
    auto configured = find("r.ripple.com:51235");
    auto persistent = find("persistent.example.com:51235");
    auto independent = find("independent.example.com:51235");
    auto crawl = find("crawl-0.example.com:51235");
    ASSERT_NE(configured, snapshot.candidates.end());
    ASSERT_NE(persistent, snapshot.candidates.end());
    ASSERT_NE(independent, snapshot.candidates.end());
    ASSERT_NE(crawl, snapshot.candidates.end());
    EXPECT_EQ(configured->source, HistorySourceLane::configured);
    EXPECT_EQ(persistent->source, HistorySourceLane::persistent_success);
    EXPECT_TRUE(persistent->eligible)
        << "persistent noncoverage is not negative exclusion authority";
    EXPECT_EQ(independent->source, HistorySourceLane::independently_discovered);
    EXPECT_EQ(crawl->source, HistorySourceLane::crawl_only);
    EXPECT_FALSE(crawl->advertises_ledger)
        << "crawl coverage is a creation hint, never direct-use evidence";

    auto proposal = plan_history_interest(snapshot, now);
    ASSERT_TRUE(proposal.next_action);
    auto selected = find(proposal.next_action->target.endpoint);
    ASSERT_NE(selected, snapshot.candidates.end());
    EXPECT_EQ(selected->source, HistorySourceLane::configured);
}

TEST(PeerSetHistorySearch, LateRetryGateDoesNotHideCoveredAdmittedPeer)
{
    boost::asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .retry_backoff = 30s});
    auto session =
        std::make_shared<NodeBatchTestSession>("ready.example.com:51235");
    session->attach_strand(io);
    PeerSetTestAccess::insert_ready_peer_with_generation(io, peers, session);

    auto const now = std::chrono::steady_clock::now();
    auto scope = catl::core::RequestScope::create(now + 5s);
    auto demand =
        run(io,
            peers->co_get_or_create_ledger_demand(
                scope, LedgerDemandOptions{.ledger_seq = 50}));
    auto acquisition =
        run(io,
            peers->co_open_history_acquisition(
                demand,
                HistoryAcquisitionOptions{
                    .fetch = HistoryFetchId{22}, .fetch_epoch = 1}));

    PeerSetTestAccess::set_failed_at(io, peers, "r.ripple.com:51235", now);
    auto snapshot = run(io, peers->co_history_search_snapshot(acquisition));
    auto deferred = std::find_if(
        snapshot.candidates.begin(),
        snapshot.candidates.end(),
        [](auto const& candidate) {
            return candidate.target.endpoint == "r.ripple.com:51235";
        });
    ASSERT_NE(deferred, snapshot.candidates.end());
    ASSERT_TRUE(deferred->retry_not_before);
    EXPECT_GT(*deferred->retry_not_before, snapshot.absolute_deadline);

    auto const plan = plan_history_interest(snapshot, now);
    ASSERT_TRUE(plan.next_action);
    EXPECT_EQ(
        plan.next_action->action, HistoryAuthorityAction::submit_known_use);
    EXPECT_EQ(plan.next_action->target.endpoint, "ready.example.com:51235");
}

TEST(PeerSetHistorySearch, SaturatedCrawlProjectionRetainsEveryStrongerLane)
{
    boost::asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0});
    auto const now = std::chrono::steady_clock::now();
    auto scope = catl::core::RequestScope::create(now + 30s);
    auto demand =
        run(io,
            peers->co_get_or_create_ledger_demand(
                scope,
                LedgerDemandOptions{
                    .ledger_seq = 50,
                    .fallback_at = now + 10s,
                    .remaining_new_dials = 1}));
    auto acquisition =
        run(io,
            peers->co_open_history_acquisition(
                demand,
                HistoryAcquisitionOptions{
                    .fetch = HistoryFetchId{21}, .fetch_epoch = 1}));
    PeerSetTestAccess::install_history_source(
        io, peers, "persistent.example.com:51235", true, false, false, false);
    PeerSetTestAccess::install_history_source(
        io, peers, "independent.example.com:51235", false, true, false, false);
    PeerSetTestAccess::install_crawl_history_candidates(
        io, peers, kMaxHistorySearchCandidates, 1, 100);

    auto snapshot = run(io, peers->co_history_search_snapshot(acquisition));
    EXPECT_LE(snapshot.candidates.size(), kMaxHistorySearchCandidates);
    for (auto lane :
         {HistorySourceLane::configured,
          HistorySourceLane::persistent_success,
          HistorySourceLane::independently_discovered,
          HistorySourceLane::crawl_only})
    {
        EXPECT_TRUE(std::any_of(
            snapshot.candidates.begin(),
            snapshot.candidates.end(),
            [lane](auto const& candidate) {
                return candidate.source == lane;
            }));
    }
    auto proposal = plan_history_interest(snapshot, now);
    ASSERT_TRUE(proposal.next_action);
    EXPECT_NE(
        std::find_if(
            snapshot.candidates.begin(),
            snapshot.candidates.end(),
            [&proposal](auto const& candidate) {
                return candidate.target == proposal.next_action->target &&
                    candidate.source != HistorySourceLane::crawl_only;
            }),
        snapshot.candidates.end());
}

TEST(PeerSetHistoryExecution, AdmittedUseOutlivesConnectDeadlineAndCommits)
{
    boost::asio::io_context io;
    auto peers = PeerSet::create(io);
    auto session =
        std::make_shared<NodeBatchTestSession>("ready.example.com:51235");
    session->attach_strand(io);
    auto const generation =
        PeerSetTestAccess::insert_ready_peer_with_generation(io, peers, session);
    auto const now = std::chrono::steady_clock::now();
    PeerSetTestAccess::set_generation_physical_deadline(
        io, peers, session->endpoint(), now - 1s);
    auto scope = catl::core::RequestScope::create(now + 30s);
    auto demand = run(
        io,
        peers->co_get_or_create_ledger_demand(
            scope, LedgerDemandOptions{.ledger_seq = 50}));
    auto acquisition = run(
        io,
        peers->co_open_history_acquisition(
            demand,
            HistoryAcquisitionOptions{
                .fetch = HistoryFetchId{30}, .fetch_epoch = 1}));
    auto fetch = SharedHistoryFetch::create(HistoryFetchId{30}, 1);
    auto proposal = plan_history_interest(
        run(io, peers->co_history_search_snapshot(acquisition)), now);
    auto interest = fetch->register_interest(
        peers, scope, demand, acquisition, std::move(proposal));
    ASSERT_TRUE(interest);
    auto sponsor = fetch->select_sponsor(now);
    ASSERT_TRUE(sponsor.sponsor);
    auto grant = fetch->mint_grant(*sponsor.sponsor, now);
    ASSERT_TRUE(grant);

    auto execution = run(
        io,
        peers->co_execute_history_grant(
            fetch, grant, PeerUseKind::ledger_header));
    ASSERT_TRUE(execution.committed());
    ASSERT_TRUE(execution.peer_use);
    EXPECT_EQ(execution.peer_use.generation().generation(), generation);
    EXPECT_EQ(PeerSetTestAccess::active_peer_use_count(io, peers), 1u);
    EXPECT_EQ(fetch->grant_state(grant), HistoryGrantState::spent);

    execution.peer_use.finish(PeerUseOutcome::success);
    while (PeerSetTestAccess::active_peer_use_count(io, peers) != 0)
    {
        io.poll();
        io.restart();
    }
    EXPECT_EQ(fetch->snapshot().spent_unacknowledged, 0u)
        << "the exact use terminal receipt closes its sponsoring grant";
}

TEST(PeerSetHistoryExecution, RequestCancellationLeavesGrantAndUseUnspent)
{
    boost::asio::io_context io;
    auto peers = PeerSet::create(io);
    auto session =
        std::make_shared<NodeBatchTestSession>("ready.example.com:51235");
    session->attach_strand(io);
    PeerSetTestAccess::insert_ready_peer_with_generation(io, peers, session);
    auto const now = std::chrono::steady_clock::now();
    auto scope = catl::core::RequestScope::create(now + 30s);
    auto demand = run(
        io,
        peers->co_get_or_create_ledger_demand(
            scope, LedgerDemandOptions{.ledger_seq = 50}));
    auto acquisition = run(
        io,
        peers->co_open_history_acquisition(
            demand,
            HistoryAcquisitionOptions{
                .fetch = HistoryFetchId{31}, .fetch_epoch = 1}));
    auto fetch = SharedHistoryFetch::create(HistoryFetchId{31}, 1);
    auto interest = fetch->register_interest(
        peers,
        scope,
        demand,
        acquisition,
        plan_history_interest(
            run(io, peers->co_history_search_snapshot(acquisition)), now));
    ASSERT_TRUE(interest);
    auto sponsor = fetch->select_sponsor(now);
    ASSERT_TRUE(sponsor.sponsor);
    auto grant = fetch->mint_grant(*sponsor.sponsor, now);
    ASSERT_TRUE(grant);
    ASSERT_TRUE(scope->cancel(now + 1ms));

    auto execution = run(
        io,
        peers->co_execute_history_grant(
            fetch, grant, PeerUseKind::ledger_header));
    EXPECT_FALSE(execution.committed());
    EXPECT_FALSE(execution.peer_use);
    EXPECT_EQ(PeerSetTestAccess::active_peer_use_count(io, peers), 0u);
    EXPECT_NE(fetch->grant_state(grant), HistoryGrantState::spent);
}

TEST(PeerSetHistoryExecution, BlindUseDebitsExactParentTokenAtomically)
{
    boost::asio::io_context io;
    auto peers = PeerSet::create(io);
    auto session =
        std::make_shared<NodeBatchTestSession>("ready.example.com:51235");
    session->attach_strand(io);
    PeerSetTestAccess::insert_ready_peer_with_generation(io, peers, session);
    auto const now = std::chrono::steady_clock::now();
    auto scope = catl::core::RequestScope::create(now + 30s);
    auto demand = run(
        io,
        peers->co_get_or_create_ledger_demand(
            scope,
            LedgerDemandOptions{
                .ledger_seq = 150,
                .fallback_at = now - 1ms,
                .remaining_blind_uses = 1}));
    auto acquisition = run(
        io,
        peers->co_open_history_acquisition(
            demand,
            HistoryAcquisitionOptions{
                .fetch = HistoryFetchId{32}, .fetch_epoch = 1}));
    auto fetch = SharedHistoryFetch::create(HistoryFetchId{32}, 1);
    auto interest = fetch->register_interest(
        peers,
        scope,
        demand,
        acquisition,
        plan_history_interest(
            run(io, peers->co_history_search_snapshot(acquisition)), now));
    ASSERT_TRUE(interest);
    auto sponsor = fetch->select_sponsor(now);
    ASSERT_TRUE(sponsor.sponsor);
    auto grant = fetch->mint_grant(*sponsor.sponsor, now);
    ASSERT_TRUE(grant);
    EXPECT_EQ(grant.action(), HistoryAuthorityAction::submit_blind_use);

    auto execution = run(
        io,
        peers->co_execute_history_grant(
            fetch, grant, PeerUseKind::ledger_header));
    ASSERT_TRUE(execution.committed());
    ASSERT_TRUE(execution.peer_use);
    auto parent = run(io, peers->co_ledger_demand_snapshot(demand));
    ASSERT_TRUE(parent);
    EXPECT_EQ(parent->remaining_blind_uses, 0u);
    EXPECT_EQ(fetch->grant_state(grant), HistoryGrantState::spent);

    execution.peer_use.finish(PeerUseOutcome::no_ledger);
    while (PeerSetTestAccess::active_peer_use_count(io, peers) != 0)
    {
        io.poll();
        io.restart();
    }
    EXPECT_EQ(fetch->snapshot().spent_unacknowledged, 0u)
        << "the exact use terminal receipt closes its sponsoring grant";
}

TEST(PeerSetHistoryExecution, CreationOwnsExactQueuedGenerationAndBothDialDebits)
{
    boost::asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_transient_peer_generations = 1,
            .max_global_history_dials = 1});
    auto const now = std::chrono::steady_clock::now();
    auto scope = catl::core::RequestScope::create(now + 30s);
    auto demand = run(
        io,
        peers->co_get_or_create_ledger_demand(
            scope,
            LedgerDemandOptions{
                .ledger_seq = 50,
                .fallback_at = now + 10s,
                .remaining_new_dials = 1}));
    auto acquisition = run(
        io,
        peers->co_open_history_acquisition(
            demand,
            HistoryAcquisitionOptions{
                .fetch = HistoryFetchId{34}, .fetch_epoch = 1}));
    auto fetch = SharedHistoryFetch::create(HistoryFetchId{34}, 1);
    auto proposal = plan_history_interest(
        run(io, peers->co_history_search_snapshot(acquisition)), now);
    ASSERT_TRUE(proposal.next_action);
    ASSERT_EQ(
        proposal.next_action->action,
        HistoryAuthorityAction::create_generation);
    auto const endpoint = proposal.next_action->target.endpoint;
    auto interest = fetch->register_interest(
        peers, scope, demand, acquisition, proposal);
    ASSERT_TRUE(interest);
    auto sponsor = fetch->select_sponsor(now);
    ASSERT_TRUE(sponsor.sponsor);
    auto grant = fetch->mint_grant(*sponsor.sponsor, now);
    ASSERT_TRUE(grant);

    auto execution = run(
        io,
        peers->co_execute_history_grant(
            fetch, grant, PeerUseKind::ledger_header));
    ASSERT_TRUE(execution.committed());
    ASSERT_TRUE(execution.created_generation);
    EXPECT_FALSE(execution.peer_use);
    EXPECT_EQ(execution.created_generation->endpoint, endpoint);
    EXPECT_NE(execution.created_generation->generation, 0u);
    EXPECT_TRUE(PeerSetTestAccess::history_generation_is_queued(
        io,
        peers,
        endpoint,
        execution.created_generation->generation));
    EXPECT_EQ(
        PeerSetTestAccess::history_generation_origin_count(io, peers, endpoint),
        1u);
    EXPECT_EQ(
        PeerSetTestAccess::history_acquisition_origin_count(
            io, peers, acquisition.context().acquisition),
        1u);
    auto census = PeerSetTestAccess::history_physical_census(io, peers);
    EXPECT_EQ(census.queued_or_connecting, 1u);
    EXPECT_EQ(census.total(), 1u);
    auto parent = run(io, peers->co_ledger_demand_snapshot(demand));
    ASSERT_TRUE(parent);
    EXPECT_EQ(parent->remaining_new_dials, 0u);
    EXPECT_EQ(
        PeerSetTestAccess::history_available_global_dials(io, peers), 0u);
    EXPECT_EQ(fetch->snapshot().spent_unacknowledged, 1u);

    auto owned = run(
        io, peers->co_history_search_snapshot(acquisition, interest));
    auto wait = plan_history_interest(owned, now);
    ASSERT_TRUE(wait.wait_until);
    EXPECT_EQ(
        wait.wait_until->kind,
        HistoryWakeSourceKind::attached_generation_deadline);
    EXPECT_EQ(
        wait.wait_until->generation,
        execution.created_generation->generation);

    run(
        io,
        peers->co_finish_history_acquisition(
            acquisition, HistoryAcquisitionTerminal::cancelled));
    EXPECT_EQ(PeerSetTestAccess::generation_for_endpoint(io, peers, endpoint), 0u);
    EXPECT_EQ(
        PeerSetTestAccess::history_acquisition_origin_count(
            io, peers, acquisition.context().acquisition),
        0u);
    census = PeerSetTestAccess::history_physical_census(io, peers);
    EXPECT_EQ(census.total(), 0u);
    EXPECT_EQ(fetch->snapshot().spent_unacknowledged, 0u);
    parent = run(io, peers->co_ledger_demand_snapshot(demand));
    ASSERT_TRUE(parent);
    EXPECT_EQ(parent->remaining_new_dials, 0u)
        << "closing an unstarted origin does not refund spent rate authority";
}

TEST(PeerSetHistoryExecution, CreationReplacesQuiescentTerminalGeneration)
{
    boost::asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_transient_peer_generations = 1,
            .max_global_history_dials = 1});
    auto const now = std::chrono::steady_clock::now();
    auto scope = catl::core::RequestScope::create(now + 30s);
    auto demand = run(
        io,
        peers->co_get_or_create_ledger_demand(
            scope,
            LedgerDemandOptions{
                .ledger_seq = 50,
                .fallback_at = now + 10s,
                .remaining_new_dials = 1}));
    auto acquisition = run(
        io,
        peers->co_open_history_acquisition(
            demand,
            HistoryAcquisitionOptions{
                .fetch = HistoryFetchId{340}, .fetch_epoch = 1}));

    auto first = plan_history_interest(
        run(io, peers->co_history_search_snapshot(acquisition)), now);
    ASSERT_TRUE(first.next_action);
    ASSERT_EQ(
        first.next_action->action,
        HistoryAuthorityAction::create_generation);
    auto const endpoint = first.next_action->target.endpoint;
    auto const terminal_generation =
        PeerSetTestAccess::install_terminal_generation_tombstone(
            io, peers, endpoint);
    ASSERT_NE(terminal_generation, 0u);

    auto proposal = plan_history_interest(
        run(io, peers->co_history_search_snapshot(acquisition)), now);
    ASSERT_TRUE(proposal.next_action);
    ASSERT_EQ(
        proposal.next_action->action,
        HistoryAuthorityAction::create_generation);
    ASSERT_EQ(proposal.next_action->target.endpoint, endpoint);
    ASSERT_GT(
        proposal.next_action->target.generation, terminal_generation);

    auto fetch = SharedHistoryFetch::create(HistoryFetchId{340}, 1);
    auto interest = fetch->register_interest(
        peers, scope, demand, acquisition, std::move(proposal));
    ASSERT_TRUE(interest);
    auto sponsor = fetch->select_sponsor(now);
    ASSERT_TRUE(sponsor.sponsor);
    auto grant = fetch->mint_grant(*sponsor.sponsor, now);
    ASSERT_TRUE(grant);

    auto execution = run(
        io,
        peers->co_execute_history_grant(
            fetch, grant, PeerUseKind::ledger_header));
    ASSERT_TRUE(execution.committed());
    ASSERT_TRUE(execution.created_generation);
    EXPECT_EQ(execution.created_generation->endpoint, endpoint);
    EXPECT_GT(
        execution.created_generation->generation, terminal_generation);
    EXPECT_EQ(
        PeerSetTestAccess::generation_for_endpoint(io, peers, endpoint),
        execution.created_generation->generation);
    EXPECT_TRUE(PeerSetTestAccess::history_generation_is_queued(
        io,
        peers,
        endpoint,
        execution.created_generation->generation));

    run(
        io,
        peers->co_finish_history_acquisition(
            acquisition, HistoryAcquisitionTerminal::cancelled));
}

TEST(PeerSetHistoryExecution, CancelledCreationSpendsNoGrantTokenOrSlot)
{
    boost::asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_transient_peer_generations = 1,
            .max_global_history_dials = 1});
    auto const now = std::chrono::steady_clock::now();
    auto scope = catl::core::RequestScope::create(now + 30s);
    auto demand = run(
        io,
        peers->co_get_or_create_ledger_demand(
            scope,
            LedgerDemandOptions{
                .ledger_seq = 50,
                .fallback_at = now + 10s,
                .remaining_new_dials = 1}));
    auto acquisition = run(
        io,
        peers->co_open_history_acquisition(
            demand,
            HistoryAcquisitionOptions{
                .fetch = HistoryFetchId{35}, .fetch_epoch = 1}));
    auto fetch = SharedHistoryFetch::create(HistoryFetchId{35}, 1);
    auto proposal = plan_history_interest(
        run(io, peers->co_history_search_snapshot(acquisition)), now);
    ASSERT_TRUE(proposal.next_action);
    ASSERT_EQ(
        proposal.next_action->action,
        HistoryAuthorityAction::create_generation);
    auto const endpoint = proposal.next_action->target.endpoint;
    auto interest = fetch->register_interest(
        peers, scope, demand, acquisition, std::move(proposal));
    ASSERT_TRUE(interest);
    auto sponsor = fetch->select_sponsor(now);
    ASSERT_TRUE(sponsor.sponsor);
    auto grant = fetch->mint_grant(*sponsor.sponsor, now);
    ASSERT_TRUE(grant);
    ASSERT_TRUE(scope->cancel(now + 1ms));

    auto execution = run(
        io,
        peers->co_execute_history_grant(
            fetch, grant, PeerUseKind::ledger_header));
    EXPECT_FALSE(execution.committed());
    EXPECT_FALSE(execution.created_generation);
    EXPECT_EQ(PeerSetTestAccess::generation_for_endpoint(io, peers, endpoint), 0u);
    EXPECT_EQ(PeerSetTestAccess::history_physical_census(io, peers).total(), 0u);
    auto parent = run(io, peers->co_ledger_demand_snapshot(demand));
    ASSERT_TRUE(parent);
    EXPECT_EQ(parent->remaining_new_dials, 1u);
    EXPECT_EQ(
        PeerSetTestAccess::history_available_global_dials(io, peers), 1u);
    EXPECT_NE(fetch->grant_state(grant), HistoryGrantState::spent);
}

TEST(PeerSetHistoryExecution, DemandReadyStaysTransientAndInstallsExactUse)
{
    boost::asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_transient_peer_generations = 1,
            .max_global_history_dials = 1});
    auto const now = std::chrono::steady_clock::now();
    auto scope = catl::core::RequestScope::create(now + 30s);
    auto demand = run(
        io,
        peers->co_get_or_create_ledger_demand(
            scope,
            LedgerDemandOptions{
                .ledger_seq = 50,
                .fallback_at = now + 10s,
                .remaining_new_dials = 1}));
    auto acquisition = run(
        io,
        peers->co_open_history_acquisition(
            demand,
            HistoryAcquisitionOptions{
                .fetch = HistoryFetchId{37}, .fetch_epoch = 1}));
    auto fetch = SharedHistoryFetch::create(HistoryFetchId{37}, 1);
    auto create_proposal = plan_history_interest(
        run(io, peers->co_history_search_snapshot(acquisition)), now);
    auto interest = fetch->register_interest(
        peers, scope, demand, acquisition, create_proposal);
    ASSERT_TRUE(interest);
    auto sponsor = fetch->select_sponsor(now);
    ASSERT_TRUE(sponsor.sponsor);
    auto create_grant = fetch->mint_grant(*sponsor.sponsor, now);
    auto creation = run(
        io,
        peers->co_execute_history_grant(
            fetch, create_grant, PeerUseKind::ledger_header));
    ASSERT_TRUE(creation.committed());
    ASSERT_TRUE(creation.created_generation);

    auto session = std::make_shared<NodeBatchTestSession>(
        creation.created_generation->endpoint);
    session->attach_strand(io);
    PeerSetTestAccess::make_reserved_generation_ready(io, peers, session);
    auto census = PeerSetTestAccess::history_physical_census(io, peers);
    EXPECT_EQ(census.queued_or_connecting, 0u);
    EXPECT_EQ(census.ready_transient, 1u);
    EXPECT_EQ(census.admitted, 0u)
        << "demand Ready cannot enter ordinary persistent membership";

    auto ready_snapshot = run(
        io, peers->co_history_search_snapshot(acquisition, interest));
    auto use_proposal = plan_history_interest(ready_snapshot, now);
    ASSERT_TRUE(use_proposal.next_action);
    ASSERT_EQ(
        use_proposal.next_action->action,
        HistoryAuthorityAction::submit_known_use);
    auto revised = fetch->revise_interest(interest, use_proposal);
    ASSERT_TRUE(revised);
    sponsor = fetch->select_sponsor(now);
    ASSERT_TRUE(sponsor.sponsor);
    auto use_grant = fetch->mint_grant(*sponsor.sponsor, now);
    ASSERT_TRUE(use_grant);
    auto use = run(
        io,
        peers->co_execute_history_grant(
            fetch, use_grant, PeerUseKind::ledger_header));
    ASSERT_TRUE(use.committed());
    ASSERT_TRUE(use.peer_use);
    EXPECT_EQ(
        use.peer_use.generation().generation(),
        creation.created_generation->generation);
    EXPECT_EQ(PeerSetTestAccess::active_peer_use_count(io, peers), 1u);
    census = PeerSetTestAccess::history_physical_census(io, peers);
    EXPECT_EQ(census.ready_transient, 1u);
    EXPECT_EQ(census.admitted, 0u);

    use.peer_use.finish(PeerUseOutcome::success);
    while (PeerSetTestAccess::active_peer_use_count(io, peers) != 0)
    {
        io.poll();
        io.restart();
    }
    EXPECT_EQ(fetch->snapshot().spent_unacknowledged, 1u)
        << "the exact transient use terminal closes only its use grant; the "
           "generation-origin grant remains owned until acquisition closure";
    run(
        io,
        peers->co_finish_history_acquisition(
            acquisition, HistoryAcquisitionTerminal::completed));
    for (std::size_t attempts = 0;
         attempts < 50 &&
         PeerSetTestAccess::history_generation_origin_count(
             io, peers, creation.created_generation->endpoint) != 0;
         ++attempts)
    {
        io.poll();
        io.restart();
    }
    EXPECT_EQ(
        PeerSetTestAccess::history_generation_origin_count(
            io, peers, creation.created_generation->endpoint),
        0u);
    EXPECT_EQ(fetch->snapshot().spent_unacknowledged, 0u);
}

TEST(PeerSetHistoryExecution, ExactAttachmentAdoptsQueuedDemandGeneration)
{
    boost::asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_transient_peer_generations = 1,
            .max_global_history_dials = 1});
    auto const now = std::chrono::steady_clock::now();
    auto scope = catl::core::RequestScope::create(now + 30s);
    auto demand = run(
        io,
        peers->co_get_or_create_ledger_demand(
            scope,
            LedgerDemandOptions{
                .ledger_seq = 50,
                .fallback_at = now + 10s,
                .remaining_new_dials = 1}));
    auto source = run(
        io,
        peers->co_open_history_acquisition(
            demand,
            HistoryAcquisitionOptions{
                .fetch = HistoryFetchId{38}, .fetch_epoch = 1}));
    auto fetch = SharedHistoryFetch::create(HistoryFetchId{38}, 1);
    auto source_interest = fetch->register_interest(
        peers,
        scope,
        demand,
        source,
        plan_history_interest(
            run(io, peers->co_history_search_snapshot(source)), now));
    ASSERT_TRUE(source_interest);
    auto sponsor = fetch->select_sponsor(now);
    ASSERT_TRUE(sponsor.sponsor);
    auto create_grant = fetch->mint_grant(*sponsor.sponsor, now);
    auto creation = run(
        io,
        peers->co_execute_history_grant(
            fetch, create_grant, PeerUseKind::ledger_header));
    ASSERT_TRUE(creation.committed());
    ASSERT_TRUE(creation.created_generation);
    auto source_wait = plan_history_interest(
        run(
            io,
            peers->co_history_search_snapshot(source, source_interest)),
        now);
    ASSERT_TRUE(source_wait.wait_until);
    auto revised_source = fetch->revise_interest(source_interest, source_wait);
    ASSERT_TRUE(revised_source);

    auto adopter = run(
        io,
        peers->co_open_history_acquisition(
            demand,
            HistoryAcquisitionOptions{
                .fetch = HistoryFetchId{38}, .fetch_epoch = 1}));
    auto attach_proposal = plan_history_interest(
        run(io, peers->co_history_search_snapshot(adopter)), now);
    ASSERT_TRUE(attach_proposal.next_action);
    ASSERT_EQ(
        attach_proposal.next_action->action,
        HistoryAuthorityAction::attach_or_adopt);
    EXPECT_EQ(
        attach_proposal.next_action->target,
        create_grant.target())
        << "predicted generation identity stays exact through attachment";
    auto adopter_interest = fetch->register_interest(
        peers, scope, demand, adopter, attach_proposal);
    ASSERT_TRUE(adopter_interest);
    sponsor = fetch->select_sponsor(now);
    ASSERT_TRUE(sponsor.sponsor);
    EXPECT_EQ(sponsor.sponsor->acquisition(), adopter.context().acquisition);
    auto attach_grant = fetch->mint_grant(*sponsor.sponsor, now);
    auto attachment = run(
        io,
        peers->co_execute_history_grant(
            fetch, attach_grant, PeerUseKind::ledger_header));
    ASSERT_TRUE(attachment.committed());
    EXPECT_EQ(fetch->snapshot().spent_unacknowledged, 2u);

    run(
        io,
        peers->co_finish_history_acquisition(
            source, HistoryAcquisitionTerminal::cancelled));
    EXPECT_TRUE(PeerSetTestAccess::history_generation_is_queued(
        io,
        peers,
        creation.created_generation->endpoint,
        creation.created_generation->generation));
    EXPECT_EQ(
        PeerSetTestAccess::history_acquisition_origin_count(
            io, peers, source.context().acquisition),
        0u);
    EXPECT_EQ(
        PeerSetTestAccess::history_acquisition_origin_count(
            io, peers, adopter.context().acquisition),
        1u);
    EXPECT_EQ(
        PeerSetTestAccess::history_generation_attachment_count(
            io, peers, creation.created_generation->endpoint),
        0u)
        << "the attachment grant becomes the sole retained physical owner";
    EXPECT_EQ(fetch->snapshot().spent_unacknowledged, 1u);

    run(
        io,
        peers->co_finish_history_acquisition(
            adopter, HistoryAcquisitionTerminal::cancelled));
    EXPECT_EQ(
        PeerSetTestAccess::generation_for_endpoint(
            io, peers, creation.created_generation->endpoint),
        0u);
    EXPECT_EQ(fetch->snapshot().spent_unacknowledged, 0u);
}

TEST(PeerSetHistoryExecution, PhysicalWatchdogNamesAndReleasesExactQueuedSlot)
{
    boost::asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_transient_peer_generations = 1,
            .max_global_history_dials = 2,
            .history_connect_timeout = 50ms});
    auto const now = std::chrono::steady_clock::now();
    auto first_scope = catl::core::RequestScope::create(now + 1s);
    auto first_demand = run(
        io,
        peers->co_get_or_create_ledger_demand(
            first_scope,
            LedgerDemandOptions{
                .ledger_seq = 50,
                .fallback_at = now + 500ms,
                .remaining_new_dials = 1}));
    auto first_acquisition = run(
        io,
        peers->co_open_history_acquisition(
            first_demand,
            HistoryAcquisitionOptions{
                .fetch = HistoryFetchId{39}, .fetch_epoch = 1}));
    auto first_fetch = SharedHistoryFetch::create(HistoryFetchId{39}, 1);
    auto first_interest = first_fetch->register_interest(
        peers,
        first_scope,
        first_demand,
        first_acquisition,
        plan_history_interest(
            run(io, peers->co_history_search_snapshot(first_acquisition)),
            now));
    auto first_sponsor = first_fetch->select_sponsor(now);
    ASSERT_TRUE(first_sponsor.sponsor);
    auto first_grant = first_fetch->mint_grant(*first_sponsor.sponsor, now);
    auto first_creation = run(
        io,
        peers->co_execute_history_grant(
            first_fetch, first_grant, PeerUseKind::ledger_header));
    ASSERT_TRUE(first_creation.committed());
    ASSERT_TRUE(first_creation.created_generation);
    ASSERT_TRUE(first_interest);

    auto const next_endpoint = std::string("next.example.com:51235");
    PeerSetTestAccess::install_history_source(
        io, peers, next_endpoint, false, true, false, false);
    auto second_scope = catl::core::RequestScope::create(now + 1s);
    auto second_demand = run(
        io,
        peers->co_get_or_create_ledger_demand(
            second_scope,
            LedgerDemandOptions{
                .ledger_seq = 50,
                .fallback_at = now + 500ms,
                .remaining_new_dials = 1}));
    auto second_acquisition = run(
        io,
        peers->co_open_history_acquisition(
            second_demand,
            HistoryAcquisitionOptions{
                .fetch = HistoryFetchId{40},
                .fetch_epoch = 1,
                .excluded_endpoints = {
                    first_creation.created_generation->endpoint}}));
    auto capped = run(
        io, peers->co_history_search_snapshot(second_acquisition));
    ASSERT_FALSE(capped.physical.can_create());
    auto wait = plan_history_interest(capped, std::chrono::steady_clock::now());
    ASSERT_TRUE(wait.wait_until);
    EXPECT_EQ(wait.wait_until->kind, HistoryWakeSourceKind::physical_watchdog);
    EXPECT_EQ(
        wait.wait_until->generation,
        first_creation.created_generation->generation);
    EXPECT_EQ(
        wait.wait_until->endpoint,
        first_creation.created_generation->endpoint);

    io.restart();
    io.run_for(100ms);
    io.restart();
    EXPECT_EQ(
        PeerSetTestAccess::generation_for_endpoint(
            io, peers, first_creation.created_generation->endpoint),
        0u);
    EXPECT_EQ(PeerSetTestAccess::history_physical_census(io, peers).total(), 0u);
    EXPECT_EQ(first_fetch->snapshot().spent_unacknowledged, 0u);

    auto released = run(
        io, peers->co_history_search_snapshot(second_acquisition));
    EXPECT_TRUE(released.physical.can_create());
    auto create = plan_history_interest(
        released, std::chrono::steady_clock::now());
    ASSERT_TRUE(create.next_action);
    EXPECT_EQ(
        create.next_action->action,
        HistoryAuthorityAction::create_generation);
    EXPECT_NE(
        create.next_action->target.endpoint,
        first_creation.created_generation->endpoint);
}

TEST(PeerSetHistoryExecution, RedirectsStayBoundedAndAcquisitionScoped)
{
    boost::asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_transient_peer_generations = 1,
            .max_global_history_dials = 1});
    auto const now = std::chrono::steady_clock::now();
    auto scope = catl::core::RequestScope::create(now + 30s);
    auto demand = run(
        io,
        peers->co_get_or_create_ledger_demand(
            scope,
            LedgerDemandOptions{
                .ledger_seq = 50,
                .fallback_at = now + 10s,
                .remaining_new_dials = 1}));
    auto source = run(
        io,
        peers->co_open_history_acquisition(
            demand,
            HistoryAcquisitionOptions{
                .fetch = HistoryFetchId{41}, .fetch_epoch = 1}));
    auto fetch = SharedHistoryFetch::create(HistoryFetchId{41}, 1);
    auto source_interest = fetch->register_interest(
        peers,
        scope,
        demand,
        source,
        plan_history_interest(
            run(io, peers->co_history_search_snapshot(source)), now));
    auto sponsor = fetch->select_sponsor(now);
    ASSERT_TRUE(sponsor.sponsor);
    auto create_grant = fetch->mint_grant(*sponsor.sponsor, now);
    auto creation = run(
        io,
        peers->co_execute_history_grant(
            fetch, create_grant, PeerUseKind::ledger_header));
    ASSERT_TRUE(creation.created_generation);
    auto source_wait = plan_history_interest(
        run(
            io,
            peers->co_history_search_snapshot(source, source_interest)),
        now);
    auto revised_source =
        fetch->revise_interest(source_interest, source_wait);
    ASSERT_TRUE(revised_source);
    source_interest = *revised_source;

    auto attached = run(
        io,
        peers->co_open_history_acquisition(
            demand,
            HistoryAcquisitionOptions{
                .fetch = HistoryFetchId{41}, .fetch_epoch = 1}));
    auto attach_interest = fetch->register_interest(
        peers,
        scope,
        demand,
        attached,
        plan_history_interest(
            run(io, peers->co_history_search_snapshot(attached)), now));
    sponsor = fetch->select_sponsor(now);
    ASSERT_TRUE(sponsor.sponsor);
    auto attach_grant = fetch->mint_grant(*sponsor.sponsor, now);
    ASSERT_TRUE(run(
                    io,
                    peers->co_execute_history_grant(
                        fetch, attach_grant, PeerUseKind::ledger_header))
                    .committed());
    ASSERT_TRUE(attach_interest);

    auto unrelated = run(
        io,
        peers->co_open_history_acquisition(
            demand,
            HistoryAcquisitionOptions{
                .fetch = HistoryFetchId{41}, .fetch_epoch = 1}));
    auto captured = PeerSetTestAccess::capture_history_redirects(
        io,
        peers,
        creation.created_generation->endpoint,
        creation.created_generation->generation,
        51235,
        {"8.8.8.8", "1.1.1.1", "127.0.0.1"});
    ASSERT_TRUE(captured);
    EXPECT_EQ(captured->first, 2u);
    EXPECT_EQ(captured->second, 1u);

    auto contains = [](HistorySearchSnapshot const& snapshot,
                       std::string const& endpoint) {
        return std::any_of(
            snapshot.candidates.begin(),
            snapshot.candidates.end(),
            [&endpoint](auto const& candidate) {
                return candidate.target.endpoint == endpoint;
            });
    };
    auto source_snapshot = run(
        io, peers->co_history_search_snapshot(source, source_interest));
    auto attached_snapshot = run(
        io, peers->co_history_search_snapshot(attached, attach_interest));
    auto unrelated_snapshot = run(
        io, peers->co_history_search_snapshot(unrelated));
    for (auto const& successor :
         {std::string("8.8.8.8:51235"), std::string("1.1.1.1:51235")})
    {
        EXPECT_TRUE(contains(source_snapshot, successor));
        EXPECT_TRUE(contains(attached_snapshot, successor));
        EXPECT_FALSE(contains(unrelated_snapshot, successor));
    }

    run(
        io,
        peers->co_finish_history_acquisition(
            source, HistoryAcquisitionTerminal::cancelled));
    run(
        io,
        peers->co_finish_history_acquisition(
            attached, HistoryAcquisitionTerminal::cancelled));
    run(
        io,
        peers->co_finish_history_acquisition(
            unrelated, HistoryAcquisitionTerminal::cancelled));
}

TEST(PeerSetHistoryExecution, StaleCreationRefusesAfterPhysicalSlotIsTaken)
{
    boost::asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_transient_peer_generations = 1,
            .max_global_history_dials = 1});
    auto const now = std::chrono::steady_clock::now();
    auto scope = catl::core::RequestScope::create(now + 30s);
    auto demand = run(
        io,
        peers->co_get_or_create_ledger_demand(
            scope,
            LedgerDemandOptions{
                .ledger_seq = 50,
                .fallback_at = now + 10s,
                .remaining_new_dials = 1}));
    auto acquisition = run(
        io,
        peers->co_open_history_acquisition(
            demand,
            HistoryAcquisitionOptions{
                .fetch = HistoryFetchId{36}, .fetch_epoch = 1}));
    auto fetch = SharedHistoryFetch::create(HistoryFetchId{36}, 1);
    auto proposal = plan_history_interest(
        run(io, peers->co_history_search_snapshot(acquisition)), now);
    ASSERT_TRUE(proposal.next_action);
    ASSERT_EQ(
        proposal.next_action->action,
        HistoryAuthorityAction::create_generation);
    auto const endpoint = proposal.next_action->target.endpoint;
    auto interest = fetch->register_interest(
        peers, scope, demand, acquisition, std::move(proposal));
    ASSERT_TRUE(interest);
    auto sponsor = fetch->select_sponsor(now);
    ASSERT_TRUE(sponsor.sponsor);
    auto grant = fetch->mint_grant(*sponsor.sponsor, now);
    ASSERT_TRUE(grant);

    PeerSetTestAccess::push_pending_connect(
        io, peers, "slot-owner.example.com:51235");
    ASSERT_FALSE(PeerSetTestAccess::history_physical_census(io, peers).can_create());
    auto execution = run(
        io,
        peers->co_execute_history_grant(
            fetch, grant, PeerUseKind::ledger_header));
    EXPECT_FALSE(execution.committed());
    EXPECT_FALSE(execution.created_generation);
    EXPECT_EQ(PeerSetTestAccess::generation_for_endpoint(io, peers, endpoint), 0u);
    auto parent = run(io, peers->co_ledger_demand_snapshot(demand));
    ASSERT_TRUE(parent);
    EXPECT_EQ(parent->remaining_new_dials, 1u);
    EXPECT_EQ(
        PeerSetTestAccess::history_available_global_dials(io, peers), 1u);
    EXPECT_NE(fetch->grant_state(grant), HistoryGrantState::spent);
}

TEST(PeerSetHistoryExecution, AttachmentHasExactReverseOwnerAndInheritedWake)
{
    boost::asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_history_attachments_per_generation = 1});
    auto const endpoint = std::string("queued.example.com:51235");
    PeerSetTestAccess::push_pending_connect(io, peers, endpoint);
    auto const generation =
        PeerSetTestAccess::generation_for_endpoint(io, peers, endpoint);
    ASSERT_NE(generation, 0u);

    auto const now = std::chrono::steady_clock::now();
    auto scope = catl::core::RequestScope::create(now + 30s);
    auto demand = run(
        io,
        peers->co_get_or_create_ledger_demand(
            scope,
            LedgerDemandOptions{
                .ledger_seq = 50, .remaining_new_dials = 1}));
    auto acquisition = run(
        io,
        peers->co_open_history_acquisition(
            demand,
            HistoryAcquisitionOptions{
                .fetch = HistoryFetchId{33}, .fetch_epoch = 1}));
    auto fetch = SharedHistoryFetch::create(HistoryFetchId{33}, 1);
    auto proposal = plan_history_interest(
        run(io, peers->co_history_search_snapshot(acquisition)), now);
    ASSERT_TRUE(proposal.next_action);
    ASSERT_EQ(
        proposal.next_action->action,
        HistoryAuthorityAction::attach_or_adopt);
    auto interest = fetch->register_interest(
        peers, scope, demand, acquisition, std::move(proposal));
    ASSERT_TRUE(interest);
    auto sponsor = fetch->select_sponsor(now);
    ASSERT_TRUE(sponsor.sponsor);
    auto grant = fetch->mint_grant(*sponsor.sponsor, now);
    ASSERT_TRUE(grant);

    auto execution = run(
        io,
        peers->co_execute_history_grant(
            fetch, grant, PeerUseKind::ledger_header));
    ASSERT_TRUE(execution.committed());
    EXPECT_FALSE(execution.peer_use);
    EXPECT_EQ(
        PeerSetTestAccess::history_generation_attachment_count(
            io, peers, endpoint),
        1u);
    EXPECT_EQ(
        PeerSetTestAccess::history_acquisition_attachment_count(
            io, peers, acquisition.context().acquisition),
        1u);
    EXPECT_EQ(fetch->snapshot().spent_unacknowledged, 1u);
    auto parent = run(io, peers->co_ledger_demand_snapshot(demand));
    ASSERT_TRUE(parent);
    EXPECT_EQ(parent->remaining_new_dials, 0u)
        << "a piggyback attachment sponsors one bounded generation probe";

    auto attached = run(
        io, peers->co_history_search_snapshot(acquisition, interest));
    auto wait = plan_history_interest(attached, now);
    ASSERT_TRUE(wait.wait_until);
    EXPECT_EQ(
        wait.wait_until->kind,
        HistoryWakeSourceKind::attached_generation_deadline);
    EXPECT_EQ(wait.wait_until->generation, generation);
    auto revised = fetch->revise_interest(interest, wait);
    ASSERT_TRUE(revised);

    auto second_acquisition = run(
        io,
        peers->co_open_history_acquisition(
            demand,
            HistoryAcquisitionOptions{
                .fetch = HistoryFetchId{33}, .fetch_epoch = 1}));
    auto second_interest = fetch->register_interest(
        peers,
        scope,
        demand,
        second_acquisition,
        plan_history_interest(
            run(io, peers->co_history_search_snapshot(second_acquisition)),
            now));
    ASSERT_TRUE(second_interest);
    auto second_sponsor = fetch->select_sponsor(now);
    ASSERT_TRUE(second_sponsor.sponsor);
    EXPECT_EQ(
        second_sponsor.sponsor->acquisition(),
        second_acquisition.context().acquisition);
    auto second_grant = fetch->mint_grant(*second_sponsor.sponsor, now);
    ASSERT_TRUE(second_grant);
    auto refused = run(
        io,
        peers->co_execute_history_grant(
            fetch, second_grant, PeerUseKind::ledger_header));
    EXPECT_FALSE(refused.committed());
    EXPECT_NE(fetch->grant_state(second_grant), HistoryGrantState::spent);
    EXPECT_EQ(
        PeerSetTestAccess::history_acquisition_attachment_count(
            io, peers, second_acquisition.context().acquisition),
        0u);

    run(
        io,
        peers->co_finish_history_acquisition(
            acquisition, HistoryAcquisitionTerminal::cancelled));
    EXPECT_EQ(
        PeerSetTestAccess::history_generation_attachment_count(
            io, peers, endpoint),
        0u);
    EXPECT_EQ(
        PeerSetTestAccess::history_acquisition_attachment_count(
            io, peers, acquisition.context().acquisition),
        0u);
    EXPECT_EQ(fetch->snapshot().spent_unacknowledged, 0u);

    auto admitted_after_release = run(
        io,
        peers->co_execute_history_grant(
            fetch, second_grant, PeerUseKind::ledger_header));
    ASSERT_TRUE(admitted_after_release.committed());
    EXPECT_EQ(
        PeerSetTestAccess::history_generation_attachment_count(
            io, peers, endpoint),
        1u);
    auto session = std::make_shared<NodeBatchTestSession>(endpoint);
    session->attach_strand(io);
    PeerSetTestAccess::make_reserved_generation_ready(io, peers, session);
    EXPECT_EQ(
        PeerSetTestAccess::history_generation_attachment_count(
            io, peers, endpoint),
        0u);
    EXPECT_EQ(
        PeerSetTestAccess::history_acquisition_attachment_count(
            io, peers, second_acquisition.context().acquisition),
        0u);
    EXPECT_EQ(fetch->snapshot().spent_unacknowledged, 0u)
        << "ready fan-out closes the exact attachment grant";
    run(
        io,
        peers->co_finish_history_acquisition(
            second_acquisition, HistoryAcquisitionTerminal::completed));
}

TEST(PeerSetHistoryExecution, AttachmentProbeBudgetDoesNotResetForNewGeneration)
{
    boost::asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0});
    auto const first = std::string("probe-first.example.com:51235");
    auto const second = std::string("probe-second.example.com:51235");
    PeerSetTestAccess::push_pending_connect(io, peers, first);
    PeerSetTestAccess::push_pending_connect(io, peers, second);

    auto const now = std::chrono::steady_clock::now();
    auto scope = catl::core::RequestScope::create(now + 30s);
    auto demand = run(
        io,
        peers->co_get_or_create_ledger_demand(
            scope,
            LedgerDemandOptions{.ledger_seq = 50, .remaining_new_dials = 1}));
    auto first_acquisition =
        run(io,
            peers->co_open_history_acquisition(
                demand,
                HistoryAcquisitionOptions{
                    .fetch = HistoryFetchId{51},
                    .fetch_epoch = 1,
                    .excluded_endpoints = {second}}));
    auto first_fetch = SharedHistoryFetch::create(HistoryFetchId{51}, 1);
    auto first_plan = plan_history_interest(
        run(io, peers->co_history_search_snapshot(first_acquisition)), now);
    ASSERT_TRUE(first_plan.next_action);
    ASSERT_EQ(
        first_plan.next_action->action,
        HistoryAuthorityAction::attach_or_adopt);
    ASSERT_EQ(first_plan.next_action->target.endpoint, first);
    auto first_interest = first_fetch->register_interest(
        peers, scope, demand, first_acquisition, std::move(first_plan));
    auto sponsor = first_fetch->select_sponsor(now);
    ASSERT_TRUE(sponsor.sponsor);
    auto grant = first_fetch->mint_grant(*sponsor.sponsor, now);
    ASSERT_TRUE(grant);
    auto attached =
        run(io,
            peers->co_execute_history_grant(
                first_fetch, grant, PeerUseKind::ledger_header));
    ASSERT_TRUE(attached.committed());
    run(io,
        peers->co_finish_history_acquisition(
            first_acquisition, HistoryAcquisitionTerminal::cancelled));
    (void)first_fetch->detach(first_interest);

    auto second_acquisition =
        run(io,
            peers->co_open_history_acquisition(
                demand,
                HistoryAcquisitionOptions{
                    .fetch = HistoryFetchId{52},
                    .fetch_epoch = 1,
                    .excluded_endpoints = {first}}));
    auto second_plan = plan_history_interest(
        run(io, peers->co_history_search_snapshot(second_acquisition)), now);
    EXPECT_FALSE(second_plan.next_action);
    EXPECT_EQ(
        second_plan.local_terminal, HistoryAcquisitionTerminal::exhausted);
    auto parent = run(io, peers->co_ledger_demand_snapshot(demand));
    ASSERT_TRUE(parent);
    EXPECT_EQ(parent->remaining_new_dials, 0u);
    run(io,
        peers->co_finish_history_acquisition(
            second_acquisition, HistoryAcquisitionTerminal::cancelled));
}

TEST(PeerSetHistoryDriver, EmptyInterestAcquiresAndClosesExactKnownUse)
{
    boost::asio::io_context io;
    auto peers = PeerSet::create(io);
    auto session =
        std::make_shared<NodeBatchTestSession>("driver-ready.example.com:51235");
    session->attach_strand(io);
    auto const generation =
        PeerSetTestAccess::insert_ready_peer_with_generation(io, peers, session);
    auto const now = std::chrono::steady_clock::now();
    auto scope = catl::core::RequestScope::create(now + 2s);
    auto demand = run(
        io,
        peers->co_get_or_create_ledger_demand(
            scope, LedgerDemandOptions{.ledger_seq = 50}));
    auto acquisition = run(
        io,
        peers->co_open_history_acquisition(
            demand,
            HistoryAcquisitionOptions{
                .fetch = HistoryFetchId{42}, .fetch_epoch = 1}));
    auto fetch = SharedHistoryFetch::create(HistoryFetchId{42}, 1);
    auto interest = fetch->register_interest(
        peers, scope, demand, acquisition);

    auto driven = run(
        io,
        peers->co_drive_history_interest(
            fetch, acquisition, interest, PeerUseKind::ledger_header));
    ASSERT_TRUE(driven.acquired());
    EXPECT_FALSE(driven.local_terminal);
    EXPECT_FALSE(driven.shared_terminal);
    EXPECT_EQ(driven.peer_use.generation().generation(), generation);
    EXPECT_EQ(driven.replans, 1u);
    EXPECT_EQ(driven.grants, 1u);
    EXPECT_EQ(driven.commits, 1u);
    EXPECT_NE(
        driven.interest.participant_revision(),
        interest.participant_revision());

    auto receipt = driven.peer_use.receipt();
    driven.peer_use.finish(PeerUseOutcome::success);
    EXPECT_EQ(run(io, receipt.wait()), PeerUseOutcome::success);
    EXPECT_EQ(fetch->snapshot().spent_unacknowledged, 0u);
    auto detached = fetch->detach(driven.interest);
    EXPECT_TRUE(detached.accepted);
    EXPECT_TRUE(detached.acknowledged);
    run(
        io,
        peers->co_finish_history_acquisition(
            acquisition, HistoryAcquisitionTerminal::completed));
}

TEST(PeerSetHistoryDriver, ExactAttachmentWakeReplansToReadyUse)
{
    boost::asio::io_context io;
    auto peers = PeerSet::create(io);
    auto const endpoint = std::string("driver-queued.example.com:51235");
    PeerSetTestAccess::push_pending_connect(io, peers, endpoint);
    auto const generation =
        PeerSetTestAccess::generation_for_endpoint(io, peers, endpoint);
    ASSERT_NE(generation, 0u);

    auto const now = std::chrono::steady_clock::now();
    auto scope = catl::core::RequestScope::create(now + 2s);
    auto demand = run(
        io,
        peers->co_get_or_create_ledger_demand(
            scope,
            LedgerDemandOptions{.ledger_seq = 50, .remaining_new_dials = 1}));
    auto acquisition = run(
        io,
        peers->co_open_history_acquisition(
            demand,
            HistoryAcquisitionOptions{
                .fetch = HistoryFetchId{43}, .fetch_epoch = 1}));
    auto fetch = SharedHistoryFetch::create(HistoryFetchId{43}, 1);
    auto interest = fetch->register_interest(
        peers, scope, demand, acquisition);
    auto future = boost::asio::co_spawn(
        io,
        peers->co_drive_history_interest(
            fetch, acquisition, interest, PeerUseKind::ledger_header),
        boost::asio::use_future);

    io.run_for(20ms);
    io.restart();
    EXPECT_EQ(
        PeerSetTestAccess::history_acquisition_attachment_count(
            io, peers, acquisition.context().acquisition),
        1u);
    auto session = std::make_shared<NodeBatchTestSession>(endpoint);
    session->attach_strand(io);
    PeerSetTestAccess::make_reserved_generation_ready(io, peers, session);
    while (future.wait_for(1ms) != std::future_status::ready)
    {
        io.poll();
        io.restart();
    }
    auto driven = future.get();
    ASSERT_TRUE(driven.acquired());
    EXPECT_EQ(driven.peer_use.generation().generation(), generation);
    EXPECT_GE(driven.replans, 3u)
        << "attach, exact wait, and Ready use are distinct revisions";
    EXPECT_EQ(driven.grants, 3u);
    EXPECT_EQ(driven.commits, 3u)
        << "Ready fan-out closes the queued attachment, so transient use "
           "requires one fresh exact attachment before its use grant";

    auto receipt = driven.peer_use.receipt();
    driven.peer_use.finish(PeerUseOutcome::success);
    EXPECT_EQ(run(io, receipt.wait()), PeerUseOutcome::success);
    (void)fetch->detach(driven.interest);
    run(
        io,
        peers->co_finish_history_acquisition(
            acquisition, HistoryAcquisitionTerminal::completed));
    EXPECT_EQ(fetch->snapshot().spent_unacknowledged, 0u);
}

TEST(PeerSetHistoryDriver, RequestCancellationWakesAttachedWaitImmediately)
{
    boost::asio::io_context io;
    auto peers = PeerSet::create(io);
    auto const endpoint = std::string("driver-cancel.example.com:51235");
    PeerSetTestAccess::push_pending_connect(io, peers, endpoint);
    auto const now = std::chrono::steady_clock::now();
    auto scope = catl::core::RequestScope::create(now + 5s);
    auto demand = run(
        io,
        peers->co_get_or_create_ledger_demand(
            scope,
            LedgerDemandOptions{.ledger_seq = 50, .remaining_new_dials = 1}));
    auto acquisition = run(
        io,
        peers->co_open_history_acquisition(
            demand,
            HistoryAcquisitionOptions{
                .fetch = HistoryFetchId{44}, .fetch_epoch = 1}));
    auto fetch = SharedHistoryFetch::create(HistoryFetchId{44}, 1);
    auto interest = fetch->register_interest(
        peers, scope, demand, acquisition);
    auto future = boost::asio::co_spawn(
        io,
        peers->co_drive_history_interest(
            fetch, acquisition, interest, PeerUseKind::ledger_header),
        boost::asio::use_future);

    io.run_for(20ms);
    io.restart();
    ASSERT_EQ(
        PeerSetTestAccess::history_acquisition_attachment_count(
            io, peers, acquisition.context().acquisition),
        1u);
    auto const cancel_at = std::chrono::steady_clock::now();
    ASSERT_TRUE(scope->cancel(cancel_at));
    while (future.wait_for(1ms) != std::future_status::ready)
    {
        io.poll();
        io.restart();
    }
    auto driven = future.get();
    EXPECT_FALSE(driven.acquired());
    ASSERT_TRUE(driven.local_terminal);
    EXPECT_EQ(
        *driven.local_terminal, HistoryAcquisitionTerminal::cancelled);
    EXPECT_LT(std::chrono::steady_clock::now() - cancel_at, 1s);
    EXPECT_EQ(
        PeerSetTestAccess::history_acquisition_attachment_count(
            io, peers, acquisition.context().acquisition),
        0u);
    EXPECT_EQ(fetch->snapshot().spent_unacknowledged, 0u);
}

TEST(PeerSetHistoryDriver, AttachedWaitIgnoresUnrelatedPoolChurn)
{
    boost::asio::io_context io;
    auto peers = PeerSet::create(io);
    auto const endpoint = std::string("driver-churn.example.com:51235");
    PeerSetTestAccess::push_pending_connect(io, peers, endpoint);
    auto const now = std::chrono::steady_clock::now();
    auto scope = catl::core::RequestScope::create(now + 5s);
    auto demand = run(
        io,
        peers->co_get_or_create_ledger_demand(
            scope,
            LedgerDemandOptions{.ledger_seq = 50, .remaining_new_dials = 1}));
    auto acquisition = run(
        io,
        peers->co_open_history_acquisition(
            demand,
            HistoryAcquisitionOptions{
                .fetch = HistoryFetchId{52}, .fetch_epoch = 1}));
    auto fetch = SharedHistoryFetch::create(HistoryFetchId{52}, 1);
    auto interest = fetch->register_interest(
        peers, scope, demand, acquisition);
    auto future = boost::asio::co_spawn(
        io,
        peers->co_drive_history_interest(
            fetch, acquisition, interest, PeerUseKind::ledger_header),
        boost::asio::use_future);

    io.run_for(20ms);
    io.restart();
    ASSERT_EQ(
        PeerSetTestAccess::history_acquisition_attachment_count(
            io, peers, acquisition.context().acquisition),
        1u);

    for (std::size_t i = 0; i < 100; ++i)
    {
        PeerSetTestAccess::post_history_pool_wake(peers, i % 2 == 0);
        io.poll();
        io.restart();
    }
    ASSERT_TRUE(scope->cancel());
    while (future.wait_for(1ms) != std::future_status::ready)
    {
        io.poll();
        io.restart();
    }
    auto driven = future.get();
    ASSERT_TRUE(driven.local_terminal);
    EXPECT_EQ(*driven.local_terminal, HistoryAcquisitionTerminal::cancelled);
    EXPECT_LE(driven.replans, 3u)
        << "an exact attached wait depends on its generation, not unrelated "
           "pool revisions";
    EXPECT_GE(driven.pool_wakes_reused, 90u);
    EXPECT_GE(driven.discovery_wakes_reused, 45u);
    EXPECT_EQ(fetch->snapshot().spent_unacknowledged, 0u);
}

TEST(PeerSetHistoryDriver, RelevantPoolChurnCoalescesAtProgressTick)
{
    boost::asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .retry_backoff = 2s});
    auto const now = std::chrono::steady_clock::now();
    PeerSetTestAccess::defer_configured_history_candidates(io, peers, now);
    auto scope = catl::core::RequestScope::create(now + 5s);
    auto demand = run(
        io,
        peers->co_get_or_create_ledger_demand(
            scope,
            LedgerDemandOptions{.ledger_seq = 50, .remaining_new_dials = 1}));
    auto acquisition = run(
        io,
        peers->co_open_history_acquisition(
            demand,
            HistoryAcquisitionOptions{
                .fetch = HistoryFetchId{53}, .fetch_epoch = 1}));
    auto initial = plan_history_interest(
        run(io, peers->co_history_search_snapshot(acquisition)), now);
    ASSERT_TRUE(initial.wait_until);
    ASSERT_EQ(
        initial.wait_until->kind,
        HistoryWakeSourceKind::candidate_retry_expiry);

    auto fetch = SharedHistoryFetch::create(HistoryFetchId{53}, 1);
    auto interest = fetch->register_interest(
        peers, scope, demand, acquisition);
    auto future = boost::asio::co_spawn(
        io,
        peers->co_drive_history_interest(
            fetch, acquisition, interest, PeerUseKind::ledger_header),
        boost::asio::use_future);
    io.run_for(20ms);
    io.restart();

    for (std::size_t i = 0; i < 100; ++i)
    {
        PeerSetTestAccess::post_history_pool_wake(peers, false);
        io.poll();
        io.restart();
    }
    boost::asio::steady_timer cancel(io, 1100ms);
    cancel.async_wait([scope](boost::system::error_code ec) {
        if (!ec)
            scope->cancel();
    });
    while (future.wait_for(1ms) != std::future_status::ready)
    {
        io.poll();
        io.restart();
    }
    auto driven = future.get();
    ASSERT_TRUE(driven.local_terminal);
    EXPECT_EQ(*driven.local_terminal, HistoryAcquisitionTerminal::cancelled);
    EXPECT_LE(driven.replans, 3u)
        << "one hundred relevant revisions must collapse into one bounded "
           "planner evaluation";
    EXPECT_GE(driven.pool_wakes_reused, 90u);
    EXPECT_GE(driven.local_wakes_coalesced, 90u);
    EXPECT_LE(driven.replans_after_local_change, 2u);
    EXPECT_EQ(fetch->snapshot().spent_unacknowledged, 0u);
}

TEST(PeerSetHistoryDriver, AttachedWaitReportsCanonicalSelectionProgress)
{
    boost::asio::io_context io;
    auto peers = PeerSet::create(io);
    auto const endpoint = std::string("driver-progress.example.com:51235");
    PeerSetTestAccess::push_pending_connect(io, peers, endpoint);

    auto const now = std::chrono::steady_clock::now();
    auto scope = catl::core::RequestScope::create(now + 5s);
    auto demand = run(
        io,
        peers->co_get_or_create_ledger_demand(
            scope,
            LedgerDemandOptions{.ledger_seq = 50, .remaining_new_dials = 1}));
    auto acquisition =
        run(io,
            peers->co_open_history_acquisition(
                demand,
                HistoryAcquisitionOptions{
                    .fetch = HistoryFetchId{47}, .fetch_epoch = 1}));
    auto fetch = SharedHistoryFetch::create(HistoryFetchId{47}, 1);
    auto interest = fetch->register_interest(peers, scope, demand, acquisition);
    auto request = std::make_shared<catl::core::RequestContext>(
        "history-driver-progress",
        "transaction",
        std::chrono::steady_clock::now());
    auto request_executor =
        catl::core::ContextExecutor(boost::asio::make_strand(io), request);
    auto future = boost::asio::co_spawn(
        request_executor,
        peers->co_drive_history_interest(
            fetch, acquisition, interest, PeerUseKind::ledger_header),
        boost::asio::use_future);

    boost::asio::steady_timer cancel(io, 1250ms);
    cancel.async_wait([scope](boost::system::error_code ec) {
        if (!ec)
            scope->cancel();
    });
    while (future.wait_for(1ms) != std::future_status::ready)
    {
        io.poll();
        io.restart();
    }
    auto driven = future.get();
    ASSERT_TRUE(driven.local_terminal);
    EXPECT_EQ(*driven.local_terminal, HistoryAcquisitionTerminal::cancelled);
    EXPECT_LT(driven.replans, 10u)
        << "a driver must not consume the wake emitted by its own interest "
           "revision and hot-loop while waiting";
    EXPECT_LE(driven.replans, 3u)
        << "progress must reuse the current local plan; cancellation may "
           "publish one final local cleanup revision";
    EXPECT_EQ(driven.replans_after_commit, 1u);
    EXPECT_LE(driven.replans_after_local_change, 1u);
    EXPECT_EQ(driven.replans_after_scheduled_wake, 0u);
    EXPECT_GE(driven.progress_wakes_reused, 1u);

    auto const events = request->drain();
    auto const progress_events =
        std::ranges::count_if(events, [](std::string const& event) {
            return event.starts_with("history source selection ledger 50:");
        });
    EXPECT_EQ(progress_events, 1u);
    EXPECT_TRUE(std::ranges::any_of(
        events,
        [&](std::string const& event) {
            return event.find(endpoint) != std::string::npos &&
                event.find("replans=") != std::string::npos &&
                event.find("replans=2 [action=1, local=0, timer=0]") !=
                std::string::npos &&
                event.find("grants/commits=") != std::string::npos &&
                event.find("arbitrations=") != std::string::npos &&
                event.find("reused=shared:") != std::string::npos &&
                event.find("eligible_candidates=") != std::string::npos &&
                event.find("physical_slots=") != std::string::npos &&
                event.find("remaining=") != std::string::npos;
        }))
        << "the history owner must expose its canonical selection state "
           "through the retained request context; events were: "
        << testing::PrintToString(events);
    EXPECT_EQ(fetch->snapshot().spent_unacknowledged, 0u);
}

TEST(PeerSetHistoryDriver, WaitReentersOwnerStrandAcrossConcurrentRunners)
{
    boost::asio::io_context io;
    auto peers = PeerSet::create(io);
    auto const endpoint = std::string("driver-concurrent.example.com:51235");
    PeerSetTestAccess::push_pending_connect(io, peers, endpoint);

    auto const now = std::chrono::steady_clock::now();
    auto scope = catl::core::RequestScope::create(now + 2s);
    auto demand = run(
        io,
        peers->co_get_or_create_ledger_demand(
            scope,
            LedgerDemandOptions{.ledger_seq = 50, .remaining_new_dials = 1}));
    auto acquisition =
        run(io,
            peers->co_open_history_acquisition(
                demand,
                HistoryAcquisitionOptions{
                    .fetch = HistoryFetchId{50}, .fetch_epoch = 1}));
    auto fetch = SharedHistoryFetch::create(HistoryFetchId{50}, 1);
    auto interest = fetch->register_interest(peers, scope, demand, acquisition);
    auto request = std::make_shared<catl::core::RequestContext>(
        "history-driver-concurrent",
        "transaction",
        std::chrono::steady_clock::now());
    auto caller_executor =
        catl::core::ContextExecutor(boost::asio::make_strand(io), request);
    auto future = boost::asio::co_spawn(
        caller_executor,
        peers->co_drive_history_interest(
            fetch, acquisition, interest, PeerUseKind::ledger_header),
        boost::asio::use_future);
    boost::asio::steady_timer cancel(io, 50ms);
    cancel.async_wait([scope](boost::system::error_code ec) {
        if (!ec)
            scope->cancel();
    });

    auto work = boost::asio::make_work_guard(io);
    std::thread first([&io]() { io.run(); });
    std::thread second([&io]() { io.run(); });
    auto driven = future.get();
    work.reset();
    io.stop();
    first.join();
    second.join();

    ASSERT_TRUE(driven.local_terminal);
    EXPECT_EQ(*driven.local_terminal, HistoryAcquisitionTerminal::cancelled);
    EXPECT_FALSE(driven.acquired());
    EXPECT_EQ(fetch->snapshot().spent_unacknowledged, 0u);
}

TEST(PeerSetHistoryDriver, FullGrantRegistryWaitsForReceiptWithoutHotLooping)
{
    boost::asio::io_context io;
    auto peers = PeerSet::create(io);
    auto session =
        std::make_shared<NodeBatchTestSession>("driver-capacity.example.com:1");
    session->attach_strand(io);
    PeerSetTestAccess::insert_ready_peer_with_generation(io, peers, session);

    auto const now = std::chrono::steady_clock::now();
    auto scope = catl::core::RequestScope::create(now + 5s);
    auto demand =
        run(io,
            peers->co_get_or_create_ledger_demand(
                scope, LedgerDemandOptions{.ledger_seq = 50}));
    auto acquisition =
        run(io,
            peers->co_open_history_acquisition(
                demand,
                HistoryAcquisitionOptions{
                    .fetch = HistoryFetchId{48}, .fetch_epoch = 1}));
    auto fetch = SharedHistoryFetch::create(
        HistoryFetchId{48},
        1,
        SharedHistoryFetch::Limits{
            .max_interests = 1, .max_grants_per_interest = 1});
    ASSERT_TRUE(fetch);
    auto plan = plan_history_interest(
        run(io, peers->co_history_search_snapshot(acquisition)), now);
    auto interest =
        fetch->register_interest(peers, scope, demand, acquisition, plan);
    auto decision = fetch->select_sponsor(now);
    ASSERT_TRUE(decision.sponsor);
    auto occupying_grant = fetch->mint_grant(*decision.sponsor, now);
    ASSERT_TRUE(occupying_grant);
    ASSERT_TRUE(fetch->try_spend(
        occupying_grant,
        occupying_grant.action(),
        occupying_grant.target(),
        now));

    auto future = boost::asio::co_spawn(
        io,
        peers->co_drive_history_interest(
            fetch, acquisition, interest, PeerUseKind::ledger_header),
        boost::asio::use_future);
    boost::asio::steady_timer cancel(io, 50ms);
    cancel.async_wait([scope](boost::system::error_code ec) {
        if (!ec)
            scope->cancel();
    });
    while (future.wait_for(1ms) != std::future_status::ready)
    {
        io.poll();
        io.restart();
    }
    auto driven = future.get();
    ASSERT_TRUE(driven.local_terminal);
    EXPECT_EQ(*driven.local_terminal, HistoryAcquisitionTerminal::cancelled);
    EXPECT_LT(driven.replans, 10u)
        << "a bounded grant refusal must suspend for a receipt or terminal, "
           "not retry immediately";
    EXPECT_LE(driven.replans, 2u)
        << "capacity refusal must wait; cancellation may cause one local "
           "cleanup replan";
    EXPECT_LE(driven.replans_after_local_change, 1u);
    EXPECT_TRUE(
        fetch->acknowledge_work(occupying_grant, HistoryWorkReceipt::closed));
    EXPECT_EQ(fetch->snapshot().spent_unacknowledged, 0u);
}

TEST(PeerSetHistoryDriver, RetryExclusionReplansAwayFromExactEndpoint)
{
    boost::asio::io_context io;
    auto peers = PeerSet::create(io);
    auto first = std::make_shared<NodeBatchTestSession>("exclude-a.example:1");
    auto second =
        std::make_shared<NodeBatchTestSession>("exclude-b.example:1");
    first->attach_strand(io);
    second->attach_strand(io);
    PeerSetTestAccess::insert_ready_peer_with_generation(io, peers, first);
    PeerSetTestAccess::insert_ready_peer_with_generation(io, peers, second);

    auto scope = catl::core::RequestScope::create(
        std::chrono::steady_clock::now() + 2s);
    auto demand = run(
        io,
        peers->co_get_or_create_ledger_demand(
            scope, LedgerDemandOptions{.ledger_seq = 50}));
    auto acquisition = run(
        io,
        peers->co_open_history_acquisition(
            demand,
            HistoryAcquisitionOptions{
                .fetch = HistoryFetchId{45}, .fetch_epoch = 1}));

    EXPECT_TRUE(run(
        io,
        peers->co_exclude_history_endpoint(
            acquisition, first->endpoint())));
    auto snapshot = run(io, peers->co_history_search_snapshot(acquisition));
    EXPECT_TRUE(snapshot.excluded_endpoints.contains(first->endpoint()));
    auto proposal =
        plan_history_interest(snapshot, std::chrono::steady_clock::now());
    ASSERT_TRUE(proposal.next_action);
    EXPECT_EQ(proposal.next_action->target.endpoint, second->endpoint());
    run(
        io,
        peers->co_finish_history_acquisition(
            acquisition, HistoryAcquisitionTerminal::completed));
}

TEST(PeerSetHistoryDriver, SharedSponsorRevisionWakesCompetingPeerSet)
{
    boost::asio::io_context io;
    auto first_peers = PeerSet::create(io);
    auto second_peers = PeerSet::create(io);
    auto first_session =
        std::make_shared<NodeBatchTestSession>("sponsor-a.example:1");
    auto second_session =
        std::make_shared<NodeBatchTestSession>("sponsor-b.example:1");
    first_session->attach_strand(io);
    second_session->attach_strand(io);
    PeerSetTestAccess::insert_ready_peer_with_generation(
        io, first_peers, first_session);
    PeerSetTestAccess::insert_ready_peer_with_generation(
        io, second_peers, second_session);

    auto const deadline = std::chrono::steady_clock::now() + 2s;
    auto first_scope = catl::core::RequestScope::create(deadline);
    auto second_scope = catl::core::RequestScope::create(deadline);
    auto first_demand = run(
        io,
        first_peers->co_get_or_create_ledger_demand(
            first_scope, LedgerDemandOptions{.ledger_seq = 50}));
    auto second_demand = run(
        io,
        second_peers->co_get_or_create_ledger_demand(
            second_scope, LedgerDemandOptions{.ledger_seq = 50}));
    auto first_acquisition = run(
        io,
        first_peers->co_open_history_acquisition(
            first_demand,
            HistoryAcquisitionOptions{
                .fetch = HistoryFetchId{46}, .fetch_epoch = 1}));
    auto second_acquisition = run(
        io,
        second_peers->co_open_history_acquisition(
            second_demand,
            HistoryAcquisitionOptions{
                .fetch = HistoryFetchId{46}, .fetch_epoch = 1}));
    auto fetch = SharedHistoryFetch::create(HistoryFetchId{46}, 1);
    auto first_interest = fetch->register_interest(
        first_peers,
        first_scope,
        first_demand,
        first_acquisition,
        plan_history_interest(
            run(
                io,
                first_peers->co_history_search_snapshot(first_acquisition)),
            std::chrono::steady_clock::now()));
    auto second_interest = fetch->register_interest(
        second_peers,
        second_scope,
        second_demand,
        second_acquisition);

    auto future = boost::asio::co_spawn(
        io,
        second_peers->co_drive_history_interest(
            fetch,
            second_acquisition,
            second_interest,
            PeerUseKind::ledger_header),
        boost::asio::use_future);
    io.run_for(20ms);
    io.restart();
    EXPECT_EQ(future.wait_for(0ms), std::future_status::timeout)
        << "the earlier fair sponsor remains selected";

    auto passive = fetch->revise_interest(first_interest, {});
    ASSERT_TRUE(passive);
    while (future.wait_for(1ms) != std::future_status::ready)
    {
        io.poll();
        io.restart();
    }
    auto driven = future.get();
    ASSERT_TRUE(driven.acquired());
    EXPECT_EQ(driven.replans, 1u)
        << "a sibling publication changes sponsor arbitration, not this "
           "owner's local candidate plan";
    EXPECT_GE(driven.arbitrations, 2u);
    EXPECT_GE(driven.shared_wakes_reused, 1u);
    EXPECT_EQ(driven.peer_use.generation().endpoint(), second_session->endpoint());
    auto receipt = driven.peer_use.receipt();
    driven.peer_use.finish(PeerUseOutcome::success);
    EXPECT_EQ(run(io, receipt.wait()), PeerUseOutcome::success);

    (void)fetch->detach(*passive);
    (void)fetch->detach(driven.interest);
    run(
        io,
        first_peers->co_finish_history_acquisition(
            first_acquisition, HistoryAcquisitionTerminal::completed));
    run(
        io,
        second_peers->co_finish_history_acquisition(
            second_acquisition, HistoryAcquisitionTerminal::completed));
}

}  // namespace
