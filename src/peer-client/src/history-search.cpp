#include <catl/peer-client/history-search.h>

#include <algorithm>
#include <functional>
#include <limits>
#include <string_view>
#include <tuple>

namespace catl::peer_client {
namespace {

using Clock = std::chrono::steady_clock;

std::size_t
lane_index(HistorySourceLane lane) noexcept
{
    return static_cast<std::size_t>(lane);
}

bool
valid_lane(HistorySourceLane lane) noexcept
{
    return lane_index(lane) < kHistorySourceLaneCount;
}

bool
ready(HistoryGenerationDisposition disposition) noexcept
{
    return disposition == HistoryGenerationDisposition::ready_transient ||
        disposition == HistoryGenerationDisposition::admitted;
}

bool
valid_wake(
    HistoryPlannedWake const& wake,
    Clock::time_point now,
    Clock::time_point deadline) noexcept
{
    if (!(now < wake.at) || wake.at > deadline)
        return false;
    switch (wake.kind)
    {
        case HistoryWakeSourceKind::attached_generation_deadline:
        case HistoryWakeSourceKind::physical_slot_release:
        case HistoryWakeSourceKind::physical_watchdog:
            return !wake.endpoint.empty() && wake.generation != 0;
        case HistoryWakeSourceKind::candidate_retry_expiry:
            return !wake.endpoint.empty();
        case HistoryWakeSourceKind::global_token_refill:
            return wake.token != 0;
    }
    return false;
}

bool
valid_candidate(HistorySearchCandidate const& candidate) noexcept
{
    if (candidate.target.endpoint.empty() || !valid_lane(candidate.source) ||
        candidate.fair_ticket == 0)
    {
        return false;
    }
    switch (candidate.disposition)
    {
        case HistoryGenerationDisposition::absent:
            return true;
        case HistoryGenerationDisposition::queued_or_connecting:
        case HistoryGenerationDisposition::ready_transient:
        case HistoryGenerationDisposition::admitted:
        case HistoryGenerationDisposition::draining:
            return candidate.target.generation != 0;
    }
    return false;
}

bool
valid_snapshot(HistorySearchSnapshot const& snapshot, bool require_control)
{
    if ((require_control && !snapshot.control) ||
        snapshot.absolute_deadline == Clock::time_point{} ||
        snapshot.candidates.size() > kMaxHistorySearchCandidates ||
        snapshot.wakes.size() > kMaxHistorySearchWakes ||
        !snapshot.physical.valid() ||
        snapshot.remaining_parent_dials > snapshot.initial_parent_dial_budget)
    {
        return false;
    }
    if (!std::all_of(
            snapshot.candidates.begin(),
            snapshot.candidates.end(),
            [](HistorySearchCandidate const& candidate) {
                return valid_candidate(candidate);
            }))
    {
        return false;
    }
    std::vector<std::string_view> endpoints;
    endpoints.reserve(snapshot.candidates.size());
    for (auto const& candidate : snapshot.candidates)
        endpoints.emplace_back(candidate.target.endpoint);
    std::ranges::sort(endpoints);
    return std::adjacent_find(endpoints.begin(), endpoints.end()) ==
        endpoints.end();
}

std::size_t
crawl_quota(std::size_t initial_parent_dial_budget) noexcept
{
    if (initial_parent_dial_budget == 0)
        return 0;
    return std::min(
        initial_parent_dial_budget,
        std::max<std::size_t>(1, initial_parent_dial_budget / 3));
}

bool
candidate_now(
    HistorySearchSnapshot const& snapshot,
    HistorySearchCandidate const& candidate,
    Clock::time_point now) noexcept
{
    return candidate.eligible &&
        !snapshot.excluded_endpoints.contains(candidate.target.endpoint) &&
        (!candidate.retry_not_before || *candidate.retry_not_before <= now);
}

auto
candidate_rank(
    HistorySearchSnapshot const& snapshot,
    HistorySearchCandidate const& candidate) noexcept
{
    auto const lane = lane_index(candidate.source);
    return std::tuple{
        snapshot.source_attempts[lane],
        lane,
        candidate.fair_ticket,
        std::string_view(candidate.target.endpoint),
        candidate.target.generation};
}

template <class Predicate>
HistorySearchCandidate const*
best_candidate(
    HistorySearchSnapshot const& snapshot,
    Clock::time_point now,
    Predicate&& predicate)
{
    HistorySearchCandidate const* best = nullptr;
    for (auto const& candidate : snapshot.candidates)
    {
        if (!candidate_now(snapshot, candidate, now) ||
            !std::invoke(predicate, candidate))
        {
            continue;
        }
        if (!best ||
            candidate_rank(snapshot, candidate) <
                candidate_rank(snapshot, *best))
        {
            best = &candidate;
        }
    }
    return best;
}

std::optional<HistoryPlannedWake>
earliest_wake(HistorySearchSnapshot const& snapshot, Clock::time_point now)
{
    std::optional<HistoryPlannedWake> best;
    auto const has_create_candidate = std::any_of(
        snapshot.candidates.begin(),
        snapshot.candidates.end(),
        [&snapshot](HistorySearchCandidate const& candidate) {
            return candidate.eligible &&
                candidate.disposition == HistoryGenerationDisposition::absent &&
                !snapshot.excluded_endpoints.contains(
                    candidate.target.endpoint);
        });
    auto const creation_can_eventually_commit = has_create_candidate &&
        snapshot.remaining_parent_dials != 0 &&
        snapshot.parent_dial_grant.has_value();
    auto const blind_can_eventually_commit =
        std::any_of(
            snapshot.candidates.begin(),
            snapshot.candidates.end(),
            [&snapshot](HistorySearchCandidate const& candidate) {
                return candidate.eligible && candidate.blind_eligible &&
                    ready(candidate.disposition) &&
                    !snapshot.excluded_endpoints.contains(
                        candidate.target.endpoint);
            }) &&
        snapshot.parent_blind_grant.has_value();
    auto relevant = [&](HistoryPlannedWake const& wake) {
        switch (wake.kind)
        {
            case HistoryWakeSourceKind::global_token_refill:
                return (creation_can_eventually_commit &&
                        !snapshot.global_dial_grant) ||
                    (blind_can_eventually_commit &&
                     !snapshot.global_blind_grant);
            case HistoryWakeSourceKind::physical_slot_release:
            case HistoryWakeSourceKind::physical_watchdog:
                return creation_can_eventually_commit &&
                    snapshot.global_dial_grant &&
                    !snapshot.physical.can_create();
            case HistoryWakeSourceKind::attached_generation_deadline:
                return std::any_of(
                    snapshot.candidates.begin(),
                    snapshot.candidates.end(),
                    [&wake,
                     &snapshot](HistorySearchCandidate const& candidate) {
                        return candidate.eligible && candidate.attached &&
                            candidate.target.endpoint == wake.endpoint &&
                            candidate.target.generation == wake.generation &&
                            !snapshot.excluded_endpoints.contains(
                                candidate.target.endpoint);
                    });
            case HistoryWakeSourceKind::candidate_retry_expiry:
                return std::any_of(
                    snapshot.candidates.begin(),
                    snapshot.candidates.end(),
                    [&wake,
                     &snapshot](HistorySearchCandidate const& candidate) {
                        return candidate.eligible &&
                            candidate.target.endpoint == wake.endpoint &&
                            !snapshot.excluded_endpoints.contains(
                                candidate.target.endpoint);
                    });
        }
        return false;
    };
    auto consider = [&](HistoryPlannedWake const& wake) {
        //@@mutation-site unrelated-wake-counts-as-history-progress
        if (!valid_wake(wake, now, snapshot.absolute_deadline) ||
            !relevant(wake))
            return;
        //@@mutation-end unrelated-wake-counts-as-history-progress
        auto const rank = std::tuple{
            wake.at,
            static_cast<std::uint8_t>(wake.kind),
            std::string_view(wake.endpoint),
            wake.generation,
            wake.token};
        if (!best)
        {
            best = wake;
            return;
        }
        auto const current = std::tuple{
            best->at,
            static_cast<std::uint8_t>(best->kind),
            std::string_view(best->endpoint),
            best->generation,
            best->token};
        if (rank < current)
            best = wake;
    };
    for (auto const& wake : snapshot.wakes)
        consider(wake);
    for (auto const& candidate : snapshot.candidates)
    {
        if (!candidate.eligible ||
            snapshot.excluded_endpoints.contains(candidate.target.endpoint) ||
            !candidate.retry_not_before)
        {
            continue;
        }
        consider(HistoryPlannedWake{
            .kind = HistoryWakeSourceKind::candidate_retry_expiry,
            .at = *candidate.retry_not_before,
            .endpoint = candidate.target.endpoint,
            .generation = candidate.target.generation});
    }
    return best;
}

HistoryParticipantPlan
terminal_interest(HistoryAcquisitionTerminal terminal)
{
    return HistoryParticipantPlan{.local_terminal = terminal};
}

HistorySearchTerminalReason
terminal_reason(HistorySearchSnapshot const& snapshot, Clock::time_point now)
{
    if (!valid_snapshot(snapshot, true))
        return HistorySearchTerminalReason::invalid_snapshot;
    if (snapshot.cancelled)
        return HistorySearchTerminalReason::cancelled;
    if (now >= snapshot.absolute_deadline)
        return HistorySearchTerminalReason::deadline_elapsed;
    return HistorySearchTerminalReason::exhausted;
}

HistoryTerminalPlan
terminal_plan(
    HistorySearchSnapshot const& snapshot,
    HistorySearchTerminalReason reason)
{
    return HistoryTerminalPlan{.control = snapshot.control, .reason = reason};
}

bool
grant_matches_control(
    HistoryControlRevision const& control,
    HistoryAuthorityGrantRef const& grant) noexcept
{
    return grant &&
        grant.participant_owner_instance() == control.peer_set_instance &&
        grant.demand() == control.demand &&
        grant.acquisition() == control.acquisition &&
        grant.fetch() == control.fetch &&
        grant.fetch_epoch() == control.fetch_epoch &&
        grant.interest() == control.interest &&
        grant.participant_revision() == control.participant_revision &&
        grant.request_scope() == control.request_scope;
}

}  // namespace

bool
HistoryPhysicalCensus::valid() const noexcept
{
    if (max_admitted == 0 || max_transient == 0 || drain_reserve == 0)
        return false;
    auto const transient_count = transient();
    auto const total_count = total();
    auto const total_limit = max_physical();
    return transient_count != std::numeric_limits<std::size_t>::max() &&
        total_count != std::numeric_limits<std::size_t>::max() &&
        total_limit != std::numeric_limits<std::size_t>::max() &&
        transient_count <= max_transient && admitted <= max_admitted &&
        total_count <= total_limit;
}

//@@authority transition=prv.T0286 status=provisional scope=function
// bind=catl::peer_client::plan_history_interest@6fc42641a3b5935a
// owner=unresolved owner_hypothesis=peer_set commit=pure
// touches=prv.S0109,prv.S0114
// label=derive_one_inert_exact_history_action_or_bounded_wake
HistoryParticipantPlan
plan_history_interest(
    HistorySearchSnapshot const& snapshot,
    Clock::time_point now)
{
    if (!valid_snapshot(snapshot, false))
        return terminal_interest(HistoryAcquisitionTerminal::exhausted);
    if (snapshot.cancelled)
        return terminal_interest(HistoryAcquisitionTerminal::cancelled);
    if (now >= snapshot.absolute_deadline)
        return terminal_interest(HistoryAcquisitionTerminal::deadline_elapsed);

    auto action = [&](HistoryAuthorityAction kind,
                      HistorySearchCandidate const& candidate) {
        return HistoryParticipantPlan{
            .next_action = HistoryPlannedAction{
                .action = kind, .target = candidate.target, .not_before = now}};
    };

    if (auto const* candidate = best_candidate(
            snapshot, now, [](HistorySearchCandidate const& current) {
                return current.advertises_ledger &&
                    (current.disposition ==
                         HistoryGenerationDisposition::admitted ||
                     (current.disposition ==
                          HistoryGenerationDisposition::ready_transient &&
                      current.attached));
            }))
    {
        return action(HistoryAuthorityAction::submit_known_use, *candidate);
    }

    if (auto const* candidate = best_candidate(
            snapshot, now, [&snapshot](HistorySearchCandidate const& current) {
                auto const has_probe_authority = current.demand_sponsored ||
                    (snapshot.remaining_parent_dials != 0 &&
                     snapshot.parent_dial_grant.has_value());
                auto const within_crawl_quota = current.demand_sponsored ||
                    current.source != HistorySourceLane::crawl_only ||
                    snapshot.crawl_only_probes <
                        crawl_quota(snapshot.initial_parent_dial_budget);
                return has_probe_authority && within_crawl_quota &&
                    current.attachable && !current.attached &&
                    (current.disposition ==
                         HistoryGenerationDisposition::queued_or_connecting ||
                     current.disposition ==
                         HistoryGenerationDisposition::ready_transient);
            }))
    {
        return action(HistoryAuthorityAction::attach_or_adopt, *candidate);
    }

    if (auto wake = earliest_wake(snapshot, now);
        wake &&
        wake->kind == HistoryWakeSourceKind::attached_generation_deadline)
    {
        return HistoryParticipantPlan{.wait_until = std::move(wake)};
    }

    if (now >= snapshot.fallback_at && snapshot.parent_blind_grant &&
        snapshot.global_blind_grant)
    {
        if (auto const* candidate = best_candidate(
                snapshot, now, [](HistorySearchCandidate const& current) {
                    return current.blind_eligible &&
                        (current.disposition ==
                             HistoryGenerationDisposition::admitted ||
                         (current.disposition ==
                              HistoryGenerationDisposition::ready_transient &&
                          current.attached));
                }))
        {
            return action(HistoryAuthorityAction::submit_blind_use, *candidate);
        }
    }

    if (snapshot.remaining_parent_dials != 0 && snapshot.parent_dial_grant &&
        snapshot.global_dial_grant && snapshot.physical.can_create())
    {
        auto const quota = crawl_quota(snapshot.initial_parent_dial_budget);
        auto create_eligible = [&](HistorySearchCandidate const& current) {
            return current.disposition ==
                HistoryGenerationDisposition::absent &&
                (current.source != HistorySourceLane::crawl_only ||
                 snapshot.crawl_only_probes < quota);
        };
        auto const non_crawl_exists = best_candidate(
            snapshot, now, [&](HistorySearchCandidate const& current) {
                return create_eligible(current) &&
                    current.source != HistorySourceLane::crawl_only;
            });
        auto const* candidate = best_candidate(
            snapshot, now, [&](HistorySearchCandidate const& current) {
                //@@mutation-site crawl-only-lane-monopolizes-bounded-budget
                if (!create_eligible(current))
                    return false;
                return snapshot.remaining_parent_dials != 1 ||
                    !non_crawl_exists ||
                    current.source != HistorySourceLane::crawl_only;
                //@@mutation-end crawl-only-lane-monopolizes-bounded-budget
            });
        if (candidate)
            return action(
                HistoryAuthorityAction::create_generation, *candidate);
    }

    if (auto wake = earliest_wake(snapshot, now))
        return HistoryParticipantPlan{.wait_until = std::move(wake)};

    return terminal_interest(HistoryAcquisitionTerminal::exhausted);
}

//@@authority transition=prv.T0287 status=provisional scope=function
// bind=catl::peer_client::plan_history_acquisition@dfcabb67a4fb16c6
// owner=unresolved owner_hypothesis=peer_set commit=pure
// touches=prv.S0110,prv.S0114
// label=bind_exact_participant_grant_to_inert_owner_commit_plan
HistorySearchPlan
plan_history_acquisition(
    HistorySearchSnapshot const& snapshot,
    HistoryAuthorityGrantRef const& participant_grant,
    Clock::time_point now)
{
    if (!valid_snapshot(snapshot, true))
    {
        return terminal_plan(
            snapshot, HistorySearchTerminalReason::invalid_snapshot);
    }
    auto const proposal = plan_history_interest(snapshot, now);
    if (proposal.wait_until)
    {
        return HistoryWaitPlan{
            .control = snapshot.control, .wake = *proposal.wait_until};
    }
    if (proposal.local_terminal)
        return terminal_plan(snapshot, terminal_reason(snapshot, now));
    if (!proposal.next_action ||
        !grant_matches_control(snapshot.control, participant_grant) ||
        //@@mutation-site history-plan-retargets-exact-participant-grant
        participant_grant.action() != proposal.next_action->action ||
        !(participant_grant.target() == proposal.next_action->target) ||
        //@@mutation-end history-plan-retargets-exact-participant-grant
        participant_grant.deadline() != snapshot.absolute_deadline ||
        participant_grant.not_before() > now)
    {
        return terminal_plan(
            snapshot, HistorySearchTerminalReason::control_changed);
    }

    switch (proposal.next_action->action)
    {
        case HistoryAuthorityAction::submit_known_use:
            return HistorySubmitKnownUsePlan{
                .control = snapshot.control,
                .participant_grant = participant_grant,
                .target = proposal.next_action->target};
        case HistoryAuthorityAction::submit_blind_use:
            if (!snapshot.parent_blind_grant || !snapshot.global_blind_grant)
            {
                return terminal_plan(
                    snapshot, HistorySearchTerminalReason::control_changed);
            }
            return HistorySubmitBlindUsePlan{
                .control = snapshot.control,
                .participant_grant = participant_grant,
                .target = proposal.next_action->target,
                .parent_blind_grant = *snapshot.parent_blind_grant,
                .global_blind_grant = *snapshot.global_blind_grant};
        case HistoryAuthorityAction::attach_or_adopt: {
            auto const found = std::find_if(
                snapshot.candidates.begin(),
                snapshot.candidates.end(),
                [&](HistorySearchCandidate const& candidate) {
                    return candidate.target == proposal.next_action->target;
                });
            if (found == snapshot.candidates.end() ||
                (!found->demand_sponsored && !snapshot.parent_dial_grant))
            {
                return terminal_plan(
                    snapshot, HistorySearchTerminalReason::control_changed);
            }
            return HistoryAttachPlan{
                .control = snapshot.control,
                .participant_grant = participant_grant,
                .target = proposal.next_action->target,
                .source = found->source,
                .parent_probe_grant = found->demand_sponsored
                    ? std::nullopt
                    : snapshot.parent_dial_grant};
        }
        case HistoryAuthorityAction::create_generation: {
            if (!snapshot.parent_dial_grant || !snapshot.global_dial_grant)
            {
                return terminal_plan(
                    snapshot, HistorySearchTerminalReason::control_changed);
            }
            auto const found = std::find_if(
                snapshot.candidates.begin(),
                snapshot.candidates.end(),
                [&](HistorySearchCandidate const& candidate) {
                    return candidate.target == proposal.next_action->target;
                });
            if (found == snapshot.candidates.end())
            {
                return terminal_plan(
                    snapshot, HistorySearchTerminalReason::control_changed);
            }
            return HistoryCreateGenerationPlan{
                .control = snapshot.control,
                .participant_grant = participant_grant,
                .target = proposal.next_action->target,
                .source = found->source,
                .parent_dial_grant = *snapshot.parent_dial_grant,
                .global_dial_grant = *snapshot.global_dial_grant};
        }
        case HistoryAuthorityAction::promote:
            break;
    }
    return terminal_plan(
        snapshot, HistorySearchTerminalReason::control_changed);
}

}  // namespace catl::peer_client
