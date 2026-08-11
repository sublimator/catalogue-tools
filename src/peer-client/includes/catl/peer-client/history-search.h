#pragma once

#include "history-fetch.h"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <unordered_set>
#include <variant>
#include <vector>

namespace catl::peer_client {

inline constexpr std::size_t kHistorySourceLaneCount = 5;
inline constexpr std::size_t kMaxHistorySearchCandidates = 4096;
inline constexpr std::size_t kMaxHistorySearchWakes = 4096;

enum class HistorySourceLane : std::uint8_t {
    configured,
    persistent_success,
    direct_verified,
    independently_discovered,
    crawl_only,
};

enum class HistoryGenerationDisposition : std::uint8_t {
    absent,
    queued_or_connecting,
    ready_transient,
    admitted,
    draining,
};

/**
 * Immutable identity of the pool snapshot from which a plan was derived.
 *
 * Every scalar is owner-scoped.  The value carries no session, token row, or
 * mutation callback; PeerSet must match the complete tuple against its
 * canonical rows immediately before committing an action.
 */
struct HistoryControlRevision
{
    std::uint64_t peer_set_instance = 0;
    std::uint64_t pool_revision = 0;
    LedgerDemandId demand;
    AcquisitionId acquisition;
    HistoryFetchId fetch;
    std::uint64_t fetch_epoch = 0;
    HistoryInterestId interest;
    std::uint64_t participant_revision = 0;
    catl::core::RequestScopeId request_scope;

    explicit
    operator bool() const noexcept
    {
        return peer_set_instance != 0 && pool_revision != 0 && demand &&
            acquisition && fetch && interest && participant_revision != 0 &&
            request_scope;
    }

    bool
    operator==(HistoryControlRevision const&) const = default;
};

/** Inert name of one canonical parent/global budget-token row. */
struct HistoryBudgetGrantRef
{
    std::uint64_t owner_instance = 0;
    std::uint64_t token = 0;
    std::uint64_t revision = 0;

    explicit
    operator bool() const noexcept
    {
        return owner_instance != 0 && token != 0 && revision != 0;
    }

    bool
    operator==(HistoryBudgetGrantRef const&) const = default;
};

struct HistoryPhysicalCensus
{
    std::size_t queued_or_connecting = 0;
    std::size_t ready_transient = 0;
    std::size_t admitted = 0;
    std::size_t draining = 0;
    std::size_t max_admitted = 0;
    std::size_t max_transient = 0;
    std::size_t drain_reserve = 0;

    std::size_t
    transient() const noexcept
    {
        if (queued_or_connecting >
            std::numeric_limits<std::size_t>::max() - ready_transient)
            return std::numeric_limits<std::size_t>::max();
        return queued_or_connecting + ready_transient;
    }

    std::size_t
    total() const noexcept
    {
        auto value = transient();
        if (value > std::numeric_limits<std::size_t>::max() - admitted)
            return std::numeric_limits<std::size_t>::max();
        value += admitted;
        if (value > std::numeric_limits<std::size_t>::max() - draining)
            return std::numeric_limits<std::size_t>::max();
        return value + draining;
    }

    std::size_t
    max_physical() const noexcept
    {
        if (max_admitted >
            std::numeric_limits<std::size_t>::max() - max_transient)
            return std::numeric_limits<std::size_t>::max();
        auto value = max_admitted + max_transient;
        if (value > std::numeric_limits<std::size_t>::max() - drain_reserve)
            return std::numeric_limits<std::size_t>::max();
        return value + drain_reserve;
    }

    bool
    valid() const noexcept;

    bool
    can_create() const noexcept
    {
        return valid() && transient() < max_transient &&
            total() < max_physical();
    }

    bool
    can_promote() const noexcept
    {
        return valid() && admitted < max_admitted;
    }

    bool
    operator==(HistoryPhysicalCensus const&) const = default;
};

/** Authority-free candidate projection. */
struct HistorySearchCandidate
{
    HistoryAuthorityTarget target;
    HistorySourceLane source = HistorySourceLane::crawl_only;
    HistoryGenerationDisposition disposition =
        HistoryGenerationDisposition::absent;
    std::uint64_t fair_ticket = 0;
    bool eligible = false;
    bool advertises_ledger = false;
    bool attachable = false;
    bool attached = false;
    bool demand_sponsored = false;
    bool blind_eligible = false;
    std::optional<std::chrono::steady_clock::time_point> retry_not_before;

    bool
    operator==(HistorySearchCandidate const&) const = default;
};

enum class HistorySearchTerminalReason : std::uint8_t {
    cancelled,
    deadline_elapsed,
    exhausted,
    invalid_snapshot,
    control_changed,
};

struct HistorySearchSnapshot
{
    //@@authority state=prv.S0114 status=provisional scope=field
    // bind=catl::peer_client::HistorySearchSnapshot::control@e616023ac46add1c
    // owner=unresolved owner_hypothesis=peer_set
    // observed_home=history_search_snapshot
    // label=exact_authority_free_history_pool_and_participant_revision
    HistoryControlRevision control;
    std::chrono::steady_clock::time_point absolute_deadline{};
    std::chrono::steady_clock::time_point fallback_at{};
    bool cancelled = false;

    std::size_t initial_parent_dial_budget = 0;
    std::size_t remaining_parent_dials = 0;
    std::size_t crawl_only_probes = 0;
    std::array<std::size_t, kHistorySourceLaneCount> source_attempts{};

    std::optional<HistoryBudgetGrantRef> parent_dial_grant;
    std::optional<HistoryBudgetGrantRef> global_dial_grant;
    std::optional<HistoryBudgetGrantRef> parent_blind_grant;
    std::optional<HistoryBudgetGrantRef> global_blind_grant;

    HistoryPhysicalCensus physical;
    std::unordered_set<std::string> excluded_endpoints;
    std::vector<HistorySearchCandidate> candidates;
    std::vector<HistoryPlannedWake> wakes;
};

struct HistorySubmitKnownUsePlan
{
    HistoryControlRevision control;
    HistoryAuthorityGrantRef participant_grant;
    HistoryAuthorityTarget target;
};

struct HistorySubmitBlindUsePlan
{
    HistoryControlRevision control;
    HistoryAuthorityGrantRef participant_grant;
    HistoryAuthorityTarget target;
    HistoryBudgetGrantRef parent_blind_grant;
    HistoryBudgetGrantRef global_blind_grant;
};

struct HistoryAttachPlan
{
    HistoryControlRevision control;
    HistoryAuthorityGrantRef participant_grant;
    HistoryAuthorityTarget target;
    HistorySourceLane source = HistorySourceLane::crawl_only;
    std::optional<HistoryBudgetGrantRef> parent_probe_grant;
};

struct HistoryCreateGenerationPlan
{
    HistoryControlRevision control;
    HistoryAuthorityGrantRef participant_grant;
    HistoryAuthorityTarget target;
    HistorySourceLane source = HistorySourceLane::crawl_only;
    HistoryBudgetGrantRef parent_dial_grant;
    HistoryBudgetGrantRef global_dial_grant;
};

struct HistoryWaitPlan
{
    HistoryControlRevision control;
    HistoryPlannedWake wake;
};

struct HistoryTerminalPlan
{
    HistoryControlRevision control;
    HistorySearchTerminalReason reason = HistorySearchTerminalReason::exhausted;
};

using HistorySearchPlan = std::variant<
    HistorySubmitKnownUsePlan,
    HistorySubmitBlindUsePlan,
    HistoryAttachPlan,
    HistoryCreateGenerationPlan,
    HistoryWaitPlan,
    HistoryTerminalPlan>;

/**
 * Build the authority-free plan advertised to SharedHistoryFetch.
 *
 * This stage selects one exact action/target or one exact bounded wake.  It
 * does not require a participant revision because SharedHistoryFetch mints
 * that revision only when the proposal is installed. It never sees a live
 * session and cannot spend a parent/global token.
 */
HistoryParticipantPlan
plan_history_interest(
    HistorySearchSnapshot const& snapshot,
    std::chrono::steady_clock::time_point now);

/**
 * Bind a SharedHistoryFetch grant into the final inert owner-commit plan.
 *
 * The returned value still has no mutation authority. PeerSet must revalidate
 * the complete ControlRevision, participant grant, candidate state, deadlines,
 * census, and budget-token rows under participant-then-request arbitration.
 */
HistorySearchPlan
plan_history_acquisition(
    HistorySearchSnapshot const& snapshot,
    HistoryAuthorityGrantRef const& participant_grant,
    std::chrono::steady_clock::time_point now);

}  // namespace catl::peer_client
