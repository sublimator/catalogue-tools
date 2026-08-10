#pragma once

#include <catl/peer-client/history-demand.h>

#include <atomic>
#include <chrono>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace catl::peer_client {

class PeerSet;

enum class HistoryAuthorityAction : std::uint8_t {
    attach_or_adopt,
    create_generation,
    submit_known_use,
    submit_blind_use,
    promote,
};

enum class HistoryWakeSourceKind : std::uint8_t {
    attached_generation_deadline,
    candidate_retry_expiry,
    global_token_refill,
    physical_slot_release,
    physical_watchdog,
};

/** Exact, generation-aware target of one authority-consuming action. */
struct HistoryAuthorityTarget
{
    std::string endpoint;
    std::uint64_t generation = 0;
    std::uint64_t use = 0;
    std::string pool;

    bool
    operator==(HistoryAuthorityTarget const&) const = default;
};

struct HistoryPlannedAction
{
    HistoryAuthorityAction action = HistoryAuthorityAction::attach_or_adopt;
    HistoryAuthorityTarget target;
    std::chrono::steady_clock::time_point not_before{};

    bool
    operator==(HistoryPlannedAction const&) const = default;
};

/** Exact bounded event which can make an otherwise inert interest viable. */
struct HistoryPlannedWake
{
    HistoryWakeSourceKind kind = HistoryWakeSourceKind::candidate_retry_expiry;
    std::chrono::steady_clock::time_point at{};
    std::string endpoint;
    std::uint64_t generation = 0;
    std::uint64_t token = 0;

    bool
    operator==(HistoryPlannedWake const&) const = default;
};

struct HistoryParticipantPlan
{
    std::optional<HistoryPlannedAction> next_action;
    std::optional<HistoryPlannedWake> wait_until;
    std::optional<HistoryAcquisitionTerminal> local_terminal;
};

enum class SharedHistoryFetchTerminal : std::uint8_t {
    completed,
    no_viable_participant,
    no_interests,
    replaced,
    shutdown,
};

enum class HistoryGrantState : std::uint8_t {
    active,
    spent,
    revoked,
};

enum class HistoryWorkReceipt : std::uint8_t {
    closed,
    adopted,
    service_owned,
};

class SharedHistoryFetch;

/** Inert identity for one revision of one shared-fetch participant. */
class HistoryInterestRef
{
public:
    HistoryInterestRef() = default;

    explicit
    operator bool() const noexcept
    {
        return owner_instance_ != 0 && participant_owner_instance_ != 0 &&
            fetch_ && interest_ && demand_ && acquisition_ && request_scope_ &&
            participant_revision_ != 0;
    }

    HistoryFetchId
    fetch() const noexcept
    {
        return fetch_;
    }

    std::uint64_t
    fetch_epoch() const noexcept
    {
        return fetch_epoch_;
    }

    HistoryInterestId
    interest() const noexcept
    {
        return interest_;
    }

    std::uint64_t
    participant_revision() const noexcept
    {
        return participant_revision_;
    }

    LedgerDemandId
    demand() const noexcept
    {
        return demand_;
    }

    AcquisitionId
    acquisition() const noexcept
    {
        return acquisition_;
    }

    catl::core::RequestScopeId
    request_scope() const noexcept
    {
        return request_scope_;
    }

    std::uint64_t
    participant_owner_instance() const noexcept
    {
        return participant_owner_instance_;
    }

    std::chrono::steady_clock::time_point
    deadline() const noexcept
    {
        return deadline_;
    }

private:
    friend class SharedHistoryFetch;
    friend class HistoryAuthorityGrantRef;

    HistoryInterestRef(
        std::uint64_t owner_instance,
        std::uint64_t participant_owner_instance,
        HistoryFetchId fetch,
        std::uint64_t fetch_epoch,
        HistoryInterestId interest,
        std::uint64_t participant_revision,
        LedgerDemandId demand,
        AcquisitionId acquisition,
        catl::core::RequestScopeId request_scope,
        std::chrono::steady_clock::time_point deadline)
        : owner_instance_(owner_instance)
        , participant_owner_instance_(participant_owner_instance)
        , fetch_(fetch)
        , fetch_epoch_(fetch_epoch)
        , interest_(interest)
        , participant_revision_(participant_revision)
        , demand_(demand)
        , acquisition_(acquisition)
        , request_scope_(request_scope)
        , deadline_(deadline)
    {
    }

    std::uint64_t owner_instance_ = 0;
    std::uint64_t participant_owner_instance_ = 0;
    HistoryFetchId fetch_;
    std::uint64_t fetch_epoch_ = 0;
    HistoryInterestId interest_;
    std::uint64_t participant_revision_ = 0;
    LedgerDemandId demand_;
    AcquisitionId acquisition_;
    catl::core::RequestScopeId request_scope_;
    std::chrono::steady_clock::time_point deadline_{};
};

/** Inert exact action/target grant. It cannot itself create pool work. */
class HistoryAuthorityGrantRef
{
public:
    HistoryAuthorityGrantRef() = default;

    explicit
    operator bool() const noexcept
    {
        return owner_instance_ != 0 && participant_owner_instance_ != 0 &&
            fetch_ && interest_ && grant_ && demand_ && acquisition_ &&
            request_scope_ && participant_revision_ != 0;
    }

    HistoryGrantId
    grant() const noexcept
    {
        return grant_;
    }

    HistoryFetchId
    fetch() const noexcept
    {
        return fetch_;
    }

    std::uint64_t
    fetch_epoch() const noexcept
    {
        return fetch_epoch_;
    }

    std::uint64_t
    participant_owner_instance() const noexcept
    {
        return participant_owner_instance_;
    }

    HistoryInterestId
    interest() const noexcept
    {
        return interest_;
    }

    LedgerDemandId
    demand() const noexcept
    {
        return demand_;
    }

    AcquisitionId
    acquisition() const noexcept
    {
        return acquisition_;
    }

    catl::core::RequestScopeId
    request_scope() const noexcept
    {
        return request_scope_;
    }

    std::chrono::steady_clock::time_point
    deadline() const noexcept
    {
        return deadline_;
    }

    std::uint64_t
    participant_revision() const noexcept
    {
        return participant_revision_;
    }

    HistoryAuthorityAction
    action() const noexcept
    {
        return action_;
    }

    HistoryAuthorityTarget const&
    target() const noexcept
    {
        return target_;
    }

    std::chrono::steady_clock::time_point
    not_before() const noexcept
    {
        return not_before_;
    }

private:
    friend class SharedHistoryFetch;

    HistoryAuthorityGrantRef(
        HistoryInterestRef const& interest,
        HistoryGrantId grant,
        HistoryAuthorityAction action,
        HistoryAuthorityTarget target,
        std::chrono::steady_clock::time_point not_before)
        : owner_instance_(interest.owner_instance_)
        , participant_owner_instance_(interest.participant_owner_instance_)
        , fetch_(interest.fetch_)
        , fetch_epoch_(interest.fetch_epoch_)
        , interest_(interest.interest_)
        , participant_revision_(interest.participant_revision_)
        , demand_(interest.demand_)
        , acquisition_(interest.acquisition_)
        , request_scope_(interest.request_scope_)
        , deadline_(interest.deadline_)
        , grant_(grant)
        , action_(action)
        , target_(std::move(target))
        , not_before_(not_before)
    {
    }

    std::uint64_t owner_instance_ = 0;
    std::uint64_t participant_owner_instance_ = 0;
    HistoryFetchId fetch_;
    std::uint64_t fetch_epoch_ = 0;
    HistoryInterestId interest_;
    std::uint64_t participant_revision_ = 0;
    LedgerDemandId demand_;
    AcquisitionId acquisition_;
    catl::core::RequestScopeId request_scope_;
    std::chrono::steady_clock::time_point deadline_{};
    HistoryGrantId grant_;
    HistoryAuthorityAction action_ = HistoryAuthorityAction::attach_or_adopt;
    HistoryAuthorityTarget target_;
    std::chrono::steady_clock::time_point not_before_{};
};

struct HistorySponsorDecision
{
    std::optional<HistoryInterestRef> sponsor;
    std::optional<std::chrono::steady_clock::time_point> aggregate_wake;
    std::size_t live_participants = 0;
    std::size_t viable_participants = 0;
};

struct HistoryGrantSpendReceipt
{
    HistoryGrantId grant;
    catl::core::RequestCommitId request_commit;
};

struct HistoryDetachReceipt
{
    bool accepted = false;
    bool acknowledged = false;
    bool terminalized_fetch = false;
    std::size_t pending_work = 0;
};

struct SharedHistoryFetchSnapshot
{
    HistoryFetchId fetch;
    std::uint64_t fetch_epoch = 0;
    std::optional<SharedHistoryFetchTerminal> terminal;
    std::uint64_t publication_revision = 0;
    std::size_t interests = 0;
    std::size_t active_interests = 0;
    std::size_t passive_interests = 0;
    std::size_t active_grants = 0;
    std::size_t spent_unacknowledged = 0;
};

/**
 * Non-dialling shared-fetch participant and grant authority.
 *
 * The fetch owner registers/revises/detaches interests. A participant cell is
 * retained independently so a grant spend can serialize with detach. Spending
 * acquires that cell first and then RequestScope::try_commit(); the committed
 * state flip only marks an exact grant spent and cannot emit network work.
 */
class SharedHistoryFetch
    : public std::enable_shared_from_this<SharedHistoryFetch>
{
public:
    using Clock = std::chrono::steady_clock;

    struct Limits
    {
        std::size_t max_interests = 512;
        std::size_t max_grants_per_interest = 32;
    };

    static std::shared_ptr<SharedHistoryFetch>
    create(HistoryFetchId fetch, std::uint64_t fetch_epoch);

    static std::shared_ptr<SharedHistoryFetch>
    create(HistoryFetchId fetch, std::uint64_t fetch_epoch, Limits limits);

    HistoryInterestRef
    register_interest(
        std::shared_ptr<PeerSet> participant_owner,
        std::shared_ptr<catl::core::RequestScope> request_scope,
        LedgerDemandRef const& demand,
        HistoryAcquisitionRef const& acquisition,
        HistoryParticipantPlan plan = {});

    std::optional<HistoryInterestRef>
    revise_interest(
        HistoryInterestRef const& interest,
        HistoryParticipantPlan plan,
        Clock::time_point now = Clock::now());

    HistorySponsorDecision
    select_sponsor(Clock::time_point now = Clock::now()) const noexcept;

    HistoryAuthorityGrantRef
    mint_grant(
        HistoryInterestRef const& selected,
        Clock::time_point now = Clock::now());

    std::optional<HistoryGrantSpendReceipt>
    try_spend(
        HistoryAuthorityGrantRef const& grant,
        HistoryAuthorityAction action,
        HistoryAuthorityTarget const& target,
        Clock::time_point now = Clock::now()) noexcept;

    HistoryDetachReceipt
    detach(HistoryInterestRef const& interest) noexcept;

    bool
    acknowledge_work(
        HistoryAuthorityGrantRef const& grant,
        HistoryWorkReceipt receipt,
        std::optional<HistoryAuthorityGrantRef> const& adopter = {}) noexcept;

    bool
    terminalize_if_no_viable_action(Clock::time_point now = Clock::now());

    bool
    publish_terminal(SharedHistoryFetchTerminal terminal) noexcept;

    SharedHistoryFetchSnapshot
    snapshot() const noexcept;

    std::optional<HistoryGrantState>
    grant_state(HistoryAuthorityGrantRef const& grant) const noexcept;

private:
    friend class PeerSet;

    using OwnerSpendCommit =
        void (*)(void*, catl::core::RequestCommitId) noexcept;

    std::optional<HistoryGrantSpendReceipt>
    try_spend_with_owner_commit(
        HistoryAuthorityGrantRef const& grant,
        HistoryAuthorityAction action,
        HistoryAuthorityTarget const& target,
        Clock::time_point now,
        void* owner_context,
        OwnerSpendCommit owner_commit) noexcept;

    enum class ParticipantState : std::uint8_t { active, revoked };

    struct GrantRecord
    {
        HistoryGrantId id;
        std::uint64_t participant_revision = 0;
        HistoryAuthorityAction action = HistoryAuthorityAction::attach_or_adopt;
        HistoryAuthorityTarget target;
        Clock::time_point not_before{};
        HistoryGrantState state = HistoryGrantState::active;
        catl::core::RequestCommitId request_commit;
        std::optional<HistoryWorkReceipt> work_receipt;
        bool adoption_claimed = false;
    };

    struct ParticipantCell
    {
        mutable std::mutex mutex;
        HistoryInterestId interest;
        std::uint64_t revision = 0;
        std::uint64_t participant_owner_instance = 0;
        LedgerDemandId demand;
        AcquisitionId acquisition;
        catl::core::RequestScopeId request_scope_id;
        Clock::time_point deadline{};
        std::uint64_t fair_ticket = 0;
        ParticipantState state = ParticipantState::active;
        //@@authority state=prv.S0109 status=provisional scope=field
        // bind=catl::peer_client::SharedHistoryFetch::ParticipantCell::plan@7532114195b7dfd6
        // owner=unresolved owner_hypothesis=shared_history_fetch
        // observed_home=shared_history_fetch
        // label=revisioned_participant_viability_plan_and_fair_ticket
        HistoryParticipantPlan plan;
        //@@authority state=prv.S0110 status=provisional scope=field
        // bind=catl::peer_client::SharedHistoryFetch::ParticipantCell::grants@d825ef6c89eb922c
        // owner=unresolved owner_hypothesis=shared_history_fetch
        // observed_home=participant_grant_arbiter
        // label=exact_one_shot_action_target_grant_registry
        std::map<HistoryGrantId, GrantRecord> grants;
        //@@authority state=prv.S0111 status=provisional scope=field
        // bind=catl::peer_client::SharedHistoryFetch::ParticipantCell::outstanding_work@d45698f3d4909e39
        // owner=unresolved owner_hypothesis=shared_history_fetch
        // observed_home=participant_grant_arbiter
        // label=spent_physical_work_awaiting_exact_close_or_adopter_receipt
        std::size_t outstanding_work = 0;
        std::atomic<bool> request_terminal{false};
        // Subscription is declared after the strong scope so it is destroyed
        // first and cannot call back into a partially destroyed cell.
        std::shared_ptr<catl::core::RequestScope> request_scope;
        //@@authority state=prv.S0113 status=provisional scope=field
        // bind=catl::peer_client::SharedHistoryFetch::ParticipantCell::participant_owner@5259a4e20b978add
        // owner=unresolved owner_hypothesis=shared_history_fetch
        // observed_home=shared_history_fetch
        // label=retained_exact_participant_peer_set_owner_capability
        std::shared_ptr<PeerSet> participant_owner;
        catl::core::RequestScope::Subscription terminal_subscription;
    };

    struct ReleasedAuthority
    {
        catl::core::RequestScope::Subscription subscription;
        std::shared_ptr<catl::core::RequestScope> scope;
        std::shared_ptr<PeerSet> participant_owner;
    };

    SharedHistoryFetch(
        std::uint64_t owner_instance,
        HistoryFetchId fetch,
        std::uint64_t fetch_epoch,
        Limits limits)
        : owner_instance_(owner_instance)
        , fetch_(fetch)
        , fetch_epoch_(fetch_epoch)
        , limits_(limits)
    {
    }

    bool
    authentic(HistoryInterestRef const& interest) const noexcept;

    bool
    authentic(HistoryAuthorityGrantRef const& grant) const noexcept;

    HistoryInterestRef
    make_ref(ParticipantCell const& cell) const noexcept;

    std::shared_ptr<ParticipantCell>
    find_cell(HistoryInterestId interest) const noexcept;

    static void
    release_authority(ReleasedAuthority authority) noexcept;

    static bool
    receipt_matches_action(
        HistoryAuthorityAction action,
        HistoryWorkReceipt receipt) noexcept;

    void
    terminalize_if_detached_and_drained() noexcept;

    void
    release_terminal_authorities() noexcept;

    /** Wake every retained participant owner after shared arbitration moves. */
    void
    wake_participant_owners(
        std::uint64_t excluded_owner_instance = 0) noexcept;

    //@@authority state=prv.S0107 status=provisional scope=field
    // bind=catl::peer_client::SharedHistoryFetch::owner_instance_@2dc3c171a1e5f53c
    // owner=unresolved owner_hypothesis=shared_history_fetch
    // observed_home=shared_history_fetch
    // label=immutable_process_unique_shared_fetch_incarnation
    std::uint64_t const owner_instance_;
    HistoryFetchId const fetch_;
    std::uint64_t const fetch_epoch_;
    Limits const limits_;

    mutable std::mutex mutex_;
    //@@authority state=prv.S0108 status=provisional scope=field
    // bind=catl::peer_client::SharedHistoryFetch::interests_@eb0dfb8890d83993
    // owner=unresolved owner_hypothesis=shared_history_fetch
    // observed_home=shared_history_fetch
    // label=bounded_revisioned_participant_registry
    std::map<HistoryInterestId, std::shared_ptr<ParticipantCell>> interests_;
    //@@authority state=prv.S0112 status=provisional scope=field
    // bind=catl::peer_client::SharedHistoryFetch::terminal_@4f7d406e18b38084
    // owner=unresolved owner_hypothesis=shared_history_fetch
    // observed_home=shared_history_fetch
    // label=sticky_exact_fetch_epoch_terminal_publication
    std::optional<SharedHistoryFetchTerminal> terminal_;
    std::uint64_t publication_revision_ = 0;
    std::uint64_t next_interest_id_ = 1;
    std::uint64_t next_participant_revision_ = 1;
    std::uint64_t next_grant_id_ = 1;
    std::uint64_t next_fair_ticket_ = 1;
};

}  // namespace catl::peer_client
