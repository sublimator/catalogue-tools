#include <catl/peer-client/history-fetch.h>

#include <catl/peer-client/peer-client-coro.h>
#include <catl/peer-client/peer-set.h>

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

namespace catl::peer_client {
namespace {

std::atomic<std::uint64_t> next_shared_fetch_owner{1};

std::optional<std::uint64_t>
mint_owner_instance() noexcept
{
    auto current = next_shared_fetch_owner.load(std::memory_order_relaxed);
    for (;;)
    {
        if (current == 0 ||
            current == std::numeric_limits<std::uint64_t>::max())
            return std::nullopt;
        if (next_shared_fetch_owner.compare_exchange_weak(
                current,
                current + 1,
                std::memory_order_relaxed,
                std::memory_order_relaxed))
        {
            return current;
        }
    }
}

bool
valid_action(HistoryAuthorityAction action) noexcept
{
    switch (action)
    {
        case HistoryAuthorityAction::attach_or_adopt:
        case HistoryAuthorityAction::create_generation:
        case HistoryAuthorityAction::submit_known_use:
        case HistoryAuthorityAction::submit_blind_use:
        case HistoryAuthorityAction::promote:
            return true;
    }
    return false;
}

bool
valid_acquisition_terminal(HistoryAcquisitionTerminal terminal) noexcept
{
    switch (terminal)
    {
        case HistoryAcquisitionTerminal::completed:
        case HistoryAcquisitionTerminal::cancelled:
        case HistoryAcquisitionTerminal::deadline_elapsed:
        case HistoryAcquisitionTerminal::exhausted:
        case HistoryAcquisitionTerminal::fetch_terminal:
        case HistoryAcquisitionTerminal::shutdown:
            return true;
    }
    return false;
}

bool
valid_fetch_terminal(SharedHistoryFetchTerminal terminal) noexcept
{
    switch (terminal)
    {
        case SharedHistoryFetchTerminal::completed:
        case SharedHistoryFetchTerminal::no_viable_participant:
        case SharedHistoryFetchTerminal::no_interests:
        case SharedHistoryFetchTerminal::replaced:
        case SharedHistoryFetchTerminal::shutdown:
            return true;
    }
    return false;
}

bool
valid_plan(
    HistoryParticipantPlan const& plan,
    std::chrono::steady_clock::time_point deadline,
    std::chrono::steady_clock::time_point now) noexcept
{
    auto const alternatives =
        static_cast<unsigned>(plan.next_action.has_value()) +
        static_cast<unsigned>(plan.wait_until.has_value()) +
        static_cast<unsigned>(plan.local_terminal.has_value());
    return alternatives <= 1 &&
        (!plan.next_action ||
         (valid_action(plan.next_action->action) &&
          plan.next_action->not_before < deadline)) &&
        (!plan.wait_until ||
         (now < plan.wait_until->at && plan.wait_until->at <= deadline)) &&
        (!plan.local_terminal ||
         valid_acquisition_terminal(*plan.local_terminal));
}

}  // namespace

std::shared_ptr<SharedHistoryFetch>
SharedHistoryFetch::create(HistoryFetchId fetch, std::uint64_t fetch_epoch)
{
    return create(fetch, fetch_epoch, Limits{});
}

std::shared_ptr<SharedHistoryFetch>
//@@authority transition=prv.T0268 status=provisional scope=function
// bind=catl::peer_client::create@d3e58e700efe404b
// owner=unresolved owner_hypothesis=shared_history_fetch_factory
// commit=process_atomic touches=prv.S0107
// label=mint_nonreused_shared_fetch_incarnation
SharedHistoryFetch::create(
    HistoryFetchId fetch,
    std::uint64_t fetch_epoch,
    Limits limits)
{
    auto const hard_limits = Limits{};
    if (!fetch || limits.max_interests == 0 ||
        limits.max_interests > hard_limits.max_interests ||
        limits.max_grants_per_interest == 0 ||
        limits.max_grants_per_interest > hard_limits.max_grants_per_interest)
    {
        return nullptr;
    }
    auto const owner = mint_owner_instance();
    if (!owner)
        return nullptr;
    return std::shared_ptr<SharedHistoryFetch>(
        new SharedHistoryFetch(*owner, fetch, fetch_epoch, limits));
}

bool
SharedHistoryFetch::authentic(HistoryInterestRef const& interest) const noexcept
{
    return interest && interest.owner_instance_ == owner_instance_ &&
        interest.fetch_ == fetch_ && interest.fetch_epoch_ == fetch_epoch_;
}

bool
SharedHistoryFetch::authentic(
    HistoryAuthorityGrantRef const& grant) const noexcept
{
    return grant && grant.owner_instance_ == owner_instance_ &&
        grant.fetch_ == fetch_ && grant.fetch_epoch_ == fetch_epoch_;
}

HistoryInterestRef
SharedHistoryFetch::make_ref(ParticipantCell const& cell) const noexcept
{
    return HistoryInterestRef{
        owner_instance_,
        cell.participant_owner_instance,
        fetch_,
        fetch_epoch_,
        cell.interest,
        cell.revision,
        cell.demand,
        cell.acquisition,
        cell.request_scope_id,
        cell.deadline};
}

std::shared_ptr<SharedHistoryFetch::ParticipantCell>
SharedHistoryFetch::find_cell(HistoryInterestId interest) const noexcept
{
    std::lock_guard lock(mutex_);
    auto const found = interests_.find(interest);
    return found == interests_.end() ? nullptr : found->second;
}

void
SharedHistoryFetch::release_authority(ReleasedAuthority authority) noexcept
{
    if (authority.participant_owner)
        authority.participant_owner->post_history_fetch_wake();
    authority.subscription.reset();
    authority.scope.reset();
    authority.participant_owner.reset();
}

void
SharedHistoryFetch::wake_participant_owners(
    std::uint64_t excluded_owner_instance) noexcept
{
    try
    {
        std::lock_guard owner_lock(mutex_);
        for (auto const& [_, cell] : interests_)
        {
            std::lock_guard participant_lock(cell->mutex);
            if (cell->participant_owner &&
                cell->participant_owner_instance != excluded_owner_instance)
            {
                cell->participant_owner->post_history_fetch_wake();
            }
        }
    }
    catch (...)
    {
        // Wakes are advisory projections of already committed shared state.
        // Exact deadlines remain the fail-closed fallback if posting fails.
    }
}

HistoryInterestRef
//@@authority transition=prv.T0269 status=provisional scope=function
// bind=catl::peer_client::register_interest@55e348b95a946322
// owner=unresolved owner_hypothesis=shared_history_fetch commit=owner_mutex
// touches=prv.S0103,prv.S0104,prv.S0105,prv.S0108,prv.S0109,prv.S0113
// label=register_exact_owner_scoped_participant_interest
SharedHistoryFetch::register_interest(
    std::shared_ptr<PeerSet> participant_owner,
    std::shared_ptr<catl::core::RequestScope> request_scope,
    LedgerDemandRef const& demand,
    HistoryAcquisitionRef const& acquisition,
    HistoryParticipantPlan plan)
{
    if (!participant_owner || !request_scope || !demand || !acquisition ||
        participant_owner->peer_set_instance_id_ == 0 ||
        demand.peer_set_instance_ != participant_owner->peer_set_instance_id_ ||
        acquisition.peer_set_instance_ !=
            participant_owner->peer_set_instance_id_ ||
        request_scope->id() != demand.request_scope_id() ||
        request_scope->id() != acquisition.request_scope_id() ||
        demand.id() != acquisition.demand_id() ||
        demand.ledger_seq() != acquisition.context().ledger_seq ||
        acquisition.context().fetch != fetch_ ||
        acquisition.context().fetch_epoch != fetch_epoch_ ||
        request_scope->absolute_deadline() != demand.absolute_deadline() ||
        request_scope->absolute_deadline() != acquisition.absolute_deadline())
    {
        throw PeerClientException(
            Error::BadRequest, "shared-fetch interest binding mismatch");
    }

    auto const request = request_scope->snapshot();
    if (request.terminal)
        throw PeerClientException(
            *request.terminal ==
                    catl::core::RequestScopeTerminal::deadline_elapsed
                ? Error::Timeout
                : Error::Cancelled,
            "shared-fetch request is terminal");
    auto const now = Clock::now();
    if (!valid_plan(plan, request.absolute_deadline, now))
    {
        throw PeerClientException(
            Error::BadRequest,
            "shared-fetch plan conflicts or reaches request deadline");
    }

    auto cell = std::make_shared<ParticipantCell>();
    std::weak_ptr<ParticipantCell> weak = cell;
    cell->terminal_subscription = request_scope->subscribe(
        [weak](catl::core::RequestScopeTerminal) noexcept {
            if (auto current = weak.lock())
                current->request_terminal.store(
                    true, std::memory_order_release);
        });
    auto const participant_owner_instance =
        participant_owner->peer_set_instance_id_;
    {
        std::lock_guard lock(mutex_);
        if (terminal_)
            throw PeerClientException(
                Error::Cancelled, "shared fetch is terminal");
        if (cell->request_terminal.load(std::memory_order_acquire))
            throw PeerClientException(
                Error::Cancelled, "shared-fetch request became terminal");
        auto const duplicate = std::find_if(
            interests_.begin(),
            interests_.end(),
            [&acquisition, participant_owner_instance](auto const& row) {
                std::lock_guard participant_lock(row.second->mutex);
                return row.second->participant_owner_instance ==
                    participant_owner_instance &&
                    row.second->acquisition ==
                    acquisition.context().acquisition;
            });
        if (duplicate != interests_.end())
        {
            throw PeerClientException(
                Error::DuplicateRequest,
                "shared-fetch acquisition is already registered");
        }
        if (interests_.size() >= limits_.max_interests ||
            next_interest_id_ == std::numeric_limits<std::uint64_t>::max() ||
            next_participant_revision_ ==
                std::numeric_limits<std::uint64_t>::max() ||
            next_fair_ticket_ == std::numeric_limits<std::uint64_t>::max())
        {
            throw PeerClientException(
                Error::ResourceExhausted,
                "shared-fetch participant capacity exhausted");
        }

        cell->interest = HistoryInterestId{next_interest_id_};
        cell->revision = next_participant_revision_;
        cell->participant_owner_instance = participant_owner_instance;
        cell->demand = demand.id();
        cell->acquisition = acquisition.context().acquisition;
        cell->request_scope_id = request.id;
        cell->deadline = request.absolute_deadline;
        cell->fair_ticket = next_fair_ticket_;
        cell->plan = std::move(plan);
        cell->request_scope = request_scope;
        cell->participant_owner = std::move(participant_owner);

        std::map<HistoryInterestId, std::shared_ptr<ParticipantCell>> prepared;
        prepared.emplace(cell->interest, cell);
        auto node = prepared.extract(cell->interest);
        interests_.insert(std::move(node));
        ++next_interest_id_;
        ++next_participant_revision_;
        ++next_fair_ticket_;
    }
    return make_ref(*cell);
}

std::optional<HistoryInterestRef>
//@@authority transition=prv.T0270 status=provisional scope=function
// bind=catl::peer_client::revise_interest@4b2a9566205051ca
// owner=unresolved owner_hypothesis=shared_history_fetch
// commit=owner_then_participant_mutex touches=prv.S0108,prv.S0109,prv.S0110
// label=replace_participant_revision_and_revoke_old_unspent_grants
SharedHistoryFetch::revise_interest(
    HistoryInterestRef const& interest,
    HistoryParticipantPlan plan,
    Clock::time_point now)
{
    if (!authentic(interest))
        return std::nullopt;
    if (now >= interest.deadline_ || !valid_plan(plan, interest.deadline_, now))
    {
        return std::nullopt;
    }

    HistoryInterestRef revised;
    {
        std::lock_guard owner_lock(mutex_);
        if (terminal_ ||
            next_participant_revision_ ==
                std::numeric_limits<std::uint64_t>::max() ||
            next_fair_ticket_ == std::numeric_limits<std::uint64_t>::max())
        {
            return std::nullopt;
        }
        auto const found = interests_.find(interest.interest_);
        if (found == interests_.end())
            return std::nullopt;
        auto const& cell = found->second;
        std::lock_guard participant_lock(cell->mutex);
        if (cell->state != ParticipantState::active ||
            cell->request_terminal.load(std::memory_order_acquire) ||
            cell->participant_owner_instance !=
                interest.participant_owner_instance_ ||
            cell->revision != interest.participant_revision_ ||
            cell->demand != interest.demand_ ||
            cell->acquisition != interest.acquisition_ ||
            cell->request_scope_id != interest.request_scope_)
        {
            return std::nullopt;
        }

        for (auto& [_, grant] : cell->grants)
        {
            if (grant.state == HistoryGrantState::active)
                grant.state = HistoryGrantState::revoked;
        }
        cell->revision = next_participant_revision_;
        cell->fair_ticket = next_fair_ticket_;
        cell->plan = std::move(plan);
        ++next_participant_revision_;
        ++next_fair_ticket_;
        revised = make_ref(*cell);
    }
    // Wake after both shared locks are released. Preserving the global
    // owner->participant order keeps this reusable if wake delivery evolves.
    wake_participant_owners(revised.participant_owner_instance());
    return revised;
}

HistorySponsorDecision
//@@authority transition=prv.T0271 status=provisional scope=function
// bind=catl::peer_client::select_sponsor@283c76c98aa8f6ba
// owner=unresolved owner_hypothesis=shared_history_fetch commit=owner_mutex
// touches=prv.S0108,prv.S0109
// label=project_earliest_deadline_fair_viable_sponsor_and_aggregate_wake
SharedHistoryFetch::select_sponsor(Clock::time_point now) const noexcept
{
    HistorySponsorDecision result;
    std::lock_guard owner_lock(mutex_);
    if (terminal_)
        return result;

    std::shared_ptr<ParticipantCell> selected;
    std::optional<std::pair<Clock::time_point, std::uint64_t>> selected_rank;
    for (auto const& [_, cell] : interests_)
    {
        std::lock_guard participant_lock(cell->mutex);
        if (cell->state != ParticipantState::active ||
            cell->request_terminal.load(std::memory_order_acquire) ||
            !cell->request_scope || now >= cell->deadline)
        {
            continue;
        }
        ++result.live_participants;
        if (!cell->plan.next_action)
        {
            if (cell->plan.wait_until && cell->plan.wait_until->at > now &&
                (!result.aggregate_wake ||
                 cell->plan.wait_until->at < *result.aggregate_wake))
            {
                ++result.viable_participants;
                result.aggregate_wake = cell->plan.wait_until->at;
            }
            continue;
        }
        ++result.viable_participants;
        if (cell->plan.next_action->not_before > now)
        {
            if (!result.aggregate_wake ||
                cell->plan.next_action->not_before < *result.aggregate_wake)
            {
                result.aggregate_wake = cell->plan.next_action->not_before;
            }
            continue;
        }
        auto const rank = std::pair{cell->deadline, cell->fair_ticket};
        if (!selected_rank || rank < *selected_rank)
        {
            selected = cell;
            selected_rank = rank;
        }
    }
    if (selected)
    {
        std::lock_guard selected_lock(selected->mutex);
        result.sponsor = make_ref(*selected);
    }
    return result;
}

HistoryAuthorityGrantRef
//@@authority transition=prv.T0272 status=provisional scope=function
// bind=catl::peer_client::mint_grant@5056aa3ebb20dd71
// owner=unresolved owner_hypothesis=shared_history_fetch
// commit=owner_then_participant_mutex touches=prv.S0108,prv.S0109,prv.S0110
// label=mint_exact_one_shot_action_target_grant_for_current_sponsor
SharedHistoryFetch::mint_grant(
    HistoryInterestRef const& selected,
    Clock::time_point now)
{
    if (!authentic(selected))
        return {};

    std::lock_guard owner_lock(mutex_);
    if (terminal_ ||
        next_grant_id_ == std::numeric_limits<std::uint64_t>::max() ||
        next_fair_ticket_ == std::numeric_limits<std::uint64_t>::max())
    {
        return {};
    }

    std::shared_ptr<ParticipantCell> winner;
    std::optional<std::pair<Clock::time_point, std::uint64_t>> winner_rank;
    for (auto const& [_, cell] : interests_)
    {
        std::lock_guard participant_lock(cell->mutex);
        if (cell->state != ParticipantState::active ||
            cell->request_terminal.load(std::memory_order_acquire) ||
            !cell->plan.next_action ||
            cell->plan.next_action->not_before > now || !cell->request_scope ||
            now >= cell->deadline)
        {
            continue;
        }
        auto const rank = std::pair{cell->deadline, cell->fair_ticket};
        if (!winner_rank || rank < *winner_rank)
        {
            winner = cell;
            winner_rank = rank;
        }
    }
    if (!winner || winner->interest != selected.interest_)
        return {};

    if (winner->participant_owner_instance !=
            selected.participant_owner_instance_ ||
        winner->revision != selected.participant_revision_ ||
        winner->demand != selected.demand_ ||
        winner->acquisition != selected.acquisition_ ||
        winner->request_scope_id != selected.request_scope_ ||
        winner->grants.size() >= limits_.max_grants_per_interest ||
        !winner->plan.next_action)
    {
        return {};
    }

    HistoryGrantId const id{next_grant_id_};
    auto const selected_ref = make_ref(*winner);
    auto const action = winner->plan.next_action->action;
    auto const target = winner->plan.next_action->target;
    auto const not_before = winner->plan.next_action->not_before;
    GrantRecord record{
        .id = id,
        .participant_revision = winner->revision,
        .action = action,
        .target = target,
        .not_before = not_before};
    std::map<HistoryGrantId, GrantRecord> prepared;
    prepared.emplace(id, std::move(record));
    auto node = prepared.extract(id);
    auto result =
        HistoryAuthorityGrantRef{selected_ref, id, action, target, not_before};
    {
        std::lock_guard participant_lock(winner->mutex);
        if (winner->state != ParticipantState::active ||
            winner->request_terminal.load(std::memory_order_acquire) ||
            winner->grants.size() >= limits_.max_grants_per_interest)
        {
            return {};
        }
        winner->grants.insert(std::move(node));
        winner->fair_ticket = next_fair_ticket_;
        ++next_grant_id_;
        ++next_fair_ticket_;
    }
    return result;
}

std::optional<HistoryGrantSpendReceipt>
SharedHistoryFetch::try_spend(
    HistoryAuthorityGrantRef const& grant,
    HistoryAuthorityAction action,
    HistoryAuthorityTarget const& target,
    Clock::time_point now) noexcept
{
    return try_spend_with_owner_commit(
        grant, action, target, now, nullptr, nullptr);
}

std::optional<HistoryGrantSpendReceipt>
//@@authority transition=prv.T0273 status=provisional scope=function bind=catl::peer_client::try_spend_with_owner_commit@48a59e6c8ca0d9f3
// owner=unresolved owner_hypothesis=participant_grant_arbiter
// commit=participant_then_request_mutex
// touches=prv.S0103,prv.S0106,prv.S0110,prv.S0111
// label=spend_exact_grant_and_owner_flip_against_sticky_request_commit
SharedHistoryFetch::try_spend_with_owner_commit(
    HistoryAuthorityGrantRef const& grant,
    HistoryAuthorityAction action,
    HistoryAuthorityTarget const& target,
    Clock::time_point now,
    void* owner_context,
    OwnerSpendCommit owner_commit) noexcept
{
    if (!authentic(grant))
        return std::nullopt;
    auto cell = find_cell(grant.interest_);
    if (!cell)
        return std::nullopt;

    std::lock_guard participant_lock(cell->mutex);
    if (cell->state != ParticipantState::active ||
        cell->request_terminal.load(std::memory_order_acquire) ||
        cell->participant_owner_instance != grant.participant_owner_instance_ ||
        cell->revision != grant.participant_revision_ ||
        cell->demand != grant.demand_ ||
        cell->acquisition != grant.acquisition_ ||
        cell->request_scope_id != grant.request_scope_ ||
        cell->deadline != grant.deadline_ || now >= cell->deadline ||
        !cell->request_scope)
    {
        return std::nullopt;
    }
    auto const found = cell->grants.find(grant.grant_);
    if (found == cell->grants.end())
        return std::nullopt;
    auto& canonical = found->second;
    if (canonical.state != HistoryGrantState::active ||
        canonical.participant_revision != grant.participant_revision_ ||
        canonical.action != action || canonical.action != grant.action_ ||
        !(canonical.target == target) || !(canonical.target == grant.target_) ||
        canonical.not_before != grant.not_before_ || now < canonical.not_before)
    {
        return std::nullopt;
    }

    auto const committed = cell->request_scope->try_commit(
        now,
        [&canonical,
         &cell,
         owner_context,
         owner_commit](catl::core::RequestCommitId id) noexcept {
            canonical.state = HistoryGrantState::spent;
            canonical.request_commit = id;
            ++cell->outstanding_work;
            //@@mutation-site history-owner-flip-shares-request-commit
            if (owner_commit)
                owner_commit(owner_context, id);
            //@@mutation-end history-owner-flip-shares-request-commit
        });
    if (!committed)
        return std::nullopt;
    return HistoryGrantSpendReceipt{grant.grant_, *committed};
}

HistoryDetachReceipt
//@@authority transition=prv.T0274 status=provisional scope=function
// bind=catl::peer_client::detach@b3f3cedf3a953b17
// owner=unresolved owner_hypothesis=shared_history_fetch
// commit=owner_then_participant_mutex
// touches=prv.S0108,prv.S0110,prv.S0111,prv.S0112,prv.S0113
// label=revoke_participant_and_report_exact_pending_work_acknowledgement
SharedHistoryFetch::detach(HistoryInterestRef const& interest) noexcept
{
    HistoryDetachReceipt result;
    if (!authentic(interest))
        return result;

    ReleasedAuthority released;
    {
        std::lock_guard owner_lock(mutex_);
        auto const found = interests_.find(interest.interest_);
        if (found == interests_.end())
            return result;
        auto const& cell = found->second;
        {
            std::lock_guard participant_lock(cell->mutex);
            if (cell->participant_owner_instance !=
                    interest.participant_owner_instance_ ||
                cell->revision != interest.participant_revision_ ||
                cell->demand != interest.demand_ ||
                cell->acquisition != interest.acquisition_ ||
                cell->request_scope_id != interest.request_scope_ ||
                cell->deadline != interest.deadline_)
            {
                return result;
            }
            if (cell->state == ParticipantState::active)
            {
                cell->state = ParticipantState::revoked;
                for (auto& [_, grant] : cell->grants)
                {
                    if (grant.state == HistoryGrantState::active)
                        grant.state = HistoryGrantState::revoked;
                }
                released.subscription = std::move(cell->terminal_subscription);
                released.scope = std::move(cell->request_scope);
                released.participant_owner = std::move(cell->participant_owner);
                result.accepted = true;
            }
            result.pending_work = cell->outstanding_work;
            result.acknowledged = cell->outstanding_work == 0;
        }

        bool any_active = false;
        bool any_pending_work = false;
        for (auto const& [_, candidate] : interests_)
        {
            std::lock_guard candidate_lock(candidate->mutex);
            any_pending_work =
                any_pending_work || candidate->outstanding_work != 0;
            if (candidate->state == ParticipantState::active)
            {
                any_active = true;
            }
        }
        if (!any_active && !any_pending_work && !terminal_)
        {
            terminal_ = SharedHistoryFetchTerminal::no_interests;
            ++publication_revision_;
            result.terminalized_fetch = true;
        }
    }
    release_authority(std::move(released));
    wake_participant_owners();
    return result;
}

bool
SharedHistoryFetch::receipt_matches_action(
    HistoryAuthorityAction action,
    HistoryWorkReceipt receipt) noexcept
{
    switch (action)
    {
        case HistoryAuthorityAction::attach_or_adopt:
        case HistoryAuthorityAction::create_generation:
            return receipt == HistoryWorkReceipt::closed ||
                receipt == HistoryWorkReceipt::adopted;
        case HistoryAuthorityAction::submit_known_use:
        case HistoryAuthorityAction::submit_blind_use:
            return receipt == HistoryWorkReceipt::closed;
        case HistoryAuthorityAction::promote:
            return receipt == HistoryWorkReceipt::closed ||
                receipt == HistoryWorkReceipt::service_owned;
    }
    return false;
}

void
SharedHistoryFetch::terminalize_if_detached_and_drained() noexcept
{
    std::lock_guard owner_lock(mutex_);
    if (terminal_)
        return;
    for (auto const& [_, cell] : interests_)
    {
        std::lock_guard participant_lock(cell->mutex);
        if (cell->state == ParticipantState::active ||
            cell->outstanding_work != 0)
        {
            return;
        }
    }
    terminal_ = SharedHistoryFetchTerminal::no_interests;
    ++publication_revision_;
}

bool
//@@authority transition=prv.T0275 status=provisional scope=function
// bind=catl::peer_client::acknowledge_work@3443e5114626a0cd
// owner=unresolved owner_hypothesis=participant_grant_arbiter
// commit=exact_participant_cell_mutexes touches=prv.S0110,prv.S0111,prv.S0112
// label=acknowledge_one_physical_owner_by_exact_close_or_adopter_receipt
SharedHistoryFetch::acknowledge_work(
    HistoryAuthorityGrantRef const& grant,
    HistoryWorkReceipt receipt,
    std::optional<HistoryAuthorityGrantRef> const& adopter) noexcept
{
    if (!authentic(grant) || !receipt_matches_action(grant.action_, receipt))
        return false;
    auto cell = find_cell(grant.interest_);
    if (!cell)
        return false;

    if (receipt == HistoryWorkReceipt::adopted)
    {
        if (!adopter || !authentic(*adopter) ||
            adopter->interest_ == grant.interest_ ||
            adopter->action_ != HistoryAuthorityAction::attach_or_adopt ||
            !(adopter->target_ == grant.target_))
        {
            return false;
        }
        auto adopter_cell = find_cell(adopter->interest_);
        if (!adopter_cell)
            return false;

        {
            std::scoped_lock participant_locks(
                cell->mutex, adopter_cell->mutex);
            auto const source_found = cell->grants.find(grant.grant_);
            auto const adopter_found =
                adopter_cell->grants.find(adopter->grant_);
            if (source_found == cell->grants.end() ||
                adopter_found == adopter_cell->grants.end())
            {
                return false;
            }
            auto& source = source_found->second;
            auto& successor = adopter_found->second;
            if (source.state != HistoryGrantState::spent ||
                source.work_receipt ||
                cell->participant_owner_instance !=
                    grant.participant_owner_instance_ ||
                source.participant_revision != grant.participant_revision_ ||
                source.action != grant.action_ ||
                source.not_before != grant.not_before_ ||
                !(source.target == grant.target_) ||
                cell->outstanding_work == 0 ||
                adopter_cell->state != ParticipantState::active ||
                adopter_cell->request_terminal.load(
                    std::memory_order_acquire) ||
                adopter_cell->participant_owner_instance !=
                    adopter->participant_owner_instance_ ||
                adopter_cell->revision != adopter->participant_revision_ ||
                adopter_cell->demand != adopter->demand_ ||
                adopter_cell->acquisition != adopter->acquisition_ ||
                adopter_cell->request_scope_id != adopter->request_scope_ ||
                successor.state != HistoryGrantState::spent ||
                successor.work_receipt || successor.adoption_claimed ||
                successor.participant_revision !=
                    adopter->participant_revision_ ||
                successor.not_before != adopter->not_before_ ||
                successor.action != HistoryAuthorityAction::attach_or_adopt ||
                !(successor.target == source.target) ||
                adopter_cell->outstanding_work == 0)
            {
                return false;
            }
            source.work_receipt = receipt;
            successor.adoption_claimed = true;
            --cell->outstanding_work;
        }
        terminalize_if_detached_and_drained();
        wake_participant_owners();
        return true;
    }
    if (adopter)
        return false;

    {
        std::lock_guard participant_lock(cell->mutex);
        auto const found = cell->grants.find(grant.grant_);
        if (found == cell->grants.end())
            return false;
        auto& canonical = found->second;
        if (canonical.state != HistoryGrantState::spent ||
            canonical.work_receipt ||
            cell->participant_owner_instance !=
                grant.participant_owner_instance_ ||
            canonical.participant_revision != grant.participant_revision_ ||
            canonical.action != grant.action_ ||
            canonical.not_before != grant.not_before_ ||
            !(canonical.target == grant.target_) || cell->outstanding_work == 0)
        {
            return false;
        }
        canonical.work_receipt = receipt;
        --cell->outstanding_work;
    }
    terminalize_if_detached_and_drained();
    wake_participant_owners();
    return true;
}

bool
//@@authority transition=prv.T0276 status=provisional scope=function
// bind=catl::peer_client::terminalize_if_no_viable_action@197a5614a4212fac
// owner=unresolved owner_hypothesis=shared_history_fetch
// commit=owner_then_participant_mutex
// touches=prv.S0108,prv.S0109,prv.S0111,prv.S0112,prv.S0113
// label=publish_no_viable_terminal_only_after_future_and_work_exhaustion
SharedHistoryFetch::terminalize_if_no_viable_action(Clock::time_point now)
{
    {
        std::lock_guard owner_lock(mutex_);
        if (terminal_)
            return false;
        for (auto const& [_, cell] : interests_)
        {
            std::lock_guard participant_lock(cell->mutex);
            if (cell->outstanding_work != 0)
                return false;
            if (cell->state == ParticipantState::active &&
                !cell->request_terminal.load(std::memory_order_acquire) &&
                cell->request_scope && now < cell->deadline &&
                ((cell->plan.next_action &&
                  cell->plan.next_action->not_before < cell->deadline) ||
                 (cell->plan.wait_until && cell->plan.wait_until->at > now &&
                  cell->plan.wait_until->at <= cell->deadline)))
            {
                return false;
            }
        }

        terminal_ = SharedHistoryFetchTerminal::no_viable_participant;
        ++publication_revision_;
        for (auto const& [_, cell] : interests_)
        {
            std::lock_guard participant_lock(cell->mutex);
            cell->state = ParticipantState::revoked;
            for (auto& [__, grant] : cell->grants)
            {
                if (grant.state == HistoryGrantState::active)
                    grant.state = HistoryGrantState::revoked;
            }
        }
    }
    release_terminal_authorities();
    return true;
}

void
SharedHistoryFetch::release_terminal_authorities() noexcept
{
    HistoryInterestId cursor;
    for (;;)
    {
        ReleasedAuthority released;
        try
        {
            std::lock_guard owner_lock(mutex_);
            auto const found = interests_.upper_bound(cursor);
            if (found == interests_.end())
                return;
            cursor = found->first;
            std::lock_guard participant_lock(found->second->mutex);
            released.subscription =
                std::move(found->second->terminal_subscription);
            released.scope = std::move(found->second->request_scope);
            released.participant_owner =
                std::move(found->second->participant_owner);
        }
        catch (...)
        {
            return;
        }
        release_authority(std::move(released));
    }
}

bool
//@@authority transition=prv.T0277 status=provisional scope=function
// bind=catl::peer_client::publish_terminal@519fc5b4d330e3d9
// owner=unresolved owner_hypothesis=shared_history_fetch
// commit=owner_then_participant_mutex
// touches=prv.S0108,prv.S0110,prv.S0112,prv.S0113
// label=publish_one_sticky_exact_fetch_epoch_terminal
SharedHistoryFetch::publish_terminal(
    SharedHistoryFetchTerminal terminal) noexcept
{
    if (!valid_fetch_terminal(terminal))
        return false;
    try
    {
        {
            std::lock_guard owner_lock(mutex_);
            if (terminal_)
                return false;
            terminal_ = terminal;
            ++publication_revision_;
            for (auto const& [_, cell] : interests_)
            {
                std::lock_guard participant_lock(cell->mutex);
                cell->state = ParticipantState::revoked;
                for (auto& [__, grant] : cell->grants)
                {
                    if (grant.state == HistoryGrantState::active)
                        grant.state = HistoryGrantState::revoked;
                }
            }
        }
        release_terminal_authorities();
        return true;
    }
    catch (...)
    {
        // A mutex failure cannot be recovered here. Preserve any terminal
        // state already committed and fail closed without escaping a cache
        // owner's noexcept response/retirement path.
        return false;
    }
}

SharedHistoryFetchSnapshot
SharedHistoryFetch::snapshot() const noexcept
{
    SharedHistoryFetchSnapshot result{
        .fetch = fetch_, .fetch_epoch = fetch_epoch_};
    std::lock_guard owner_lock(mutex_);
    result.terminal = terminal_;
    result.publication_revision = publication_revision_;
    result.interests = interests_.size();
    for (auto const& [_, cell] : interests_)
    {
        std::lock_guard participant_lock(cell->mutex);
        if (cell->state == ParticipantState::active)
        {
            ++result.active_interests;
            if (!cell->plan.next_action)
                ++result.passive_interests;
        }
        result.spent_unacknowledged += cell->outstanding_work;
        for (auto const& [__, grant] : cell->grants)
        {
            if (grant.state == HistoryGrantState::active)
                ++result.active_grants;
        }
    }
    return result;
}

std::optional<HistoryGrantState>
SharedHistoryFetch::grant_state(
    HistoryAuthorityGrantRef const& grant) const noexcept
{
    if (!authentic(grant))
        return std::nullopt;
    auto cell = find_cell(grant.interest_);
    if (!cell)
        return std::nullopt;
    std::lock_guard participant_lock(cell->mutex);
    auto const found = cell->grants.find(grant.grant_);
    if (found == cell->grants.end() ||
        cell->participant_owner_instance != grant.participant_owner_instance_ ||
        found->second.participant_revision != grant.participant_revision_ ||
        found->second.action != grant.action_ ||
        found->second.not_before != grant.not_before_ ||
        !(found->second.target == grant.target_))
    {
        return std::nullopt;
    }
    return found->second.state;
}

}  // namespace catl::peer_client
