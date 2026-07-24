#pragma once

// Coordinator increment 3c: interests with two-phase detach (accepted
// design 21af68e/345061f, the section Sol's sole-interest
// counterexample forced). TEST-ONLY: production construction must
// never reference this.
//
// The contract:
//  - each interest retains its ORIGINAL (OperationId, InterestChildId);
//    shared work never adopts the first waiter's root — the fetch's
//    sponsor set is derived from ACTIVE interests, not a stored field;
//  - detach is TWO-PHASE: active -> detach_pending seals new
//    sponsorship and initiates participant revocation but removes
//    NOTHING; the pending row retains the original identities, the
//    cancellation subscription, submitted-use identities, and delivery
//    obligations;
//  - only the terminal JOIN removes the interest — after the revoke
//    ack, every submitted use terminal, every delivery acknowledged,
//    and the unsubscribe acknowledged — and the join emits the
//    sole-minted terminal acknowledgement bound to the original child,
//    exactly once;
//  - the sole-interest case is the same path: zero active interests
//    means "draining under retained authority," never "ownerless";
//  - use completion and delivery acknowledgement are legal while
//    ACTIVE too (normal operation); revoke/unsubscribe acks exist only
//    once detach initiated them. Duplicate acks are typed no-ops.

#include "deterministic-scheduler.h"

#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <type_traits>
#include <vector>

namespace catl::provider_lab {

using InterestId = std::uint64_t;
using InterestOperationId = std::uint64_t;
using InterestChildId = std::uint64_t;
using InterestFetchId = std::uint64_t;
using InterestUseId = std::uint64_t;
using DeliveryId = std::uint64_t;

class InterestRegistry;

/// Sole-minted terminal acknowledgement: the exact original-child
/// evidence the operation ledger's ack gate consumes. Minted only by
/// the registry's terminal join, exactly once per interest.
class InterestTerminalAck
{
public:
    InterestTerminalAck(InterestTerminalAck const&) = default;
    InterestTerminalAck&
    operator=(InterestTerminalAck const&) = default;

    InterestId
    interest() const
    {
        return interest_;
    }
    InterestOperationId
    operation() const
    {
        return operation_;
    }
    InterestChildId
    child() const
    {
        return child_;
    }

    bool
    operator==(InterestTerminalAck const&) const = default;

private:
    friend class InterestRegistry;  // sole minter
    InterestTerminalAck(
        InterestId interest,
        InterestOperationId operation,
        InterestChildId child)
        : interest_(interest), operation_(operation), child_(child)
    {
    }

    InterestId interest_;
    InterestOperationId operation_;
    InterestChildId child_;
};

static_assert(
    !std::is_default_constructible_v<InterestTerminalAck>,
    "a terminal ack must be minted, never defaulted");
static_assert(
    !std::is_constructible_v<
        InterestTerminalAck,
        InterestId,
        InterestOperationId,
        InterestChildId>,
    "raw-field construction would make every caller a second minter");

class InterestRegistry
{
public:
    using strand_type = DeterministicScheduler::strand_type;

    enum class Refusal : std::uint8_t {
        ok,
        off_strand,
        unknown_interest,
        not_active,        // sealed: new sponsorship after detach began
        not_detach_pending,  // revoke/unsubscribe acks need a detach
        unknown_use,
        unknown_delivery,
        already_acked,     // duplicate ack: typed no-op
    };

    enum class JoinRefusal : std::uint8_t {
        ok,
        off_strand,
        unknown_interest,
        not_detach_pending,
        obligations_open,
    };

    struct JoinResult
    {
        JoinRefusal refusal{};
        std::optional<InterestTerminalAck> ack;  // exactly-once
    };

    explicit InterestRegistry(strand_type strand) : strand_(std::move(strand))
    {
    }

    bool
    on_owner_strand() const
    {
        return strand_.running_in_this_thread();
    }

    /// Mint one interest retaining its original operation/child. Ids
    /// are non-reused; zero is reserved.
    //@@authority transition=provider_lab.register_interest owner=provider_lab commit=strand
    std::optional<InterestId>
    register_interest(
        InterestOperationId operation,
        InterestChildId child,
        InterestFetchId fetch)
    {
        if (!on_owner_strand())
            return std::nullopt;
        auto const id = ++next_interest_;
        interests_.emplace(
            id, Interest{operation, child, fetch});
        return id;
    }

    /// Register a submitted use under an ACTIVE interest (new
    /// sponsorship is sealed once detach begins).
    //@@authority transition=provider_lab.interest_add_use owner=provider_lab commit=strand
    Refusal
    add_submitted_use(InterestId id, InterestUseId use)
    {
        return add_obligation(
            id, [&](Interest& i) { i.open_uses.insert(use); });
    }

    /// Register a delivery obligation under an ACTIVE interest.
    //@@authority transition=provider_lab.interest_add_delivery owner=provider_lab commit=strand
    Refusal
    add_delivery_obligation(InterestId id, DeliveryId delivery)
    {
        return add_obligation(
            id, [&](Interest& i) { i.open_deliveries.insert(delivery); });
    }

    /// active -> detach_pending: seals new sponsorship, initiates
    /// participant revocation and the unsubscribe, removes NOTHING.
    //@@authority transition=provider_lab.interest_begin_detach owner=provider_lab commit=strand
    Refusal
    begin_detach(InterestId id)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        auto it = interests_.find(id);
        if (it == interests_.end())
            return Refusal::unknown_interest;
        if (it->second.phase != Phase::active)
            return Refusal::not_active;
        it->second.phase = Phase::detach_pending;
        return Refusal::ok;
    }

    /// The participant revocation acknowledgement (exists only once
    /// detach initiated it). Duplicate acks are typed no-ops.
    //@@authority transition=provider_lab.interest_ack_revoke owner=provider_lab commit=strand
    Refusal
    ack_revoke(InterestId id)
    {
        return pending_ack(id, [](Interest& i) -> bool* {
            return &i.revoke_acked;
        });
    }

    /// The cancellation-subscription detach receipt.
    //@@authority transition=provider_lab.interest_ack_unsubscribe owner=provider_lab commit=strand
    Refusal
    ack_unsubscribe(InterestId id)
    {
        return pending_ack(id, [](Interest& i) -> bool* {
            return &i.unsubscribe_acked;
        });
    }

    /// A submitted use reached terminality. Legal while active (normal
    /// completion) and while detach-pending (draining).
    //@@authority transition=provider_lab.interest_use_terminal owner=provider_lab commit=strand
    Refusal
    use_terminal(InterestId id, InterestUseId use)
    {
        return close_obligation(id, [&](Interest& i) -> Refusal {
            return i.open_uses.erase(use) ? Refusal::ok
                                          : Refusal::unknown_use;
        });
    }

    /// A delivery obligation was acknowledged. Legal in both phases.
    //@@authority transition=provider_lab.interest_ack_delivery owner=provider_lab commit=strand
    Refusal
    ack_delivery(InterestId id, DeliveryId delivery)
    {
        return close_obligation(id, [&](Interest& i) -> Refusal {
            return i.open_deliveries.erase(delivery)
                ? Refusal::ok
                : Refusal::unknown_delivery;
        });
    }

    /// The terminal JOIN: the sole remover. Refuses while any
    /// obligation is open; on success removes the interest and mints
    /// the original-child terminal ack exactly once (a second join is
    /// unknown_interest by construction).
    //@@authority transition=provider_lab.interest_terminal_join owner=provider_lab commit=strand
    JoinResult
    terminal_join(InterestId id)
    {
        if (!on_owner_strand())
            return {JoinRefusal::off_strand, std::nullopt};
        auto it = interests_.find(id);
        if (it == interests_.end())
            return {JoinRefusal::unknown_interest, std::nullopt};
        auto& i = it->second;
        if (i.phase != Phase::detach_pending)
            return {JoinRefusal::not_detach_pending, std::nullopt};
        if (!i.revoke_acked || !i.unsubscribe_acked ||
            !i.open_uses.empty() || !i.open_deliveries.empty())
            return {JoinRefusal::obligations_open, std::nullopt};
        InterestTerminalAck ack{id, i.operation, i.child};
        interests_.erase(it);
        return {JoinRefusal::ok, ack};
    }

    // Owner-strand introspection; refusal dominates every read.
    /// ACTIVE sponsors of a fetch — the derived set that makes
    /// no-root-adoption structural: there is no first-waiter field.
    std::optional<std::vector<InterestId>>
    sponsors_of(InterestFetchId fetch) const
    {
        if (!on_owner_strand())
            return std::nullopt;
        std::vector<InterestId> out;
        for (auto const& [id, i] : interests_)
            if (i.fetch == fetch && i.phase == Phase::active)
                out.push_back(id);
        return out;
    }
    /// Detach-pending interests of a fetch: draining under retained
    /// authority, visible and owned, never "ownerless."
    std::optional<std::vector<InterestId>>
    draining_of(InterestFetchId fetch) const
    {
        if (!on_owner_strand())
            return std::nullopt;
        std::vector<InterestId> out;
        for (auto const& [id, i] : interests_)
            if (i.fetch == fetch && i.phase == Phase::detach_pending)
                out.push_back(id);
        return out;
    }
    std::optional<bool>
    interest_present(InterestId id) const
    {
        if (!on_owner_strand())
            return std::nullopt;
        return interests_.count(id) > 0;
    }

private:
    enum class Phase : std::uint8_t { active, detach_pending };

    struct Interest
    {
        InterestOperationId operation;
        InterestChildId child;
        InterestFetchId fetch;
        Phase phase = Phase::active;
        bool revoke_acked = false;
        bool unsubscribe_acked = false;
        std::set<InterestUseId> open_uses;
        std::set<DeliveryId> open_deliveries;
    };

    template <typename Fn>
    Refusal
    add_obligation(InterestId id, Fn mutate)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        auto it = interests_.find(id);
        if (it == interests_.end())
            return Refusal::unknown_interest;
        if (it->second.phase != Phase::active)
            return Refusal::not_active;  // sealed by detach
        mutate(it->second);
        return Refusal::ok;
    }

    template <typename Fn>
    Refusal
    close_obligation(InterestId id, Fn mutate)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        auto it = interests_.find(id);
        if (it == interests_.end())
            return Refusal::unknown_interest;
        return mutate(it->second);
    }

    template <typename FlagOf>
    Refusal
    pending_ack(InterestId id, FlagOf flag_of)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        auto it = interests_.find(id);
        if (it == interests_.end())
            return Refusal::unknown_interest;
        if (it->second.phase != Phase::detach_pending)
            return Refusal::not_detach_pending;
        bool* flag = flag_of(it->second);
        if (*flag)
            return Refusal::already_acked;
        *flag = true;
        return Refusal::ok;
    }

    strand_type strand_;
    InterestId next_interest_ = 0;
    //@@authority state=provider_lab.interest_registry owner=provider_lab
    std::map<InterestId, Interest> interests_;
};

}  // namespace catl::provider_lab
