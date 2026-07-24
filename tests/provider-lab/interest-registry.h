#pragma once

// Coordinator increment 3c, round 2: interests with two-phase detach,
// OWNED command/receipt seams, and a terminal witness that survives
// the fact it attests. TEST-ONLY: production construction must never
// reference this.
//
// Round-1 review rulings folded in:
//  - "calling the raw ID a typed event does not type its authority":
//    begin_detach now COMMITS a sender-owned, replayable revoke
//    command (retained outbox on the interest) and revocation
//    completes only by consuming a bound, sole-minted
//    ParticipantRevokedReceipt — increment 3d's participant cell is
//    its sole production minter; until 3d lands, tests mint through
//    an explicit test-only seam. The unsubscribe seam is identical:
//    the interest RETAINS its subscription identity from registration
//    and only a bound UnsubscribeReceipt completes it;
//  - "the terminal witness must survive the fact it attests": the
//    join no longer erases-and-returns. Readiness transitions the
//    interest to terminal_ready, which RETAINS a sender-owned
//    terminal-ack outbox; the identical ack replays on request, and
//    only retire_terminal — consuming a genuinely minted ack whose
//    binding matches — finally removes the row, exactly once;
//  - InterestIds fail closed at the ceiling (interest_id_exhausted;
//    the wrap/false-agreement class, fourth sighting, now refused
//    here too) with zero reserved and a test-only first-id seam;
//  - duplicate obligation registration is a typed already_registered,
//    never a fresh ok: two logically distinct debts can no longer
//    collapse into one.

#include "deterministic-scheduler.h"

#include <cstdint>
#include <limits>
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
using SubscriptionId = std::uint64_t;
using RevokeCommandId = std::uint64_t;
using ParticipantRevision = std::uint64_t;

class InterestRegistry;
class ParticipantReceiptTestMint;
class UnsubscribeReceiptTestMint;

/// Sender-owned revoke command: a non-authoritative, replayable
/// projection from the interest's retained outbox. Losing it is safe;
/// replay re-derives the identical value.
struct RevokeCommand
{
    RevokeCommandId command = 0;
    InterestId interest = 0;
    InterestFetchId fetch = 0;
    InterestOperationId operation = 0;

    bool
    operator==(RevokeCommand const&) const = default;
};

/// Bound receipt proving the participant cell revoked its grants for
/// this exact interest/command. Sole production minter: increment 3d's
/// participant cell. Tests mint via the explicit test-only seam.
class ParticipantRevokedReceipt
{
public:
    ParticipantRevokedReceipt(ParticipantRevokedReceipt const&) = default;
    ParticipantRevokedReceipt&
    operator=(ParticipantRevokedReceipt const&) = default;

    InterestId
    interest() const
    {
        return interest_;
    }
    RevokeCommandId
    command() const
    {
        return command_;
    }
    ParticipantRevision
    revision() const
    {
        return revision_;
    }

private:
    friend class ParticipantReceiptTestMint;  // test-only until 3d
    ParticipantRevokedReceipt(
        InterestId interest,
        RevokeCommandId command,
        ParticipantRevision revision)
        : interest_(interest), command_(command), revision_(revision)
    {
    }

    InterestId interest_;
    RevokeCommandId command_;
    ParticipantRevision revision_;
};

static_assert(
    !std::is_default_constructible_v<ParticipantRevokedReceipt>,
    "a receipt must be minted, never defaulted");
static_assert(
    !std::is_constructible_v<
        ParticipantRevokedReceipt,
        InterestId,
        RevokeCommandId,
        ParticipantRevision>,
    "raw-field construction would make every caller a second minter");

/// Bound receipt proving the cancellation-subscription owner detached
/// this exact subscription. Sole production minter: the subscription
/// owner (request-session composition). Tests mint via the seam.
class UnsubscribeReceipt
{
public:
    UnsubscribeReceipt(UnsubscribeReceipt const&) = default;
    UnsubscribeReceipt&
    operator=(UnsubscribeReceipt const&) = default;

    InterestId
    interest() const
    {
        return interest_;
    }
    SubscriptionId
    subscription() const
    {
        return subscription_;
    }

private:
    friend class UnsubscribeReceiptTestMint;  // test-only until composed
    UnsubscribeReceipt(InterestId interest, SubscriptionId subscription)
        : interest_(interest), subscription_(subscription)
    {
    }

    InterestId interest_;
    SubscriptionId subscription_;
};

static_assert(
    !std::is_default_constructible_v<UnsubscribeReceipt>,
    "a receipt must be minted, never defaulted");
static_assert(
    !std::is_constructible_v<UnsubscribeReceipt, InterestId, SubscriptionId>,
    "raw-field construction would make every caller a second minter");

/// Sole-minted terminal acknowledgement, bound to the original child.
/// Minted once at terminal readiness, RETAINED in the interest's
/// outbox, replayed identically on request, and consumed exactly once
/// by retire_terminal.
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
        not_active,
        not_detach_pending,
        unknown_use,
        unknown_delivery,
        already_registered,  // duplicate obligation: typed, no new debt
        already_acked,
        wrong_binding,  // receipt does not match the retained outbox
        interest_id_exhausted,
    };

    struct RegisterResult
    {
        Refusal refusal{};
        InterestId interest = 0;
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
        std::optional<InterestTerminalAck> ack;  // identical on replay
    };

    explicit InterestRegistry(
        strand_type strand,
        InterestId first_minted_id = 1)
        : strand_(std::move(strand))
        , next_interest_(first_minted_id == 0 ? 1 : first_minted_id)
    {
        // Zero reserved; first_minted_id is a test-only seam so the
        // ceiling control avoids 2^64 warm-up mints.
    }

    bool
    on_owner_strand() const
    {
        return strand_.running_in_this_thread();
    }

    /// Mint one interest retaining its original operation/child and
    /// its cancellation-subscription identity. Fail-closed at the id
    /// ceiling: refuses, mints nothing, mutates nothing.
    //@@authority transition=provider_lab.register_interest owner=provider_lab commit=strand
    RegisterResult
    register_interest(
        InterestOperationId operation,
        InterestChildId child,
        InterestFetchId fetch,
        SubscriptionId subscription)
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, 0};
        if (next_interest_ == std::numeric_limits<InterestId>::max())
            return {Refusal::interest_id_exhausted, 0};
        auto const id = next_interest_++;
        interests_.emplace(
            id,
            Interest{
                .operation = operation,
                .child = child,
                .fetch = fetch,
                .subscription = subscription});
        return {Refusal::ok, id};
    }

    /// Register a submitted use under an ACTIVE interest. A duplicate
    /// UseId is typed already_registered — it creates no new debt and
    /// must not report a fresh commit.
    //@@authority transition=provider_lab.interest_add_use owner=provider_lab commit=strand
    Refusal
    add_submitted_use(InterestId id, InterestUseId use)
    {
        return add_obligation(id, [&](Interest& i) {
            return i.open_uses.insert(use).second;
        });
    }

    /// Register a delivery obligation under an ACTIVE interest; same
    /// duplicate discipline.
    //@@authority transition=provider_lab.interest_add_delivery owner=provider_lab commit=strand
    Refusal
    add_delivery_obligation(InterestId id, DeliveryId delivery)
    {
        return add_obligation(id, [&](Interest& i) {
            return i.open_deliveries.insert(delivery).second;
        });
    }

    /// active -> detach_pending: seals new sponsorship AND commits the
    /// sender-owned revoke command in the same owner-strand commit —
    /// initiation is a fact with a home, not an arrow. The command is
    /// retained (replayable) until its bound receipt is consumed.
    //@@authority transition=provider_lab.interest_begin_detach owner=provider_lab commit=strand
    std::optional<RevokeCommand>
    begin_detach(InterestId id)
    {
        if (!on_owner_strand())
            return std::nullopt;
        auto it = interests_.find(id);
        if (it == interests_.end() || it->second.phase != Phase::active)
            return std::nullopt;
        auto& i = it->second;
        i.phase = Phase::detach_pending;
        i.revoke_outbox = RevokeCommand{
            ++next_revoke_command_, id, i.fetch, i.operation};
        return i.revoke_outbox;
    }

    /// Replay the retained revoke command (identical) until its
    /// receipt is consumed. Loss of the projection is safe.
    std::optional<RevokeCommand>
    replay_revoke_command(InterestId id) const
    {
        if (!on_owner_strand())
            return std::nullopt;
        auto it = interests_.find(id);
        if (it == interests_.end())
            return std::nullopt;
        return it->second.revoke_outbox;
    }

    /// Revocation completes ONLY by consuming the bound receipt: the
    /// receipt's command must match the retained outbox. Binding is
    /// checked BEFORE the done-flag — the consumed identity is
    /// retained so an exact replay stays already_acked while a
    /// mismatched receipt is wrong_binding, never laundered.
    //@@authority transition=provider_lab.interest_consume_revoke_receipt owner=provider_lab commit=strand
    Refusal
    consume_revoke_receipt(ParticipantRevokedReceipt const& receipt)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        auto it = interests_.find(receipt.interest());
        if (it == interests_.end())
            return Refusal::unknown_interest;
        auto& i = it->second;
        if (i.phase == Phase::active)
            return Refusal::not_detach_pending;
        if (i.revoke_consumed.has_value())
            return receipt.command() == *i.revoke_consumed
                ? Refusal::already_acked
                : Refusal::wrong_binding;
        if (!i.revoke_outbox.has_value() ||
            i.revoke_outbox->command != receipt.command())
            return Refusal::wrong_binding;
        i.revoke_consumed = receipt.command();
        i.revoke_outbox.reset();  // replay home retires; dedup remains
        return Refusal::ok;
    }

    /// Unsubscription completes ONLY by consuming a receipt bound to
    /// the subscription identity retained since registration. Binding
    /// dominates the done-flag here too.
    //@@authority transition=provider_lab.interest_consume_unsubscribe_receipt owner=provider_lab commit=strand
    Refusal
    consume_unsubscribe_receipt(UnsubscribeReceipt const& receipt)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        auto it = interests_.find(receipt.interest());
        if (it == interests_.end())
            return Refusal::unknown_interest;
        auto& i = it->second;
        if (i.phase == Phase::active)
            return Refusal::not_detach_pending;
        if (receipt.subscription() != i.subscription)
            return Refusal::wrong_binding;
        if (i.unsubscribe_done)
            return Refusal::already_acked;
        i.unsubscribe_done = true;
        return Refusal::ok;
    }

    /// A submitted use reached terminality (legal in both phases).
    //@@authority transition=provider_lab.interest_use_terminal owner=provider_lab commit=strand
    Refusal
    use_terminal(InterestId id, InterestUseId use)
    {
        return close_obligation(id, [&](Interest& i) -> Refusal {
            return i.open_uses.erase(use) ? Refusal::ok
                                          : Refusal::unknown_use;
        });
    }

    /// A delivery obligation was acknowledged (legal in both phases).
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

    /// Terminal readiness: when every obligation is closed, the
    /// interest transitions to terminal_ready and mints its ack ONCE
    /// into a retained outbox. The identical ack replays on every
    /// later call — the witness survives the fact it attests. Removal
    /// happens ONLY at retire_terminal.
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
        if (i.phase == Phase::terminal_ready)
            return {JoinRefusal::ok, i.terminal_outbox};  // replay
        if (i.phase != Phase::detach_pending)
            return {JoinRefusal::not_detach_pending, std::nullopt};
        if (!i.revoke_consumed.has_value() || !i.unsubscribe_done ||
            !i.open_uses.empty() || !i.open_deliveries.empty())
            return {JoinRefusal::obligations_open, std::nullopt};
        i.phase = Phase::terminal_ready;
        i.terminal_outbox = InterestTerminalAck{id, i.operation, i.child};
        return {JoinRefusal::ok, i.terminal_outbox};
    }

    /// The exactly-once remover: consumes a genuinely minted terminal
    /// ack whose binding matches the terminal_ready row. Possession is
    /// evidence (the type is unforgeable); a second retire finds no
    /// row and is typed.
    //@@authority transition=provider_lab.interest_retire_terminal owner=provider_lab commit=strand
    Refusal
    retire_terminal(InterestTerminalAck const& ack)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        auto it = interests_.find(ack.interest());
        if (it == interests_.end())
            return Refusal::unknown_interest;
        auto& i = it->second;
        if (i.phase != Phase::terminal_ready)
            return Refusal::not_detach_pending;
        if (!i.terminal_outbox.has_value() || !(*i.terminal_outbox == ack))
            return Refusal::wrong_binding;
        interests_.erase(it);
        return Refusal::ok;
    }

    // Owner-strand introspection; refusal dominates every read.
    std::optional<std::vector<InterestId>>
    sponsors_of(InterestFetchId fetch) const
    {
        return phase_view(fetch, Phase::active);
    }
    std::optional<std::vector<InterestId>>
    draining_of(InterestFetchId fetch) const
    {
        return phase_view(fetch, Phase::detach_pending);
    }
    std::optional<bool>
    interest_present(InterestId id) const
    {
        if (!on_owner_strand())
            return std::nullopt;
        return interests_.count(id) > 0;
    }

private:
    enum class Phase : std::uint8_t {
        active,
        detach_pending,
        terminal_ready,
    };

    struct Interest
    {
        InterestOperationId operation;
        InterestChildId child;
        InterestFetchId fetch;
        SubscriptionId subscription;
        Phase phase = Phase::active;
        std::optional<RevokeCommandId> revoke_consumed;  // dedup domain
        bool unsubscribe_done = false;
        std::optional<RevokeCommand> revoke_outbox;
        std::optional<InterestTerminalAck> terminal_outbox;
        std::set<InterestUseId> open_uses;
        std::set<DeliveryId> open_deliveries;
    };

    template <typename Fn>
    Refusal
    add_obligation(InterestId id, Fn insert)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        auto it = interests_.find(id);
        if (it == interests_.end())
            return Refusal::unknown_interest;
        if (it->second.phase != Phase::active)
            return Refusal::not_active;
        return insert(it->second) ? Refusal::ok
                                  : Refusal::already_registered;
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

    std::optional<std::vector<InterestId>>
    phase_view(InterestFetchId fetch, Phase phase) const
    {
        if (!on_owner_strand())
            return std::nullopt;
        std::vector<InterestId> out;
        for (auto const& [id, i] : interests_)
            if (i.fetch == fetch && i.phase == phase)
                out.push_back(id);
        return out;
    }

    strand_type strand_;
    InterestId next_interest_;
    RevokeCommandId next_revoke_command_ = 0;
    //@@authority state=provider_lab.interest_registry owner=provider_lab
    std::map<InterestId, Interest> interests_;
};

/// Test-only receipt mints for the seams whose sole production minters
/// arrive with increment 3d (participant cell) and the subscription
/// owner. Explicitly not production authority; no authority annotation.
class ParticipantReceiptTestMint
{
public:
    static ParticipantRevokedReceipt
    mint(
        InterestId interest,
        RevokeCommandId command,
        ParticipantRevision revision)
    {
        return ParticipantRevokedReceipt{interest, command, revision};
    }
};

class UnsubscribeReceiptTestMint
{
public:
    static UnsubscribeReceipt
    mint(InterestId interest, SubscriptionId subscription)
    {
        return UnsubscribeReceipt{interest, subscription};
    }
};

}  // namespace catl::provider_lab
