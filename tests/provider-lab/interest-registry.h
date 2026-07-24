#pragma once

// Coordinator increment 3c, round 3: interests with two-phase detach,
// fully-bound artifact seams, and three-phase handoffs. TEST-ONLY:
// production construction must never reference this.
//
// Composed-review rulings folded in (on top of rounds 1-2):
//  - the interest RETAINS its participant destination expectation
//    (scope, epoch, revision) from registration; every revoke
//    artifact is validated against the COMPLETE binding — a receipt
//    with the right command and the wrong revision is wrong_binding,
//    before and after exact consumption;
//  - consuming the participant's revoked receipt mints and RETAINS a
//    distinct ParticipantReceiptConsumed witness (replayable): the
//    participant cell retires on THAT, never on its own outgoing
//    receipt;
//  - the terminal ack cannot retire the interest: the operation
//    ledger consumes it and mints InterestTerminalConsumed, and only
//    that witness removes the terminal-ready row;
//  - use registration/closure are NOT public verbs: they are
//    committed solely by the coordinator seam façade that owns both
//    state homes on one strand (split-atomic spend/register and
//    raw-id use_terminal forgery are inexpressible);
//  - delivery acknowledgement's owner is not composed yet: closure is
//    an explicit test-only seam, not an annotated authority any
//    caller can assert;
//  - unsubscribe honesty: detach makes NO claim that an unsubscribe
//    was initiated (no command, no outbox, no replay exists here).
//    The row is merely unsubscribe-pending until the subscription
//    owner is composed; the consumption verb stays typed and bound,
//    with a test-only minter;
//  - id counters are unique WITHIN this registry instance (scoped by
//    its PartitionScope), stated as such, and fail closed at their
//    ceilings with zero reserved.

#include "coordinator-artifacts.h"
#include "deterministic-scheduler.h"

#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <vector>

namespace catl::provider_lab {

class DeliveryAckTestSeam;

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
        wrong_binding,  // artifact does not match the retained binding
        interest_id_exhausted,
        revoke_command_id_exhausted,
    };

    struct RegisterResult
    {
        Refusal refusal{};
        InterestId interest = 0;
    };

    struct DetachResult
    {
        Refusal refusal{};
        std::optional<RevokeCommand> command;
    };

    struct ConsumeRevokeResult
    {
        Refusal refusal{};
        // Fresh on ok; the IDENTICAL retained witness on already_acked.
        std::optional<ParticipantReceiptConsumed> consumed;
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
        PartitionScope scope,
        InterestId first_minted_id = 1,
        RevokeCommandId first_minted_command = 1)
        : strand_(std::move(strand))
        , scope_(scope)
        , next_interest_(first_minted_id == 0 ? 1 : first_minted_id)
        , next_revoke_command_(
              first_minted_command == 0 ? 1 : first_minted_command)
    {
        // Zero reserved for both mints; the first-id parameters are
        // test-only seams so ceiling controls avoid 2^64 warm-ups.
        // Uniqueness of minted ids is scoped to THIS registry
        // instance — cross-instance discrimination comes from the
        // PartitionScope carried on every artifact, never from the
        // counters.
    }

    bool
    on_owner_strand() const
    {
        return strand_.running_in_this_thread();
    }

    PartitionScope
    scope() const
    {
        return scope_;
    }

    /// Mint one interest retaining its original operation/child, its
    /// cancellation-subscription identity, and its participant
    /// destination EXPECTATION (scope/epoch/revision of the cell
    /// opened for it). Fail-closed at the id ceiling.
    //@@authority transition=provider_lab.register_interest owner=provider_lab commit=strand
    RegisterResult
    register_interest(
        InterestOperationId operation,
        InterestChildId child,
        InterestFetchId fetch,
        SubscriptionId subscription,
        ParticipantDestination expected)
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
                .subscription = subscription,
                .expected = expected});
        return {Refusal::ok, id};
    }

    /// Register a delivery obligation under an ACTIVE interest
    /// (interest-local debt registration by the interest owner). A
    /// duplicate DeliveryId is typed already_registered. NOTE: debt
    /// CLOSURE has no composed owner yet — see DeliveryAckTestSeam.
    //@@authority transition=provider_lab.interest_add_delivery owner=provider_lab commit=strand
    Refusal
    add_delivery_obligation(InterestId id, DeliveryId delivery)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        auto it = interests_.find(id);
        if (it == interests_.end())
            return Refusal::unknown_interest;
        if (it->second.phase != Phase::active)
            return Refusal::not_active;
        return it->second.open_deliveries.insert(delivery).second
            ? Refusal::ok
            : Refusal::already_registered;
    }

    /// active -> detach_pending: seals new sponsorship AND commits the
    /// sender-owned revoke command — bound to the COMPLETE retained
    /// destination expectation — in the same owner-strand commit. The
    /// command is retained (replayable) until its receipt is consumed.
    /// Command ids fail closed at the ceiling BEFORE the seal.
    ///
    /// HONESTY: detach claims revocation was initiated (the command
    /// exists, owned and replayable). It makes NO such claim for the
    /// cancellation subscription: no unsubscribe command or outbox
    /// exists here. The row is unsubscribe-pending until the
    /// subscription owner is composed.
    //@@authority transition=provider_lab.interest_begin_detach owner=provider_lab commit=strand
    DetachResult
    begin_detach(InterestId id)
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, std::nullopt};
        auto it = interests_.find(id);
        if (it == interests_.end())
            return {Refusal::unknown_interest, std::nullopt};
        if (it->second.phase != Phase::active)
            return {Refusal::not_active, std::nullopt};
        if (next_revoke_command_ ==
            std::numeric_limits<RevokeCommandId>::max())
            return {Refusal::revoke_command_id_exhausted, std::nullopt};
        auto& i = it->second;
        i.phase = Phase::detach_pending;
        i.revoke_outbox = RevokeCommand{
            next_revoke_command_++, id, i.fetch, i.operation, i.expected};
        return {Refusal::ok, i.revoke_outbox};
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

    /// Revocation completes ONLY by consuming the bound receipt, and
    /// consumption mints + RETAINS the distinct third-phase witness.
    /// Binding precedence: the COMPLETE destination (scope, epoch,
    /// revision, vs the retained expectation) is checked BEFORE the
    /// dedup read, so a wrong-revision receipt is wrong_binding both
    /// before and after exact consumption. Exact replay returns
    /// already_acked with the IDENTICAL retained witness.
    //@@authority transition=provider_lab.interest_consume_revoke_receipt owner=provider_lab commit=strand
    ConsumeRevokeResult
    consume_revoke_receipt(ParticipantRevokedReceipt const& receipt)
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, std::nullopt};
        auto it = interests_.find(receipt.interest());
        if (it == interests_.end())
            return {Refusal::unknown_interest, std::nullopt};
        auto& i = it->second;
        if (i.phase == Phase::active)
            return {Refusal::not_detach_pending, std::nullopt};
        if (receipt.scope() != i.expected.scope ||
            receipt.epoch() != i.expected.epoch ||
            receipt.revision() != i.expected.revision)
            return {Refusal::wrong_binding, std::nullopt};
        if (i.revoke_consumed.has_value())
            return receipt.command() == i.revoke_consumed->command()
                ? ConsumeRevokeResult{Refusal::already_acked,
                                      i.revoke_consumed}
                : ConsumeRevokeResult{Refusal::wrong_binding,
                                      std::nullopt};
        if (!i.revoke_outbox.has_value() ||
            i.revoke_outbox->command() != receipt.command())
            return {Refusal::wrong_binding, std::nullopt};
        i.revoke_consumed = ParticipantReceiptConsumed{
            receipt.scope(),
            receipt.epoch(),
            receipt.interest(),
            receipt.command(),
            receipt.revision()};
        i.revoke_outbox.reset();  // replay home retires; witness remains
        return {Refusal::ok, i.revoke_consumed};
    }

    /// Replay home for the third-phase witness: survives loss of both
    /// the receipt and the returned witness copy.
    std::optional<ParticipantReceiptConsumed>
    replay_revoke_consumed(InterestId id) const
    {
        if (!on_owner_strand())
            return std::nullopt;
        auto it = interests_.find(id);
        if (it == interests_.end())
            return std::nullopt;
        return it->second.revoke_consumed;
    }

    /// Unsubscription completes ONLY by consuming a receipt bound to
    /// the subscription identity retained since registration. Binding
    /// dominates the done-flag. The MINTER is not composed yet
    /// (test-only seam); this verb owns consumption alone.
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
        if (i.unsubscribe_consumed)
            return Refusal::already_acked;
        i.unsubscribe_consumed = true;
        return Refusal::ok;
    }

    /// Terminal readiness: when every obligation is closed, the
    /// interest transitions to terminal_ready and mints its ack ONCE
    /// into a retained outbox; the identical ack replays on every
    /// later call. The ack is phase TWO of the terminal handoff —
    /// removal requires the ledger-minted consumption witness.
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
        if (!i.revoke_consumed.has_value() || !i.unsubscribe_consumed ||
            !i.open_uses.empty() || !i.open_deliveries.empty())
            return {JoinRefusal::obligations_open, std::nullopt};
        i.phase = Phase::terminal_ready;
        i.terminal_outbox =
            InterestTerminalAck{scope_, id, i.operation, i.child};
        return {JoinRefusal::ok, i.terminal_outbox};
    }

    /// The exactly-once remover: consumes the LEDGER-minted terminal
    /// consumption witness whose binding matches the retained ack
    /// outbox. The interest's own outgoing ack is not evidence — it
    /// proves the ack was produced, not that downstream consumed it —
    /// and it does not compile here.
    //@@authority transition=provider_lab.interest_retire_terminal owner=provider_lab commit=strand
    Refusal
    retire_terminal(InterestTerminalConsumed const& witness)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        auto it = interests_.find(witness.interest());
        if (it == interests_.end())
            return Refusal::unknown_interest;
        auto& i = it->second;
        if (i.phase != Phase::terminal_ready)
            return Refusal::not_detach_pending;
        if (!i.terminal_outbox.has_value() ||
            witness.scope() != i.terminal_outbox->scope() ||
            witness.operation() != i.terminal_outbox->operation() ||
            witness.child() != i.terminal_outbox->child())
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
    std::optional<bool>
    use_open(InterestId id, InterestUseId use) const
    {
        if (!on_owner_strand())
            return std::nullopt;
        auto it = interests_.find(id);
        if (it == interests_.end())
            return std::nullopt;
        return it->second.open_uses.count(use) > 0;
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
        ParticipantDestination expected;
        Phase phase = Phase::active;
        std::optional<ParticipantReceiptConsumed> revoke_consumed;
        bool unsubscribe_consumed = false;
        std::optional<RevokeCommand> revoke_outbox;
        std::optional<InterestTerminalAck> terminal_outbox;
        std::set<InterestUseId> open_uses;
        std::set<DeliveryId> open_deliveries;
    };

    // Use debt registration/closure: committed ONLY by the seam
    // façade (which owns both state homes on this strand) and, for
    // delivery closure, the explicit test seam. NOT public authority:
    // the façade's transitions carry the annotations.
    friend class CoordinatorInterestSeams;
    friend class DeliveryAckTestSeam;

    bool
    can_add_use(InterestId id, InterestUseId use) const
    {
        auto it = interests_.find(id);
        return it != interests_.end() &&
            it->second.phase == Phase::active &&
            it->second.open_uses.count(use) == 0;
    }

    void
    commit_add_use(InterestId id, InterestUseId use)
    {
        interests_.at(id).open_uses.insert(use);
    }

    bool
    can_close_use(InterestId id, InterestUseId use) const
    {
        auto it = interests_.find(id);
        return it != interests_.end() &&
            it->second.open_uses.count(use) > 0;
    }

    void
    commit_close_use(InterestId id, InterestUseId use)
    {
        interests_.at(id).open_uses.erase(use);
    }

    Refusal
    close_delivery(InterestId id, DeliveryId delivery)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        auto it = interests_.find(id);
        if (it == interests_.end())
            return Refusal::unknown_interest;
        return it->second.open_deliveries.erase(delivery)
            ? Refusal::ok
            : Refusal::unknown_delivery;
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
    PartitionScope scope_;
    InterestId next_interest_;
    RevokeCommandId next_revoke_command_;
    //@@authority state=provider_lab.interest_registry owner=provider_lab
    std::map<InterestId, Interest> interests_;
};

/// Test-only closure of delivery debts: the delivery owner is not
/// composed, so closure must not be an annotated production authority
/// that any caller can assert. Explicitly a seam.
class DeliveryAckTestSeam
{
public:
    static InterestRegistry::Refusal
    close(InterestRegistry& reg, InterestId id, DeliveryId delivery)
    {
        return reg.close_delivery(id, delivery);
    }
};

}  // namespace catl::provider_lab
