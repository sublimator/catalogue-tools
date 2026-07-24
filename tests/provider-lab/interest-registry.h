#pragma once

// Coordinator increment 3c, round 4: interests with two-phase detach,
// scope-first authentication, and three-phase handoffs. TEST-ONLY:
// production construction must never reference this.
//
// Round-4 rulings folded in (on top of rounds 1-3):
//  - SCOPE FIRST, actually: every verb consuming a scope-carrying
//    artifact compares the artifact's PartitionScope against this
//    registry's immutable scope immediately after the strand guard
//    and BEFORE any lookup keyed by the artifact's ids. Foreign-scope
//    artifacts get the same wrong_scope whether the local id exists
//    or not — presence is never leaked before authentication;
//  - UnsubscribeReceipt now carries PartitionScope and is
//    authenticated scope-first like every other artifact;
//  - the interest row is CREATED only by the aggregate façade's
//    atomic open_interest_with_cell (prepare/commit primitives below;
//    node insertion is allocation-free so the commit section cannot
//    throw), and RETIRED only by the façade's atomic
//    retire_interest_terminal, which releases the ledger's witness
//    home in the same commit. register_interest/retire_terminal are
//    no longer public verbs;
//  - use debt registration/closure remain façade-only (round 3), with
//    the debt node PREPARED before any mutation so the seam commit is
//    exception-atomic.

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
        wrong_scope,  // scope-first: refused before any lookup
        unknown_interest,
        not_active,
        not_detach_pending,
        unknown_delivery,
        already_registered,  // duplicate obligation: typed, no new debt
        already_acked,
        wrong_binding,  // artifact does not match the retained binding
        revoke_command_id_exhausted,
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
        // Id uniqueness is scoped to THIS registry instance —
        // cross-instance discrimination comes from the PartitionScope
        // carried on every artifact, never from the counters.
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

    /// Register a delivery obligation under an ACTIVE interest
    /// (interest-local debt registration by the interest owner). A
    /// duplicate DeliveryId is typed already_registered. NOTE: debt
    /// CLOSURE has no composed owner yet — see DeliveryAckTestSeam.
    //@@authority transition=provider_lab.interest_add_delivery owner=provider_lab commit=strand lifecycle=coordinator_3c_3d
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
    //@@authority transition=provider_lab.interest_begin_detach owner=provider_lab commit=strand lifecycle=coordinator_3c_3d
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
    /// SCOPE FIRST: the receipt's partition scope is authenticated
    /// against this registry before its interest id touches any
    /// lookup. Then the full destination (epoch, revision vs the
    /// retained expectation) is checked BEFORE the dedup read, so a
    /// wrong-revision receipt is wrong_binding both before and after
    /// exact consumption. Exact replay returns already_acked with the
    /// IDENTICAL retained witness.
    //@@authority transition=provider_lab.interest_consume_revoke_receipt owner=provider_lab commit=strand lifecycle=coordinator_3c_3d
    ConsumeRevokeResult
    consume_revoke_receipt(ParticipantRevokedReceipt const& receipt)
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, std::nullopt};
        if (receipt.scope() != scope_)
            return {Refusal::wrong_scope, std::nullopt};
        auto it = interests_.find(receipt.interest());
        if (it == interests_.end())
            return {Refusal::unknown_interest, std::nullopt};
        auto& i = it->second;
        if (i.phase == Phase::active)
            return {Refusal::not_detach_pending, std::nullopt};
        if (receipt.epoch() != i.expected.epoch ||
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
    /// the subscription identity retained since registration. SCOPE
    /// FIRST, then lookup, then binding; binding dominates the
    /// done-flag. The MINTER is not composed yet (test-only seam);
    /// this verb owns consumption alone.
    //@@authority transition=provider_lab.interest_consume_unsubscribe_receipt owner=provider_lab commit=strand lifecycle=coordinator_3c_3d
    Refusal
    consume_unsubscribe_receipt(UnsubscribeReceipt const& receipt)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        if (receipt.scope() != scope_)
            return Refusal::wrong_scope;
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
    /// removal happens only through the façade's atomic
    /// retire_interest_terminal, which consumes the ledger-minted
    /// witness and releases the ledger's replay home in one commit.
    //@@authority transition=provider_lab.interest_terminal_join owner=provider_lab commit=strand lifecycle=coordinator_3c_3d
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

    using InterestMap = std::map<InterestId, Interest>;
    using InterestNode = InterestMap::node_type;
    using UseSet = std::set<InterestUseId>;
    using UseNode = UseSet::node_type;

    // Row creation, use debt, and terminal retirement are committed
    // ONLY by the aggregate seam façade (which owns this registry) —
    // and delivery closure by its explicit test seam. NOT public
    // authority: the façade's transitions carry the annotations.
    friend class CoordinatorInterestSeams;
    friend class InterestRegistryTestAccess;  // component-test creation
    friend class DeliveryAckTestSeam;

    bool
    interest_mint_available() const
    {
        return next_interest_ != std::numeric_limits<InterestId>::max();
    }

    InterestId
    peek_interest_id() const
    {
        return next_interest_;
    }

    /// PREPARE: all allocation happens here, no state changes. The
    /// returned node commits without allocating.
    InterestNode
    prepare_interest(
        InterestId id,
        InterestOperationId operation,
        InterestChildId child,
        InterestFetchId fetch,
        SubscriptionId subscription,
        ParticipantDestination expected)
    {
        InterestMap tmp;
        tmp.emplace(
            id,
            Interest{
                .operation = operation,
                .child = child,
                .fetch = fetch,
                .subscription = subscription,
                .expected = expected});
        return tmp.extract(id);
    }

    /// COMMIT: node insertion is allocation-free; integer comparison
    /// cannot throw. Counter advances only here.
    void
    commit_interest(InterestNode&& node) noexcept
    {
        interests_.insert(std::move(node));
        ++next_interest_;
    }

    Interest const*
    find_row(InterestId id) const
    {
        auto it = interests_.find(id);
        return it == interests_.end() ? nullptr : &it->second;
    }

    bool
    row_terminal_ready(Interest const& row) const
    {
        return row.phase == Phase::terminal_ready;
    }

    bool
    can_add_use(InterestId id, InterestUseId use) const
    {
        auto it = interests_.find(id);
        return it != interests_.end() &&
            it->second.phase == Phase::active &&
            it->second.open_uses.count(use) == 0;
    }

    UseNode
    prepare_use(InterestUseId use)
    {
        UseSet tmp;
        tmp.insert(use);
        return tmp.extract(use);
    }

    void
    commit_add_use(InterestId id, UseNode&& node) noexcept
    {
        interests_.find(id)->second.open_uses.insert(std::move(node));
    }

    bool
    can_close_use(InterestId id, InterestUseId use) const
    {
        auto it = interests_.find(id);
        return it != interests_.end() &&
            it->second.open_uses.count(use) > 0;
    }

    void
    commit_close_use(InterestId id, InterestUseId use) noexcept
    {
        interests_.find(id)->second.open_uses.erase(use);
    }

    void
    erase_interest(InterestId id) noexcept
    {
        interests_.erase(id);
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
    //@@authority state=provider_lab.interest_registry owner=provider_lab lifecycle=coordinator_3c_3d
    InterestMap interests_;
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

/// Test-only row creation for COMPONENT tests of this registry alone.
/// Production rows are created solely by the aggregate façade's
/// atomic open_interest_with_cell; this seam exists so the component
/// suite can exercise interest-local behavior without composing the
/// participant registry. Explicitly not production authority.
class InterestRegistryTestAccess
{
public:
    struct Result
    {
        bool ok = false;
        InterestId interest = 0;
    };

    static Result
    create(
        InterestRegistry& reg,
        InterestOperationId operation,
        InterestChildId child,
        InterestFetchId fetch,
        SubscriptionId subscription,
        ParticipantDestination expected)
    {
        if (!reg.on_owner_strand() || !reg.interest_mint_available())
            return {};
        auto const id = reg.peek_interest_id();
        reg.commit_interest(reg.prepare_interest(
            id, operation, child, fetch, subscription, expected));
        return {true, id};
    }

    static void
    erase(InterestRegistry& reg, InterestId id)
    {
        if (reg.on_owner_strand())
            reg.erase_interest(id);
    }
};

}  // namespace catl::provider_lab
