#pragma once

// Cross-component artifacts for the coordinator increments (3c/3d and
// the operation-ledger seam). TEST-ONLY: production construction must
// never reference this.
//
// Review rulings folded in (coordinator 3c/3d composed review):
//  - every artifact that crosses a component boundary carries the
//    COMPLETE destination key — partition scope, provider epoch,
//    interest, and target participant revision where applicable — so
//    a delayed command from a retired cell instance, or an artifact
//    from a different registry instance with colliding local
//    counters, is refused by binding, not by luck. Counter uniqueness
//    is scoped to one registry instance and is stated as such;
//  - possession of an outbox's own OUTGOING message proves
//    production, never downstream consumption. Each handoff completes
//    in three phases: sender-owned command/outbox, receiver-minted
//    bound receipt, and a DISTINCT receiver-minted consumption
//    witness that alone may retire the sender's home. The consumed
//    witnesses here are those third-phase artifacts.

#include <cstdint>
#include <type_traits>

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
using ProviderId = std::uint64_t;
using PartitionId = std::uint64_t;
using ProviderEpochId = std::uint64_t;

class InterestRegistry;
class ParticipantCellRegistry;
class CoordinatorInterestSeams;
class OperationLedger;
class ParticipantReceiptTestMint;
class ParticipantConsumedTestMint;
class UnsubscribeReceiptTestMint;
class RevokeCommandTestMint;
class TerminalAckTestMint;
class TerminalConsumedTestMint;

/// Instance identity: which provider/partition a registry belongs to.
/// A configuration VALUE, not authority — brace-constructible.
struct PartitionScope
{
    ProviderId provider = 0;
    PartitionId partition = 0;

    bool
    operator==(PartitionScope const&) const = default;
};

/// The interest's retained EXPECTATION of its participant cell:
/// where the cell lives and which instance (revision) it is. Set at
/// registration from the already-opened cell; every revoke artifact
/// is validated against it. Data, not authority.
struct ParticipantDestination
{
    PartitionScope scope;
    ProviderEpochId epoch = 0;
    ParticipantRevision revision = 0;

    bool
    operator==(ParticipantDestination const&) const = default;
};

/// Sender-owned revoke command: a replayable projection from the
/// interest's retained outbox — losing a copy is safe; replay
/// re-derives the identical value. Sole-minted nonetheless: the
/// participant cell treats a presented command as the trigger to
/// revoke. Carries the COMPLETE destination (scope, epoch, target
/// revision) so stale-instance and cross-registry commands refuse.
class RevokeCommand
{
public:
    RevokeCommand(RevokeCommand const&) = default;
    RevokeCommand&
    operator=(RevokeCommand const&) = default;

    RevokeCommandId
    command() const
    {
        return command_;
    }
    InterestId
    interest() const
    {
        return interest_;
    }
    InterestFetchId
    fetch() const
    {
        return fetch_;
    }
    InterestOperationId
    operation() const
    {
        return operation_;
    }
    ParticipantDestination
    destination() const
    {
        return destination_;
    }

    bool
    operator==(RevokeCommand const&) const = default;

private:
    friend class InterestRegistry;       // sole production minter
    friend class RevokeCommandTestMint;  // explicit test-only seam
    RevokeCommand(
        RevokeCommandId command,
        InterestId interest,
        InterestFetchId fetch,
        InterestOperationId operation,
        ParticipantDestination destination)
        : command_(command)
        , interest_(interest)
        , fetch_(fetch)
        , operation_(operation)
        , destination_(destination)
    {
    }

    RevokeCommandId command_;
    InterestId interest_;
    InterestFetchId fetch_;
    InterestOperationId operation_;
    ParticipantDestination destination_;
};

static_assert(
    !std::is_default_constructible_v<RevokeCommand>,
    "a command must be minted from the owned outbox, never defaulted");
static_assert(
    !std::is_constructible_v<
        RevokeCommand,
        RevokeCommandId,
        InterestId,
        InterestFetchId,
        InterestOperationId,
        ParticipantDestination>,
    "raw-field construction would let any caller trigger revocation");

/// Bound receipt proving the participant cell revoked its grants for
/// this exact command, at this exact destination and revision. Sole
/// production minter: ParticipantCellRegistry. Phase two of three —
/// consuming it yields the distinct ParticipantReceiptConsumed.
class ParticipantRevokedReceipt
{
public:
    ParticipantRevokedReceipt(ParticipantRevokedReceipt const&) = default;
    ParticipantRevokedReceipt&
    operator=(ParticipantRevokedReceipt const&) = default;

    PartitionScope
    scope() const
    {
        return scope_;
    }
    ProviderEpochId
    epoch() const
    {
        return epoch_;
    }
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

    bool
    operator==(ParticipantRevokedReceipt const&) const = default;

private:
    friend class ParticipantCellRegistry;  // sole production minter
    friend class ParticipantReceiptTestMint;  // explicit test-only seam
    ParticipantRevokedReceipt(
        PartitionScope scope,
        ProviderEpochId epoch,
        InterestId interest,
        RevokeCommandId command,
        ParticipantRevision revision)
        : scope_(scope)
        , epoch_(epoch)
        , interest_(interest)
        , command_(command)
        , revision_(revision)
    {
    }

    PartitionScope scope_;
    ProviderEpochId epoch_;
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
        PartitionScope,
        ProviderEpochId,
        InterestId,
        RevokeCommandId,
        ParticipantRevision>,
    "raw-field construction would make every caller a second minter");

/// Phase three of the revoke handoff: the INTEREST's witness that it
/// consumed the participant's revoked receipt. Sole production
/// minter: InterestRegistry. Only THIS may retire the participant
/// cell — possession of the cell's own outgoing receipt proves
/// production, not consumption.
class ParticipantReceiptConsumed
{
public:
    ParticipantReceiptConsumed(ParticipantReceiptConsumed const&) = default;
    ParticipantReceiptConsumed&
    operator=(ParticipantReceiptConsumed const&) = default;

    PartitionScope
    scope() const
    {
        return scope_;
    }
    ProviderEpochId
    epoch() const
    {
        return epoch_;
    }
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

    bool
    operator==(ParticipantReceiptConsumed const&) const = default;

private:
    friend class InterestRegistry;  // sole production minter
    friend class ParticipantConsumedTestMint;  // explicit test-only seam
    ParticipantReceiptConsumed(
        PartitionScope scope,
        ProviderEpochId epoch,
        InterestId interest,
        RevokeCommandId command,
        ParticipantRevision revision)
        : scope_(scope)
        , epoch_(epoch)
        , interest_(interest)
        , command_(command)
        , revision_(revision)
    {
    }

    PartitionScope scope_;
    ProviderEpochId epoch_;
    InterestId interest_;
    RevokeCommandId command_;
    ParticipantRevision revision_;
};

static_assert(
    !std::is_default_constructible_v<ParticipantReceiptConsumed>,
    "a consumption witness must be minted, never defaulted");
static_assert(
    !std::is_constructible_v<
        ParticipantReceiptConsumed,
        PartitionScope,
        ProviderEpochId,
        InterestId,
        RevokeCommandId,
        ParticipantRevision>,
    "raw-field construction would fake downstream consumption");

/// Bound receipt proving the cancellation-subscription owner detached
/// this exact subscription. Sole production minter: the subscription
/// owner (NOT yet composed — tests mint via the seam, and the interest
/// makes NO claim that an unsubscribe was initiated; see the honesty
/// note on InterestRegistry::begin_detach). Cross-component artifact,
/// so the schema carries PartitionScope even while its producer is
/// deferred; any lifecycle generation the subscription owner needs to
/// prevent reuse is that increment's obligation, stated here so it is
/// not forgotten.
class UnsubscribeReceipt
{
public:
    UnsubscribeReceipt(UnsubscribeReceipt const&) = default;
    UnsubscribeReceipt&
    operator=(UnsubscribeReceipt const&) = default;

    PartitionScope
    scope() const
    {
        return scope_;
    }
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
    UnsubscribeReceipt(
        PartitionScope scope,
        InterestId interest,
        SubscriptionId subscription)
        : scope_(scope), interest_(interest), subscription_(subscription)
    {
    }

    PartitionScope scope_;
    InterestId interest_;
    SubscriptionId subscription_;
};

static_assert(
    !std::is_default_constructible_v<UnsubscribeReceipt>,
    "a receipt must be minted, never defaulted");
static_assert(
    !std::is_constructible_v<
        UnsubscribeReceipt,
        PartitionScope,
        InterestId,
        SubscriptionId>,
    "raw-field construction would make every caller a second minter");

/// Sole-minted terminal acknowledgement, bound to the original child
/// AND the minting registry's partition scope. Minted once at terminal
/// readiness, retained in the interest's outbox, replayed identically.
/// Phase two of three: the operation ledger consumes it and mints the
/// distinct InterestTerminalConsumed; the ack itself can NOT retire
/// the interest.
class InterestTerminalAck
{
public:
    InterestTerminalAck(InterestTerminalAck const&) = default;
    InterestTerminalAck&
    operator=(InterestTerminalAck const&) = default;

    PartitionScope
    scope() const
    {
        return scope_;
    }
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
    friend class InterestRegistry;      // sole production minter
    friend class TerminalAckTestMint;   // explicit test-only seam
    InterestTerminalAck(
        PartitionScope scope,
        InterestId interest,
        InterestOperationId operation,
        InterestChildId child)
        : scope_(scope)
        , interest_(interest)
        , operation_(operation)
        , child_(child)
    {
    }

    PartitionScope scope_;
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
        PartitionScope,
        InterestId,
        InterestOperationId,
        InterestChildId>,
    "raw-field construction would make every caller a second minter");

/// Phase three of the terminal handoff: the OPERATION LEDGER's witness
/// that it consumed the interest's terminal ack. Sole production
/// minter: OperationLedger. Only THIS may remove the terminal-ready
/// interest.
class InterestTerminalConsumed
{
public:
    InterestTerminalConsumed(InterestTerminalConsumed const&) = default;
    InterestTerminalConsumed&
    operator=(InterestTerminalConsumed const&) = default;

    PartitionScope
    scope() const
    {
        return scope_;
    }
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
    operator==(InterestTerminalConsumed const&) const = default;

private:
    friend class OperationLedger;           // sole production minter
    friend class TerminalConsumedTestMint;  // explicit test-only seam
    InterestTerminalConsumed(
        PartitionScope scope,
        InterestId interest,
        InterestOperationId operation,
        InterestChildId child)
        : scope_(scope)
        , interest_(interest)
        , operation_(operation)
        , child_(child)
    {
    }

    PartitionScope scope_;
    InterestId interest_;
    InterestOperationId operation_;
    InterestChildId child_;
};

static_assert(
    !std::is_default_constructible_v<InterestTerminalConsumed>,
    "a consumption witness must be minted, never defaulted");
static_assert(
    !std::is_constructible_v<
        InterestTerminalConsumed,
        PartitionScope,
        InterestId,
        InterestOperationId,
        InterestChildId>,
    "raw-field construction would fake downstream consumption");

// Test-only mints for every artifact whose production minter is either
// deferred or under adversarial test. Explicitly not production
// authority; no authority annotations.

class RevokeCommandTestMint
{
public:
    static RevokeCommand
    mint(
        RevokeCommandId command,
        InterestId interest,
        InterestFetchId fetch,
        InterestOperationId operation,
        ParticipantDestination destination)
    {
        return RevokeCommand{
            command, interest, fetch, operation, destination};
    }
};

class ParticipantReceiptTestMint
{
public:
    static ParticipantRevokedReceipt
    mint(
        PartitionScope scope,
        ProviderEpochId epoch,
        InterestId interest,
        RevokeCommandId command,
        ParticipantRevision revision)
    {
        return ParticipantRevokedReceipt{
            scope, epoch, interest, command, revision};
    }
};

class ParticipantConsumedTestMint
{
public:
    static ParticipantReceiptConsumed
    mint(
        PartitionScope scope,
        ProviderEpochId epoch,
        InterestId interest,
        RevokeCommandId command,
        ParticipantRevision revision)
    {
        return ParticipantReceiptConsumed{
            scope, epoch, interest, command, revision};
    }
};

class UnsubscribeReceiptTestMint
{
public:
    static UnsubscribeReceipt
    mint(
        PartitionScope scope,
        InterestId interest,
        SubscriptionId subscription)
    {
        return UnsubscribeReceipt{scope, interest, subscription};
    }
};

class TerminalAckTestMint
{
public:
    static InterestTerminalAck
    mint(
        PartitionScope scope,
        InterestId interest,
        InterestOperationId operation,
        InterestChildId child)
    {
        return InterestTerminalAck{scope, interest, operation, child};
    }
};

class TerminalConsumedTestMint
{
public:
    static InterestTerminalConsumed
    mint(
        PartitionScope scope,
        InterestId interest,
        InterestOperationId operation,
        InterestChildId child)
    {
        return InterestTerminalConsumed{scope, interest, operation, child};
    }
};

}  // namespace catl::provider_lab
