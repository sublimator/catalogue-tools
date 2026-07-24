#pragma once

// C09 transport-terminal verification lab. TEST-ONLY: this is the first
// implementation contact for the PeerClient -> PeerSet terminal boundary.
//
// This increment owns only the PeerClient sender side:
//  - one strand owns member registration, attempt publication permission,
//    terminal selection, and the replayable terminal-command outbox;
//  - the command identity is complete across provider/partition, both actor
//    incarnations, operation/member lineage, transport attempt, command, and
//    payload;
//  - transport_complete/rejected/cancelled require a committed attempt;
//    cancellation before publication atomically seals never_published;
//  - exactly one terminal variant wins and creates exactly one retained
//    command. Delivery is a projection of that row, never a second mint;
//  - only a receiver-minted, fully bound receipt consumes the outbox.
//
// The PeerSet inbox, finish-ready derivation, and retention fence are separate
// increments. Keeping them out of this aggregate is deliberate: neither owner
// may inspect or mutate the other's containers.

#include "deterministic-scheduler.h"

#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <tuple>
#include <type_traits>
#include <utility>

namespace catl::provider_lab {

using C09ProviderId = std::uint64_t;
using C09ProviderEpoch = std::uint64_t;
using C09PartitionId = std::uint64_t;
using C09PeerSetIncarnation = std::uint64_t;
using C09PeerClientIncarnation = std::uint64_t;
using C09ReceiverGeneration = std::uint64_t;
using C09SenderGeneration = std::uint64_t;
using C09OperationId = std::uint64_t;
using C09InterestId = std::uint64_t;
using C09UseId = std::uint64_t;
using C09PipelineId = std::uint64_t;
using C09ChannelGeneration = std::uint64_t;
using C09TransportAttemptId = std::uint64_t;
using C09CommandId = std::uint64_t;
using C09PayloadDigest = std::uint64_t;
using C09ReceiverCommitSequence = std::uint64_t;

struct C09Destination
{
    C09ProviderId provider = 0;
    C09ProviderEpoch provider_epoch = 0;
    C09PartitionId partition = 0;
    C09PeerSetIncarnation peer_set_incarnation = 0;
    C09ReceiverGeneration receiver_generation = 0;

    bool
    operator==(C09Destination const&) const = default;
};

struct C09MemberLineage
{
    C09OperationId operation = 0;
    C09InterestId interest = 0;
    C09UseId use = 0;
    C09PipelineId pipeline = 0;
    C09ChannelGeneration channel_generation = 0;

    bool
    operator==(C09MemberLineage const&) const = default;
};

struct C09MemberIdentity
{
    C09Destination destination;
    C09PeerClientIncarnation peer_client_incarnation = 0;
    C09SenderGeneration sender_generation = 0;
    C09MemberLineage lineage;

    bool
    operator==(C09MemberIdentity const&) const = default;
};

struct C09CommandIdentity
{
    C09MemberIdentity member;
    C09TransportAttemptId transport_attempt = 0;
    C09CommandId command = 0;
    C09PayloadDigest payload = 0;

    bool
    operator==(C09CommandIdentity const&) const = default;
};

enum class C09TerminalOutcome : std::uint8_t {
    transport_complete,
    rejected,
    cancelled,
    never_published,
};

class C09PeerClientTerminalOutbox;
class C09PeerSetTerminalInbox;
class C09TerminalReceiptTestMint;

/// Sender-owned projection of one selected terminal variant. Construction is
/// private: PeerSet may treat possession as evidence that the PeerClient owner
/// committed exactly one terminal outbox row.
class C09TerminalCommand
{
public:
    C09TerminalCommand(C09TerminalCommand const&) = default;
    C09TerminalCommand&
    operator=(C09TerminalCommand const&) = default;

    C09CommandIdentity const&
    identity() const
    {
        return identity_;
    }

    C09TerminalOutcome
    outcome() const
    {
        return outcome_;
    }

    bool
    operator==(C09TerminalCommand const&) const = default;

private:
    friend class C09PeerClientTerminalOutbox;
    C09TerminalCommand(C09CommandIdentity identity, C09TerminalOutcome outcome)
        : identity_(std::move(identity)), outcome_(outcome)
    {
    }

    C09CommandIdentity identity_;
    C09TerminalOutcome outcome_;
};

static_assert(
    !std::is_default_constructible_v<C09TerminalCommand>,
    "a terminal command must come from the PeerClient outbox");
static_assert(
    !std::is_constructible_v<
        C09TerminalCommand,
        C09CommandIdentity,
        C09TerminalOutcome>,
    "raw fields must not create a second terminal-command minter");

/// Receiver-owned acknowledgement of one exact inbox commit. The sender may
/// consume its outbox only when every command field and the selected outcome
/// match. ReceiverCommitSequence zero is reserved.
class C09TerminalReceipt
{
public:
    C09TerminalReceipt(C09TerminalReceipt const&) = default;
    C09TerminalReceipt&
    operator=(C09TerminalReceipt const&) = default;

    C09CommandIdentity const&
    identity() const
    {
        return identity_;
    }

    C09TerminalOutcome
    outcome() const
    {
        return outcome_;
    }

    C09ReceiverCommitSequence
    receiver_commit_sequence() const
    {
        return receiver_commit_sequence_;
    }

    bool
    operator==(C09TerminalReceipt const&) const = default;

private:
    friend class C09PeerSetTerminalInbox;
    friend class C09TerminalReceiptTestMint;
    C09TerminalReceipt(
        C09CommandIdentity identity,
        C09TerminalOutcome outcome,
        C09ReceiverCommitSequence receiver_commit_sequence)
        : identity_(std::move(identity))
        , outcome_(outcome)
        , receiver_commit_sequence_(receiver_commit_sequence)
    {
    }

    C09CommandIdentity identity_;
    C09TerminalOutcome outcome_;
    C09ReceiverCommitSequence receiver_commit_sequence_;
};

static_assert(
    !std::is_default_constructible_v<C09TerminalReceipt>,
    "a terminal receipt must come from the PeerSet inbox");
static_assert(
    !std::is_constructible_v<
        C09TerminalReceipt,
        C09CommandIdentity,
        C09TerminalOutcome,
        C09ReceiverCommitSequence>,
    "raw fields must not create a second terminal-receipt minter");

class C09AttemptLease
{
public:
    C09AttemptLease() = default;
    C09AttemptLease(C09AttemptLease const&) = delete;
    C09AttemptLease&
    operator=(C09AttemptLease const&) = delete;
    C09AttemptLease(C09AttemptLease&&) noexcept = default;
    C09AttemptLease&
    operator=(C09AttemptLease&&) noexcept = default;

    C09TransportAttemptId
    id() const
    {
        return attempt_;
    }

    bool
    valid() const
    {
        return attempt_ != 0 && owner_ != nullptr;
    }

private:
    friend class C09PeerClientTerminalOutbox;
    C09AttemptLease(
        C09TransportAttemptId attempt,
        std::shared_ptr<void const> owner)
        : attempt_(attempt), owner_(std::move(owner))
    {
    }

    C09TransportAttemptId attempt_ = 0;
    std::shared_ptr<void const> owner_;
};

class C09PeerClientTerminalOutbox
{
public:
    using strand_type = DeterministicScheduler::strand_type;

    enum class AttemptState : std::uint8_t {
        unpublished_open,
        attempt_committed,
        terminal,
    };

    enum class Refusal : std::uint8_t {
        ok,
        off_strand,
        invalid_identity,
        unknown_member,
        member_sealed,
        wrong_sender,
        unknown_attempt,
        attempt_already_committed,
        attempt_not_committed,
        attempt_already_terminal,
        never_published_after_attempt,
        receiver_sequence_invalid,
        receipt_binding_mismatch,
        id_exhausted,
    };

    struct MemberResult
    {
        Refusal refusal = Refusal::invalid_identity;
        std::optional<C09MemberIdentity> member;
    };

    struct AttemptResult
    {
        Refusal refusal = Refusal::invalid_identity;
        C09AttemptLease lease;
    };

    struct TerminalResult
    {
        Refusal refusal = Refusal::invalid_identity;
        std::optional<C09TerminalCommand> command;
    };

    struct ReplayResult
    {
        Refusal refusal = Refusal::invalid_identity;
        std::optional<C09TerminalCommand> command;
    };

    C09PeerClientTerminalOutbox(
        strand_type strand,
        C09Destination destination,
        C09PeerClientIncarnation peer_client_incarnation,
        C09SenderGeneration sender_generation)
        : strand_(std::move(strand))
        , destination_(destination)
        , peer_client_incarnation_(peer_client_incarnation)
        , sender_generation_(sender_generation)
    {
    }

    bool
    on_owner_strand() const
    {
        return strand_.running_in_this_thread();
    }

    //@@authority transition=provider_lab.c09_open_member owner=provider_lab
    // commit=strand
    MemberResult
    open_member(C09MemberLineage lineage)
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, std::nullopt};
        if (!valid_destination(destination_) || peer_client_incarnation_ == 0 ||
            sender_generation_ == 0 || !valid_lineage(lineage))
            return {Refusal::invalid_identity, std::nullopt};

        C09MemberIdentity member{
            destination_,
            peer_client_incarnation_,
            sender_generation_,
            lineage};
        members_.try_emplace(member_key(member), MemberRow{member, false, {}});
        return {Refusal::ok, member};
    }

    //@@authority transition=provider_lab.c09_register_attempt
    // owner=provider_lab commit=strand
    AttemptResult
    register_attempt(C09MemberIdentity const& member, C09PayloadDigest payload)
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, {}};
        auto member_it = members_.find(member_key(member));
        if (member_it == members_.end() || member_it->second.member != member)
            return {Refusal::unknown_member, {}};
        if (member_it->second.sealed)
            return {Refusal::member_sealed, {}};
        if (next_attempt_id_ == std::numeric_limits<std::uint64_t>::max() ||
            next_command_id_ == std::numeric_limits<std::uint64_t>::max())
            return {Refusal::id_exhausted, {}};

        auto const attempt = next_attempt_id_ + 1;
        auto const command = next_command_id_ + 1;
        C09CommandIdentity identity{member, attempt, command, payload};
        attempts_.emplace(
            attempt,
            AttemptRow{
                std::move(identity),
                AttemptState::unpublished_open,
                std::nullopt});
        next_attempt_id_ = attempt;
        next_command_id_ = command;
        return {Refusal::ok, C09AttemptLease{attempt, owner_token_}};
    }

    //@@authority transition=provider_lab.c09_publish_attempt owner=provider_lab
    // commit=strand
    Refusal
    publish_attempt(C09AttemptLease const& lease)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        auto* row = find_attempt(lease);
        if (!row)
            return foreign(lease) ? Refusal::wrong_sender
                                  : Refusal::unknown_attempt;
        if (row->state == AttemptState::attempt_committed)
            return Refusal::attempt_already_committed;
        if (row->state == AttemptState::terminal)
            return Refusal::attempt_already_terminal;
        row->state = AttemptState::attempt_committed;
        return Refusal::ok;
    }

    //@@authority transition=provider_lab.c09_complete_transport
    // owner=provider_lab commit=strand
    TerminalResult
    complete(C09AttemptLease const& lease)
    {
        return select_published_terminal(
            lease, C09TerminalOutcome::transport_complete);
    }

    //@@authority transition=provider_lab.c09_reject_transport
    // owner=provider_lab commit=strand
    TerminalResult
    reject(C09AttemptLease const& lease)
    {
        return select_published_terminal(lease, C09TerminalOutcome::rejected);
    }

    /// Cancellation is the exact owner-local absence proof. If publication
    /// has not committed, this one commit consumes the publication permit and
    /// selects never_published. If it has committed, cancellation selects the
    /// published attempt's cancelled terminal variant.
    //@@authority transition=provider_lab.c09_cancel_transport
    // owner=provider_lab commit=strand
    TerminalResult
    cancel(C09AttemptLease const& lease)
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, std::nullopt};
        auto* row = find_attempt(lease);
        if (!row)
            return {
                foreign(lease) ? Refusal::wrong_sender
                               : Refusal::unknown_attempt,
                std::nullopt};
        if (row->state == AttemptState::terminal)
            return {Refusal::attempt_already_terminal, row->command};
        auto const outcome = row->state == AttemptState::unpublished_open
            ? C09TerminalOutcome::never_published
            : C09TerminalOutcome::cancelled;
        return commit_terminal(*row, outcome);
    }

    /// An enqueue/publish failure selects the same sealed absence variant as
    /// pre-publication cancellation, but cannot overwrite a committed attempt.
    //@@authority transition=provider_lab.c09_seal_never_published
    // owner=provider_lab commit=strand
    TerminalResult
    seal_never_published(C09AttemptLease const& lease)
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, std::nullopt};
        auto* row = find_attempt(lease);
        if (!row)
            return {
                foreign(lease) ? Refusal::wrong_sender
                               : Refusal::unknown_attempt,
                std::nullopt};
        if (row->state == AttemptState::attempt_committed)
            return {Refusal::never_published_after_attempt, std::nullopt};
        if (row->state == AttemptState::terminal)
            return {Refusal::attempt_already_terminal, row->command};
        return commit_terminal(*row, C09TerminalOutcome::never_published);
    }

    ReplayResult
    replay(C09AttemptLease const& lease) const
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, std::nullopt};
        auto const* row = find_attempt(lease);
        if (!row)
            return {
                foreign(lease) ? Refusal::wrong_sender
                               : Refusal::unknown_attempt,
                std::nullopt};
        if (!row->command)
            return {Refusal::attempt_not_committed, std::nullopt};
        return {Refusal::ok, row->command};
    }

    //@@authority transition=provider_lab.c09_consume_terminal_receipt
    // owner=provider_lab commit=strand
    Refusal
    consume_receipt(C09TerminalReceipt const& receipt)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        if (receipt.receiver_commit_sequence() == 0)
            return Refusal::receiver_sequence_invalid;
        auto it = attempts_.find(receipt.identity().transport_attempt);
        if (it == attempts_.end())
            return Refusal::unknown_attempt;
        auto const& row = it->second;
        if (!row.command || row.command->identity() != receipt.identity() ||
            row.command->outcome() != receipt.outcome())
            return Refusal::receipt_binding_mismatch;

        auto member_it = members_.find(member_key(row.identity.member));
        if (member_it == members_.end())
            return Refusal::unknown_member;
        member_it->second.settlements.emplace(
            row.identity.command,
            Settlement{
                row.identity.command,
                receipt.receiver_commit_sequence(),
                row.command->outcome()});
        attempts_.erase(it);
        return Refusal::ok;
    }

    std::optional<AttemptState>
    attempt_state(C09AttemptLease const& lease) const
    {
        if (!on_owner_strand())
            return std::nullopt;
        auto const* row = find_attempt(lease);
        return row ? std::optional<AttemptState>{row->state} : std::nullopt;
    }

    std::size_t
    outbox_count() const
    {
        if (!on_owner_strand())
            return 0;
        std::size_t count = 0;
        for (auto const& [_, row] : attempts_)
            count += row.command.has_value() ? 1u : 0u;
        return count;
    }

    std::size_t
    attempt_count() const
    {
        return on_owner_strand() ? attempts_.size() : 0;
    }

private:
    struct MemberKey
    {
        C09OperationId operation = 0;
        C09InterestId interest = 0;
        C09UseId use = 0;
        C09PipelineId pipeline = 0;
        C09ChannelGeneration channel_generation = 0;

        bool
        operator<(MemberKey const& other) const
        {
            return std::tie(
                       operation, interest, use, pipeline, channel_generation) <
                std::tie(
                       other.operation,
                       other.interest,
                       other.use,
                       other.pipeline,
                       other.channel_generation);
        }
    };

    struct Settlement
    {
        C09CommandId command = 0;
        C09ReceiverCommitSequence receiver_commit_sequence = 0;
        C09TerminalOutcome outcome = C09TerminalOutcome::never_published;
    };

    struct MemberRow
    {
        C09MemberIdentity member;
        bool sealed = false;
        std::map<C09CommandId, Settlement> settlements;
    };

    struct AttemptRow
    {
        C09CommandIdentity identity;
        AttemptState state = AttemptState::unpublished_open;
        std::optional<C09TerminalCommand> command;
    };

    static bool
    valid_destination(C09Destination const& destination)
    {
        return destination.provider != 0 && destination.provider_epoch != 0 &&
            destination.partition != 0 &&
            destination.peer_set_incarnation != 0 &&
            destination.receiver_generation != 0;
    }

    static bool
    valid_lineage(C09MemberLineage const& lineage)
    {
        return lineage.operation != 0 && lineage.interest != 0 &&
            lineage.use != 0 && lineage.pipeline != 0;
    }

    static MemberKey
    member_key(C09MemberIdentity const& member)
    {
        return {
            member.lineage.operation,
            member.lineage.interest,
            member.lineage.use,
            member.lineage.pipeline,
            member.lineage.channel_generation};
    }

    bool
    foreign(C09AttemptLease const& lease) const
    {
        return !lease.valid() || lease.owner_ != owner_token_;
    }

    AttemptRow*
    find_attempt(C09AttemptLease const& lease)
    {
        if (foreign(lease))
            return nullptr;
        auto it = attempts_.find(lease.attempt_);
        return it == attempts_.end() ? nullptr : &it->second;
    }

    AttemptRow const*
    find_attempt(C09AttemptLease const& lease) const
    {
        if (foreign(lease))
            return nullptr;
        auto it = attempts_.find(lease.attempt_);
        return it == attempts_.end() ? nullptr : &it->second;
    }

    TerminalResult
    select_published_terminal(
        C09AttemptLease const& lease,
        C09TerminalOutcome outcome)
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, std::nullopt};
        auto* row = find_attempt(lease);
        if (!row)
            return {
                foreign(lease) ? Refusal::wrong_sender
                               : Refusal::unknown_attempt,
                std::nullopt};
        if (row->state == AttemptState::unpublished_open)
            return {Refusal::attempt_not_committed, std::nullopt};
        if (row->state == AttemptState::terminal)
            return {Refusal::attempt_already_terminal, row->command};
        return commit_terminal(*row, outcome);
    }

    static TerminalResult
    commit_terminal(AttemptRow& row, C09TerminalOutcome outcome)
    {
        C09TerminalCommand command{row.identity, outcome};
        row.command = command;
        row.state = AttemptState::terminal;
        return {Refusal::ok, std::move(command)};
    }

    //@@authority state=provider_lab.c09_terminal_outbox owner=provider_lab
    strand_type strand_;
    C09Destination destination_;
    C09PeerClientIncarnation peer_client_incarnation_ = 0;
    C09SenderGeneration sender_generation_ = 0;
    std::shared_ptr<void const> owner_token_ = std::make_shared<int>(0);
    C09TransportAttemptId next_attempt_id_ = 0;
    C09CommandId next_command_id_ = 0;
    std::map<MemberKey, MemberRow> members_;
    std::map<C09TransportAttemptId, AttemptRow> attempts_;
};

/// Explicit test-only receiver seam. Production construction remains private.
class C09TerminalReceiptTestMint
{
public:
    static C09TerminalReceipt
    mint(
        C09CommandIdentity identity,
        C09TerminalOutcome outcome,
        C09ReceiverCommitSequence receiver_commit_sequence)
    {
        return C09TerminalReceipt{
            std::move(identity), outcome, receiver_commit_sequence};
    }
};

}  // namespace catl::provider_lab
