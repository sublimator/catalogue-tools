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
//  - retry attempts remain owner-local; publication by any attempt latches on
//    the member, so a later retry cannot launder it into never_published;
//  - admission reserves the member's sole command and first attempt identity;
//    retry terminalization reserves its successor before detaching the old;
//  - exactly one final terminal variant per member wins and creates exactly
//    one retained command. Its attempt ID identifies the final attempt;
//    delivery is a projection of that row, never a second mint;
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
#include <stdexcept>
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
class C09SenderTestSeed;
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
        reserved,
        unpublished_open,
        attempt_committed,
        terminal,
        retry_terminal_unpublished,
        retry_terminal_published,
    };

    enum class TestFailpoint : std::uint8_t {
        none,
        open_after_attempt_reserve,
        register_pre_commit,
        terminal_pre_commit,
        receipt_pre_commit,
    };

    enum class Refusal : std::uint8_t {
        ok,
        off_strand,
        invalid_identity,
        unknown_member,
        already_open,
        member_sealed,
        member_attempt_active,
        invalid_lease,
        wrong_sender,
        unknown_attempt,
        attempt_already_committed,
        attempt_not_committed,
        attempt_already_terminal,
        attempt_retired,
        never_published_after_attempt,
        receiver_sequence_invalid,
        receipt_scope_mismatch,
        receipt_binding_mismatch,
        unknown_settlement,
        id_exhausted,
        seed_not_pristine,
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

    struct SettlementResult
    {
        Refusal refusal = Refusal::invalid_identity;
        std::optional<C09TerminalReceipt> receipt;
    };

    struct AttemptStateResult
    {
        Refusal refusal = Refusal::invalid_identity;
        std::optional<AttemptState> state;
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

    C09PeerClientTerminalOutbox(C09PeerClientTerminalOutbox const&) = delete;
    C09PeerClientTerminalOutbox&
    operator=(C09PeerClientTerminalOutbox const&) = delete;
    C09PeerClientTerminalOutbox(C09PeerClientTerminalOutbox&&) = delete;
    C09PeerClientTerminalOutbox&
    operator=(C09PeerClientTerminalOutbox&&) = delete;

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
        auto const key = member_key(member);
        auto const existing = members_.find(key);
        if (existing != members_.end())
            return {Refusal::already_open, existing->second.member};
        if (next_command_id_ == std::numeric_limits<std::uint64_t>::max() ||
            next_attempt_id_ == std::numeric_limits<std::uint64_t>::max())
            return {Refusal::id_exhausted, std::nullopt};
        auto const reserved_command = next_command_id_ + 1;
        auto const reserved_attempt = next_attempt_id_ + 1;
        auto const [_, attempt_inserted] = attempts_.emplace(
            reserved_attempt,
            AttemptRow{
                member,
                reserved_attempt,
                0,
                AttemptState::reserved,
                std::nullopt});
        if (!attempt_inserted)
            return {Refusal::id_exhausted, std::nullopt};
        try
        {
            test_fail(TestFailpoint::open_after_attempt_reserve);
            auto const [member_it, inserted] = members_.emplace(
                key,
                MemberRow{
                    member,
                    reserved_command,
                    reserved_attempt,
                    false,
                    false,
                    std::nullopt,
                    {}});
            if (!inserted)
            {
                attempts_.erase(reserved_attempt);
                return {Refusal::already_open, member_it->second.member};
            }
        }
        catch (...)
        {
            attempts_.erase(reserved_attempt);
            throw;
        }
        next_command_id_ = reserved_command;
        next_attempt_id_ = reserved_attempt;
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
        if (member_it->second.active_attempt)
            return {Refusal::member_attempt_active, {}};
        if (!member_it->second.reserved_attempt)
            return {Refusal::id_exhausted, {}};

        auto const attempt = *member_it->second.reserved_attempt;
        auto attempt_it = attempts_.find(attempt);
        if (attempt_it == attempts_.end() ||
            attempt_it->second.member != member ||
            attempt_it->second.state != AttemptState::reserved)
            return {Refusal::unknown_attempt, {}};
        test_fail(TestFailpoint::register_pre_commit);
        attempt_it->second.payload = payload;
        attempt_it->second.state = AttemptState::unpublished_open;
        member_it->second.active_attempt = attempt;
        member_it->second.reserved_attempt.reset();
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
            return missing_attempt_refusal(lease);
        if (row->state == AttemptState::attempt_committed)
            return Refusal::attempt_already_committed;
        if (row->state == AttemptState::terminal)
            return Refusal::attempt_already_terminal;
        if (is_retry_terminal(row->state))
            return Refusal::attempt_retired;
        auto* member = find_member(row->member);
        if (!member)
            return Refusal::unknown_member;
        row->state = AttemptState::attempt_committed;
        member->ever_published = true;
        return Refusal::ok;
    }

    /// Commit the owner-local terminal state of a retryable attempt. This
    /// crosses no actor boundary and therefore creates no C09 command. The
    /// retained row keeps delayed callbacks owned until the later fence.
    //@@authority transition=provider_lab.c09_commit_retry_terminal
    // owner=provider_lab commit=strand
    Refusal
    commit_retry_terminal(C09AttemptLease const& lease)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        auto it = find_attempt_iterator(lease);
        if (it == attempts_.end())
            return missing_attempt_refusal(lease);
        if (it->second.state == AttemptState::terminal)
            return Refusal::attempt_already_terminal;
        if (is_retry_terminal(it->second.state))
            return Refusal::attempt_retired;
        auto* member = find_member(it->second.member);
        if (!member)
            return Refusal::unknown_member;
        if (member->active_attempt != lease.id())
            return Refusal::unknown_attempt;
        if (next_attempt_id_ == std::numeric_limits<std::uint64_t>::max())
            return Refusal::id_exhausted;
        auto const successor = next_attempt_id_ + 1;
        auto const [_, inserted] = attempts_.emplace(
            successor,
            AttemptRow{
                it->second.member,
                successor,
                0,
                AttemptState::reserved,
                std::nullopt});
        if (!inserted)
            return Refusal::id_exhausted;
        it->second.state = it->second.state == AttemptState::attempt_committed
            ? AttemptState::retry_terminal_published
            : AttemptState::retry_terminal_unpublished;
        member->active_attempt.reset();
        member->reserved_attempt = successor;
        next_attempt_id_ = successor;
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

    /// Cancellation is the exact owner-local absence proof. It selects
    /// never_published only if no attempt for this member ever committed
    /// publication. Otherwise it selects the member's cancelled variant, even
    /// when the final retry itself remained unpublished.
    //@@authority transition=provider_lab.c09_cancel_transport
    // owner=provider_lab commit=strand
    TerminalResult
    cancel(C09AttemptLease const& lease)
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, std::nullopt};
        auto* row = find_attempt(lease);
        if (!row)
            return {missing_attempt_refusal(lease), std::nullopt};
        if (row->state == AttemptState::terminal)
            return {Refusal::attempt_already_terminal, row->command};
        if (is_retry_terminal(row->state))
            return {Refusal::attempt_retired, std::nullopt};
        auto* member = find_member(row->member);
        if (!member)
            return {Refusal::unknown_member, std::nullopt};
        auto const outcome = row->state == AttemptState::unpublished_open &&
                !member->ever_published
            ? C09TerminalOutcome::never_published
            : C09TerminalOutcome::cancelled;
        return commit_terminal(*member, *row, outcome);
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
            return {missing_attempt_refusal(lease), std::nullopt};
        auto* member = find_member(row->member);
        if (!member)
            return {Refusal::unknown_member, std::nullopt};
        if (row->state == AttemptState::terminal)
            return {Refusal::attempt_already_terminal, row->command};
        if (is_retry_terminal(row->state))
            return {Refusal::attempt_retired, std::nullopt};
        if (member->ever_published ||
            row->state == AttemptState::attempt_committed)
            return {Refusal::never_published_after_attempt, std::nullopt};
        return commit_terminal(
            *member, *row, C09TerminalOutcome::never_published);
    }

    ReplayResult
    replay(C09AttemptLease const& lease) const
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, std::nullopt};
        auto const* row = find_attempt(lease);
        if (!row)
            return {missing_attempt_refusal(lease), std::nullopt};
        if (is_retry_terminal(row->state))
            return {Refusal::attempt_retired, std::nullopt};
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
        auto const& receipt_member = receipt.identity().member;
        if (receipt_member.destination != destination_ ||
            receipt_member.peer_client_incarnation !=
                peer_client_incarnation_ ||
            receipt_member.sender_generation != sender_generation_)
            return Refusal::receipt_scope_mismatch;
        auto it = attempts_.find(receipt.identity().transport_attempt);
        if (it == attempts_.end())
            return Refusal::unknown_attempt;
        auto const& row = it->second;
        if (!row.command || row.command->identity() != receipt.identity() ||
            row.command->outcome() != receipt.outcome())
            return Refusal::receipt_binding_mismatch;

        auto member_it = members_.find(member_key(row.member));
        if (member_it == members_.end())
            return Refusal::unknown_member;
        test_fail(TestFailpoint::receipt_pre_commit);
        member_it->second.settlements.emplace(
            receipt.identity().command, receipt);
        member_it->second.active_attempt.reset();
        attempts_.erase(it);
        return Refusal::ok;
    }

    SettlementResult
    settlement(C09MemberIdentity const& member, C09CommandId command) const
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, std::nullopt};
        auto const* row = find_member(member);
        if (!row)
            return {Refusal::unknown_member, std::nullopt};
        auto const it = row->settlements.find(command);
        if (it == row->settlements.end())
            return {Refusal::unknown_settlement, std::nullopt};
        return {Refusal::ok, it->second};
    }

    AttemptStateResult
    attempt_state(C09AttemptLease const& lease) const
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, std::nullopt};
        auto const* row = find_attempt(lease);
        if (!row)
            return {missing_attempt_refusal(lease), std::nullopt};
        return {Refusal::ok, row->state};
    }

    std::optional<std::size_t>
    outbox_count() const
    {
        if (!on_owner_strand())
            return std::nullopt;
        std::size_t count = 0;
        for (auto const& [_, row] : attempts_)
            count += row.command.has_value() ? 1u : 0u;
        return count;
    }

    std::optional<std::size_t>
    attempt_count() const
    {
        if (!on_owner_strand())
            return std::nullopt;
        return attempts_.size();
    }

    std::optional<std::size_t>
    live_attempt_count() const
    {
        if (!on_owner_strand())
            return std::nullopt;
        std::size_t count = 0;
        for (auto const& [_, row] : attempts_)
            count += row.state == AttemptState::reserved ||
                    row.state == AttemptState::unpublished_open ||
                    row.state == AttemptState::attempt_committed
                ? 1u
                : 0u;
        return count;
    }

    std::optional<std::size_t>
    retry_history_count() const
    {
        if (!on_owner_strand())
            return std::nullopt;
        std::size_t count = 0;
        for (auto const& [_, row] : attempts_)
            count += is_retry_terminal(row.state) ? 1u : 0u;
        return count;
    }

private:
    friend class C09SenderTestSeed;

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

    struct MemberRow
    {
        C09MemberIdentity member;
        C09CommandId reserved_command = 0;
        std::optional<C09TransportAttemptId> reserved_attempt;
        bool ever_published = false;
        bool sealed = false;
        std::optional<C09TransportAttemptId> active_attempt;
        std::map<C09CommandId, C09TerminalReceipt> settlements;
    };

    struct AttemptRow
    {
        C09MemberIdentity member;
        C09TransportAttemptId attempt = 0;
        C09PayloadDigest payload = 0;
        AttemptState state = AttemptState::unpublished_open;
        std::optional<C09TerminalCommand> command;
    };

    static bool
    is_retry_terminal(AttemptState state)
    {
        return state == AttemptState::retry_terminal_unpublished ||
            state == AttemptState::retry_terminal_published;
    }

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
        return lease.valid() && lease.owner_ != owner_token_;
    }

    Refusal
    missing_attempt_refusal(C09AttemptLease const& lease) const
    {
        if (!lease.valid())
            return Refusal::invalid_lease;
        if (foreign(lease))
            return Refusal::wrong_sender;
        return Refusal::unknown_attempt;
    }

    MemberRow*
    find_member(C09MemberIdentity const& member)
    {
        auto it = members_.find(member_key(member));
        if (it == members_.end() || it->second.member != member)
            return nullptr;
        return &it->second;
    }

    MemberRow const*
    find_member(C09MemberIdentity const& member) const
    {
        auto it = members_.find(member_key(member));
        if (it == members_.end() || it->second.member != member)
            return nullptr;
        return &it->second;
    }

    std::map<C09TransportAttemptId, AttemptRow>::iterator
    find_attempt_iterator(C09AttemptLease const& lease)
    {
        if (!lease.valid() || foreign(lease))
            return attempts_.end();
        return attempts_.find(lease.attempt_);
    }

    AttemptRow*
    find_attempt(C09AttemptLease const& lease)
    {
        if (!lease.valid() || foreign(lease))
            return nullptr;
        auto it = attempts_.find(lease.attempt_);
        return it == attempts_.end() ? nullptr : &it->second;
    }

    AttemptRow const*
    find_attempt(C09AttemptLease const& lease) const
    {
        if (!lease.valid() || foreign(lease))
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
            return {missing_attempt_refusal(lease), std::nullopt};
        if (row->state == AttemptState::unpublished_open)
            return {Refusal::attempt_not_committed, std::nullopt};
        if (row->state == AttemptState::terminal)
            return {Refusal::attempt_already_terminal, row->command};
        if (is_retry_terminal(row->state))
            return {Refusal::attempt_retired, std::nullopt};
        auto* member = find_member(row->member);
        if (!member)
            return {Refusal::unknown_member, std::nullopt};
        return commit_terminal(*member, *row, outcome);
    }

    TerminalResult
    commit_terminal(
        MemberRow& member,
        AttemptRow& row,
        C09TerminalOutcome outcome)
    {
        C09TerminalCommand command{
            C09CommandIdentity{
                row.member, row.attempt, member.reserved_command, row.payload},
            outcome};
        test_fail(TestFailpoint::terminal_pre_commit);
        row.command = command;
        row.state = AttemptState::terminal;
        member.sealed = true;
        member.active_attempt.reset();
        return {Refusal::ok, std::move(command)};
    }

    void
    test_fail(TestFailpoint point) const
    {
        if (test_failpoint_ == point)
            throw std::runtime_error{"C09 injected pre-commit failure"};
    }

    //@@authority state=provider_lab.c09_terminal_outbox owner=provider_lab
    strand_type strand_;
    C09Destination destination_;
    C09PeerClientIncarnation peer_client_incarnation_ = 0;
    C09SenderGeneration sender_generation_ = 0;
    TestFailpoint test_failpoint_ = TestFailpoint::none;
    std::shared_ptr<void const> owner_token_ = std::make_shared<int>(0);
    C09TransportAttemptId next_attempt_id_ = 0;
    C09CommandId next_command_id_ = 0;
    std::map<MemberKey, MemberRow> members_;
    std::map<C09TransportAttemptId, AttemptRow> attempts_;
};

/// Owner-strand-only seam for exercising identifier ceilings without billions
/// of registrations. It cannot rewrite counters while an attempt is live.
class C09SenderTestSeed
{
public:
    static C09PeerClientTerminalOutbox::Refusal
    fail_at(
        C09PeerClientTerminalOutbox& sender,
        C09PeerClientTerminalOutbox::TestFailpoint failpoint)
    {
        using Refusal = C09PeerClientTerminalOutbox::Refusal;
        if (!sender.on_owner_strand())
            return Refusal::off_strand;
        sender.test_failpoint_ = failpoint;
        return Refusal::ok;
    }

    static C09PeerClientTerminalOutbox::Refusal
    next_ids(
        C09PeerClientTerminalOutbox& sender,
        C09TransportAttemptId next_attempt_id,
        C09CommandId next_command_id)
    {
        using Refusal = C09PeerClientTerminalOutbox::Refusal;
        if (!sender.on_owner_strand())
            return Refusal::off_strand;
        if (!sender.attempts_.empty())
            return Refusal::member_attempt_active;
        if (sender.next_attempt_id_ != 0 || sender.next_command_id_ != 0)
            return Refusal::seed_not_pristine;
        sender.next_attempt_id_ = next_attempt_id;
        sender.next_command_id_ = next_command_id;
        return Refusal::ok;
    }
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
