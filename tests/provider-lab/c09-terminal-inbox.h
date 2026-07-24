#pragma once

// C09 PeerSet receiver verification lab. TEST-ONLY: this is the receiver half
// of the PeerClient -> PeerSet terminal channel.
//
// The receiver facet deliberately has no public member-registration or erase
// verb. Production registration must be part of the future T36 PeerSet
// aggregate commit, and receiver rows cannot be reclaimed until a later
// sender-incarnation fence proves that receipt retransmission has quiesced.

#include "c09-transport-terminal.h"

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

class C09PeerSetTerminalAggregate;
class C11PeerSetGenerationAggregate;
class C09ReceiverTestRegistration;
class C09ReceiverTestSeed;

class C09PeerSetTerminalInbox
{
public:
    using strand_type = DeterministicScheduler::strand_type;

    enum class MemberState : std::uint8_t {
        awaiting_terminal,
        terminal_received,
    };

    enum class RegisterRefusal : std::uint8_t {
        ok,
        off_strand,
        invalid_identity,
        wrong_destination,
        unknown_member,
        already_registered,
        member_binding_conflict,
        receiver_sequence_exhausted,
    };

    enum class Case : std::uint8_t {
        first_commit,
        exact_replay,
        off_strand,
        wrong_destination,
        invalid_identity,
        unknown_member,
        member_binding_conflict,
        command_reuse_conflict,
        member_terminal_conflict,
        receiver_sequence_exhausted,
    };

    struct DispatchResult
    {
        Case result = Case::invalid_identity;
        std::optional<C09TerminalReceipt> receipt;
    };

    struct MemberStateResult
    {
        RegisterRefusal refusal = RegisterRefusal::invalid_identity;
        std::optional<MemberState> state;
    };

    enum class TestFailpoint : std::uint8_t {
        none,
        dispatch_pre_commit,
    };

    C09PeerSetTerminalInbox(strand_type strand, C09Destination destination)
        : strand_(std::move(strand)), destination_(destination)
    {
    }

    C09PeerSetTerminalInbox(C09PeerSetTerminalInbox const&) = delete;
    C09PeerSetTerminalInbox&
    operator=(C09PeerSetTerminalInbox const&) = delete;
    C09PeerSetTerminalInbox(C09PeerSetTerminalInbox&&) = delete;
    C09PeerSetTerminalInbox&
    operator=(C09PeerSetTerminalInbox&&) = delete;

    bool
    on_owner_strand() const
    {
        return strand_.running_in_this_thread();
    }

    //@@authority transition=provider_lab.c09_receive_terminal
    // owner=provider_lab commit=strand
    DispatchResult
    dispatch(C09TerminalCommand const& command)
    {
        if (!on_owner_strand())
            return {Case::off_strand, std::nullopt};
        auto const& identity = command.identity();
        if (identity.member.destination != destination_)
            return {Case::wrong_destination, std::nullopt};
        if (!valid_command_identity(identity) ||
            !valid_outcome(command.outcome()))
            return {Case::invalid_identity, std::nullopt};

        auto* member = find_member(identity.member);
        if (!member)
            return {Case::unknown_member, std::nullopt};
        if (member->identity != identity.member)
            return {Case::member_binding_conflict, std::nullopt};

        auto const dedup_key = command_key(identity);
        if (auto const existing = inbox_.find(dedup_key);
            existing != inbox_.end())
        {
            if (existing->second.command == command)
                return {Case::exact_replay, existing->second.receipt};
            return {Case::command_reuse_conflict, std::nullopt};
        }

        if (member->state == MemberState::terminal_received)
            return {Case::member_terminal_conflict, std::nullopt};
        if (!member->sequence_permit)
            return {Case::receiver_sequence_exhausted, std::nullopt};

        auto const sequence = next_receiver_sequence_ + 1;
        C09TerminalReceipt receipt{identity, command.outcome(), sequence};
        test_fail(TestFailpoint::dispatch_pre_commit);

        auto const [inbox, inserted] =
            inbox_.emplace(dedup_key, InboxRow{command, receipt});
        if (!inserted)
            return {Case::command_reuse_conflict, std::nullopt};

        member->state = MemberState::terminal_received;
        member->sequence_permit = false;
        member->committed_command = dedup_key;
        --reserved_sequence_permits_;
        next_receiver_sequence_ = sequence;
        return {Case::first_commit, inbox->second.receipt};
    }

    MemberStateResult
    member_state(C09MemberIdentity const& identity) const
    {
        if (!on_owner_strand())
            return {RegisterRefusal::off_strand, std::nullopt};
        if (identity.destination != destination_)
            return {RegisterRefusal::wrong_destination, std::nullopt};
        if (!valid_member_identity(identity))
            return {RegisterRefusal::invalid_identity, std::nullopt};
        auto const* member = find_member(identity);
        if (!member)
            return {RegisterRefusal::unknown_member, std::nullopt};
        if (member->identity != identity)
            return {RegisterRefusal::member_binding_conflict, std::nullopt};
        return {RegisterRefusal::ok, member->state};
    }

    std::optional<std::size_t>
    member_count() const
    {
        if (!on_owner_strand())
            return std::nullopt;
        return member_count_;
    }

    std::optional<std::size_t>
    inbox_count() const
    {
        if (!on_owner_strand())
            return std::nullopt;
        return inbox_.size();
    }

    std::optional<C09ReceiverCommitSequence>
    reserved_sequence_permits() const
    {
        if (!on_owner_strand())
            return std::nullopt;
        return reserved_sequence_permits_;
    }

private:
    friend class C09PeerSetTerminalAggregate;
    friend class C11PeerSetGenerationAggregate;
    friend class C09ReceiverTestRegistration;
    friend class C09ReceiverTestSeed;

    struct MemberKey
    {
        C09ChannelGeneration channel_generation = 0;
        C09PipelineId pipeline = 0;

        bool
        operator<(MemberKey const& other) const
        {
            return std::tie(channel_generation, pipeline) <
                std::tie(other.channel_generation, other.pipeline);
        }
    };

    struct CommandKey
    {
        C09PeerClientIncarnation peer_client_incarnation = 0;
        C09SenderGeneration sender_generation = 0;
        C09CommandId command = 0;

        bool
        operator<(CommandKey const& other) const
        {
            return std::tie(
                       peer_client_incarnation, sender_generation, command) <
                std::tie(
                       other.peer_client_incarnation,
                       other.sender_generation,
                       other.command);
        }

        bool
        operator==(CommandKey const&) const = default;
    };

    static_assert(std::is_nothrow_copy_assignable_v<CommandKey>);

    struct MemberRow
    {
        C09MemberIdentity identity;
        MemberState state = MemberState::awaiting_terminal;
        bool sequence_permit = true;
        CommandKey committed_command;
        std::unique_ptr<MemberRow> next;
    };

    struct InboxRow
    {
        C09TerminalCommand command;
        C09TerminalReceipt receipt;
    };

    static MemberKey
    member_key(C09MemberIdentity const& identity)
    {
        return {identity.lineage.channel_generation, identity.lineage.pipeline};
    }

    static CommandKey
    command_key(C09CommandIdentity const& identity)
    {
        return {
            identity.member.peer_client_incarnation,
            identity.member.sender_generation,
            identity.command};
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
    valid_member_identity(C09MemberIdentity const& identity)
    {
        auto const& lineage = identity.lineage;
        return valid_destination(identity.destination) &&
            identity.peer_client_incarnation != 0 &&
            identity.sender_generation != 0 && lineage.operation != 0 &&
            lineage.interest != 0 && lineage.use != 0 &&
            lineage.pipeline != 0 && lineage.channel_generation != 0;
    }

    static bool
    valid_command_identity(C09CommandIdentity const& identity)
    {
        return valid_member_identity(identity.member) &&
            identity.transport_attempt != 0 && identity.command != 0;
    }

    static bool
    valid_outcome(C09TerminalOutcome outcome)
    {
        switch (outcome)
        {
            case C09TerminalOutcome::transport_complete:
            case C09TerminalOutcome::rejected:
            case C09TerminalOutcome::cancelled:
            case C09TerminalOutcome::never_published:
                return true;
        }
        return false;
    }

    MemberRow*
    find_member(C09MemberIdentity const& identity)
    {
        auto const key = member_key(identity);
        for (auto* row = members_.get(); row; row = row->next.get())
        {
            auto const row_key = member_key(row->identity);
            if (!(row_key < key) && !(key < row_key))
                return row;
        }
        return nullptr;
    }

    MemberRow const*
    find_member(C09MemberIdentity const& identity) const
    {
        auto const key = member_key(identity);
        for (auto const* row = members_.get(); row; row = row->next.get())
        {
            auto const row_key = member_key(row->identity);
            if (!(row_key < key) && !(key < row_key))
                return row;
        }
        return nullptr;
    }

    using PreparedRegistration = std::unique_ptr<MemberRow>;

    // Allocating prepare half of the private T36 facet. The future PeerSet
    // aggregate prepares all of its owned homes before committing any of them.
    std::pair<RegisterRefusal, PreparedRegistration>
    prepare_registration(C09MemberIdentity const& identity)
    {
        if (!on_owner_strand())
            return {RegisterRefusal::off_strand, PreparedRegistration{}};
        if (identity.destination != destination_)
            return {RegisterRefusal::wrong_destination, PreparedRegistration{}};
        if (!valid_member_identity(identity))
            return {RegisterRefusal::invalid_identity, PreparedRegistration{}};

        if (auto const* existing = find_member(identity))
            return {
                existing->identity == identity
                    ? RegisterRefusal::already_registered
                    : RegisterRefusal::member_binding_conflict,
                PreparedRegistration{}};

        auto const max = std::numeric_limits<C09ReceiverCommitSequence>::max();
        if (reserved_sequence_permits_ >= max - next_receiver_sequence_)
            return {
                RegisterRefusal::receiver_sequence_exhausted,
                PreparedRegistration{}};

        return {
            RegisterRefusal::ok,
            std::make_unique<MemberRow>(MemberRow{
                identity,
                MemberState::awaiting_terminal,
                true,
                CommandKey{},
                nullptr})};
    }

    // No-throw commit half. Prepare and commit form one unsuspended
    // owner-strand aggregate operation after every other T36 facet prepares.
    //@@authority transition=provider_lab.c09_register_receiver_member
    // owner=provider_lab commit=strand
    void
    commit_registration(PreparedRegistration prepared) noexcept
    {
        prepared->next = std::move(members_);
        members_ = std::move(prepared);
        ++reserved_sequence_permits_;
        ++member_count_;
    }

    void
    test_fail(TestFailpoint point) const
    {
        if (test_failpoint_ == point)
            throw std::runtime_error{"C09 receiver injected failure"};
    }

    //@@authority state=provider_lab.c09_terminal_inbox owner=provider_lab
    strand_type strand_;
    C09Destination destination_;
    C09ReceiverCommitSequence next_receiver_sequence_ = 0;
    C09ReceiverCommitSequence reserved_sequence_permits_ = 0;
    std::size_t member_count_ = 0;
    TestFailpoint test_failpoint_ = TestFailpoint::none;
    std::unique_ptr<MemberRow> members_;
    std::map<CommandKey, InboxRow> inbox_;
};

static_assert(!std::is_copy_constructible_v<C09PeerSetTerminalInbox>);
static_assert(!std::is_move_constructible_v<C09PeerSetTerminalInbox>);
static_assert(std::is_nothrow_copy_assignable_v<C09TerminalCommand>);
static_assert(std::is_nothrow_copy_assignable_v<C09TerminalReceipt>);
static_assert(std::is_nothrow_copy_constructible_v<C09TerminalReceipt>);

/// Explicit standalone-lab seam for the future aggregate-owned T36 facet.
class C09ReceiverTestRegistration
{
public:
    static C09PeerSetTerminalInbox::RegisterRefusal
    register_member(
        C09PeerSetTerminalInbox& inbox,
        C09MemberIdentity const& identity)
    {
        auto [refusal, prepared] = inbox.prepare_registration(identity);
        if (refusal != C09PeerSetTerminalInbox::RegisterRefusal::ok)
            return refusal;
        inbox.commit_registration(std::move(prepared));
        return C09PeerSetTerminalInbox::RegisterRefusal::ok;
    }
};

class C09ReceiverTestSeed
{
public:
    static C09PeerSetTerminalInbox::RegisterRefusal
    next_sequence(
        C09PeerSetTerminalInbox& inbox,
        C09ReceiverCommitSequence next)
    {
        using Refusal = C09PeerSetTerminalInbox::RegisterRefusal;
        if (!inbox.on_owner_strand())
            return Refusal::off_strand;
        if (inbox.members_ || !inbox.inbox_.empty() ||
            inbox.next_receiver_sequence_ != 0 ||
            inbox.reserved_sequence_permits_ != 0)
            return Refusal::already_registered;
        inbox.next_receiver_sequence_ = next;
        return Refusal::ok;
    }

    static C09PeerSetTerminalInbox::RegisterRefusal
    fail_at(
        C09PeerSetTerminalInbox& inbox,
        C09PeerSetTerminalInbox::TestFailpoint failpoint)
    {
        using Refusal = C09PeerSetTerminalInbox::RegisterRefusal;
        if (!inbox.on_owner_strand())
            return Refusal::off_strand;
        inbox.test_failpoint_ = failpoint;
        return Refusal::ok;
    }
};

}  // namespace catl::provider_lab
