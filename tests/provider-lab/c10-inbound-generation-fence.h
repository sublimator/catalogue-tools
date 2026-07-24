#pragma once

// C10 inbound-generation fence verification lab. TEST-ONLY.
//
// One PeerClient owner strand holds one generation-lifetime lifecycle root and
// privately owns its C09 terminal outbox. Admission and fence installation are
// synchronous owner-strand commits:
//
//  - while open, a complete member identity may be accepted;
//  - a fence freezes one exact member snapshot only after proving that it
//    contains every member already opened in the authoritative C09 outbox;
//  - at the fence, every expected member absent from C09 is materialized with
//    the sole owned never-published terminal path;
//  - after the fence, an exact delayed acceptance therefore finds the retained
//    member, while any identity absent from the snapshot is refused;
//  - the fence does not stop the transport lifecycle of accepted members.
//
// This increment deliberately has no retirement operation. C11 must mint the
// bound settlement artifact that authorizes retirement; exposing a raw
// retire() here would create a second lifecycle authority.

#include "c09-transport-terminal.h"

#include <cstdint>
#include <map>
#include <optional>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace catl::provider_lab {

using C10FenceId = std::uint64_t;

/// Receiver-side projection of a C10 fence command. The complete C10
/// sender-owned command/inbox/receipt channel is a later increment; this value
/// is intentionally named Candidate so possession alone grants no authority.
struct C10InboundFenceCandidate
{
    C09Destination destination;
    C09PeerClientIncarnation peer_client_incarnation = 0;
    C09SenderGeneration sender_generation = 0;
    C10FenceId fence = 0;
    std::vector<C09MemberIdentity> expected_members;

    bool
    operator==(C10InboundFenceCandidate const&) const = default;
};

class C10LifecycleTestSeed;

//@@authority state=provider_lab.c10_peer_client_generation_lifecycle
// owner=provider_lab
class C10PeerClientGenerationLifecycle
{
public:
    using strand_type = DeterministicScheduler::strand_type;
    using TerminalOutbox = C09PeerClientTerminalOutbox;

    enum class Mode : std::uint8_t {
        open,
        fenced,
    };

    enum class Refusal : std::uint8_t {
        ok,
        off_strand,
        invalid_member_identity,
        foreign_member,
        member_not_expected,
        member_identity_conflict,
        unknown_member,
        already_open,
        id_exhausted,
        invalid_fence,
        foreign_fence,
        duplicate_snapshot_member,
        snapshot_identity_conflict,
        incomplete_snapshot,
        already_fenced,
        fence_conflict,
        generation_already_fenced,
    };

    enum class TestFailpoint : std::uint8_t {
        none,
        fence_pre_commit,
    };

    struct AcceptResult
    {
        Refusal refusal = Refusal::invalid_member_identity;
        std::optional<C09MemberIdentity> member;
    };

    struct FenceResult
    {
        Refusal refusal = Refusal::invalid_fence;
        C10FenceId fence = 0;
    };

    struct ReservationResult
    {
        Refusal refusal = Refusal::invalid_member_identity;
        std::optional<TerminalOutbox::MemberReservation> reservation;
    };

    C10PeerClientGenerationLifecycle(
        strand_type strand,
        C09Destination destination,
        C09PeerClientIncarnation peer_client_incarnation,
        C09SenderGeneration sender_generation)
        : strand_(strand)
        , destination_(destination)
        , peer_client_incarnation_(peer_client_incarnation)
        , sender_generation_(sender_generation)
        , terminal_outbox_(
              std::move(strand),
              destination,
              peer_client_incarnation,
              sender_generation)
    {
        if (!TerminalOutbox::valid_destination(destination_) ||
            peer_client_incarnation_ == 0 || sender_generation_ == 0)
            throw std::invalid_argument{"invalid C10 generation scope"};
    }

    C10PeerClientGenerationLifecycle(C10PeerClientGenerationLifecycle const&) =
        delete;
    C10PeerClientGenerationLifecycle&
    operator=(C10PeerClientGenerationLifecycle const&) = delete;
    C10PeerClientGenerationLifecycle(C10PeerClientGenerationLifecycle&&) =
        delete;
    C10PeerClientGenerationLifecycle&
    operator=(C10PeerClientGenerationLifecycle&&) = delete;

    bool
    on_owner_strand() const
    {
        return strand_.running_in_this_thread();
    }

    //@@authority transition=provider_lab.c10_accept_member owner=provider_lab
    // commit=strand
    AcceptResult
    accept(C09MemberIdentity const& member)
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, std::nullopt};
        auto const validation = validate_member(member);
        if (validation != Refusal::ok)
            return {validation, std::nullopt};

        auto const key = member_key(member);
        auto existing_members = terminal_outbox_.member_snapshot();
        if (!existing_members)
            return {Refusal::off_strand, std::nullopt};
        for (auto const& existing : *existing_members)
        {
            auto const existing_key = member_key(existing);
            if (existing_key == key && existing != member)
                return {Refusal::member_identity_conflict, std::nullopt};
        }

        if (mode_ == Mode::fenced)
        {
            auto const it = frozen_snapshot_.find(key);
            if (it == frozen_snapshot_.end())
                return {Refusal::member_not_expected, std::nullopt};
            if (it->second != member)
                return {Refusal::member_identity_conflict, std::nullopt};
        }

        // open_member() is itself an atomic owner-strand commit and rolls back
        // its reserved attempt on failure. Nothing fallible may follow it in
        // this transition.
        auto opened = terminal_outbox_.open_member(member.lineage);
        switch (opened.refusal)
        {
            case TerminalOutbox::Refusal::ok:
                return {Refusal::ok, std::move(opened.member)};
            case TerminalOutbox::Refusal::already_open:
                return {Refusal::already_open, std::move(opened.member)};
            case TerminalOutbox::Refusal::id_exhausted:
                return {Refusal::id_exhausted, std::nullopt};
            case TerminalOutbox::Refusal::invalid_identity:
                return {Refusal::invalid_member_identity, std::nullopt};
            case TerminalOutbox::Refusal::off_strand:
                return {Refusal::off_strand, std::nullopt};
            default:
                throw std::logic_error{
                    "unexpected C09 refusal while accepting a member"};
        }
    }

    //@@authority transition=provider_lab.c10_install_fence
    // owner=provider_lab commit=strand
    FenceResult
    install_fence(C10InboundFenceCandidate const& candidate)
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, 0};

        Snapshot prepared;
        auto const validation = prepare_snapshot(candidate, prepared);
        if (validation != Refusal::ok)
            return {validation, 0};

        if (mode_ == Mode::fenced)
        {
            if (candidate.fence != fence_)
                return {Refusal::generation_already_fenced, fence_};
            if (prepared != frozen_snapshot_)
                return {Refusal::fence_conflict, fence_};
            return {Refusal::already_fenced, fence_};
        }

        auto existing_members = terminal_outbox_.member_snapshot();
        if (!existing_members)
            return {Refusal::off_strand, 0};
        for (auto const& member : *existing_members)
        {
            auto const it = prepared.find(member_key(member));
            if (it == prepared.end() || it->second != member)
                return {Refusal::incomplete_snapshot, 0};
        }

        std::vector<C09MemberIdentity> expected;
        expected.reserve(prepared.size());
        for (auto const& [_, member] : prepared)
            expected.push_back(member);
        auto c09_prepared = terminal_outbox_.prepare_fence_members(expected, 0);
        if (c09_prepared.refusal != TerminalOutbox::Refusal::ok ||
            !c09_prepared.prepared)
            return {map_prepare_refusal(c09_prepared.refusal), 0};

        test_fail(TestFailpoint::fence_pre_commit);

        // Both components are now fully prepared. These swaps and scalar
        // stores are the single no-throw owner-local aggregate commit.
        terminal_outbox_.commit_fence_members(
            std::move(*c09_prepared.prepared));
        frozen_snapshot_.swap(prepared);
        fence_ = candidate.fence;
        mode_ = Mode::fenced;
        return {Refusal::ok, fence_};
    }

    TerminalOutbox::AttemptResult
    register_attempt(C09MemberIdentity const& member, C09PayloadDigest payload)
    {
        return terminal_outbox_.register_attempt(member, payload);
    }

    TerminalOutbox::Refusal
    publish_attempt(C09AttemptLease const& lease)
    {
        return terminal_outbox_.publish_attempt(lease);
    }

    TerminalOutbox::Refusal
    commit_retry_terminal(C09AttemptLease const& lease)
    {
        return terminal_outbox_.commit_retry_terminal(lease);
    }

    TerminalOutbox::TerminalResult
    complete(C09AttemptLease const& lease)
    {
        return terminal_outbox_.complete(lease);
    }

    TerminalOutbox::TerminalResult
    reject(C09AttemptLease const& lease)
    {
        return terminal_outbox_.reject(lease);
    }

    TerminalOutbox::TerminalResult
    cancel(C09AttemptLease const& lease)
    {
        return terminal_outbox_.cancel(lease);
    }

    TerminalOutbox::TerminalResult
    seal_never_published(C09AttemptLease const& lease)
    {
        return terminal_outbox_.seal_never_published(lease);
    }

    TerminalOutbox::ReplayResult
    replay(C09AttemptLease const& lease) const
    {
        return terminal_outbox_.replay(lease);
    }

    TerminalOutbox::ReplayResult
    replay(C09MemberIdentity const& member) const
    {
        return terminal_outbox_.replay(member);
    }

    TerminalOutbox::Refusal
    consume_receipt(C09TerminalReceipt const& receipt)
    {
        return terminal_outbox_.consume_receipt(receipt);
    }

    TerminalOutbox::SettlementResult
    settlement(C09MemberIdentity const& member, C09CommandId command) const
    {
        return terminal_outbox_.settlement(member, command);
    }

    TerminalOutbox::AttemptStateResult
    attempt_state(C09AttemptLease const& lease) const
    {
        return terminal_outbox_.attempt_state(lease);
    }

    std::optional<Mode>
    mode() const
    {
        if (!on_owner_strand())
            return std::nullopt;
        return mode_;
    }

    std::optional<C10FenceId>
    fence_id() const
    {
        if (!on_owner_strand() || mode_ != Mode::fenced)
            return std::nullopt;
        return fence_;
    }

    std::optional<std::vector<C09MemberIdentity>>
    frozen_snapshot() const
    {
        if (!on_owner_strand())
            return std::nullopt;
        std::vector<C09MemberIdentity> result;
        result.reserve(frozen_snapshot_.size());
        for (auto const& [_, member] : frozen_snapshot_)
            result.push_back(member);
        return result;
    }

    std::optional<std::size_t>
    member_count() const
    {
        return terminal_outbox_.member_count();
    }

    std::optional<std::size_t>
    attempt_count() const
    {
        return terminal_outbox_.attempt_count();
    }

    std::optional<std::size_t>
    live_attempt_count() const
    {
        return terminal_outbox_.live_attempt_count();
    }

    std::optional<std::size_t>
    outbox_count() const
    {
        return terminal_outbox_.outbox_count();
    }

    std::optional<std::size_t>
    retry_history_count() const
    {
        return terminal_outbox_.retry_history_count();
    }

    ReservationResult
    reservation(C09MemberIdentity const& member) const
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, std::nullopt};
        auto const validation = validate_member(member);
        if (validation != Refusal::ok)
            return {validation, std::nullopt};
        auto result = terminal_outbox_.reservation(member);
        if (result.refusal == TerminalOutbox::Refusal::unknown_member)
            return {Refusal::unknown_member, std::nullopt};
        if (result.refusal != TerminalOutbox::Refusal::ok)
            return {Refusal::invalid_member_identity, std::nullopt};
        return {Refusal::ok, std::move(result.reservation)};
    }

    std::optional<bool>
    invariant_holds() const
    {
        if (!on_owner_strand())
            return std::nullopt;
        if (mode_ == Mode::open)
            return fence_ == 0 && frozen_snapshot_.empty();
        if (fence_ == 0)
            return false;
        auto members = terminal_outbox_.member_snapshot();
        if (!members || members->size() != frozen_snapshot_.size())
            return false;
        for (auto const& member : *members)
        {
            auto const it = frozen_snapshot_.find(member_key(member));
            if (it == frozen_snapshot_.end() || it->second != member)
                return false;
        }
        return true;
    }

private:
    friend class C10LifecycleTestSeed;

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

        bool
        operator==(MemberKey const&) const = default;
    };

    using Snapshot = std::map<MemberKey, C09MemberIdentity>;

    static_assert(std::is_nothrow_swappable_v<Snapshot>);

    static MemberKey
    member_key(C09MemberIdentity const& member)
    {
        return {member.lineage.channel_generation, member.lineage.pipeline};
    }

    Refusal
    validate_member(C09MemberIdentity const& member) const
    {
        if (!TerminalOutbox::valid_destination(member.destination) ||
            !TerminalOutbox::valid_lineage(member.lineage) ||
            member.peer_client_incarnation == 0 ||
            member.sender_generation == 0)
            return Refusal::invalid_member_identity;
        if (member.destination != destination_ ||
            member.peer_client_incarnation != peer_client_incarnation_ ||
            member.sender_generation != sender_generation_)
            return Refusal::foreign_member;
        return Refusal::ok;
    }

    Refusal
    prepare_snapshot(
        C10InboundFenceCandidate const& candidate,
        Snapshot& prepared) const
    {
        if (candidate.fence == 0 ||
            !TerminalOutbox::valid_destination(candidate.destination) ||
            candidate.peer_client_incarnation == 0 ||
            candidate.sender_generation == 0)
            return Refusal::invalid_fence;
        if (candidate.destination != destination_ ||
            candidate.peer_client_incarnation != peer_client_incarnation_ ||
            candidate.sender_generation != sender_generation_)
            return Refusal::foreign_fence;

        for (auto const& member : candidate.expected_members)
        {
            auto const validation = validate_member(member);
            if (validation == Refusal::invalid_member_identity)
                return validation;
            if (validation != Refusal::ok)
                return Refusal::foreign_fence;
            auto const [it, inserted] =
                prepared.emplace(member_key(member), member);
            if (!inserted)
                return it->second == member
                    ? Refusal::duplicate_snapshot_member
                    : Refusal::snapshot_identity_conflict;
        }
        return Refusal::ok;
    }

    static Refusal
    map_prepare_refusal(TerminalOutbox::Refusal refusal)
    {
        switch (refusal)
        {
            case TerminalOutbox::Refusal::off_strand:
                return Refusal::off_strand;
            case TerminalOutbox::Refusal::id_exhausted:
                return Refusal::id_exhausted;
            default:
                return Refusal::invalid_member_identity;
        }
    }

    void
    test_fail(TestFailpoint point) const
    {
        if (test_failpoint_ == point)
            throw std::runtime_error{"C10 injected pre-commit failure"};
    }

    strand_type strand_;
    C09Destination destination_;
    C09PeerClientIncarnation peer_client_incarnation_ = 0;
    C09SenderGeneration sender_generation_ = 0;
    Mode mode_ = Mode::open;
    C10FenceId fence_ = 0;
    Snapshot frozen_snapshot_;
    TestFailpoint test_failpoint_ = TestFailpoint::none;
    TerminalOutbox terminal_outbox_;
};

/// Owner-strand-only failpoint seam. It can inject before either half of the
/// aggregate commit without exposing the private C09 outbox as a mutation path.
class C10LifecycleTestSeed
{
public:
    static C10PeerClientGenerationLifecycle::Refusal
    fail_at(
        C10PeerClientGenerationLifecycle& lifecycle,
        C10PeerClientGenerationLifecycle::TestFailpoint failpoint)
    {
        using Refusal = C10PeerClientGenerationLifecycle::Refusal;
        if (!lifecycle.on_owner_strand())
            return Refusal::off_strand;
        lifecycle.test_failpoint_ = failpoint;
        return Refusal::ok;
    }

    static C09PeerClientTerminalOutbox::Refusal
    fail_c09_at(
        C10PeerClientGenerationLifecycle& lifecycle,
        C09PeerClientTerminalOutbox::TestFailpoint failpoint)
    {
        return C09SenderTestSeed::fail_at(
            lifecycle.terminal_outbox_, failpoint);
    }

    static C09PeerClientTerminalOutbox::Refusal
    next_c09_ids(
        C10PeerClientGenerationLifecycle& lifecycle,
        C09TransportAttemptId next_attempt_id,
        C09CommandId next_command_id)
    {
        return C09SenderTestSeed::next_ids(
            lifecycle.terminal_outbox_, next_attempt_id, next_command_id);
    }
};

static_assert(!std::is_copy_constructible_v<C10PeerClientGenerationLifecycle>);
static_assert(!std::is_copy_assignable_v<C10PeerClientGenerationLifecycle>);
static_assert(!std::is_move_constructible_v<C10PeerClientGenerationLifecycle>);
static_assert(!std::is_move_assignable_v<C10PeerClientGenerationLifecycle>);

}  // namespace catl::provider_lab
