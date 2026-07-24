#pragma once

// C11 PeerClient -> PeerSet generation-close verification lab. TEST-ONLY.
//
// C11 is not a stop or reclamation command. It is the exact handoff between:
//
//  - the PeerClient owner, which retains C09/C10 detail and one replayable
//    close-command outbox until an exact receipt is consumed; and
//  - the PeerSet owner, which retains the C09 terminal inbox, the C10 fence
//    snapshot, and one replayable close receipt after first commit.
//
// Only the sender's exact receipt consumer retires PeerClient-side generation
// detail. Receiver-side C09/C10 state deliberately survives the close; T72
// (PeerClient stop) and T39 (PeerSet reclamation) are later authorities.

#include "c09-terminal-inbox.h"
#include "c10-inbound-generation-fence.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace catl::provider_lab {

using C11GenerationId = std::uint64_t;
using C11FenceSequence = std::uint64_t;
using C11MemberSetDigest = std::uint64_t;
using C11FinalProjectionSequence = std::uint64_t;
using C11C10CommandId = std::uint64_t;
using C11C10PayloadDigest = std::uint64_t;
using C11C10ReceiverCommitSequence = std::uint64_t;
using C11ReceiptPermitId = std::uint64_t;
using C11CloseSequence = std::uint64_t;
using C11CommandId = std::uint64_t;
using C11PayloadDigest = std::uint64_t;
using C11ReceiverCommitSequence = std::uint64_t;
using C11AsyncSourceId = std::uint64_t;

enum class C11C10Outcome : std::uint8_t {
    installed,
};

enum class C11CloseKind : std::uint8_t {
    generation,
};

enum class C11CloseOutcome : std::uint8_t {
    closed,
};

struct C11FenceBinding
{
    C11FenceSequence fence_sequence = 0;
    C11C10CommandId command = 0;
    C11C10PayloadDigest payload = 0;
    C11C10Outcome outcome = C11C10Outcome::installed;
    C11C10ReceiverCommitSequence receiver_commit_sequence = 0;
    C11ReceiptPermitId receipt_permit = 0;

    bool
    operator==(C11FenceBinding const&) const = default;
};

struct C11CloseIdentity
{
    C09Destination destination;
    C09PeerClientIncarnation peer_client_incarnation = 0;
    C11GenerationId generation = 0;
    C09SenderGeneration sender_generation = 0;
    C10FenceId fence = 0;
    C11FenceSequence fence_sequence = 0;
    std::uint64_t expected_member_count = 0;
    C11MemberSetDigest expected_member_set_digest = 0;
    std::uint64_t settled_member_count = 0;
    C11MemberSetDigest settled_member_set_digest = 0;
    C11FinalProjectionSequence final_projection_sequence = 0;
    C11C10CommandId c10_command = 0;
    C11C10PayloadDigest c10_payload = 0;
    C11C10Outcome c10_outcome = C11C10Outcome::installed;
    C11C10ReceiverCommitSequence c10_receiver_commit_sequence = 0;
    C11ReceiptPermitId receipt_permit = 0;
    C11CloseSequence close_sequence = 0;
    C11CloseKind close_kind = C11CloseKind::generation;
    C11CommandId command = 0;
    C11PayloadDigest payload = 0;

    bool
    operator==(C11CloseIdentity const&) const = default;
};

class C11PeerClientGenerationAggregate;
class C11PeerSetGenerationAggregate;
class C11PeerClientTestAccess;
class C11PeerSetTestAccess;

/// Owner-local capability for one callback or posted action that may still
/// address C09/C10 generation detail. T172 cannot commit while any such lease
/// remains live.
class C11AsyncSourceLease
{
public:
    C11AsyncSourceLease() = default;
    C11AsyncSourceLease(C11AsyncSourceLease const&) = delete;
    C11AsyncSourceLease&
    operator=(C11AsyncSourceLease const&) = delete;
    C11AsyncSourceLease(C11AsyncSourceLease&&) noexcept = default;
    C11AsyncSourceLease&
    operator=(C11AsyncSourceLease&&) noexcept = default;

    C11AsyncSourceId
    id() const
    {
        return id_;
    }

    bool
    valid() const
    {
        return id_ != 0 && owner_ != nullptr;
    }

private:
    friend class C11PeerClientGenerationAggregate;

    C11AsyncSourceLease(C11AsyncSourceId id, std::shared_ptr<void const> owner)
        : id_(id), owner_(std::move(owner))
    {
    }

    C11AsyncSourceId id_ = 0;
    std::shared_ptr<void const> owner_;
};

/// Sender-owned projection. Raw fields cannot mint close authority.
class C11PeerGenerationCloseCommand
{
public:
    C11PeerGenerationCloseCommand(C11PeerGenerationCloseCommand const&) =
        default;
    C11PeerGenerationCloseCommand&
    operator=(C11PeerGenerationCloseCommand const&) = default;

    C11CloseIdentity const&
    identity() const
    {
        return identity_;
    }

    bool
    operator==(C11PeerGenerationCloseCommand const&) const = default;

private:
    friend class C11PeerClientGenerationAggregate;

    explicit C11PeerGenerationCloseCommand(C11CloseIdentity identity)
        : identity_(std::move(identity))
    {
    }

    C11CloseIdentity identity_;
};

/// Receiver-owned acknowledgement of one exact close-inbox commit.
class C11PeerGenerationCloseReceipt
{
public:
    C11PeerGenerationCloseReceipt(C11PeerGenerationCloseReceipt const&) =
        default;
    C11PeerGenerationCloseReceipt&
    operator=(C11PeerGenerationCloseReceipt const&) = default;

    C11CloseIdentity const&
    identity() const
    {
        return identity_;
    }

    C11CloseOutcome
    outcome() const
    {
        return outcome_;
    }

    C11ReceiverCommitSequence
    receiver_commit_sequence() const
    {
        return receiver_commit_sequence_;
    }

    bool
    operator==(C11PeerGenerationCloseReceipt const&) const = default;

private:
    friend class C11PeerSetGenerationAggregate;

    C11PeerGenerationCloseReceipt(
        C11CloseIdentity identity,
        C11CloseOutcome outcome,
        C11ReceiverCommitSequence receiver_commit_sequence)
        : identity_(std::move(identity))
        , outcome_(outcome)
        , receiver_commit_sequence_(receiver_commit_sequence)
    {
    }

    C11CloseIdentity identity_;
    C11CloseOutcome outcome_ = C11CloseOutcome::closed;
    C11ReceiverCommitSequence receiver_commit_sequence_ = 0;
};

static_assert(!std::is_default_constructible_v<C11PeerGenerationCloseCommand>);
static_assert(!std::is_default_constructible_v<C11PeerGenerationCloseReceipt>);
static_assert(
    std::is_nothrow_copy_constructible_v<C11PeerGenerationCloseCommand>);
static_assert(
    std::is_nothrow_copy_constructible_v<C11PeerGenerationCloseReceipt>);
static_assert(
    !std::is_constructible_v<C11PeerGenerationCloseCommand, C11CloseIdentity>);
static_assert(!std::is_constructible_v<
              C11PeerGenerationCloseReceipt,
              C11CloseIdentity,
              C11CloseOutcome,
              C11ReceiverCommitSequence>);

namespace c11_detail {

inline bool
valid_destination(C09Destination const& destination)
{
    return destination.provider != 0 && destination.provider_epoch != 0 &&
        destination.partition != 0 && destination.peer_set_incarnation != 0 &&
        destination.receiver_generation != 0;
}

inline bool
valid_member(C09MemberIdentity const& member)
{
    auto const& lineage = member.lineage;
    return valid_destination(member.destination) &&
        member.peer_client_incarnation != 0 && member.sender_generation != 0 &&
        lineage.operation != 0 && lineage.interest != 0 && lineage.use != 0 &&
        lineage.pipeline != 0 && lineage.channel_generation != 0;
}

inline auto
member_tuple(C09MemberIdentity const& member)
{
    auto const& destination = member.destination;
    auto const& lineage = member.lineage;
    return std::tie(
        destination.provider,
        destination.provider_epoch,
        destination.partition,
        destination.peer_set_incarnation,
        destination.receiver_generation,
        member.peer_client_incarnation,
        member.sender_generation,
        lineage.operation,
        lineage.interest,
        lineage.use,
        lineage.pipeline,
        lineage.channel_generation);
}

inline void
digest_word(std::uint64_t& digest, std::uint64_t word)
{
    // Fixed byte order makes this a deterministic lab digest, unlike
    // std::hash. Production replaces it with the protocol digest.
    constexpr std::uint64_t prime = 1099511628211ULL;
    for (unsigned shift = 0; shift != 64; shift += 8)
    {
        digest ^= (word >> shift) & 0xffU;
        digest *= prime;
    }
}

inline C11MemberSetDigest
member_set_digest(std::vector<C09MemberIdentity> members)
{
    std::sort(
        members.begin(),
        members.end(),
        [](auto const& left, auto const& right) {
            return member_tuple(left) < member_tuple(right);
        });
    std::uint64_t digest = 1469598103934665603ULL;
    digest_word(digest, members.size());
    for (auto const& member : members)
    {
        auto const& d = member.destination;
        auto const& l = member.lineage;
        for (auto word :
             {d.provider,
              d.provider_epoch,
              d.partition,
              d.peer_set_incarnation,
              d.receiver_generation,
              member.peer_client_incarnation,
              member.sender_generation,
              l.operation,
              l.interest,
              l.use,
              l.pipeline,
              l.channel_generation})
            digest_word(digest, word);
    }
    return digest == 0 ? 1 : digest;
}

inline C11PayloadDigest
close_payload_digest(C11CloseIdentity const& identity)
{
    std::uint64_t digest = 1469598103934665603ULL;
    auto const& d = identity.destination;
    for (auto word :
         {d.provider,
          d.provider_epoch,
          d.partition,
          d.peer_set_incarnation,
          d.receiver_generation,
          identity.peer_client_incarnation,
          identity.generation,
          identity.sender_generation,
          identity.fence,
          identity.fence_sequence,
          identity.expected_member_count,
          identity.expected_member_set_digest,
          identity.settled_member_count,
          identity.settled_member_set_digest,
          identity.final_projection_sequence,
          identity.c10_command,
          identity.c10_payload,
          static_cast<std::uint64_t>(identity.c10_outcome),
          identity.c10_receiver_commit_sequence,
          identity.receipt_permit,
          identity.close_sequence,
          static_cast<std::uint64_t>(identity.close_kind),
          identity.command})
        digest_word(digest, word);
    return digest == 0 ? 1 : digest;
}

}  // namespace c11_detail

//@@authority state=provider_lab.c11_peer_client_generation
// owner=provider_lab
class C11PeerClientGenerationAggregate
{
public:
    using strand_type = DeterministicScheduler::strand_type;
    using C10 = C10PeerClientGenerationLifecycle;

    enum class Mode : std::uint8_t {
        active,
        close_pending,
        closed,
    };

    enum class Refusal : std::uint8_t {
        ok,
        off_strand,
        generation_retired,
        not_fenced,
        fence_binding_missing,
        settlement_incomplete,
        async_source_pending,
        invalid_async_source,
        async_source_already_drained,
        projection_barrier_committed,
        projection_barrier_missing,
        command_permit_missing,
        close_already_pending,
        close_already_consumed,
        id_exhausted,
        receiver_sequence_invalid,
        receipt_scope_mismatch,
        receipt_binding_mismatch,
    };

    enum class TestFailpoint : std::uint8_t {
        none,
        projection_pre_commit,
        close_pre_commit,
        receipt_pre_commit,
    };

    struct BarrierResult
    {
        Refusal refusal = Refusal::settlement_incomplete;
        C11FinalProjectionSequence final_projection_sequence = 0;
    };

    struct CloseResult
    {
        Refusal refusal = Refusal::not_fenced;
        std::optional<C11PeerGenerationCloseCommand> command;
    };

    struct AsyncSourceResult
    {
        Refusal refusal = Refusal::invalid_async_source;
        C11AsyncSourceLease lease;
    };

    enum class AsyncAttemptAction : std::uint8_t {
        publish,
        retry_terminal,
        complete,
        reject,
        cancel,
        seal_never_published,
        replay,
        state,
    };

    enum class AsyncDispatchCase : std::uint8_t {
        forwarded,
        off_strand,
        generation_retired,
        invalid_source,
        source_already_consumed,
        invalid_action,
    };

    struct AsyncAttemptResult
    {
        AsyncDispatchCase dispatch = AsyncDispatchCase::off_strand;
        std::optional<C09PeerClientTerminalOutbox::Refusal> refusal;
        std::optional<C09TerminalCommand> command;
        std::optional<C09PeerClientTerminalOutbox::AttemptState> state;

        AsyncAttemptResult() = default;

        AsyncAttemptResult(AsyncDispatchCase selected) : dispatch(selected)
        {
        }
    };

    struct ClosedWitness
    {
        C11GenerationId generation = 0;
        C10FenceId fence = 0;
        C11CommandId command = 0;
        C11ReceiverCommitSequence receiver_commit_sequence = 0;

        bool
        operator==(ClosedWitness const&) const = default;
    };

    struct BeginCloseReceipt
    {
        C11GenerationId generation = 0;
        C10FenceId fence = 0;
        C11CommandId command = 0;

        bool
        operator==(BeginCloseReceipt const&) const = default;
    };

    struct C10ClosedHighWater
    {
        C11GenerationId generation = 0;
        C10FenceId fence = 0;

        bool
        operator==(C10ClosedHighWater const&) const = default;
    };

    C11PeerClientGenerationAggregate(
        strand_type strand,
        C09Destination destination,
        C09PeerClientIncarnation peer_client_incarnation,
        C09SenderGeneration sender_generation,
        C11GenerationId generation)
        : strand_(strand)
        , destination_(destination)
        , peer_client_incarnation_(peer_client_incarnation)
        , sender_generation_(sender_generation)
        , generation_(generation)
        , lifecycle_(
              std::in_place,
              std::move(strand),
              destination,
              peer_client_incarnation,
              sender_generation)
    {
        if (!c11_detail::valid_destination(destination_) ||
            peer_client_incarnation_ == 0 || sender_generation_ == 0 ||
            generation_ == 0)
            throw std::invalid_argument{"invalid C11 PeerClient scope"};
    }

    C11PeerClientGenerationAggregate(C11PeerClientGenerationAggregate const&) =
        delete;
    C11PeerClientGenerationAggregate&
    operator=(C11PeerClientGenerationAggregate const&) = delete;
    C11PeerClientGenerationAggregate(C11PeerClientGenerationAggregate&&) =
        delete;
    C11PeerClientGenerationAggregate&
    operator=(C11PeerClientGenerationAggregate&&) = delete;

    bool
    on_owner_strand() const
    {
        return strand_.running_in_this_thread();
    }

    //@@authority transition=provider_lab.c11_open_async_source
    // owner=provider_lab commit=strand
    AsyncSourceResult
    open_async_source()
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, {}};
        if (!lifecycle_)
            return {Refusal::generation_retired, {}};
        if (projection_barrier_)
            return {Refusal::projection_barrier_committed, {}};
        if (next_async_source_ == std::numeric_limits<C11AsyncSourceId>::max())
            return {Refusal::id_exhausted, {}};
        auto const id = next_async_source_ + 1;
        auto const [_, inserted] = async_sources_.emplace(id);
        if (!inserted)
            return {Refusal::id_exhausted, {}};
        next_async_source_ = id;
        return {Refusal::ok, C11AsyncSourceLease{id, async_owner_token_}};
    }

    /// Execute one queued C09 action and discharge the exact source that owned
    /// it. Callback authority and ledger authority are the same move-only
    /// capability: callers cannot declare a callback drained before running it,
    /// nor run one without first opening its source.
    ///
    /// The nested C09 transition and source erasure are one non-suspending
    /// owner-strand commit. A throwing nested test failpoint leaves the source
    /// pending, so T172 remains closed.
    //@@authority transition=provider_lab.c11_dispatch_async_attempt
    // owner=provider_lab commit=strand
    AsyncAttemptResult
    dispatch_async_attempt(
        C11AsyncSourceLease&& source,
        C09AttemptLease const& attempt,
        AsyncAttemptAction action)
    {
        if (!on_owner_strand())
            return {AsyncDispatchCase::off_strand};
        if (!lifecycle_)
            return {AsyncDispatchCase::generation_retired};
        if (!source.valid() || source.owner_ != async_owner_token_)
            return {AsyncDispatchCase::invalid_source};
        auto const pending = async_sources_.find(source.id_);
        if (pending == async_sources_.end())
            return {AsyncDispatchCase::source_already_consumed};

        AsyncAttemptResult result{AsyncDispatchCase::forwarded};
        switch (action)
        {
            case AsyncAttemptAction::publish:
                result.refusal = lifecycle_->publish_attempt(attempt);
                break;
            case AsyncAttemptAction::retry_terminal:
                result.refusal = lifecycle_->commit_retry_terminal(attempt);
                break;
            case AsyncAttemptAction::complete: {
                auto terminal = lifecycle_->complete(attempt);
                result.refusal = terminal.refusal;
                result.command = std::move(terminal.command);
                break;
            }
            case AsyncAttemptAction::reject: {
                auto terminal = lifecycle_->reject(attempt);
                result.refusal = terminal.refusal;
                result.command = std::move(terminal.command);
                break;
            }
            case AsyncAttemptAction::cancel: {
                auto terminal = lifecycle_->cancel(attempt);
                result.refusal = terminal.refusal;
                result.command = std::move(terminal.command);
                break;
            }
            case AsyncAttemptAction::seal_never_published: {
                auto terminal = lifecycle_->seal_never_published(attempt);
                result.refusal = terminal.refusal;
                result.command = std::move(terminal.command);
                break;
            }
            case AsyncAttemptAction::replay: {
                auto replay = lifecycle_->replay(attempt);
                result.refusal = replay.refusal;
                result.command = std::move(replay.command);
                break;
            }
            case AsyncAttemptAction::state: {
                auto state = lifecycle_->attempt_state(attempt);
                result.refusal = state.refusal;
                result.state = state.state;
                break;
            }
            default:
                return {AsyncDispatchCase::invalid_action};
        }

        async_sources_.erase(pending);
        source.id_ = 0;
        source.owner_.reset();
        return result;
    }

    /// T172: derive the pre-C11 barrier from the authoritative C10 snapshot
    /// and exact C09 settlements. The explicit async-source ledger owns every
    /// queued callback that may still address C09/C10 detail.
    //@@authority transition=provider_lab.c11_projection_barrier
    // owner=provider_lab commit=strand
    BarrierResult
    commit_projection_barrier()
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, 0};
        if (!lifecycle_)
            return {Refusal::generation_retired, 0};
        if (lifecycle_->mode() != C10::Mode::fenced)
            return {Refusal::not_fenced, 0};
        if (!fence_binding_)
            return {Refusal::fence_binding_missing, 0};
        if (projection_barrier_)
            return {Refusal::ok, projection_barrier_->final_sequence};
        if (!async_sources_.empty())
            return {Refusal::async_source_pending, 0};

        auto summary = exact_settlement_summary();
        if (!summary)
            return {Refusal::settlement_incomplete, 0};
        test_fail(TestFailpoint::projection_pre_commit);
        projection_barrier_.emplace(*summary);
        return {Refusal::ok, projection_barrier_->final_sequence};
    }

    /// T116: mint one retained close command. Replays project this row; they
    /// never mint another command or consume another permit.
    //@@authority transition=provider_lab.c11_begin_close
    // owner=provider_lab commit=strand
    CloseResult
    begin_close()
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, std::nullopt};
        if (!lifecycle_)
            return {Refusal::generation_retired, std::nullopt};
        if (close_outbox_)
            return {Refusal::close_already_pending, close_outbox_};
        if (!projection_barrier_)
            return {Refusal::projection_barrier_missing, std::nullopt};
        if (!fence_binding_)
            return {Refusal::fence_binding_missing, std::nullopt};
        if (!command_permit_available_)
            return {Refusal::command_permit_missing, std::nullopt};
        if (next_close_command_ == std::numeric_limits<C11CommandId>::max() ||
            next_close_sequence_ ==
                std::numeric_limits<C11CloseSequence>::max())
            return {Refusal::id_exhausted, std::nullopt};

        auto const command_id = next_close_command_ + 1;
        auto const close_sequence = next_close_sequence_ + 1;
        C11CloseIdentity identity{
            destination_,
            peer_client_incarnation_,
            generation_,
            sender_generation_,
            *lifecycle_->fence_id(),
            fence_binding_->fence_sequence,
            projection_barrier_->member_count,
            projection_barrier_->member_digest,
            projection_barrier_->member_count,
            projection_barrier_->member_digest,
            projection_barrier_->final_sequence,
            fence_binding_->command,
            fence_binding_->payload,
            fence_binding_->outcome,
            fence_binding_->receiver_commit_sequence,
            fence_binding_->receipt_permit,
            close_sequence,
            C11CloseKind::generation,
            command_id,
            0};
        identity.payload = c11_detail::close_payload_digest(identity);
        C11PeerGenerationCloseCommand command{identity};
        BeginCloseReceipt const begin_receipt{
            generation_, identity.fence, command_id};
        C10ClosedHighWater const high_water{generation_, identity.fence};
        test_fail(TestFailpoint::close_pre_commit);
        close_outbox_.emplace(command);
        begin_close_receipt_.emplace(begin_receipt);
        c10_closed_high_water_.emplace(high_water);
        command_permit_available_ = false;
        next_close_command_ = command_id;
        next_close_sequence_ = close_sequence;
        mode_ = Mode::close_pending;
        return {Refusal::ok, command};
    }

    // T161: sender-owned replay projects the retained row.
    CloseResult
    replay_close() const
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, std::nullopt};
        if (close_outbox_)
            return {Refusal::ok, close_outbox_};
        return {
            lifecycle_ ? Refusal::projection_barrier_missing
                       : Refusal::close_already_consumed,
            std::nullopt};
    }

    /// T164: the sole PeerClient detail-retirement commit. Every fallible
    /// check precedes the no-throw destruction of C09/C10 detail.
    //@@authority transition=provider_lab.c11_consume_close_receipt
    // owner=provider_lab commit=strand
    Refusal
    consume_close_receipt(C11PeerGenerationCloseReceipt const& receipt)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        if (closed_state_)
            return closed_state_->receipt == receipt
                ? Refusal::close_already_consumed
                : Refusal::receipt_binding_mismatch;
        if (!close_outbox_ || !lifecycle_)
            return Refusal::receipt_binding_mismatch;
        if (receipt.receiver_commit_sequence() == 0)
            return Refusal::receiver_sequence_invalid;
        if (receipt.identity().destination != destination_ ||
            receipt.identity().peer_client_incarnation !=
                peer_client_incarnation_ ||
            receipt.identity().generation != generation_ ||
            receipt.identity().sender_generation != sender_generation_)
            return Refusal::receipt_scope_mismatch;
        if (receipt.identity() != close_outbox_->identity() ||
            receipt.outcome() != C11CloseOutcome::closed)
            return Refusal::receipt_binding_mismatch;

        ClosedWitness const witness{
            generation_,
            receipt.identity().fence,
            receipt.identity().command,
            receipt.receiver_commit_sequence()};
        test_fail(TestFailpoint::receipt_pre_commit);

        // Single owner-strand commit: the retained receipt/witness replace all
        // sender detail. optional destruction is noexcept and cannot expose an
        // intermediate state because this operation never suspends.
        closed_state_.emplace(ClosedState{witness, receipt});
        close_outbox_.reset();
        projection_barrier_.reset();
        fence_binding_.reset();
        lifecycle_.reset();
        mode_ = Mode::closed;
        return Refusal::ok;
    }

    std::optional<Mode>
    mode() const
    {
        return on_owner_strand() ? std::optional{mode_} : std::nullopt;
    }

    std::optional<bool>
    has_generation_detail() const
    {
        return on_owner_strand() ? std::optional{lifecycle_.has_value()}
                                 : std::nullopt;
    }

    std::optional<ClosedWitness>
    closed_witness() const
    {
        return on_owner_strand() && closed_state_
            ? std::optional{closed_state_->witness}
            : std::nullopt;
    }

    std::optional<BeginCloseReceipt>
    begin_close_receipt() const
    {
        return on_owner_strand() ? begin_close_receipt_ : std::nullopt;
    }

    std::optional<C10ClosedHighWater>
    c10_closed_high_water() const
    {
        return on_owner_strand() ? c10_closed_high_water_ : std::nullopt;
    }

private:
    friend class C11PeerClientTestAccess;

    struct ClosedState
    {
        ClosedWitness witness;
        C11PeerGenerationCloseReceipt receipt;
    };

    struct SettlementSummary
    {
        std::uint64_t member_count = 0;
        C11MemberSetDigest member_digest = 0;
        C11FinalProjectionSequence final_sequence = 0;
    };

    std::optional<SettlementSummary>
    exact_settlement_summary() const
    {
        auto members = lifecycle_->frozen_snapshot();
        auto outbox_count = lifecycle_->outbox_count();
        auto live_count = lifecycle_->live_attempt_count();
        if (!members || !outbox_count || !live_count || *outbox_count != 0 ||
            *live_count != 0)
            return std::nullopt;

        for (auto const& member : *members)
        {
            auto reservation = lifecycle_->reservation(member);
            if (reservation.refusal != C10::Refusal::ok ||
                !reservation.reservation)
                return std::nullopt;
            auto settlement = lifecycle_->settlement(
                member, reservation.reservation->command);
            if (settlement.refusal !=
                    C09PeerClientTerminalOutbox::Refusal::ok ||
                !settlement.receipt)
                return std::nullopt;
        }
        return SettlementSummary{
            static_cast<std::uint64_t>(members->size()),
            c11_detail::member_set_digest(*members),
            next_async_source_};
    }

    void
    bind_fence(C11FenceBinding binding)
    {
        if (!on_owner_strand())
            throw std::logic_error{"C11 fence binding off owner strand"};
        if (binding.fence_sequence == 0 || binding.command == 0 ||
            binding.payload == 0 || binding.receiver_commit_sequence == 0 ||
            binding.receipt_permit == 0 ||
            binding.outcome != C11C10Outcome::installed)
            throw std::invalid_argument{"invalid C11 fence binding"};
        if (!lifecycle_ || lifecycle_->mode() != C10::Mode::fenced)
            throw std::logic_error{"C11 fence binding precedes C10 commit"};
        if (fence_binding_)
        {
            if (*fence_binding_ == binding)
                return;
            throw std::logic_error{"C11 fence binding conflict"};
        }
        fence_binding_ = binding;
        command_permit_available_ = true;
    }

    void
    test_fail(TestFailpoint point) const
    {
        if (test_failpoint_ == point)
            throw std::runtime_error{"C11 PeerClient injected failure"};
    }

    //@@authority state=provider_lab.c11_peer_client_close_outbox
    // owner=provider_lab
    strand_type strand_;
    C09Destination destination_;
    C09PeerClientIncarnation peer_client_incarnation_ = 0;
    C09SenderGeneration sender_generation_ = 0;
    C11GenerationId generation_ = 0;
    Mode mode_ = Mode::active;
    TestFailpoint test_failpoint_ = TestFailpoint::none;
    C11CommandId next_close_command_ = 0;
    C11CloseSequence next_close_sequence_ = 0;
    //@@authority state=provider_lab.c11_async_sources owner=provider_lab
    std::shared_ptr<void const> async_owner_token_ = std::make_shared<int>(0);
    C11AsyncSourceId next_async_source_ = 0;
    std::set<C11AsyncSourceId> async_sources_;
    std::optional<C11FenceBinding> fence_binding_;
    std::optional<SettlementSummary> projection_barrier_;
    //@@authority state=provider_lab.c11_command_permit owner=provider_lab
    bool command_permit_available_ = false;
    std::optional<C11PeerGenerationCloseCommand> close_outbox_;
    //@@authority state=provider_lab.c11_begin_close_receipt owner=provider_lab
    std::optional<BeginCloseReceipt> begin_close_receipt_;
    //@@authority state=provider_lab.c11_c10_closed_high_water
    // owner=provider_lab
    std::optional<C10ClosedHighWater> c10_closed_high_water_;
    //@@authority state=provider_lab.c11_closed_state owner=provider_lab
    std::optional<ClosedState> closed_state_;
    std::optional<C10> lifecycle_;
};

//@@authority state=provider_lab.c11_peer_set_generation
// owner=provider_lab
class C11PeerSetGenerationAggregate
{
public:
    using strand_type = DeterministicScheduler::strand_type;

    enum class Case : std::uint8_t {
        first_commit,
        exact_replay,
        off_strand,
        wrong_receiver,
        invalid_identity,
        fence_not_installed,
        settlement_mismatch,
        command_reuse_conflict,
        receipt_permit_consumed,
        receiver_sequence_exhausted,
    };

    enum class InstallRefusal : std::uint8_t {
        ok,
        off_strand,
        invalid_binding,
        already_installed,
        member_binding_conflict,
        receiver_sequence_exhausted,
    };

    enum class TestFailpoint : std::uint8_t {
        none,
        install_pre_commit,
        close_pre_commit,
    };

    struct DispatchResult
    {
        Case result = Case::invalid_identity;
        std::optional<C11PeerGenerationCloseReceipt> receipt;
    };

    C11PeerSetGenerationAggregate(
        strand_type strand,
        C09Destination destination,
        C09PeerClientIncarnation peer_client_incarnation,
        C09SenderGeneration sender_generation,
        C11GenerationId generation)
        : strand_(strand)
        , destination_(destination)
        , peer_client_incarnation_(peer_client_incarnation)
        , sender_generation_(sender_generation)
        , generation_(generation)
        , terminal_inbox_(std::move(strand), destination)
    {
        if (!c11_detail::valid_destination(destination_) ||
            peer_client_incarnation_ == 0 || sender_generation_ == 0 ||
            generation_ == 0)
            throw std::invalid_argument{"invalid C11 PeerSet scope"};
    }

    C11PeerSetGenerationAggregate(C11PeerSetGenerationAggregate const&) =
        delete;
    C11PeerSetGenerationAggregate&
    operator=(C11PeerSetGenerationAggregate const&) = delete;
    C11PeerSetGenerationAggregate(C11PeerSetGenerationAggregate&&) = delete;
    C11PeerSetGenerationAggregate&
    operator=(C11PeerSetGenerationAggregate&&) = delete;

    bool
    on_owner_strand() const
    {
        return strand_.running_in_this_thread();
    }

    C09PeerSetTerminalInbox::DispatchResult
    dispatch_terminal(C09TerminalCommand const& command)
    {
        return terminal_inbox_.dispatch(command);
    }

    /// T162/T163/T166/T170 receiver dispatch. T165 post-fence classification
    /// belongs to the later T39 endpoint high-water and is not claimed here.
    /// First commit creates
    /// closure-pending state and a replayable receipt but retains C09/C10.
    //@@authority transition=provider_lab.c11_receive_close
    // owner=provider_lab commit=strand
    DispatchResult
    dispatch_close(C11PeerGenerationCloseCommand const& command)
    {
        if (!on_owner_strand())
            return {Case::off_strand, std::nullopt};
        auto const& identity = command.identity();
        if (identity.destination != destination_ ||
            identity.peer_client_incarnation != peer_client_incarnation_ ||
            identity.generation != generation_ ||
            identity.sender_generation != sender_generation_)
            return {Case::wrong_receiver, std::nullopt};
        if (!valid_close_identity(identity))
            return {Case::invalid_identity, std::nullopt};

        if (close_inbox_)
        {
            return close_inbox_->command == command
                ? DispatchResult{Case::exact_replay, close_inbox_->receipt}
                : DispatchResult{Case::command_reuse_conflict, std::nullopt};
        }
        if (!snapshot_)
            return {Case::fence_not_installed, std::nullopt};
        if (!matches_installed_fence(identity) ||
            !receiver_exact_settlement(identity))
            return {Case::settlement_mismatch, std::nullopt};
        if (!receipt_permit_available_)
            return {Case::receipt_permit_consumed, std::nullopt};
        if (next_receiver_sequence_ ==
            std::numeric_limits<C11ReceiverCommitSequence>::max())
            return {Case::receiver_sequence_exhausted, std::nullopt};

        auto const sequence = next_receiver_sequence_ + 1;
        C11PeerGenerationCloseReceipt receipt{
            identity, C11CloseOutcome::closed, sequence};
        InboxRow prepared{command, receipt};
        test_fail(TestFailpoint::close_pre_commit);

        close_inbox_.emplace(std::move(prepared));
        receipt_permit_available_ = false;
        closure_pending_ = true;
        next_receiver_sequence_ = sequence;
        return {Case::first_commit, close_inbox_->receipt};
    }

    std::optional<std::size_t>
    terminal_member_count() const
    {
        return terminal_inbox_.member_count();
    }

    std::optional<std::size_t>
    terminal_inbox_count() const
    {
        return terminal_inbox_.inbox_count();
    }

    std::optional<bool>
    closure_pending() const
    {
        return on_owner_strand() ? std::optional{closure_pending_}
                                 : std::nullopt;
    }

    std::optional<bool>
    c09_retained_after_close() const
    {
        if (!on_owner_strand())
            return std::nullopt;
        if (!closure_pending_ || !snapshot_)
            return false;
        return terminal_inbox_.member_count() == snapshot_->members.size() &&
            terminal_inbox_.inbox_count() == snapshot_->members.size();
    }

private:
    friend class C11PeerSetTestAccess;

    struct Snapshot
    {
        C10FenceId fence = 0;
        C11FenceBinding binding;
        std::vector<C09MemberIdentity> members;
        C11MemberSetDigest digest = 0;
    };

    struct InboxRow
    {
        C11PeerGenerationCloseCommand command;
        C11PeerGenerationCloseReceipt receipt;
    };

    InstallRefusal
    install_fence(
        C10FenceId fence,
        C11FenceBinding binding,
        std::vector<C09MemberIdentity> members)
    {
        using Receiver = C09PeerSetTerminalInbox;
        if (!on_owner_strand())
            return InstallRefusal::off_strand;
        if (snapshot_)
            return InstallRefusal::already_installed;
        if (fence == 0 || binding.fence_sequence == 0 || binding.command == 0 ||
            binding.payload == 0 || binding.receiver_commit_sequence == 0 ||
            binding.receipt_permit == 0 ||
            binding.outcome != C11C10Outcome::installed)
            return InstallRefusal::invalid_binding;

        std::sort(
            members.begin(),
            members.end(),
            [](auto const& left, auto const& right) {
                return c11_detail::member_tuple(left) <
                    c11_detail::member_tuple(right);
            });
        for (std::size_t index = 0; index != members.size(); ++index)
        {
            auto const& member = members[index];
            if (!c11_detail::valid_member(member) ||
                member.destination != destination_ ||
                member.peer_client_incarnation != peer_client_incarnation_ ||
                member.sender_generation != sender_generation_)
                return InstallRefusal::invalid_binding;
            for (std::size_t prior = 0; prior != index; ++prior)
            {
                auto const& previous = members[prior];
                if (previous.lineage.channel_generation ==
                        member.lineage.channel_generation &&
                    previous.lineage.pipeline == member.lineage.pipeline)
                    return InstallRefusal::member_binding_conflict;
            }
        }

        using Prepared = Receiver::PreparedRegistration;
        auto const max = std::numeric_limits<C09ReceiverCommitSequence>::max();
        if (members.size() > max - terminal_inbox_.next_receiver_sequence_ -
                terminal_inbox_.reserved_sequence_permits_)
            return InstallRefusal::receiver_sequence_exhausted;
        std::vector<Prepared> prepared_members;
        prepared_members.reserve(members.size());
        for (auto const& member : members)
        {
            auto [refusal, prepared] =
                terminal_inbox_.prepare_registration(member);
            if (refusal ==
                Receiver::RegisterRefusal::receiver_sequence_exhausted)
                return InstallRefusal::receiver_sequence_exhausted;
            if (refusal != Receiver::RegisterRefusal::ok)
                return InstallRefusal::member_binding_conflict;
            prepared_members.push_back(std::move(prepared));
        }

        auto const digest = c11_detail::member_set_digest(members);
        Snapshot prepared_snapshot{fence, binding, std::move(members), digest};
        test_fail(TestFailpoint::install_pre_commit);

        for (auto& prepared : prepared_members)
            terminal_inbox_.commit_registration(std::move(prepared));
        snapshot_.emplace(std::move(prepared_snapshot));
        receipt_permit_available_ = true;
        return InstallRefusal::ok;
    }

    bool
    valid_close_identity(C11CloseIdentity const& identity) const
    {
        if (identity.fence == 0 || identity.fence_sequence == 0 ||
            identity.expected_member_set_digest == 0 ||
            identity.settled_member_set_digest == 0 ||
            identity.c10_command == 0 || identity.c10_payload == 0 ||
            identity.c10_receiver_commit_sequence == 0 ||
            identity.receipt_permit == 0 || identity.close_sequence == 0 ||
            identity.command == 0 || identity.payload == 0)
            return false;
        if (identity.close_kind != C11CloseKind::generation ||
            identity.c10_outcome != C11C10Outcome::installed)
            return false;
        auto copy = identity;
        copy.payload = 0;
        return c11_detail::close_payload_digest(copy) == identity.payload;
    }

    bool
    matches_installed_fence(C11CloseIdentity const& identity) const
    {
        return identity.fence == snapshot_->fence &&
            identity.fence_sequence == snapshot_->binding.fence_sequence &&
            identity.c10_command == snapshot_->binding.command &&
            identity.c10_payload == snapshot_->binding.payload &&
            identity.c10_outcome == snapshot_->binding.outcome &&
            identity.c10_receiver_commit_sequence ==
            snapshot_->binding.receiver_commit_sequence &&
            identity.receipt_permit == snapshot_->binding.receipt_permit;
    }

    bool
    receiver_exact_settlement(C11CloseIdentity const& identity) const
    {
        if (identity.expected_member_count != snapshot_->members.size() ||
            identity.settled_member_count != snapshot_->members.size() ||
            identity.expected_member_set_digest != snapshot_->digest ||
            identity.settled_member_set_digest != snapshot_->digest ||
            terminal_inbox_.member_count() != snapshot_->members.size() ||
            terminal_inbox_.inbox_count() != snapshot_->members.size())
            return false;
        for (auto const& member : snapshot_->members)
        {
            auto state = terminal_inbox_.member_state(member);
            if (state.refusal != C09PeerSetTerminalInbox::RegisterRefusal::ok ||
                state.state !=
                    C09PeerSetTerminalInbox::MemberState::terminal_received)
                return false;
        }
        return true;
    }

    void
    test_fail(TestFailpoint point) const
    {
        if (test_failpoint_ == point)
            throw std::runtime_error{"C11 PeerSet injected failure"};
    }

    //@@authority state=provider_lab.c11_peer_set_close_inbox
    // owner=provider_lab
    strand_type strand_;
    C09Destination destination_;
    C09PeerClientIncarnation peer_client_incarnation_ = 0;
    C09SenderGeneration sender_generation_ = 0;
    C11GenerationId generation_ = 0;
    TestFailpoint test_failpoint_ = TestFailpoint::none;
    C11ReceiverCommitSequence next_receiver_sequence_ = 0;
    bool receipt_permit_available_ = false;
    bool closure_pending_ = false;
    std::optional<Snapshot> snapshot_;
    std::optional<InboxRow> close_inbox_;
    C09PeerSetTerminalInbox terminal_inbox_;
};

/// Named, non-escaping test verbs for composing the already-tested C10/C09
/// transitions with C11. No method returns the nested lifecycle or accepts an
/// arbitrary callback, so mutation authority cannot leak past one operation.
class C11PeerClientTestAccess
{
public:
    static C10PeerClientGenerationLifecycle::AcceptResult
    accept(
        C11PeerClientGenerationAggregate& sender,
        C09MemberIdentity const& member)
    {
        return lifecycle(sender).accept(member);
    }

    static C10PeerClientGenerationLifecycle::FenceResult
    install_fence(
        C11PeerClientGenerationAggregate& sender,
        C10InboundFenceCandidate const& candidate)
    {
        return lifecycle(sender).install_fence(candidate);
    }

    static C09PeerClientTerminalOutbox::AttemptResult
    register_attempt(
        C11PeerClientGenerationAggregate& sender,
        C09MemberIdentity const& member,
        C09PayloadDigest payload)
    {
        return lifecycle(sender).register_attempt(member, payload);
    }

    static C09PeerClientTerminalOutbox::Refusal
    publish_attempt(
        C11PeerClientGenerationAggregate& sender,
        C09AttemptLease const& lease)
    {
        return lifecycle(sender).publish_attempt(lease);
    }

    static C09PeerClientTerminalOutbox::TerminalResult
    complete(
        C11PeerClientGenerationAggregate& sender,
        C09AttemptLease const& lease)
    {
        return lifecycle(sender).complete(lease);
    }

    static C09PeerClientTerminalOutbox::Refusal
    fail_c09_at(
        C11PeerClientGenerationAggregate& sender,
        C09PeerClientTerminalOutbox::TestFailpoint failpoint)
    {
        return C10LifecycleTestSeed::fail_c09_at(lifecycle(sender), failpoint);
    }

    static C09PeerClientTerminalOutbox::Refusal
    consume_terminal_receipt(
        C11PeerClientGenerationAggregate& sender,
        C09TerminalReceipt const& receipt)
    {
        return lifecycle(sender).consume_receipt(receipt);
    }

    /// Stand-in for the not-yet-integrated typed C10 receiver receipt. It
    /// verifies that the C10 owner-local fence already committed, then installs
    /// its exact binding and the one sender-owned C11 command permit.
    static void
    bind_fence(
        C11PeerClientGenerationAggregate& sender,
        C11FenceBinding binding)
    {
        sender.bind_fence(binding);
    }

    static void
    fail_at(
        C11PeerClientGenerationAggregate& sender,
        C11PeerClientGenerationAggregate::TestFailpoint failpoint)
    {
        if (!sender.on_owner_strand())
            throw std::logic_error{"C11 sender failpoint off strand"};
        sender.test_failpoint_ = failpoint;
    }

    static void
    next_close_ids(
        C11PeerClientGenerationAggregate& sender,
        C11CommandId command,
        C11CloseSequence sequence)
    {
        if (!sender.on_owner_strand() || sender.close_outbox_)
            throw std::logic_error{"C11 sender IDs not seedable"};
        sender.next_close_command_ = command;
        sender.next_close_sequence_ = sequence;
    }

private:
    static C10PeerClientGenerationLifecycle&
    lifecycle(C11PeerClientGenerationAggregate& sender)
    {
        if (!sender.on_owner_strand() || !sender.lifecycle_)
            throw std::logic_error{"C11 sender lifecycle unavailable"};
        return *sender.lifecycle_;
    }
};

class C11PeerSetTestAccess
{
public:
    static C11PeerSetGenerationAggregate::InstallRefusal
    install_fence(
        C11PeerSetGenerationAggregate& receiver,
        C10FenceId fence,
        C11FenceBinding binding,
        std::vector<C09MemberIdentity> members)
    {
        return receiver.install_fence(fence, binding, std::move(members));
    }

    static void
    fail_at(
        C11PeerSetGenerationAggregate& receiver,
        C11PeerSetGenerationAggregate::TestFailpoint failpoint)
    {
        if (!receiver.on_owner_strand())
            throw std::logic_error{"C11 receiver failpoint off strand"};
        receiver.test_failpoint_ = failpoint;
    }

    static void
    next_close_receipt_sequence(
        C11PeerSetGenerationAggregate& receiver,
        C11ReceiverCommitSequence sequence)
    {
        if (!receiver.on_owner_strand() || receiver.close_inbox_)
            throw std::logic_error{"C11 receiver sequence not seedable"};
        receiver.next_receiver_sequence_ = sequence;
    }
};

static_assert(!std::is_copy_constructible_v<C11PeerClientGenerationAggregate>);
static_assert(!std::is_move_constructible_v<C11PeerClientGenerationAggregate>);
static_assert(!std::is_copy_constructible_v<C11PeerSetGenerationAggregate>);
static_assert(!std::is_move_constructible_v<C11PeerSetGenerationAggregate>);

}  // namespace catl::provider_lab
