#pragma once

// Coordinator increments 3b + 3e: the closed C01 receiver dispatch
// cell with full destination authentication, and the C02 generation
// close authority. TEST-ONLY: production construction must never
// reference this.
//
// Increment 3e (C02 close), per the accepted design:
//  - the close command is sole-minted (its production minter — the
//    sender's channel owner — is not composed; tests mint via the
//    explicit seam) and carries the COMPLETE destination plus its
//    close identity; destination is authenticated before any read;
//  - closes are STRICT CONTIGUOUS SUCCESSORS: the sole production
//    mover of the stale high-water calls the pure
//    check_close_successor. What THIS component proves is
//    receiver-side strict contiguous ordering plus a retained,
//    replayable latest receipt; RECEIPT-GATED succession — the sender
//    consumes close receipt N before minting close N+1 — is the
//    sender composition's obligation, stated here as a precondition
//    and proven only when that composition lands (R6's citation is
//    completed there, not here). Succession is ordering, not
//    permission: the authority is this cell's serialized close slot
//    plus the bound command, never the arithmetic alone;
//  - the close consumes the SERIALIZED SLOT (begin occupies, complete
//    applies-and-releases; a second begin refuses while one is in
//    flight) and while generation g of a namespace is closing, the
//    cell seals that closing prefix: commands at or below g for that
//    namespace refuse, so reclamation can never race a first-commit;
//  - completion advances the high-water and bulk-reclaims EXACTLY the
//    named generation's inbox rows (the exact prefix property follows
//    from contiguity: every earlier generation was reclaimed by its
//    own close); it mints and RETAINS the bound close receipt —
//    identical replay for the latest closed generation per namespace;
//  - boundedness, stated honestly: retained close receipts <= sender
//    namespaces (older receipts are reclaimed by the successor close;
//    the sender's serialized close pipeline consumes each receipt
//    before issuing the next close — composition assumption, stated);
//    the slot is <= 1; inbox row SIZE stays bounded and row COUNT
//    stays unbounded until sender credits compose (unchanged claims);
//  - completion is exception-atomic under the consistent minter rule:
//    nodes prepared before a non-throwing commit, with a test
//    failpoint between.
//
// The dispatch contract, from the v4 corpus plus two review rulings:
//  - an incoming command carries its DESTINATION (ProviderId,
//    PartitionId, ProviderEpoch, ReceiverGeneration) and the cell
//    compares all four before any reclaimable read — a receipt can
//    only ever authenticate a command that named this exact cell;
//    otherwise the mint would launder a misroute into valid evidence;
//  - commands carry the full canonical identity; dedup rows are keyed
//    by (SenderGeneration, ChannelGeneration, CommandId) and retain
//    every remaining field — exact full binding replays the identical
//    receipt, ANY mismatch conflicts without effect or receipt;
//  - stale rejects at or below the per-sender-namespace high-water
//    BEFORE inbox absence can select first-commit;
//  - 3b exposes NO close authority. Strict succession is ordering, not
//    permission: "current + 1" proves nothing about who may close.
//    The successor ARITHMETIC lives here as a pure, directly tested
//    function (including the executed ceiling case); the C02 close
//    operation — bound close identity, destination validation,
//    serialized slot, sole-minted close receipt, exact-prefix
//    reclamation — is increment 3e's sole authority. Until then the
//    only way to move a mark is the explicitly test-only seed, which
//    carries no authority annotation;
//  - CommandId 0 is reserved (invalid identity, checked after
//    destination, before any reclaimable read); ChannelGeneration 0 is
//    valid vocabulary (the namespace sentinel sits immediately before
//    it);
//  - receipts are sole-minted, bound to the complete cell + command
//    identity + outcome.

#include "deterministic-scheduler.h"

#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <optional>
#include <tuple>
#include <type_traits>

namespace catl::provider_lab {

using SenderGeneration = std::uint64_t;
using ChannelGeneration = std::uint64_t;
using CommandId = std::uint64_t;
using CloseSequence = std::uint64_t;

/// The canonical close-kind domain is CLOSED: downstream close-result
/// handling (T150/T146) is exhaustive over exactly this partition, so
/// an open integer here could mint a close no canonical consumer
/// handles. A closed enum makes invalid construction impossible.
enum class CloseKind : std::uint8_t {
    capacity,
    clear,
    terminal,
};
using ReceiverGeneration = std::uint64_t;
using PayloadDigest = std::uint64_t;
using Outcome = std::uint64_t;
using OperationId = std::uint64_t;
using InterestId = std::uint64_t;
using UseId = std::uint64_t;
using PipelineId = std::uint64_t;
using ProviderId = std::uint64_t;
using PartitionId = std::uint64_t;
using Epoch = std::uint64_t;

/// Where a command claims it is going. The cell authenticates all four
/// fields before reading anything reclaimable.
struct C01Destination
{
    ProviderId provider = 0;
    PartitionId partition = 0;
    Epoch provider_epoch = 0;
    ReceiverGeneration receiver = 0;

    bool
    operator==(C01Destination const&) const = default;
};

/// The canonical command identity a C01 projection carries. The row
/// KEY is (sender_generation, channel_generation, command); everything
/// else is retained and compared at dispatch.
struct C01CommandIdentity
{
    OperationId operation = 0;
    InterestId interest = 0;
    UseId use = 0;
    PipelineId pipeline = 0;
    SenderGeneration sender_generation = 0;
    ChannelGeneration channel_generation = 0;
    CommandId command = 0;
    PayloadDigest payload = 0;

    bool
    operator==(C01CommandIdentity const&) const = default;
};

/// One incoming C01 projection: destination plus canonical identity.
struct C01Command
{
    C01Destination destination;
    C01CommandIdentity identity;
};

class C01DispatchCell;

/// Sole-minted dispatch receipt, bound to the complete cell identity
/// plus the full command identity and outcome — the exact evidence a
/// T117-analog needs to retire one outbox row. Truthful because
/// dispatch authenticated the destination before minting.
class C01Receipt
{
public:
    C01Receipt(C01Receipt const&) = default;
    C01Receipt&
    operator=(C01Receipt const&) = default;

    C01Destination const&
    cell() const
    {
        return cell_;
    }
    C01CommandIdentity const&
    identity() const
    {
        return identity_;
    }
    Outcome
    outcome() const
    {
        return outcome_;
    }

    bool
    operator==(C01Receipt const&) const = default;

private:
    friend class C01DispatchCell;  // sole minter
    C01Receipt(
        C01Destination cell,
        C01CommandIdentity identity,
        Outcome outcome)
        : cell_(cell), identity_(identity), outcome_(outcome)
    {
    }

    C01Destination cell_;
    C01CommandIdentity identity_;
    Outcome outcome_;
};

static_assert(
    !std::is_default_constructible_v<C01Receipt>,
    "a receipt must be minted, never defaulted");
static_assert(
    !std::is_constructible_v<
        C01Receipt,
        C01Destination,
        C01CommandIdentity,
        Outcome>,
    "raw-field construction would make every caller a second minter");

class C02CloseCommandTestMint;

/// Sender-owned C02 close command: the sender's channel owner (not
/// yet composed — tests mint via the explicit seam) asserts that
/// generation `generation` of its namespace is finished. Sole-minted
/// because the cell treats a presented close command as the trigger
/// to occupy the close slot. Carries the CANONICAL C02 identity:
/// the complete destination (provider, partition, epoch, receiver
/// generation) plus sender generation, channel generation, close
/// sequence, close kind, command id, and payload digest. The receiver
/// retains and echoes close sequence/kind/payload and never collapses
/// distinct bindings; close-sequence SUCCESSION is the sender's
/// obligation when composed.
class C02CloseCommand
{
public:
    C02CloseCommand(C02CloseCommand const&) = default;
    C02CloseCommand&
    operator=(C02CloseCommand const&) = default;

    C01Destination const&
    destination() const
    {
        return destination_;
    }
    SenderGeneration
    sender_generation() const
    {
        return sender_generation_;
    }
    ChannelGeneration
    generation() const
    {
        return generation_;
    }
    CloseSequence
    close_sequence() const
    {
        return close_sequence_;
    }
    CloseKind
    close_kind() const
    {
        return close_kind_;
    }
    CommandId
    close_command() const
    {
        return close_command_;
    }
    PayloadDigest
    payload() const
    {
        return payload_;
    }

    bool
    operator==(C02CloseCommand const&) const = default;

private:
    friend class C02CloseCommandTestMint;  // test-only until composed
    C02CloseCommand(
        C01Destination destination,
        SenderGeneration sender_generation,
        ChannelGeneration generation,
        CloseSequence close_sequence,
        CloseKind close_kind,
        CommandId close_command,
        PayloadDigest payload)
        : destination_(destination)
        , sender_generation_(sender_generation)
        , generation_(generation)
        , close_sequence_(close_sequence)
        , close_kind_(close_kind)
        , close_command_(close_command)
        , payload_(payload)
    {
    }

    C01Destination destination_;
    SenderGeneration sender_generation_;
    ChannelGeneration generation_;
    CloseSequence close_sequence_;
    CloseKind close_kind_;
    CommandId close_command_;
    PayloadDigest payload_;
};

static_assert(
    !std::is_default_constructible_v<C02CloseCommand>,
    "a close command must be minted, never defaulted");
static_assert(
    !std::is_constructible_v<
        C02CloseCommand,
        C01Destination,
        SenderGeneration,
        ChannelGeneration,
        CloseSequence,
        CloseKind,
        CommandId,
        PayloadDigest>,
    "raw-field construction would let any caller trigger a close");

class C01DispatchCell;

/// Sole-minted close receipt, bound to the complete cell identity plus
/// the FULL canonical close identity (sequence, kind, command, payload
/// echoed): the evidence the sender's serialized close pipeline — and
/// the close-kind-specific result consumers behind it — consume to
/// retire the close outbox. Retained by the cell for the LATEST closed
/// generation of each namespace and replayed identically.
class C02CloseReceipt
{
public:
    C02CloseReceipt(C02CloseReceipt const&) = default;
    C02CloseReceipt&
    operator=(C02CloseReceipt const&) = default;

    C01Destination const&
    cell() const
    {
        return cell_;
    }
    SenderGeneration
    sender_generation() const
    {
        return sender_generation_;
    }
    ChannelGeneration
    generation() const
    {
        return generation_;
    }
    CloseSequence
    close_sequence() const
    {
        return close_sequence_;
    }
    CloseKind
    close_kind() const
    {
        return close_kind_;
    }
    CommandId
    close_command() const
    {
        return close_command_;
    }
    PayloadDigest
    payload() const
    {
        return payload_;
    }

    bool
    operator==(C02CloseReceipt const&) const = default;

private:
    friend class C01DispatchCell;  // sole minter
    C02CloseReceipt(
        C01Destination cell,
        SenderGeneration sender_generation,
        ChannelGeneration generation,
        CloseSequence close_sequence,
        CloseKind close_kind,
        CommandId close_command,
        PayloadDigest payload)
        : cell_(cell)
        , sender_generation_(sender_generation)
        , generation_(generation)
        , close_sequence_(close_sequence)
        , close_kind_(close_kind)
        , close_command_(close_command)
        , payload_(payload)
    {
    }

    C01Destination cell_;
    SenderGeneration sender_generation_;
    ChannelGeneration generation_;
    CloseSequence close_sequence_;
    CloseKind close_kind_;
    CommandId close_command_;
    PayloadDigest payload_;
};

static_assert(
    !std::is_default_constructible_v<C02CloseReceipt>,
    "a receipt must be minted, never defaulted");
static_assert(
    !std::is_constructible_v<
        C02CloseReceipt,
        C01Destination,
        SenderGeneration,
        ChannelGeneration,
        CloseSequence,
        CloseKind,
        CommandId,
        PayloadDigest>,
    "raw-field construction would make every caller a second minter");

class C01CellTestSeed;

class C01DispatchCell
{
public:
    using strand_type = DeterministicScheduler::strand_type;

    enum class Case : std::uint8_t {
        first_commit,
        exact_replay,
        stale_rejected,
        conflict_rejected,
        wrong_destination_rejected,  // any of the four destination
                                     // fields disagrees with this cell
        invalid_identity,            // reserved CommandId 0
        closing_rejected,  // the closing prefix is sealed mid-close
        off_strand,
    };

    struct DispatchResult
    {
        Case dispatch{};
        std::optional<C01Receipt> receipt;  // first_commit + exact_replay
    };

    /// Pure successor arithmetic for generation closes — ordering
    /// only, NOT permission. Directly testable, including the executed
    /// ceiling case. Increment 3e's close authority is its sole
    /// production caller.
    enum class SuccessorCheck : std::uint8_t {
        ok,
        not_successor,
        generation_exhausted,
    };

    static SuccessorCheck
    check_close_successor(
        std::optional<ChannelGeneration> current,
        ChannelGeneration generation)
    {
        if (!current.has_value())
            return generation == 0 ? SuccessorCheck::ok
                                   : SuccessorCheck::not_successor;
        if (*current == std::numeric_limits<ChannelGeneration>::max())
            return SuccessorCheck::generation_exhausted;
        return generation == *current + 1 ? SuccessorCheck::ok
                                          : SuccessorCheck::not_successor;
    }

    C01DispatchCell(strand_type strand, C01Destination cell_identity)
        : strand_(std::move(strand)), cell_(cell_identity)
    {
    }

    bool
    on_owner_strand() const
    {
        return strand_.running_in_this_thread();
    }

    /// The closed dispatch. `outcome_if_first` models the receiver
    /// effect: consumed ONLY by a first commit; replays return the
    /// retained outcome untouched; every rejection consumes nothing.
    //@@authority transition=provider_lab.dispatch_c01 owner=provider_lab commit=strand
    DispatchResult
    dispatch(C01Command const& command, Outcome outcome_if_first)
    {
        if (!on_owner_strand())
            return {Case::off_strand, std::nullopt};
        // Destination authentication dominates everything: all four
        // fields, before any reclaimable read. A command that did not
        // name THIS cell can neither commit nor learn state here.
        if (command.destination != cell_)
            return {Case::wrong_destination_rejected, std::nullopt};
        auto const& id = command.identity;
        // Reserved-identity refusal precedes reclaimable reads.
        if (id.command == 0)
            return {Case::invalid_identity, std::nullopt};
        // Stale precedes inbox absence.
        auto hw = high_water_.find(id.sender_generation);
        if (hw != high_water_.end() &&
            id.channel_generation <= hw->second)
            return {Case::stale_rejected, std::nullopt};
        // A generation mid-close seals its closing prefix: reclamation
        // must never race a first-commit for the generation it is
        // about to erase.
        if (close_slot_.has_value() &&
            id.sender_generation == close_slot_->sender_generation() &&
            id.channel_generation <= close_slot_->generation())
            return {Case::closing_rejected, std::nullopt};
        auto const key = std::tuple{
            id.sender_generation, id.channel_generation, id.command};
        if (auto it = inbox_.find(key); it != inbox_.end())
        {
            if (it->second.receipt.identity() != id)
                return {Case::conflict_rejected, std::nullopt};
            return {Case::exact_replay, it->second.receipt};
        }
        C01Receipt receipt{cell_, id, outcome_if_first};
        inbox_.emplace(key, Row{receipt});
        return {Case::first_commit, receipt};
    }

    enum class CloseRefusal : std::uint8_t {
        ok,
        off_strand,
        wrong_destination,
        invalid_identity,  // reserved close-command id 0
        already_closed,    // exact replay: the retained receipt returns
        close_conflict,    // T129: same CommandId, different binding
        unmatched_equal,   // T127: different CommandId at the retained
                           // high-water generation
        stale_close,  // older than the retained receipt home covers
        slot_occupied,
        not_successor,
        generation_exhausted,
        no_close_in_flight,
    };

    struct CloseResult
    {
        CloseRefusal refusal{};
        // already_closed replay (begin) and the fresh mint (complete).
        std::optional<C02CloseReceipt> receipt;
    };

    /// Occupy the serialized close slot for one strictly-successive
    /// generation close. Order: destination authentication; reserved
    /// identity; the retained-receipt dedup domain (exact replay
    /// returns the IDENTICAL receipt; a different close identity for
    /// the closed generation conflicts; older generations are typed
    /// stale — their receipt home was reclaimed by the successor
    /// close); the slot; then the pure successor arithmetic. Nothing
    /// mutates except the slot.
    //@@authority transition=provider_lab.c02_begin_close owner=provider_lab commit=strand
    CloseResult
    begin_close(C02CloseCommand const& command)
    {
        if (!on_owner_strand())
            return {CloseRefusal::off_strand, std::nullopt};
        if (command.destination() != cell_)
            return {CloseRefusal::wrong_destination, std::nullopt};
        if (command.close_command() == 0)
            return {CloseRefusal::invalid_identity, std::nullopt};
        auto const ns = command.sender_generation();
        if (auto closed = closed_.find(ns); closed != closed_.end())
        {
            auto const& receipt = closed->second;
            if (command.generation() == receipt.generation())
            {
                // The canonical equal-generation partition: a
                // DIFFERENT CommandId is T127 unmatched-equal; the
                // SAME CommandId with any bound field differing is
                // the T129 identity/payload conflict; only the exact
                // full binding replays. All three are no-effect here.
                if (command.close_command() != receipt.close_command())
                    return {CloseRefusal::unmatched_equal, std::nullopt};
                if (command.close_sequence() == receipt.close_sequence() &&
                    command.close_kind() == receipt.close_kind() &&
                    command.payload() == receipt.payload())
                    return {CloseRefusal::already_closed, receipt};
                return {CloseRefusal::close_conflict, std::nullopt};
            }
            if (command.generation() < receipt.generation())
                return {CloseRefusal::stale_close, std::nullopt};
        }
        if (close_slot_.has_value())
            return {CloseRefusal::slot_occupied, std::nullopt};
        auto hw = high_water_.find(ns);
        auto const current = hw == high_water_.end()
            ? std::optional<ChannelGeneration>{}
            : std::optional<ChannelGeneration>{hw->second};
        switch (check_close_successor(current, command.generation()))
        {
            case SuccessorCheck::not_successor:
                return {CloseRefusal::not_successor, std::nullopt};
            case SuccessorCheck::generation_exhausted:
                return {CloseRefusal::generation_exhausted, std::nullopt};
            case SuccessorCheck::ok:
                break;
        }
        close_slot_ = command;
        return {CloseRefusal::ok, std::nullopt};
    }

    /// Apply the in-flight close: advance the high-water, bulk-reclaim
    /// EXACTLY the named generation's inbox rows, mint + RETAIN the
    /// bound close receipt (displacing the namespace's previous one),
    /// and release the slot — one owner-strand commit. Exception-
    /// atomic: both map nodes are PREPARED before the non-throwing
    /// commit; a failpoint throw leaves the mark, the rows, the
    /// receipts, and the still-occupied slot untouched, and the retry
    /// completes identically.
    //@@authority transition=provider_lab.c02_complete_close owner=provider_lab commit=strand
    CloseResult
    complete_close()
    {
        if (!on_owner_strand())
            return {CloseRefusal::off_strand, std::nullopt};
        if (!close_slot_.has_value())
            return {CloseRefusal::no_close_in_flight, std::nullopt};
        auto const& cmd = *close_slot_;
        auto const ns = cmd.sender_generation();
        auto const generation = cmd.generation();
        // PREPARE (throwing): both nodes, no state changed.
        std::map<SenderGeneration, ChannelGeneration> tmp_hw;
        tmp_hw.emplace(ns, generation);
        auto hw_node = tmp_hw.extract(ns);
        std::map<SenderGeneration, C02CloseReceipt> tmp_receipt;
        tmp_receipt.emplace(
            ns,
            C02CloseReceipt{
                cell_, ns, generation, cmd.close_sequence(),
                cmd.close_kind(), cmd.close_command(), cmd.payload()});
        auto receipt_node = tmp_receipt.extract(ns);
        if (test_complete_pre_commit)
            test_complete_pre_commit();
        // COMMIT (non-throwing): displace-and-insert both nodes,
        // reclaim exactly this generation's rows, release the slot.
        high_water_.erase(ns);
        high_water_.insert(std::move(hw_node));
        closed_.erase(ns);
        auto const it = closed_.insert(std::move(receipt_node)).position;
        // Range end via upper_bound of the maximal key IN the
        // generation — no successor arithmetic, so closing the
        // ceiling generation cannot wrap the range (the class again,
        // refused at the reclamation boundary).
        inbox_.erase(
            inbox_.lower_bound(std::tuple{ns, generation, CommandId{0}}),
            inbox_.upper_bound(std::tuple{
                ns, generation,
                std::numeric_limits<CommandId>::max()}));
        close_slot_.reset();
        return {CloseRefusal::ok, it->second};
    }

    /// TEST-ONLY failpoint between complete_close's prepare and
    /// commit. Empty in production shapes.
    std::function<void()> test_complete_pre_commit;

    // Owner-strand introspection; refusal dominates every read.
    std::optional<std::size_t>
    row_count() const
    {
        if (!on_owner_strand())
            return std::nullopt;
        return inbox_.size();
    }
    std::optional<bool>
    close_in_flight() const
    {
        if (!on_owner_strand())
            return std::nullopt;
        return close_slot_.has_value();
    }
    std::optional<ChannelGeneration>
    stale_high_water(SenderGeneration ns) const
    {
        if (!on_owner_strand())
            return std::nullopt;
        auto it = high_water_.find(ns);
        if (it == high_water_.end())
            return std::nullopt;  // sentinel: nothing closed yet
        return it->second;
    }

private:
    struct Row
    {
        C01Receipt receipt;  // full identity + outcome live inside
    };

    // Test-only stale-state seeding; explicitly NOT production
    // authority (no authority annotation). Increment 3e's C02 close
    // operation is the sole production mover of these marks.
    friend class ::catl::provider_lab::C01CellTestSeed;

    strand_type strand_;
    C01Destination cell_;
    //@@authority state=provider_lab.c01_inbox owner=provider_lab
    std::map<
        std::tuple<SenderGeneration, ChannelGeneration, CommandId>,
        Row>
        inbox_;
    //@@authority state=provider_lab.c01_stale_high_water owner=provider_lab
    std::map<SenderGeneration, ChannelGeneration> high_water_;
    //@@authority state=provider_lab.c02_close_slot owner=provider_lab
    std::optional<C02CloseCommand> close_slot_;
    //@@authority state=provider_lab.c02_closed_receipts owner=provider_lab
    std::map<SenderGeneration, C02CloseReceipt> closed_;
};

/// Test-only mint for the C02 close command, whose production minter
/// (the sender's channel owner) is not composed. Explicitly not
/// production authority.
class C02CloseCommandTestMint
{
public:
    static C02CloseCommand
    mint(
        C01Destination destination,
        SenderGeneration sender_generation,
        ChannelGeneration generation,
        CloseSequence close_sequence,
        CloseKind close_kind,
        CommandId close_command,
        PayloadDigest payload)
    {
        return C02CloseCommand{
            destination,   sender_generation, generation,
            close_sequence, close_kind,       close_command,
            payload};
    }
};

/// Test-only seed for stale state. Not a transition, not annotated,
/// never a production caller: it exists so 3b dispatch controls can
/// construct stale scenarios before 3e lands the real close authority.
class C01CellTestSeed
{
public:
    static void
    seed_stale_high_water(
        C01DispatchCell& cell,
        SenderGeneration ns,
        ChannelGeneration mark)
    {
        cell.high_water_[ns] = mark;
    }
};

}  // namespace catl::provider_lab
