#pragma once

// Coordinator increment 3b, round 3: the closed C01 receiver dispatch
// cell with full destination authentication. TEST-ONLY: production
// construction must never reference this.
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
#include <limits>
#include <map>
#include <optional>
#include <tuple>
#include <type_traits>

namespace catl::provider_lab {

using SenderGeneration = std::uint64_t;
using ChannelGeneration = std::uint64_t;
using CommandId = std::uint64_t;
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

    // Owner-strand introspection; refusal dominates every read.
    std::optional<std::size_t>
    row_count() const
    {
        if (!on_owner_strand())
            return std::nullopt;
        return inbox_.size();
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
