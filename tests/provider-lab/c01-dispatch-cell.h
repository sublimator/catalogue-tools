#pragma once

// Coordinator increment 3b: the closed five-case C01 receiver dispatch
// cell (accepted design: provider-lab-coordinator-design.md, invariant
// 4). TEST-ONLY: production construction must never reference this.
//
// The dispatch contract, from the v4 corpus:
//  - the owner-local cell rejects a wrong ReceiverGeneration BEFORE
//    touching any reclaimable sender-generation state (guard
//    precedence — identity/phase dominate reads);
//  - at or below the per-sender stale high-water rejects BEFORE inbox
//    absence can select first-commit;
//  - above it: an exact identity+payload match REPLAYS the retained
//    receipt (identical, effect not repeated); the same identity with
//    a different payload CONFLICTS without effect or receipt; only an
//    absent inbox row FIRST-COMMITS — effect exactly once, bounded row
//    retained {identity, payload digest, outcome, receipt}, never
//    payload bytes;
//  - the high-water is a monotonic acceptance gate whose ordering
//    authority is receipt-gated succession (C02 closes are strict
//    contiguous successors; increment 3e is its caller);
//  - receipts are sole-minted by the cell: construction from raw
//    fields is a compile error, so possession is evidence.

#include "deterministic-scheduler.h"

#include <cstdint>
#include <map>
#include <optional>
#include <tuple>
#include <type_traits>

namespace catl::provider_lab {

using SenderId = std::uint64_t;
using ChannelGeneration = std::uint64_t;
using CommandId = std::uint64_t;
using ReceiverGeneration = std::uint64_t;
using PayloadDigest = std::uint64_t;
using Outcome = std::uint64_t;

class C01DispatchCell;

/// Sole-minted dispatch receipt: first commit derives it, exact replay
/// returns the IDENTICAL retained value. No default or raw-field
/// construction exists outside the cell.
class C01Receipt
{
public:
    C01Receipt(C01Receipt const&) = default;
    C01Receipt&
    operator=(C01Receipt const&) = default;

    ReceiverGeneration
    receiver() const
    {
        return receiver_;
    }
    SenderId
    sender() const
    {
        return sender_;
    }
    ChannelGeneration
    channel_generation() const
    {
        return channel_generation_;
    }
    CommandId
    command() const
    {
        return command_;
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
        ReceiverGeneration receiver,
        SenderId sender,
        ChannelGeneration channel_generation,
        CommandId command,
        Outcome outcome)
        : receiver_(receiver)
        , sender_(sender)
        , channel_generation_(channel_generation)
        , command_(command)
        , outcome_(outcome)
    {
    }

    ReceiverGeneration receiver_;
    SenderId sender_;
    ChannelGeneration channel_generation_;
    CommandId command_;
    Outcome outcome_;
};

static_assert(
    !std::is_default_constructible_v<C01Receipt>,
    "a receipt must be minted, never defaulted");
static_assert(
    !std::is_constructible_v<
        C01Receipt,
        ReceiverGeneration,
        SenderId,
        ChannelGeneration,
        CommandId,
        Outcome>,
    "raw-field construction would make every caller a second minter");

class C01DispatchCell
{
public:
    using strand_type = DeterministicScheduler::strand_type;

    /// The closed five-case dispatch, plus the off-strand refusal.
    /// Mirrors the accepted v5 contract's mutually-exclusive-exhaustive
    /// dispatch_cases proof shape.
    enum class Case : std::uint8_t {
        first_commit,
        exact_replay,
        stale_rejected,
        conflict_rejected,
        wrong_receiver_rejected,
        off_strand,
    };

    struct DispatchResult
    {
        Case dispatch{};
        std::optional<C01Receipt> receipt;  // first_commit + exact_replay
    };

    enum class HighWaterResult : std::uint8_t {
        ok,
        off_strand,
        not_monotonic,  // strict increase only: the acceptance gate
    };

    C01DispatchCell(strand_type strand, ReceiverGeneration receiver)
        : strand_(std::move(strand)), receiver_(receiver)
    {
    }

    bool
    on_owner_strand() const
    {
        return strand_.running_in_this_thread();
    }

    /// T67/T119/T126/T128-analog in one total verb. `outcome_if_first`
    /// models the receiver effect: it is consumed ONLY by a first
    /// commit; replays return the retained outcome untouched (effect
    /// exactly once), conflicts and rejections consume nothing.
    //@@authority transition=provider_lab.dispatch_c01 owner=provider_lab commit=strand
    DispatchResult
    dispatch(
        ReceiverGeneration receiver_tag,
        SenderId sender,
        ChannelGeneration channel_generation,
        CommandId command,
        PayloadDigest payload,
        Outcome outcome_if_first)
    {
        if (!on_owner_strand())
            return {Case::off_strand, std::nullopt};
        // Guard precedence, exactly as the model orders it: the
        // receiver-generation check dominates every read of
        // reclaimable sender-generation state (high-water, inbox).
        if (receiver_tag != receiver_)
            return {Case::wrong_receiver_rejected, std::nullopt};
        // Stale rejects BEFORE inbox absence can select first-commit.
        auto hw = high_water_.find(sender);
        if (hw != high_water_.end() && channel_generation <= hw->second)
            return {Case::stale_rejected, std::nullopt};
        auto const key = std::tuple{sender, channel_generation, command};
        if (auto it = inbox_.find(key); it != inbox_.end())
        {
            if (it->second.payload != payload)
                return {Case::conflict_rejected, std::nullopt};
            return {Case::exact_replay, it->second.receipt};
        }
        // First commit: effect exactly once, bounded row retained.
        C01Receipt receipt{
            receiver_, sender, channel_generation, command,
            outcome_if_first};
        inbox_.emplace(key, Row{payload, receipt});
        return {Case::first_commit, receipt};
    }

    /// The stale high-water is a monotonic acceptance gate; its
    /// ordering authority is receipt-gated succession — increment 3e's
    /// generation close is the caller, and closes are strict
    /// contiguous successors, so the mark's ordering assumption is
    /// proven, not assumed. Reclaims exactly the closed generations'
    /// rows for this sender.
    //@@authority transition=provider_lab.advance_stale_high_water owner=provider_lab commit=strand
    HighWaterResult
    advance_stale_high_water(SenderId sender, ChannelGeneration to)
    {
        if (!on_owner_strand())
            return HighWaterResult::off_strand;
        auto it = high_water_.find(sender);
        if (it != high_water_.end() && to <= it->second)
            return HighWaterResult::not_monotonic;
        high_water_[sender] = to;
        // Bulk-reclaim exactly this sender's rows at or below the new
        // mark (3e semantics; row COUNT boundedness still arrives only
        // with sender credit evidence at composition, per the design).
        auto row = inbox_.lower_bound(std::tuple{sender, 0, 0});
        while (row != inbox_.end() && std::get<0>(row->first) == sender &&
               std::get<1>(row->first) <= to)
            row = inbox_.erase(row);
        return HighWaterResult::ok;
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
    stale_high_water(SenderId sender) const
    {
        if (!on_owner_strand())
            return std::nullopt;
        auto it = high_water_.find(sender);
        if (it == high_water_.end())
            return std::nullopt;  // sentinel: nothing closed yet
        return it->second;
    }

private:
    struct Row
    {
        PayloadDigest payload;
        C01Receipt receipt;  // outcome lives inside the receipt
    };

    strand_type strand_;
    ReceiverGeneration receiver_;
    //@@authority state=provider_lab.c01_inbox owner=provider_lab
    std::map<std::tuple<SenderId, ChannelGeneration, CommandId>, Row> inbox_;
    //@@authority state=provider_lab.c01_stale_high_water owner=provider_lab
    std::map<SenderId, ChannelGeneration> high_water_;
};

}  // namespace catl::provider_lab
