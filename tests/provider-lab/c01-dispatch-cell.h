#pragma once

// Coordinator increment 3b, round 2: the closed C01 receiver dispatch
// cell with FULL canonical identity and the strict-successor close gate
// pulled forward (accepted design + round-1 review corrections).
// TEST-ONLY: production construction must never reference this.
//
// The dispatch contract, from the v4 corpus:
//  - the cell owns immutable (ProviderId, PartitionId, ProviderEpoch,
//    ReceiverGeneration) identity; a wrong receiver generation rejects
//    BEFORE any read of reclaimable sender-generation state;
//  - commands carry the full canonical identity; dedup rows are KEYED
//    by (SenderGeneration, ChannelGeneration, CommandId) and RETAIN
//    every remaining identity/payload field — an exact full binding
//    replays the identical receipt, ANY mismatch conflicts without
//    effect or receipt (distinct operation/interest/use/pipeline
//    identities can never alias one row's outcome);
//  - stale rejects at or below the per-sender-namespace high-water
//    BEFORE inbox absence can select first-commit; namespaces are
//    SenderGenerations, so sender restarts own independent marks;
//  - the high-water moves ONLY through the strict-successor close:
//    an untouched namespace closes generation 0 first, then exactly
//    current+1 — gaps, replays, and regressions are typed refusals,
//    the ceiling fails closed, and reclamation removes EXACTLY the
//    generation being closed (monotonic is weaker than contiguous;
//    exact-prefix deletion cannot amplify a gap into multi-generation
//    destruction);
//  - CommandId 0 is reserved by its non-reused minter: rejected as
//    invalid identity after receiver validation, before any
//    high-water/inbox read. ChannelGeneration 0 IS valid: a namespace
//    begins at the sentinel immediately before generation zero;
//  - receipts are sole-minted, bound to the complete cell + command
//    identity + outcome, so T117 can retire the exact outbox.

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

class C01DispatchCell;

/// Sole-minted dispatch receipt, bound to the complete cell identity
/// plus the full command identity and outcome — the exact evidence a
/// T117-analog needs to retire one outbox row.
class C01Receipt
{
public:
    C01Receipt(C01Receipt const&) = default;
    C01Receipt&
    operator=(C01Receipt const&) = default;

    ProviderId
    provider() const
    {
        return provider_;
    }
    PartitionId
    partition() const
    {
        return partition_;
    }
    Epoch
    provider_epoch() const
    {
        return provider_epoch_;
    }
    ReceiverGeneration
    receiver() const
    {
        return receiver_;
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
        ProviderId provider,
        PartitionId partition,
        Epoch provider_epoch,
        ReceiverGeneration receiver,
        C01CommandIdentity identity,
        Outcome outcome)
        : provider_(provider)
        , partition_(partition)
        , provider_epoch_(provider_epoch)
        , receiver_(receiver)
        , identity_(identity)
        , outcome_(outcome)
    {
    }

    ProviderId provider_;
    PartitionId partition_;
    Epoch provider_epoch_;
    ReceiverGeneration receiver_;
    C01CommandIdentity identity_;
    Outcome outcome_;
};

static_assert(
    !std::is_default_constructible_v<C01Receipt>,
    "a receipt must be minted, never defaulted");
static_assert(
    !std::is_constructible_v<
        C01Receipt,
        ProviderId,
        PartitionId,
        Epoch,
        ReceiverGeneration,
        C01CommandIdentity,
        Outcome>,
    "raw-field construction would make every caller a second minter");

class C01DispatchCell
{
public:
    using strand_type = DeterministicScheduler::strand_type;

    enum class Case : std::uint8_t {
        first_commit,
        exact_replay,
        stale_rejected,
        conflict_rejected,
        wrong_receiver_rejected,
        invalid_identity,  // CommandId 0: reserved by the non-reused
                           // minter; rejected after receiver validation,
                           // before any high-water/inbox read
        off_strand,
    };

    struct DispatchResult
    {
        Case dispatch{};
        std::optional<C01Receipt> receipt;  // first_commit + exact_replay
    };

    enum class CloseResult : std::uint8_t {
        ok,
        off_strand,
        not_successor,         // gap, replay, or regression: closes are
                               // strict contiguous successors
        generation_exhausted,  // high-water at ceiling: no successor
                               // exists; fail closed
    };

    C01DispatchCell(
        strand_type strand,
        ProviderId provider,
        PartitionId partition,
        Epoch provider_epoch,
        ReceiverGeneration receiver)
        : strand_(std::move(strand))
        , provider_(provider)
        , partition_(partition)
        , provider_epoch_(provider_epoch)
        , receiver_(receiver)
    {
    }

    bool
    on_owner_strand() const
    {
        return strand_.running_in_this_thread();
    }

    /// The closed dispatch. `outcome_if_first` models the receiver
    /// effect: consumed ONLY by a first commit; replays return the
    /// retained outcome untouched; conflicts and rejections consume
    /// nothing.
    //@@authority transition=provider_lab.dispatch_c01 owner=provider_lab commit=strand
    DispatchResult
    dispatch(
        ReceiverGeneration receiver_tag,
        C01CommandIdentity const& id,
        Outcome outcome_if_first)
    {
        if (!on_owner_strand())
            return {Case::off_strand, std::nullopt};
        // Guard precedence: receiver identity first, always.
        if (receiver_tag != receiver_)
            return {Case::wrong_receiver_rejected, std::nullopt};
        // Reserved-identity refusal precedes every reclaimable read.
        if (id.command == 0)
            return {Case::invalid_identity, std::nullopt};
        // Stale precedes inbox absence. Namespaces are sender
        // GENERATIONS: a restarted sender owns a fresh mark.
        auto hw = high_water_.find(id.sender_generation);
        if (hw != high_water_.end() &&
            id.channel_generation <= hw->second)
            return {Case::stale_rejected, std::nullopt};
        auto const key = std::tuple{
            id.sender_generation, id.channel_generation, id.command};
        if (auto it = inbox_.find(key); it != inbox_.end())
        {
            // Exact FULL binding replays; ANY mismatch — payload or
            // any retained identity field — conflicts. Distinct
            // operation/interest/use/pipeline identities can never
            // alias one row's outcome.
            if (it->second.receipt.identity() != id)
                return {Case::conflict_rejected, std::nullopt};
            return {Case::exact_replay, it->second.receipt};
        }
        C01Receipt receipt{
            provider_, partition_, provider_epoch_, receiver_, id,
            outcome_if_first};
        inbox_.emplace(key, Row{receipt});
        return {Case::first_commit, receipt};
    }

    /// The minimal C02 close gate, pulled forward from 3e so no public
    /// verb is a second authority: closes are STRICT CONTIGUOUS
    /// successors (an untouched namespace closes generation 0 first,
    /// then exactly current+1), and reclamation removes EXACTLY the
    /// generation being closed. 3e adds the serialized slot and close
    /// identity/receipt on top of this gate; it cannot weaken it.
    //@@authority transition=provider_lab.close_generation owner=provider_lab commit=strand
    CloseResult
    close_generation(SenderGeneration ns, ChannelGeneration generation)
    {
        if (!on_owner_strand())
            return CloseResult::off_strand;
        auto it = high_water_.find(ns);
        if (it == high_water_.end())
        {
            if (generation != 0)
                return CloseResult::not_successor;
        }
        else
        {
            if (it->second ==
                std::numeric_limits<ChannelGeneration>::max())
                return CloseResult::generation_exhausted;
            if (generation != it->second + 1)
                return CloseResult::not_successor;
        }
        high_water_[ns] = generation;
        // Exact-prefix reclamation: only THIS generation's rows. With
        // strict succession, earlier generations are already gone.
        auto row = inbox_.lower_bound(std::tuple{ns, generation, 0});
        while (row != inbox_.end() && std::get<0>(row->first) == ns &&
               std::get<1>(row->first) == generation)
            row = inbox_.erase(row);
        return CloseResult::ok;
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

    strand_type strand_;
    ProviderId provider_;
    PartitionId partition_;
    Epoch provider_epoch_;
    ReceiverGeneration receiver_;
    //@@authority state=provider_lab.c01_inbox owner=provider_lab
    std::map<
        std::tuple<SenderGeneration, ChannelGeneration, CommandId>,
        Row>
        inbox_;
    //@@authority state=provider_lab.c01_stale_high_water owner=provider_lab
    std::map<SenderGeneration, ChannelGeneration> high_water_;
};

}  // namespace catl::provider_lab
