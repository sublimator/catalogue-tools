#pragma once

// Coordinator partition, increment 3a: the canonical fetch-key index
// (accepted design: ai-docs provider-lab-coordinator-design.md at
// 21af68e/345061f). TEST-ONLY: production construction must never
// reference this.
//
// Load-bearing invariants in this increment:
//  - ONE FetchId per logical need: the index
//    (accepted epoch, DemandFetchKey) -> FetchId is created and removed
//    atomically with its fetch record, in single owner-strand commits —
//    no tear is expressible;
//  - TransportShape is attempt data inside the record, never identity:
//    a differently-shaped second miss JOINS the existing fetch;
//  - FetchIds are minted monotonically, never reused, zero reserved,
//    and the mint is fail-closed at the ceiling (fetch_id_exhausted) —
//    the false-agreement-via-wrap class, pre-empted; existing records
//    stay fully usable, only NEW mints refuse;
//  - removal consumes record and index together; a re-miss after
//    removal mints a NEW FetchId (no resurrection);
//  - the epoch/barrier/stop machine is the shared EpochControl; this
//    partition's clear/advance receipt is a LEAF later consumed by the
//    provider-level fan-out join.

#include "deterministic-scheduler.h"
#include "epoch-control.h"

#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <string>
#include <utility>

namespace catl::provider_lab {

using FetchId = std::uint64_t;
using DemandFetchKey = std::string;
using TransportShape = std::string;
using ProviderId = std::uint64_t;
using PartitionId = std::uint64_t;

/// Immutable exact receipt for one partition epoch advance — the leaf
/// evidence a later provider-level fan-out join reconciles. Only the
/// drained success path can mint one; no refusal carries a receipt.
struct PartitionAdvanceReceipt
{
    ProviderId provider = 0;
    PartitionId partition = 0;
    BarrierId barrier = 0;
    Epoch old_epoch = 0;
    Epoch new_epoch = 0;
};

class CoordinatorPartition
{
public:
    using strand_type = DeterministicScheduler::strand_type;

    enum class Refusal : std::uint8_t {
        ok,
        off_strand,
        wrong_epoch,
        barrier_installed,
        wrong_barrier,
        not_running,
        stopped,
        epoch_exhausted,
        barrier_reused,
        fetch_id_exhausted,  // mint at ceiling: refused, records intact
        unknown_fetch,
        not_drained,  // advance requires the zero witness as a
                      // PRECONDITION; live rows refuse it, the barrier
                      // stays installed, and no receipt exists
    };

    struct AdvanceResult
    {
        Refusal refusal{};
        std::optional<PartitionAdvanceReceipt> receipt;
    };

    struct FetchResult
    {
        Refusal refusal{};
        FetchId fetch = 0;
        bool created = false;  // false: joined the existing fetch
    };

    CoordinatorPartition(
        strand_type strand,
        ProviderId provider,
        PartitionId partition,
        Epoch initial_epoch,
        FetchId first_minted_id = 1)
        : strand_(std::move(strand))
        , provider_(provider)
        , partition_(partition)
        , control_(initial_epoch)
        , next_fetch_(first_minted_id == 0 ? 1 : first_minted_id)
    {
        // Zero is reserved (matching the acceptance-gate conventions);
        // first_minted_id is a lab seam so ceiling controls can start
        // the mint near the maximum without 2^64 warm-up calls.
    }

    bool
    on_owner_strand() const
    {
        return strand_.running_in_this_thread();
    }

    /// One FetchId per logical need. A miss mints atomically with the
    /// record; a hit JOINS — shape is recorded as attempt data and
    /// never participates in identity.
    //@@authority transition=provider_lab.get_or_create_fetch owner=provider_lab commit=strand
    FetchResult
    get_or_create_fetch(Epoch tag, DemandFetchKey key, TransportShape shape)
    {
        if (!on_owner_strand())
            return {Refusal::off_strand};
        if (auto r = admit(tag); r != Refusal::ok)
            return {r};
        if (auto it = index_.find(key); it != index_.end())
        {
            // A joiner has NO authority over the active attempt's
            // response contract: the shape that was sent is the shape
            // that validates. A hit is accounting only; shape changes
            // arrive solely through an explicit owner transition
            // (advance_revision, owned by the attempt/retry increment)
            // after the prior attempt terminalizes.
            auto& record = records_.at(it->second);
            if (record.joins < std::numeric_limits<std::uint64_t>::max())
                ++record.joins;  // diagnostic; saturates, never wraps
            return {Refusal::ok, it->second, false};
        }
        if (next_fetch_ == std::numeric_limits<FetchId>::max())
        {
            // Minting would exhaust the id space; wrapping or reuse
            // would let an ancient FetchId falsely agree with a new
            // need. Refuse the MINT only: joins of existing fetches
            // remain fully serviceable.
            return {Refusal::fetch_id_exhausted};
        }
        auto const id = next_fetch_++;
        index_.emplace(key, id);
        records_.emplace(
            id, FetchRecord{std::move(key), std::move(shape), 0});
        return {Refusal::ok, id, true};
    }

    /// Remove the fetch record and its index entry in one owner-strand
    /// commit. A later re-miss mints a NEW FetchId — no resurrection.
    /// Terminal/drain admission: legal in running OR under the barrier
    /// (draining old-epoch work is the barrier's purpose); the exact
    /// epoch tag is still validated. New-work admission stays
    /// running-only in get_or_create_fetch.
    //@@authority transition=provider_lab.remove_fetch owner=provider_lab commit=strand
    Refusal
    remove_fetch(Epoch tag, FetchId id)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        if (auto r = admit_terminal(tag); r != Refusal::ok)
            return r;
        auto it = records_.find(id);
        if (it == records_.end())
            return Refusal::unknown_fetch;
        index_.erase(it->second.key);
        records_.erase(it);
        return Refusal::ok;
    }

    //@@authority transition=provider_lab.install_partition_barrier owner=provider_lab commit=strand
    Refusal
    install_barrier(BarrierId id)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        return map_check(control_.install_barrier(id));
    }

    /// Advance requires the zero witness as a PRECONDITION: live
    /// index/records refuse `not_drained`, the barrier stays installed,
    /// and no receipt exists. On the drained success path this mints
    /// the immutable PartitionAdvanceReceipt — the exact leaf evidence
    /// the later provider-level fan-out join reconciles.
    //@@authority transition=provider_lab.clear_and_advance_partition owner=provider_lab commit=strand
    AdvanceResult
    advance_epoch(BarrierId id)
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, std::nullopt};
        if (!records_.empty() || !index_.empty())
            return {Refusal::not_drained, std::nullopt};
        auto const old_epoch = control_.epoch();
        auto const check = control_.consume_barrier_and_advance(id);
        if (check != EpochControl::Check::ok)
            return {map_check(check), std::nullopt};
        return {
            Refusal::ok,
            PartitionAdvanceReceipt{
                provider_, partition_, id, old_epoch, control_.epoch()}};
    }

    // Owner-strand introspection; refusal dominates every read.
    std::optional<Epoch>
    accepted_epoch() const
    {
        if (!on_owner_strand())
            return std::nullopt;
        return control_.epoch();
    }
    std::optional<std::size_t>
    fetch_count() const
    {
        if (!on_owner_strand())
            return std::nullopt;
        return records_.size();
    }
    std::optional<std::uint64_t>
    joins_of(FetchId id) const
    {
        if (!on_owner_strand())
            return std::nullopt;
        auto it = records_.find(id);
        if (it == records_.end())
            return std::nullopt;
        return it->second.joins;
    }
    /// The ACTIVE attempt's shape — the contract responses validate
    /// against. Joiners cannot rewrite it (see get_or_create_fetch).
    std::optional<TransportShape>
    active_shape_of(FetchId id) const
    {
        if (!on_owner_strand())
            return std::nullopt;
        auto it = records_.find(id);
        if (it == records_.end())
            return std::nullopt;
        return it->second.last_shape;
    }

private:
    struct FetchRecord
    {
        DemandFetchKey key;
        TransportShape last_shape;  // attempt data, never identity
        std::uint64_t joins = 0;
    };

    Refusal
    admit(Epoch tag) const
    {
        return map_check(control_.check_running(tag));
    }

    /// Terminal/drain admission: running or barrier, exact epoch only.
    Refusal
    admit_terminal(Epoch tag) const
    {
        if (control_.state() == EpochControl::State::stopped)
            return Refusal::stopped;
        if (tag != control_.epoch())
            return Refusal::wrong_epoch;
        return Refusal::ok;
    }

    static Refusal
    map_check(EpochControl::Check check)
    {
        switch (check)
        {
            case EpochControl::Check::ok:
                return Refusal::ok;
            case EpochControl::Check::wrong_epoch:
                return Refusal::wrong_epoch;
            case EpochControl::Check::barrier_installed:
                return Refusal::barrier_installed;
            case EpochControl::Check::stopped:
                return Refusal::stopped;
            case EpochControl::Check::wrong_barrier:
                return Refusal::wrong_barrier;
            case EpochControl::Check::not_running:
                return Refusal::not_running;
            case EpochControl::Check::epoch_exhausted:
                return Refusal::epoch_exhausted;
            case EpochControl::Check::barrier_reused:
                return Refusal::barrier_reused;
        }
        return Refusal::not_running;
    }

    strand_type strand_;
    ProviderId provider_;
    PartitionId partition_;
    //@@authority state=provider_lab.partition_control owner=provider_lab
    EpochControl control_;
    FetchId next_fetch_;
    //@@authority state=provider_lab.fetch_index owner=provider_lab
    std::map<DemandFetchKey, FetchId> index_;
    std::map<FetchId, FetchRecord> records_;
};

}  // namespace catl::provider_lab
