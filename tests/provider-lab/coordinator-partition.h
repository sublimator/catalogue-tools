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
    };

    struct FetchResult
    {
        Refusal refusal{};
        FetchId fetch = 0;
        bool created = false;  // false: joined the existing fetch
    };

    CoordinatorPartition(
        strand_type strand,
        Epoch initial_epoch,
        FetchId first_minted_id = 1)
        : strand_(std::move(strand))
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
            auto& record = records_.at(it->second);
            record.last_shape = std::move(shape);
            ++record.joins;
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
    //@@authority transition=provider_lab.remove_fetch owner=provider_lab commit=strand
    Refusal
    remove_fetch(Epoch tag, FetchId id)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        if (auto r = admit(tag); r != Refusal::ok)
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

    /// The clear/advance receipt this returns (ok) is the LEAF evidence
    /// the later provider-level fan-out join consumes.
    //@@authority transition=provider_lab.clear_and_advance_partition owner=provider_lab commit=strand
    Refusal
    clear_and_advance(BarrierId id)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        auto const check = control_.consume_barrier_and_advance(id);
        if (check == EpochControl::Check::ok)
        {
            index_.clear();
            records_.clear();
        }
        return map_check(check);
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
    //@@authority state=provider_lab.partition_control owner=provider_lab
    EpochControl control_;
    FetchId next_fetch_;
    //@@authority state=provider_lab.fetch_index owner=provider_lab
    std::map<DemandFetchKey, FetchId> index_;
    std::map<FetchId, FetchRecord> records_;
};

}  // namespace catl::provider_lab
