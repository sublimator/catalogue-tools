#pragma once

// Epoch-guarded object store — target-stack package 4, increment 1
// (ledger-data-provider decomposition v4, "Store ownership and epochs").
// TEST-ONLY: production construction must never reference this.
//
// The contract, from the design body and state ledger (S60/S61,
// T80/T81/T82/T83/T87):
//  - the store contains IMMUTABLE values only, keyed by content hash;
//    insertion verifies the hash before any mutation, and a re-insert
//    of the same content is an idempotent no-op;
//  - owner state initializes with provider epoch E; every command is
//    epoch-tagged and rechecked AT THE OWNER COMMIT — a wrong-epoch
//    command is a typed rejection, never a race;
//  - the epoch changes only via the named barrier protocol: T80
//    installs barrier(E, BarrierId) after which old-E commits reject;
//    T81 consumes the exact barrier, clears values, advances to
//    running(E+1), and returns the exactly-once acknowledgement;
//  - eviction (LRU, capacity-bounded) is a distinct owner-local
//    operation, never part of clear or a substitute for it;
//  - T82 stop is absorbing: the stopped dispatch tombstone permits
//    only rejection, and stop emits the Q08-analog witness exactly
//    once. Destructors are passive; off-strand touches are refused.

#include "deterministic-scheduler.h"

#include <cstdint>
#include <functional>
#include <list>
#include <map>
#include <optional>
#include <string>
#include <utility>

namespace catl::provider_lab {

using Epoch = std::uint64_t;
using BarrierId = std::uint64_t;
using ContentHash = std::uint64_t;

/// Lab stand-in for canonical hashing: the CHECK is the point, not the
/// function. Production uses the real content hash.
inline ContentHash
lab_content_hash(std::string const& bytes)
{
    return std::hash<std::string>{}(bytes);
}

class EpochGuardedObjectStore
{
public:
    using strand_type = DeterministicScheduler::strand_type;

    enum class Refusal : std::uint8_t {
        ok,
        off_strand,
        wrong_epoch,       // command tag != accepted running epoch
        barrier_installed, // store is fencing; old-epoch work rejects
        hash_mismatch,     // bytes do not hash to the claimed key
        wrong_barrier,     // advance with a non-matching BarrierId
        not_running,       // barrier install outside running state
        stopped,           // absorbing tombstone: rejection only
    };

    struct StopWitness
    {
        Epoch final_epoch = 0;
        std::size_t values_at_stop = 0;
        std::uint64_t total_inserts = 0;
        std::uint64_t total_evictions = 0;
    };

    EpochGuardedObjectStore(
        strand_type strand,
        Epoch initial_epoch,
        std::size_t capacity)
        : strand_(std::move(strand))
        , epoch_(initial_epoch)
        , capacity_(capacity == 0 ? 1 : capacity)
    {
        // A zero-capacity store would breach its own bound on first
        // insert (evict-then-insert still lands one row). Clamp to the
        // smallest honest capacity instead of lying about zero.
    }

    bool
    on_owner_strand() const
    {
        return strand_.running_in_this_thread();
    }

    /// T83-analog: epoch-checked immutable insert with hash
    /// verification and LRU eviction at capacity.
    //@@authority transition=provider_lab.insert_object owner=provider_lab commit=strand
    Refusal
    insert(Epoch tag, ContentHash key, std::string bytes)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        if (auto r = check_running(tag); r != Refusal::ok)
            return r;
        if (lab_content_hash(bytes) != key)
            return Refusal::hash_mismatch;
        if (auto it = values_.find(key); it != values_.end())
        {
            touch(it);
            return Refusal::ok;  // immutable: idempotent re-insert
        }
        while (values_.size() >= capacity_ && !lru_.empty())
        {
            // T87-analog eviction: owner-local, distinct from clear.
            auto victim = lru_.back();
            lru_.pop_back();
            values_.erase(victim);
            ++total_evictions_;
        }
        lru_.push_front(key);
        values_.emplace(key, Value{std::move(bytes), lru_.begin()});
        ++total_inserts_;
        return Refusal::ok;
    }

    /// Epoch-checked lookup; refreshes LRU recency on hit.
    std::pair<Refusal, std::optional<std::string>>
    lookup(Epoch tag, ContentHash key)
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, std::nullopt};
        if (auto r = check_running(tag); r != Refusal::ok)
            return {r, std::nullopt};
        auto it = values_.find(key);
        if (it == values_.end())
            return {Refusal::ok, std::nullopt};
        touch(it);
        return {Refusal::ok, it->second.bytes};
    }

    /// T80-analog: install the epoch barrier. After this commit every
    /// old-epoch command rejects at the owner; only the matching
    /// advance may move the store forward.
    //@@authority transition=provider_lab.install_epoch_barrier owner=provider_lab commit=strand
    Refusal
    install_barrier(BarrierId id)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        if (state_ == State::stopped)
            return Refusal::stopped;
        if (state_ != State::running)
            return Refusal::not_running;
        state_ = State::barrier;
        barrier_ = id;
        return Refusal::ok;
    }

    /// T81-analog: consume the exact barrier, clear all values, advance
    /// to running(E+1). Exactly-once: the state change consumes the
    /// barrier, so a repeat is wrong_barrier/not_running by construction.
    //@@authority transition=provider_lab.clear_and_advance_epoch owner=provider_lab commit=strand
    Refusal
    clear_and_advance(BarrierId id)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        if (state_ == State::stopped)
            return Refusal::stopped;
        if (state_ != State::barrier)
            return Refusal::not_running;
        if (barrier_ != id)
            return Refusal::wrong_barrier;
        values_.clear();
        lru_.clear();
        ++epoch_;
        state_ = State::running;
        return Refusal::ok;
    }

    /// T82-analog: absorbing stop; emits the witness exactly once.
    //@@authority transition=provider_lab.stop_store owner=provider_lab commit=strand
    std::optional<StopWitness>
    stop()
    {
        if (!on_owner_strand() || state_ == State::stopped)
            return std::nullopt;
        state_ = State::stopped;
        return StopWitness{
            epoch_, values_.size(), total_inserts_, total_evictions_};
    }

    // Owner-strand introspection: the strand check dominates every
    // read; off-strand callers get refusal, never a stale scalar.
    std::optional<Epoch>
    accepted_epoch() const
    {
        if (!on_owner_strand())
            return std::nullopt;
        return epoch_;
    }
    std::size_t
    size() const
    {
        return on_owner_strand() ? values_.size() : 0;
    }

private:
    enum class State : std::uint8_t { running, barrier, stopped };

    struct Value
    {
        std::string bytes;
        std::list<ContentHash>::iterator lru_pos;
    };

    Refusal
    check_running(Epoch tag) const
    {
        if (state_ == State::stopped)
            return Refusal::stopped;
        if (state_ == State::barrier)
            return Refusal::barrier_installed;
        if (tag != epoch_)
            return Refusal::wrong_epoch;
        return Refusal::ok;
    }

    void
    touch(std::map<ContentHash, Value>::iterator it)
    {
        lru_.erase(it->second.lru_pos);
        lru_.push_front(it->first);
        it->second.lru_pos = lru_.begin();
    }

    strand_type strand_;
    //@@authority state=provider_lab.store_control owner=provider_lab
    State state_ = State::running;
    Epoch epoch_;
    BarrierId barrier_ = 0;
    std::size_t capacity_;
    //@@authority state=provider_lab.object_store_values owner=provider_lab
    std::map<ContentHash, Value> values_;
    std::list<ContentHash> lru_;
    std::uint64_t total_inserts_ = 0;
    std::uint64_t total_evictions_ = 0;
};

}  // namespace catl::provider_lab
