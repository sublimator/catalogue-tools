#pragma once

// Epoch-guarded header store — target-stack package 4, increment 2
// (ledger-data-provider decomposition v4, "Store ownership and epochs").
// TEST-ONLY: production construction must never reference this.
//
// The contract, from the design body (S60/S62):
//  - headers are keyed by (sequence, computed header hash) with an
//    immutable secondary sequence index — multiple CANDIDATE headers
//    may coexist for one sequence, and candidate SELECTION is outside
//    the store: it returns candidates and never picks one, so nothing
//    here can bypass ExpectedHeaderRef;
//  - insertion verifies canonical encoding/hash agreement AND
//    internal/claimed sequence agreement before any mutation;
//  - values are immutable; re-insert is idempotent; LRU eviction is
//    owner-local, capacity-bounded, distinct from clear, and keeps the
//    secondary index exactly consistent;
//  - the epoch/barrier/stop machine is the shared EpochControl — one
//    implementation for both stores, not two spellings.

#include "deterministic-scheduler.h"
#include "epoch-control.h"
#include "object-store.h"  // lab_content_hash, ContentHash

#include <cstdint>
#include <limits>
#include <list>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace catl::provider_lab {

using Sequence = std::uint32_t;

/// Lab stand-in for canonical header encoding: "<seq>:<content>". The
/// internal-sequence CHECK is the point; production parses the real
/// canonical header.
inline std::optional<Sequence>
lab_header_sequence(std::string const& bytes)
{
    auto const colon = bytes.find(':');
    if (colon == std::string::npos || colon == 0)
        return std::nullopt;
    Sequence seq = 0;
    for (char const ch : bytes.substr(0, colon))
    {
        if (ch < '0' || ch > '9')
            return std::nullopt;
        auto const digit = static_cast<Sequence>(ch - '0');
        // Refuse overflow rather than wrap: a wrapped parse could
        // FALSELY AGREE with a claimed sequence equal to the wrapped
        // value, defeating the very check this function exists for.
        if (seq > (std::numeric_limits<Sequence>::max() - digit) / 10)
            return std::nullopt;
        seq = seq * 10 + digit;
    }
    return seq;
}

class EpochGuardedHeaderStore
{
public:
    using strand_type = DeterministicScheduler::strand_type;
    using Key = std::pair<Sequence, ContentHash>;

    enum class Refusal : std::uint8_t {
        ok,
        off_strand,
        wrong_epoch,
        barrier_installed,
        hash_mismatch,      // bytes do not hash to the claimed key
        sequence_mismatch,  // internal sequence disagrees with the claim
        wrong_barrier,
        not_running,
        stopped,
    };

    struct StopWitness
    {
        Epoch final_epoch = 0;
        std::size_t values_at_stop = 0;
        std::uint64_t total_inserts = 0;
        std::uint64_t total_evictions = 0;
    };

    EpochGuardedHeaderStore(
        strand_type strand,
        Epoch initial_epoch,
        std::size_t capacity)
        : strand_(std::move(strand))
        , control_(initial_epoch)
        , capacity_(capacity == 0 ? 1 : capacity)
    {
    }

    bool
    on_owner_strand() const
    {
        return strand_.running_in_this_thread();
    }

    /// T85-analog: epoch-checked immutable insert. Verifies the hash
    /// AND the internal/claimed sequence agreement before any mutation.
    //@@authority transition=provider_lab.insert_header owner=provider_lab commit=strand
    Refusal
    insert(Epoch tag, Sequence seq, ContentHash hash, std::string bytes)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        if (auto r = admit(tag); r != Refusal::ok)
            return r;
        if (lab_content_hash(bytes) != hash)
            return Refusal::hash_mismatch;
        auto const internal = lab_header_sequence(bytes);
        if (!internal.has_value() || *internal != seq)
            return Refusal::sequence_mismatch;
        Key const key{seq, hash};
        if (auto it = values_.find(key); it != values_.end())
        {
            touch(it);
            return Refusal::ok;  // immutable: idempotent re-insert
        }
        while (values_.size() >= capacity_ && !lru_.empty())
        {
            // T88-analog eviction: owner-local, and the secondary
            // index stays exactly consistent with the value map.
            auto victim = lru_.back();
            lru_.pop_back();
            values_.erase(victim);
            unindex(victim);
            ++total_evictions_;
        }
        lru_.push_front(key);
        values_.emplace(key, Value{std::move(bytes), lru_.begin()});
        sequence_index_[seq].insert(hash);
        ++total_inserts_;
        return Refusal::ok;
    }

    /// Epoch-checked exact lookup by (sequence, hash).
    std::pair<Refusal, std::optional<std::string>>
    lookup(Epoch tag, Sequence seq, ContentHash hash)
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, std::nullopt};
        if (auto r = admit(tag); r != Refusal::ok)
            return {r, std::nullopt};
        auto it = values_.find(Key{seq, hash});
        if (it == values_.end())
            return {Refusal::ok, std::nullopt};
        touch(it);
        return {Refusal::ok, it->second.bytes};
    }

    /// Candidate enumeration via the immutable secondary index. The
    /// store returns EVERY candidate hash for the sequence and never
    /// selects one: candidate selection is outside the store and
    /// cannot bypass ExpectedHeaderRef.
    std::pair<Refusal, std::vector<ContentHash>>
    candidates(Epoch tag, Sequence seq)
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, {}};
        if (auto r = admit(tag); r != Refusal::ok)
            return {r, {}};
        auto it = sequence_index_.find(seq);
        if (it == sequence_index_.end())
            return {Refusal::ok, {}};
        return {
            Refusal::ok,
            std::vector<ContentHash>(it->second.begin(), it->second.end())};
    }

    //@@authority transition=provider_lab.install_header_epoch_barrier owner=provider_lab commit=strand
    Refusal
    install_barrier(BarrierId id)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        return map_check(control_.install_barrier(id));
    }

    //@@authority transition=provider_lab.clear_and_advance_header_epoch owner=provider_lab commit=strand
    Refusal
    clear_and_advance(BarrierId id)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        auto const check = control_.consume_barrier_and_advance(id);
        if (check == EpochControl::Check::ok)
        {
            values_.clear();
            lru_.clear();
            sequence_index_.clear();
        }
        return map_check(check);
    }

    //@@authority transition=provider_lab.stop_header_store owner=provider_lab commit=strand
    std::optional<StopWitness>
    stop()
    {
        if (!on_owner_strand() || !control_.stop())
            return std::nullopt;
        return StopWitness{
            control_.epoch(),
            values_.size(),
            total_inserts_,
            total_evictions_};
    }

    std::optional<Epoch>
    accepted_epoch() const
    {
        if (!on_owner_strand())
            return std::nullopt;
        return control_.epoch();
    }
    std::optional<std::size_t>
    size() const
    {
        if (!on_owner_strand())
            return std::nullopt;
        return values_.size();
    }

private:
    struct Value
    {
        std::string bytes;
        std::list<Key>::iterator lru_pos;
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
        }
        return Refusal::not_running;
    }

    void
    touch(std::map<Key, Value>::iterator it)
    {
        lru_.erase(it->second.lru_pos);
        lru_.push_front(it->first);
        it->second.lru_pos = lru_.begin();
    }

    void
    unindex(Key const& key)
    {
        auto it = sequence_index_.find(key.first);
        if (it == sequence_index_.end())
            return;
        it->second.erase(key.second);
        if (it->second.empty())
            sequence_index_.erase(it);
    }

    strand_type strand_;
    //@@authority state=provider_lab.header_store_control owner=provider_lab
    EpochControl control_;
    std::size_t capacity_;
    //@@authority state=provider_lab.header_store_values owner=provider_lab
    std::map<Key, Value> values_;
    std::list<Key> lru_;
    std::map<Sequence, std::set<ContentHash>> sequence_index_;
    std::uint64_t total_inserts_ = 0;
    std::uint64_t total_evictions_ = 0;
};

}  // namespace catl::provider_lab
