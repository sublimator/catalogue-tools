#pragma once

#include "peer-session.h"

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <utility>

namespace catl::peer_client {

enum class PeerLifecycle : std::uint8_t {
    created,
    connecting,
    ready,
    closing,
    closed,
    failed,
};

struct PeerStatusSnapshot
{
    std::uint32_t current_seq = 0;
    AdvertisedLedgerRange advertised_range;
    std::map<std::string, std::string> headers;
    std::optional<std::string> authenticated_node_key;

    friend bool
    operator==(PeerStatusSnapshot const&, PeerStatusSnapshot const&) = default;
};

struct PeerLifecycleEvent
{
    std::string endpoint;
    std::uint64_t generation = 0;
    std::uint64_t event_sequence = 0;
    PeerLifecycle lifecycle = PeerLifecycle::created;
    PeerStatusSnapshot status;
    PeerTerminalReason reason;
};

enum class PeerLifecycleApply : std::uint8_t {
    accepted,
    wrong_identity,
    stale_sequence,
    illegal_transition,
};

/**
 * Value-only lifecycle state for one exact peer generation.
 *
 * The transport publishes immutable events. PeerSet owns this state machine
 * and is the only place an event becomes selectable live evidence. Local close
 * is sticky; a delayed transport event cannot resurrect a generation that the
 * owner has begun retiring.
 */
class PeerGenerationLifecycle
{
public:
    PeerGenerationLifecycle() = default;

    PeerGenerationLifecycle(std::string endpoint, std::uint64_t generation)
        : endpoint_(std::move(endpoint)), generation_(generation)
    {
    }

    PeerLifecycleApply
    apply(PeerLifecycleEvent const& event)
    {
        if (event.endpoint != endpoint_ || event.generation != generation_ ||
            event.generation == 0)
        {
            return PeerLifecycleApply::wrong_identity;
        }
        if (event.event_sequence == 0 ||
            event.event_sequence <= last_event_sequence_)
        {
            return PeerLifecycleApply::stale_sequence;
        }
        if (!allows(event.lifecycle))
            return PeerLifecycleApply::illegal_transition;

        // Prepare the only allocating projection before committing identity,
        // sequence, or lifecycle. An allocation failure therefore leaves the
        // exact generation unchanged and eligible for a later event.
        std::optional<PeerStatusSnapshot> next_status;
        if (event.lifecycle == PeerLifecycle::ready)
            next_status = event.status;

        //@@authority transition=prv.T0238 status=provisional scope=statement
        //bind=expression_statement@948d7e4f1d32dcfc
        // owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
        // touches=prv.S0085,prv.S0086,prv.S0087
        // label=commit_generation_scoped_monotonic_lifecycle_event
        last_event_sequence_ =
            event.event_sequence;  //@@authority-target ids=prv.T0238
        lifecycle_ = event.lifecycle;
        if (event.lifecycle == PeerLifecycle::ready)
            status_ = std::move(next_status);
        else if (
            is_terminal(event.lifecycle) ||
            event.lifecycle == PeerLifecycle::closing)
            status_.reset();
        return PeerLifecycleApply::accepted;
    }

    bool
    begin_local_close() noexcept
    {
        if (is_terminal(lifecycle_) || lifecycle_ == PeerLifecycle::closing)
            return false;
        //@@authority transition=prv.T0239 status=provisional scope=statement
        //bind=expression_statement@52f61cc3fa47c829
        // owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
        // touches=prv.S0086,prv.S0087
        // label=invalidate_generation_status_before_local_close
        lifecycle_ = PeerLifecycle::closing;  //@@authority-target ids=prv.T0239
        status_.reset();
        return true;
    }

    [[nodiscard]] bool
    ready() const noexcept
    {
        return lifecycle_ == PeerLifecycle::ready && status_.has_value();
    }

    [[nodiscard]] bool
    terminal() const noexcept
    {
        return is_terminal(lifecycle_);
    }

    [[nodiscard]] PeerLifecycle
    lifecycle() const noexcept
    {
        return lifecycle_;
    }

    [[nodiscard]] std::uint64_t
    generation() const noexcept
    {
        return generation_;
    }

    [[nodiscard]] std::uint64_t
    last_event_sequence() const noexcept
    {
        return last_event_sequence_;
    }

    [[nodiscard]] std::optional<PeerStatusSnapshot> const&
    status() const noexcept
    {
        return status_;
    }

private:
    static bool
    is_terminal(PeerLifecycle lifecycle) noexcept
    {
        return lifecycle == PeerLifecycle::closed ||
            lifecycle == PeerLifecycle::failed;
    }

    bool
    allows(PeerLifecycle next) const noexcept
    {
        if (is_terminal(lifecycle_))
            return false;
        switch (lifecycle_)
        {
            case PeerLifecycle::created:
                return next == PeerLifecycle::connecting ||
                    next == PeerLifecycle::closed ||
                    next == PeerLifecycle::failed;
            case PeerLifecycle::connecting:
                return next == PeerLifecycle::connecting ||
                    next == PeerLifecycle::ready ||
                    next == PeerLifecycle::closing ||
                    next == PeerLifecycle::closed ||
                    next == PeerLifecycle::failed;
            case PeerLifecycle::ready:
                return next == PeerLifecycle::ready ||
                    next == PeerLifecycle::closing ||
                    next == PeerLifecycle::closed ||
                    next == PeerLifecycle::failed;
            case PeerLifecycle::closing:
                return next == PeerLifecycle::closing ||
                    next == PeerLifecycle::closed ||
                    next == PeerLifecycle::failed;
            case PeerLifecycle::closed:
            case PeerLifecycle::failed:
                return false;
        }
        return false;
    }

    std::string endpoint_;
    std::uint64_t generation_ = 0;
    //@@authority state=prv.S0085 status=provisional scope=field
    //bind=catl::peer_client::PeerGenerationLifecycle::last_event_sequence_@4a0d415414244c9e
    // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
    // label=last_accepted_peer_generation_lifecycle_event_sequence
    std::uint64_t last_event_sequence_ = 0;
    //@@authority state=prv.S0086 status=provisional scope=field
    //bind=catl::peer_client::PeerGenerationLifecycle::lifecycle_@dfa5e41fedef3d68
    // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
    // label=canonical_peer_generation_lifecycle
    PeerLifecycle lifecycle_ = PeerLifecycle::created;
    //@@authority state=prv.S0087 status=provisional scope=field
    //bind=catl::peer_client::PeerGenerationLifecycle::status_@88b36fb33216936e
    // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
    // label=ready_only_peer_generation_status_snapshot
    std::optional<PeerStatusSnapshot> status_;
};

}  // namespace catl::peer_client
