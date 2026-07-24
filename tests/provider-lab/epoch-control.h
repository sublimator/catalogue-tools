#pragma once

// Shared S60-analog store control machine: running(E) -> barrier(E, id)
// -> running(E+1), with an absorbing stop. Extracted so the object and
// header stores share ONE implementation of the barrier protocol rather
// than two spellings of it. TEST-ONLY, like everything in this lab.

#include <cstdint>
#include <limits>
#include <optional>

namespace catl::provider_lab {

using Epoch = std::uint64_t;
using BarrierId = std::uint64_t;

class EpochControl
{
public:
    enum class State : std::uint8_t { running, barrier, stopped };

    enum class Check : std::uint8_t {
        ok,
        wrong_epoch,
        barrier_installed,
        stopped,
        wrong_barrier,
        not_running,
        epoch_exhausted,  // advance would wrap: refuse, fail closed
        barrier_reused,   // BarrierIds are a strictly increasing mint
    };

    explicit EpochControl(Epoch initial_epoch) : epoch_(initial_epoch) {}

    /// Epoch-tagged command admission: the owner commit's recheck.
    Check
    check_running(Epoch tag) const
    {
        if (state_ == State::stopped)
            return Check::stopped;
        if (state_ == State::barrier)
            return Check::barrier_installed;
        if (tag != epoch_)
            return Check::wrong_epoch;
        return Check::ok;
    }

    /// T80-analog: install the barrier; old-epoch commands reject after.
    /// BarrierIds must be strictly increasing: the exact-barrier claim
    /// is only as strong as the id's non-reuse. The controller is a
    /// MONOTONIC ACCEPTANCE GATE, not a mint — the caller supplies the
    /// id and this gate refuses reuse/regression via last_barrier_
    /// (ordering authority: single-producer sequence). Zero is
    /// implicitly reserved: last_barrier_ starts at zero, so id 0 is
    /// never acceptable and stays outside the id vocabulary.
    Check
    install_barrier(BarrierId id)
    {
        if (state_ == State::stopped)
            return Check::stopped;
        if (state_ != State::running)
            return Check::not_running;
        if (id <= last_barrier_)
            return Check::barrier_reused;
        state_ = State::barrier;
        barrier_ = id;
        last_barrier_ = id;
        return Check::ok;
    }

    /// T81-analog: consume the exact barrier and advance. The state
    /// change consumes the barrier, so the acknowledgement is
    /// exactly-once by construction. The CALLER clears its values in
    /// the same owner-strand commit on Check::ok.
    Check
    consume_barrier_and_advance(BarrierId id)
    {
        if (state_ == State::stopped)
            return Check::stopped;
        if (state_ != State::barrier)
            return Check::not_running;
        if (barrier_ != id)
            return Check::wrong_barrier;
        if (epoch_ == std::numeric_limits<Epoch>::max())
        {
            // Advancing would wrap to epoch zero and reopen every
            // ancient zero-tagged command — the same false-agreement
            // class as a wrapped sequence parse, refused the same way.
            // The barrier stays installed: fail closed, values intact.
            return Check::epoch_exhausted;
        }
        ++epoch_;
        state_ = State::running;
        return Check::ok;
    }

    /// T82-analog: absorbing. Returns false if already stopped, so the
    /// caller can emit its witness exactly once.
    bool
    stop()
    {
        if (state_ == State::stopped)
            return false;
        state_ = State::stopped;
        return true;
    }

    Epoch
    epoch() const
    {
        return epoch_;
    }
    State
    state() const
    {
        return state_;
    }

private:
    State state_ = State::running;
    Epoch epoch_;
    BarrierId barrier_ = 0;
    BarrierId last_barrier_ = 0;
};

}  // namespace catl::provider_lab
