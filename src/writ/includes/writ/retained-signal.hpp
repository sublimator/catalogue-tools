#pragma once

// writ::retained_signal — a retained one-shot wake with per-subscriber
// deadlines and typed outcomes.
// schema: writ.core/retained-signal/v1
//
// Extracted from the peer-client's HistoryWaitSignal, which replaced a
// parallel_group signal/deadline race after that composition's
// executor-ambiguous boundary and loser-cancellation protocol proved
// unnecessary: when two branches are merely two dispositions of one owned
// wait, encode them in one operation.
//
// The design encodes four lessons, each of which has a named counterexample
// in the conformance corpus:
//
//   retained completion   timer.cancel() is an edge delivered to
//                         already-registered waits, not retained state. A
//                         wake published before the waiter registers is lost
//                         on every platform. complete() therefore retains a
//                         completion fact; a wait that arrives later observes
//                         it without suspending.
//
//   per-subscriber        each waiter owns its deadline timer, so one
//   deadlines             subscriber's timeout retires only that subscriber
//                         and can neither consume the retained wake nor
//                         poison a sibling.
//
//   typed outcomes        signaled, deadline_elapsed, and canceled are
//                         distinct dispositions. Consumers must handle all
//                         three; an operation_aborted error is never evidence
//                         by itself of WHY the waiter woke.
//
//   cancellation          cancellation is sampled at the subscriber boundary
//   dominates             and dominates a concurrently queued shared wake for
//                         THAT subscriber; siblings still consume the shared
//                         wake normally. A canceled subscriber cannot be
//                         resurrected by a later complete().
//
// All signal state is confined to the home. The owner completes on the home
// (complete()) or from anywhere via post_complete(). Waiters either run on
// the home already (wait_until, the drive-loop shape) or cross through the
// foreign-caller gateway (co_wait_until), which births the complete wait on
// the home via co_spawn.
//
// A completed signal stays completed: for repeated wake generations, the
// owner rotates — create a fresh signal, then complete() the old one, so a
// subscriber arming against the current generation can never miss a wake
// (the peer-client's wake_waiters_without_revision pattern).

#include <writ/home.hpp>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/system/error_code.hpp>

#include <chrono>
#include <cstdint>
#include <memory>
#include <vector>

namespace writ {

enum class wait_outcome : std::uint8_t {
    signaled,
    deadline_elapsed,
    canceled,
};

class retained_signal : public std::enable_shared_from_this<retained_signal>
{
public:
    using executor_type = home::executor_type;
    using time_point = std::chrono::steady_clock::time_point;

    explicit retained_signal(home h);

    home const&
    signal_home() const noexcept
    {
        return home_;
    }

    /// Owner-side, on the home: retain the completion fact and wake every
    /// registered subscriber by cancelling its deadline timer. Idempotent;
    /// the no-throw commit cannot be observed half-applied on the home.
    void
    complete() noexcept;

    /// Owner-side, from any thread: post the completion onto the home. The
    /// signal must be owned by shared_ptr. A failed post is swallowed: wakes
    /// are advisory projections of already-committed state, and every
    /// subscriber's own deadline is the fail-closed fallback.
    void
    post_complete() noexcept;

    /// On the home only. Diagnostic; the retained fact is consumed through
    /// register_wait/consume_wait, not by polling this.
    bool
    completed() const noexcept;

    // ── low-level on-home protocol (the drive-loop shape) ────────────
    //
    // Arm before observing: register_wait BEFORE sampling the condition the
    // wait stands for, so a publication between the sample and the suspend
    // cannot be lost.

    /// Register one subscriber with its own deadline. Returns nullptr when
    /// the signal has already completed — the retained fact, consumed without
    /// suspending. On the home only.
    [[nodiscard]] std::shared_ptr<boost::asio::steady_timer>
    register_wait(time_point deadline);

    /// Classify one woken subscriber and retire its registration. On the
    /// home only. Cancellation dominates; then the retained completion; then
    /// a clean expiry is the deadline; any other error is cancellation.
    wait_outcome
    consume_wait(
        std::shared_ptr<boost::asio::steady_timer> const& waiter,
        boost::system::error_code const& error,
        boost::asio::cancellation_type_t cancellation) noexcept;

    // ── packaged waits ───────────────────────────────────────────────

    /// The complete on-home wait: register, suspend, classify. The coroutine
    /// frame must be pumped on the home (typed awaitable); use the gateway
    /// below from foreign executors.
    boost::asio::awaitable<wait_outcome, executor_type>
    wait_until(time_point deadline);

    /// Foreign-caller gateway: births the complete wait on the home. The
    /// caller's continuation resumes on the CALLER's executor afterward.
    boost::asio::awaitable<wait_outcome>
    co_wait_until(time_point deadline);

private:
    home home_;
    std::vector<std::weak_ptr<boost::asio::steady_timer>> waiters_;
    bool completed_ = false;
};

}  // namespace writ
