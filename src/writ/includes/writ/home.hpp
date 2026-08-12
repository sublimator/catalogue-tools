#pragma once

// writ::home — an execution authority.
//
// A home is the strand on which one owner's state transitions are serialized.
// It represents authority to EXECUTE there; it does not own any state object
// (state ownership is a later, separate noun).
//
// Two claims a home supports, kept deliberately distinct:
//
//   birth        home.spawn(f) births the COMPLETE child coroutine on this
//                home. Asio's co_spawn entry point dispatches through the
//                spawn executor before the child body runs, and every
//                use_awaitable completion of an I/O object born on this home
//                re-enters through the home's serialized boundary (the
//                completion handler's associated executor is the awaitable
//                thread's stored executor — the home).
//
//   continuation a parent that co_awaits home.spawn(f) re-enters through ITS
//                OWN executor after the child completes. Birth is not
//                relocation. Note the claim is positive only: the completion
//                dispatch may run inline, transiently NESTING the parent's
//                boundary inside this home's still-active handler, so
//                running_here() can be true for BOTH homes at that instant.
//                running_here() is containment, not exclusivity — assert the
//                boundary you require, never the absence of another.
//
// The probe running_here() is a diagnostic tripwire, NOT the proof.
// Correctness comes from structural birth/association above.

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <chrono>
#include <memory>
#include <utility>

namespace writ {

class home
{
public:
    using executor_type =
        boost::asio::strand<boost::asio::io_context::executor_type>;

    explicit home(boost::asio::io_context& io)
        : executor_(boost::asio::make_strand(io))
    {
    }

    explicit home(executor_type executor) : executor_(std::move(executor))
    {
    }

    executor_type const&
    executor() const noexcept
    {
        return executor_;
    }

    /// Whether the calling thread is currently inside this home's serialized
    /// boundary. Out-of-line and never inlined: the underlying check reads a
    /// thread_local, and an inlined thread-local read inside a large coroutine
    /// can be hoisted across suspension points by a defective optimizer, after
    /// which a cross-thread resume walks the PREVIOUS thread's state
    /// (LLVM #63022; fixed in clang 17 by 84c033d9). A probe must never be
    /// inlined into the frame it audits.
    bool
    running_here() const noexcept;

    /// Diagnostic tripwire for owner-state touch points. Returns true when on
    /// the home. Off-home it logs, returns false, and aborts only when the
    /// WRIT_HOME_FAIL_FAST environment variable is set — the same fail-fast
    /// discipline as the peer-client strand asserts, with the probe hardened
    /// as above.
    bool
    assert_here(char const* what, char const* file, int line) const noexcept;

    /// Birth gateway: the complete child coroutine is born on this home. The
    /// returned awaitable is co_awaited by the caller; the caller's own
    /// continuation resumes on the CALLER's executor afterward (see header
    /// comment — birth is not relocation). Cancellation from the awaiting
    /// parent propagates into the child through Asio's co_spawn machinery and
    /// is emitted on this home.
    template <typename F>
    [[nodiscard]] auto
    spawn(F&& f) const
    {
        return boost::asio::co_spawn(
            executor_, std::forward<F>(f), boost::asio::use_awaitable);
    }

    /// A timer born on this home: its use_awaitable completions re-enter the
    /// home's serialized boundary, including completion by cancel().
    [[nodiscard]] std::shared_ptr<boost::asio::steady_timer>
    timer(std::chrono::steady_clock::time_point expiry) const
    {
        return std::make_shared<boost::asio::steady_timer>(executor_, expiry);
    }

    friend bool
    operator==(home const& a, home const& b) noexcept
    {
        return a.executor_ == b.executor_;
    }

    friend bool
    operator!=(home const& a, home const& b) noexcept
    {
        return !(a == b);
    }

private:
    executor_type executor_;
};

}  // namespace writ

/// Owner-state touch-point assert. Expands to a call through the out-of-line
/// probe so it stays valid inside coroutines on every toolchain.
#define WRIT_ASSERT_HOME(home_ref) \
    (home_ref).assert_here(__func__, __FILE__, __LINE__)
