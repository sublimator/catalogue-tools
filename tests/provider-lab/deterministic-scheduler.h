#pragma once

// Deterministic scheduler harness for target-stack interleaving tests.
//
// This is the first package of the ledger-data-provider decomposition's
// pre-cutover verification net ("build the deterministic scheduler,
// sticky cancellation cells, ..."). It is TEST-ONLY infrastructure:
// production construction must never reference this directory, and the
// eventual link/call-graph CI check enforces that.
//
// Determinism model: all work runs on ONE thread through an io_context
// the test steps explicitly. Authority commits in target components are
// strand-confined synchronous sections, so the reachable interleaving
// space is exactly the handler execution order — which step() /
// run_until_idle() / trigger ordering make test-controlled. Time is
// virtual: timers fire only when the test advances the clock, so no
// wall-clock race can make an interleaving test flaky.

#include <boost/asio/any_completion_handler.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>
#include <boost/system/error_code.hpp>

#include <chrono>
#include <cstddef>
#include <map>
#include <utility>
#include <vector>

namespace catl::provider_lab {

namespace asio = boost::asio;

class DeterministicScheduler
{
public:
    using clock = std::chrono::steady_clock;
    using time_point = clock::time_point;
    using duration = clock::duration;
    using strand_type = asio::strand<asio::io_context::executor_type>;

    DeterministicScheduler() : guard_(asio::make_work_guard(io_)) {}

    asio::io_context&
    io()
    {
        return io_;
    }

    strand_type
    make_strand()
    {
        return asio::make_strand(io_);
    }

    /// Run exactly one ready handler. Returns false when none is ready.
    bool
    step()
    {
        io_.restart();
        return io_.poll_one() > 0;
    }

    /// Drain every ready handler (virtual-time timers stay pending).
    /// Returns the number of handlers run.
    std::size_t
    run_until_idle()
    {
        io_.restart();
        return io_.poll();
    }

    /// Virtual now. Starts at the epoch; only advance() moves it.
    time_point
    now() const
    {
        return now_;
    }

    /// Advance virtual time with discrete-event semantics: time flows
    /// TO each due deadline in order (so a fired handler that re-arms
    /// relative to now() computes from its own fire time, and cascades
    /// land inside the window), handlers drain to idle after each fire,
    /// and now() finishes at the target.
    void
    advance(duration d)
    {
        // Virtual time is monotonic; a backward step would silently
        // strand pending timers. Refuse rather than wrap.
        if (d < duration::zero())
            return;
        if (now_ > time_point::max() - d)
            return;  // clamp: overflow would wrap virtual time backward
        auto const target = now_ + d;
        for (;;)
        {
            auto it = timers_.begin();
            if (it == timers_.end() || it->first > target)
                break;
            now_ = it->first;  // the event's own fire time
            auto handler = std::move(it->second);
            timers_.erase(it);
            // A live entry implies a live owner (the Timer's destructor
            // cancels); clearing armed_ BEFORE the handler runs lets a
            // fired handler re-arm its own timer.
            if (handler.owner)
                handler.owner->armed_ = false;
            asio::post(
                io_,
                [h = std::move(handler)]() mutable {
                    std::move(h)(boost::system::error_code{});
                });
            run_until_idle();
        }
        now_ = target;
    }

    /// A virtual timer. Target components take a scheduler reference and
    /// use Timer instead of asio::steady_timer; the awaitable interface
    /// matches the async_wait shape.
    class Timer
    {
    public:
        explicit Timer(DeterministicScheduler& sched) : sched_(sched) {}

        ~Timer()
        {
            cancel();
        }

        Timer(Timer const&) = delete;
        Timer&
        operator=(Timer const&) = delete;

        void
        expires_after(duration d)
        {
            cancel();
            deadline_ = sched_.now_ + d;
        }

        /// Cancel a pending wait; its handler completes with
        /// operation_aborted. Returns true if a wait was cancelled.
        bool
        cancel()
        {
            if (!armed_)
                return false;
            auto range = sched_.timers_.equal_range(armed_deadline_);
            for (auto it = range.first; it != range.second; ++it)
            {
                if (it->second.owner == this)
                {
                    auto handler = std::move(it->second);
                    sched_.timers_.erase(it);
                    armed_ = false;
                    asio::post(
                        sched_.io_,
                        [h = std::move(handler)]() mutable {
                            std::move(h)(asio::error::operation_aborted);
                        });
                    return true;
                }
            }
            armed_ = false;
            return false;
        }

        template <typename CompletionToken>
        auto
        async_wait(CompletionToken&& token)
        {
            return asio::async_initiate<
                CompletionToken,
                void(boost::system::error_code)>(
                [this](auto handler) {
                    if (armed_)
                    {
                        // Single-wait contract: a second concurrent wait
                        // would orphan the first entry on cancel. Refuse
                        // it loudly instead of mishandling it silently.
                        asio::post(
                            sched_.io_,
                            [h = std::move(handler)]() mutable {
                                std::move(h)(
                                    asio::error::operation_not_supported);
                            });
                        return;
                    }
                    armed_ = true;
                    armed_deadline_ = deadline_;
                    sched_.timers_.emplace(
                        deadline_,
                        Entry{
                            asio::any_completion_handler<
                                void(boost::system::error_code)>(
                                std::move(handler)),
                            this});
                },
                token);
        }

    private:
        friend class DeterministicScheduler;
        DeterministicScheduler& sched_;
        time_point deadline_{};
        time_point armed_deadline_{};
        bool armed_ = false;
    };

private:
    struct Entry
    {
        asio::any_completion_handler<void(boost::system::error_code)>
            handler;
        Timer* owner = nullptr;

        void
        operator()(boost::system::error_code ec) &&
        {
            std::move(handler)(ec);
        }
    };

    asio::io_context io_;
    asio::executor_work_guard<asio::io_context::executor_type> guard_;
    time_point now_{};
    std::multimap<time_point, Entry> timers_;
};

/// A manual latch for structuring forced interleavings: a coroutine (or
/// callback chain) parks on wait() until the test opens the gate. Open
/// is sticky — waits after open complete immediately, in post order.
///
/// PRECONDITION (local to this type, not just the scheduler): Gate has
/// NO runtime confinement — open()/async_wait() must only ever run on
/// the single test thread driving the io_context. It is a test-harness
/// tool and must not be copied into production as a general async
/// primitive; the link-graph boundary keeps it test-only.
class Gate
{
public:
    explicit Gate(asio::io_context& io) : io_(io) {}

    bool
    is_open() const
    {
        return open_;
    }

    void
    open()
    {
        if (open_)
            return;
        open_ = true;
        auto waiters = std::move(waiters_);
        waiters_.clear();
        for (auto& w : waiters)
        {
            asio::post(io_, [h = std::move(w)]() mutable {
                std::move(h)(boost::system::error_code{});
            });
        }
    }

    template <typename CompletionToken>
    auto
    async_wait(CompletionToken&& token)
    {
        return asio::async_initiate<
            CompletionToken,
            void(boost::system::error_code)>(
            [this](auto handler) {
                if (open_)
                {
                    asio::post(io_, [h = std::move(handler)]() mutable {
                        std::move(h)(boost::system::error_code{});
                    });
                    return;
                }
                waiters_.emplace_back(std::move(handler));
            },
            token);
    }

private:
    asio::io_context& io_;
    bool open_ = false;
    std::vector<asio::any_completion_handler<void(boost::system::error_code)>>
        waiters_;
};

/// Sticky, owner-strand-confined cancellation cell. Semantic
/// cancellation is owned state, not an asio mechanism: request() is
/// set-once and level-triggered, observers poll requested() at commit
/// points or park on async_wait(), and a wait that begins after the
/// request completes immediately. Off-strand touches are refused.
//@@authority state=provider_lab.sticky_cancel_cell owner=provider_lab
class StickyCancelCell
{
public:
    using strand_type = DeterministicScheduler::strand_type;

    explicit StickyCancelCell(strand_type strand)
        : strand_(std::move(strand))
    {
    }

    /// True only while running on the owning strand.
    bool
    on_owner_strand() const
    {
        return strand_.running_in_this_thread();
    }

    bool
    requested() const
    {
        return on_owner_strand() && requested_;
    }

    /// Set-once. Returns false (and has no effect) off-strand; the
    /// second and later requests are no-ops returning true.
    //@@authority transition=provider_lab.sticky_cancel_request owner=provider_lab commit=strand
    bool
    request()
    {
        if (!on_owner_strand())
            return false;
        if (!requested_)
        {
            requested_ = true;
            auto waiters = std::move(waiters_);
            waiters_.clear();
            for (auto& w : waiters)
            {
                asio::post(strand_, [h = std::move(w)]() mutable {
                    std::move(h)(boost::system::error_code{});
                });
            }
        }
        return true;
    }

    template <typename CompletionToken>
    auto
    async_wait(CompletionToken&& token)
    {
        return asio::async_initiate<
            CompletionToken,
            void(boost::system::error_code)>(
            [this](auto handler) {
                asio::post(
                    strand_,
                    [this, h = std::move(handler)]() mutable {
                        if (requested_)
                        {
                            std::move(h)(boost::system::error_code{});
                            return;
                        }
                        waiters_.emplace_back(std::move(h));
                    });
            },
            token);
    }

private:
    strand_type strand_;
    bool requested_ = false;
    std::vector<asio::any_completion_handler<void(boost::system::error_code)>>
        waiters_;
};

}  // namespace catl::provider_lab
