#pragma once

// Shared deterministic scheduler support for executor-local asynchronous
// tests.
//
// Determinism model: all work runs on one thread through an io_context which
// the test steps explicitly. Time is virtual only for Timer; production
// steady_timer instances and kernel I/O remain outside this scheduler.
//
// This is test-only infrastructure. Production construction must never
// reference tests/test-support.

#include <boost/asio/any_completion_handler.hpp>
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

namespace catl::test {

namespace asio = boost::asio;

class DeterministicScheduler
{
public:
    using clock = std::chrono::steady_clock;
    using time_point = clock::time_point;
    using duration = clock::duration;
    using strand_type = asio::strand<asio::io_context::executor_type>;

    DeterministicScheduler() : guard_(asio::make_work_guard(io_))
    {
    }

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

    /// Advance virtual time with discrete-event semantics: time flows to each
    /// due deadline in order, ready handlers drain after every fire, and now()
    /// finishes at the target.
    void
    advance(duration d)
    {
        if (d < duration::zero())
            return;
        if (now_ > time_point::max() - d)
            return;

        auto const target = now_ + d;
        for (;;)
        {
            auto it = timers_.begin();
            if (it == timers_.end() || it->first > target)
                break;

            now_ = it->first;
            auto handler = std::move(it->second);
            timers_.erase(it);
            if (handler.owner)
                handler.owner->armed_ = false;
            asio::post(io_, [h = std::move(handler)]() mutable {
                std::move(h)(boost::system::error_code{});
            });
            run_until_idle();
        }
        now_ = target;
    }

    class Timer
    {
    public:
        explicit Timer(DeterministicScheduler& scheduler)
            : scheduler_(scheduler)
        {
        }

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
            if (d < duration::zero())
                d = duration::zero();
            deadline_ = (scheduler_.now_ > time_point::max() - d)
                ? time_point::max()
                : scheduler_.now_ + d;
        }

        /// Cancel a pending wait and complete it with operation_aborted.
        bool
        cancel()
        {
            if (!armed_)
                return false;
            auto range = scheduler_.timers_.equal_range(armed_deadline_);
            for (auto it = range.first; it != range.second; ++it)
            {
                if (it->second.owner == this)
                {
                    auto handler = std::move(it->second);
                    scheduler_.timers_.erase(it);
                    armed_ = false;
                    asio::post(
                        scheduler_.io_, [h = std::move(handler)]() mutable {
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
                        asio::post(
                            scheduler_.io_, [h = std::move(handler)]() mutable {
                                std::move(h)(
                                    asio::error::operation_not_supported);
                            });
                        return;
                    }
                    armed_ = true;
                    armed_deadline_ = deadline_;
                    scheduler_.timers_.emplace(
                        deadline_,
                        Entry{
                            asio::any_completion_handler<void(
                                boost::system::error_code)>(std::move(handler)),
                            this});
                },
                token);
        }

    private:
        friend class DeterministicScheduler;
        DeterministicScheduler& scheduler_;
        time_point deadline_{};
        time_point armed_deadline_{};
        bool armed_ = false;
    };

private:
    struct Entry
    {
        asio::any_completion_handler<void(boost::system::error_code)> handler;
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

/// A sticky manual latch for forced interleavings.
///
/// Gate has no runtime confinement. open() and async_wait() must run on the
/// one test thread which drives the scheduler's io_context.
class Gate
{
public:
    explicit Gate(asio::io_context& io) : io_(io)
    {
    }

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
        for (auto& waiter : waiters)
        {
            asio::post(io_, [handler = std::move(waiter)]() mutable {
                std::move(handler)(boost::system::error_code{});
            });
        }
    }

    template <typename CompletionToken>
    auto
    async_wait(CompletionToken&& token)
    {
        return asio::
            async_initiate<CompletionToken, void(boost::system::error_code)>(
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

}  // namespace catl::test
