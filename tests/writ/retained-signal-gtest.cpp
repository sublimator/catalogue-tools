#include <writ/home.hpp>
#include <writ/retained-signal.hpp>

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <memory>

namespace asio = boost::asio;
using namespace std::chrono_literals;
using writ::wait_outcome;

namespace {

std::chrono::steady_clock::time_point
in(std::chrono::steady_clock::duration d)
{
    return std::chrono::steady_clock::now() + d;
}

/// Drain every handler that is currently runnable, without waiting on
/// timers: repeat poll until a pass makes no progress.
void
drain(asio::io_context& io)
{
    while (io.poll() != 0)
        io.restart();
    io.restart();
}

template <class T>
T
await(asio::io_context& io, std::future<T>& result)
{
    while (result.wait_for(1ms) != std::future_status::ready)
    {
        io.poll();
        io.restart();
    }
    io.restart();
    return result.get();
}

}  // namespace

// The lost-wake counterexample and its repair, side by side. A bare timer's
// cancel() is an edge: published before the wait registers, it is lost. The
// retained signal converts the same publication into state.
TEST(WritRetainedSignal, CompletionBeforeSubscriptionIsRetained)
{
    asio::io_context io;
    writ::home h(io);

    // Negative control: the bare-timer construction loses the pre-wake.
    auto bare = h.timer(asio::steady_timer::time_point::max());
    EXPECT_EQ(bare->cancel(), 0u)
        << "no wait existed, so the edge notified nobody";

    auto signal = std::make_shared<writ::retained_signal>(h);
    signal->post_complete();
    drain(io);

    auto woken =
        asio::co_spawn(io, signal->co_wait_until(in(5s)), asio::use_future);
    EXPECT_EQ(await(io, woken), wait_outcome::signaled)
        << "a completion retained before subscription must be consumed "
           "without suspending until the deadline";
}

// Cancellation is sampled at the subscriber boundary and dominates a shared
// wake queued in the same owner turn — for THAT subscriber only. The
// canceled subscriber must not be resurrected as signaled, and a live
// sibling registered against the same signal must still consume the shared
// completion normally (round 5: the schema's rule claimed the sibling half
// before this test evidenced it).
TEST(WritRetainedSignal, CancelThenSignalInOneOwnerTurnIsCanceled)
{
    asio::io_context io;
    writ::home h(io);
    auto signal = std::make_shared<writ::retained_signal>(h);

    asio::cancellation_signal cancellation;
    auto woken = asio::co_spawn(
        io,
        signal->co_wait_until(in(5s)),
        asio::bind_cancellation_slot(cancellation.slot(), asio::use_future));
    auto sibling =
        asio::co_spawn(io, signal->co_wait_until(in(5s)), asio::use_future);
    drain(io);  // both subscribers are now registered and suspended

    asio::post(h.executor(), [&cancellation, signal]() {
        cancellation.emit(asio::cancellation_type::terminal);
        signal->complete();
    });

    EXPECT_EQ(await(io, woken), wait_outcome::canceled);
    EXPECT_EQ(await(io, sibling), wait_outcome::signaled)
        << "the sibling still consumes the shared wake the canceled "
           "subscriber could not";
}

// One subscriber's deadline retires only that subscriber: it can neither
// consume the retained wake nor poison a sibling.
TEST(WritRetainedSignal, SiblingDeadlineCannotConsumeOrPoisonTheWake)
{
    asio::io_context io;
    writ::home h(io);
    auto signal = std::make_shared<writ::retained_signal>(h);

    auto short_waiter =
        asio::co_spawn(io, signal->co_wait_until(in(30ms)), asio::use_future);
    auto long_waiter =
        asio::co_spawn(io, signal->co_wait_until(in(5s)), asio::use_future);

    EXPECT_EQ(await(io, short_waiter), wait_outcome::deadline_elapsed);
    EXPECT_EQ(long_waiter.wait_for(0ms), std::future_status::timeout)
        << "the sibling must still be suspended";

    signal->post_complete();
    EXPECT_EQ(await(io, long_waiter), wait_outcome::signaled);
}

TEST(WritRetainedSignal, SignalBeforeDeadlineIsSignaled)
{
    asio::io_context io;
    writ::home h(io);
    auto signal = std::make_shared<writ::retained_signal>(h);

    auto woken =
        asio::co_spawn(io, signal->co_wait_until(in(5s)), asio::use_future);
    drain(io);
    signal->post_complete();
    EXPECT_EQ(await(io, woken), wait_outcome::signaled);
}

// Repeated wake generations rotate: a fresh signal must not leak the
// previous generation's retained completion, and completing the fresh
// generation wakes its own subscribers.
TEST(WritRetainedSignal, FreshSignalAfterRetainedCompletionStartsClean)
{
    asio::io_context io;
    writ::home h(io);

    auto first = std::make_shared<writ::retained_signal>(h);
    first->post_complete();
    drain(io);

    auto second = std::make_shared<writ::retained_signal>(h);
    auto woken =
        asio::co_spawn(io, second->co_wait_until(in(5s)), asio::use_future);
    drain(io);
    EXPECT_EQ(woken.wait_for(0ms), std::future_status::timeout)
        << "the fresh generation must not observe the old completion";

    second->post_complete();
    EXPECT_EQ(await(io, woken), wait_outcome::signaled);
}

// A subscriber whose caller was already canceled never registers at all.
TEST(WritRetainedSignal, PreexistingCancellationRefusesTheWait)
{
    asio::io_context io;
    writ::home h(io);
    auto signal = std::make_shared<writ::retained_signal>(h);

    asio::cancellation_signal cancellation;
    cancellation.emit(asio::cancellation_type::terminal);
    auto woken = asio::co_spawn(
        io,
        signal->co_wait_until(in(5s)),
        asio::bind_cancellation_slot(cancellation.slot(), asio::use_future));

    // Emission before the spawned frame first runs: the pre-check consumes
    // it without registering a waiter.
    cancellation.emit(asio::cancellation_type::terminal);
    EXPECT_EQ(await(io, woken), wait_outcome::canceled);
}
