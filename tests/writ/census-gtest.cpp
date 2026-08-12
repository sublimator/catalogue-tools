#include <writ/census.hpp>
#include <writ/home.hpp>
#include <writ/retained-signal.hpp>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_future.hpp>

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <optional>
#include <string>

namespace asio = boost::asio;
using namespace std::chrono_literals;
using writ::wait_outcome;

namespace {

std::chrono::steady_clock::time_point
in(std::chrono::steady_clock::duration d)
{
    return std::chrono::steady_clock::now() + d;
}

void
drain_io(asio::io_context& io)
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

using string_census = writ::census<std::string>;

}  // namespace

TEST(WritCensus, AdmitSealRetireDrainsAndCompletesTheWake)
{
    asio::io_context io;
    writ::home h(io);
    string_census c(h);

    auto woken = asio::co_spawn(
        io, c.drained_signal()->co_wait_until(in(5s)), asio::use_future);

    bool first_retire_drained = true;
    bool second_retire_drained = false;
    asio::post(h.executor(), [&]() {
        auto a = c.admit("a");
        auto b = c.admit("b");
        ASSERT_TRUE(a && *a);
        ASSERT_TRUE(b && *b);
        c.seal();
        EXPECT_FALSE(c.drained()) << "sealed but two members live";
        first_retire_drained = c.retire(std::move(*a));
        second_retire_drained = c.retire(std::move(*b));
    });

    EXPECT_EQ(await(io, woken), wait_outcome::signaled)
        << "the drain is received from its owner through the retained wake";
    EXPECT_FALSE(first_retire_drained);
    EXPECT_TRUE(second_retire_drained)
        << "exactly the drain-completing retirement reports it";
}

// An empty population before seal is a photograph, not quiescence: the
// census refuses to drain until admission is sealed.
TEST(WritCensus, ZeroBeforeSealIsNotDrained)
{
    asio::io_context io;
    writ::home h(io);
    string_census c(h);

    auto woken = asio::co_spawn(
        io, c.drained_signal()->co_wait_until(in(5s)), asio::use_future);

    asio::post(h.executor(), [&]() {
        auto a = c.admit("a");
        ASSERT_TRUE(a && *a);
        (void)c.retire(std::move(*a));
        EXPECT_EQ(c.live(), 0u);
        EXPECT_FALSE(c.drained()) << "zero live is not drained before seal";
    });
    drain_io(io);
    EXPECT_EQ(woken.wait_for(0ms), std::future_status::timeout);

    asio::post(h.executor(), [&]() { c.seal(); });
    EXPECT_EQ(await(io, woken), wait_outcome::signaled)
        << "sealing an already-empty census drains it immediately";
}

TEST(WritCensus, SealFencesLateAdmissionAndDuplicatesRefuse)
{
    asio::io_context io;
    writ::home h(io);
    string_census c(h);

    asio::post(h.executor(), [&]() {
        auto a = c.admit("a");
        ASSERT_TRUE(a && *a);
        EXPECT_FALSE(c.admit("a").has_value())
            << "the live row itself is the dedup state";
        (void)c.retire(std::move(*a));
        auto again = c.admit("a");
        EXPECT_TRUE(again && *again)
            << "a retired key may be re-admitted (successor identity)";
        (void)c.retire(std::move(*again));
        c.seal();
        EXPECT_FALSE(c.admit("late").has_value())
            << "seal fences every later admission";
        EXPECT_TRUE(c.drained());
    });
    drain_io(io);
}

// The essay's rule 2: a launch refused synchronously (or a body that never
// started under inherited cancellation) still holds its member and retires
// it through the caller's never-started path. Census-side this is an
// ordinary retirement; the point is that registration happened BEFORE the
// fallible launch, so no failure shape can skip the census.
TEST(WritCensus, NeverStartedStillRetiresThroughTheCensus)
{
    asio::io_context io;
    writ::home h(io);
    string_census c(h);

    asio::post(h.executor(), [&]() {
        auto m = c.admit("servant");
        ASSERT_TRUE(m && *m);
        bool const launch_refused = true;  // simulated synchronous refusal
        if (launch_refused)
            (void)c.retire(std::move(*m));  // never-started disposition
        c.seal();
        EXPECT_TRUE(c.drained());
    });
    drain_io(io);
}

// The wedge is the feature: a member dropped without retirement is recorded,
// and the census refuses to drain forever — visibly, instead of a shutdown
// that silently hangs on an unaccounted obligation.
TEST(WritCensus, LeakedMemberWedgesTheDrainVisibly)
{
    asio::io_context io;
    writ::home h(io);
    string_census c(h);

    auto woken = asio::co_spawn(
        io, c.drained_signal()->co_wait_until(in(50ms)), asio::use_future);

    asio::post(h.executor(), [&]() {
        auto kept = c.admit("kept");
        auto dropped = c.admit("dropped");
        ASSERT_TRUE(kept && *kept);
        ASSERT_TRUE(dropped && *dropped);
        c.seal();
        (void)c.retire(std::move(*kept));
        dropped.reset();  // dropped without retirement: the leak
    });
    drain_io(io);

    asio::post(h.executor(), [&]() {
        EXPECT_EQ(c.leaked(), 1u);
        EXPECT_EQ(c.live(), 0u);
        EXPECT_FALSE(c.drained()) << "a leak wedges the drain forever, visibly";
    });

    EXPECT_EQ(await(io, woken), wait_outcome::deadline_elapsed)
        << "the drain wake must not fire past a recorded leak";
}

// A member retired against a foreign census is refused WITHOUT being
// consumed: the refusal must not counterfeit consumption, so the obligation
// survives and can still balance at its rightful owner.
TEST(WritCensus, ForeignRetireRefusesWithoutConsumingTheObligation)
{
    asio::io_context io;
    writ::home h(io);
    string_census first(h);
    string_census second(h);

    asio::post(h.executor(), [&]() {
        auto m = first.admit("a");
        ASSERT_TRUE(m && *m);
        EXPECT_FALSE(second.retire(std::move(*m)))
            << "a foreign member must refuse";
        EXPECT_TRUE(*m) << "the refusal must leave the obligation intact";
        first.seal();
        EXPECT_TRUE(first.retire(std::move(*m)))
            << "the surviving token still balances its rightful census";
        EXPECT_TRUE(first.drained());
    });
    drain_io(io);
}
