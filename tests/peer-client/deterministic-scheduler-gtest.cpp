#include "deterministic-scheduler.h"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <gtest/gtest.h>

#include <string>
#include <vector>

using namespace catl::provider_lab;
namespace asio = boost::asio;

TEST(DeterministicScheduler, StepRunsExactlyOneHandler)
{
    DeterministicScheduler sched;
    int ran = 0;
    asio::post(sched.io(), [&] { ++ran; });
    asio::post(sched.io(), [&] { ++ran; });

    EXPECT_TRUE(sched.step());
    EXPECT_EQ(ran, 1);
    EXPECT_TRUE(sched.step());
    EXPECT_EQ(ran, 2);
    EXPECT_FALSE(sched.step()) << "no ready handler may remain";
}

TEST(DeterministicScheduler, RunUntilIdleDrainsCascades)
{
    DeterministicScheduler sched;
    int ran = 0;
    asio::post(sched.io(), [&] {
        ++ran;
        asio::post(sched.io(), [&] { ++ran; });
    });
    EXPECT_EQ(sched.run_until_idle(), 2u);
    EXPECT_EQ(ran, 2);
}

TEST(DeterministicScheduler, VirtualTimersFireOnlyOnAdvance)
{
    DeterministicScheduler sched;
    DeterministicScheduler::Timer timer(sched);
    bool fired = false;
    boost::system::error_code fired_ec;

    timer.expires_after(std::chrono::milliseconds(100));
    timer.async_wait([&](boost::system::error_code ec) {
        fired = true;
        fired_ec = ec;
    });

    // Negative control: idle-draining and near-deadline advance must not
    // fire the timer early.
    sched.run_until_idle();
    EXPECT_FALSE(fired);
    sched.advance(std::chrono::milliseconds(99));
    EXPECT_FALSE(fired) << "99ms must not fire a 100ms timer";

    sched.advance(std::chrono::milliseconds(1));
    EXPECT_TRUE(fired);
    EXPECT_FALSE(fired_ec) << "on-time fire completes with success";
}

TEST(DeterministicScheduler, TimersFireInDeadlineOrder)
{
    DeterministicScheduler sched;
    DeterministicScheduler::Timer late(sched);
    DeterministicScheduler::Timer early(sched);
    std::vector<std::string> order;

    late.expires_after(std::chrono::milliseconds(200));
    late.async_wait([&](boost::system::error_code) {
        order.push_back("late");
    });
    early.expires_after(std::chrono::milliseconds(100));
    early.async_wait([&](boost::system::error_code) {
        order.push_back("early");
    });

    sched.advance(std::chrono::milliseconds(250));
    ASSERT_EQ(order.size(), 2u);
    EXPECT_EQ(order[0], "early");
    EXPECT_EQ(order[1], "late");
}

TEST(DeterministicScheduler, CancelDeliversOperationAborted)
{
    DeterministicScheduler sched;
    DeterministicScheduler::Timer timer(sched);
    boost::system::error_code got;
    bool completed = false;

    timer.expires_after(std::chrono::seconds(5));
    timer.async_wait([&](boost::system::error_code ec) {
        completed = true;
        got = ec;
    });
    EXPECT_TRUE(timer.cancel());
    sched.run_until_idle();
    EXPECT_TRUE(completed);
    EXPECT_EQ(got, asio::error::operation_aborted);

    // Negative control: the cancelled timer must not fire again when
    // time later crosses its old deadline.
    completed = false;
    sched.advance(std::chrono::seconds(10));
    EXPECT_FALSE(completed);
}

TEST(DeterministicScheduler, ForcedWinnerBothOrders)
{
    // Two competing "commits" race for one cell; the test chooses the
    // winner deterministically by trigger order — the forced-winner
    // primitive every interleaving test builds on.
    for (bool a_first : {true, false})
    {
        DeterministicScheduler sched;
        auto strand = sched.make_strand();
        std::string cell;

        auto commit = [&](std::string name) {
            asio::post(strand, [&, name] {
                if (cell.empty())
                    cell = name;
            });
        };

        if (a_first)
        {
            commit("A");
            commit("B");
        }
        else
        {
            commit("B");
            commit("A");
        }
        sched.run_until_idle();
        EXPECT_EQ(cell, a_first ? "A" : "B");
    }
}

TEST(DeterministicScheduler, GateParksCoroutineUntilOpened)
{
    DeterministicScheduler sched;
    Gate gate(sched.io());
    std::vector<std::string> order;

    asio::co_spawn(
        sched.io(),
        [&]() -> asio::awaitable<void> {
            order.push_back("before-gate");
            co_await gate.async_wait(asio::use_awaitable);
            order.push_back("after-gate");
        },
        asio::detached);

    sched.run_until_idle();
    ASSERT_EQ(order.size(), 1u);
    EXPECT_EQ(order[0], "before-gate");

    gate.open();
    sched.run_until_idle();
    ASSERT_EQ(order.size(), 2u);
    EXPECT_EQ(order[1], "after-gate");

    // Sticky: a wait after open completes without a new open().
    bool late = false;
    asio::co_spawn(
        sched.io(),
        [&]() -> asio::awaitable<void> {
            co_await gate.async_wait(asio::use_awaitable);
            late = true;
        },
        asio::detached);
    sched.run_until_idle();
    EXPECT_TRUE(late);
}

TEST(StickyCancelCell, LevelTriggeredBothOrders)
{
    DeterministicScheduler sched;
    auto strand = sched.make_strand();
    StickyCancelCell cell(strand);

    // Wait first, then request: waiter wakes.
    bool woke = false;
    cell.async_wait([&](boost::system::error_code) { woke = true; });
    sched.run_until_idle();
    EXPECT_FALSE(woke);
    asio::post(strand, [&] { EXPECT_TRUE(cell.request()); });
    sched.run_until_idle();
    EXPECT_TRUE(woke);

    // Request already latched: a later wait completes immediately —
    // the level-trigger, not an edge.
    bool late_woke = false;
    cell.async_wait([&](boost::system::error_code) { late_woke = true; });
    sched.run_until_idle();
    EXPECT_TRUE(late_woke);

    // Second request is a sticky no-op.
    asio::post(strand, [&] { EXPECT_TRUE(cell.request()); });
    sched.run_until_idle();
}

TEST(StickyCancelCell, OffStrandTouchesAreRefused)
{
    DeterministicScheduler sched;
    auto strand = sched.make_strand();
    StickyCancelCell cell(strand);

    // Negative control: from a plain (non-strand) handler, the cell
    // refuses both observation and mutation instead of racing.
    bool refused_request = true;
    bool observed_requested = false;
    asio::post(sched.io(), [&] {
        refused_request = cell.request();
        observed_requested = cell.requested();
    });
    sched.run_until_idle();
    EXPECT_FALSE(refused_request) << "off-strand request must be refused";
    EXPECT_FALSE(observed_requested);

    // The refused request must not have latched the cell.
    bool latched = true;
    asio::post(strand, [&] { latched = cell.requested(); });
    sched.run_until_idle();
    EXPECT_FALSE(latched);
}

TEST(DeterministicScheduler, SecondConcurrentWaitIsRefusedLoudly)
{
    DeterministicScheduler sched;
    DeterministicScheduler::Timer timer(sched);
    timer.expires_after(std::chrono::milliseconds(100));

    boost::system::error_code first_ec, second_ec;
    bool first_done = false, second_done = false;
    timer.async_wait([&](boost::system::error_code ec) {
        first_done = true;
        first_ec = ec;
    });
    timer.async_wait([&](boost::system::error_code ec) {
        second_done = true;
        second_ec = ec;
    });
    sched.run_until_idle();
    EXPECT_FALSE(first_done) << "the armed wait stays armed";
    EXPECT_TRUE(second_done);
    EXPECT_EQ(second_ec, asio::error::operation_not_supported)
        << "a second concurrent wait is refused, not silently mishandled";

    sched.advance(std::chrono::milliseconds(100));
    EXPECT_TRUE(first_done);
    EXPECT_FALSE(first_ec);
}

TEST(DeterministicScheduler, NegativeAdvanceIsRefused)
{
    DeterministicScheduler sched;
    auto const before = sched.now();
    DeterministicScheduler::Timer timer(sched);
    timer.expires_after(std::chrono::milliseconds(50));
    bool fired = false;
    timer.async_wait([&](boost::system::error_code) { fired = true; });

    sched.advance(-std::chrono::seconds(10));
    EXPECT_EQ(sched.now(), before) << "virtual time is monotonic";
    EXPECT_FALSE(fired);
    sched.advance(std::chrono::milliseconds(50));
    EXPECT_TRUE(fired);
}

TEST(DeterministicScheduler, SuccessfulFireAllowsRearmWithoutCancel)
{
    DeterministicScheduler sched;
    DeterministicScheduler::Timer timer(sched);
    int fires = 0;

    timer.expires_after(std::chrono::milliseconds(10));
    timer.async_wait([&](boost::system::error_code ec) {
        if (!ec)
            ++fires;
    });
    sched.advance(std::chrono::milliseconds(10));
    ASSERT_EQ(fires, 1);

    // The armed state must clear on successful fire: a fresh wait is a
    // new single wait, not a refused "concurrent" one.
    timer.expires_after(std::chrono::milliseconds(10));
    boost::system::error_code second_ec = asio::error::operation_not_supported;
    timer.async_wait([&](boost::system::error_code ec) {
        second_ec = ec;
        if (!ec)
            ++fires;
    });
    sched.advance(std::chrono::milliseconds(10));
    EXPECT_FALSE(second_ec) << "re-arm after successful fire must succeed";
    EXPECT_EQ(fires, 2);
}

TEST(DeterministicScheduler, HandlerMayRearmItsOwnTimer)
{
    DeterministicScheduler sched;
    DeterministicScheduler::Timer timer(sched);
    int fires = 0;

    std::function<void(boost::system::error_code)> tick =
        [&](boost::system::error_code ec) {
            if (ec)
                return;
            ++fires;
            if (fires < 3)
            {
                timer.expires_after(std::chrono::milliseconds(10));
                timer.async_wait(tick);
            }
        };
    timer.expires_after(std::chrono::milliseconds(10));
    timer.async_wait(tick);

    sched.advance(std::chrono::milliseconds(30));
    EXPECT_EQ(fires, 3) << "a fired handler re-arming its own timer must "
                           "cascade within one advance";
}

TEST(DeterministicScheduler, NegativeExpiryClampsAndCannotRegressTime)
{
    DeterministicScheduler sched;
    sched.advance(std::chrono::seconds(100));
    auto const before = sched.now();

    DeterministicScheduler::Timer timer(sched);
    timer.expires_after(-std::chrono::seconds(50));
    bool fired = false;
    timer.async_wait([&](boost::system::error_code ec) {
        if (!ec)
            fired = true;
    });

    // advance(0) fires the already-due timer WITHOUT moving time
    // backward — the side door Codex's residual named.
    sched.advance(std::chrono::seconds(0));
    EXPECT_TRUE(fired) << "negative expiry means already due";
    EXPECT_EQ(sched.now(), before) << "virtual time must never regress";
}

TEST(DeterministicScheduler, NearMaxExpiryClampsWithoutWrap)
{
    DeterministicScheduler sched;
    sched.advance(std::chrono::seconds(100));
    auto const before = sched.now();

    DeterministicScheduler::Timer timer(sched);
    timer.expires_after(DeterministicScheduler::duration::max());
    bool fired = false;
    timer.async_wait([&](boost::system::error_code) { fired = true; });

    sched.advance(std::chrono::seconds(100));
    EXPECT_FALSE(fired)
        << "a clamped far-future deadline must not wrap into the past";
    EXPECT_EQ(sched.now(), before + std::chrono::seconds(100))
        << "advance stays monotonic under clamped arithmetic";
}
