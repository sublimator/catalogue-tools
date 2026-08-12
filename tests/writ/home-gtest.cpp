#include <writ/home.hpp>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <thread>

namespace asio = boost::asio;
using namespace std::chrono_literals;

namespace {

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

// The WaitReenters fossil, generalized: a home-born child that suspends on a
// home-born timer resumes inside the home's serialized boundary, across two
// concurrent runner threads, on every iteration. This exercises the real
// contention (timer completions landing on either runner) rather than one
// fixed schedule.
TEST(WritHome, SuspendedOperationResumesUnderHomeAcrossConcurrentRunners)
{
    asio::io_context io;
    writ::home h(io);

    auto worker = [&h]() -> asio::awaitable<int, writ::home::executor_type> {
        int violations = 0;
        for (int i = 0; i < 60; ++i)
        {
            auto timer = h.timer(std::chrono::steady_clock::now() + 1ms);
            boost::system::error_code error;
            co_await timer->async_wait(asio::redirect_error(
                asio::use_awaitable_t<writ::home::executor_type>{}, error));
            if (!h.running_here())
                ++violations;
            if (!WRIT_ASSERT_HOME(h))
                ++violations;
        }
        co_return violations;
    };

    auto result = asio::co_spawn(io, h.spawn(worker), asio::use_future);

    auto guard = asio::make_work_guard(io);
    std::thread first([&io]() { io.run(); });
    std::thread second([&io]() { io.run(); });
    int const violations = result.get();
    guard.reset();
    io.stop();
    first.join();
    second.join();

    EXPECT_EQ(violations, 0);
}

// Birth is not relocation: the child is born on the home, and the parent's
// continuation re-enters through the PARENT's executor. The claim is positive
// only — running_here() is containment, not exclusivity, and the completion
// dispatch may run inline, transiently nesting the parent's boundary inside
// the home's still-active handler. (This suite's first draft asserted
// !h.running_here() at parent resume and the control correctly refused it.)
TEST(WritHome, ChildBirthDoesNotRelocateTheParentContinuation)
{
    asio::io_context io;
    writ::home h(io);
    writ::home caller(io);

    bool child_ran_on_home = false;
    bool parent_resumed_on_caller = false;

    auto parent = [&]() -> asio::awaitable<void> {
        co_await h.spawn(
            [&]() -> asio::awaitable<void, writ::home::executor_type> {
                child_ran_on_home = h.running_here();
                co_return;
            });
        parent_resumed_on_caller = caller.running_here();
    };

    auto done = asio::co_spawn(caller.executor(), parent(), asio::use_future);
    await(io, done);

    EXPECT_TRUE(child_ran_on_home);
    EXPECT_TRUE(parent_resumed_on_caller)
        << "the parent must re-enter through its own executor's boundary";
}

// The witness stays armed, and the negative control demonstrably trips:
// off-home the probe returns false (and assert_here reports without aborting
// unless WRIT_HOME_FAIL_FAST is set).
TEST(WritHome, WitnessReportsOnHomeAndTripsOffHome)
{
    asio::io_context io;
    writ::home h(io);

    // Off-home: the calling test thread is inside no strand.
    EXPECT_FALSE(h.running_here());
    EXPECT_FALSE(h.assert_here("off-home-control", __FILE__, __LINE__));

    bool on_home = false;
    bool off_home_from_plain_handler = true;
    asio::post(h.executor(), [&]() { on_home = WRIT_ASSERT_HOME(h); });
    asio::post(io, [&]() { off_home_from_plain_handler = h.running_here(); });
    io.run();

    EXPECT_TRUE(on_home);
    EXPECT_FALSE(off_home_from_plain_handler)
        << "a plain io_context handler is outside the home's boundary";
}
