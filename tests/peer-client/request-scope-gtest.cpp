#include <catl/core/request-scope.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

using namespace catl::core;
using namespace std::chrono_literals;

namespace {

TEST(RequestScope, MintsUniqueNonzeroIdentityAndImmutableDeadline)
{
    auto const now = RequestScope::Clock::now();
    auto first = RequestScope::create(now + 5s);
    auto second = RequestScope::create(now + 10s);

    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    EXPECT_TRUE(first->id());
    EXPECT_TRUE(second->id());
    EXPECT_NE(first->id(), second->id());
    EXPECT_EQ(first->absolute_deadline(), now + 5s);
    EXPECT_EQ(second->absolute_deadline(), now + 10s);
}

TEST(RequestScope, RefusesAlreadyElapsedDeadline)
{
    auto const now = RequestScope::Clock::now();
    EXPECT_FALSE(RequestScope::create(now));
    EXPECT_FALSE(RequestScope::create(now - 1ms));
}

TEST(RequestScope, CancellationIsStickyAndCannotBeReplacedByDeadline)
{
    auto const now = RequestScope::Clock::now();
    auto scope = RequestScope::create(now + 5s);
    ASSERT_TRUE(scope);

    EXPECT_TRUE(scope->cancel(now + 1s));
    EXPECT_FALSE(scope->cancel(now + 2s));
    auto const after_deadline = scope->snapshot(now + 6s);
    EXPECT_EQ(after_deadline.terminal, RequestScopeTerminal::cancelled);
}

TEST(RequestScope, DeadlineWinsCancellationAtOrAfterBoundary)
{
    auto const now = RequestScope::Clock::now();
    auto scope = RequestScope::create(now + 5s);
    ASSERT_TRUE(scope);

    EXPECT_FALSE(scope->cancel(now + 5s));
    EXPECT_EQ(
        scope->snapshot(now + 5s).terminal,
        RequestScopeTerminal::deadline_elapsed);
}

TEST(RequestScope, ConcurrentCancellationMintsOneTerminalWinner)
{
    auto const now = RequestScope::Clock::now();
    auto scope = RequestScope::create(now + 30s);
    ASSERT_TRUE(scope);

    std::atomic<unsigned> winners{0};
    std::vector<std::thread> threads;
    threads.reserve(16);
    for (std::size_t i = 0; i < 16; ++i)
    {
        threads.emplace_back([&] {
            if (scope->cancel(now + 1s))
                winners.fetch_add(1, std::memory_order_relaxed);
        });
    }
    for (auto& thread : threads)
        thread.join();

    EXPECT_EQ(winners.load(std::memory_order_relaxed), 1u);
    EXPECT_EQ(
        scope->snapshot(now + 2s).terminal, RequestScopeTerminal::cancelled);
}

}  // namespace
