#include <catl/core/request-scope.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <optional>
#include <stdexcept>
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

TEST(RequestScope, CancellationBeforeCommitRejectsPreparedMutation)
{
    auto const now = RequestScope::Clock::now();
    auto scope = RequestScope::create(now + 30s);
    ASSERT_TRUE(scope);
    ASSERT_TRUE(scope->cancel(now + 1s));

    bool applied = false;
    auto const commit = scope->try_commit(
        now + 2s, [&applied](RequestCommitId) noexcept { applied = true; });

    EXPECT_FALSE(commit);
    EXPECT_FALSE(applied);
    EXPECT_FALSE(scope->snapshot(now + 2s).last_accepted_commit);
}

TEST(RequestScope, AcceptedCommitLinearizesBeforeLaterCancellation)
{
    auto const now = RequestScope::Clock::now();
    auto scope = RequestScope::create(now + 30s);
    ASSERT_TRUE(scope);

    RequestCommitId applied;
    auto const commit = scope->try_commit(
        now + 1s, [&applied](RequestCommitId id) noexcept { applied = id; });
    ASSERT_TRUE(commit);
    EXPECT_EQ(applied, *commit);
    EXPECT_EQ(scope->snapshot(now + 1s).last_accepted_commit, *commit);

    EXPECT_TRUE(scope->cancel(now + 2s));
    auto const terminal = scope->snapshot(now + 2s);
    EXPECT_EQ(terminal.terminal, RequestScopeTerminal::cancelled);
    EXPECT_EQ(terminal.last_accepted_commit, *commit);
}

TEST(RequestScope, TerminalObserverRunsExactlyOnceOutsideScopeLock)
{
    auto const now = RequestScope::Clock::now();
    auto scope = RequestScope::create(now + 30s);
    ASSERT_TRUE(scope);

    std::atomic<unsigned> calls{0};
    auto subscription = scope->subscribe([scope, &calls](RequestScopeTerminal) {
        calls.fetch_add(1, std::memory_order_relaxed);
        // Re-entry proves the callback does not run under the scope mutex.
        (void)scope->snapshot();
    });
    ASSERT_TRUE(subscription);

    EXPECT_TRUE(scope->cancel(now + 1s));
    EXPECT_FALSE(scope->cancel(now + 2s));
    EXPECT_EQ(calls.load(std::memory_order_relaxed), 1u);
}

TEST(RequestScope, ResetSubscriptionPreventsLaterNotification)
{
    auto const now = RequestScope::Clock::now();
    auto scope = RequestScope::create(now + 30s);
    ASSERT_TRUE(scope);

    std::atomic<unsigned> calls{0};
    auto subscription = scope->subscribe([&calls](RequestScopeTerminal) {
        calls.fetch_add(1, std::memory_order_relaxed);
    });
    subscription.reset();
    ASSERT_TRUE(scope->cancel(now + 1s));
    EXPECT_EQ(calls.load(std::memory_order_relaxed), 0u);
}

TEST(RequestScope, LateSubscriberObservesExistingTerminalExactlyOnce)
{
    auto const now = RequestScope::Clock::now();
    auto scope = RequestScope::create(now + 30s);
    ASSERT_TRUE(scope);
    ASSERT_TRUE(scope->cancel(now + 1s));

    std::atomic<unsigned> calls{0};
    auto subscription =
        scope->subscribe([&calls](RequestScopeTerminal terminal) {
            EXPECT_EQ(terminal, RequestScopeTerminal::cancelled);
            calls.fetch_add(1, std::memory_order_relaxed);
        });
    EXPECT_TRUE(subscription);
    EXPECT_EQ(calls.load(std::memory_order_relaxed), 1u);
}

TEST(RequestScope, ThrowingTerminalObserverCannotEscapeCancellation)
{
    auto const now = RequestScope::Clock::now();
    auto scope = RequestScope::create(now + 30s);
    ASSERT_TRUE(scope);
    auto subscription = scope->subscribe(
        [](RequestScopeTerminal) { throw std::runtime_error("observer"); });

    EXPECT_NO_THROW(EXPECT_TRUE(scope->cancel(now + 1s)));
    EXPECT_EQ(
        scope->snapshot(now + 1s).terminal, RequestScopeTerminal::cancelled);
}

TEST(RequestScope, LastOwnerDestructionPublishesCompletedExactlyOnce)
{
    auto const now = RequestScope::Clock::now();
    std::optional<RequestScopeTerminal> observed;
    std::atomic<unsigned> calls{0};
    RequestScope::Subscription subscription;
    {
        auto scope = RequestScope::create(now + 30s);
        ASSERT_TRUE(scope);
        subscription = scope->subscribe(
            [&observed, &calls](RequestScopeTerminal terminal) {
                observed = terminal;
                calls.fetch_add(1, std::memory_order_relaxed);
            });
    }

    EXPECT_EQ(observed, RequestScopeTerminal::completed);
    EXPECT_EQ(calls.load(std::memory_order_relaxed), 1u);
}

TEST(RequestScope, LastOwnerCannotReplaceEarlierCancellation)
{
    auto const now = RequestScope::Clock::now();
    std::optional<RequestScopeTerminal> observed;
    std::atomic<unsigned> calls{0};
    RequestScope::Subscription subscription;
    {
        auto scope = RequestScope::create(now + 30s);
        ASSERT_TRUE(scope);
        subscription = scope->subscribe(
            [&observed, &calls](RequestScopeTerminal terminal) {
                observed = terminal;
                calls.fetch_add(1, std::memory_order_relaxed);
            });
        ASSERT_TRUE(scope->cancel(now + 1s));
    }

    EXPECT_EQ(observed, RequestScopeTerminal::cancelled);
    EXPECT_EQ(calls.load(std::memory_order_relaxed), 1u);
}

}  // namespace
