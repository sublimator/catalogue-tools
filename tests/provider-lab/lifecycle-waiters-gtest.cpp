#include "lifecycle-waiters.h"

#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

#include <optional>
#include <utility>
#include <vector>

using namespace catl::provider_lab;
namespace asio = boost::asio;

namespace {

struct WaiterFixture : ::testing::Test
{
    DeterministicScheduler sched;
    DeterministicScheduler::strand_type owner = sched.make_strand();
    DeterministicScheduler::strand_type caller = sched.make_strand();
    LifecycleWaiterLedger ledger{owner};

    template <typename Fn>
    void
    on_owner(Fn fn)
    {
        asio::post(owner, std::move(fn));
        sched.run_until_idle();
    }

    LifecycleWaiterId
    add(WaiterKind kind,
        LifecycleTargetId target,
        std::vector<LifecycleResult>& sink,
        bool* on_caller_strand = nullptr)
    {
        LifecycleWaiterId id = 0;
        on_owner([&, this] {
            auto [r, got] = ledger.register_waiter(
                kind,
                target,
                caller,
                [&sink, this, on_caller_strand](LifecycleResult res) {
                    if (on_caller_strand)
                        *on_caller_strand = caller.running_in_this_thread();
                    sink.push_back(res);
                });
            ASSERT_EQ(r, LifecycleWaiterLedger::Refusal::ok);
            id = got;
        });
        return id;
    }
};

}  // namespace

TEST_F(WaiterFixture, CompletesExactlyOnceOnTheWaiterExecutor)
{
    std::vector<LifecycleResult> got;
    bool delivered_on_caller = false;
    add(WaiterKind::clear, 1, got, &delivered_on_caller);

    on_owner([&] {
        EXPECT_EQ(
            ledger.complete_clear(
                1, {LifecycleResult::Outcome::cleared, 42}),
            1u);
    });
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got[0].outcome, LifecycleResult::Outcome::cleared);
    EXPECT_EQ(got[0].value, 42u);
    EXPECT_TRUE(delivered_on_caller)
        << "delivery must run on the waiter's own executor";

    // Duplicate completion is a counted no-op.
    on_owner([&] {
        EXPECT_EQ(
            ledger.complete_clear(
                1, {LifecycleResult::Outcome::cleared, 43}),
            0u);
    });
    EXPECT_EQ(got.size(), 1u) << "exactly-once delivery";
}

TEST_F(WaiterFixture, CancelRemovesOnlyItsWaiterAndTransactionContinues)
{
    std::vector<LifecycleResult> initiator, bystander;
    auto const initiator_id = add(WaiterKind::clear, 1, initiator);
    add(WaiterKind::clear, 1, bystander);

    // The initiating caller leaves...
    on_owner([&] {
        EXPECT_EQ(
            ledger.cancel_waiter(initiator_id),
            LifecycleWaiterLedger::Refusal::ok);
    });
    ASSERT_EQ(initiator.size(), 1u);
    EXPECT_EQ(initiator[0].outcome, LifecycleResult::Outcome::cancelled);
    EXPECT_TRUE(bystander.empty()) << "cancel removes ONLY its waiter";

    // ...and the lifecycle transaction still completes the bystander.
    on_owner([&] {
        EXPECT_EQ(
            ledger.complete_clear(
                1, {LifecycleResult::Outcome::cleared, 7}),
            1u);
    });
    ASSERT_EQ(bystander.size(), 1u);
    EXPECT_EQ(bystander[0].outcome, LifecycleResult::Outcome::cleared);

    // Cancelling the already-completed initiator again is a typed no-op.
    on_owner([&] {
        EXPECT_EQ(
            ledger.cancel_waiter(initiator_id),
            LifecycleWaiterLedger::Refusal::unknown_waiter);
    });
    EXPECT_EQ(initiator.size(), 1u);
}

TEST_F(WaiterFixture, ShutdownTakeoverBothOrders)
{
    // clear-completion and shutdown-takeover compete for the clear
    // waiters; whichever fires first wins, the second is a counted
    // no-op, and shutdown waiters are untouched until stopped.
    for (bool takeover_first : {true, false})
    {
        std::vector<LifecycleResult> clear_got, shutdown_got;
        DeterministicScheduler s2;
        auto owner2 = s2.make_strand();
        LifecycleWaiterLedger l2{owner2};
        auto run2 = [&](auto fn) {
            asio::post(owner2, std::move(fn));
            s2.run_until_idle();
        };
        run2([&] {
            auto [r1, id1] = l2.register_waiter(
                WaiterKind::clear, 1, owner2, [&](LifecycleResult res) {
                    clear_got.push_back(res);
                });
            auto [r2, id2] = l2.register_waiter(
                WaiterKind::shutdown, 1, owner2, [&](LifecycleResult res) {
                    shutdown_got.push_back(res);
                });
            ASSERT_EQ(r1, LifecycleWaiterLedger::Refusal::ok);
            ASSERT_EQ(r2, LifecycleWaiterLedger::Refusal::ok);
        });

        run2([&] {
            if (takeover_first)
            {
                EXPECT_EQ(l2.shutdown_takeover(1), 1u);
                EXPECT_EQ(
                    l2.complete_clear(
                        1, {LifecycleResult::Outcome::cleared, 9}),
                    0u);
            }
            else
            {
                EXPECT_EQ(
                    l2.complete_clear(
                        1, {LifecycleResult::Outcome::cleared, 9}),
                    1u);
                EXPECT_EQ(l2.shutdown_takeover(1), 0u);
            }
        });
        ASSERT_EQ(clear_got.size(), 1u);
        EXPECT_EQ(
            clear_got[0].outcome,
            takeover_first
                ? LifecycleResult::Outcome::superseded_by_shutdown
                : LifecycleResult::Outcome::cleared);
        EXPECT_TRUE(shutdown_got.empty())
            << "shutdown waiters wait for stopped, not takeover";

        run2([&] { EXPECT_EQ(l2.complete_stopped(1, 99), 1u); });
        ASSERT_EQ(shutdown_got.size(), 1u);
        EXPECT_EQ(shutdown_got[0].outcome, LifecycleResult::Outcome::stopped);
        EXPECT_EQ(shutdown_got[0].value, 99u);
    }
}

TEST_F(WaiterFixture, TargetsAreIndependent)
{
    std::vector<LifecycleResult> t1, t2;
    add(WaiterKind::clear, 1, t1);
    add(WaiterKind::clear, 2, t2);

    on_owner([&] {
        EXPECT_EQ(
            ledger.complete_clear(
                1, {LifecycleResult::Outcome::failed, 3}),
            1u);
    });
    EXPECT_EQ(t1.size(), 1u);
    EXPECT_TRUE(t2.empty()) << "completion is target-scoped";
}

TEST_F(WaiterFixture, WaiterIdsAreNeverReused)
{
    std::vector<LifecycleResult> got;
    auto const first = add(WaiterKind::clear, 1, got);
    on_owner([&] {
        ledger.complete_clear(1, {LifecycleResult::Outcome::cleared, 1});
    });
    auto const second = add(WaiterKind::clear, 1, got);
    EXPECT_GT(second, first);
}

TEST_F(WaiterFixture, OffStrandTouchesAreRefused)
{
    std::vector<LifecycleResult> got;
    add(WaiterKind::clear, 1, got);

    bool checked = false;
    asio::post(sched.io(), [&] {
        auto [r, id] = ledger.register_waiter(
            WaiterKind::clear, 1, caller, [](LifecycleResult) {});
        EXPECT_EQ(r, LifecycleWaiterLedger::Refusal::off_strand);
        EXPECT_EQ(id, 0u);
        EXPECT_EQ(
            ledger.cancel_waiter(1),
            LifecycleWaiterLedger::Refusal::off_strand);
        EXPECT_EQ(
            ledger.complete_clear(
                1, {LifecycleResult::Outcome::cleared, 0}),
            0u);
        EXPECT_EQ(ledger.pending_count(), 0u)
            << "off-strand observation is refused, not raced";
        checked = true;
    });
    sched.run_until_idle();
    EXPECT_TRUE(checked);
    EXPECT_TRUE(got.empty()) << "no delivery may result from refusals";
}
