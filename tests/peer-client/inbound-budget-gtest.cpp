// Tests the process-wide inbound-buffer budget (security #0055): the aggregate
// ceiling that bounds total in-flight frame memory across all peer connections,
// on top of the per-frame kMaxFramePayloadSize cap.

#include <catl/peer-client/connection-types.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <limits>
#include <thread>
#include <vector>

namespace catl::peer_client::test {

TEST(InboundBudget, AcquireAndReleaseTrackTotal)
{
    InboundBudget b(1000);
    EXPECT_EQ(b.in_flight(), 0u);
    EXPECT_EQ(b.ceiling(), 1000u);

    EXPECT_TRUE(b.try_acquire(400));
    EXPECT_EQ(b.in_flight(), 400u);
    EXPECT_TRUE(b.try_acquire(600));  // exactly fills to the ceiling
    EXPECT_EQ(b.in_flight(), 1000u);

    b.release(600);
    EXPECT_EQ(b.in_flight(), 400u);
    b.release(400);
    EXPECT_EQ(b.in_flight(), 0u);
}

TEST(InboundBudget, RefusesOverCeilingAndReservesNothing)
{
    InboundBudget b(1000);
    ASSERT_TRUE(b.try_acquire(800));

    // 800 + 300 > 1000 → refused, and nothing reserved (in_flight unchanged).
    EXPECT_FALSE(b.try_acquire(300));
    EXPECT_EQ(b.in_flight(), 800u);

    // The remaining 200 still fits.
    EXPECT_TRUE(b.try_acquire(200));
    EXPECT_EQ(b.in_flight(), 1000u);

    // Now full — even a 1-byte request is refused.
    EXPECT_FALSE(b.try_acquire(1));
    EXPECT_EQ(b.in_flight(), 1000u);
}

TEST(InboundBudget, OverflowSafeOnHugeRequest)
{
    InboundBudget b(1000);
    ASSERT_TRUE(b.try_acquire(500));
    // A request near SIZE_MAX must be refused without integer overflow making
    // (cur + n) wrap below the ceiling.
    EXPECT_FALSE(b.try_acquire(std::numeric_limits<std::size_t>::max()));
    EXPECT_FALSE(b.try_acquire(std::numeric_limits<std::size_t>::max() - 499));
    EXPECT_EQ(b.in_flight(), 500u);
}

TEST(InboundBudget, ConcurrentAcquiresNeverExceedCeiling)
{
    // 8 threads each try many small acquires against a tight ceiling; the
    // running total must never exceed the ceiling (no lost-update overshoot).
    constexpr std::size_t kCeiling = 64;
    InboundBudget b(kCeiling);
    std::atomic<std::size_t> peak{0};
    std::atomic<int> granted{0};

    auto worker = [&] {
        for (int i = 0; i < 5000; ++i)
        {
            if (b.try_acquire(1))
            {
                std::size_t cur = b.in_flight();
                std::size_t prev = peak.load(std::memory_order_relaxed);
                while (cur > prev &&
                       !peak.compare_exchange_weak(prev, cur))
                {
                }
                granted.fetch_add(1, std::memory_order_relaxed);
                b.release(1);
            }
        }
    };

    std::vector<std::thread> threads;
    for (int t = 0; t < 8; ++t)
        threads.emplace_back(worker);
    for (auto& th : threads)
        th.join();

    EXPECT_LE(peak.load(), kCeiling) << "ceiling was overshot under contention";
    EXPECT_EQ(b.in_flight(), 0u) << "every acquire must be balanced by release";
    EXPECT_GT(granted.load(), 0);
}

TEST(InboundBudget, OverReleaseClampsAndDoesNotWrap)
{
    // A stray double-release must not underflow in_flight_ and wrap to ~SIZE_MAX
    // — which would silently disable the budget, since try_acquire's headroom
    // check (n > ceiling_ - cur) would then always pass (sec #0054). release()
    // clamps to the current total.
    InboundBudget b(1000);
    ASSERT_TRUE(b.try_acquire(300));
    b.release(300);
    EXPECT_EQ(b.in_flight(), 0u);

    // Buggy extra release: must clamp at 0, not wrap.
    b.release(300);
    EXPECT_EQ(b.in_flight(), 0u);

    // The budget still enforces its ceiling (not disabled by a wrapped counter).
    EXPECT_TRUE(b.try_acquire(1000));
    EXPECT_FALSE(b.try_acquire(1));
    EXPECT_EQ(b.in_flight(), 1000u);
}

TEST(InboundBudget, FrameCapFitsWithinAggregateBudget)
{
    // Sanity on the production constants: a single max frame fits, and the
    // aggregate is a small multiple of it (so legit traffic is never refused).
    EXPECT_GT(kAggregateInboundBudget, kMaxFramePayloadSize);
    EXPECT_GE(kAggregateInboundBudget / kMaxFramePayloadSize, 8u);

    InboundBudget b(kAggregateInboundBudget);
    EXPECT_TRUE(b.try_acquire(kMaxFramePayloadSize));
    b.release(kMaxFramePayloadSize);
    EXPECT_EQ(b.in_flight(), 0u);
}

}  // namespace catl::peer_client::test
