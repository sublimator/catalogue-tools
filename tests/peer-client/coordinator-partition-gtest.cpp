#include "coordinator-partition.h"

#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

#include <limits>
#include <utility>

using namespace catl::provider_lab;
namespace asio = boost::asio;

namespace {

struct PartitionFixture : ::testing::Test
{
    DeterministicScheduler sched;
    DeterministicScheduler::strand_type strand = sched.make_strand();
    CoordinatorPartition part{
        strand, /*provider*/ 11, /*partition*/ 2, /*epoch*/ 3};

    template <typename Fn>
    void
    run(Fn fn)
    {
        asio::post(strand, std::move(fn));
        sched.run_until_idle();
    }
};

using R = CoordinatorPartition::Refusal;

}  // namespace

TEST_F(PartitionFixture, OneFetchIdPerLogicalNeedAcrossShapes)
{
    run([&] {
        auto first = part.get_or_create_fetch(3, "hdr:100", "batch=4");
        ASSERT_EQ(first.refusal, R::ok);
        EXPECT_TRUE(first.created);

        // A differently-shaped second miss JOINS: shape is attempt
        // data inside the record, never identity.
        auto second = part.get_or_create_fetch(3, "hdr:100", "batch=16");
        EXPECT_EQ(second.refusal, R::ok);
        EXPECT_FALSE(second.created);
        EXPECT_EQ(second.fetch, first.fetch)
            << "two misses, any shapes, one FetchId";
        EXPECT_EQ(part.joins_of(first.fetch).value_or(0), 1u);
        EXPECT_EQ(part.fetch_count().value_or(0), 1u);
    });
}

TEST_F(PartitionFixture, JoinerCannotRewriteTheActiveAttemptContract)
{
    run([&] {
        auto created = part.get_or_create_fetch(3, "hdr:200", "S1");
        ASSERT_EQ(created.refusal, R::ok);

        // A joiner with a different shape has NO authority over the
        // active attempt's response contract: validation uses the
        // exact shape that was SENT.
        auto joined = part.get_or_create_fetch(3, "hdr:200", "S2");
        ASSERT_EQ(joined.refusal, R::ok);
        ASSERT_FALSE(joined.created);
        EXPECT_EQ(part.active_shape_of(created.fetch).value_or(""), "S1")
            << "the active revision/shape remains exactly as sent; "
               "shape changes need an explicit owner transition after "
               "the prior attempt terminalizes";
    });
}

TEST_F(PartitionFixture, RemovalIsAtomicAndNothingResurrects)
{
    run([&] {
        auto first = part.get_or_create_fetch(3, "node:abc", "s1");
        ASSERT_EQ(first.refusal, R::ok);
        ASSERT_EQ(part.remove_fetch(3, first.fetch), R::ok);
        EXPECT_EQ(part.fetch_count().value_or(99), 0u)
            << "record and index leave together — no tear";

        auto again = part.get_or_create_fetch(3, "node:abc", "s1");
        ASSERT_EQ(again.refusal, R::ok);
        EXPECT_TRUE(again.created);
        EXPECT_GT(again.fetch, first.fetch)
            << "a re-miss mints a NEW FetchId; nothing resurrects";

        EXPECT_EQ(part.remove_fetch(3, first.fetch), R::unknown_fetch)
            << "the removed id stays dead (typed inert no-op)";
    });
}

TEST_F(PartitionFixture, BarrierSealsAdmissionWhileExistingWorkDrains)
{
    run([&] {
        auto f = part.get_or_create_fetch(3, "k", "s");
        ASSERT_EQ(f.refusal, R::ok);

        EXPECT_EQ(
            part.get_or_create_fetch(2, "k", "s").refusal, R::wrong_epoch);
        EXPECT_EQ(part.remove_fetch(4, f.fetch), R::wrong_epoch);

        ASSERT_EQ(part.install_barrier(7), R::ok);

        // New-work admission is sealed...
        EXPECT_EQ(
            part.get_or_create_fetch(3, "k2", "s").refusal,
            R::barrier_installed);

        // ...and advance refuses while live rows exist: the zero
        // witness is a PRECONDITION, never destructive work. The
        // barrier stays installed and no receipt exists.
        auto blocked = part.advance_epoch(7);
        EXPECT_EQ(blocked.refusal, R::not_drained);
        EXPECT_FALSE(blocked.receipt.has_value());
        EXPECT_EQ(part.fetch_count().value_or(0), 1u)
            << "advance must not drop live work";

        // Terminal removal of registered old-epoch work is the
        // barrier's purpose: legal under the barrier, exact tag.
        EXPECT_EQ(part.remove_fetch(3, f.fetch), R::ok);

        // Drained: advance succeeds and mints the exact bound receipt.
        auto advanced = part.advance_epoch(7);
        ASSERT_EQ(advanced.refusal, R::ok);
        ASSERT_TRUE(advanced.receipt.has_value());
        EXPECT_EQ(advanced.receipt->provider(), 11u);
        EXPECT_EQ(advanced.receipt->partition(), 2u);
        EXPECT_EQ(advanced.receipt->barrier(), 7u);
        EXPECT_EQ(advanced.receipt->old_epoch(), 3u);
        EXPECT_EQ(advanced.receipt->new_epoch(), 4u);
        EXPECT_EQ(part.accepted_epoch().value_or(0), 4u);

        // The old epoch's key mints FRESH under the new epoch.
        auto fresh = part.get_or_create_fetch(4, "k", "s");
        EXPECT_EQ(fresh.refusal, R::ok);
        EXPECT_TRUE(fresh.created);
        EXPECT_GT(fresh.fetch, f.fetch) << "ids never reuse across epochs";
    });
}

TEST(PartitionEdge, FetchIdCeilingRefusesMintButServicesJoins)
{
    DeterministicScheduler sched;
    auto strand = sched.make_strand();
    // Lab seam: start the mint one below the ceiling.
    CoordinatorPartition part{
        strand, 1, 1, 1, std::numeric_limits<FetchId>::max() - 1};
    asio::post(strand, [&] {
        auto last = part.get_or_create_fetch(1, "existing", "s");
        ASSERT_EQ(last.refusal, CoordinatorPartition::Refusal::ok);
        ASSERT_EQ(last.fetch, std::numeric_limits<FetchId>::max() - 1);

        // The next mint would consume the reserved ceiling: refused,
        // fail closed, records intact.
        auto blocked = part.get_or_create_fetch(1, "new-need", "s");
        EXPECT_EQ(
            blocked.refusal,
            CoordinatorPartition::Refusal::fetch_id_exhausted)
            << "wrap/reuse would let an ancient id falsely agree";

        // Joins of EXISTING fetches remain fully serviceable.
        auto join = part.get_or_create_fetch(1, "existing", "s2");
        EXPECT_EQ(join.refusal, CoordinatorPartition::Refusal::ok);
        EXPECT_FALSE(join.created);
        EXPECT_EQ(part.fetch_count().value_or(0), 1u)
            << "exhaustion refuses the mint only; nothing is lost";
    });
    sched.run_until_idle();
}

TEST_F(PartitionFixture, OffStrandTouchesAreRefusedBeforeAnyRead)
{
    bool checked = false;
    asio::post(sched.io(), [&] {
        EXPECT_EQ(
            part.get_or_create_fetch(3, "k", "s").refusal, R::off_strand);
        EXPECT_EQ(part.remove_fetch(3, 1), R::off_strand);
        EXPECT_EQ(part.install_barrier(1), R::off_strand);
        EXPECT_EQ(part.advance_epoch(1).refusal, R::off_strand);
        EXPECT_FALSE(part.advance_epoch(1).receipt.has_value());
        EXPECT_FALSE(part.accepted_epoch().has_value());
        EXPECT_FALSE(part.fetch_count().has_value());
        EXPECT_FALSE(part.joins_of(1).has_value());
        EXPECT_FALSE(part.active_shape_of(1).has_value());
        checked = true;
    });
    sched.run_until_idle();
    EXPECT_TRUE(checked);
    run([&] { EXPECT_EQ(part.fetch_count().value_or(99), 0u); });
}

TEST_F(PartitionFixture, BarrierAuthorityDominatesTheZeroRead)
{
    run([&] {
        // A live row exists throughout; authority/phase refusals must
        // win BEFORE drain state is consulted, and the row plus the
        // absence of any receipt must survive every refusal.
        auto f = part.get_or_create_fetch(3, "live", "s");
        ASSERT_EQ(f.refusal, R::ok);

        // Running (no barrier installed) + arbitrary id: phase refusal,
        // never not_drained.
        auto running = part.advance_epoch(99);
        EXPECT_EQ(running.refusal, R::not_running)
            << "an unauthorized caller must not learn drain state";
        EXPECT_FALSE(running.receipt.has_value());

        ASSERT_EQ(part.install_barrier(7), R::ok);

        // Installed barrier + WRONG id: identity refusal, never
        // not_drained.
        auto wrong = part.advance_epoch(8);
        EXPECT_EQ(wrong.refusal, R::wrong_barrier);
        EXPECT_FALSE(wrong.receipt.has_value());

        // Exact installed id + live row: only NOW may drain state
        // answer.
        auto drained_check = part.advance_epoch(7);
        EXPECT_EQ(drained_check.refusal, R::not_drained);
        EXPECT_FALSE(drained_check.receipt.has_value());
        EXPECT_EQ(part.fetch_count().value_or(0), 1u)
            << "every refusal leaves the row intact";
    });
}

TEST_F(PartitionFixture, BarrierIdsRemainAMonotonicAcceptanceGate)
{
    run([&] {
        ASSERT_EQ(part.install_barrier(5), R::ok);
        ASSERT_EQ(part.advance_epoch(5).refusal, R::ok);
        EXPECT_EQ(part.install_barrier(5), R::barrier_reused);
        EXPECT_EQ(part.install_barrier(6), R::ok);
        ASSERT_EQ(part.advance_epoch(6).refusal, R::ok);
        EXPECT_EQ(part.accepted_epoch().value_or(0), 5u);
    });
}
