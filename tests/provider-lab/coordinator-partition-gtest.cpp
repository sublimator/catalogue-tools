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
    CoordinatorPartition part{strand, /*epoch*/ 3};

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
            << "the removed id stays dead";
    });
}

TEST_F(PartitionFixture, EpochGateGovernsBothVerbs)
{
    run([&] {
        auto f = part.get_or_create_fetch(3, "k", "s");
        ASSERT_EQ(f.refusal, R::ok);

        EXPECT_EQ(part.get_or_create_fetch(2, "k", "s").refusal, R::wrong_epoch);
        EXPECT_EQ(part.remove_fetch(4, f.fetch), R::wrong_epoch);

        ASSERT_EQ(part.install_barrier(7), R::ok);
        EXPECT_EQ(
            part.get_or_create_fetch(3, "k", "s").refusal,
            R::barrier_installed);
        EXPECT_EQ(part.remove_fetch(3, f.fetch), R::barrier_installed);

        ASSERT_EQ(part.clear_and_advance(7), R::ok);
        EXPECT_EQ(part.accepted_epoch().value_or(0), 4u);
        EXPECT_EQ(part.fetch_count().value_or(99), 0u)
            << "advance clears index and records together";

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
        strand, 1, std::numeric_limits<FetchId>::max() - 1};
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
        EXPECT_EQ(part.clear_and_advance(1), R::off_strand);
        EXPECT_FALSE(part.accepted_epoch().has_value());
        EXPECT_FALSE(part.fetch_count().has_value());
        EXPECT_FALSE(part.joins_of(1).has_value());
        checked = true;
    });
    sched.run_until_idle();
    EXPECT_TRUE(checked);
    run([&] { EXPECT_EQ(part.fetch_count().value_or(99), 0u); });
}

TEST_F(PartitionFixture, BarrierIdsRemainAMonotonicAcceptanceGate)
{
    run([&] {
        ASSERT_EQ(part.install_barrier(5), R::ok);
        ASSERT_EQ(part.clear_and_advance(5), R::ok);
        EXPECT_EQ(part.install_barrier(5), R::barrier_reused);
        EXPECT_EQ(part.install_barrier(6), R::ok);
        ASSERT_EQ(part.clear_and_advance(6), R::ok);
        EXPECT_EQ(part.accepted_epoch().value_or(0), 5u);
    });
}
