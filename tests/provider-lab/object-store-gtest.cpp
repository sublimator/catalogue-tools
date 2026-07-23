#include "object-store.h"

#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <utility>

using namespace catl::provider_lab;
namespace asio = boost::asio;

namespace {

struct StoreFixture : ::testing::Test
{
    DeterministicScheduler sched;
    DeterministicScheduler::strand_type strand = sched.make_strand();
    EpochGuardedObjectStore store{strand, /*epoch*/ 5, /*capacity*/ 3};

    template <typename Fn>
    void
    run(Fn fn)
    {
        asio::post(strand, std::move(fn));
        sched.run_until_idle();
    }

    static std::pair<ContentHash, std::string>
    value(std::string bytes)
    {
        auto h = lab_content_hash(bytes);
        return {h, std::move(bytes)};
    }
};

using R = EpochGuardedObjectStore::Refusal;

}  // namespace

TEST_F(StoreFixture, EpochCheckedInsertAndLookup)
{
    run([&] {
        auto [h, bytes] = value("node-a");
        EXPECT_EQ(store.insert(5, h, bytes), R::ok);
        auto [r, got] = store.lookup(5, h);
        EXPECT_EQ(r, R::ok);
        ASSERT_TRUE(got.has_value());
        EXPECT_EQ(*got, "node-a");

        // Wrong-epoch commands reject at the owner commit, both verbs.
        EXPECT_EQ(store.insert(4, h, bytes), R::wrong_epoch);
        EXPECT_EQ(store.lookup(6, h).first, R::wrong_epoch);
    });
}

TEST_F(StoreFixture, HashVerificationGuardsEveryInsert)
{
    run([&] {
        auto [h, bytes] = value("node-a");
        EXPECT_EQ(store.insert(5, h + 1, bytes), R::hash_mismatch)
            << "bytes must hash to the claimed key before any mutation";
        EXPECT_EQ(store.size(), 0u);
        EXPECT_EQ(store.insert(5, h, bytes), R::ok);
    });
}

TEST_F(StoreFixture, ImmutableValuesReinsertIdempotently)
{
    run([&] {
        auto [h, bytes] = value("node-a");
        EXPECT_EQ(store.insert(5, h, bytes), R::ok);
        EXPECT_EQ(store.insert(5, h, bytes), R::ok);
        EXPECT_EQ(store.size(), 1u) << "immutable: one row per content";
    });
}

TEST_F(StoreFixture, LruEvictionRespectsLookupRecency)
{
    run([&] {
        auto [ha, a] = value("a");
        auto [hb, b] = value("b");
        auto [hc, c] = value("c");
        auto [hd, d] = value("d");
        ASSERT_EQ(store.insert(5, ha, a), R::ok);
        ASSERT_EQ(store.insert(5, hb, b), R::ok);
        ASSERT_EQ(store.insert(5, hc, c), R::ok);

        // Touch "a" so "b" is now least-recently used.
        ASSERT_TRUE(store.lookup(5, ha).second.has_value());
        ASSERT_EQ(store.insert(5, hd, d), R::ok);

        EXPECT_TRUE(store.lookup(5, ha).second.has_value());
        EXPECT_FALSE(store.lookup(5, hb).second.has_value())
            << "the least-recently-used row is the eviction victim";
        EXPECT_TRUE(store.lookup(5, hc).second.has_value());
        EXPECT_TRUE(store.lookup(5, hd).second.has_value());
    });
}

TEST_F(StoreFixture, BarrierRejectsOldEpochBothOrders)
{
    run([&] {
        auto [h, bytes] = value("node-a");

        // Insert before the barrier lands: accepted.
        EXPECT_EQ(store.insert(5, h, bytes), R::ok);

        // Barrier installed: every old-epoch command now rejects.
        EXPECT_EQ(store.install_barrier(77), R::ok);
        EXPECT_EQ(store.insert(5, h, bytes), R::barrier_installed);
        EXPECT_EQ(store.lookup(5, h).first, R::barrier_installed);

        // A second barrier cannot stack on the first.
        EXPECT_EQ(store.install_barrier(78), R::not_running);
    });
}

TEST_F(StoreFixture, ClearAndAdvanceConsumesExactBarrierExactlyOnce)
{
    run([&] {
        auto [h, bytes] = value("node-a");
        ASSERT_EQ(store.insert(5, h, bytes), R::ok);
        ASSERT_EQ(store.install_barrier(77), R::ok);

        // Negative control: a non-matching BarrierId cannot advance.
        EXPECT_EQ(store.clear_and_advance(78), R::wrong_barrier);

        EXPECT_EQ(store.clear_and_advance(77), R::ok);
        EXPECT_EQ(store.accepted_epoch(), 6u);
        EXPECT_EQ(store.size(), 0u) << "advance clears every value";

        // Exactly-once: the consumed barrier cannot advance again.
        EXPECT_EQ(store.clear_and_advance(77), R::not_running);

        // New-epoch commands are accepted; the old epoch stays dead.
        EXPECT_EQ(store.insert(6, h, bytes), R::ok);
        EXPECT_EQ(store.insert(5, h, bytes), R::wrong_epoch);
    });
}

TEST_F(StoreFixture, StopIsAbsorbingAndWitnessedExactlyOnce)
{
    run([&] {
        auto [h, bytes] = value("node-a");
        ASSERT_EQ(store.insert(5, h, bytes), R::ok);

        auto witness = store.stop();
        ASSERT_TRUE(witness.has_value());
        EXPECT_EQ(witness->final_epoch, 5u);
        EXPECT_EQ(witness->values_at_stop, 1u);
        EXPECT_EQ(witness->total_inserts, 1u);

        EXPECT_FALSE(store.stop().has_value())
            << "the stop witness is emitted exactly once";
        EXPECT_EQ(store.insert(5, h, bytes), R::stopped);
        EXPECT_EQ(store.lookup(5, h).first, R::stopped);
        EXPECT_EQ(store.install_barrier(9), R::stopped);
        EXPECT_EQ(store.clear_and_advance(9), R::stopped)
            << "the stopped tombstone permits rejection only";
    });
}

TEST_F(StoreFixture, OffStrandTouchesAreRefused)
{
    bool checked = false;
    asio::post(sched.io(), [&] {
        auto [h, bytes] = StoreFixture::value("node-a");
        EXPECT_EQ(store.insert(5, h, bytes), R::off_strand);
        EXPECT_EQ(store.lookup(5, h).first, R::off_strand);
        EXPECT_EQ(store.install_barrier(1), R::off_strand);
        EXPECT_EQ(store.clear_and_advance(1), R::off_strand);
        EXPECT_FALSE(store.stop().has_value());
        checked = true;
    });
    sched.run_until_idle();
    EXPECT_TRUE(checked);
    run([&] { EXPECT_EQ(store.size(), 0u); });
}

TEST(ObjectStoreEdge, ZeroCapacityClampsToOneHonestRow)
{
    DeterministicScheduler sched;
    auto strand = sched.make_strand();
    EpochGuardedObjectStore store{strand, 1, /*capacity*/ 0};
    asio::post(strand, [&] {
        auto bytes = std::string{"a"};
        auto h = lab_content_hash(bytes);
        EXPECT_EQ(
            store.insert(1, h, bytes), EpochGuardedObjectStore::Refusal::ok);
        EXPECT_EQ(store.size(), 1u)
            << "capacity clamps to one honest row rather than breaching "
               "a zero bound";
    });
    sched.run_until_idle();
}
