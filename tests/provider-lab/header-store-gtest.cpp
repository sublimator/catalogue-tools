#include "header-store.h"

#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <utility>

using namespace catl::provider_lab;
namespace asio = boost::asio;

namespace {

struct HeaderFixture : ::testing::Test
{
    DeterministicScheduler sched;
    DeterministicScheduler::strand_type strand = sched.make_strand();
    EpochGuardedHeaderStore store{strand, /*epoch*/ 2, /*capacity*/ 3};

    template <typename Fn>
    void
    run(Fn fn)
    {
        asio::post(strand, std::move(fn));
        sched.run_until_idle();
    }

    /// A lab header claiming `seq` with distinguishing content.
    static std::pair<ContentHash, std::string>
    header(Sequence seq, std::string salt)
    {
        auto bytes = std::to_string(seq) + ":" + std::move(salt);
        auto h = lab_content_hash(bytes);
        return {h, std::move(bytes)};
    }
};

using R = EpochGuardedHeaderStore::Refusal;

}  // namespace

TEST_F(HeaderFixture, CompositeKeyInsertAndExactLookup)
{
    run([&] {
        auto [h, bytes] = header(100, "a");
        EXPECT_EQ(store.insert(2, 100, h, bytes), R::ok);
        auto [r, got] = store.lookup(2, 100, h);
        EXPECT_EQ(r, R::ok);
        ASSERT_TRUE(got.has_value());
        EXPECT_EQ(*got, bytes);

        // The same hash under a different sequence is a different key
        // and (here) a sequence mismatch at insert.
        EXPECT_EQ(store.insert(2, 101, h, bytes), R::sequence_mismatch);
        EXPECT_FALSE(store.lookup(2, 101, h).second.has_value());
    });
}

TEST_F(HeaderFixture, InternalSequenceAgreementGuardsInsert)
{
    run([&] {
        // Bytes internally claim 100; the caller claims 200.
        auto [h, bytes] = header(100, "a");
        EXPECT_EQ(store.insert(2, 200, h, bytes), R::sequence_mismatch)
            << "internal/claimed sequence agreement is checked before "
               "any mutation";
        EXPECT_EQ(store.size().value_or(999), 0u);

        // Unparseable internal sequence is likewise refused.
        auto junk = std::string{"nocolonhere"};
        EXPECT_EQ(
            store.insert(2, 100, lab_content_hash(junk), junk),
            R::sequence_mismatch);
    });
}

TEST_F(HeaderFixture, CandidatesCoexistAndAreNeverSelected)
{
    run([&] {
        // Two fork candidates for one sequence: both live in the store.
        auto [ha, a] = header(100, "candidate-a");
        auto [hb, b] = header(100, "candidate-b");
        ASSERT_EQ(store.insert(2, 100, ha, a), R::ok);
        ASSERT_EQ(store.insert(2, 100, hb, b), R::ok);

        auto [r, hashes] = store.candidates(2, 100);
        EXPECT_EQ(r, R::ok);
        ASSERT_EQ(hashes.size(), 2u)
            << "the store returns EVERY candidate and never picks — "
               "selection cannot bypass ExpectedHeaderRef";
        EXPECT_TRUE(
            std::find(hashes.begin(), hashes.end(), ha) != hashes.end());
        EXPECT_TRUE(
            std::find(hashes.begin(), hashes.end(), hb) != hashes.end());

        // An unknown sequence yields an empty candidate set, not an
        // error: absence is a fact, not a failure.
        auto [r2, none] = store.candidates(2, 999);
        EXPECT_EQ(r2, R::ok);
        EXPECT_TRUE(none.empty());
    });
}

TEST_F(HeaderFixture, EvictionKeepsSequenceIndexExactlyConsistent)
{
    run([&] {
        auto [ha, a] = header(100, "a");
        auto [hb, b] = header(100, "b");
        auto [hc, c] = header(200, "c");
        auto [hd, d] = header(300, "d");
        ASSERT_EQ(store.insert(2, 100, ha, a), R::ok);
        ASSERT_EQ(store.insert(2, 100, hb, b), R::ok);
        ASSERT_EQ(store.insert(2, 200, hc, c), R::ok);

        // Touch both seq-100 candidates so seq-200 is LRU, then insert
        // a fourth value to force one eviction.
        ASSERT_TRUE(store.lookup(2, 100, ha).second.has_value());
        ASSERT_TRUE(store.lookup(2, 100, hb).second.has_value());
        ASSERT_EQ(store.insert(2, 300, hd, d), R::ok);

        EXPECT_TRUE(store.candidates(2, 200).second.empty())
            << "the evicted victim must leave the secondary index";
        EXPECT_EQ(store.candidates(2, 100).second.size(), 2u);
        EXPECT_EQ(store.candidates(2, 300).second.size(), 1u);
    });
}

TEST_F(HeaderFixture, SharedControlMachineGovernsBothVerbs)
{
    run([&] {
        auto [h, bytes] = header(100, "a");
        ASSERT_EQ(store.insert(2, 100, h, bytes), R::ok);

        EXPECT_EQ(store.insert(1, 100, h, bytes), R::wrong_epoch);
        EXPECT_EQ(store.candidates(3, 100).first, R::wrong_epoch);

        ASSERT_EQ(store.install_barrier(9), R::ok);
        EXPECT_EQ(store.insert(2, 100, h, bytes), R::barrier_installed);
        EXPECT_EQ(store.candidates(2, 100).first, R::barrier_installed);
        EXPECT_EQ(store.clear_and_advance(8), R::wrong_barrier);

        ASSERT_EQ(store.clear_and_advance(9), R::ok);
        EXPECT_EQ(store.accepted_epoch().value_or(0), 3u);
        EXPECT_EQ(store.size().value_or(999), 0u);
        EXPECT_TRUE(store.candidates(3, 100).second.empty())
            << "advance clears the secondary index with the values";
        EXPECT_EQ(store.clear_and_advance(9), R::not_running)
            << "the consumed barrier cannot advance twice";
    });
}

TEST_F(HeaderFixture, StopIsAbsorbingWithOneWitness)
{
    run([&] {
        auto [h, bytes] = header(100, "a");
        ASSERT_EQ(store.insert(2, 100, h, bytes), R::ok);
        auto witness = store.stop();
        ASSERT_TRUE(witness.has_value());
        EXPECT_EQ(witness->final_epoch, 2u);
        EXPECT_EQ(witness->values_at_stop, 1u);
        EXPECT_FALSE(store.stop().has_value());
        EXPECT_EQ(store.insert(2, 100, h, bytes), R::stopped);
        EXPECT_EQ(store.candidates(2, 100).first, R::stopped);
    });
}

TEST_F(HeaderFixture, OffStrandTouchesAreRefusedBeforeAnyRead)
{
    bool checked = false;
    asio::post(sched.io(), [&] {
        auto [h, bytes] = HeaderFixture::header(100, "a");
        EXPECT_EQ(store.insert(2, 100, h, bytes), R::off_strand);
        EXPECT_EQ(store.lookup(2, 100, h).first, R::off_strand);
        EXPECT_EQ(store.candidates(2, 100).first, R::off_strand);
        EXPECT_FALSE(store.accepted_epoch().has_value());
        EXPECT_FALSE(store.size().has_value())
            << "off-strand size is refused, never a zero sentinel";
        EXPECT_FALSE(store.stop().has_value());
        checked = true;
    });
    sched.run_until_idle();
    EXPECT_TRUE(checked);
    run([&] { EXPECT_EQ(store.size().value_or(999), 0u); });
}

TEST_F(HeaderFixture, ImmutableReinsertIsIdempotentAcrossTheIndex)
{
    run([&] {
        auto [h, bytes] = header(100, "a");
        ASSERT_EQ(store.insert(2, 100, h, bytes), R::ok);
        ASSERT_EQ(store.insert(2, 100, h, bytes), R::ok);
        EXPECT_EQ(store.size().value_or(0), 1u);
        EXPECT_EQ(store.candidates(2, 100).second.size(), 1u)
            << "re-insert must not duplicate the index entry";
    });
}

TEST(HeaderStoreEdge, OverflowingInternalSequenceIsRefusedNotWrapped)
{
    DeterministicScheduler sched;
    auto strand = sched.make_strand();
    EpochGuardedHeaderStore store{strand, 1, 3};
    asio::post(strand, [&] {
        // 2^32 wraps to 0 under naive parsing; a claim of 0 would then
        // falsely agree. The parse must refuse instead.
        auto bytes = std::string{"4294967296:x"};
        auto h = lab_content_hash(bytes);
        EXPECT_EQ(
            store.insert(1, 0, h, bytes),
            EpochGuardedHeaderStore::Refusal::sequence_mismatch)
            << "overflow must refuse, not wrap into false agreement";
    });
    sched.run_until_idle();
}
