#include "c01-dispatch-cell.h"

#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

#include <utility>

using namespace catl::provider_lab;
namespace asio = boost::asio;

namespace {

struct CellFixture : ::testing::Test
{
    DeterministicScheduler sched;
    DeterministicScheduler::strand_type strand = sched.make_strand();
    C01DispatchCell cell{strand, /*receiver generation*/ 5};

    template <typename Fn>
    void
    run(Fn fn)
    {
        asio::post(strand, std::move(fn));
        sched.run_until_idle();
    }
};

using Case = C01DispatchCell::Case;
using HW = C01DispatchCell::HighWaterResult;

}  // namespace

TEST_F(CellFixture, FirstCommitMintsAndRetainsBoundedRow)
{
    run([&] {
        auto r = cell.dispatch(5, /*sender*/ 1, /*gen*/ 3, /*cmd*/ 40,
                               /*payload*/ 777, /*outcome*/ 9);
        ASSERT_EQ(r.dispatch, Case::first_commit);
        ASSERT_TRUE(r.receipt.has_value());
        EXPECT_EQ(r.receipt->receiver(), 5u);
        EXPECT_EQ(r.receipt->sender(), 1u);
        EXPECT_EQ(r.receipt->channel_generation(), 3u);
        EXPECT_EQ(r.receipt->command(), 40u);
        EXPECT_EQ(r.receipt->outcome(), 9u);
        EXPECT_EQ(cell.row_count().value_or(0), 1u);
    });
}

TEST_F(CellFixture, ExactReplayReturnsIdenticalReceiptEffectOnce)
{
    run([&] {
        auto first = cell.dispatch(5, 1, 3, 40, 777, 9);
        ASSERT_EQ(first.dispatch, Case::first_commit);

        // The replay arrives with a DIFFERENT would-be outcome: the
        // effect must not repeat — the retained receipt is returned
        // IDENTICAL, outcome untouched.
        auto replay = cell.dispatch(5, 1, 3, 40, 777, /*outcome*/ 999);
        ASSERT_EQ(replay.dispatch, Case::exact_replay);
        ASSERT_TRUE(replay.receipt.has_value());
        EXPECT_TRUE(*replay.receipt == *first.receipt)
            << "replay derives the same receipt; the effect ran once";
        EXPECT_EQ(replay.receipt->outcome(), 9u);
        EXPECT_EQ(cell.row_count().value_or(0), 1u);
    });
}

TEST_F(CellFixture, ConflictHasNoEffectAndNoReceipt)
{
    run([&] {
        auto first = cell.dispatch(5, 1, 3, 40, 777, 9);
        ASSERT_EQ(first.dispatch, Case::first_commit);

        // Same identity, different payload: rejected without effect,
        // without receipt, row intact with the original binding.
        auto conflict = cell.dispatch(5, 1, 3, 40, /*payload*/ 778, 1);
        EXPECT_EQ(conflict.dispatch, Case::conflict_rejected);
        EXPECT_FALSE(conflict.receipt.has_value());

        auto replay = cell.dispatch(5, 1, 3, 40, 777, 0);
        EXPECT_EQ(replay.dispatch, Case::exact_replay)
            << "the original payload binding survives the conflict";
    });
}

TEST_F(CellFixture, StaleRejectsAtAndBelowTheHighWater)
{
    run([&] {
        ASSERT_EQ(cell.advance_stale_high_water(1, 3), HW::ok);

        EXPECT_EQ(cell.dispatch(5, 1, 3, 40, 7, 1).dispatch,
                  Case::stale_rejected)
            << "at the mark is stale";
        EXPECT_EQ(cell.dispatch(5, 1, 2, 41, 7, 1).dispatch,
                  Case::stale_rejected)
            << "below the mark is stale";
        EXPECT_EQ(cell.dispatch(5, 1, 4, 42, 7, 1).dispatch,
                  Case::first_commit)
            << "above the mark first-commits";
    });
}

TEST_F(CellFixture, WrongReceiverDominatesEveryOtherCase)
{
    run([&] {
        ASSERT_EQ(cell.advance_stale_high_water(1, 3), HW::ok);
        ASSERT_EQ(cell.dispatch(5, 1, 4, 40, 777, 9).dispatch,
                  Case::first_commit);

        // Inputs that are ALSO stale, ALSO a conflict, ALSO a replay:
        // the wrong receiver tag must win every pairing, before any
        // reclaimable state is consulted.
        EXPECT_EQ(cell.dispatch(4, 1, 2, 41, 7, 1).dispatch,
                  Case::wrong_receiver_rejected)
            << "wrong receiver beats stale";
        EXPECT_EQ(cell.dispatch(4, 1, 4, 40, 778, 1).dispatch,
                  Case::wrong_receiver_rejected)
            << "wrong receiver beats conflict";
        EXPECT_EQ(cell.dispatch(4, 1, 4, 40, 777, 1).dispatch,
                  Case::wrong_receiver_rejected)
            << "wrong receiver beats replay";
        EXPECT_EQ(cell.row_count().value_or(0), 1u);
    });
}

TEST_F(CellFixture, HighWaterIsAMonotonicAcceptanceGatePerSender)
{
    run([&] {
        ASSERT_EQ(cell.advance_stale_high_water(1, 5), HW::ok);
        EXPECT_EQ(cell.advance_stale_high_water(1, 5), HW::not_monotonic);
        EXPECT_EQ(cell.advance_stale_high_water(1, 4), HW::not_monotonic);
        EXPECT_EQ(cell.advance_stale_high_water(1, 6), HW::ok);

        // Sender namespaces are independent: sender 2 is untouched by
        // sender 1's closes.
        EXPECT_FALSE(cell.stale_high_water(2).has_value())
            << "sentinel: nothing closed for this sender yet";
        EXPECT_EQ(cell.dispatch(5, 2, 1, 40, 7, 1).dispatch,
                  Case::first_commit)
            << "generation 1 is live for a fresh sender namespace";
    });
}

TEST_F(CellFixture, AdvanceReclaimsExactlyTheClosedRows)
{
    run([&] {
        ASSERT_EQ(cell.dispatch(5, 1, 1, 40, 7, 1).dispatch,
                  Case::first_commit);
        ASSERT_EQ(cell.dispatch(5, 1, 2, 41, 7, 1).dispatch,
                  Case::first_commit);
        ASSERT_EQ(cell.dispatch(5, 1, 3, 42, 7, 1).dispatch,
                  Case::first_commit);
        ASSERT_EQ(cell.dispatch(5, 2, 1, 43, 7, 1).dispatch,
                  Case::first_commit);
        ASSERT_EQ(cell.row_count().value_or(0), 4u);

        ASSERT_EQ(cell.advance_stale_high_water(1, 2), HW::ok);
        EXPECT_EQ(cell.row_count().value_or(0), 2u)
            << "exactly sender 1's generations <= 2 are reclaimed";

        // The reclaimed identity is now stale, not replayable: the
        // high-water refuses BEFORE inbox absence could first-commit.
        EXPECT_EQ(cell.dispatch(5, 1, 1, 40, 7, 1).dispatch,
                  Case::stale_rejected);
        // Sender 2's row survived.
        EXPECT_EQ(cell.dispatch(5, 2, 1, 43, 7, 0).dispatch,
                  Case::exact_replay);
    });
}

TEST_F(CellFixture, OffStrandTouchesAreRefusedBeforeAnyRead)
{
    bool checked = false;
    asio::post(sched.io(), [&] {
        auto r = cell.dispatch(5, 1, 3, 40, 7, 1);
        EXPECT_EQ(r.dispatch, Case::off_strand);
        EXPECT_FALSE(r.receipt.has_value());
        EXPECT_EQ(cell.advance_stale_high_water(1, 9), HW::off_strand);
        EXPECT_FALSE(cell.row_count().has_value());
        EXPECT_FALSE(cell.stale_high_water(1).has_value());
        checked = true;
    });
    sched.run_until_idle();
    EXPECT_TRUE(checked);
    run([&] { EXPECT_EQ(cell.row_count().value_or(99), 0u); });
}
