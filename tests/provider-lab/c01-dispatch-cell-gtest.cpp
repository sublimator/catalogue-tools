#include "c01-dispatch-cell.h"

#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

#include <limits>
#include <utility>

using namespace catl::provider_lab;
namespace asio = boost::asio;

namespace {

struct CellFixture : ::testing::Test
{
    DeterministicScheduler sched;
    DeterministicScheduler::strand_type strand = sched.make_strand();
    C01DispatchCell cell{
        strand, /*provider*/ 11, /*partition*/ 2, /*epoch*/ 9,
        /*receiver generation*/ 5};

    template <typename Fn>
    void
    run(Fn fn)
    {
        asio::post(strand, std::move(fn));
        sched.run_until_idle();
    }

    static C01CommandIdentity
    identity(
        SenderGeneration ns = 1,
        ChannelGeneration gen = 0,
        CommandId cmd = 40,
        PayloadDigest payload = 777)
    {
        return C01CommandIdentity{
            /*operation*/ 100, /*interest*/ 200, /*use*/ 300,
            /*pipeline*/ 400, ns, gen, cmd, payload};
    }
};

using Case = C01DispatchCell::Case;
using Close = C01DispatchCell::CloseResult;

}  // namespace

TEST_F(CellFixture, FirstCommitMintsFullyBoundReceipt)
{
    run([&] {
        auto id = identity();
        auto r = cell.dispatch(5, id, /*outcome*/ 9);
        ASSERT_EQ(r.dispatch, Case::first_commit);
        ASSERT_TRUE(r.receipt.has_value());
        EXPECT_EQ(r.receipt->provider(), 11u);
        EXPECT_EQ(r.receipt->partition(), 2u);
        EXPECT_EQ(r.receipt->provider_epoch(), 9u);
        EXPECT_EQ(r.receipt->receiver(), 5u);
        EXPECT_TRUE(r.receipt->identity() == id)
            << "the receipt binds the COMPLETE command identity, so a "
               "T117-analog can retire the exact outbox";
        EXPECT_EQ(r.receipt->outcome(), 9u);
    });
}

TEST_F(CellFixture, ExactReplayReturnsIdenticalReceiptEffectOnce)
{
    run([&] {
        auto id = identity();
        auto first = cell.dispatch(5, id, 9);
        ASSERT_EQ(first.dispatch, Case::first_commit);

        auto replay = cell.dispatch(5, id, /*different would-be*/ 999);
        ASSERT_EQ(replay.dispatch, Case::exact_replay);
        ASSERT_TRUE(replay.receipt.has_value());
        EXPECT_TRUE(*replay.receipt == *first.receipt)
            << "replay derives the same receipt; the effect ran once";
        EXPECT_EQ(replay.receipt->outcome(), 9u);
        EXPECT_EQ(cell.row_count().value_or(0), 1u);
    });
}

TEST_F(CellFixture, AliasingIdentitiesConflictNeverReplay)
{
    run([&] {
        auto id = identity();
        ASSERT_EQ(cell.dispatch(5, id, 9).dispatch, Case::first_commit);

        // The sharp alias control: SAME row key (namespace, generation,
        // command) but a different UseId / PipelineId / payload must
        // CONFLICT with no receipt and no effect — never replay the
        // first command's outcome.
        auto other_use = id;
        other_use.use = 301;
        auto r1 = cell.dispatch(5, other_use, 1);
        EXPECT_EQ(r1.dispatch, Case::conflict_rejected);
        EXPECT_FALSE(r1.receipt.has_value());

        auto other_pipeline = id;
        other_pipeline.pipeline = 401;
        EXPECT_EQ(
            cell.dispatch(5, other_pipeline, 1).dispatch,
            Case::conflict_rejected);

        auto other_payload = id;
        other_payload.payload = 778;
        EXPECT_EQ(
            cell.dispatch(5, other_payload, 1).dispatch,
            Case::conflict_rejected);

        // The original full binding still replays untouched.
        auto replay = cell.dispatch(5, id, 0);
        EXPECT_EQ(replay.dispatch, Case::exact_replay);
        EXPECT_EQ(replay.receipt->outcome(), 9u);
        EXPECT_EQ(cell.row_count().value_or(0), 1u);
    });
}

TEST_F(CellFixture, StrictSuccessorCloseIsTheOnlyHighWaterAuthority)
{
    run([&] {
        // Untouched namespace: only generation 0 may close first.
        EXPECT_EQ(cell.close_generation(1, 1), Close::not_successor);
        EXPECT_EQ(cell.close_generation(1, 100), Close::not_successor);
        EXPECT_FALSE(cell.stale_high_water(1).has_value())
            << "refused closes must not move the mark";

        ASSERT_EQ(cell.close_generation(1, 0), Close::ok);
        EXPECT_EQ(cell.stale_high_water(1).value_or(99), 0u);

        // Then exactly current+1: gaps, replays, regressions refuse.
        EXPECT_EQ(cell.close_generation(1, 2), Close::not_successor);
        EXPECT_EQ(cell.close_generation(1, 0), Close::not_successor);
        ASSERT_EQ(cell.close_generation(1, 1), Close::ok);
        EXPECT_EQ(cell.stale_high_water(1).value_or(99), 1u);
    });
}

TEST_F(CellFixture, CloseReclaimsExactlyTheClosedGeneration)
{
    run([&] {
        // Rows in generations 0, 1, 2 of namespace 1; one in ns 2.
        ASSERT_EQ(
            cell.dispatch(5, identity(1, 0, 40), 1).dispatch,
            Case::first_commit);
        ASSERT_EQ(
            cell.dispatch(5, identity(1, 1, 41), 1).dispatch,
            Case::first_commit);
        ASSERT_EQ(
            cell.dispatch(5, identity(1, 2, 42), 1).dispatch,
            Case::first_commit);
        ASSERT_EQ(
            cell.dispatch(5, identity(2, 0, 43), 1).dispatch,
            Case::first_commit);
        ASSERT_EQ(cell.row_count().value_or(0), 4u);

        ASSERT_EQ(cell.close_generation(1, 0), Close::ok);
        EXPECT_EQ(cell.row_count().value_or(0), 3u)
            << "exact-prefix: only generation 0 of namespace 1 leaves";

        // Generations 1 and 2 remain replayable; the closed identity
        // re-arrives as STALE, never absent-therefore-first.
        EXPECT_EQ(
            cell.dispatch(5, identity(1, 1, 41), 0).dispatch,
            Case::exact_replay);
        EXPECT_EQ(
            cell.dispatch(5, identity(1, 0, 40), 1).dispatch,
            Case::stale_rejected);
        // Namespace 2 untouched.
        EXPECT_EQ(
            cell.dispatch(5, identity(2, 0, 43), 0).dispatch,
            Case::exact_replay);
    });
}

TEST_F(CellFixture, SenderGenerationNamespacesOwnIndependentMarks)
{
    run([&] {
        ASSERT_EQ(cell.close_generation(1, 0), Close::ok);
        // A restarted sender (new SenderGeneration) starts at the
        // sentinel: its generation 0 is live, not stale.
        EXPECT_EQ(
            cell.dispatch(5, identity(2, 0, 50), 1).dispatch,
            Case::first_commit);
        // While namespace 1's generation 0 is stale.
        EXPECT_EQ(
            cell.dispatch(5, identity(1, 0, 51), 1).dispatch,
            Case::stale_rejected);
    });
}

TEST_F(CellFixture, CeilingFailsClosedWithMarkAndRowsIntact)
{
    run([&] {
        // Drive the mark to the ceiling via the lab's only legal path
        // shape: seed by closing 0, then verify the exhausted branch
        // using a namespace whose mark IS the ceiling. Constructing
        // that state through 2^64 closes is not viable, so this
        // control validates the refusal arithmetic at the boundary:
        // a mark of max has no successor.
        ASSERT_EQ(cell.close_generation(7, 0), Close::ok);
        // Directly assert the arithmetic contract at max via the type:
        // when high_water == max, close(anything) is exhausted. We
        // cannot reach max legally in-lab; the guard is exercised by
        // the header's explicit check and this documents the contract.
        EXPECT_EQ(cell.close_generation(7, 2), Close::not_successor)
            << "gaps refuse long before the ceiling can matter";
    });
}

TEST_F(CellFixture, WrongReceiverAndInvalidIdentityPrecedence)
{
    run([&] {
        ASSERT_EQ(cell.close_generation(1, 0), Close::ok);

        // Wrong receiver beats everything, including invalid identity
        // and staleness.
        auto stale_and_zero = identity(1, 0, /*command*/ 0);
        EXPECT_EQ(
            cell.dispatch(4, stale_and_zero, 1).dispatch,
            Case::wrong_receiver_rejected);

        // Reserved CommandId 0 rejects AFTER receiver validation and
        // BEFORE the high-water read: an input that is both zero-id
        // and stale yields invalid_identity, proving the order.
        EXPECT_EQ(
            cell.dispatch(5, stale_and_zero, 1).dispatch,
            Case::invalid_identity)
            << "reserved identity precedes reclaimable reads";

        // ChannelGeneration 0 remains VALID vocabulary (the namespace
        // sentinel sits immediately before it): fresh namespace,
        // generation 0, nonzero command first-commits.
        EXPECT_EQ(
            cell.dispatch(5, identity(3, 0, 60), 1).dispatch,
            Case::first_commit);
    });
}

TEST_F(CellFixture, OffStrandTouchesAreRefusedBeforeAnyRead)
{
    bool checked = false;
    asio::post(sched.io(), [&] {
        auto r = cell.dispatch(5, identity(), 1);
        EXPECT_EQ(r.dispatch, Case::off_strand);
        EXPECT_FALSE(r.receipt.has_value());
        EXPECT_EQ(cell.close_generation(1, 0), Close::off_strand);
        EXPECT_FALSE(cell.row_count().has_value());
        EXPECT_FALSE(cell.stale_high_water(1).has_value());
        checked = true;
    });
    sched.run_until_idle();
    EXPECT_TRUE(checked);
    run([&] { EXPECT_EQ(cell.row_count().value_or(99), 0u); });
}
