#include "c01-dispatch-cell.h"

#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

#include <limits>
#include <utility>

using namespace catl::provider_lab;
namespace asio = boost::asio;

namespace {

constexpr C01Destination kCell{
    /*provider*/ 11, /*partition*/ 2, /*epoch*/ 9, /*receiver*/ 5};

struct CellFixture : ::testing::Test
{
    DeterministicScheduler sched;
    DeterministicScheduler::strand_type strand = sched.make_strand();
    C01DispatchCell cell{strand, kCell};

    template <typename Fn>
    void
    run(Fn fn)
    {
        asio::post(strand, std::move(fn));
        sched.run_until_idle();
    }

    static C01Command
    command(
        SenderGeneration ns = 1,
        ChannelGeneration gen = 0,
        CommandId cmd = 40,
        PayloadDigest payload = 777,
        C01Destination destination = kCell)
    {
        return C01Command{
            destination,
            C01CommandIdentity{
                /*operation*/ 100, /*interest*/ 200, /*use*/ 300,
                /*pipeline*/ 400, ns, gen, cmd, payload}};
    }
};

using Case = C01DispatchCell::Case;
using Succ = C01DispatchCell::SuccessorCheck;

}  // namespace

TEST_F(CellFixture, FirstCommitMintsFullyBoundReceipt)
{
    run([&] {
        auto c = command();
        auto r = cell.dispatch(c, /*outcome*/ 9);
        ASSERT_EQ(r.dispatch, Case::first_commit);
        ASSERT_TRUE(r.receipt.has_value());
        EXPECT_TRUE(r.receipt->cell() == kCell)
            << "the receipt binds the authenticated destination";
        EXPECT_TRUE(r.receipt->identity() == c.identity);
        EXPECT_EQ(r.receipt->outcome(), 9u);
    });
}

TEST_F(CellFixture, DestinationMismatchesRefuseBeforeAnyStateBothWays)
{
    // Table-driven: each of the four destination fields, varied alone,
    // must refuse with no receipt and no row — and the SAME command
    // must still first-commit on its intended cell.
    struct Variant
    {
        char const* name;
        C01Destination destination;
    };
    Variant const variants[] = {
        {"provider", {12, 2, 9, 5}},
        {"partition", {11, 3, 9, 5}},
        {"epoch", {11, 2, 10, 5}},
        {"receiver", {11, 2, 9, 6}},
    };
    for (auto const& v : variants)
    {
        run([&] {
            auto misrouted = command(1, 0, 40, 777, v.destination);
            auto r = cell.dispatch(misrouted, 1);
            EXPECT_EQ(r.dispatch, Case::wrong_destination_rejected)
                << v.name << " mismatch must refuse";
            EXPECT_FALSE(r.receipt.has_value())
                << v.name << ": a misroute must never mint evidence";
            EXPECT_EQ(cell.row_count().value_or(99), 0u)
                << v.name << ": a misroute must never commit a row";
        });
    }
    // The intended cell accepts the identical identity.
    DeterministicScheduler sched2;
    auto strand2 = sched2.make_strand();
    C01Destination const other{12, 2, 9, 5};
    C01DispatchCell intended{strand2, other};
    asio::post(strand2, [&] {
        auto routed = command(1, 0, 40, 777, other);
        auto r = intended.dispatch(routed, 9);
        EXPECT_EQ(r.dispatch, Case::first_commit)
            << "the same command first-commits where it was addressed";
        EXPECT_TRUE(r.receipt->cell() == other);
    });
    sched2.run_until_idle();
}

TEST_F(CellFixture, ExactReplayReturnsIdenticalReceiptEffectOnce)
{
    run([&] {
        auto c = command();
        auto first = cell.dispatch(c, 9);
        ASSERT_EQ(first.dispatch, Case::first_commit);

        auto replay = cell.dispatch(c, /*different would-be*/ 999);
        ASSERT_EQ(replay.dispatch, Case::exact_replay);
        EXPECT_TRUE(*replay.receipt == *first.receipt)
            << "replay derives the same receipt; the effect ran once";
        EXPECT_EQ(replay.receipt->outcome(), 9u);
        EXPECT_EQ(cell.row_count().value_or(0), 1u);
    });
}

TEST_F(CellFixture, AliasingIdentitiesConflictNeverReplay)
{
    run([&] {
        auto c = command();
        ASSERT_EQ(cell.dispatch(c, 9).dispatch, Case::first_commit);

        auto vary = [&](auto mutate) {
            auto aliased = c;
            mutate(aliased.identity);
            auto r = cell.dispatch(aliased, 1);
            EXPECT_EQ(r.dispatch, Case::conflict_rejected);
            EXPECT_FALSE(r.receipt.has_value());
        };
        vary([](C01CommandIdentity& i) { i.use = 301; });
        vary([](C01CommandIdentity& i) { i.pipeline = 401; });
        vary([](C01CommandIdentity& i) { i.operation = 101; });
        vary([](C01CommandIdentity& i) { i.interest = 201; });
        vary([](C01CommandIdentity& i) { i.payload = 778; });

        auto replay = cell.dispatch(c, 0);
        EXPECT_EQ(replay.dispatch, Case::exact_replay)
            << "the original full binding still replays untouched";
        EXPECT_EQ(replay.receipt->outcome(), 9u);
        EXPECT_EQ(cell.row_count().value_or(0), 1u);
    });
}

TEST_F(CellFixture, StaleRejectsAtAndBelowTheSeededMark)
{
    run([&] {
        C01CellTestSeed::seed_stale_high_water(cell, 1, 3);

        EXPECT_EQ(cell.dispatch(command(1, 3, 40), 1).dispatch,
                  Case::stale_rejected)
            << "at the mark is stale";
        EXPECT_EQ(cell.dispatch(command(1, 2, 41), 1).dispatch,
                  Case::stale_rejected)
            << "below the mark is stale";
        EXPECT_EQ(cell.dispatch(command(1, 4, 42), 1).dispatch,
                  Case::first_commit)
            << "above the mark first-commits";
        // A different sender GENERATION owns an independent mark: a
        // restarted sender starts at the sentinel.
        EXPECT_EQ(cell.dispatch(command(2, 0, 43), 1).dispatch,
                  Case::first_commit);
    });
}

TEST_F(CellFixture, RefusalPrecedenceIsTotalOrder)
{
    run([&] {
        C01CellTestSeed::seed_stale_high_water(cell, 1, 3);

        // Wrong destination beats reserved identity AND staleness.
        auto worst = command(1, 0, /*command*/ 0, 777,
                             C01Destination{12, 2, 9, 5});
        EXPECT_EQ(cell.dispatch(worst, 1).dispatch,
                  Case::wrong_destination_rejected);

        // Destination valid: reserved CommandId 0 rejects BEFORE the
        // high-water read — both-zero-and-stale yields invalid_identity.
        auto zero_and_stale = command(1, 0, 0);
        EXPECT_EQ(cell.dispatch(zero_and_stale, 1).dispatch,
                  Case::invalid_identity);

        // ChannelGeneration 0 stays valid vocabulary on a fresh
        // namespace (the sentinel sits immediately before zero).
        EXPECT_EQ(cell.dispatch(command(3, 0, 60), 1).dispatch,
                  Case::first_commit);
    });
}

TEST(C01SuccessorArithmetic, PureFunctionIncludesExecutedCeiling)
{
    using Cell = C01DispatchCell;
    constexpr auto kMax = std::numeric_limits<ChannelGeneration>::max();

    // Untouched namespace: only generation 0 may close first.
    EXPECT_EQ(Cell::check_close_successor(std::nullopt, 0), Succ::ok);
    EXPECT_EQ(
        Cell::check_close_successor(std::nullopt, 1), Succ::not_successor);
    EXPECT_EQ(
        Cell::check_close_successor(std::nullopt, 100),
        Succ::not_successor);

    // Then exactly current+1; gaps, replays, regressions refuse.
    EXPECT_EQ(Cell::check_close_successor(0, 1), Succ::ok);
    EXPECT_EQ(Cell::check_close_successor(0, 0), Succ::not_successor);
    EXPECT_EQ(Cell::check_close_successor(0, 2), Succ::not_successor);
    EXPECT_EQ(Cell::check_close_successor(41, 42), Succ::ok);
    EXPECT_EQ(Cell::check_close_successor(41, 41), Succ::not_successor);

    // The EXECUTED ceiling refusals: a mark at max has no successor —
    // every candidate, including max itself and a wrap to 0, is
    // generation_exhausted, never ok.
    EXPECT_EQ(
        Cell::check_close_successor(kMax, 0), Succ::generation_exhausted);
    EXPECT_EQ(
        Cell::check_close_successor(kMax, kMax),
        Succ::generation_exhausted);
    EXPECT_EQ(
        Cell::check_close_successor(kMax - 1, kMax), Succ::ok)
        << "the last legal close is max itself";
}

TEST_F(CellFixture, SeededMaxMarkKeepsDispatchStateIntact)
{
    run([&] {
        // A namespace whose mark IS the ceiling (test seed): every
        // generation is stale, rows and marks stay untouched by
        // dispatch refusals.
        ASSERT_EQ(cell.dispatch(command(2, 0, 50), 1).dispatch,
                  Case::first_commit);
        C01CellTestSeed::seed_stale_high_water(
            cell, 1, std::numeric_limits<ChannelGeneration>::max());
        EXPECT_EQ(cell.dispatch(command(1, 0, 40), 1).dispatch,
                  Case::stale_rejected);
        EXPECT_EQ(
            cell.stale_high_water(1).value_or(0),
            std::numeric_limits<ChannelGeneration>::max())
            << "the mark is unchanged";
        EXPECT_EQ(cell.row_count().value_or(0), 1u)
            << "existing rows are unchanged";
    });
}

TEST_F(CellFixture, OffStrandTouchesAreRefusedBeforeAnyRead)
{
    bool checked = false;
    asio::post(sched.io(), [&] {
        auto r = cell.dispatch(command(), 1);
        EXPECT_EQ(r.dispatch, Case::off_strand);
        EXPECT_FALSE(r.receipt.has_value());
        EXPECT_FALSE(cell.row_count().has_value());
        EXPECT_FALSE(cell.stale_high_water(1).has_value());
        checked = true;
    });
    sched.run_until_idle();
    EXPECT_TRUE(checked);
    run([&] { EXPECT_EQ(cell.row_count().value_or(99), 0u); });
}
