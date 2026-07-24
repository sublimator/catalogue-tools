// Increment 3e controls: the C02 generation-close authority on the
// C01 dispatch cell — serialized slot, strict contiguous succession,
// exact reclamation, retained close receipts, and the mid-close seal.

#include "c01-dispatch-cell.h"

#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

#include <limits>
#include <stdexcept>
#include <utility>

using namespace catl::provider_lab;
namespace asio = boost::asio;

namespace {

C01Destination const kCell{1, 2, 3, 4};

struct CloseFixture : ::testing::Test
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

    static C02CloseCommand
    close_cmd(
        SenderGeneration ns,
        ChannelGeneration generation,
        CommandId id = 77,
        C01Destination dest = kCell,
        CloseSequence seq = 1,
        CloseKind kind = 2,
        PayloadDigest payload = 0xC1)
    {
        return C02CloseCommandTestMint::mint(
            dest, ns, generation, seq, kind, id, payload);
    }

    /// Dispatch one command into (ns, generation) with a distinct id.
    C01DispatchCell::Case
    fill_row(SenderGeneration ns, ChannelGeneration g, CommandId id)
    {
        C01Command c{
            kCell,
            C01CommandIdentity{
                .operation = 100,
                .interest = 1,
                .use = id,
                .pipeline = 9,
                .sender_generation = ns,
                .channel_generation = g,
                .command = id,
                .payload = 0xAB}};
        return cell.dispatch(c, /*outcome*/ 7).dispatch;
    }

    /// Run one full close (begin + complete), asserting both commit.
    C02CloseReceipt
    closed(SenderGeneration ns, ChannelGeneration g, CommandId id)
    {
        auto begin = cell.begin_close(close_cmd(ns, g, id));
        EXPECT_EQ(begin.refusal, C01DispatchCell::CloseRefusal::ok);
        auto done = cell.complete_close();
        EXPECT_EQ(done.refusal, C01DispatchCell::CloseRefusal::ok);
        EXPECT_TRUE(done.receipt.has_value());
        return *done.receipt;
    }
};

using CR = C01DispatchCell::CloseRefusal;
using DC = C01DispatchCell::Case;

}  // namespace

TEST_F(CloseFixture, ClosesAreStrictContiguousSuccessors)
{
    run([&] {
        // The first close of a namespace must be generation 0.
        EXPECT_EQ(cell.begin_close(close_cmd(5, 2)).refusal,
                  CR::not_successor)
            << "succession is proven from the mark, never assumed";
        auto r0 = closed(5, 0, 70);
        EXPECT_EQ(r0.generation(), 0u);
        EXPECT_EQ(cell.stale_high_water(5), 0u);

        // Skipping refuses; the exact successor commits.
        EXPECT_EQ(cell.begin_close(close_cmd(5, 2)).refusal,
                  CR::not_successor);
        auto r1 = closed(5, 1, 71);
        EXPECT_EQ(cell.stale_high_water(5), 1u);
        EXPECT_EQ(r1.close_command(), 71u);

        // Namespaces are independent: 6 still starts at 0.
        EXPECT_EQ(cell.begin_close(close_cmd(6, 1)).refusal,
                  CR::not_successor);
        closed(6, 0, 72);
    });
}

TEST_F(CloseFixture, CeilingRefusesTypedNothingMutated)
{
    run([&] {
        // SEEDED state, stated honestly: the mark sits at max with NO
        // retained receipt (only the test seam can produce this — a
        // real max close retains its receipt, so representable inputs
        // classify as replay/conflict/stale BEFORE the pure exhaustion
        // branch). The seed is how the executed exhaustion refusal is
        // reached through the real authority.
        C01CellTestSeed::seed_stale_high_water(
            cell, 5, std::numeric_limits<ChannelGeneration>::max());
        EXPECT_EQ(cell.begin_close(close_cmd(5, 0)).refusal,
                  CR::generation_exhausted)
            << "the wrap class, refused at the close authority";
        EXPECT_FALSE(cell.close_in_flight().value_or(true));
        EXPECT_EQ(
            cell.stale_high_water(5),
            std::numeric_limits<ChannelGeneration>::max());
    });
}

TEST_F(CloseFixture, CeilingGenerationClosesAndReclaimsWithoutWrap)
{
    run([&] {
        auto const g_max = std::numeric_limits<ChannelGeneration>::max();
        // A legitimate close OF generation max: mark at max-1, a live
        // row at (5, max) with the maximal CommandId — the exact
        // range endpoint that motivated upper_bound over
        // generation + 1.
        C01CellTestSeed::seed_stale_high_water(cell, 5, g_max - 1);
        ASSERT_EQ(
            fill_row(5, g_max, std::numeric_limits<CommandId>::max()),
            DC::first_commit);
        ASSERT_EQ(fill_row(6, 0, 13), DC::first_commit);
        ASSERT_EQ(cell.row_count(), 2u);

        auto receipt = closed(5, g_max, 70);
        EXPECT_EQ(receipt.generation(), g_max)
            << "the receipt records the ceiling generation itself";
        EXPECT_EQ(cell.stale_high_water(5), g_max);
        EXPECT_EQ(cell.row_count(), 1u)
            << "exactly the (5, max) row reclaimed — no wrap erased "
               "or spared the wrong range";
        EXPECT_EQ(fill_row(6, 0, 13), DC::exact_replay)
            << "the unrelated namespace survived the ceiling close";

        // Post-ceiling, representable inputs classify via the
        // retained receipt, never the exhaustion branch.
        EXPECT_EQ(cell.begin_close(close_cmd(5, g_max, 70)).refusal,
                  CR::already_closed);
        EXPECT_EQ(cell.begin_close(close_cmd(5, 0, 71)).refusal,
                  CR::stale_close);
    });
}

TEST_F(CloseFixture, TheSlotSerializesCloses)
{
    run([&] {
        ASSERT_EQ(cell.begin_close(close_cmd(5, 0, 70)).refusal, CR::ok);
        EXPECT_TRUE(cell.close_in_flight().value_or(false));
        EXPECT_EQ(cell.begin_close(close_cmd(6, 0, 71)).refusal,
                  CR::slot_occupied)
            << "one close in flight, cell-wide: the slot serializes";
        EXPECT_EQ(cell.complete_close().refusal, CR::ok);
        EXPECT_FALSE(cell.close_in_flight().value_or(true));
        EXPECT_EQ(cell.complete_close().refusal, CR::no_close_in_flight)
            << "completion without a begin is typed";
        ASSERT_EQ(cell.begin_close(close_cmd(6, 0, 71)).refusal, CR::ok);
        EXPECT_EQ(cell.complete_close().refusal, CR::ok);
    });
}

TEST_F(CloseFixture, ReclamationIsExactInBothDirections)
{
    run([&] {
        ASSERT_EQ(fill_row(5, 0, 10), DC::first_commit);
        ASSERT_EQ(fill_row(5, 0, 11), DC::first_commit);
        ASSERT_EQ(fill_row(5, 1, 12), DC::first_commit);
        ASSERT_EQ(fill_row(6, 0, 13), DC::first_commit);
        ASSERT_EQ(cell.row_count(), 4u);

        closed(5, 0, 70);

        EXPECT_EQ(cell.row_count(), 2u)
            << "EXACTLY generation (5,0)'s rows reclaimed: both of "
               "them, and nothing else";
        EXPECT_EQ(fill_row(5, 0, 10), DC::stale_rejected)
            << "the reclaimed generation is below the mark";
        EXPECT_EQ(fill_row(5, 1, 12), DC::exact_replay)
            << "generation 1's row survived intact";
        EXPECT_EQ(fill_row(6, 0, 13), DC::exact_replay)
            << "namespace 6 was untouched";
        EXPECT_FALSE(cell.stale_high_water(6).has_value())
            << "no mark moved for the untouched namespace";
    });
}

TEST_F(CloseFixture, FullBindingGovernsReplayVersusConflict)
{
    run([&] {
        auto receipt = closed(5, 0, 70);

        // T129 at the close channel: at the closed generation, vary
        // each identity field INDEPENDENTLY — sequence, kind, command,
        // payload. Every mismatch conflicts without a receipt, without
        // the slot, without state; only the exact full binding replays.
        struct Variation
        {
            CloseSequence seq;
            CloseKind kind;
            CommandId id;
            PayloadDigest payload;
        };
        for (auto v : {Variation{9, 2, 70, 0xC1},    // sequence differs
                       Variation{1, 9, 70, 0xC1},    // kind differs
                       Variation{1, 2, 71, 0xC1},    // command differs
                       Variation{1, 2, 70, 0xD2}})   // payload differs
        {
            auto r = cell.begin_close(C02CloseCommandTestMint::mint(
                kCell, 5, 0, v.seq, v.kind, v.id, v.payload));
            EXPECT_EQ(r.refusal, CR::close_conflict)
                << "same identity, payload/kind/sequence mismatch is a "
                   "conflict, never collapsed into replay";
            EXPECT_FALSE(r.receipt.has_value());
            EXPECT_FALSE(cell.close_in_flight().value_or(true));
        }

        auto exact = cell.begin_close(close_cmd(5, 0, 70));
        EXPECT_EQ(exact.refusal, CR::already_closed);
        EXPECT_TRUE(*exact.receipt == receipt)
            << "only the exact full binding replays, identically";
    });
}

TEST_F(CloseFixture, CloseReceiptBindsRetainsAndReplays)
{
    run([&] {
        auto receipt = closed(5, 0, 70);
        EXPECT_EQ(receipt.cell(), kCell);
        EXPECT_EQ(receipt.sender_generation(), 5u);
        EXPECT_EQ(receipt.generation(), 0u);
        EXPECT_EQ(receipt.close_sequence(), 1u);
        EXPECT_EQ(receipt.close_kind(), 2u);
        EXPECT_EQ(receipt.close_command(), 70u);
        EXPECT_EQ(receipt.payload(), 0xC1u)
            << "the fresh receipt echoes every identity field";

        // The caller loses the receipt: re-presenting the IDENTICAL
        // close command replays the IDENTICAL retained receipt.
        auto replay = cell.begin_close(close_cmd(5, 0, 70));
        EXPECT_EQ(replay.refusal, CR::already_closed);
        ASSERT_TRUE(replay.receipt.has_value());
        EXPECT_TRUE(*replay.receipt == receipt);
        EXPECT_FALSE(cell.close_in_flight().value_or(true))
            << "replay never occupies the slot";

        // A DIFFERENT close identity claiming the closed generation
        // conflicts without effect or receipt.
        auto conflict = cell.begin_close(close_cmd(5, 0, 71));
        EXPECT_EQ(conflict.refusal, CR::close_conflict);
        EXPECT_FALSE(conflict.receipt.has_value());

        // The successor close DISPLACES the retained receipt: the
        // older generation's home is reclaimed and typed stale.
        closed(5, 1, 72);
        EXPECT_EQ(cell.begin_close(close_cmd(5, 0, 70)).refusal,
                  CR::stale_close)
            << "receipt retention is bounded to the latest close per "
               "namespace; the sender's serialized close pipeline "
               "consumes each receipt before the next close";
    });
}

TEST_F(CloseFixture, MidCloseSealsExactlyTheClosingPrefix)
{
    run([&] {
        ASSERT_EQ(fill_row(5, 1, 20), DC::first_commit);
        ASSERT_EQ(cell.begin_close(close_cmd(5, 0, 70)).refusal, CR::ok);

        // The closing prefix (<= 0 in namespace 5) is sealed: no
        // first-commit can race the imminent reclamation.
        EXPECT_EQ(fill_row(5, 0, 21), DC::closing_rejected);
        EXPECT_EQ(cell.row_count(), 1u) << "the seal committed nothing";
        // Above the closing generation and other namespaces proceed.
        EXPECT_EQ(fill_row(5, 1, 20), DC::exact_replay);
        EXPECT_EQ(fill_row(5, 2, 22), DC::first_commit);
        EXPECT_EQ(fill_row(6, 0, 23), DC::first_commit);

        ASSERT_EQ(cell.complete_close().refusal, CR::ok);
        EXPECT_EQ(fill_row(5, 0, 21), DC::stale_rejected)
            << "after completion the seal is the mark itself";
    });
}

TEST_F(CloseFixture, WrongDestinationAndReservedIdentityRefuseFirst)
{
    run([&] {
        for (auto dest : {C01Destination{9, 2, 3, 4},
                          C01Destination{1, 9, 3, 4},
                          C01Destination{1, 2, 9, 4},
                          C01Destination{1, 2, 3, 9}})
        {
            EXPECT_EQ(
                cell.begin_close(close_cmd(5, 0, 70, dest)).refusal,
                CR::wrong_destination)
                << "a close that did not name THIS cell learns nothing";
        }
        EXPECT_EQ(cell.begin_close(close_cmd(5, 0, /*id*/ 0)).refusal,
                  CR::invalid_identity)
            << "close-command id 0 is reserved";
        EXPECT_FALSE(cell.close_in_flight().value_or(true));
        // The cell is untouched: the legitimate close still starts at 0.
        EXPECT_EQ(cell.begin_close(close_cmd(5, 0, 70)).refusal, CR::ok);
    });
}

TEST_F(CloseFixture, CompletionIsExceptionAtomicAtTheFailpoint)
{
    run([&] {
        ASSERT_EQ(fill_row(5, 0, 10), DC::first_commit);
        ASSERT_EQ(cell.begin_close(close_cmd(5, 0, 70)).refusal, CR::ok);

        cell.test_complete_pre_commit = [] {
            throw std::runtime_error{"injected"};
        };
        EXPECT_THROW(cell.complete_close(), std::runtime_error);
        cell.test_complete_pre_commit = nullptr;

        EXPECT_FALSE(cell.stale_high_water(5).has_value())
            << "the mark did not move";
        EXPECT_EQ(cell.row_count(), 1u) << "no row was reclaimed";
        EXPECT_TRUE(cell.close_in_flight().value_or(false))
            << "the slot is still occupied: the close is still owed";
        EXPECT_EQ(
            cell.begin_close(close_cmd(5, 0, 70)).refusal,
            CR::slot_occupied)
            << "no second authority can sneak in behind the failure";

        // The retry completes identically.
        auto done = cell.complete_close();
        ASSERT_EQ(done.refusal, CR::ok);
        EXPECT_EQ(done.receipt->generation(), 0u);
        EXPECT_EQ(cell.stale_high_water(5), 0u);
        EXPECT_EQ(cell.row_count(), 0u);
    });
}

TEST_F(CloseFixture, OffStrandCloseTouchesAreRefusedBeforeAnyRead)
{
    run([&] {
        ASSERT_EQ(fill_row(5, 0, 10), DC::first_commit);
    });

    bool checked = false;
    asio::post(sched.io(), [&] {
        EXPECT_EQ(cell.begin_close(close_cmd(5, 0, 70)).refusal,
                  CR::off_strand);
        EXPECT_EQ(cell.complete_close().refusal, CR::off_strand);
        EXPECT_FALSE(cell.close_in_flight().has_value());
        EXPECT_FALSE(cell.row_count().has_value());
        checked = true;
    });
    sched.run_until_idle();
    EXPECT_TRUE(checked);

    // The refusals read nothing and occupied nothing.
    run([&] {
        EXPECT_EQ(cell.begin_close(close_cmd(5, 0, 70)).refusal, CR::ok);
        EXPECT_EQ(cell.complete_close().refusal, CR::ok);
    });
}
