#include "operation-ledger.h"

#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

#include <functional>
#include <optional>
#include <utility>

using namespace catl::provider_lab;
namespace asio = boost::asio;

namespace {

/// Run fn on the ledger's owner strand and drain to idle.
template <typename Sched, typename Fn>
void
on_strand(Sched& sched, DeterministicScheduler::strand_type& strand, Fn fn)
{
    asio::post(strand, std::move(fn));
    sched.run_until_idle();
}

struct LedgerFixture : ::testing::Test
{
    DeterministicScheduler sched;
    DeterministicScheduler::strand_type strand = sched.make_strand();
    OperationLedger ledger{strand};

    template <typename Fn>
    void
    run(Fn fn)
    {
        on_strand(sched, strand, std::move(fn));
    }
};

}  // namespace

TEST_F(LedgerFixture, ChildRegistersBeforeClosingAndRefusesAfter)
{
    run([&] {
        auto root = ledger.open_root();
        ASSERT_TRUE(root);
        auto [r1, child] = ledger.register_child(*root);
        EXPECT_EQ(r1, OperationLedger::Refusal::ok);
        EXPECT_TRUE(child.valid());

        EXPECT_EQ(
            ledger.begin_close(*root), OperationLedger::Refusal::ok);
        auto [r2, refused] = ledger.register_child(*root);
        EXPECT_EQ(r2, OperationLedger::Refusal::root_closing)
            << "registration must reject after the root enters closing";
        EXPECT_FALSE(refused.valid());
    });
}

TEST_F(LedgerFixture, PublicationCellFlipsExactlyOnce)
{
    run([&] {
        auto root = ledger.open_root();
        auto [r, child] = ledger.register_child(*root);
        ASSERT_EQ(r, OperationLedger::Refusal::ok);

        EXPECT_EQ(
            ledger.publish(child, /*dest*/ 7, /*work*/ 42),
            OperationLedger::Refusal::ok);
        EXPECT_EQ(
            ledger.publish(child, 8, 43),
            OperationLedger::Refusal::already_flipped)
            << "the publication cell is sealed after the first flip";
        EXPECT_EQ(
            ledger.mark_never_published(child),
            OperationLedger::Refusal::already_flipped)
            << "never_published cannot overwrite published";
    });
}

TEST_F(LedgerFixture, NeverPublishedRemovesViaOneSealedAck)
{
    run([&] {
        auto root = ledger.open_root();
        auto const op = root->id();
        auto [r, child] = ledger.register_child(*root);
        ASSERT_EQ(r, OperationLedger::Refusal::ok);
        auto const cid = child.id();

        EXPECT_EQ(
            ledger.mark_never_published(child),
            OperationLedger::Refusal::ok);
        EXPECT_EQ(
            ledger.ack_terminal(cid, 7, 42),
            OperationLedger::AckOutcome::unpublished)
            << "a terminal ack cannot substitute for the sealed "
               "never-published ack";
        EXPECT_EQ(
            ledger.ack_never_published(cid),
            OperationLedger::AckOutcome::removed);
        EXPECT_EQ(ledger.child_count(op), 0u);
        EXPECT_EQ(
            ledger.ack_never_published(cid),
            OperationLedger::AckOutcome::duplicate_or_late)
            << "the sealed ack is exactly-once";
    });
}

TEST_F(LedgerFixture, OnlyMatchingDestinationAckWinsBothOrders)
{
    // Competing terminality requesters: wrong destination, wrong work,
    // and the exact match. The exact match must win exactly once,
    // whichever order the competitors arrive in.
    for (bool wrong_first : {true, false})
    {
        DeterministicScheduler s2;
        auto strand2 = s2.make_strand();
        OperationLedger l2{strand2};
        on_strand(s2, strand2, [&] {
            auto root = l2.open_root();
            auto [r, child] = l2.register_child(*root);
            ASSERT_EQ(r, OperationLedger::Refusal::ok);
            auto const cid = child.id();
            ASSERT_EQ(l2.publish(child, 7, 42), OperationLedger::Refusal::ok);

            auto wrong_dest = [&] {
                return l2.ack_terminal(cid, /*dest*/ 9, /*work*/ 42);
            };
            auto wrong_work = [&] {
                return l2.ack_terminal(cid, 7, /*work*/ 41);
            };
            auto exact = [&] { return l2.ack_terminal(cid, 7, 42); };

            if (wrong_first)
            {
                EXPECT_EQ(
                    wrong_dest(),
                    OperationLedger::AckOutcome::wrong_destination);
                EXPECT_EQ(
                    wrong_work(), OperationLedger::AckOutcome::wrong_work);
                EXPECT_EQ(exact(), OperationLedger::AckOutcome::removed);
            }
            else
            {
                EXPECT_EQ(exact(), OperationLedger::AckOutcome::removed);
                EXPECT_EQ(
                    wrong_dest(),
                    OperationLedger::AckOutcome::duplicate_or_late);
                EXPECT_EQ(
                    wrong_work(),
                    OperationLedger::AckOutcome::duplicate_or_late);
            }
            EXPECT_EQ(
                exact(), OperationLedger::AckOutcome::duplicate_or_late)
                << "duplicate exact ack must be a no-op";
        });
    }
}

TEST_F(LedgerFixture, CancellationCannotFabricateTerminality)
{
    run([&] {
        auto root = ledger.open_root();
        auto const op = root->id();
        auto [r, child] = ledger.register_child(*root);
        ASSERT_EQ(r, OperationLedger::Refusal::ok);
        auto const cid = child.id();
        ASSERT_EQ(ledger.publish(child, 7, 42), OperationLedger::Refusal::ok);

        EXPECT_EQ(
            ledger.request_cancel(cid), OperationLedger::Refusal::ok);
        EXPECT_TRUE(ledger.is_inert(cid))
            << "cancellation makes the destination capability inert";
        EXPECT_EQ(ledger.child_count(op), 1u)
            << "cancellation must not decrement the child";

        // The destination's exact ack remains the only remover.
        EXPECT_EQ(
            ledger.ack_terminal(cid, 7, 42),
            OperationLedger::AckOutcome::removed);
        EXPECT_EQ(ledger.child_count(op), 0u);
    });
}

TEST_F(LedgerFixture, RootRemovedOnlyWhenChildMapEmpty)
{
    run([&] {
        auto root = ledger.open_root();
        auto const op = root->id();
        auto [r1, c1] = ledger.register_child(*root);
        auto [r2, c2] = ledger.register_child(*root);
        ASSERT_EQ(r1, OperationLedger::Refusal::ok);
        ASSERT_EQ(r2, OperationLedger::Refusal::ok);
        ASSERT_EQ(ledger.publish(c1, 7, 1), OperationLedger::Refusal::ok);
        ASSERT_EQ(ledger.publish(c2, 7, 2), OperationLedger::Refusal::ok);

        EXPECT_EQ(ledger.begin_close(*root), OperationLedger::Refusal::ok);
        EXPECT_TRUE(ledger.root_active(op))
            << "closing root with live children must remain visible";

        EXPECT_EQ(
            ledger.ack_terminal(c1.id(), 7, 1),
            OperationLedger::AckOutcome::removed);
        EXPECT_TRUE(ledger.root_active(op));
        EXPECT_EQ(
            ledger.ack_terminal(c2.id(), 7, 2),
            OperationLedger::AckOutcome::removed);
        EXPECT_FALSE(ledger.root_active(op))
            << "last child's exact ack removes the closing root";
    });
}

TEST_F(LedgerFixture, DroppedLeaseLeavesVisibleRowNeverFalseEmpty)
{
    run([&] {
        auto root = ledger.open_root();
        auto const op = root->id();
        {
            auto [r, child] = ledger.register_child(*root);
            ASSERT_EQ(r, OperationLedger::Refusal::ok);
            ASSERT_EQ(
                ledger.publish(child, 7, 42), OperationLedger::Refusal::ok);
            // child lease dropped here without any acknowledgement
        }
        EXPECT_EQ(ledger.child_count(op), 1u)
            << "a dropped lease must leave its row visible — the drain "
               "stays incomplete rather than false-empty";
        EXPECT_EQ(ledger.begin_close(*root), OperationLedger::Refusal::ok);
        EXPECT_TRUE(ledger.root_active(op))
            << "close cannot complete over an unacknowledged child";
    });
}

TEST_F(LedgerFixture, ChildIdsAreNeverReused)
{
    run([&] {
        auto root = ledger.open_root();
        auto [r1, c1] = ledger.register_child(*root);
        auto const first = c1.id();
        ASSERT_EQ(ledger.publish(c1, 7, 1), OperationLedger::Refusal::ok);
        ASSERT_EQ(
            ledger.ack_terminal(first, 7, 1),
            OperationLedger::AckOutcome::removed);

        auto [r2, c2] = ledger.register_child(*root);
        ASSERT_EQ(r2, OperationLedger::Refusal::ok);
        EXPECT_GT(c2.id(), first) << "ChildIds are non-reused";
    });
}

TEST_F(LedgerFixture, OffStrandMutationsAreRefused)
{
    // Negative control: every mutator refuses off the owner strand.
    std::optional<RootLease> root;
    run([&] { root = ledger.open_root(); });

    bool checked = false;
    asio::post(sched.io(), [&] {
        auto opened = ledger.open_root();
        EXPECT_FALSE(opened.has_value());
        auto [r, child] = ledger.register_child(*root);
        EXPECT_EQ(r, OperationLedger::Refusal::off_strand);
        EXPECT_FALSE(child.valid());
        EXPECT_EQ(
            ledger.ack_terminal(1, 1, 1),
            OperationLedger::AckOutcome::off_strand);
        EXPECT_EQ(
            ledger.begin_close(*root), OperationLedger::Refusal::off_strand);
        checked = true;
    });
    sched.run_until_idle();
    EXPECT_TRUE(checked);
}
