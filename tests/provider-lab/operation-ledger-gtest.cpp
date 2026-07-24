#include "operation-ledger.h"

#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
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

PartitionScope const kScope{1, 1};

/// Open a root asserting the typed ok refusal (round-4: open_root
/// fails closed at the id ceiling, so it returns RootResult).
RootLease
open_ok(OperationLedger& l)
{
    auto result = l.open_root();
    EXPECT_EQ(result.refusal, OperationLedger::Refusal::ok);
    return std::move(result.lease);
}

struct LedgerFixture : ::testing::Test
{
    DeterministicScheduler sched;
    DeterministicScheduler::strand_type strand = sched.make_strand();
    OperationLedger ledger{strand, kScope};

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
        auto root = open_ok(ledger);
        ASSERT_TRUE(root.valid());
        auto [r1, child] = ledger.register_child(root);
        EXPECT_EQ(r1, OperationLedger::Refusal::ok);
        EXPECT_TRUE(child.valid());

        EXPECT_EQ(
            ledger.begin_close(root), OperationLedger::Refusal::ok);
        auto [r2, refused] = ledger.register_child(root);
        EXPECT_EQ(r2, OperationLedger::Refusal::root_closing)
            << "registration must reject after the root enters closing";
        EXPECT_FALSE(refused.valid());
    });
}

TEST_F(LedgerFixture, PublicationCellFlipsExactlyOnce)
{
    run([&] {
        auto root = open_ok(ledger);
        auto [r, child] = ledger.register_child(root);
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
        auto root = open_ok(ledger);
        auto const op = root.id();
        auto [r, child] = ledger.register_child(root);
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
        OperationLedger l2{strand2, kScope};
        on_strand(s2, strand2, [&] {
            auto root = open_ok(l2);
            auto [r, child] = l2.register_child(root);
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
        auto root = open_ok(ledger);
        auto const op = root.id();
        auto [r, child] = ledger.register_child(root);
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
        auto root = open_ok(ledger);
        auto const op = root.id();
        auto [r1, c1] = ledger.register_child(root);
        auto [r2, c2] = ledger.register_child(root);
        ASSERT_EQ(r1, OperationLedger::Refusal::ok);
        ASSERT_EQ(r2, OperationLedger::Refusal::ok);
        ASSERT_EQ(ledger.publish(c1, 7, 1), OperationLedger::Refusal::ok);
        ASSERT_EQ(ledger.publish(c2, 7, 2), OperationLedger::Refusal::ok);

        EXPECT_EQ(ledger.begin_close(root), OperationLedger::Refusal::ok);
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
        auto root = open_ok(ledger);
        auto const op = root.id();
        {
            auto [r, child] = ledger.register_child(root);
            ASSERT_EQ(r, OperationLedger::Refusal::ok);
            ASSERT_EQ(
                ledger.publish(child, 7, 42), OperationLedger::Refusal::ok);
            // child lease dropped here without any acknowledgement
        }
        EXPECT_EQ(ledger.child_count(op), 1u)
            << "a dropped lease must leave its row visible — the drain "
               "stays incomplete rather than false-empty";
        EXPECT_EQ(ledger.begin_close(root), OperationLedger::Refusal::ok);
        EXPECT_TRUE(ledger.root_active(op))
            << "close cannot complete over an unacknowledged child";
    });
}

TEST_F(LedgerFixture, ChildIdsAreNeverReused)
{
    run([&] {
        auto root = open_ok(ledger);
        auto [r1, c1] = ledger.register_child(root);
        auto const first = c1.id();
        ASSERT_EQ(ledger.publish(c1, 7, 1), OperationLedger::Refusal::ok);
        ASSERT_EQ(
            ledger.ack_terminal(first, 7, 1),
            OperationLedger::AckOutcome::removed);

        auto [r2, c2] = ledger.register_child(root);
        ASSERT_EQ(r2, OperationLedger::Refusal::ok);
        EXPECT_GT(c2.id(), first) << "ChildIds are non-reused";
    });
}

TEST_F(LedgerFixture, OffStrandMutationsAreRefused)
{
    // Negative control: every mutator refuses off the owner strand.
    RootLease root;
    run([&] { root = open_ok(ledger); });

    bool checked = false;
    asio::post(sched.io(), [&] {
        EXPECT_EQ(ledger.open_root().refusal,
                  OperationLedger::Refusal::off_strand);
        auto [r, child] = ledger.register_child(root);
        EXPECT_EQ(r, OperationLedger::Refusal::off_strand);
        EXPECT_FALSE(child.valid());
        EXPECT_EQ(
            ledger.ack_terminal(1, 1, 1),
            OperationLedger::AckOutcome::off_strand);
        EXPECT_EQ(
            ledger.begin_close(root), OperationLedger::Refusal::off_strand);
        checked = true;
    });
    sched.run_until_idle();
    EXPECT_TRUE(checked);
}

TEST_F(LedgerFixture, IntrospectorsRefuseOffStrandBeforeAnyRead)
{
    RootLease root;
    OperationId op = 0;
    ChildId cid = 0;
    run([&] {
        root = open_ok(ledger);
        op = root.id();
        auto [r, child] = ledger.register_child(root);
        ASSERT_EQ(r, OperationLedger::Refusal::ok);
        cid = child.id();
        ASSERT_EQ(ledger.publish(child, 7, 42), OperationLedger::Refusal::ok);
    });
    bool checked = false;
    asio::post(sched.io(), [&] {
        EXPECT_FALSE(ledger.root_active(op));
        EXPECT_EQ(ledger.child_count(op), 0u);
        EXPECT_FALSE(ledger.publication_state(cid).has_value());
        EXPECT_FALSE(ledger.is_inert(cid));
        checked = true;
    });
    sched.run_until_idle();
    EXPECT_TRUE(checked);
}

TEST_F(LedgerFixture, PrePublicationCancelSealsThePublicationPathBothOrders)
{
    run([&] {
        auto root = open_ok(ledger);
        auto const op = root.id();

        // Order 1: cancel before publish — publication must refuse, and
        // the child follows the never_published path to removal.
        {
            auto [r, child] = ledger.register_child(root);
            ASSERT_EQ(r, OperationLedger::Refusal::ok);
            auto const cid = child.id();
            ASSERT_EQ(
                ledger.request_cancel(cid), OperationLedger::Refusal::ok);
            EXPECT_EQ(
                ledger.publish(child, 7, 42),
                OperationLedger::Refusal::cancelled_inert)
                << "cancelled pre-publication authority must not publish";
            EXPECT_EQ(
                ledger.mark_never_published(child),
                OperationLedger::Refusal::ok);
            EXPECT_EQ(
                ledger.ack_never_published(cid),
                OperationLedger::AckOutcome::removed);
        }

        // Order 2: publish before cancel — cancellation marks inert and
        // the destination ack remains the only remover (existing
        // contract, re-checked here for the order pair).
        {
            auto [r, child] = ledger.register_child(root);
            ASSERT_EQ(r, OperationLedger::Refusal::ok);
            auto const cid = child.id();
            ASSERT_EQ(
                ledger.publish(child, 7, 42), OperationLedger::Refusal::ok);
            ASSERT_EQ(
                ledger.request_cancel(cid), OperationLedger::Refusal::ok);
            EXPECT_TRUE(ledger.is_inert(cid));
            EXPECT_EQ(ledger.child_count(op), 1u);
            EXPECT_EQ(
                ledger.ack_terminal(cid, 7, 42),
                OperationLedger::AckOutcome::removed);
        }
    });
}

TEST_F(LedgerFixture, ConsumeTerminalAckMintsAndReplaysTheWitness)
{
    run([&] {
        auto root = open_ok(ledger);
        auto [r, child] = ledger.register_child(root);
        ASSERT_EQ(r, OperationLedger::Refusal::ok);
        ASSERT_EQ(
            OperationLedgerTestAccess::reserve(ledger, child, 42),
            OperationLedger::Refusal::ok);
        ASSERT_EQ(
            OperationLedgerTestAccess::publish_reserved(
                ledger, child, 9, 4),
            OperationLedger::Refusal::ok);

        auto const ack = TerminalAckTestMint::mint(
            kScope, /*interest*/ 42, root.id(), child.id());

        // Scope is authenticated FIRST; a foreign-partition ack is
        // inert even with matching local identities.
        EXPECT_EQ(
            ledger
                .consume_terminal_ack(
                    TerminalAckTestMint::mint(
                        PartitionScope{1, 2}, 42, root.id(), child.id()),
                    9, 4)
                .outcome,
            OperationLedger::AckOutcome::wrong_scope);

        // An ack naming an operation that does not own the child is
        // wrong_binding, not a removal.
        EXPECT_EQ(
            ledger
                .consume_terminal_ack(
                    TerminalAckTestMint::mint(
                        kScope, 42, root.id() + 1, child.id()),
                    9, 4)
                .outcome,
            OperationLedger::AckOutcome::wrong_binding);

        // The existing gate still guards destination and work.
        EXPECT_EQ(
            ledger.consume_terminal_ack(ack, 8, 4).outcome,
            OperationLedger::AckOutcome::wrong_destination);
        EXPECT_EQ(
            ledger.consume_terminal_ack(ack, 9, 5).outcome,
            OperationLedger::AckOutcome::wrong_work);
        EXPECT_EQ(ledger.child_count(root.id()), 1u)
            << "every refusal above changed nothing";

        // The exactly-once win removes the child and mints the witness.
        auto first = ledger.consume_terminal_ack(ack, 9, 4);
        ASSERT_EQ(first.outcome, OperationLedger::AckOutcome::removed);
        ASSERT_TRUE(first.witness.has_value());
        EXPECT_EQ(first.witness->interest(), 42u);
        EXPECT_EQ(ledger.child_count(root.id()), 0u);

        // Replay: the identical retained witness, from both homes.
        auto again = ledger.consume_terminal_ack(ack, 9, 4);
        EXPECT_EQ(again.outcome, OperationLedger::AckOutcome::removed);
        EXPECT_TRUE(*again.witness == *first.witness);
        auto from_home = ledger.replay_terminal_consumed(42);
        ASSERT_TRUE(from_home.has_value());
        EXPECT_TRUE(*from_home == *first.witness);

        // A DIFFERENT claim under the same interest is wrong_binding,
        // never a second mint.
        EXPECT_EQ(
            ledger
                .consume_terminal_ack(
                    TerminalAckTestMint::mint(
                        kScope, 42, root.id(), child.id() + 1),
                    9, 4)
                .outcome,
            OperationLedger::AckOutcome::wrong_binding);
    });
}

TEST_F(LedgerFixture, TerminalVariantIsClosedInBothOrders)
{
    run([&] {
        auto root = open_ok(ledger);

        // Interest-bound row: the raw gate REFUSES first, and the
        // three-phase consume still wins after. The reservation is
        // the variant choice: store publication refuses a reserved
        // child, and interest publication refuses an unreserved one.
        {
            auto [r, child] = ledger.register_child(root);
            ASSERT_EQ(r, OperationLedger::Refusal::ok);
            EXPECT_EQ(
                OperationLedgerTestAccess::publish_reserved(
                    ledger, child, 9, 4),
                OperationLedger::Refusal::not_reserved)
                << "no reservation, no interest publication";
            ASSERT_EQ(
                OperationLedgerTestAccess::reserve(ledger, child, 42),
                OperationLedger::Refusal::ok);
            EXPECT_EQ(
                ledger.publish(child, 9, 4),
                OperationLedger::Refusal::interest_reserved)
                << "the store verb refuses a reserved child";
            ASSERT_EQ(
                OperationLedgerTestAccess::publish_reserved(
                    ledger, child, 9, 4),
                OperationLedger::Refusal::ok);
            EXPECT_EQ(
                ledger.ack_terminal(child.id(), 9, 4),
                OperationLedger::AckOutcome::interest_bound)
                << "the old gate cannot accept the new variant";
            EXPECT_EQ(ledger.child_count(root.id()), 1u)
                << "the refused raw ack destroyed nothing";

            // A different interest's sole-minted ack cannot steal it.
            EXPECT_EQ(
                ledger
                    .consume_terminal_ack(
                        TerminalAckTestMint::mint(
                            kScope, 43, root.id(), child.id()),
                        9, 4)
                    .outcome,
                OperationLedger::AckOutcome::wrong_binding);

            auto win = ledger.consume_terminal_ack(
                TerminalAckTestMint::mint(
                    kScope, 42, root.id(), child.id()),
                9, 4);
            EXPECT_EQ(win.outcome, OperationLedger::AckOutcome::removed);
            ASSERT_TRUE(win.witness.has_value());
            // The raw gate stays refused AFTER the win too: the
            // witness home replays, nothing is stolen or destroyed.
            EXPECT_EQ(
                ledger.ack_terminal(child.id(), 9, 4),
                OperationLedger::AckOutcome::duplicate_or_late);
            EXPECT_TRUE(
                ledger.replay_terminal_consumed(42).has_value());
        }

        // Store row: consume refuses, the raw gate remains the owner.
        {
            auto [r, child] = ledger.register_child(root);
            ASSERT_EQ(r, OperationLedger::Refusal::ok);
            ASSERT_EQ(
                ledger.publish(child, 9, 5), OperationLedger::Refusal::ok);
            EXPECT_EQ(
                ledger
                    .consume_terminal_ack(
                        TerminalAckTestMint::mint(
                            kScope, 44, root.id(), child.id()),
                        9, 5)
                    .outcome,
                OperationLedger::AckOutcome::not_interest_bound)
                << "the new gate cannot accept the old variant";
            EXPECT_EQ(ledger.child_count(root.id()), 1u);
            EXPECT_EQ(
                ledger.ack_terminal(child.id(), 9, 5),
                OperationLedger::AckOutcome::removed);
        }
    });
}

TEST_F(LedgerFixture, ConsumeCommitIsExceptionAtomicAtTheFailpoint)
{
    run([&] {
        auto root = open_ok(ledger);
        auto [r, child] = ledger.register_child(root);
        ASSERT_EQ(r, OperationLedger::Refusal::ok);
        ASSERT_EQ(
            OperationLedgerTestAccess::reserve(ledger, child, 42),
            OperationLedger::Refusal::ok);
        ASSERT_EQ(
            OperationLedgerTestAccess::publish_reserved(
                ledger, child, 9, 4),
            OperationLedger::Refusal::ok);
        auto const ack = TerminalAckTestMint::mint(
            kScope, 42, root.id(), child.id());

        // Inject a throw between PREPARE and COMMIT: the child must
        // survive and no witness may be observable — pre-state exactly.
        ledger.test_hooks.consume_pre_commit = [] {
            throw std::runtime_error{"injected"};
        };
        EXPECT_THROW(ledger.consume_terminal_ack(ack, 9, 4),
                     std::runtime_error);
        EXPECT_EQ(ledger.child_count(root.id()), 1u)
            << "the terminal obligation survived the failure";
        EXPECT_FALSE(ledger.replay_terminal_consumed(42).has_value())
            << "no half-minted witness is observable";

        // Disarm: the identical consume now wins exactly once.
        ledger.test_hooks.consume_pre_commit = nullptr;
        auto win = ledger.consume_terminal_ack(ack, 9, 4);
        EXPECT_EQ(win.outcome, OperationLedger::AckOutcome::removed);
        EXPECT_EQ(ledger.child_count(root.id()), 0u);
    });
}

TEST(LedgerEdge, MinterCeilingsFailClosedWithNothingMutated)
{
    // Operation ceiling.
    {
        DeterministicScheduler sched;
        auto strand = sched.make_strand();
        OperationLedger l{
            strand, kScope,
            std::numeric_limits<OperationId>::max(), 1};
        asio::post(strand, [&] {
            EXPECT_EQ(l.open_root().refusal,
                      OperationLedger::Refusal::operation_id_exhausted)
                << "the wrap class, refused at the root minter";
            EXPECT_FALSE(l.root_active(0)) << "zero stays reserved";
            EXPECT_EQ(l.open_root().refusal,
                      OperationLedger::Refusal::operation_id_exhausted)
                << "the counter did not move";
        });
        sched.run_until_idle();
    }
    // Child ceiling: root intact, nothing registered.
    {
        DeterministicScheduler sched;
        auto strand = sched.make_strand();
        OperationLedger l{
            strand, kScope, 1, std::numeric_limits<ChildId>::max()};
        asio::post(strand, [&] {
            auto root = open_ok(l);
            auto [r, child] = l.register_child(root);
            EXPECT_EQ(r, OperationLedger::Refusal::child_id_exhausted);
            EXPECT_FALSE(child.valid());
            EXPECT_EQ(l.child_count(root.id()), 0u);
            EXPECT_EQ(
                l.register_child(root).first,
                OperationLedger::Refusal::child_id_exhausted);
        });
        sched.run_until_idle();
    }
}

TEST_F(LedgerFixture, MinterCommitsAreExceptionAtomicAtTheFailpoints)
{
    run([&] {
        // open_root: a throw after preparation leaves no operation row
        // and an unmoved counter — the retry mints the SAME id.
        ledger.test_hooks.open_root_pre_commit = [] {
            throw std::runtime_error{"injected"};
        };
        EXPECT_THROW(ledger.open_root(), std::runtime_error);
        ledger.test_hooks.open_root_pre_commit = nullptr;
        auto root = open_ok(ledger);
        EXPECT_TRUE(ledger.root_active(root.id()))
            << "the failed attempt minted nothing; this one is real";

        // register_child: TWO homes (child row + index). A throw after
        // preparation leaves row, index, and counter mutually
        // unchanged — the retry mints the SAME child id and the child
        // is fully locatable (publish + ack work).
        ledger.test_hooks.register_child_pre_commit = [] {
            throw std::runtime_error{"injected"};
        };
        EXPECT_THROW(ledger.register_child(root), std::runtime_error);
        ledger.test_hooks.register_child_pre_commit = nullptr;
        EXPECT_EQ(ledger.child_count(root.id()), 0u)
            << "no half-registered child row";
        auto [r, child] = ledger.register_child(root);
        ASSERT_EQ(r, OperationLedger::Refusal::ok);
        EXPECT_EQ(ledger.child_count(root.id()), 1u);
        ASSERT_EQ(ledger.publish(child, 7, 42), OperationLedger::Refusal::ok);
        EXPECT_EQ(
            ledger.ack_terminal(child.id(), 7, 42),
            OperationLedger::AckOutcome::removed)
            << "row and index agree: the child is locatable and "
               "removable — no two-home tear";
    });
}

TEST(LedgerEdge, ForeignLeasesRefuseBeforeAnyLookup)
{
    // Ledger A is the target; B (same scope) and C (different scope)
    // mint colliding local ids; D is destroyed and recreated in the
    // same scope to prove incarnation identity, not scope, is the
    // authority.
    DeterministicScheduler sched;
    auto strand_a = sched.make_strand();
    OperationLedger a{strand_a, kScope};

    DeterministicScheduler s_b;
    auto strand_b = s_b.make_strand();
    OperationLedger b{strand_b, kScope};  // SAME scope
    RootLease b_root;
    ChildLease b_child;
    ChildLease b_child2;
    on_strand(s_b, strand_b, [&] {
        b_root = open_ok(b);
        auto [r1, c1] = b.register_child(b_root);
        ASSERT_EQ(r1, OperationLedger::Refusal::ok);
        b_child = std::move(c1);
        auto [r2, c2] = b.register_child(b_root);
        ASSERT_EQ(r2, OperationLedger::Refusal::ok);
        b_child2 = std::move(c2);  // id 2: ABSENT in A
    });

    DeterministicScheduler s_c;
    auto strand_c = s_c.make_strand();
    OperationLedger c{strand_c, PartitionScope{1, 2}};
    RootLease c_root;
    on_strand(s_c, strand_c, [&] { c_root = open_ok(c); });

    on_strand(sched, strand_a, [&] {
        auto a_root = open_ok(a);
        auto [ar, a_child] = a.register_child(a_root);
        ASSERT_EQ(ar, OperationLedger::Refusal::ok);
        ASSERT_EQ(a_root.id(), b_root.id())
            << "the collision is real: identical local ids";

        // Present-id foreign leases: the SAME wrong_ledger refusal,
        // and A's own state untouched.
        EXPECT_EQ(a.register_child(b_root).first,
                  OperationLedger::Refusal::wrong_ledger);
        EXPECT_EQ(a.begin_close(b_root),
                  OperationLedger::Refusal::wrong_ledger);
        EXPECT_EQ(a.publish(b_child, 7, 42),
                  OperationLedger::Refusal::wrong_ledger);
        EXPECT_EQ(a.mark_never_published(b_child),
                  OperationLedger::Refusal::wrong_ledger);
        // Absent-id foreign lease: identical refusal, no presence leak.
        EXPECT_EQ(a.publish(b_child2, 7, 42),
                  OperationLedger::Refusal::wrong_ledger);
        // Different scope changes nothing about the refusal.
        EXPECT_EQ(a.register_child(c_root).first,
                  OperationLedger::Refusal::wrong_ledger);

        EXPECT_EQ(a.child_count(a_root.id()), 1u)
            << "every foreign refusal mutated nothing";
        EXPECT_EQ(a.publish(a_child, 7, 42), OperationLedger::Refusal::ok)
            << "A's own authority is intact";
    });

    // Recreated incarnation, same scope: old leases are foreign to
    // the successor even though its counters restart identically.
    DeterministicScheduler s_d;
    auto strand_d = s_d.make_strand();
    auto d = std::make_unique<OperationLedger>(strand_d, kScope);
    RootLease old_root;
    on_strand(s_d, strand_d, [&] { old_root = open_ok(*d); });
    d = std::make_unique<OperationLedger>(strand_d, kScope);
    on_strand(s_d, strand_d, [&] {
        auto fresh = open_ok(*d);
        ASSERT_EQ(fresh.id(), old_root.id())
            << "the successor restarted its counters";
        EXPECT_EQ(d->register_child(old_root).first,
                  OperationLedger::Refusal::wrong_ledger)
            << "an old incarnation's lease has no authority over the "
               "successor";
        EXPECT_EQ(d->register_child(fresh).first,
                  OperationLedger::Refusal::ok);
    });
}
