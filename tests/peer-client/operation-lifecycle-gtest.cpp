// Controls for the target-only control-root primitive.
//
// Every mutation runs ON the owner strand, because the class now asserts it and the
// peer-client discovery loop sets XPRV_STRAND_FAIL_FAST=1. That is deliberate: the
// first version of this class constructed a strand and never used it while claiming
// "one strand owns every mutation", and controls that called from the test thread
// could not have noticed.

#include "operation-lifecycle-test-access.h"

#include <catl/peer-client/operation-lifecycle.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

#include <limits>
#include <vector>

using catl::peer_client::OperationLifecycle;
using Access = OperationLifecycleTestAccess;
using Open = OperationLifecycle::Open;
using Close = OperationLifecycle::Close;

namespace {

struct Fixture
{
    boost::asio::io_context io;
    std::shared_ptr<OperationLifecycle> ldc = OperationLifecycle::create(io);

    /// Run one mutation on the owner strand and hand back its disposition. The
    /// io_context is pumped on the TEST thread, so `running_in_this_thread()` holds
    /// inside the handler and the assertion is satisfied honestly rather than
    /// bypassed.
    template <class F>
    auto
    on_strand(F&& fn) -> decltype(fn())
    {
        decltype(fn()) result{};
        boost::asio::post(ldc->strand(), [&] { result = fn(); });
        io.restart();
        io.run();
        return result;
    }

    void
    run_on_strand(std::function<void()> fn)
    {
        boost::asio::post(ldc->strand(), std::move(fn));
        io.restart();
        io.run();
    }

    Open
    open()
    {
        return on_strand([&] { return ldc->open_control_root(); });
    }

    Close
    close()
    {
        return on_strand([&] { return ldc->close_control_root(); });
    }

    ~Fixture()
    {
        EXPECT_FALSE(ldc->accounting_broken())
            << "an invariant broke during this control, whatever else it asserted";
    }
};

}  // namespace

/// 1. Open then close, and the identity is consumed rather than merely flagged.
TEST(OperationLifecycle, OpenThenCloseConsumesTheRoot)
{
    Fixture f;
    EXPECT_FALSE(f.ldc->control_root_open());

    ASSERT_EQ(f.open(), Open::opened);
    EXPECT_TRUE(f.ldc->control_root_open());
    EXPECT_NE(Access::root_id(f.ldc), 0u) << "zero is reserved as no-root";

    ASSERT_EQ(f.close(), Close::closed);
    EXPECT_FALSE(f.ldc->control_root_open());
    EXPECT_EQ(Access::root_id(f.ldc), 0u)
        << "the identity is consumed, not merely flagged closed";
}

/// 2. Opening twice refuses by name and mints nothing.
TEST(OperationLifecycle, DuplicateOpenIsRefusedWithoutMinting)
{
    Fixture f;
    ASSERT_EQ(f.open(), Open::opened);
    auto const first = Access::root_id(f.ldc);
    auto const minted_before = Access::next_root_id();

    EXPECT_EQ(f.open(), Open::already_open);
    EXPECT_EQ(Access::root_id(f.ldc), first);
    EXPECT_EQ(Access::next_root_id(), minted_before)
        << "a refused open consumed nothing from the minter";
}

/// 3. THE INTERLEAVING, deterministically. A second open arrives while the first is
///    mid-transition — after the claim, before it returns — by re-entering from the
///    failpoint. No threads, no timing.
///
///    The pre-repair code loaded, minted, then stored, so this second call would see
///    `root_open_ == false`, mint a SECOND identity, and also return `opened`: two
///    winners and two consumed identities. Claim-before-mint makes it refuse.
TEST(OperationLifecycle, ReentrantOpenDuringTheTransitionFindsOneWinner)
{
    Fixture f;
    auto const minted_before = Access::next_root_id();

    Open nested = Open::opened;
    int nested_calls = 0;
    bool projection_during = true;
    Access::set_before_commit(f.ldc, [&] {
        // THE assertion the previous pin could not make. The hook now stands where
        // the identity exists but the commit has not happened, so a projection that
        // reads open here means the claim is being published as a committed fact —
        // the defect that survived two repairs.
        projection_during = f.ldc->control_root_open();
        // ONE-SHOT. Under a split commit the nested call also passes the check and
        // re-enters the failpoint, so an unguarded hook recurses until the stack is
        // gone — and a stack overflow is a crash, not an assertion, so the control
        // would report "crashed" instead of naming the defect. Firing once turns
        // that into a clean, readable failure.
        if (nested_calls++ > 0)
        {
            return;
        }
        nested = f.ldc->open_control_root();
    });

    ASSERT_EQ(f.open(), Open::opened) << "the first caller wins";
    ASSERT_EQ(nested_calls, 1) << "the failpoint fired, so this control is not vacuous";

    EXPECT_EQ(nested, Open::open_in_progress)
        << "a second open arriving mid-transition must be refused, and named for what "
           "is true: claimed but not committed, so there is no root to be ALREADY open";
    EXPECT_FALSE(projection_during)
        << "the committed projection must stay CLOSED for the whole in-flight open; "
           "an off-strand reader must never see an open root before it is committed";
    EXPECT_EQ(Access::next_root_id(), minted_before + 1)
        << "EXACTLY ONE identity was minted; two winners would consume two";
    EXPECT_NE(Access::root_id(f.ldc), 0u);
    EXPECT_TRUE(f.ldc->control_root_open()) << "and it IS published once committed";
}

/// 4. CLOSE BEFORE COMMIT is a named REFUSAL, not a success.
///
///    The previous version of this control let the nested close WIN, and then let the
///    outer open return `opened` while the final state was closed. That is not a
///    truthful disposition for a public operation: the caller is told its open
///    succeeded when nothing is open. A close arriving before the commit cannot
///    consume a root that does not exist yet.
TEST(OperationLifecycle, CloseBeforeTheCommitIsRefusedAndTheOpenStillWins)
{
    Fixture f;
    Close nested = Close::closed;
    int nested_calls = 0;
    Access::set_before_commit(f.ldc, [&] {
        if (nested_calls++ > 0)
        {
            return;
        }
        nested = f.ldc->close_control_root();
    });

    ASSERT_EQ(f.open(), Open::opened) << "the open completes and says so truthfully";
    ASSERT_EQ(nested_calls, 1);

    EXPECT_EQ(nested, Close::open_in_progress)
        << "a close during an uncommitted open must be refused BY NAME; it cannot "
           "consume a root that has not committed";
    EXPECT_TRUE(f.ldc->control_root_open())
        << "and the refusal left the committed open intact";
    EXPECT_NE(Access::root_id(f.ldc), 0u);
}

/// 5. CLOSE AFTER COMMIT is an ordinary later transition — the other half of the
///    ordering pair, so the refusal above cannot be mistaken for "close never works
///    near an open".
TEST(OperationLifecycle, CloseAfterTheCommitIsAnOrdinaryTransition)
{
    Fixture f;
    ASSERT_EQ(f.open(), Open::opened);
    ASSERT_TRUE(f.ldc->control_root_open());

    EXPECT_EQ(f.close(), Close::closed);
    EXPECT_FALSE(f.ldc->control_root_open());
    EXPECT_EQ(Access::root_id(f.ldc), 0u)
        << "the projection is retracted BEFORE the identity is cleared, so no reader "
           "sees an open root whose identity has already gone";
}

/// 5. Duplicate close is not a second success.
TEST(OperationLifecycle, DuplicateCloseIsNotASecondSuccess)
{
    Fixture f;
    ASSERT_EQ(f.open(), Open::opened);
    ASSERT_EQ(f.close(), Close::closed);
    EXPECT_EQ(f.close(), Close::not_open);
}

/// 6. Late close: closing what never opened.
TEST(OperationLifecycle, CloseWithoutOpenIsRefused)
{
    Fixture f;
    EXPECT_EQ(f.close(), Close::not_open);
    EXPECT_EQ(Access::root_id(f.ldc), 0u);
}

/// 7. THE GUARD. Nothing here can raise the count, so the control forces it —
///    otherwise the guard is never seen refusing and is decoration.
TEST(OperationLifecycle, CloseRefusesWhileOperationsOutstanding)
{
    Fixture f;
    ASSERT_EQ(f.open(), Open::opened);
    auto const root = Access::root_id(f.ldc);

    Access::force_operations_outstanding(f.ldc, 1);
    EXPECT_EQ(f.close(), Close::operations_outstanding);
    EXPECT_TRUE(f.ldc->control_root_open()) << "a refused close leaves the root OPEN";
    EXPECT_EQ(Access::root_id(f.ldc), root) << "and does not consume the identity";

    Access::force_operations_outstanding(f.ldc, 0);
    EXPECT_EQ(f.close(), Close::closed) << "the count is what gated it";
}

/// 8. Mint at ceiling: absorbing, never wrapping through the reserved zero, and the
///    claim is ROLLED BACK so a refused mint does not wedge the lifecycle open.
TEST(OperationLifecycle, RootMinterStaysExhaustedAtTheCeiling)
{
    auto const saved = Access::next_root_id();
    Access::set_next_root_id(std::numeric_limits<std::uint64_t>::max());
    {
        Fixture f;
        EXPECT_EQ(f.open(), Open::root_exhausted);
        EXPECT_FALSE(f.ldc->control_root_open())
            << "the claim was rolled back; a refused mint must not leave the "
               "lifecycle open with no identity behind it";
        EXPECT_EQ(Access::root_id(f.ldc), 0u);

        EXPECT_EQ(f.open(), Open::root_exhausted) << "absorbing, not one-shot";
        EXPECT_EQ(Access::next_root_id(), std::numeric_limits<std::uint64_t>::max())
            << "the counter did not wrap past the ceiling";
    }
    Access::set_next_root_id(saved);
}

/// 9. Successive roots get distinct identities.
TEST(OperationLifecycle, SuccessiveRootsGetDistinctIdentities)
{
    Fixture f;
    ASSERT_EQ(f.open(), Open::opened);
    auto const first = Access::root_id(f.ldc);
    ASSERT_EQ(f.close(), Close::closed);

    ASSERT_EQ(f.open(), Open::opened);
    EXPECT_NE(Access::root_id(f.ldc), first)
        << "a reopened root is a NEW incarnation; reuse would let a stale reference "
           "address the wrong one";
    EXPECT_EQ(f.close(), Close::closed);
}

/// 10. Distinct across instances, since the minter is process-wide.
TEST(OperationLifecycle, IdentitiesAreDistinctAcrossInstances)
{
    Fixture a;
    Fixture b;
    ASSERT_EQ(a.open(), Open::opened);
    ASSERT_EQ(b.open(), Open::opened);
    EXPECT_NE(Access::root_id(a.ldc), Access::root_id(b.ldc));
    EXPECT_EQ(a.close(), Close::closed);
    EXPECT_EQ(b.close(), Close::closed);
}
