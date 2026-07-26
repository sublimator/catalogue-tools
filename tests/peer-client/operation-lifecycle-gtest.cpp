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
    Access::set_before_commit(f.ldc, [&] {
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

    EXPECT_EQ(nested, Open::already_open)
        << "a second open arriving mid-transition must be refused, not granted";
    EXPECT_EQ(Access::next_root_id(), minted_before + 1)
        << "EXACTLY ONE identity was minted; two winners would consume two";
    EXPECT_NE(Access::root_id(f.ldc), 0u);
    EXPECT_TRUE(f.ldc->control_root_open());
}

/// 4. CLOSE VERSUS OPEN ordering, per review. A close arriving mid-open must not
///    observe a split projection — an open flag with no identity behind it, or an
///    identity with the flag not yet published.
TEST(OperationLifecycle, CloseDuringTheOpenTransitionSeesNoSplitProjection)
{
    Fixture f;
    Close nested = Close::closed;
    int nested_calls = 0;
    Access::set_before_commit(f.ldc, [&] {
        ++nested_calls;
        // Whatever this observes, it must be self-consistent: if the root reads
        // open, an identity must already stand behind it.
        if (f.ldc->control_root_open())
        {
            EXPECT_NE(Access::root_id(f.ldc), 0u)
                << "open was published before the identity existed: a split commit";
        }
        nested = f.ldc->close_control_root();
    });

    ASSERT_EQ(f.open(), Open::opened);
    ASSERT_EQ(nested_calls, 1);

    // The nested close ran inside the transition and legitimately succeeds: it is on
    // the same strand, the identity exists, and no operations are outstanding. What
    // must NOT happen is a half-state afterwards.
    EXPECT_EQ(nested, Close::closed);
    EXPECT_FALSE(f.ldc->control_root_open())
        << "the close won and the outer open must not resurrect the flag";
    EXPECT_EQ(Access::root_id(f.ldc), 0u)
        << "identity and flag agree; neither is left behind by the other";
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
