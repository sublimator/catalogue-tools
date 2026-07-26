// Controls for the empty-operation lifecycle, target-only increment 1.
//
// The review list was: deterministic open/close, duplicate close, late close,
// fence, and mint-at-ceiling. Everything here is a NEGATIVE control or a
// discrimination — an end-state assertion that two different routes both satisfy
// proves nothing, which is the vacuity shape that has bitten three times on the
// preceding slice.

#include "operation-lifecycle-test-access.h"

#include <catl/peer-client/operation-lifecycle.h>

#include <boost/asio/io_context.hpp>

#include <gtest/gtest.h>

#include <limits>

using catl::peer_client::OperationLifecycle;
using Access = OperationLifecycleTestAccess;
using Open = OperationLifecycle::Open;
using Close = OperationLifecycle::Close;

namespace {

struct Fixture
{
    boost::asio::io_context io;
    std::shared_ptr<OperationLifecycle> ldc = OperationLifecycle::create(io);

    ~Fixture()
    {
        EXPECT_FALSE(ldc->accounting_broken())
            << "an invariant broke during this control, whatever else it asserted";
    }
};

}  // namespace

/// 1. Open then close, and every home minted is consumed exactly.
TEST(OperationLifecycle, OpenThenCloseConsumesTheRoot)
{
    Fixture f;
    EXPECT_FALSE(f.ldc->control_root_open()) << "nothing is open before T50";

    ASSERT_EQ(f.ldc->open_control_root(), Open::opened);
    EXPECT_TRUE(f.ldc->control_root_open());
    EXPECT_NE(Access::root_id(f.ldc), 0u) << "zero is reserved as no-root";

    ASSERT_EQ(f.ldc->close_control_root(), Close::closed);
    EXPECT_FALSE(f.ldc->control_root_open());
    EXPECT_EQ(Access::root_id(f.ldc), 0u)
        << "the root identity is consumed, not merely flagged closed";
}

/// 2. Opening twice does not mint a second root. The refusal is NAMED, so a caller
///    can tell it apart from exhaustion.
TEST(OperationLifecycle, DuplicateOpenIsRefusedWithoutMinting)
{
    Fixture f;
    ASSERT_EQ(f.ldc->open_control_root(), Open::opened);
    auto const first = Access::root_id(f.ldc);
    auto const minted_before = Access::next_root_id();

    EXPECT_EQ(f.ldc->open_control_root(), Open::already_open);
    EXPECT_EQ(Access::root_id(f.ldc), first) << "the identity did not change";
    EXPECT_EQ(Access::next_root_id(), minted_before)
        << "and the refused open consumed nothing from the minter";
}

/// 3. Duplicate close. The second must be distinguishable from the first, and must
///    not report success for work it did not do.
TEST(OperationLifecycle, DuplicateCloseIsNotASecondSuccess)
{
    Fixture f;
    ASSERT_EQ(f.ldc->open_control_root(), Open::opened);
    ASSERT_EQ(f.ldc->close_control_root(), Close::closed);

    EXPECT_EQ(f.ldc->close_control_root(), Close::not_open)
        << "closing an already-closed root is refused, never a second 'closed'";
}

/// 4. LATE close: closing something that never opened. Same disposition as the
///    duplicate, deliberately — both mean "there is no root here" — but reached by
///    a different route, and the control exists so that a future change which
///    splits them cannot do so silently.
TEST(OperationLifecycle, CloseWithoutOpenIsRefused)
{
    Fixture f;
    EXPECT_EQ(f.ldc->close_control_root(), Close::not_open);
    EXPECT_EQ(Access::root_id(f.ldc), 0u) << "and nothing was minted by the attempt";
}

/// 5. THE GUARD. S31.zero / S35.zero is what makes the close legitimate. Nothing in
///    this increment can raise the count, so the control forces it — otherwise the
///    guard is never observed refusing and is decoration.
TEST(OperationLifecycle, CloseRefusesWhileOperationsOutstanding)
{
    Fixture f;
    ASSERT_EQ(f.ldc->open_control_root(), Open::opened);
    auto const root = Access::root_id(f.ldc);

    Access::force_operations_outstanding(f.ldc, 1);
    EXPECT_EQ(f.ldc->close_control_root(), Close::operations_outstanding);
    EXPECT_TRUE(f.ldc->control_root_open())
        << "a refused close leaves the root OPEN, not half-closed";
    EXPECT_EQ(Access::root_id(f.ldc), root) << "and does not consume the identity";

    // And it is the count that gates it, not something incidental about the state:
    // drop the count and the same call now succeeds.
    Access::force_operations_outstanding(f.ldc, 0);
    EXPECT_EQ(f.ldc->close_control_root(), Close::closed);
}

/// 6. MINT AT CEILING. Absorbing: exhausted stays exhausted, and it never wraps
///    back through the reserved zero.
TEST(OperationLifecycle, RootMinterStaysExhaustedAtTheCeiling)
{
    auto const saved = Access::next_root_id();
    Access::set_next_root_id(std::numeric_limits<std::uint64_t>::max());
    {
        Fixture f;
        EXPECT_EQ(f.ldc->open_control_root(), Open::root_exhausted);
        EXPECT_FALSE(f.ldc->control_root_open());
        EXPECT_EQ(Access::root_id(f.ldc), 0u)
            << "a refused mint must not publish the reserved sentinel as an identity";

        // Absorbing, not a one-shot refusal: a second attempt must refuse too, and
        // must not have wrapped the counter to zero in the meantime.
        EXPECT_EQ(f.ldc->open_control_root(), Open::root_exhausted);
        EXPECT_EQ(Access::next_root_id(), std::numeric_limits<std::uint64_t>::max())
            << "the counter did not wrap past the ceiling";
    }
    // Static counter: restore it or every later control in this binary inherits an
    // exhausted minter.
    Access::set_next_root_id(saved);
}

/// 7. Identities are not reused across successive roots in one lifecycle.
TEST(OperationLifecycle, SuccessiveRootsGetDistinctIdentities)
{
    Fixture f;
    ASSERT_EQ(f.ldc->open_control_root(), Open::opened);
    auto const first = Access::root_id(f.ldc);
    ASSERT_EQ(f.ldc->close_control_root(), Close::closed);

    ASSERT_EQ(f.ldc->open_control_root(), Open::opened);
    auto const second = Access::root_id(f.ldc);
    EXPECT_NE(second, first)
        << "a reopened root is a NEW incarnation; reusing the identity would let a "
           "stale reference address the wrong one";
    EXPECT_EQ(f.ldc->close_control_root(), Close::closed);
}

/// 8. Two lifecycles draw from the same process-wide minter, so identities are
///    distinct across instances as well as across incarnations.
TEST(OperationLifecycle, IdentitiesAreDistinctAcrossInstances)
{
    Fixture a;
    Fixture b;
    ASSERT_EQ(a.ldc->open_control_root(), Open::opened);
    ASSERT_EQ(b.ldc->open_control_root(), Open::opened);
    EXPECT_NE(Access::root_id(a.ldc), Access::root_id(b.ldc));
    EXPECT_EQ(a.ldc->close_control_root(), Close::closed);
    EXPECT_EQ(b.ldc->close_control_root(), Close::closed);
}
