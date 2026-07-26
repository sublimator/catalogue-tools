#include <catl/peer-client/operation-lifecycle.h>

#include <catl/core/logger.h>

#include <cstdlib>
#include <limits>

namespace catl::peer_client {

static LogPartition log_("ldc", LogLevel::INHERIT);

// Same discipline as PeerSet: an off-strand mutation is an OWNERSHIP violation, not a
// race to paper over. The strand was previously constructed and never used, so the
// header's ownership claim was aspirational; this is what makes it true.
#define ASSERT_ON_STRAND()                            \
    do                                                \
    {                                                 \
        if (!strand_.running_in_this_thread())        \
        {                                             \
            PLOGE(log_, "OFF-STRAND: ", __func__);    \
            if (std::getenv("XPRV_STRAND_FAIL_FAST")) \
            {                                         \
                std::abort();                         \
            }                                         \
        }                                             \
    } while (0)

std::atomic<std::uint64_t> OperationLifecycle::next_root_id_{0};

std::shared_ptr<OperationLifecycle>
OperationLifecycle::create(asio::io_context& io)
{
    return std::shared_ptr<OperationLifecycle>(new OperationLifecycle(io));
}

OperationLifecycle::OperationLifecycle(asio::io_context& io)
    : strand_(asio::make_strand(io))
{
}

// NO v4 REFERENCE, deliberately. The first version claimed v4=T50 and the gate went
// green anyway, because conformance verifies declared LABELS, not semantic coverage.
// Canonical T50 requires S01.running + S04.consumed_same_epoch + S10 and retains
// S04/S10; none of that is represented here. This is a local control-root open and an
// explicitly unbound primitive — it cannot become T50 by relabelling.
//@@authority transition=ledger_data_control.open_control_root
// owner=ledger_data_control commit=strand
OperationLifecycle::Open
OperationLifecycle::open_control_root()
{
    ASSERT_ON_STRAND();

    // CLAIM the transition in owner-local state. Strand-confined, so a plain read
    // and write are correct here and, crucially, INVISIBLE to projection readers.
    if (phase_ == Phase::opening)
    {
        return Open::open_in_progress;
    }
    if (phase_ == Phase::open)
    {
        return Open::already_open;
    }
    phase_ = Phase::opening;

    // A CAS below the ceiling, never fetch_add. An unconditional increment keeps
    // walking past the first refusal and wraps through zero, which would hand out
    // the reserved no-root sentinel and then re-issue every value already issued.
    // Same rule and same reasoning as the coordinator's sender generation.
    auto current = next_root_id_.load(std::memory_order_relaxed);
    for (;;)
    {
        if (current == std::numeric_limits<std::uint64_t>::max())
        {
            PLOGE(log_, "control root minter exhausted; refusing to open");
            // ROLL BACK the claim, or a refused mint wedges the lifecycle in
            // `opening` forever — trading a race for a deadlock, which is worse.
            // Nothing was published, so there is no projection to retract.
            phase_ = Phase::closed;
            return Open::root_exhausted;
        }
        if (next_root_id_.compare_exchange_weak(
                current,
                current + 1,
                std::memory_order_acq_rel,
                std::memory_order_relaxed))
        {
            root_ = ControlRootId{current + 1};
            break;
        }
    }

    // The failpoint fires HERE: identity assigned, projection NOT yet published.
    // That is the exact window both previous versions got wrong, so this is where a
    // control has to stand to see it. A hook placed after the commit would assert
    // "if open then an identity exists" and be true by placement rather than by the
    // code — which is how the last control passed while the defect was live.
    if (before_commit_for_test_)
    {
        before_commit_for_test_();
    }

    // COMMIT. Identity first, then publish: a reader that acquires `true` is
    // guaranteed to see the identity written before it.
    open_projection_.store(true, std::memory_order_release);
    phase_ = Phase::open;
    PLOGD(log_, "control root ", root_.value(), " open");
    return Open::opened;
}

// NO v4 REFERENCE — same reason. Canonical T52 requires S31.zero AND S35.zero as two
// SEPARATELY OWNED facts (S31 ledger_data_control, S35 peer_set) and produces the
// Q01.operations / Q01.operation_children witnesses. This collapses both zero-checks
// into one local counter and publishes no witness.
//@@authority transition=ledger_data_control.close_control_root
// owner=ledger_data_control commit=strand
OperationLifecycle::Close
OperationLifecycle::close_control_root()
{
    ASSERT_ON_STRAND();

    if (phase_ == Phase::opening)
    {
        // Refused, not deferred, and named: there is no committed root to consume.
        return Close::open_in_progress;
    }
    if (phase_ != Phase::open)
    {
        return Close::not_open;
    }

    // S31.zero / S35.zero. In this increment nothing can raise the count, so the
    // guard is true by construction — which is exactly why it is written as a real
    // refusal and probed by a control that forces the count up through a test seam.
    // A guard that only ever sees its happy value is decoration, and this slice's
    // whole claim is that the close is legitimate BECAUSE the guard holds.
    auto const outstanding = operations_.load(std::memory_order_acquire);
    if (outstanding != 0)
    {
        PLOGE(
            log_,
            "refusing to close control root ",
            root_.value(),
            ": ",
            outstanding,
            " operation home(s) outstanding");
        return Close::operations_outstanding;
    }

    PLOGD(log_, "control root ", root_.value(), " closed");
    // Retract the projection BEFORE clearing the identity. The reverse order opens
    // the symmetric window on the way out: a reader seeing `open` with the identity
    // already gone.
    open_projection_.store(false, std::memory_order_release);
    root_ = ControlRootId{};
    phase_ = Phase::closed;
    return Close::closed;
}

bool
OperationLifecycle::control_root_open() const noexcept
{
    // Reads the COMMITTED projection only. It stays false for the whole of an
    // in-flight open, which is the property the previous pin violated.
    return open_projection_.load(std::memory_order_acquire);
}

std::uint64_t
OperationLifecycle::operations_outstanding() const noexcept
{
    return operations_.load(std::memory_order_acquire);
}

bool
OperationLifecycle::accounting_broken() const noexcept
{
    return accounting_broken_.load(std::memory_order_acquire);
}

}  // namespace catl::peer_client
