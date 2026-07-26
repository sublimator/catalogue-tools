#include <catl/peer-client/operation-lifecycle.h>

#include <catl/core/logger.h>

#include <limits>

namespace catl::peer_client {

static LogPartition log_("ldc", LogLevel::INHERIT);

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

//@@authority transition=ledger_data_control.open_control_root v4=T50
// owner=ledger_data_control commit=atomic
OperationLifecycle::Open
OperationLifecycle::open_control_root()
{
    if (root_open_.load(std::memory_order_acquire))
    {
        return Open::already_open;
    }

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

    root_open_.store(true, std::memory_order_release);
    PLOGD(log_, "control root ", root_.value(), " open");
    return Open::opened;
}

//@@authority transition=ledger_data_control.close_control_root v4=T52
// owner=ledger_data_control commit=atomic
OperationLifecycle::Close
OperationLifecycle::close_control_root()
{
    if (!root_open_.load(std::memory_order_acquire))
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
    root_ = ControlRootId{};
    root_open_.store(false, std::memory_order_release);
    return Close::closed;
}

bool
OperationLifecycle::control_root_open() const noexcept
{
    return root_open_.load(std::memory_order_acquire);
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
