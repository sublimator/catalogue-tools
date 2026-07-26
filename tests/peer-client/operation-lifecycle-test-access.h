#pragma once

// Direct access to owned state for controls. Single-threaded use only: these do
// NOT hop to the owner strand, because a control that posted would be asserting on
// a queue rather than on the state.

#include <catl/peer-client/operation-lifecycle.h>

#include <cstdint>
#include <memory>

class OperationLifecycleTestAccess
{
public:
    using Lifecycle = catl::peer_client::OperationLifecycle;

    /// Force the outstanding-operation count. The close guard reads S31.zero /
    /// S35.zero, and nothing in THIS increment can raise that count — T53 is not
    /// implemented, deliberately. Without a seam the guard could never be seen to
    /// refuse, and a guard no control can fail is decoration.
    ///
    /// This is a seam, not a second minter: it moves a counter, it does not mint an
    /// operation identity or create an S31/S35 home.
    static void
    force_operations_outstanding(
        std::shared_ptr<Lifecycle> const& lifecycle,
        std::uint64_t count)
    {
        lifecycle->operations_.store(count, std::memory_order_release);
    }

    /// Drive the PROCESS-WIDE root minter. Reaching the ceiling honestly would need
    /// 2^64 opens, so the ceiling control forces it — and must restore it, or it
    /// would poison every later lifecycle in the run.
    static void
    set_next_root_id(std::uint64_t value)
    {
        Lifecycle::next_root_id_.store(value, std::memory_order_release);
    }

    static std::uint64_t
    next_root_id()
    {
        return Lifecycle::next_root_id_.load(std::memory_order_acquire);
    }

    static std::uint64_t
    root_id(std::shared_ptr<Lifecycle> const& lifecycle)
    {
        return lifecycle->root_.value();
    }
};
