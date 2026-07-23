#pragma once

// Lifecycle waiter model — target-stack package 3, increment 3
// (ledger-data-provider decomposition v4, "Normalized lifecycle and
// waiters"). TEST-ONLY: production construction must never reference
// this.
//
// The contract, from the design body:
//  - each lifecycle call registers a non-reused LifecycleWaiterId with
//    kind, delivery executor, and target lifecycle ID;
//  - caller cancellation removes only its own waiter; the lifecycle
//    transaction continues if its initiating waiter leaves;
//  - clear completion, failure, or shutdown takeover completes every
//    remaining clear waiter exactly once with a typed LifecycleResult;
//  - shutdown waiters complete only at stopped;
//  - every delivery is posted to the WAITER's own executor, exactly
//    once, whatever completes it. The ledger itself is owner-strand
//    confined and refuses off-strand touches.

#include "deterministic-scheduler.h"

#include <boost/asio/any_io_executor.hpp>

#include <cstdint>
#include <map>
#include <utility>

namespace catl::provider_lab {

using LifecycleWaiterId = std::uint64_t;
using LifecycleTargetId = std::uint64_t;

enum class WaiterKind : std::uint8_t { clear, shutdown };

struct LifecycleResult
{
    enum class Outcome : std::uint8_t {
        cleared,                 // clear reached running(E+1)
        failed,                  // clear failed closed
        superseded_by_shutdown,  // shutdown takeover completed the clear
        stopped,                 // shutdown reached stopped(final epoch)
        cancelled,               // this waiter's caller left
    };
    Outcome outcome{};
    std::uint64_t value = 0;  // epoch / reason code, by outcome
};

class LifecycleWaiterLedger
{
public:
    using strand_type = DeterministicScheduler::strand_type;
    using Handler = asio::any_completion_handler<void(LifecycleResult)>;

    enum class Refusal : std::uint8_t {
        ok,
        off_strand,
        unknown_waiter,
    };

    explicit LifecycleWaiterLedger(strand_type strand)
        : strand_(std::move(strand))
    {
    }

    bool
    on_owner_strand() const
    {
        return strand_.running_in_this_thread();
    }

    /// Register one waiter. The handler will be posted to `executor`
    /// exactly once, whichever completion path fires first.
    //@@authority transition=provider_lab.register_waiter owner=provider_lab commit=strand
    std::pair<Refusal, LifecycleWaiterId>
    register_waiter(
        WaiterKind kind,
        LifecycleTargetId target,
        asio::any_io_executor executor,
        Handler handler)
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, 0};
        auto const id = ++next_waiter_;
        waiters_.emplace(
            id,
            Waiter{kind, target, std::move(executor), std::move(handler)});
        return {Refusal::ok, id};
    }

    /// Caller cancellation: removes ONLY this waiter, delivering the
    /// typed cancelled result to it. The lifecycle transaction is not
    /// affected. Cancelling an already-completed waiter is a no-op.
    //@@authority transition=provider_lab.cancel_waiter owner=provider_lab commit=strand
    Refusal
    cancel_waiter(LifecycleWaiterId id)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        auto it = waiters_.find(id);
        if (it == waiters_.end())
            return Refusal::unknown_waiter;
        deliver(
            std::move(it->second),
            {LifecycleResult::Outcome::cancelled, 0});
        waiters_.erase(it);
        return Refusal::ok;
    }

    /// Clear completion or failure: completes every remaining clear
    /// waiter for the target exactly once. Returns how many were
    /// completed; a duplicate completion therefore returns zero.
    //@@authority transition=provider_lab.complete_clear owner=provider_lab commit=strand
    std::size_t
    complete_clear(LifecycleTargetId target, LifecycleResult result)
    {
        if (!on_owner_strand())
            return 0;
        return complete_kind(WaiterKind::clear, target, result);
    }

    /// Shutdown takeover: every remaining clear waiter completes with
    /// superseded_by_shutdown; shutdown waiters keep waiting for
    /// stopped.
    //@@authority transition=provider_lab.shutdown_takeover owner=provider_lab commit=strand
    std::size_t
    shutdown_takeover(LifecycleTargetId target)
    {
        if (!on_owner_strand())
            return 0;
        return complete_kind(
            WaiterKind::clear,
            target,
            {LifecycleResult::Outcome::superseded_by_shutdown, 0});
    }

    /// Stopped: completes every remaining shutdown waiter for the
    /// target exactly once.
    //@@authority transition=provider_lab.complete_stopped owner=provider_lab commit=strand
    std::size_t
    complete_stopped(LifecycleTargetId target, std::uint64_t final_epoch)
    {
        if (!on_owner_strand())
            return 0;
        return complete_kind(
            WaiterKind::shutdown,
            target,
            {LifecycleResult::Outcome::stopped, final_epoch});
    }

    std::size_t
    pending_count() const
    {
        return on_owner_strand() ? waiters_.size() : 0;
    }

private:
    struct Waiter
    {
        WaiterKind kind;
        LifecycleTargetId target;
        asio::any_io_executor executor;
        Handler handler;
    };

    void
    deliver(Waiter&& w, LifecycleResult result)
    {
        asio::post(
            w.executor,
            [h = std::move(w.handler), result]() mutable {
                std::move(h)(result);
            });
    }

    std::size_t
    complete_kind(
        WaiterKind kind,
        LifecycleTargetId target,
        LifecycleResult result)
    {
        std::size_t completed = 0;
        for (auto it = waiters_.begin(); it != waiters_.end();)
        {
            if (it->second.kind == kind && it->second.target == target)
            {
                deliver(std::move(it->second), result);
                it = waiters_.erase(it);
                ++completed;
            }
            else
            {
                ++it;
            }
        }
        return completed;
    }

    strand_type strand_;
    LifecycleWaiterId next_waiter_ = 0;
    //@@authority state=provider_lab.lifecycle_waiters owner=provider_lab
    std::map<LifecycleWaiterId, Waiter> waiters_;
};

}  // namespace catl::provider_lab
