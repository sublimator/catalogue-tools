#include <catl/core/request-scope.h>

#include <atomic>
#include <limits>

namespace catl::core {
namespace {

std::atomic<std::uint64_t> next_request_scope_id{1};

std::optional<RequestScopeId>
mint_request_scope_id() noexcept
{
    auto current = next_request_scope_id.load(std::memory_order_relaxed);
    for (;;)
    {
        if (current == 0 ||
            current == std::numeric_limits<std::uint64_t>::max())
            return std::nullopt;
        if (next_request_scope_id.compare_exchange_weak(
                current,
                current + 1,
                std::memory_order_relaxed,
                std::memory_order_relaxed))
        {
            return RequestScopeId{current};
        }
    }
}

}  // namespace

//@@authority transition=prv.T0258 status=provisional scope=function
// bind=catl::core::create@21d82afa70437edf
// owner=unresolved owner_hypothesis=request_scope commit=atomic_id_mint
// touches=prv.S0102 label=mint_one_nonreused_request_scope
std::shared_ptr<RequestScope>
RequestScope::create(Clock::time_point absolute_deadline)
{
    if (absolute_deadline <= Clock::now())
        return nullptr;
    auto const id = mint_request_scope_id();
    if (!id)
        return nullptr;
    return std::shared_ptr<RequestScope>(
        new RequestScope(*id, absolute_deadline));
}

void
RequestScope::latch_deadline_locked(Clock::time_point now) const noexcept
{
    if (!terminal_ && now >= absolute_deadline_)
        terminal_ = RequestScopeTerminal::deadline_elapsed;
}

//@@authority transition=prv.T0259 status=provisional scope=function
// bind=catl::core::cancel@89c66a090eb493de
// owner=unresolved owner_hypothesis=request_scope commit=mutex
// touches=prv.S0103 label=select_sticky_request_terminal_reason
bool
RequestScope::cancel(Clock::time_point now) noexcept
{
    std::lock_guard lock(mutex_);
    latch_deadline_locked(now);
    if (terminal_)
        return false;
    terminal_ = RequestScopeTerminal::cancelled;
    return true;
}

//@@authority transition=prv.T0260 status=provisional scope=function
// bind=catl::core::snapshot@7157ed11fb8bbdef
// owner=unresolved owner_hypothesis=request_scope commit=mutex
// touches=prv.S0102,prv.S0103 label=observe_and_latch_request_deadline
RequestScopeSnapshot
RequestScope::snapshot(Clock::time_point now) const noexcept
{
    std::lock_guard lock(mutex_);
    latch_deadline_locked(now);
    return RequestScopeSnapshot{id_, absolute_deadline_, terminal_};
}

}  // namespace catl::core
