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

RequestScope::Subscription::~Subscription()
{
    reset();
}

void
RequestScope::Subscription::reset() noexcept
{
    if (cell_)
        cell_->active.store(false, std::memory_order_release);
    cell_.reset();
}

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

bool
RequestScope::latch_deadline_locked(
    Clock::time_point now,
    ObserverList& observers) const noexcept
{
    if (!terminal_ && now >= absolute_deadline_)
    {
        terminal_ = RequestScopeTerminal::deadline_elapsed;
        observers.swap(observers_);
        return true;
    }
    return false;
}

void
RequestScope::notify_observers(
    ObserverList& observers,
    RequestScopeTerminal terminal) noexcept
{
    for (auto const& observer : observers)
    {
        bool expected = true;
        if (!observer ||
            !observer->active.compare_exchange_strong(
                expected,
                false,
                std::memory_order_acq_rel,
                std::memory_order_acquire))
        {
            continue;
        }
        try
        {
            observer->callback(terminal);
        }
        catch (...)
        {
        }
    }
}

//@@authority transition=prv.T0259 status=provisional scope=function
// bind=catl::core::cancel@89c66a090eb493de
// owner=unresolved owner_hypothesis=request_scope commit=mutex
// touches=prv.S0103 label=select_sticky_request_terminal_reason
bool
RequestScope::cancel(Clock::time_point now) noexcept
{
    ObserverList observers;
    RequestScopeTerminal selected = RequestScopeTerminal::cancelled;
    bool won = false;
    {
        std::lock_guard lock(mutex_);
        if (latch_deadline_locked(now, observers))
        {
            selected = RequestScopeTerminal::deadline_elapsed;
        }
        else if (!terminal_)
        {
            terminal_ = RequestScopeTerminal::cancelled;
            observers.swap(observers_);
            won = true;
        }
    }
    notify_observers(observers, selected);
    return won;
}

RequestScope::Subscription
RequestScope::subscribe(TerminalObserver observer)
{
    if (!observer)
        return {};

    auto cell = std::make_shared<ObserverCell>(std::move(observer));
    ObserverList observers;
    std::optional<RequestScopeTerminal> terminal;
    {
        std::lock_guard lock(mutex_);
        latch_deadline_locked(Clock::now(), observers);
        terminal = terminal_;
        if (!terminal)
            observers_.push_back(cell);
        else
            observers.push_back(cell);
    }
    if (terminal)
        notify_observers(observers, *terminal);
    return Subscription{std::move(cell)};
}

//@@authority transition=prv.T0260 status=provisional scope=function
// bind=catl::core::snapshot@7157ed11fb8bbdef
// owner=unresolved owner_hypothesis=request_scope commit=mutex
// touches=prv.S0102,prv.S0103 label=observe_and_latch_request_deadline
RequestScopeSnapshot
RequestScope::snapshot(Clock::time_point now) const noexcept
{
    ObserverList observers;
    RequestScopeSnapshot result;
    {
        std::lock_guard lock(mutex_);
        latch_deadline_locked(now, observers);
        result = RequestScopeSnapshot{
            id_, absolute_deadline_, terminal_, last_accepted_commit_};
    }
    notify_observers(observers, RequestScopeTerminal::deadline_elapsed);
    return result;
}

}  // namespace catl::core
