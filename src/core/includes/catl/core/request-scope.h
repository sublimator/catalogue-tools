#pragma once

#include <chrono>
#include <compare>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>

namespace catl::core {

struct RequestScopeId
{
    std::uint64_t value = 0;

    explicit
    operator bool() const noexcept
    {
        return value != 0;
    }

    auto
    operator<=>(RequestScopeId const&) const = default;
};

enum class RequestScopeTerminal : std::uint8_t {
    cancelled,
    deadline_elapsed,
};

struct RequestScopeSnapshot
{
    RequestScopeId id;
    std::chrono::steady_clock::time_point absolute_deadline;
    std::optional<RequestScopeTerminal> terminal;

    bool
    active() const noexcept
    {
        return !terminal.has_value();
    }
};

/**
 * One request-owned identity, deadline, and sticky terminal arbiter.
 *
 * The identity is process-unique and never reused. The absolute deadline is
 * immutable. Cancellation and deadline observation serialize through one
 * mutex, so exactly one terminal reason wins and later observations cannot
 * revive the scope or replace its reason.
 *
 * This object intentionally owns no peer, fetch, or dial authority. Cut 3
 * threads it through the proof stack so later participant grants can bind to
 * one request-wide terminal decision without resetting a duration at each
 * child wait.
 */
class RequestScope
{
public:
    using Clock = std::chrono::steady_clock;

    RequestScope(RequestScope const&) = delete;
    RequestScope&
    operator=(RequestScope const&) = delete;

    static std::shared_ptr<RequestScope>
    create(Clock::time_point absolute_deadline);

    RequestScopeId
    id() const noexcept
    {
        return id_;
    }

    Clock::time_point
    absolute_deadline() const noexcept
    {
        return absolute_deadline_;
    }

    /// Commit cancellation unless the immutable deadline already elapsed.
    /// Returns true only for the call that selected the terminal reason.
    bool
    cancel(Clock::time_point now = Clock::now()) noexcept;

    /// Observe/latch deadline terminality and return an immutable snapshot.
    RequestScopeSnapshot
    snapshot(Clock::time_point now = Clock::now()) const noexcept;

private:
    RequestScope(RequestScopeId id, Clock::time_point absolute_deadline)
        : id_(id), absolute_deadline_(absolute_deadline)
    {
    }

    void
    latch_deadline_locked(Clock::time_point now) const noexcept;

    //@@authority state=prv.S0102 status=provisional scope=field
    // bind=catl::core::RequestScope::id_@60a668cace66724a
    // owner=unresolved owner_hypothesis=request_scope
    // observed_home=request_scope
    // label=immutable_process_unique_request_identity_and_absolute_deadline
    RequestScopeId const id_;
    Clock::time_point const absolute_deadline_;

    mutable std::mutex mutex_;
    //@@authority state=prv.S0103 status=provisional scope=field
    // bind=catl::core::RequestScope::terminal_@4d3f35a1a4ce90ff
    // owner=unresolved owner_hypothesis=request_scope
    // observed_home=request_scope
    // label=sticky_request_cancel_or_deadline_terminal_winner
    mutable std::optional<RequestScopeTerminal> terminal_;
};

}  // namespace catl::core
