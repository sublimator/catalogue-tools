#pragma once

#include <atomic>
#include <chrono>
#include <compare>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

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

struct RequestCommitId
{
    std::uint64_t value = 0;

    explicit
    operator bool() const noexcept
    {
        return value != 0;
    }

    auto
    operator<=>(RequestCommitId const&) const = default;
};

enum class RequestScopeTerminal : std::uint8_t {
    cancelled,
    deadline_elapsed,
    completed,
};

struct RequestScopeSnapshot
{
    RequestScopeId id;
    std::chrono::steady_clock::time_point absolute_deadline;
    std::optional<RequestScopeTerminal> terminal;
    RequestCommitId last_accepted_commit;

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
    using TerminalObserver = std::function<void(RequestScopeTerminal)>;

private:
    struct ObserverCell;

public:
    /**
     * Passive subscription to the sticky terminal decision.
     *
     * Resetting only prevents a future callback; it never changes request
     * state. The callback runs after the request mutex is released.
     */
    class Subscription
    {
    public:
        Subscription() = default;
        Subscription(Subscription const&) = delete;
        Subscription&
        operator=(Subscription const&) = delete;
        Subscription(Subscription&& other) noexcept
            : cell_(std::move(other.cell_))
        {
        }
        Subscription&
        operator=(Subscription&& other) noexcept
        {
            if (this != &other)
            {
                reset();
                cell_ = std::move(other.cell_);
            }
            return *this;
        }

        ~Subscription();

        explicit
        operator bool() const noexcept
        {
            return static_cast<bool>(cell_);
        }

        void
        reset() noexcept;

    private:
        friend class RequestScope;
        explicit Subscription(std::shared_ptr<ObserverCell> cell)
            : cell_(std::move(cell))
        {
        }

        std::shared_ptr<ObserverCell> cell_;
    };

    RequestScope(RequestScope const&) = delete;
    RequestScope&
    operator=(RequestScope const&) = delete;

    /// Last-owner fallback. Explicit cancellation/deadline always remains
    /// sticky; otherwise destruction closes every subscribed child as
    /// completed so an abandoned owner cannot retain authority until deadline.
    ~RequestScope();

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

    /// Register one terminal observer. If already terminal, the callback is
    /// invoked before this function returns and the returned token is inert.
    Subscription
    subscribe(TerminalObserver observer);

    /**
     * Atomically choose a live request commit against cancellation/deadline.
     *
     * `apply` executes while the request mutex is held and therefore must be
     * non-throwing, non-blocking, allocation-free, and must not invoke foreign
     * code. The returned identity is the linearization witness. Cancellation
     * and every participant spend use this same mutex; callers that also hold
     * a participant cell must always acquire that cell first.
     */
    template <class Apply>
    //@@authority transition=prv.T0264 status=provisional scope=function
    // bind=catl::core::RequestScope::try_commit@933c99977bcd3d4b
    // owner=unresolved owner_hypothesis=request_scope
    // commit=request_scope_mutex touches=prv.S0103,prv.S0106
    // label=accept_nonreused_request_commit_before_terminal
    std::optional<RequestCommitId>
    try_commit(Clock::time_point now, Apply&& apply) noexcept
    {
        static_assert(
            std::is_nothrow_invocable_v<Apply&, RequestCommitId>,
            "request commit mutation must be noexcept");

        ObserverList observers;
        std::optional<RequestCommitId> accepted;
        {
            std::lock_guard lock(mutex_);
            latch_deadline_locked(now, observers);
            if (!terminal_ &&
                next_commit_id_ != std::numeric_limits<std::uint64_t>::max())
            {
                RequestCommitId const id{next_commit_id_};
                std::invoke(apply, id);
                last_accepted_commit_ = id;
                ++next_commit_id_;
                accepted = id;
            }
        }
        notify_observers(observers, RequestScopeTerminal::deadline_elapsed);
        return accepted;
    }

    /// Observe/latch deadline terminality and return an immutable snapshot.
    RequestScopeSnapshot
    snapshot(Clock::time_point now = Clock::now()) const noexcept;

private:
    struct ObserverCell
    {
        explicit ObserverCell(TerminalObserver callback_)
            : callback(std::move(callback_))
        {
        }

        std::atomic<bool> active{true};
        TerminalObserver callback;
    };

    using ObserverList = std::vector<std::shared_ptr<ObserverCell>>;

    RequestScope(RequestScopeId id, Clock::time_point absolute_deadline)
        : id_(id), absolute_deadline_(absolute_deadline)
    {
    }

    bool
    latch_deadline_locked(Clock::time_point now, ObserverList& observers)
        const noexcept;

    void
    close_from_last_owner() noexcept;

    static void
    notify_observers(
        ObserverList& observers,
        RequestScopeTerminal terminal) noexcept;

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
    mutable ObserverList observers_;
    //@@authority state=prv.S0106 status=provisional scope=field
    // bind=catl::core::RequestScope::next_commit_id_@5501bcdfacdb3a60
    // owner=unresolved owner_hypothesis=request_scope
    // observed_home=request_scope
    // label=nonreused_request_commit_sequence_and_last_accepted_commit
    std::uint64_t next_commit_id_ = 1;
    RequestCommitId last_accepted_commit_;
};

}  // namespace catl::core
