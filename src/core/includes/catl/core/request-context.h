#pragma once

// RequestContext — per-request metadata propagated via executor wrapper.
//
// The ContextExecutor sets a thread_local pointer to the active
// RequestContext before each handler runs and clears it after.
// Any code (logging, metrics, etc.) can check the thread_local
// to get the current request's ID without explicit parameter passing.
//
// Safe across thread migration when resumption is scheduled through the
// wrapper. A child born on another executor must carry the shared context as
// explicit data; neither awaiting post(other_executor) nor directly invoking a
// nested awaitable handler is a portable coroutine migration primitive.

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace catl::core {

struct RequestContext
{
    std::string request_id;  // 8-char hex, unique per request
    std::string tx_hash;     // abbreviated tx hash for log context
    std::chrono::steady_clock::time_point started_at;

    // ── Status event bus ────────────────────────────────────────
    // Any code running in this request's coroutine chain can push
    // status messages. The SSE handler drains them periodically.
    mutable std::mutex status_mutex;
    //@@authority state=prv.S0074 status=provisional scope=field
    // bind=catl::core::RequestContext::status_events@52d5358cb4a3d5fc
    // owner=unresolved owner_hypothesis=request_context
    // observed_home=request_context
    // label=request_owned_status_event_queue
    mutable std::vector<std::string> status_events;

    RequestContext() = default;

    RequestContext(
        std::string request_id_,
        std::string tx_hash_,
        std::chrono::steady_clock::time_point started_at_)
        : request_id(std::move(request_id_))
        , tx_hash(std::move(tx_hash_))
        , started_at(started_at_)
    {
    }

    //@@authority transition=prv.T0218 status=provisional scope=function
    // bind=catl::core::RequestContext::emit@e6c1f1a8c2b8b6f0
    // owner=unresolved owner_hypothesis=request_context commit=status_mutex
    // touches=prv.S0074
    // label=publish_request_owned_status_observation
    void
    emit(std::string msg) const
    {
        std::lock_guard lock(status_mutex);
        status_events.push_back(std::move(msg));
    }

    std::vector<std::string>
    drain() const
    {
        std::lock_guard lock(status_mutex);
        std::vector<std::string> out;
        out.swap(status_events);
        return out;
    }
};

/// Thread-local owner for the currently active request context.
/// Set by ContextExecutor before each handler, restored after. Keeping the
/// shared owner available lets nested executors preserve the context across
/// an intentional strand hop.
inline thread_local std::shared_ptr<RequestContext const>
    g_current_request_owner;

/// Borrow the active context for synchronous logging/status emission.
inline RequestContext const*
current_request() noexcept
{
    return g_current_request_owner.get();
}

/// Retain the active context across an asynchronous executor hop.
inline std::shared_ptr<RequestContext const>
retain_current_request()
{
    return g_current_request_owner;
}

/// Emit a status message to the current request's event bus (if any).
/// Safe to call from anywhere — no-op when no request is active.
inline void
emit_status(std::string msg)
{
    if (auto const* ctx = current_request())
        ctx->emit(std::move(msg));
}

/// Emit through an explicitly retained request capability. Use this when
/// serialization and request context deliberately have different executors.
inline void
emit_status(std::shared_ptr<RequestContext const> const& ctx, std::string msg)
{
    if (ctx)
        ctx->emit(std::move(msg));
}

/// RAII guard that sets/restores the thread_local.
struct RequestContextGuard
{
    std::shared_ptr<RequestContext const> prev;

    explicit RequestContextGuard(std::shared_ptr<RequestContext const> ctx)
        : prev(std::move(g_current_request_owner))
    {
        g_current_request_owner = std::move(ctx);
    }

    ~RequestContextGuard()
    {
        g_current_request_owner = std::move(prev);
    }

    RequestContextGuard(RequestContextGuard const&) = delete;
    RequestContextGuard&
    operator=(RequestContextGuard const&) = delete;
};

}  // namespace catl::core
