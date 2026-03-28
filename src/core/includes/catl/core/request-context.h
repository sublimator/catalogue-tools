#pragma once

// RequestContext — per-request metadata propagated via executor wrapper.
//
// The ContextExecutor sets a thread_local pointer to the active
// RequestContext before each handler runs and clears it after.
// Any code (logging, metrics, etc.) can check the thread_local
// to get the current request's ID without explicit parameter passing.
//
// Safe across thread migration: the executor wrapper handles
// save/restore on every co_await resume, regardless of which
// thread the coroutine lands on.

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace catl::core {

struct RequestContext
{
    std::string request_id;    // 8-char hex, unique per request
    std::string tx_hash;       // abbreviated tx hash for log context
    std::chrono::steady_clock::time_point started_at;

    // ── Status event bus ────────────────────────────────────────
    // Any code running in this request's coroutine chain can push
    // status messages. The SSE handler drains them periodically.
    mutable std::mutex status_mutex;
    mutable std::vector<std::string> status_events;

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

/// Thread-local pointer to the currently active request context.
/// Set by ContextExecutor before each handler, cleared after.
/// nullptr when no request is active (background work, startup, etc.)
inline thread_local RequestContext const* g_current_request = nullptr;

/// Emit a status message to the current request's event bus (if any).
/// Safe to call from anywhere — no-op when no request is active.
inline void
emit_status(std::string msg)
{
    if (auto const* ctx = g_current_request)
        ctx->emit(std::move(msg));
}

/// RAII guard that sets/restores the thread_local.
struct RequestContextGuard
{
    RequestContext const* prev;

    explicit RequestContextGuard(RequestContext const* ctx)
        : prev(g_current_request)
    {
        g_current_request = ctx;
    }

    ~RequestContextGuard()
    {
        g_current_request = prev;
    }

    RequestContextGuard(RequestContextGuard const&) = delete;
    RequestContextGuard& operator=(RequestContextGuard const&) = delete;
};

}  // namespace catl::core
