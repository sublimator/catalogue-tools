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
#include <string>

namespace catl::core {

struct RequestContext
{
    std::string request_id;    // 8-char hex, unique per request
    std::string tx_hash;       // abbreviated tx hash for log context
    std::chrono::steady_clock::time_point started_at;
};

/// Thread-local pointer to the currently active request context.
/// Set by ContextExecutor before each handler, cleared after.
/// nullptr when no request is active (background work, startup, etc.)
inline thread_local RequestContext const* g_current_request = nullptr;

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
