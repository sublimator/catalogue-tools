#pragma once

// Per-request context passed through the prove pipeline.
// Carries cancellation state and will carry session/auth info later.

#include <atomic>
#include <memory>
#include <stdexcept>

namespace xproof {

/// Thrown when a request is cancelled (client disconnected).
class RequestCancelled : public std::runtime_error
{
public:
    RequestCancelled()
        : std::runtime_error("request cancelled (client disconnected)")
    {
    }
};

class RequestContext
{
    std::atomic<bool> cancelled_{false};

public:
    /// Check if the request has been cancelled (client disconnected).
    bool
    is_cancelled() const
    {
        return cancelled_.load(std::memory_order_relaxed);
    }

    /// Mark the request as cancelled. Called by HTTP session on error/close.
    void
    cancel()
    {
        cancelled_.store(true, std::memory_order_relaxed);
    }

    /// Throw RequestCancelled if cancelled. Call at safe check points
    /// (between tree walks, before header fetches, in retry loops).
    void
    check() const
    {
        if (is_cancelled())
            throw RequestCancelled();
    }

    // Future: session ID, auth token, cookies, rate limit state, etc.
};

}  // namespace xproof
