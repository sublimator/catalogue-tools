#pragma once

// Per-request context — placeholder for future session state.
// Cancellation is now handled by the || operator pattern in
// the HTTP session (no manual check points needed).
//
// Future: session ID, auth token, cookies, rate limit state, etc.

#include <memory>

namespace xproof {

class RequestContext
{
public:
    // Future session state goes here
};

}  // namespace xproof
