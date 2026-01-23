#pragma once

#include <catl/core/types.h>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace catl::peer {

/**
 * Shared cache for SHAMap nodes, keyed by content hash (hex string).
 *
 * Enables multiple TransactionSetAcquirers to share nodes:
 * - Same transaction appearing in multiple txsets → fetched once
 * - Same inner node structure → fetched once
 *
 * Supports "promises" - if node is being fetched, waiters queue up
 * rather than duplicate the request.
 */
class SharedNodeCache
{
public:
    // Callback when a pending node resolves (success=true, data) or fails
    // (success=false)
    using WaiterCallback = std::function<void(bool success, Slice data)>;

    // Result of get_or_claim
    enum class ClaimResult {
        Ready,    // Node is ready, data is valid
        Claimed,  // You claimed it, you should fetch it
        Waiting   // Someone else is fetching, you're queued as waiter
    };

    struct GetResult
    {
        ClaimResult result;
        std::vector<uint8_t> data;  // Valid only if result == Ready
    };

    /**
     * Try to get a node by hash, or claim it for fetching.
     *
     * @param hash The content hash of the node we want
     * @param waiter If result is Waiting, this callback will be invoked
     *               when the node becomes ready or fails
     * @return GetResult with status and data (if ready)
     */
    GetResult
    get_or_claim(Hash256 const& hash, WaiterCallback waiter = nullptr);

    /**
     * Check if a node exists (ready) without claiming
     */
    bool
    has(Hash256 const& hash) const;

    /**
     * Resolve a pending node with data (success).
     * Notifies all waiters with (true, data).
     */
    void
    resolve(Hash256 const& hash, Slice data);

    /**
     * Reject a pending node (failure).
     * Notifies all waiters with (false, {}).
     * Removes entry so it can be retried later.
     */
    void
    reject(Hash256 const& hash);

    /**
     * Get cache statistics
     */
    struct Stats
    {
        size_t total_entries;
        size_t ready_entries;
        size_t pending_entries;
        size_t total_bytes;
    };
    Stats
    get_stats() const;

    /**
     * Clear all entries (for testing or reset)
     */
    void
    clear();

private:
    enum class State { Pending, Ready, Failed };

    struct Entry
    {
        State state = State::Pending;
        std::vector<uint8_t> data;
        std::vector<WaiterCallback> waiters;
    };

    mutable std::mutex mutex_;
    std::map<std::string, Entry> entries_;  // keyed by hash.hex()
};

}  // namespace catl::peer
