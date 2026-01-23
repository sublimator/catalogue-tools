#pragma once

#include <catl/core/types.h>
#include <catl/crypto/sha512-half-hasher.h>
#include <catl/peer/wire-format.h>
#include <catl/shamap/shamap.h>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>

namespace catl::peer {

// Forward declare
class peer_connection;
class SharedNodeCache;

/**
 * Entry in connection pool with performance tracking
 */
struct PooledConnection
{
    peer_connection* conn;
    int errors = 0;            // Consecutive error count
    int successes = 0;         // Successful response count
    double total_latency = 0;  // Sum of response times (ms)
    std::chrono::steady_clock::time_point
        cooldown_until{};  // Ignore until this time
    std::chrono::steady_clock::time_point
        request_sent{};  // When last request was sent

    // Average latency in ms (returns high value if no successes)
    double
    avg_latency() const
    {
        return successes > 0 ? total_latency / successes : 9999.0;
    }

    // Score for ranking (lower = better)
    // Combines error rate and latency
    double
    score() const
    {
        double error_penalty = errors * 100.0;  // 100ms penalty per error
        return avg_latency() + error_penalty;
    }

    // Check if in cooldown period
    bool
    in_cooldown() const
    {
        return std::chrono::steady_clock::now() < cooldown_until;
    }
};

/**
 * Represents a SHAMapNodeID for requesting nodes
 */
struct SHAMapNodeID
{
    Hash256 id;     // 32-byte path (masked by depth)
    uint8_t depth;  // Depth in tree (0 = root, max = 64)

    SHAMapNodeID() : id(), depth(0)
    {
    }
    SHAMapNodeID(Hash256 const& i, uint8_t d) : id(i), depth(d)
    {
    }

    // Get wire format: [32 bytes id][1 byte depth]
    std::string
    get_wire_string() const;

    // Create child node ID for given branch (0-15)
    // Returns nullopt if branch is invalid (>=16) or depth is at max
    std::optional<SHAMapNodeID>
    get_child(uint8_t branch) const;

    // Comparison for use in maps
    bool
    operator<(SHAMapNodeID const& other) const;
    bool
    operator==(SHAMapNodeID const& other) const;
};

/**
 * State machine for acquiring a complete transaction set from the network
 *
 * Workflow:
 * 1. Start with transaction set hash
 * 2. Request root node
 * 3. When nodes arrive:
 *    - If inner node: parse children and request them
 *    - If leaf node: call transaction callback
 * 4. Track completion when all nodes received
 */
class TransactionSetAcquirer
{
public:
    // Callback when a transaction is found
    using TransactionCallback =
        std::function<void(std::string const& tx_hash, Slice const& tx_data)>;

    // Callback when acquisition completes (success or failure)
    // unique_requested/unique_received are the actual node counts for debugging
    // computed_hash is the hash we computed from the acquired transactions
    using CompletionCallback = std::function<void(
        bool success,
        size_t num_transactions,
        size_t peers_used,
        size_t unique_requested,
        size_t unique_received,
        std::string const& computed_hash)>;

    // Callback when a request batch is sent (for tracking)
    using RequestCallback = std::function<void(size_t nodes_requested)>;

    /**
     * Create acquirer with a pool of connections
     * @param set_hash Transaction set hash to acquire
     * @param connections Pool of connections to use (round-robin)
     * @param on_transaction Called for each transaction found
     * @param on_complete Called when acquisition completes or fails
     * @param cache Optional shared cache for node deduplication
     */
    TransactionSetAcquirer(
        std::string const& set_hash,
        std::vector<peer_connection*> connections,
        TransactionCallback on_transaction,
        CompletionCallback on_complete,
        SharedNodeCache* cache = nullptr);

    // Set optional request callback (called each time a batch is sent)
    void
    set_request_callback(RequestCallback cb)
    {
        on_request_ = std::move(cb);
    }

    // Set shared cache (alternative to constructor param)
    void
    set_shared_cache(SharedNodeCache* cache)
    {
        cache_ = cache;
    }

    // Add a connection to the pool dynamically
    void
    add_connection(peer_connection* conn);

    // Record that a connection successfully responded
    // Call this when you receive data from a peer
    void
    record_response(peer_connection* conn);

    // Record that a connection failed/timed out
    // Puts connection in cooldown if too many errors
    void
    record_error(peer_connection* conn);

    /**
     * Start the acquisition by requesting the root node
     */
    void
    start();

    /**
     * Process a received node from TMLedgerData
     *
     * @param node_id The SHAMapNodeID from the response
     * @param node_data The raw node data
     */
    void
    on_node_received(
        SHAMapNodeID const& node_id,
        const uint8_t* data,
        size_t size);

    /**
     * Check if acquisition is complete
     */
    bool
    is_complete() const
    {
        return complete_;
    }

    /**
     * Check for timeout and retry if needed.
     * Call this periodically (e.g., when receiving any response).
     * Returns true if a retry was triggered.
     */
    bool
    check_timeout();

    /**
     * Get the transaction set hash being acquired
     */
    std::string const&
    get_set_hash() const
    {
        return set_hash_;
    }

    /**
     * Get number of transactions found so far
     */
    size_t
    get_transaction_count() const
    {
        return transaction_count_;
    }

    /**
     * Get number of unique peers used during acquisition
     */
    size_t
    get_peers_used() const
    {
        return peers_used_.size();
    }

    /**
     * Get number of unique nodes requested
     */
    size_t
    get_requested_count() const
    {
        return requested_nodes_.size();
    }

    /**
     * Get number of unique nodes received
     */
    size_t
    get_received_count() const
    {
        return received_nodes_.size();
    }

private:
    void
    request_node(SHAMapNodeID const& node_id);
    void
    flush_pending_requests();  // Send batched requests
    void
    process_inner_node(
        SHAMapNodeID const& node_id,
        std::vector<InnerNodeChild> const& children);
    void
    process_leaf_node(SHAMapNodeID const& node_id, Slice const& tx_data);
    void
    check_completion();

    std::string set_hash_;
    std::vector<PooledConnection> connections_;  // Connection pool
    size_t next_conn_idx_ = 0;                   // Round-robin index
    size_t preferred_conn_idx_ = 0;  // Preferred peer (source of proposal)
    TransactionCallback on_transaction_;
    CompletionCallback on_complete_;
    RequestCallback on_request_;

    // Track requested and received nodes
    std::set<SHAMapNodeID> requested_nodes_;
    std::set<SHAMapNodeID> received_nodes_;

    // Batch pending requests
    std::vector<SHAMapNodeID> pending_requests_;

    size_t transaction_count_;
    std::set<peer_connection*> peers_used_;   // Track unique peers used
    size_t retry_count_ = 0;                  // How many times we've retried
    static constexpr size_t MAX_RETRIES = 3;  // Max retries before giving up
    bool complete_;
    bool failed_;

    // SHAMap for verifying the transaction set hash
    // Uses tnTRANSACTION_NM (transaction, no metadata)
    std::unique_ptr<shamap::SHAMap> verification_map_;
    Hash256 expected_hash_;  // Parsed from set_hash_

    // Add transaction to verification map and return its hash
    Hash256
    add_to_verification_map(Slice const& tx_data);

    // Track which connection was used for last request (for latency tracking)
    peer_connection* last_request_conn_ = nullptr;

    // Cooldown duration for bad connections
    static constexpr auto COOLDOWN_DURATION = std::chrono::seconds(30);
    static constexpr int MAX_CONSECUTIVE_ERRORS = 3;

    // Request timeout - retry if no response within this time
    static constexpr auto REQUEST_TIMEOUT = std::chrono::seconds(5);
    std::chrono::steady_clock::time_point last_request_time_{};

    // Get best available connection (by score, skip cooldowns)
    peer_connection*
    get_best_connection();

    // Find pool entry for a connection
    PooledConnection*
    find_connection(peer_connection* conn);

    // Try to retry with a different peer, returns true if retry initiated
    bool
    try_retry();

    // Shared node cache (optional, for deduplication across acquirers)
    SharedNodeCache* cache_ = nullptr;

    // Map from node position to expected content hash
    // (learned from parent inner node, used for cache lookup/verification)
    std::map<SHAMapNodeID, Hash256> expected_hashes_;

    // Request a node with known expected hash (for cache integration)
    void
    request_node_with_hash(SHAMapNodeID const& node_id, Hash256 const& hash);

    // Process a node that was retrieved from cache
    void
    process_cached_node(
        SHAMapNodeID const& node_id,
        Hash256 const& hash,
        Slice const& data);
};

}  // namespace catl::peer
