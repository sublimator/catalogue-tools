#pragma once

#include <catl/core/types.h>
#include <catl/peer/wire-format.h>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <set>
#include <string>

namespace catl::peer {

// Forward declare
class peer_connection;

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
    SHAMapNodeID
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
    using CompletionCallback = std::function<
        void(bool success, size_t num_transactions, size_t peers_used)>;

    // Callback when a request batch is sent (for tracking)
    using RequestCallback = std::function<void(size_t nodes_requested)>;

    /**
     * Create acquirer with a pool of connections
     * @param set_hash Transaction set hash to acquire
     * @param connections Pool of connections to use (round-robin)
     * @param on_transaction Called for each transaction found
     * @param on_complete Called when acquisition completes or fails
     */
    TransactionSetAcquirer(
        std::string const& set_hash,
        std::vector<peer_connection*> connections,
        TransactionCallback on_transaction,
        CompletionCallback on_complete);

    // Set optional request callback (called each time a batch is sent)
    void
    set_request_callback(RequestCallback cb)
    {
        on_request_ = std::move(cb);
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

    // Track which connection was used for last request (for latency tracking)
    peer_connection* last_request_conn_ = nullptr;

    // Cooldown duration for bad connections
    static constexpr auto COOLDOWN_DURATION = std::chrono::seconds(30);
    static constexpr int MAX_CONSECUTIVE_ERRORS = 3;

    // Get best available connection (by score, skip cooldowns)
    peer_connection*
    get_best_connection();

    // Find pool entry for a connection
    PooledConnection*
    find_connection(peer_connection* conn);

    // Try to retry with a different peer, returns true if retry initiated
    bool
    try_retry();
};

}  // namespace catl::peer
