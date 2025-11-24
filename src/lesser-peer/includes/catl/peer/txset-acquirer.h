#pragma once

#include <catl/core/types.h>
#include <catl/peer/wire-format.h>
#include <cstdint>
#include <functional>
#include <map>
#include <set>
#include <string>

namespace catl::peer {

// Forward declare
class peer_connection;

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
    using CompletionCallback =
        std::function<void(bool success, size_t num_transactions)>;

    TransactionSetAcquirer(
        std::string const& set_hash,
        peer_connection* connection,
        TransactionCallback on_transaction,
        CompletionCallback on_complete);

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
    peer_connection* connection_;
    TransactionCallback on_transaction_;
    CompletionCallback on_complete_;

    // Track requested and received nodes
    std::set<SHAMapNodeID> requested_nodes_;
    std::set<SHAMapNodeID> received_nodes_;

    // Batch pending requests
    std::vector<SHAMapNodeID> pending_requests_;

    size_t transaction_count_;
    bool complete_;
    bool failed_;
};

}  // namespace catl::peer
