#include <catl/core/logger.h>
#include <catl/peer/peer-connection.h>
#include <catl/peer/txset-acquirer.h>
#include <cstdlib>
#include <iomanip>
#include <limits>
#include <sstream>

namespace catl::peer {

// Logging partition for transaction set acquisition
// Can be disabled with LOG_TXSET=0 environment variable
static LogPartition txset_partition("txset", []() -> LogLevel {
    const char* env = std::getenv("LOG_TXSET");
    if (env && std::string(env) == "0")
    {
        return LogLevel::NONE;  // Disable txset logging
    }
    return LogLevel::INFO;  // Default to INFO
}());

// Helper to create depth mask (only keep relevant bits for depth)
static Hash256
apply_depth_mask(Hash256 const& id, uint8_t depth)
{
    Hash256 result = id;

    // Zero out bits beyond the depth
    // Each depth level uses 4 bits (1 nibble)
    size_t bytes_to_keep = (depth + 1) / 2;

    // Clear bytes beyond what we need
    for (size_t i = bytes_to_keep + 1; i < 32; ++i)
    {
        result.data()[i] = 0;
    }

    // If depth is odd, clear lower 4 bits of the boundary byte
    if (depth % 2 == 0 && bytes_to_keep < 32)
    {
        result.data()[bytes_to_keep] &= 0xF0;
    }

    return result;
}

std::string
SHAMapNodeID::get_wire_string() const
{
    std::string result;
    result.reserve(33);

    // Add 32 bytes of id
    result.append(reinterpret_cast<const char*>(id.data()), 32);

    // Add 1 byte of depth
    result.push_back(static_cast<char>(depth));

    return result;
}

SHAMapNodeID
SHAMapNodeID::get_child(uint8_t branch) const
{
    if (branch >= 16)
    {
        PLOGE(
            txset_partition,
            "Invalid branch number: ",
            static_cast<int>(branch));
        return *this;
    }

    if (depth >= 64)
    {
        PLOGE(txset_partition, "Cannot get child of leaf node at depth 64");
        return *this;
    }

    SHAMapNodeID child(id, depth + 1);

    // Set the appropriate nibble (4 bits) for this depth
    size_t byte_index = depth / 2;
    if (depth % 2 == 0)
    {
        // Even depth: use upper 4 bits
        child.id.data()[byte_index] |= (branch << 4);
    }
    else
    {
        // Odd depth: use lower 4 bits
        child.id.data()[byte_index] |= branch;
    }

    // Apply depth mask to ensure clean ID
    child.id = apply_depth_mask(child.id, child.depth);

    return child;
}

bool
SHAMapNodeID::operator<(SHAMapNodeID const& other) const
{
    if (depth != other.depth)
        return depth < other.depth;

    return std::memcmp(id.data(), other.id.data(), 32) < 0;
}

bool
SHAMapNodeID::operator==(SHAMapNodeID const& other) const
{
    return depth == other.depth && id == other.id;
}

//------------------------------------------------------------------------------

TransactionSetAcquirer::TransactionSetAcquirer(
    std::string const& set_hash,
    std::vector<peer_connection*> connections,
    TransactionCallback on_transaction,
    CompletionCallback on_complete)
    : set_hash_(set_hash)
    , on_transaction_(std::move(on_transaction))
    , on_complete_(std::move(on_complete))
    , transaction_count_(0)
    , complete_(false)
    , failed_(false)
{
    // Initialize connection pool
    for (auto* conn : connections)
    {
        if (conn)
            connections_.push_back({conn, 0});
    }

    if (connections_.empty())
    {
        PLOGE(
            txset_partition,
            "TransactionSetAcquirer created with no connections!");
        failed_ = true;
    }
}

void
TransactionSetAcquirer::add_connection(peer_connection* conn)
{
    if (conn)
        connections_.push_back({conn, 0});
}

PooledConnection*
TransactionSetAcquirer::find_connection(peer_connection* conn)
{
    for (auto& pc : connections_)
    {
        if (pc.conn == conn)
            return &pc;
    }
    return nullptr;
}

void
TransactionSetAcquirer::record_response(peer_connection* conn)
{
    auto* pc = find_connection(conn);
    if (!pc)
        return;

    // Calculate latency if we have a request timestamp
    if (pc->request_sent != std::chrono::steady_clock::time_point{})
    {
        auto now = std::chrono::steady_clock::now();
        auto latency_ms =
            std::chrono::duration<double, std::milli>(now - pc->request_sent)
                .count();
        pc->total_latency += latency_ms;
        pc->successes++;

        PLOGD(
            txset_partition,
            "  📊 Response latency: ",
            static_cast<int>(latency_ms),
            "ms (avg: ",
            static_cast<int>(pc->avg_latency()),
            "ms)");
    }

    // Reset consecutive errors on success
    pc->errors = 0;
}

void
TransactionSetAcquirer::record_error(peer_connection* conn)
{
    auto* pc = find_connection(conn);
    if (!pc)
        return;

    pc->errors++;

    PLOGW(
        txset_partition,
        "  ⚠️ Connection error #",
        pc->errors,
        " (score: ",
        static_cast<int>(pc->score()),
        ")");

    // Put in cooldown if too many consecutive errors
    if (pc->errors >= MAX_CONSECUTIVE_ERRORS)
    {
        pc->cooldown_until =
            std::chrono::steady_clock::now() + COOLDOWN_DURATION;
        PLOGW(
            txset_partition,
            "  🚫 Connection in cooldown for ",
            COOLDOWN_DURATION.count(),
            "s");
    }
}

peer_connection*
TransactionSetAcquirer::get_best_connection()
{
    if (connections_.empty())
        return nullptr;

    // Round-robin through connections, skipping bad ones
    size_t start = next_conn_idx_;
    do
    {
        auto& pc = connections_[next_conn_idx_];
        next_conn_idx_ = (next_conn_idx_ + 1) % connections_.size();

        // Skip connections in cooldown or with too many errors
        if (pc.in_cooldown() || pc.errors >= MAX_CONSECUTIVE_ERRORS)
            continue;

        PLOGD(
            txset_partition,
            "  🎯 Using conn #",
            (next_conn_idx_ == 0 ? connections_.size() : next_conn_idx_) - 1,
            " (errs=",
            pc.errors,
            ", score=",
            static_cast<int>(pc.score()),
            ")");

        return pc.conn;
    } while (next_conn_idx_ != start);

    // All connections are bad - just use first one
    PLOGW(
        txset_partition,
        "All ",
        connections_.size(),
        " connections have errors, using first");
    return connections_[0].conn;
}

bool
TransactionSetAcquirer::try_retry()
{
    if (retry_count_ >= MAX_RETRIES)
    {
        PLOGW(
            txset_partition,
            "  ❌ Max retries (",
            MAX_RETRIES,
            ") reached, giving up");
        return false;
    }

    // Mark current connection as having an error
    if (last_request_conn_)
    {
        record_error(last_request_conn_);
    }

    retry_count_++;
    PLOGI(
        txset_partition,
        "  🔄 Retry #",
        retry_count_,
        "/",
        MAX_RETRIES,
        " - re-requesting missing nodes");

    // Re-request all nodes we haven't received yet
    for (auto const& node_id : requested_nodes_)
    {
        if (received_nodes_.count(node_id) == 0)
        {
            pending_requests_.push_back(node_id);
        }
    }

    if (pending_requests_.empty())
    {
        PLOGW(txset_partition, "  No missing nodes to retry");
        return false;
    }

    PLOGI(
        txset_partition,
        "  📝 Re-requesting ",
        pending_requests_.size(),
        " missing nodes");

    flush_pending_requests();
    return true;
}

void
TransactionSetAcquirer::start()
{
    PLOGI(
        txset_partition,
        "🌳 Starting transaction set acquisition for ",
        set_hash_.substr(0, 16),
        "...");

    // Request root node (depth 0, id all zeros)
    SHAMapNodeID root;
    request_node(root);
    flush_pending_requests();
}

void
TransactionSetAcquirer::request_node(SHAMapNodeID const& node_id)
{
    // Check if already requested or received
    if (requested_nodes_.count(node_id) > 0)
    {
        PLOGD(
            txset_partition,
            "Node already requested (depth=",
            static_cast<int>(node_id.depth),
            ")");
        return;
    }

    requested_nodes_.insert(node_id);

    PLOGD(
        txset_partition,
        "  📝 Queuing node at depth ",
        static_cast<int>(node_id.depth));

    // Add to pending batch
    pending_requests_.push_back(node_id);
}

void
TransactionSetAcquirer::flush_pending_requests()
{
    if (pending_requests_.empty())
        return;

    size_t num_nodes = pending_requests_.size();

    // Get best connection from pool (by score)
    auto* conn = get_best_connection();
    if (!conn)
    {
        PLOGE(txset_partition, "No connections available for request!");
        failed_ = true;
        check_completion();
        return;
    }

    PLOGI(txset_partition, "  📨 Requesting ", num_nodes, " nodes");

    // Convert to wire format
    std::vector<std::string> node_ids_wire;
    node_ids_wire.reserve(num_nodes);

    for (auto const& node_id : pending_requests_)
    {
        node_ids_wire.push_back(node_id.get_wire_string());
    }

    // Track which connection we're using and when
    last_request_conn_ = conn;
    peers_used_.insert(conn);  // Track unique peers
    auto* pc = find_connection(conn);
    if (pc)
        pc->request_sent = std::chrono::steady_clock::now();

    // Send the batch request
    conn->request_transaction_set_nodes(set_hash_, node_ids_wire);

    // Clear pending
    pending_requests_.clear();

    // Notify callback that a request was sent
    if (on_request_)
    {
        on_request_(num_nodes);
    }
}

void
TransactionSetAcquirer::on_node_received(
    SHAMapNodeID const& node_id,
    const uint8_t* data,
    size_t size)
{
    // Mark as received
    received_nodes_.insert(node_id);

    PLOGI(
        txset_partition,
        "  ✅ Received node at depth ",
        static_cast<int>(node_id.depth),
        " (",
        size,
        " bytes)");

    // Determine node type
    auto wire_type = get_wire_type(data, size);
    if (!wire_type)
    {
        PLOGE(txset_partition, "Invalid wire type for node");
        // Don't mark received - we'll retry
        received_nodes_.erase(node_id);
        if (!try_retry())
            failed_ = true;
        check_completion();
        return;
    }

    switch (*wire_type)
    {
        case SHAMapWireType::CompressedInner: {
            auto children = parse_compressed_inner_node(data, size);
            if (children.empty())
            {
                PLOGE(txset_partition, "Failed to parse inner node");
                received_nodes_.erase(node_id);
                if (!try_retry())
                    failed_ = true;
            }
            else
            {
                process_inner_node(node_id, children);
            }
            break;
        }

        case SHAMapWireType::Transaction: {
            auto tx_data = parse_transaction_leaf_node(data, size);
            if (tx_data.empty())
            {
                PLOGE(txset_partition, "Failed to parse transaction leaf");
                received_nodes_.erase(node_id);
                if (!try_retry())
                    failed_ = true;
            }
            else
            {
                process_leaf_node(node_id, tx_data);
            }
            break;
        }

        default:
            PLOGE(
                txset_partition,
                "Unsupported wire type: ",
                static_cast<int>(*wire_type));
            received_nodes_.erase(node_id);
            if (!try_retry())
                failed_ = true;
            break;
    }

    check_completion();
}

void
TransactionSetAcquirer::process_inner_node(
    SHAMapNodeID const& node_id,
    std::vector<InnerNodeChild> const& children)
{
    PLOGI(
        txset_partition, "  🌿 Inner node with ", children.size(), " children");

    for (auto const& child : children)
    {
        PLOGD(
            txset_partition,
            "    - Branch ",
            static_cast<int>(child.branch),
            ": ",
            child.hash.hex().substr(0, 16),
            "...");

        // Create child node ID
        SHAMapNodeID child_id = node_id.get_child(child.branch);

        // Request the child
        request_node(child_id);
    }

    // Flush all queued requests in a single batch
    flush_pending_requests();
}

void
TransactionSetAcquirer::process_leaf_node(
    SHAMapNodeID const& node_id,
    Slice const& tx_data)
{
    // Calculate transaction hash from the data
    // For now, just use a placeholder
    std::stringstream tx_hash_hex;
    tx_hash_hex << "tx_" << transaction_count_;

    PLOGI(txset_partition, "  🍃 Transaction leaf (", tx_data.size(), " bytes)");

    transaction_count_++;

    // Call transaction callback
    if (on_transaction_)
    {
        on_transaction_(tx_hash_hex.str(), tx_data);
    }
}

void
TransactionSetAcquirer::check_completion()
{
    if (complete_)
        return;

    // Complete if we've received all requested nodes
    if (received_nodes_.size() == requested_nodes_.size())
    {
        complete_ = true;

        PLOGI(txset_partition, "✅ Transaction set acquisition complete!");
        PLOGI(txset_partition, "   Transactions found: ", transaction_count_);
        PLOGI(txset_partition, "   Nodes processed: ", received_nodes_.size());

        if (on_complete_)
        {
            on_complete_(!failed_, transaction_count_, peers_used_.size());
        }
    }
}

}  // namespace catl::peer
