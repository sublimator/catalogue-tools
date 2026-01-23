#include <catl/core/logger.h>
#include <catl/peer/peer-connection.h>
#include <catl/peer/shared-node-cache.h>
#include <catl/peer/txset-acquirer.h>
#include <catl/shamap/shamap-nodetype.h>
#include <cstdlib>
#include <iomanip>
#include <limits>
#include <sstream>

namespace catl::peer {

// Helper to parse hex string to Hash256
static Hash256
hash256_from_hex(std::string const& hex)
{
    Hash256 result;
    if (hex.size() != 64)
        return result;

    for (size_t i = 0; i < 32; ++i)
    {
        unsigned int byte;
        std::sscanf(hex.c_str() + i * 2, "%2x", &byte);
        result.data()[i] = static_cast<uint8_t>(byte);
    }
    return result;
}

// OwnedMmapItem - owns its memory for use in SHAMap
class OwnedMmapItem : public MmapItem
{
private:
    std::unique_ptr<uint8_t[]> owned_memory_;

    OwnedMmapItem(
        const uint8_t* key_ptr,
        const uint8_t* data_ptr,
        std::size_t data_size,
        std::unique_ptr<uint8_t[]> owned_memory)
        : MmapItem(key_ptr, data_ptr, data_size)
        , owned_memory_(std::move(owned_memory))
    {
    }

public:
    static boost::intrusive_ptr<MmapItem>
    create(Hash256 const& key, Slice const& data)
    {
        // One allocation: [32-byte key][variable data]
        std::size_t total_size = 32 + data.size();
        auto owned_memory = std::make_unique<uint8_t[]>(total_size);

        // Copy key to start of buffer
        std::memcpy(owned_memory.get(), key.data(), 32);

        // Copy data after key
        std::memcpy(owned_memory.get() + 32, data.data(), data.size());

        // Create slices into the single buffer
        const uint8_t* key_ptr = owned_memory.get();
        const uint8_t* data_ptr = owned_memory.get() + 32;

        return boost::intrusive_ptr<MmapItem>(new OwnedMmapItem(
            key_ptr, data_ptr, data.size(), std::move(owned_memory)));
    }
};

// Logging partition for transaction set acquisition
static LogPartition txset_partition("txset", LogLevel::INFO);

// Helper to create depth mask (only keep relevant bits for depth)
// Depth equals number of hex nibbles (4-bit chunks) in the path
static Hash256
apply_depth_mask(Hash256 const& id, uint8_t depth)
{
    Hash256 result = id;
    size_t nibbles = depth;
    size_t full_bytes = nibbles / 2;

    // Clear bytes after the boundary
    for (size_t i = full_bytes + 1; i < 32; ++i)
    {
        result.data()[i] = 0;
    }

    // Handle the boundary byte
    if (nibbles % 2 == 0)
    {
        // Even depth (0, 2, 4...): clear the byte at full_bytes entirely
        // depth=0 means root, entire ID should be 0
        if (full_bytes < 32)
            result.data()[full_bytes] = 0;
    }
    else
    {
        // Odd depth (1, 3, 5...): keep upper nibble, clear lower nibble
        if (full_bytes < 32)
            result.data()[full_bytes] &= 0xF0;
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

std::optional<SHAMapNodeID>
SHAMapNodeID::get_child(uint8_t branch) const
{
    if (branch >= 16)
    {
        PLOGE(
            txset_partition,
            "Invalid branch number: ",
            static_cast<int>(branch));
        return std::nullopt;
    }

    if (depth >= 64)
    {
        PLOGE(txset_partition, "Cannot get child of leaf node at depth 64");
        return std::nullopt;
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
    CompletionCallback on_complete,
    SharedNodeCache* cache)
    : set_hash_(set_hash)
    , on_transaction_(std::move(on_transaction))
    , on_complete_(std::move(on_complete))
    , transaction_count_(0)
    , complete_(false)
    , failed_(false)
    , cache_(cache)
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

    // Create SHAMap for verification (transaction set, no metadata)
    verification_map_ =
        std::make_unique<shamap::SHAMap>(shamap::tnTRANSACTION_NM);

    // Parse expected hash from hex string
    if (set_hash_.size() == 64)
    {
        expected_hash_ = hash256_from_hex(set_hash_);
        PLOGI(
            txset_partition,
            "Created verification map, expecting hash: ",
            expected_hash_.hex().substr(0, 16),
            "...");
    }
    else
    {
        PLOGE(
            txset_partition,
            "Invalid set_hash length: ",
            set_hash_.size(),
            " (expected 64)");
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

    // Promote this peer to preferred if it has better score than current
    // (dynamic affinity based on actual performance)
    size_t this_idx = static_cast<size_t>(pc - connections_.data());
    if (this_idx != preferred_conn_idx_ &&
        preferred_conn_idx_ < connections_.size())
    {
        auto& preferred = connections_[preferred_conn_idx_];
        // Promote if this peer has at least 3 successes and 20% better score
        if (pc->successes >= 3 && pc->score() < preferred.score() * 0.8)
        {
            PLOGI(
                txset_partition,
                "  ⬆️ Promoting conn #",
                this_idx,
                " to preferred (score ",
                static_cast<int>(pc->score()),
                " < ",
                static_cast<int>(preferred.score()),
                ")");
            preferred_conn_idx_ = this_idx;
        }
    }
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

    // First, try the preferred peer (source of proposal) if available
    if (preferred_conn_idx_ < connections_.size())
    {
        auto& preferred = connections_[preferred_conn_idx_];
        if (!preferred.in_cooldown() &&
            preferred.errors < MAX_CONSECUTIVE_ERRORS)
        {
            PLOGD(
                txset_partition,
                "  🎯 Using preferred conn #",
                preferred_conn_idx_,
                " (errs=",
                preferred.errors,
                ", score=",
                static_cast<int>(preferred.score()),
                ")");
            return preferred.conn;
        }
    }

    // Preferred is unavailable - round-robin through others, skipping bad ones
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
            "  🎯 Using fallback conn #",
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
    // Root's expected hash is the txset_hash itself
    SHAMapNodeID root;
    request_node_with_hash(root, expected_hash_);
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
TransactionSetAcquirer::request_node_with_hash(
    SHAMapNodeID const& node_id,
    Hash256 const& hash)
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

    // Store expected hash for verification when received
    expected_hashes_[node_id] = hash;

    // If we have a shared cache, try to get from there first
    if (cache_)
    {
        auto result = cache_->get_or_claim(
            hash, [this, node_id, hash](bool success, Slice data) {
                // Callback when another acquirer resolves this node
                if (success && !complete_ && !failed_)
                {
                    PLOGI(
                        txset_partition,
                        "  📦 Got node from cache waiter (depth=",
                        static_cast<int>(node_id.depth),
                        ")");
                    process_cached_node(node_id, hash, data);
                }
            });

        switch (result.result)
        {
            case SharedNodeCache::ClaimResult::Ready:
                // Already in cache! Process directly
                PLOGI(
                    txset_partition,
                    "  ✨ Cache HIT for node at depth ",
                    static_cast<int>(node_id.depth));
                requested_nodes_.insert(node_id);
                process_cached_node(
                    node_id,
                    hash,
                    Slice(result.data.data(), result.data.size()));
                return;

            case SharedNodeCache::ClaimResult::Waiting:
                // Someone else is fetching, we're queued as waiter
                PLOGI(
                    txset_partition,
                    "  ⏳ Waiting for node at depth ",
                    static_cast<int>(node_id.depth),
                    " (another acquirer fetching)");
                requested_nodes_.insert(node_id);
                return;

            case SharedNodeCache::ClaimResult::Claimed:
                // We claimed it, proceed to fetch
                PLOGD(
                    txset_partition,
                    "  📝 Claimed node at depth ",
                    static_cast<int>(node_id.depth),
                    " (will fetch)");
                break;
        }
    }

    // No cache or we claimed it - proceed with network request
    requested_nodes_.insert(node_id);
    pending_requests_.push_back(node_id);
}

void
TransactionSetAcquirer::process_cached_node(
    SHAMapNodeID const& node_id,
    Hash256 const& expected_hash,
    Slice const& data)
{
    // Mark as received
    received_nodes_.insert(node_id);

    // Process the node data (same as on_node_received but without network
    // stuff)
    auto wire_type = get_wire_type(data.data(), data.size());
    if (!wire_type)
    {
        PLOGE(txset_partition, "Invalid wire type for cached node");
        received_nodes_.erase(node_id);
        if (cache_)
            cache_->reject(expected_hash);
        return;
    }

    switch (*wire_type)
    {
        case SHAMapWireType::CompressedInner: {
            auto children =
                parse_compressed_inner_node(data.data(), data.size());
            if (children.empty())
            {
                PLOGE(txset_partition, "Failed to parse compressed inner node");
                received_nodes_.erase(node_id);
            }
            else
            {
                process_inner_node(node_id, children);
            }
            break;
        }

        case SHAMapWireType::Inner: {
            auto children =
                parse_uncompressed_inner_node(data.data(), data.size());
            if (children.empty())
            {
                PLOGW(
                    txset_partition,
                    "Uncompressed inner node has no children (or parse "
                    "failed)");
            }
            else
            {
                process_inner_node(node_id, children);
            }
            break;
        }

        case SHAMapWireType::Transaction: {
            auto tx_data =
                parse_transaction_leaf_node(data.data(), data.size());
            if (tx_data.empty())
            {
                PLOGE(txset_partition, "Failed to parse transaction leaf");
                received_nodes_.erase(node_id);
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
                "Unsupported wire type in cached node: ",
                static_cast<int>(*wire_type));
            received_nodes_.erase(node_id);
            break;
    }

    check_completion();
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
    last_request_time_ = std::chrono::steady_clock::now();
    peers_used_.insert(conn);  // Track unique peers
    auto* pc = find_connection(conn);
    if (pc)
        pc->request_sent = last_request_time_;

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

    // Look up expected hash for this node (if using cache)
    Hash256 expected_hash;
    bool have_expected_hash = false;
    auto hash_it = expected_hashes_.find(node_id);
    if (hash_it != expected_hashes_.end())
    {
        expected_hash = hash_it->second;
        have_expected_hash = true;
    }

    // Determine node type
    auto wire_type = get_wire_type(data, size);
    if (!wire_type)
    {
        PLOGE(txset_partition, "Invalid wire type for node");
        // Don't mark received - we'll retry
        received_nodes_.erase(node_id);
        if (cache_ && have_expected_hash)
            cache_->reject(expected_hash);
        if (!try_retry())
            failed_ = true;
        check_completion();
        return;
    }

    // Resolve in cache so other acquirers can use this data
    if (cache_ && have_expected_hash)
    {
        cache_->resolve(expected_hash, Slice(data, size));
    }

    switch (*wire_type)
    {
        case SHAMapWireType::CompressedInner: {
            auto children = parse_compressed_inner_node(data, size);
            if (children.empty())
            {
                PLOGE(txset_partition, "Failed to parse compressed inner node");
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

        case SHAMapWireType::Inner: {
            auto children = parse_uncompressed_inner_node(data, size);
            if (children.empty())
            {
                // Note: empty children is valid for uncompressed nodes
                // (all 16 branches could theoretically be empty, though
                // unlikely)
                PLOGW(
                    txset_partition,
                    "Uncompressed inner node has no children (or parse "
                    "failed)");
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
        auto child_id_opt = node_id.get_child(child.branch);
        if (!child_id_opt)
        {
            PLOGE(
                txset_partition,
                "Failed to create child ID for branch ",
                static_cast<int>(child.branch));
            failed_ = true;
            pending_requests_.clear();  // Don't leave orphaned requests
            return;
        }

        // Request the child with its known hash (enables caching)
        request_node_with_hash(*child_id_opt, child.hash);
    }

    // Flush all queued requests in a single batch
    flush_pending_requests();
}

void
TransactionSetAcquirer::process_leaf_node(
    SHAMapNodeID const& node_id,
    Slice const& tx_data)
{
    // Compute transaction ID (SHA512-Half of tx data)
    Hash256 tx_id = add_to_verification_map(tx_data);

    PLOGI(
        txset_partition,
        "  🍃 Transaction leaf (",
        tx_data.size(),
        " bytes) id=",
        tx_id.hex().substr(0, 16),
        "...");

    transaction_count_++;

    // Call transaction callback
    if (on_transaction_)
    {
        on_transaction_(tx_id.hex(), tx_data);
    }
}

Hash256
TransactionSetAcquirer::add_to_verification_map(Slice const& tx_data)
{
    // Transaction ID = SHA512-Half("TXN\0" + tx_blob)
    // The "TXN\0" prefix is HashPrefix::transactionID = 0x54584E00
    static constexpr uint8_t tx_id_prefix[4] = {'T', 'X', 'N', 0x00};

    crypto::Sha512HalfHasher hasher;
    hasher.update(tx_id_prefix, 4);
    hasher.update(tx_data.data(), tx_data.size());
    Hash256 tx_id = hasher.finalize();

    // Create OwnedMmapItem that owns a copy of the data
    auto item = OwnedMmapItem::create(tx_id, tx_data);

    // Add to verification map
    if (verification_map_)
    {
        auto result = verification_map_->add_item(item);
        if (result == shamap::SetResult::FAILED)
        {
            PLOGW(
                txset_partition,
                "Failed to add tx to verification map: ",
                static_cast<int>(result));
        }
    }

    return tx_id;
}

void
TransactionSetAcquirer::check_completion()
{
    if (complete_)
        return;

    PLOGD(
        txset_partition,
        "check_completion: requested=",
        requested_nodes_.size(),
        " received=",
        received_nodes_.size(),
        " pending=",
        pending_requests_.size(),
        " txns=",
        transaction_count_,
        " failed=",
        failed_);

    // Check for explicit failure state first (e.g., retries exhausted)
    if (failed_)
    {
        PLOGE(
            txset_partition,
            "❌ FAILED: requested=",
            requested_nodes_.size(),
            " received=",
            received_nodes_.size());
        complete_ = true;
        if (on_complete_)
        {
            on_complete_(
                false,
                transaction_count_,
                peers_used_.size(),
                requested_nodes_.size(),
                received_nodes_.size(),
                "");
        }
        return;
    }

    // Complete if we've received all requested nodes
    if (received_nodes_.size() == requested_nodes_.size())
    {
        PLOGI(
            txset_partition,
            "⚡ COMPLETING: requested=",
            requested_nodes_.size(),
            " received=",
            received_nodes_.size());
        complete_ = true;

        // Verify the computed hash matches the expected hash
        bool hash_matches = false;
        Hash256 computed_hash;

        if (verification_map_)
        {
            computed_hash = verification_map_->get_hash();
            hash_matches = (computed_hash == expected_hash_);

            PLOGI(
                txset_partition,
                "🔍 Hash check: txn_count=",
                transaction_count_);
            PLOGI(txset_partition, "   Expected: ", expected_hash_.hex());
            PLOGI(txset_partition, "   Computed: ", computed_hash.hex());

            if (hash_matches)
            {
                PLOGI(txset_partition, "✅ Transaction set VERIFIED!");
            }
            else
            {
                PLOGE(txset_partition, "❌ HASH MISMATCH!");
                failed_ = true;
            }
        }
        else
        {
            PLOGE(txset_partition, "❌ No verification map!");
            failed_ = true;
        }

        PLOGI(txset_partition, "   Transactions found: ", transaction_count_);
        PLOGI(txset_partition, "   Nodes processed: ", received_nodes_.size());

        if (on_complete_)
        {
            on_complete_(
                !failed_ && hash_matches,
                transaction_count_,
                peers_used_.size(),
                requested_nodes_.size(),
                received_nodes_.size(),
                computed_hash.hex());
        }
    }
}

bool
TransactionSetAcquirer::check_timeout()
{
    if (complete_ || failed_)
        return false;

    // Check if we have pending nodes (requested but not received)
    if (received_nodes_.size() >= requested_nodes_.size())
        return false;  // Nothing pending

    // Check if enough time has passed since last request
    auto now = std::chrono::steady_clock::now();
    if (now - last_request_time_ < REQUEST_TIMEOUT)
        return false;  // Not timed out yet

    PLOGW(
        txset_partition,
        "⏱️ Request timeout! Pending nodes: ",
        requested_nodes_.size() - received_nodes_.size(),
        " (retry ",
        retry_count_ + 1,
        "/",
        MAX_RETRIES,
        ")");

    // Record error for the connection that timed out
    if (last_request_conn_)
        record_error(last_request_conn_);

    // Try to retry with a different peer
    if (!try_retry())
    {
        PLOGE(txset_partition, "❌ All retries exhausted after timeout");
        failed_ = true;
        check_completion();
    }

    return true;
}

}  // namespace catl::peer
