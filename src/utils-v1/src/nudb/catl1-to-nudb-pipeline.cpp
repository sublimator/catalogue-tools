#include "catl/utils-v1/nudb/catl1-to-nudb-pipeline.h"
#include "catl/core/log-macros.h"
#include "catl/core/logger.h"
#include "catl/shamap/shamap-innernode.h"
#include "catl/shamap/shamap-leafnode.h"
#include "catl/shamap/shamap-treenode.h"
#include "catl/shamap/shamap.h"      // For walk_nodes_log
#include "catl/v1/catl-v1-reader.h"  // For map_ops_log
#include "catl/v1/catl-v1-utils.h"
#include "catl/xdata/json-visitor.h"
#include "catl/xdata/parser-context.h"
#include "catl/xdata/parser.h"
#include "catl/xdata/protocol.h"
#include <boost/json.hpp>
#include <chrono>
#include <iostream>
#include <stdexcept>

namespace catl::v1::utils::nudb {

// LogPartition for pipeline version tracking - disabled by default
// Enable with: pipeline_version_log.enable(LogLevel::DEBUG)
LogPartition pipeline_version_log("PIPE_VERSION", LogLevel::NONE);

CatlNudbPipeline::CatlNudbPipeline(
    const shamap::SHAMapOptions& map_options,
    const catl::xdata::Protocol& protocol)
    : map_options_(map_options), protocol_(protocol)
{
}

void
CatlNudbPipeline::set_hasher_threads(int threads)
{
    // Validate it's a power of 2
    if (threads <= 0 || (threads & (threads - 1)) != 0 || threads > 16)
    {
        throw std::invalid_argument(
            "Hasher threads must be power of 2 (1, 2, 4, 8, or 16)");
    }
    hasher_threads_ = threads;
    LOGI("Set hasher threads to ", threads);
}

void
CatlNudbPipeline::set_walk_nodes_ledger(uint32_t ledger_seq)
{
    walk_nodes_ledger_ = ledger_seq;
    LOGD("Set walk_nodes_ledger to ", ledger_seq);
}

void
CatlNudbPipeline::set_walk_nodes_debug_key(const std::string& key_hex)
{
    walk_nodes_debug_key_ = key_hex;
    LOGD("Set walk_nodes_debug_key to ", key_hex);
}

Hash256
CatlNudbPipeline::parallel_hash(const std::shared_ptr<shamap::SHAMap>& map)
{
    if (hasher_threads_ == 1)
    {
        // Single threaded - just hash directly, no thread pool overhead
        LOGD("Using single-threaded hashing");
        return map->get_hash();
    }

    // Multi-threaded hashing using thread pool
    std::vector<std::future<void>> futures;
    futures.reserve(hasher_threads_);

    LOGD("Starting parallel hash with ", hasher_threads_, " threads");
    auto start_time = std::chrono::steady_clock::now();

    // Launch worker threads
    for (int i = 0; i < hasher_threads_; ++i)
    {
        auto job = map->get_hash_job(i, hasher_threads_);
        futures.push_back(std::async(std::launch::async, job));
    }

    // Wait for all workers to complete
    for (auto& future : futures)
    {
        future.wait();
    }

    auto parallel_time = std::chrono::steady_clock::now();

    // Now do the final hash from the main thread (should be very fast)
    Hash256 result = map->get_hash();

    auto finish_time = std::chrono::steady_clock::now();

    auto parallel_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                           parallel_time - start_time)
                           .count();
    auto final_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        finish_time - parallel_time)
                        .count();

    LOGD(
        "Parallel hash complete: ",
        parallel_ms,
        "ms parallel + ",
        final_ms,
        "ms final");

    return result;
}

std::optional<LedgerSnapshot>
CatlNudbPipeline::build_and_snapshot(
    Reader& reader,
    std::shared_ptr<shamap::SHAMap>& state_map,
    bool allow_deltas)
{
    bool did_enable_map_ops = false;  // Track if we enabled MAP_OPS for cleanup

    try
    {
        // Read ledger info
        auto v1_ledger_info = reader.read_ledger_info();
        auto canonical_info = to_canonical_ledger_info(v1_ledger_info);

        LOGD("Building ledger ", canonical_info.seq);

        // Check if we should enable MAP_OPS logging for this specific ledger
        bool should_enable_map_ops =
            walk_nodes_ledger_ && (*walk_nodes_ledger_ == canonical_info.seq);

        if (should_enable_map_ops)
        {
            LOGD("Enabling MAP_OPS logging for ledger ", canonical_info.seq);
            catl::v1::map_ops_log.enable(LogLevel::DEBUG);
            did_enable_map_ops = true;
        }

        // NEW APPROACH: Take a snapshot FIRST, then build on the snapshot
        // This gives us predictable version numbers
        PLOGD(
            pipeline_version_log,
            "Ledger ",
            canonical_info.seq,
            " - Taking working snapshot from state_map (version ",
            state_map->get_version(),
            ")");

        auto working_snapshot = state_map->snapshot();
        PLOGD(
            pipeline_version_log,
            "  Working snapshot created with version: ",
            working_snapshot->get_version(),
            ", state_map remains at: ",
            state_map->get_version());

        // The processing_version will be the snapshot's version after lazy bump
        // (which happens on first write)
        int pre_processing_version = working_snapshot->get_version();

        // Debug: Check root status before processing
        auto root_before = working_snapshot->get_root();
        if (root_before)
        {
            LOGD(
                "  Root before processing: version=",
                root_before->get_version(),
                " children=",
                root_before->get_branch_count());
            PLOGD(
                pipeline_version_log,
                "  Root version before processing: ",
                root_before->get_version());
        }

        // Load working snapshot with deltas
        PLOGD(
            pipeline_version_log,
            "  About to read into working snapshot with allow_deltas=",
            allow_deltas);
        MapOperations state_ops = reader.read_map_with_shamap_owned_items(
            *working_snapshot, shamap::tnACCOUNT_STATE, allow_deltas);

        // NOW we can capture the actual processing version (after lazy bump)
        int processing_version = working_snapshot->get_version();
        LOGD("  Processing version (after operations): ", processing_version);
        PLOGD(
            pipeline_version_log,
            "  Actual processing_version after operations: ",
            processing_version,
            " (was ",
            pre_processing_version,
            " before)");

        LOGD(
            "  State map: ",
            state_ops.nodes_added,
            " added, ",
            state_ops.nodes_updated,
            " updated, ",
            state_ops.nodes_deleted,
            " deleted");
        PLOGD(
            pipeline_version_log,
            "  State map operations: ",
            state_ops.nodes_added,
            " added, ",
            state_ops.nodes_updated,
            " updated, ",
            state_ops.nodes_deleted,
            " deleted");

        // Working snapshot is now our state snapshot for this ledger
        auto state_snapshot = working_snapshot;
        PLOGD(
            pipeline_version_log,
            "  Using working snapshot as state snapshot, version: ",
            state_snapshot->get_version());

        // Build fresh transaction map
        auto tx_map = std::make_shared<shamap::SHAMap>(
            shamap::tnTRANSACTION_MD, map_options_);
        MapOperations tx_ops = reader.read_map_with_shamap_owned_items(
            *tx_map,
            shamap::tnTRANSACTION_MD,
            false  // No deltas for tx maps
        );

        LOGD("  Tx map: ", tx_ops.nodes_added, " added");

        // Disable MAP_OPS logging if we enabled it for this specific ledger
        if (did_enable_map_ops)
        {
            LOGD("Disabling MAP_OPS logging after ledger ", canonical_info.seq);
            catl::v1::map_ops_log.disable();
        }

        return LedgerSnapshot{
            canonical_info,
            state_snapshot,
            tx_map,
            state_ops,
            tx_ops,
            processing_version};
    }
    catch (const CatlV1Error& e)
    {
        // Disable MAP_OPS logging if we enabled it (cleanup on error)
        if (did_enable_map_ops)
        {
            catl::v1::map_ops_log.disable();
        }

        // EOF or other read error
        LOGI("End of file or read error: ", e.what());
        return std::nullopt;
    }
}

HashedLedger
CatlNudbPipeline::hash_and_verify(LedgerSnapshot snapshot)
{
    LOGD(
        "Hashing ledger ",
        snapshot.info.seq,
        hasher_threads_ == 1
            ? " (single-threaded)"
            : " with " + std::to_string(hasher_threads_) + " threads");

    // Compute state map hash using parallel hashing
    Hash256 computed_account_hash = parallel_hash(snapshot.state_snapshot);
    bool state_matches = (computed_account_hash == snapshot.info.account_hash);

    if (!state_matches)
    {
        LOGE("  ❌ State hash mismatch!");
        LOGE("    Computed: ", computed_account_hash.hex());
        LOGE("    Expected: ", snapshot.info.account_hash.hex());
        throw std::runtime_error(
            "State hash mismatch for ledger " +
            std::to_string(snapshot.info.seq));
    }
    LOGD("  ✅ State hash matches");

    // Compute tx map hash using parallel hashing
    Hash256 computed_tx_hash = parallel_hash(snapshot.tx_map);
    bool tx_matches = (computed_tx_hash == snapshot.info.tx_hash);

    if (!tx_matches)
    {
        LOGE("  ❌ Tx hash mismatch!");
        LOGE("    Computed: ", computed_tx_hash.hex());
        LOGE("    Expected: ", snapshot.info.tx_hash.hex());
        throw std::runtime_error(
            "Tx hash mismatch for ledger " + std::to_string(snapshot.info.seq));
    }
    LOGD("  ✅ Tx hash matches");

    bool verified = true;  // If we got here, both hashes matched

    return HashedLedger{
        snapshot.info,
        snapshot.state_snapshot,
        snapshot.tx_map,
        verified,
        snapshot.state_ops,
        snapshot.tx_ops,
        snapshot.processing_version};
}

// Mock flush interface for now - will be replaced with actual NuDB writes
static void
flush_node(const Hash256& key, const std::vector<uint8_t>& value)
{
    // For now, just log what we would write
    static size_t total_nodes = 0;
    static size_t total_bytes = 0;

    total_nodes++;
    total_bytes += value.size();

    // Log every 10000th node to avoid spam
    if (total_nodes % 10000 == 0)
    {
        LOGD(
            "Would flush node ",
            total_nodes,
            " - key: ",
            key.hex().substr(0, 16),
            "...",
            " value_size: ",
            value.size(),
            " bytes",
            " (total: ",
            total_bytes / 1024,
            " KB)");
    }
}

// Helper to serialize an inner node
static std::vector<uint8_t>
serialize_inner_node(
    const boost::intrusive_ptr<catl::shamap::SHAMapInnerNode>& inner,
    const catl::shamap::SHAMapOptions& options)
{
    std::vector<uint8_t> data;

    // Simple format: [branch_mask_16bits][hash1][hash2]...[hashN]
    // Get branch mask (which children exist)
    uint16_t branch_mask = inner->get_branch_mask();
    data.push_back((branch_mask >> 8) & 0xFF);
    data.push_back(branch_mask & 0xFF);

    // Add hash for each existing child
    for (int i = 0; i < 16; ++i)
    {
        if (branch_mask & (1 << i))
        {
            auto child = inner->get_child(i);
            if (child)
            {
                Hash256 child_hash = child->get_hash(options);
                data.insert(
                    data.end(), child_hash.data(), child_hash.data() + 32);
            }
        }
    }

    return data;
}

// Helper to serialize a leaf node
static std::vector<uint8_t>
serialize_leaf_node(
    const boost::intrusive_ptr<catl::shamap::SHAMapLeafNode>& leaf)
{
    // Simple format: just the raw item data
    auto item = leaf->get_item();
    if (item)
    {
        return std::vector<uint8_t>(
            item->slice().data(), item->slice().data() + item->slice().size());
    }
    return {};
}

bool
CatlNudbPipeline::flush_to_nudb(HashedLedger hashed)
{
    if (!hashed.verified)
    {
        LOGE("Cannot flush unverified ledger ", hashed.info.seq);
        return false;
    }

    LOGD("Flushing ledger ", hashed.info.seq, " to NuDB");

    // Check if we should enable walk_nodes logging for this specific ledger
    bool should_enable_walk_nodes =
        walk_nodes_ledger_ && (*walk_nodes_ledger_ == hashed.info.seq);
    bool did_enable_walk_nodes = false;

    if (should_enable_walk_nodes)
    {
        LOGD("Enabling WALK_NODES logging for ledger ", hashed.info.seq);
        catl::shamap::walk_nodes_log.enable(LogLevel::DEBUG);
        did_enable_walk_nodes = true;
    }

    // TODO: Consider parallel flushing in the future
    // Now that we have parallel hashing working efficiently, the flush stage
    // may become the bottleneck ("waiting on flusher"). We could potentially:
    // 1. Partition nodes by hash prefix for parallel flushing
    // 2. Use a thread pool similar to the hashing approach
    // 3. Have multiple NuDB writer threads (if NuDB supports concurrent writes)
    // For now, single-threaded flushing is simpler and may be sufficient.

    // Track counts to verify against MapOperations
    size_t state_inner_count = 0;
    size_t state_leaf_count = 0;
    size_t tx_inner_count = 0;
    size_t tx_leaf_count = 0;

    // Flush only NEW nodes from state map (nodes with processing_version)
    if (hashed.state_snapshot)
    {
        LOGD(
            "  Flushing nodes with processing_version: ",
            hashed.processing_version);
        LOGD("  Snapshot version: ", hashed.state_snapshot->get_version());

        // Debug: Let's see what versions nodes actually have
        auto root = hashed.state_snapshot->get_root();
        if (root)
        {
            LOGD("  Root node has version: ", root->get_version());
            // Check first few children
            for (int i = 0; i < 16; ++i)
            {
                if (root->has_child(i))
                {
                    auto child = root->get_child(i);
                    if (child && child->is_leaf())
                    {
                        auto leaf = boost::static_pointer_cast<
                            catl::shamap::SHAMapLeafNode>(child);
                        LOGD(
                            "    Child ",
                            i,
                            " (leaf) has version: ",
                            leaf->get_version());
                        break;  // Just check first leaf
                    }
                }
            }
        }

        // Use walk_new_nodes with the specific processing_version
        hashed.state_snapshot->walk_new_nodes(
            [&](const boost::intrusive_ptr<catl::shamap::SHAMapTreeNode>&
                    node) {
                if (node->is_inner())
                {
                    auto inner = boost::static_pointer_cast<
                        catl::shamap::SHAMapInnerNode>(node);
                    std::vector<uint8_t> serialized =
                        serialize_inner_node(inner, map_options_);
                    state_inner_count++;
                    flush_node(node->get_hash(map_options_), serialized);
                }
                else if (node->is_leaf())
                {
                    auto leaf = boost::static_pointer_cast<
                        catl::shamap::SHAMapLeafNode>(node);
                    std::vector<uint8_t> serialized = serialize_leaf_node(leaf);
                    state_leaf_count++;
                    flush_node(node->get_hash(map_options_), serialized);

                    // Debug: Check if this leaf has a valid item
                    auto item = leaf->get_item();
                    if (!item)
                    {
                        LOGE(
                            "    WARNING: Leaf #",
                            state_leaf_count,
                            " has NULL item!");
                    }
                    else if (walk_nodes_debug_key_)
                    {
                        // Check if this leaf's key matches the debug key prefix
                        std::string key_hex = item->key().hex();
                        if (key_hex.find(*walk_nodes_debug_key_) == 0)
                        {
                            // This key matches the debug prefix!
                            LOGI("🔍 DEBUG KEY MATCH:");
                            LOGI("  Key: ", key_hex);
                            LOGI("  Version: ", leaf->get_version());
                            LOGI(
                                "  Data size: ",
                                item->slice().size(),
                                " bytes");

                            // Print hex data
                            std::string hex_data;
                            hex_data.reserve(item->slice().size() * 2);
                            for (size_t i = 0; i < item->slice().size(); ++i)
                            {
                                char buf[3];
                                snprintf(
                                    buf,
                                    sizeof(buf),
                                    "%02X",
                                    item->slice().data()[i]);
                                hex_data += buf;
                            }
                            LOGI("  Data (hex): ", hex_data);

                            // Parse and print JSON
                            try
                            {
                                Slice data_slice(
                                    item->slice().data(), item->slice().size());
                                catl::xdata::JsonVisitor visitor(protocol_);
                                catl::xdata::ParserContext ctx(data_slice);
                                catl::xdata::parse_with_visitor(
                                    ctx, protocol_, visitor);
                                boost::json::value json_result =
                                    visitor.get_result();

                                LOGI("  Parsed JSON:");
                                std::cout << boost::json::serialize(json_result)
                                          << std::endl;
                            }
                            catch (const std::exception& e)
                            {
                                LOGE("  Failed to parse JSON: ", e.what());
                            }
                        }
                    }
                }
                return true;  // Continue walking
            },
            hashed.processing_version);

        // Verify counts match MapOperations
        // Note: MapOperations counts ITEMS (leaves), not inner nodes
        size_t expected_leaf_operations =
            hashed.state_ops.nodes_added + hashed.state_ops.nodes_updated;
        LOGD(
            "  State map flushed: ",
            state_leaf_count,
            " leaves, ",
            state_inner_count,
            " inner nodes");

        // Inner nodes are created/modified when leaves are added/updated
        // So we expect some inner nodes with the processing_version too
        size_t total_flushed = state_leaf_count + state_inner_count;
        LOGD(
            "  Total nodes flushed with version ",
            hashed.processing_version,
            ": ",
            total_flushed);

        if (state_leaf_count != expected_leaf_operations)
        {
            LOGW(
                "  Warning [Ledger ",
                hashed.info.seq,
                "]: flushed ",
                state_leaf_count,
                " leaves but expected ",
                expected_leaf_operations,
                " leaf operations (",
                hashed.state_ops.nodes_added,
                " added + ",
                hashed.state_ops.nodes_updated,
                " updated)");
            // This mismatch might occur if inner nodes also get the
            // processing_version when they're created to hold new leaves
        }
    }

    // Flush ALL nodes from tx map (it's rebuilt fresh each ledger)
    if (hashed.tx_map)
    {
        // Since tx_map is fresh, all nodes have the same version
        // Use walk_new_nodes() without specifying version (uses root's version)
        hashed.tx_map->walk_new_nodes([&](const boost::intrusive_ptr<
                                          catl::shamap::SHAMapTreeNode>& node) {
            if (node->is_inner())
            {
                auto inner =
                    boost::static_pointer_cast<catl::shamap::SHAMapInnerNode>(
                        node);
                std::vector<uint8_t> serialized =
                    serialize_inner_node(inner, map_options_);
                tx_inner_count++;
                flush_node(node->get_hash(map_options_), serialized);
            }
            else if (node->is_leaf())
            {
                auto leaf =
                    boost::static_pointer_cast<catl::shamap::SHAMapLeafNode>(
                        node);
                std::vector<uint8_t> serialized = serialize_leaf_node(leaf);
                tx_leaf_count++;
                flush_node(node->get_hash(map_options_), serialized);
            }
            return true;  // Continue walking
        });

        LOGD(
            "  Tx map flushed: ",
            tx_leaf_count,
            " leaves, ",
            tx_inner_count,
            " inner nodes");
        if (tx_leaf_count != hashed.tx_ops.nodes_added)
        {
            LOGW(
                "  Warning [Ledger ",
                hashed.info.seq,
                "]: flushed ",
                tx_leaf_count,
                " tx leaves but expected ",
                hashed.tx_ops.nodes_added,
                " added");
        }
    }

    // Disable walk_nodes logging if we enabled it for this specific ledger
    if (did_enable_walk_nodes)
    {
        LOGD("Disabling WALK_NODES logging after ledger ", hashed.info.seq);
        catl::shamap::walk_nodes_log.disable();
    }

    return true;
}

}  // namespace catl::v1::utils::nudb
