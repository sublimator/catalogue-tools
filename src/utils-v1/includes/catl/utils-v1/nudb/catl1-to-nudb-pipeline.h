#pragma once

#include "catl/common/ledger-info.h"
#include "catl/core/logger.h"
#include "catl/shamap/shamap.h"
#include "catl/v1/catl-v1-reader.h"
#include "catl/v1/catl-v1-types.h"  // For MapOperations
#include "catl/xdata/protocol.h"
#include <future>
#include <memory>
#include <optional>
#include <thread>
#include <vector>

namespace catl::v1::utils::nudb {

// LogPartition for pipeline version tracking - enable with
// pipeline_version_log.enable(LogLevel::DEBUG)
extern LogPartition pipeline_version_log;

/**
 * Data passed between pipeline stages
 */

// Output of Stage 1 (Build + Snapshot)
struct LedgerSnapshot
{
    catl::common::LedgerInfo info;
    std::shared_ptr<shamap::SHAMap> state_snapshot;
    std::shared_ptr<shamap::SHAMap> tx_map;
    catl::v1::MapOperations state_ops;  // Stats from building state map
    catl::v1::MapOperations tx_ops;     // Stats from building tx map
    int processing_version;  // Version when we started processing this ledger
};

// Output of Stage 2 (Hash)
struct HashedLedger
{
    catl::common::LedgerInfo info;
    std::shared_ptr<shamap::SHAMap> state_snapshot;
    std::shared_ptr<shamap::SHAMap> tx_map;
    bool verified;
    catl::v1::MapOperations state_ops;  // Carry forward from snapshot
    catl::v1::MapOperations tx_ops;     // Carry forward from snapshot
    int processing_version;  // Version when we started processing this ledger
};

/**
 * Three-stage pipeline for CATL to NuDB conversion
 *
 * Stage 1: Build + Snapshot - Read CATL, apply deltas, snapshot state
 * Stage 2: Hash - Compute and verify Merkle tree hashes (with parallel support)
 * Stage 3: Flush - Write nodes to NuDB
 */
class CatlNudbPipeline
{
public:
    explicit CatlNudbPipeline(
        const shamap::SHAMapOptions& map_options,
        const catl::xdata::Protocol& protocol);

    /**
     * Stage 1: Build ledger and snapshot state
     *
     * Reads from CATL file, applies deltas to persistent state_map,
     * creates immutable snapshot, builds fresh tx_map.
     *
     * @param reader CATL file reader
     * @param state_map Persistent state map (modified in place)
     * @param allow_deltas Whether to allow delta operations
     * @return Snapshot ready for hashing, or nullopt if EOF
     */
    std::optional<LedgerSnapshot>
    build_and_snapshot(
        Reader& reader,
        std::shared_ptr<shamap::SHAMap>& state_map,
        bool allow_deltas);

    /**
     * Stage 2: Hash and verify ledger
     *
     * Computes Merkle tree hashes for both maps and verifies
     * against expected values from ledger header.
     * Uses parallel hashing with configured number of threads.
     *
     * @param snapshot Snapshot from stage 1
     * @return Hashed ledger with verification status
     */
    HashedLedger
    hash_and_verify(LedgerSnapshot snapshot);

    /**
     * Stage 3: Flush to NuDB
     *
     * Walks tree, serializes nodes, writes to NuDB.
     * For now, uses a simple interface: flush(hash, serialized_data)
     *
     * @param hashed Hashed ledger from stage 2
     * @return true if flush succeeded
     */
    bool
    flush_to_nudb(HashedLedger hashed);

    /**
     * Set the number of threads to use for parallel hashing
     * Must be a power of 2 (1, 2, 4, 8, or 16)
     * Default is 2
     */
    void
    set_hasher_threads(int threads);

    /**
     * Set the ledger to enable walk_nodes debugging for
     * @param ledger_seq The ledger sequence number to debug
     */
    void
    set_walk_nodes_ledger(uint32_t ledger_seq);

    /**
     * Set a debug key prefix to print detailed info during walk_nodes
     * @param key_hex Hex string prefix of the key to debug (e.g.,
     * "567D5DABE2E1AF17")
     */
    void
    set_walk_nodes_debug_key(const std::string& key_hex);

private:
    shamap::SHAMapOptions map_options_;
    catl::xdata::Protocol protocol_;  // Protocol definitions for JSON parsing
    int hasher_threads_ =
        1;  // Default to 1 (single-threaded often faster due to overhead)
    std::optional<uint32_t>
        walk_nodes_ledger_;  // Ledger to debug with walk_nodes
    std::optional<std::string>
        walk_nodes_debug_key_;  // Key prefix (hex) to debug

    // Helper function to hash a SHAMap using thread pool (or direct for single
    // thread) NOTE: Performance testing revealed that single-threaded hashing
    // often outperforms multi-threaded due to thread coordination overhead
    // (futures, synchronization, etc.) outweighing the benefits of
    // parallelization. Cache locality is also better with a single thread.
    // Multi-threading is still available for experimentation.
    Hash256
    parallel_hash(const std::shared_ptr<shamap::SHAMap>& map);
};

}  // namespace catl::v1::utils::nudb
