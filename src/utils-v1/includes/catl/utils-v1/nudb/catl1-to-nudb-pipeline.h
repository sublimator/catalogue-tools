#pragma once

#include "catl/common/ledger-info.h"
#include "catl/core/logger.h"
#include "catl/shamap/shamap.h"
#include "catl/v1/catl-v1-reader.h"
#include "catl/v1/catl-v1-types.h"  // For MapOperations
#include "catl/xdata/protocol.h"
#include <fstream>
#include <future>
#include <memory>
#include <nudb/basic_store.hpp>
#include <nudb/posix_file.hpp>
#include <nudb/xxhasher.hpp>
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

    /**
     * Enable mock mode - skip or redirect database operations (for performance
     * testing)
     * @param mode "noop"/"memory" = skip all I/O, "disk" = buffered append-only
     * file
     */
    void
    set_mock_mode(const std::string& mode);

    /**
     * Create and open the NuDB database
     * @param path Directory path for the database files
     * @param key_size Size of keys in bytes (default 32)
     * @param block_size Block size for database (default 4096)
     * @param load_factor Load factor 0.0-1.0 (default 0.5)
     * @return true on success
     */
    bool
    create_database(
        const std::string& path,
        uint32_t key_size = 32,
        uint32_t block_size = 4096,
        double load_factor = 0.5);

    /**
     * Open an existing NuDB database (does not create/remove files)
     * @param path Directory path for the database files
     * @return true on success
     */
    bool
    open_database(const std::string& path);

    /**
     * Close the NuDB database (flushes final in-memory pool)
     * @return true on success, false if close failed
     */
    bool
    close_database();

    /**
     * Verify all inserted keys are readable from NuDB
     * @param num_threads Number of threads to use for verification (default 8)
     * @return true if all keys are readable, false if any are missing
     */
    bool
    verify_all_keys(int num_threads = 8);

    /**
     * Get total bytes written to NuDB
     */
    uint64_t
    get_total_bytes_written() const
    {
        return total_bytes_written_.load();
    }

private:
    shamap::SHAMapOptions map_options_;
    catl::xdata::Protocol protocol_;  // Protocol definitions for JSON parsing
    int hasher_threads_ =
        1;  // Default to 1 (single-threaded often faster due to overhead)
    std::optional<uint32_t>
        walk_nodes_ledger_;  // Ledger to debug with walk_nodes
    std::optional<std::string>
        walk_nodes_debug_key_;    // Key prefix (hex) to debug
    std::string mock_mode_ = "";  // Mock mode: "", "noop", "memory", or "disk"

    // NuDB store (use xxhasher like xahaud)
    using store_type =
        ::nudb::basic_store<::nudb::xxhasher, ::nudb::posix_file>;
    std::unique_ptr<store_type> db_;
    std::string db_path_;

    // Mock mode "disk" - buffered file for append-only writes
    std::unique_ptr<std::ofstream> mock_disk_file_;

    // Track total bytes written to NuDB for stats
    std::atomic<uint64_t> total_bytes_written_{0};

    // Custom hasher for Hash256 using xxhasher (same as NuDB)
    struct Hash256Hasher
    {
        std::size_t
        operator()(const Hash256& key) const noexcept
        {
            // Use xxhasher to hash the 32-byte key (seed = 0)
            ::nudb::xxhasher h(0);
            return static_cast<std::size_t>(h(key.data(), key.size()));
        }
    };

    // Track inserted keys with their sizes for deduplication and verification
    // (faster than letting NuDB check for duplicates, and allows size
    // verification)
    std::unordered_map<Hash256, size_t, Hash256Hasher>
        inserted_keys_with_sizes_;

    // Helper function to hash a SHAMap using thread pool (or direct for single
    // thread) NOTE: Performance testing revealed that single-threaded hashing
    // often outperforms multi-threaded due to thread coordination overhead
    // (futures, synchronization, etc.) outweighing the benefits of
    // parallelization. Cache locality is also better with a single thread.
    // Multi-threading is still available for experimentation.
    Hash256
    parallel_hash(const std::shared_ptr<shamap::SHAMap>& map);

    // Helper to write a node to NuDB
    void
    flush_node(const Hash256& key, const uint8_t* data, size_t size);
};

}  // namespace catl::v1::utils::nudb
