#pragma once

#include "catl/common/ledger-info.h"
#include "catl/core/logger.h"
#include "catl/shamap/shamap.h"
#include "catl/utils-v1/nudb/nudb-bulk-writer.h"  // For NudbBulkWriter
#include "catl/utils-v1/nudb/stats-report-sink.h"
#include "catl/v1/catl-v1-reader.h"
#include "catl/v1/catl-v1-types.h"  // For MapOperations
#include "catl/xdata/protocol.h"
#include <boost/lockfree/queue.hpp>
#include <boost/lockfree/spsc_queue.hpp>
#include <condition_variable>
#include <fstream>
#include <future>
#include <memory>
#include <mutex>
#include <nudb/basic_store.hpp>
#include <nudb/posix_file.hpp>
#include <nudb/xxhasher.hpp>
#include <optional>
#include <queue>
#include <thread>
#include <vector>

namespace catl::v1::utils::nudb {

// LogPartition for pipeline version tracking - enable with
// pipeline_version_log.enable(LogLevel::DEBUG)
extern LogPartition pipeline_version_log;

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

    // Comparison for priority queue (oldest ledger = highest priority)
    bool
    operator<(const HashedLedger& other) const
    {
        return info.seq >
            other.info.seq;  // Reverse: lower seq = higher priority
    }
};

// Node type enumeration for tracking
enum class PipelineNodeType : uint8_t {
    StateInner = 0,  // State tree inner node
    TxInner = 1,     // Transaction tree inner node
    StateLeaf = 2,   // Account state leaf
    TxLeaf = 3       // Transaction leaf
};

/**
 * Deduplication strategy for different node types
 */
enum class NodeDedupeStrategy {
    All,       // Dedupe all node types (StateInner, TxInner, StateLeaf, TxLeaf)
    TxLeaves,  // Skip TxLeaf deduplication
    TxAll      // Skip both TxInner and TxLeaf deduplication (default - fastest)
};

// Compressed node blob ready for writing
struct CompressedNode
{
    uint32_t ledger_seq;  // For ordering
    Hash256 hash;
    std::vector<uint8_t> blob;   // Compressed data
    size_t uncompressed_size;    // Original size before compression
    PipelineNodeType node_type;  // Inner, StateLeaf, or TxLeaf
};

// Deduplication work item (for parallel dedupe thread)
// Memory-safe: only passes hashes, not full tree structures
struct DedupeWork
{
    uint32_t ledger_seq;
    std::vector<Hash256> hashes;  // Just the hashes to check
};

// Writer job assembly (solves out-of-order problem)
// Waits for BOTH compression AND dedupe results before writing
struct WriterJob
{
    std::vector<CompressedNode> compressed_batch;
    std::unordered_set<Hash256, Hash256Hasher>
        duplicate_set;  // Result from dedupe
    bool compression_done = false;
    bool dedupe_done = false;
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
     * Enqueue a snapshot to the hasher thread
     * This is the entry point for the pipeline when using internal hasher
     * Blocks if hasher queue is full (backpressure)
     *
     * @param snapshot Snapshot from stage 1
     */
    void
    enqueue_to_hasher(LedgerSnapshot snapshot);

    /**
     * Set the number of threads to use for parallel hashing
     * Must be a power of 2 (1, 2, 4, 8, or 16)
     * Default is 1
     */
    void
    set_hasher_threads(int threads);

    /**
     * Set the number of threads to use for parallel compression
     * Default is 2
     * Must be called before create_database()
     */
    void
    set_compression_threads(int threads);

    /**
     * Set the max write queue size in megabytes
     * Default is 2048 MB (2 GB)
     * Must be called before start_compression_pipeline()
     */
    void
    set_max_write_queue_mb(uint32_t mb);

    /**
     * Start the compression thread pool and writer thread
     * Called automatically by create_database()
     */
    void
    start_compression_pipeline();

    /**
     * Stop the compression thread pool and writer thread
     * Called automatically by close_database()
     */
    void
    stop_compression_pipeline();

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
     * Set deduplication strategy
     * @param strategy Strategy name: "none", "cuckoo-rocks", "nudb",
     * "memory-full", "memory-xxhash"
     */
    void
    set_dedupe_strategy(const std::string& strategy);

    /**
     * Enable/disable parallel dedupe thread
     * @param use_thread If true, run dedupe in separate thread
     */
    void
    set_use_dedupe_thread(bool use_thread)
    {
        use_dedupe_thread_ = use_thread;
    }

    /**
     * Set node deduplication strategy
     * @param strategy Which node types to deduplicate
     */
    void
    set_node_dedupe_strategy(NodeDedupeStrategy strategy)
    {
        node_dedupe_strategy_ = strategy;
    }

    /**
     * Set stats report sink for real-time monitoring
     * @param sink Shared pointer to sink
     */
    void
    set_stats_sink(std::shared_ptr<StatsReportSink> sink)
    {
        stats_sink_ = sink;
    }

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
     * Get total bytes written to NuDB (compressed)
     */
    uint64_t
    get_total_bytes_written() const
    {
        return total_bytes_written_.load();
    }

    /**
     * Get total uncompressed bytes
     */
    uint64_t
    get_total_bytes_uncompressed() const
    {
        return total_bytes_uncompressed_.load();
    }

    /**
     * Get total inner nodes written (state + tx)
     */
    uint64_t
    get_total_inner_nodes() const
    {
        return total_state_inner_.load() + total_tx_inner_.load();
    }

    /**
     * Get total leaf nodes written (state + tx)
     */
    uint64_t
    get_total_leaf_nodes() const
    {
        return total_state_leaf_.load() + total_tx_leaf_.load();
    }

    /**
     * Get total nodes by type
     */
    uint64_t
    get_total_state_inner() const
    {
        return total_state_inner_.load();
    }

    uint64_t
    get_total_tx_inner() const
    {
        return total_tx_inner_.load();
    }

    uint64_t
    get_total_state_leaf() const
    {
        return total_state_leaf_.load();
    }

    uint64_t
    get_total_tx_leaf() const
    {
        return total_tx_leaf_.load();
    }

    /**
     * Get total duplicate count (from deduplication strategy)
     */
    uint64_t
    get_duplicate_count() const;

    /**
     * Get state inner duplicate count
     */
    uint64_t
    get_duplicate_state_inner_count() const;

    /**
     * Get tx inner duplicate count
     */
    uint64_t
    get_duplicate_tx_inner_count() const;

    /**
     * Get state leaf duplicate count
     */
    uint64_t
    get_duplicate_state_leaf_count() const;

    /**
     * Get hasher queue depth (ledgers waiting to be hashed)
     */
    size_t
    get_hasher_queue_depth() const
    {
        return hasher_queue_depth_.load();
    }

    /**
     * Get compression queue depth (ledgers waiting to be compressed)
     */
    size_t
    get_compression_queue_depth() const
    {
        return compression_queue_depth_.load();
    }

    /**
     * Get write queue depth (compressed nodes waiting to be written)
     */
    size_t
    get_write_queue_depth() const
    {
        return write_queue_nodes_.load();
    }

    /**
     * Get write queue bytes (total compressed bytes waiting to be written)
     */
    uint64_t
    get_write_queue_bytes() const
    {
        return write_queue_bytes_.load();
    }

    /**
     * Get dedupe queue depth (ledgers waiting for deduplication)
     * Only meaningful when use_dedupe_thread_ is true
     */
    size_t
    get_dedupe_queue_depth() const
    {
        if (!use_dedupe_thread_)
        {
            return 0;  // Not using dedupe thread
        }
        return dedupe_queue_depth_.load();
    }

    /**
     * Get assembly station depth (ledgers waiting at writer assembly)
     * Only meaningful when use_dedupe_thread_ is true
     */
    size_t
    get_assembly_station_depth() const
    {
        if (!use_dedupe_thread_)
        {
            return 0;  // Not using dedupe thread
        }
        return assembly_station_depth_.load();
    }

    /**
     * Print deduplication statistics from the pipeline strategy
     * (Only useful when use_dedupe_thread_ is true)
     */
    void
    print_dedup_stats() const;

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
    std::string dedupe_strategy_ = "cuckoo-rocks";  // Deduplication strategy
    bool use_dedupe_thread_ = false;  // Run dedupe in separate parallel thread
    NodeDedupeStrategy node_dedupe_strategy_ =
        NodeDedupeStrategy::TxAll;  // Which node types to deduplicate

    // Stats reporting (optional, for dashboard or metrics export)
    std::shared_ptr<StatsReportSink> stats_sink_;

    // NuDB configuration parameters
    uint32_t key_size_ = 32;
    uint32_t block_size_ = 4096;
    double load_factor_ = 0.5;

    // NuDB bulk writer for fast import
    std::unique_ptr<NudbBulkWriter> bulk_writer_;

    // Pipeline-level deduplication strategy (the "brain")
    // When use_dedupe_thread_ is true, this is used by dedupe_worker
    // When false, bulk_writer_ uses its own strategy
    std::unique_ptr<DeduplicationStrategy> pipeline_dedup_strategy_;

    // NuDB store for verification after bulk import
    using store_type =
        ::nudb::basic_store<::nudb::xxhasher, ::nudb::posix_file>;
    std::unique_ptr<store_type> db_;
    std::string db_path_;

    // Mock mode "disk" - buffered file for append-only writes
    std::unique_ptr<std::ofstream> mock_disk_file_;

    // Track total bytes written to NuDB for stats
    std::atomic<uint64_t> total_bytes_written_{0};       // Compressed bytes
    std::atomic<uint64_t> total_bytes_uncompressed_{0};  // Uncompressed bytes

    // Track total nodes by type
    std::atomic<uint64_t> total_state_inner_{0};  // State inner nodes
    std::atomic<uint64_t> total_tx_inner_{0};     // Tx inner nodes
    std::atomic<uint64_t> total_state_leaf_{0};   // State leaf nodes
    std::atomic<uint64_t> total_tx_leaf_{0};      // Tx leaf nodes

    // Track duplicates by node type (TxLeaf is never deduplicated)
    std::atomic<uint64_t> duplicates_state_inner_{0};  // State inner duplicates
    std::atomic<uint64_t> duplicates_tx_inner_{0};     // Tx inner duplicates
    std::atomic<uint64_t> duplicates_state_leaf_{0};   // State leaf duplicates
    // Note: TxLeaf is never deduplicated (always 0 duplicates)

    // Helper function to hash a SHAMap using thread pool (or direct for single
    // thread) NOTE: Performance testing revealed that single-threaded hashing
    // often outperforms multi-threaded due to thread coordination overhead
    // (futures, synchronization, etc.) outweighing the benefits of
    // parallelization. Cache locality is also better with a single thread.
    // Multi-threading is still available for experimentation.
    Hash256
    parallel_hash(const std::shared_ptr<shamap::SHAMap>& map);

    // Helper to write a node to NuDB
    // node_type: 0=inner, 1=leaf
    // Returns true if inserted, false if duplicate
    bool
    flush_node(
        const Hash256& key,
        const uint8_t* data,
        size_t size,
        uint8_t node_type);

    // ===== Compression Pipeline =====

    int compression_threads_ = 2;  // Default compression thread count
    uint64_t max_write_queue_bytes_ = 2048ULL * 1024 * 1024;  // Default 2GB

    // ===== Pipeline Queues =====

    // Queue for unhashed ledgers (FIFO from builder)
    // Lock-free SPSC: main thread -> hasher thread
    boost::lockfree::spsc_queue<LedgerSnapshot, boost::lockfree::capacity<512>>
        hasher_queue_;
    // Note: No mutex/cv needed for lock-free queue, but keep cv for wait/notify
    std::condition_variable hasher_queue_cv_;
    std::mutex hasher_queue_cv_mutex_;  // Only for cv wait

    // Priority queue for compression jobs (ordered by ledger_seq)
    std::priority_queue<HashedLedger> compression_queue_;
    std::mutex compression_queue_mutex_;
    std::condition_variable compression_queue_cv_;

    // Output queue for compressed node BATCHES (FIFO, maintains ledger order)
    // Each batch contains all nodes from one ledger
    // Lock-free MPSC: compression workers -> writer thread
    boost::lockfree::queue<std::vector<CompressedNode>*> write_queue_{512};
    std::condition_variable write_queue_cv_;
    std::mutex write_queue_cv_mutex_;  // Only for cv wait

    // ===== Parallel Dedupe Thread (optional) =====

    // Dedupe work queue (receives hash lists from hasher)
    // Lock-free SPSC: hasher thread -> dedupe thread
    boost::lockfree::spsc_queue<DedupeWork, boost::lockfree::capacity<512>>
        dedupe_queue_;
    std::condition_variable dedupe_queue_cv_;
    std::mutex dedupe_queue_cv_mutex_;  // Only for cv wait

    // Writer assembly station (solves out-of-order problem)
    // Maps ledger_seq → WriterJob (compression + dedupe results)
    std::map<uint32_t, WriterJob> writer_assembly_map_;
    std::mutex writer_assembly_mutex_;
    std::condition_variable writer_assembly_cv_;
    uint32_t next_ledger_to_write_ = 0;  // Next expected ledger in sequence

    // ===== Worker Threads =====

    std::thread hasher_thread_;
    std::vector<std::thread> compression_workers_;
    std::thread writer_thread_;
    std::thread dedupe_worker_;  // Optional parallel dedupe thread

    // Shutdown flags
    std::atomic<bool> shutdown_{false};
    std::atomic<bool> pipeline_stopped_{false};

    // Track write queue size for stats and backpressure
    std::atomic<uint64_t> write_queue_bytes_{0};  // Total compressed bytes
    std::atomic<uint64_t> write_queue_nodes_{0};  // Total node count

    // Atomic queue depth counters (for lock-free stats reading)
    std::atomic<size_t> hasher_queue_depth_{0};
    std::atomic<size_t> compression_queue_depth_{0};
    std::atomic<size_t> dedupe_queue_depth_{0};
    std::atomic<size_t> assembly_station_depth_{0};

    // Worker thread functions
    void
    hasher_worker();
    void
    compression_worker();
    void
    writer_worker();
    void
    dedupe_worker();  // Optional parallel dedupe thread worker

    // Helper: enqueue compressed node with backpressure
    void
    enqueue_compressed_batch(std::vector<CompressedNode>&& batch);
};

}  // namespace catl::v1::utils::nudb
