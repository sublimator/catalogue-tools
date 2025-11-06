#include "catl/core/log-macros.h"
#include "catl/shamap/shamap.h"
#include "catl/utils-v1/nudb/catl1-to-nudb-arg-options.h"
#include "catl/utils-v1/nudb/catl1-to-nudb-pipeline.h"
#include "catl/v1/catl-v1-reader.h"
#include "catl/v1/catl-v1-utils.h"
#include "catl/xdata/protocol.h"

#include <boost/filesystem.hpp>
#include <boost/lockfree/spsc_queue.hpp>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace catl::v1;
using namespace catl::v1::utils::nudb;
using namespace catl::shamap;
namespace fs = boost::filesystem;

// LogPartition for version tracking - disabled by default
// Enable with: version_tracking_log.enable(LogLevel::DEBUG)
static LogPartition version_tracking_log("VERSION_TRACK", LogLevel::NONE);

// Pipeline queue configuration constants
static constexpr size_t SNAPSHOT_QUEUE_SIZE =
    100;  // Buffer size for snapshot queue
static constexpr size_t HASHED_QUEUE_SIZE =
    100;  // Buffer size for hashed ledger queue

/**
 * Load protocol definitions based on network ID
 */
static catl::xdata::Protocol
load_protocol_for_network(uint32_t network_id)
{
    if (network_id == 0)  // XRPL
    {
        LOGI(
            "Auto-detected network ID ",
            network_id,
            " - using embedded XRPL protocol definitions");
        return catl::xdata::Protocol::load_embedded_xrpl_protocol();
    }
    else if (network_id == 21337)  // XAHAU
    {
        LOGI(
            "Auto-detected network ID ",
            network_id,
            " - using embedded Xahau protocol definitions");
        return catl::xdata::Protocol::load_embedded_xahau_protocol();
    }
    else
    {
        LOGW(
            "Unknown network ID ",
            network_id,
            " - falling back to Xahau protocol definitions");
        return catl::xdata::Protocol::load_embedded_xahau_protocol();
    }
}

/**
 * Catl1ToNudbConverter - Converts CATL v1 files to NuDB database format
 *
 * This tool reads ledger data from a CATL file and stores it in a NuDB
 * database for efficient key-value lookups. The database uses ledger
 * sequence numbers as keys and stores the serialized ledger data as values.
 */
class Catl1ToNudbConverter
{
private:
    const Catl1ToNudbOptions& options_;

public:
    Catl1ToNudbConverter(const Catl1ToNudbOptions& options) : options_(options)
    {
        // Validate input file exists
        if (!fs::exists(*options_.input_file))
        {
            throw std::runtime_error(
                "Input file does not exist: " + *options_.input_file);
        }
    }

    bool
    convert()
    {
        try
        {
            // Open the input CATL file to read header
            LOGI("Opening input file: ", *options_.input_file);
            Reader header_reader(*options_.input_file);
            const CatlHeader& header = header_reader.header();

            LOGI("File information:");
            LOGI(
                "  Ledger range: ",
                header.min_ledger,
                " - ",
                header.max_ledger);
            LOGI("  Network ID: ", header.network_id);

            // Determine ledger range to process
            uint32_t start_ledger =
                options_.start_ledger.value_or(header.min_ledger);
            uint32_t end_ledger =
                options_.end_ledger.value_or(header.max_ledger);

            // Close header reader - builder thread will create its own
            // (Reader is not thread-safe)

            // Validate range
            if (start_ledger < header.min_ledger ||
                end_ledger > header.max_ledger)
            {
                LOGE(
                    "Requested ledger range (",
                    start_ledger,
                    "-",
                    end_ledger,
                    ") is outside file's range (",
                    header.min_ledger,
                    "-",
                    header.max_ledger,
                    ")");
                return false;
            }

            // Check that end >= start
            if (end_ledger < start_ledger)
            {
                LOGE(
                    "Invalid range: end_ledger (",
                    end_ledger,
                    ") is less than start_ledger (",
                    start_ledger,
                    "). Did you mean to process ",
                    end_ledger,
                    " ledgers starting from ",
                    start_ledger,
                    "?");
                // Suggest the corrected command
                LOGE("Try: --end-ledger ", start_ledger + end_ledger - 1);
                return false;
            }

            LOGI("Processing ledgers ", start_ledger, " to ", end_ledger);

            // Enable debug logging partitions if requested
            if (options_.enable_debug_partitions)
            {
                catl::v1::map_ops_log.enable(LogLevel::DEBUG);
                catl::shamap::walk_nodes_log.enable(LogLevel::DEBUG);
                version_tracking_log.enable(LogLevel::DEBUG);
                catl::v1::utils::nudb::pipeline_version_log.enable(
                    LogLevel::DEBUG);
                LOGI(
                    "Enabled debug log partitions: MAP_OPS, WALK_NODES, "
                    "VERSION_TRACK, and PIPE_VERSION");
            }

            // Log walk-nodes-ledger option if specified
            if (options_.walk_nodes_ledger)
            {
                LOGI(
                    "WALK_NODES logging will be enabled only for ledger ",
                    *options_.walk_nodes_ledger);
            }

            // Load protocol definitions for JSON parsing
            catl::xdata::Protocol protocol =
                load_protocol_for_network(header.network_id);

            // Create SHAMap options for non-collapsed tree (we need inner nodes
            // for NuDB)
            SHAMapOptions map_options;
            map_options.tree_collapse_impl = TreeCollapseImpl::leafs_only;

            // Create pipeline
            CatlNudbPipeline pipeline(map_options, protocol);

            // Configure hasher threads
            pipeline.set_hasher_threads(options_.hasher_threads);

            // Configure walk-nodes-ledger if specified
            if (options_.walk_nodes_ledger)
            {
                pipeline.set_walk_nodes_ledger(*options_.walk_nodes_ledger);
            }

            // Configure walk-nodes-debug-key if specified
            if (options_.walk_nodes_debug_key)
            {
                pipeline.set_walk_nodes_debug_key(
                    *options_.walk_nodes_debug_key);
            }

            // Configure mock mode if enabled
            if (!options_.nudb_mock.empty())
            {
                pipeline.set_mock_mode(options_.nudb_mock);
            }

            // Create NuDB database
            LOGI("Creating NuDB database...");
            if (!pipeline.create_database(
                    *options_.nudb_path,
                    options_.key_size,
                    options_.block_size,
                    options_.load_factor))
            {
                LOGE("Failed to create NuDB database");
                return false;
            }

            // Create SPSC queues between stages
            // Queue sizes are configured by constants for consistent reporting
            boost::lockfree::spsc_queue<LedgerSnapshot> snapshot_queue(
                SNAPSHOT_QUEUE_SIZE);
            boost::lockfree::spsc_queue<HashedLedger> hashed_queue(
                HASHED_QUEUE_SIZE);

            // Error tracking (atomic for thread safety)
            std::atomic<bool> error_occurred{false};
            std::atomic<bool> builder_done{false};
            std::atomic<bool> hasher_done{false};

            // Thread 1: Build + Snapshot
            std::thread builder_thread([&]() {
                try
                {
                    LOGI("[Builder] Starting...");

                    // Create reader in builder thread (Reader is not
                    // thread-safe)
                    Reader reader(*options_.input_file);
                    auto state_map =
                        std::make_shared<SHAMap>(tnACCOUNT_STATE, map_options);

                    PLOGD(
                        version_tracking_log,
                        "[Builder] Created state_map, initial version: ",
                        state_map->get_version());

                    // Enable CoW by taking an initial snapshot before any
                    // processing This ensures all nodes get proper versions
                    // instead of -1
                    PLOGD(
                        version_tracking_log,
                        "[Builder] Taking initial snapshot to enable CoW");
                    auto initial_snapshot = state_map->snapshot();
                    PLOGD(
                        version_tracking_log,
                        "[Builder] Initial snapshot created with version: ",
                        initial_snapshot->get_version(),
                        ", state_map now has version: ",
                        state_map->get_version());
                    initial_snapshot.reset();  // Immediately discard it
                    PLOGD(
                        version_tracking_log,
                        "[Builder] Initial snapshot discarded, state_map "
                        "version remains: ",
                        state_map->get_version());

                    // Stats tracking
                    auto start_time = std::chrono::steady_clock::now();
                    auto last_stats_time = start_time;
                    uint64_t total_state_nodes_added = 0;
                    uint64_t total_state_nodes_updated = 0;
                    uint64_t total_state_nodes_deleted = 0;
                    uint64_t total_tx_nodes_added = 0;
                    uint32_t last_stats_ledger = start_ledger;
                    uint32_t queue_full_waits = 0;
                    uint32_t last_queue_full_waits = 0;
                    uint64_t last_bytes_written = 0;
                    uint64_t last_bytes_read = 0;

                    for (uint32_t ledger_seq = start_ledger;
                         ledger_seq <= end_ledger;
                         ++ledger_seq)
                    {
                        if (error_occurred.load())
                            break;

                        PLOGD(
                            version_tracking_log,
                            "[Builder] ========== LEDGER ",
                            ledger_seq,
                            " ==========");
                        PLOGD(
                            version_tracking_log,
                            "[Builder] State map version BEFORE processing: ",
                            state_map->get_version());

                        bool allow_deltas = (ledger_seq > start_ledger);
                        PLOGD(
                            version_tracking_log,
                            "[Builder] Calling build_and_snapshot with "
                            "allow_deltas=",
                            allow_deltas);

                        auto snapshot = pipeline.build_and_snapshot(
                            reader, state_map, allow_deltas);

                        if (!snapshot)
                        {
                            LOGE(
                                "[Builder] Failed to build ledger ",
                                ledger_seq);
                            error_occurred.store(true);
                            break;
                        }

                        PLOGD(
                            version_tracking_log,
                            "[Builder] Snapshot created for ledger ",
                            snapshot->info.seq,
                            " with processing_version: ",
                            snapshot->processing_version,
                            ", state_map still at version: ",
                            state_map->get_version());
                        PLOGD(
                            version_tracking_log,
                            "[Builder] Snapshot contains state_ops: ",
                            snapshot->state_ops.nodes_added,
                            " added, ",
                            snapshot->state_ops.nodes_updated,
                            " updated, ",
                            snapshot->state_ops.nodes_deleted,
                            " deleted");

                        if (snapshot->info.seq != ledger_seq)
                        {
                            LOGE(
                                "[Builder] Ledger sequence mismatch! Expected ",
                                ledger_seq,
                                " but got ",
                                snapshot->info.seq);
                            error_occurred.store(true);
                            break;
                        }

                        // IMPORTANT: Update state_map to point to the snapshot
                        // for next iteration This is the "snapshot chain"
                        // approach
                        state_map = snapshot->state_snapshot;
                        PLOGD(
                            version_tracking_log,
                            "[Builder] Updated state_map to snapshot for next "
                            "ledger, version now: ",
                            state_map->get_version());

                        // Track nodes for stats from the snapshot
                        total_state_nodes_added +=
                            snapshot->state_ops.nodes_added;
                        total_state_nodes_updated +=
                            snapshot->state_ops.nodes_updated;
                        total_state_nodes_deleted +=
                            snapshot->state_ops.nodes_deleted;
                        total_tx_nodes_added += snapshot->tx_ops.nodes_added;

                        // Log comprehensive stats every 1000 ledgers
                        if (ledger_seq % 1000 == 0 &&
                            ledger_seq > last_stats_ledger)
                        {
                            auto now = std::chrono::steady_clock::now();
                            auto elapsed =
                                std::chrono::duration_cast<
                                    std::chrono::seconds>(now - start_time)
                                    .count();
                            auto period_elapsed_ms =
                                std::chrono::duration_cast<
                                    std::chrono::milliseconds>(
                                    now - last_stats_time)
                                    .count();
                            uint32_t ledgers_processed =
                                ledger_seq - start_ledger;
                            uint64_t total_nodes = total_state_nodes_added +
                                total_state_nodes_updated +
                                total_state_nodes_deleted +
                                total_tx_nodes_added;

                            // Get NuDB bytes written and CATL bytes read
                            uint64_t current_bytes_written =
                                pipeline.get_total_bytes_written();
                            uint64_t period_bytes_written =
                                current_bytes_written - last_bytes_written;

                            uint64_t current_bytes_read =
                                reader.body_bytes_consumed();
                            uint64_t period_bytes_read =
                                current_bytes_read - last_bytes_read;

                            double ledgers_per_sec = elapsed > 0
                                ? static_cast<double>(ledgers_processed) /
                                    elapsed
                                : 0;
                            double nodes_per_sec = elapsed > 0
                                ? static_cast<double>(total_nodes) / elapsed
                                : 0;
                            // Use milliseconds for period calculations to
                            // handle fast processing
                            double write_bytes_per_sec = period_elapsed_ms > 0
                                ? static_cast<double>(period_bytes_written) *
                                    1000.0 / period_elapsed_ms
                                : 0;
                            double read_bytes_per_sec = period_elapsed_ms > 0
                                ? static_cast<double>(period_bytes_read) *
                                    1000.0 / period_elapsed_ms
                                : 0;

                            last_bytes_written = current_bytes_written;
                            last_bytes_read = current_bytes_read;
                            last_stats_time = now;

                            size_t snapshot_depth =
                                snapshot_queue.read_available();
                            size_t hashed_depth = hashed_queue.read_available();

                            LOGI("=====================================");
                            LOGI("📊 PIPELINE STATS @ Ledger ", ledger_seq);
                            LOGI("=====================================");
                            LOGI("⏱️  Performance:");
                            LOGI("   - Ledgers processed: ", ledgers_processed);
                            LOGI("   - Elapsed time: ", elapsed, " seconds");
                            LOGI(
                                "   - Throughput: ",
                                std::fixed,
                                std::setprecision(2),
                                ledgers_per_sec,
                                " ledgers/sec, ",
                                nodes_per_sec,
                                " nodes/sec");
                            // Calculate total average throughput
                            double total_write_mb_per_sec = elapsed > 0
                                ? (static_cast<double>(current_bytes_written) /
                                   elapsed) /
                                    1024 / 1024
                                : 0;
                            double total_read_mb_per_sec = elapsed > 0
                                ? (static_cast<double>(current_bytes_read) /
                                   elapsed) /
                                    1024 / 1024
                                : 0;

                            LOGI(
                                "   - CATL read: ",
                                std::fixed,
                                std::setprecision(2),
                                read_bytes_per_sec / 1024 / 1024,
                                " MB/sec (period), ",
                                total_read_mb_per_sec,
                                " MB/sec (total avg) [",
                                current_bytes_read / 1024 / 1024,
                                " MB]");
                            LOGI(
                                "   - NuDB write: ",
                                std::fixed,
                                std::setprecision(2),
                                write_bytes_per_sec / 1024 / 1024,
                                " MB/sec (period), ",
                                total_write_mb_per_sec,
                                " MB/sec (total avg) [",
                                current_bytes_written / 1024 / 1024,
                                " MB]");

                            LOGI("📦 Queue depths:");
                            LOGI(
                                "   - Snapshot queue: ",
                                snapshot_depth,
                                "/",
                                SNAPSHOT_QUEUE_SIZE);
                            LOGI(
                                "   - Hashed queue: ",
                                hashed_depth,
                                "/",
                                HASHED_QUEUE_SIZE);
                            LOGI(
                                "   - Total snapshots in memory: ",
                                snapshot_depth + hashed_depth);

                            uint32_t ledgers_in_period =
                                ledger_seq - last_stats_ledger;
                            uint32_t period_waits =
                                queue_full_waits - last_queue_full_waits;
                            uint32_t period_no_waits =
                                ledgers_in_period - period_waits;

                            // Cumulative stats
                            uint32_t total_ledgers = ledger_seq - start_ledger;
                            uint32_t total_no_waits =
                                total_ledgers - queue_full_waits;

                            if (period_waits > 0 || queue_full_waits > 0)
                            {
                                LOGI(
                                    "⚠️  Backpressure: ",
                                    period_waits,
                                    " / ",
                                    period_no_waits,
                                    " (last ",
                                    ledgers_in_period,
                                    ") | ",
                                    queue_full_waits,
                                    " / ",
                                    total_no_waits,
                                    " (all)");
                            }

                            last_queue_full_waits = queue_full_waits;

                            LOGI("🗺️  Accumulated Node Operations:");
                            LOGI(
                                "   - State nodes added: ",
                                total_state_nodes_added);
                            LOGI(
                                "   - State nodes updated: ",
                                total_state_nodes_updated);
                            LOGI(
                                "   - State nodes deleted: ",
                                total_state_nodes_deleted);
                            LOGI("   - Tx nodes added: ", total_tx_nodes_added);
                            LOGI("   - Total operations: ", total_nodes);
                            LOGI("=====================================");

                            last_stats_ledger = ledger_seq;
                        }

                        // Push to queue (blocking if full)
                        bool had_to_wait = false;
                        while (!snapshot_queue.push(*snapshot))
                        {
                            had_to_wait = true;
                            std::this_thread::yield();
                        }
                        if (had_to_wait)
                        {
                            queue_full_waits++;
                        }
                    }

                    LOGI("[Builder] Done");
                    builder_done.store(true);
                }
                catch (const std::exception& e)
                {
                    LOGE("[Builder] Exception: ", e.what());
                    error_occurred.store(true);
                    builder_done.store(true);
                }
            });

            // Thread 2: Hash + Verify
            std::thread hasher_thread([&]() {
                try
                {
                    LOGI("[Hasher] Starting...");

                    while (true)
                    {
                        if (error_occurred.load())
                            break;

                        LedgerSnapshot snapshot;
                        if (snapshot_queue.pop(snapshot))
                        {
                            PLOGD(
                                version_tracking_log,
                                "[Hasher] Processing ledger ",
                                snapshot.info.seq,
                                " with processing_version: ",
                                snapshot.processing_version);

                            auto hashed = pipeline.hash_and_verify(snapshot);

                            PLOGD(
                                version_tracking_log,
                                "[Hasher] Hashed ledger ",
                                hashed.info.seq,
                                ", verified: ",
                                hashed.verified,
                                ", processing_version carried forward: ",
                                hashed.processing_version);

                            if (!hashed.verified)
                            {
                                LOGE(
                                    "[Hasher] Hash verification failed for "
                                    "ledger ",
                                    hashed.info.seq);
                                error_occurred.store(true);
                                break;
                            }

                            // Push to next queue (blocking if full)
                            while (!hashed_queue.push(hashed))
                            {
                                std::this_thread::yield();
                            }
                        }
                        else if (builder_done.load())
                        {
                            // Builder is done and queue is empty
                            break;
                        }
                        else
                        {
                            std::this_thread::yield();
                        }
                    }

                    LOGI("[Hasher] Done");
                    hasher_done.store(true);
                }
                catch (const std::exception& e)
                {
                    LOGE("[Hasher] Exception: ", e.what());
                    error_occurred.store(true);
                    hasher_done.store(true);
                }
            });

            // Thread 3: Flush to NuDB (main thread does this)
            LOGI("[Flusher] Starting...");
            size_t flushed_count = 0;

            while (true)
            {
                if (error_occurred.load())
                    break;

                HashedLedger hashed;
                if (hashed_queue.pop(hashed))
                {
                    PLOGD(
                        version_tracking_log,
                        "[Flusher] About to flush ledger ",
                        hashed.info.seq,
                        " with processing_version: ",
                        hashed.processing_version,
                        ", state_ops: ",
                        hashed.state_ops.nodes_added,
                        " added, ",
                        hashed.state_ops.nodes_updated,
                        " updated");

                    if (!pipeline.flush_to_nudb(hashed))
                    {
                        LOGE(
                            "[Flusher] Failed to flush ledger ",
                            hashed.info.seq);
                        error_occurred.store(true);
                        break;
                    }

                    PLOGD(
                        version_tracking_log,
                        "[Flusher] Successfully flushed ledger ",
                        hashed.info.seq);
                    flushed_count++;
                }
                else if (hasher_done.load())
                {
                    // Hasher is done and queue is empty
                    break;
                }
                else
                {
                    std::this_thread::yield();
                }
            }

            LOGI("[Flusher] Done - flushed ", flushed_count, " ledgers");

            // Wait for threads to complete
            builder_thread.join();
            hasher_thread.join();

            if (error_occurred.load())
            {
                LOGE("Pipeline error occurred");
                return false;
            }

            LOGI("\n========================================");
            LOGI(
                "Successfully processed ledgers ",
                start_ledger,
                " to ",
                end_ledger);
            LOGI("========================================");

            // Close NuDB database (this flushes final in-memory pool to disk)
            LOGI("\nClosing database to flush final batch...");
            if (!pipeline.close_database())
            {
                LOGE("Failed to close NuDB database!");
                return false;
            }

            // Reopen database for verification
            LOGI("Reopening database for verification...");
            if (!pipeline.open_database(*options_.nudb_path))
            {
                LOGE("Failed to reopen NuDB database for verification!");
                return false;
            }

            // Verify all keys are readable from NuDB (using 8 threads)
            LOGI("\nVerifying NuDB database integrity...");
            if (!pipeline.verify_all_keys(8))
            {
                LOGE("Database verification failed!");
                pipeline.close_database();  // Close before returning
                return false;
            }

            // Final close
            pipeline.close_database();

            return true;
        }
        catch (const std::exception& e)
        {
            LOGE("Error during conversion: ", e.what());
            return false;
        }
    }
};

/**
 * Test snapshot memory usage by reading ledgers and creating snapshots
 * without the full pipeline processing
 */
bool
test_snapshot_memory(const Catl1ToNudbOptions& options)
{
    try
    {
        LOGI("Starting snapshot memory test mode");
        LOGI("Reading input file: ", *options.input_file);

        // Open the input CATL file
        Reader reader(*options.input_file);
        const CatlHeader& header = reader.header();

        LOGI("File information:");
        LOGI("  Ledger range: ", header.min_ledger, " - ", header.max_ledger);
        LOGI("  Network ID: ", header.network_id);

        // Determine ledger range to process
        uint32_t start_ledger =
            options.start_ledger.value_or(header.min_ledger);
        uint32_t end_ledger = options.end_ledger.value_or(header.max_ledger);

        LOGI("Testing ledgers ", start_ledger, " to ", end_ledger);

        // Create SHAMap options for non-collapsed tree
        SHAMapOptions map_options;
        map_options.tree_collapse_impl = TreeCollapseImpl::leafs_only;

        // Create state map
        auto state_map = std::make_shared<SHAMap>(tnACCOUNT_STATE, map_options);

        // Enable CoW by taking an initial snapshot before any processing
        // This ensures all nodes get proper versions instead of -1
        auto initial_snapshot = state_map->snapshot();
        initial_snapshot.reset();  // Immediately discard it

        auto start_time = std::chrono::steady_clock::now();
        uint32_t ledgers_processed = 0;

        for (uint32_t ledger_seq = start_ledger; ledger_seq <= end_ledger;
             ++ledger_seq)
        {
            // Read ledger info
            (void)reader.read_ledger_info();  // need to move past the ledger
            // auto canonical_info = to_canonical_ledger_info(v1_ledger_info);

            // Load state map with deltas
            bool allow_deltas = (ledger_seq > start_ledger);
            MapOperations state_ops = reader.read_map_with_shamap_owned_items(
                *state_map, tnACCOUNT_STATE, allow_deltas);

            // Create a snapshot (this is what we're testing)
            auto snapshot = state_map->snapshot();

            // Build fresh transaction map
            SHAMap tx_map(tnTRANSACTION_MD, map_options);
            MapOperations tx_ops = reader.read_map_with_shamap_owned_items(
                tx_map, tnTRANSACTION_MD, false);

            ledgers_processed++;

            // Log progress every 1000 ledgers
            if (ledger_seq % 1000 == 0 && ledger_seq > start_ledger)
            {
                auto now = std::chrono::steady_clock::now();
                auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                                   now - start_time)
                                   .count();
                double ledgers_per_sec = elapsed > 0
                    ? static_cast<double>(ledgers_processed) / elapsed
                    : 0;

                LOGI("=====================================");
                LOGI("📊 SNAPSHOT TEST @ Ledger ", ledger_seq);
                LOGI("=====================================");
                LOGI("  - Ledgers processed: ", ledgers_processed);
                LOGI("  - Elapsed time: ", elapsed, " seconds");
                LOGI(
                    "  - Throughput: ",
                    std::fixed,
                    std::setprecision(2),
                    ledgers_per_sec,
                    " ledgers/sec");
                LOGI(
                    "  - State ops: ",
                    state_ops.nodes_added,
                    " added, ",
                    state_ops.nodes_updated,
                    " updated, ",
                    state_ops.nodes_deleted,
                    " deleted");
                LOGI("  - Tx ops: ", tx_ops.nodes_added, " added");
                LOGI("=====================================");
            }

            // IMPORTANT: The snapshot goes out of scope here and should be
            // destroyed If memory isn't being released, we'll see it grow
            // continuously
        }

        auto end_time = std::chrono::steady_clock::now();
        auto total_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                                 end_time - start_time)
                                 .count();

        LOGI("========================================");
        LOGI("Snapshot test completed");
        LOGI("  - Total ledgers: ", ledgers_processed);
        LOGI("  - Total time: ", total_elapsed, " seconds");
        LOGI(
            "  - Average: ",
            std::fixed,
            std::setprecision(2),
            static_cast<double>(ledgers_processed) / total_elapsed,
            " ledgers/sec");
        LOGI("========================================");
        LOGI("Check memory usage now - snapshots should have been released!");

        return true;
    }
    catch (const std::exception& e)
    {
        LOGE("Snapshot test error: ", e.what());
        return false;
    }
}

int
main(int argc, char* argv[])
{
    // Parse command line arguments
    Catl1ToNudbOptions options = parse_catl1_to_nudb_argv(argc, argv);

    // Display help if requested or if there was a parsing error
    if (options.show_help || !options.valid)
    {
        if (!options.valid && options.error_message)
        {
            std::cerr << "Error: " << *options.error_message << std::endl
                      << std::endl;
        }
        std::cout << options.help_text << std::endl;
        return options.valid ? 0 : 1;
    }

    try
    {
        // Set the log level
        if (!Logger::set_level(options.log_level))
        {
            Logger::set_level(LogLevel::INFO);
            std::cerr << "Unrecognized log level: " << options.log_level
                      << ", falling back to 'info'" << std::endl;
        }

        // Check if we're in test snapshot mode
        if (options.test_snapshots)
        {
            LOGI("Running in snapshot test mode");
            if (test_snapshot_memory(options))
            {
                LOGI("Snapshot test completed successfully");
                return 0;
            }
            else
            {
                LOGE("Snapshot test failed");
                return 1;
            }
        }

        LOGI("Starting CATL to NuDB conversion");

        Catl1ToNudbConverter converter(options);
        if (converter.convert())
        {
            LOGI("Conversion completed successfully");
            return 0;
        }
        else
        {
            LOGE("Conversion failed");
            return 1;
        }
    }
    catch (const std::exception& e)
    {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
}