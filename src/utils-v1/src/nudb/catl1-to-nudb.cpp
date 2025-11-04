#include "catl/core/log-macros.h"
#include "catl/utils-v1/nudb/catl1-to-nudb-arg-options.h"
#include "catl/utils-v1/nudb/catl1-to-nudb-pipeline.h"
#include "catl/v1/catl-v1-reader.h"
#include "catl/v1/catl-v1-utils.h"
#include "catl/shamap/shamap.h"

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

// Pipeline queue configuration constants
static constexpr size_t SNAPSHOT_QUEUE_SIZE = 100;  // Buffer size for snapshot queue
static constexpr size_t HASHED_QUEUE_SIZE = 100;    // Buffer size for hashed ledger queue

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

            LOGI("Processing ledgers ", start_ledger, " to ", end_ledger);

            // Create SHAMap options for non-collapsed tree (we need inner nodes for NuDB)
            SHAMapOptions map_options;
            map_options.tree_collapse_impl = TreeCollapseImpl::leafs_only;

            // Create pipeline
            CatlNudbPipeline pipeline(map_options);

            // Create SPSC queues between stages
            // Queue sizes are configured by constants for consistent reporting
            boost::lockfree::spsc_queue<LedgerSnapshot> snapshot_queue(SNAPSHOT_QUEUE_SIZE);
            boost::lockfree::spsc_queue<HashedLedger> hashed_queue(HASHED_QUEUE_SIZE);

            // Error tracking (atomic for thread safety)
            std::atomic<bool> error_occurred{false};
            std::atomic<bool> builder_done{false};
            std::atomic<bool> hasher_done{false};

            // Thread 1: Build + Snapshot
            std::thread builder_thread([&]() {
                try
                {
                    LOGI("[Builder] Starting...");

                    // Create reader in builder thread (Reader is not thread-safe)
                    Reader reader(*options_.input_file);
                    SHAMap state_map(tnACCOUNT_STATE, map_options);

                    // Stats tracking
                    auto start_time = std::chrono::steady_clock::now();
                    uint64_t total_state_nodes_added = 0;
                    uint64_t total_state_nodes_updated = 0;
                    uint64_t total_state_nodes_deleted = 0;
                    uint64_t total_tx_nodes_added = 0;
                    uint32_t last_stats_ledger = start_ledger;

                    for (uint32_t ledger_seq = start_ledger; ledger_seq <= end_ledger;
                         ++ledger_seq)
                    {
                        if (error_occurred.load())
                            break;

                        bool allow_deltas = (ledger_seq > start_ledger);
                        auto snapshot = pipeline.build_and_snapshot(reader, state_map, allow_deltas);

                        if (!snapshot)
                        {
                            LOGE("[Builder] Failed to build ledger ", ledger_seq);
                            error_occurred.store(true);
                            break;
                        }

                        if (snapshot->info.seq != ledger_seq)
                        {
                            LOGE("[Builder] Ledger sequence mismatch! Expected ", ledger_seq, " but got ", snapshot->info.seq);
                            error_occurred.store(true);
                            break;
                        }

                        // Track nodes for stats from the snapshot
                        total_state_nodes_added += snapshot->state_ops.nodes_added;
                        total_state_nodes_updated += snapshot->state_ops.nodes_updated;
                        total_state_nodes_deleted += snapshot->state_ops.nodes_deleted;
                        total_tx_nodes_added += snapshot->tx_ops.nodes_added;

                        // Log comprehensive stats every 1000 ledgers
                        if (ledger_seq % 1000 == 0 && ledger_seq > last_stats_ledger)
                        {
                            auto now = std::chrono::steady_clock::now();
                            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - start_time).count();
                            uint32_t ledgers_processed = ledger_seq - start_ledger;
                            uint64_t total_nodes = total_state_nodes_added + total_state_nodes_updated +
                                                  total_state_nodes_deleted + total_tx_nodes_added;

                            double ledgers_per_sec = elapsed > 0 ? static_cast<double>(ledgers_processed) / elapsed : 0;
                            double nodes_per_sec = elapsed > 0 ? static_cast<double>(total_nodes) / elapsed : 0;

                            size_t snapshot_depth = snapshot_queue.read_available();
                            size_t hashed_depth = hashed_queue.read_available();

                            LOGI("=====================================");
                            LOGI("📊 PIPELINE STATS @ Ledger ", ledger_seq);
                            LOGI("=====================================");
                            LOGI("⏱️  Performance:");
                            LOGI("   - Ledgers processed: ", ledgers_processed);
                            LOGI("   - Elapsed time: ", elapsed, " seconds");
                            LOGI("   - Throughput: ", std::fixed, std::setprecision(2),
                                 ledgers_per_sec, " ledgers/sec, ",
                                 nodes_per_sec, " nodes/sec");

                            LOGI("📦 Queue depths:");
                            LOGI("   - Snapshot queue: ", snapshot_depth, "/", SNAPSHOT_QUEUE_SIZE);
                            LOGI("   - Hashed queue: ", hashed_depth, "/", HASHED_QUEUE_SIZE);
                            LOGI("   - Total snapshots in memory: ", snapshot_depth + hashed_depth);

                            LOGI("🗺️  Accumulated Node Operations:");
                            LOGI("   - State nodes added: ", total_state_nodes_added);
                            LOGI("   - State nodes updated: ", total_state_nodes_updated);
                            LOGI("   - State nodes deleted: ", total_state_nodes_deleted);
                            LOGI("   - Tx nodes added: ", total_tx_nodes_added);
                            LOGI("   - Total operations: ", total_nodes);
                            LOGI("=====================================");

                            last_stats_ledger = ledger_seq;
                        }

                        // Push to queue (blocking if full)
                        while (!snapshot_queue.push(*snapshot))
                        {
                            // Queue is full - log if we're waiting
                            if (ledger_seq % 100 == 0)
                            {
                                LOGW("[Builder] Queue FULL at ledger ", ledger_seq, " - waiting for hasher...");
                            }
                            std::this_thread::yield();
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
                            auto hashed = pipeline.hash_and_verify(snapshot);

                            if (!hashed.verified)
                            {
                                LOGE("[Hasher] Hash verification failed for ledger ", hashed.info.seq);
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
                    if (!pipeline.flush_to_nudb(hashed))
                    {
                        LOGE("[Flusher] Failed to flush ledger ", hashed.info.seq);
                        error_occurred.store(true);
                        break;
                    }
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
            LOGI("Successfully processed ledgers ",
                 start_ledger,
                 " to ",
                 end_ledger);
            LOGI("========================================");

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
        uint32_t start_ledger = options.start_ledger.value_or(header.min_ledger);
        uint32_t end_ledger = options.end_ledger.value_or(header.max_ledger);

        LOGI("Testing ledgers ", start_ledger, " to ", end_ledger);

        // Create SHAMap options for non-collapsed tree
        SHAMapOptions map_options;
        map_options.tree_collapse_impl = TreeCollapseImpl::leafs_only;

        // Create state map
        SHAMap state_map(tnACCOUNT_STATE, map_options);

        auto start_time = std::chrono::steady_clock::now();
        uint32_t ledgers_processed = 0;

        for (uint32_t ledger_seq = start_ledger; ledger_seq <= end_ledger; ++ledger_seq)
        {
            // Read ledger info
            auto v1_ledger_info = reader.read_ledger_info();
            auto canonical_info = to_canonical_ledger_info(v1_ledger_info);

            // Load state map with deltas
            bool allow_deltas = (ledger_seq > start_ledger);
            MapOperations state_ops = reader.read_map_with_shamap_owned_items(
                state_map, tnACCOUNT_STATE, allow_deltas);

            // Create a snapshot (this is what we're testing)
            auto snapshot = state_map.snapshot();

            // Build fresh transaction map
            SHAMap tx_map(tnTRANSACTION_MD, map_options);
            MapOperations tx_ops = reader.read_map_with_shamap_owned_items(
                tx_map, tnTRANSACTION_MD, false);

            ledgers_processed++;

            // Log progress every 1000 ledgers
            if (ledger_seq % 1000 == 0 && ledger_seq > start_ledger)
            {
                auto now = std::chrono::steady_clock::now();
                auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - start_time).count();
                double ledgers_per_sec = elapsed > 0 ? static_cast<double>(ledgers_processed) / elapsed : 0;

                LOGI("=====================================");
                LOGI("📊 SNAPSHOT TEST @ Ledger ", ledger_seq);
                LOGI("=====================================");
                LOGI("  - Ledgers processed: ", ledgers_processed);
                LOGI("  - Elapsed time: ", elapsed, " seconds");
                LOGI("  - Throughput: ", std::fixed, std::setprecision(2), ledgers_per_sec, " ledgers/sec");
                LOGI("  - State ops: ", state_ops.nodes_added, " added, ",
                     state_ops.nodes_updated, " updated, ",
                     state_ops.nodes_deleted, " deleted");
                LOGI("  - Tx ops: ", tx_ops.nodes_added, " added");
                LOGI("=====================================");
            }

            // IMPORTANT: The snapshot goes out of scope here and should be destroyed
            // If memory isn't being released, we'll see it grow continuously
        }

        auto end_time = std::chrono::steady_clock::now();
        auto total_elapsed = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time).count();

        LOGI("========================================");
        LOGI("Snapshot test completed");
        LOGI("  - Total ledgers: ", ledgers_processed);
        LOGI("  - Total time: ", total_elapsed, " seconds");
        LOGI("  - Average: ", std::fixed, std::setprecision(2),
             static_cast<double>(ledgers_processed) / total_elapsed, " ledgers/sec");
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