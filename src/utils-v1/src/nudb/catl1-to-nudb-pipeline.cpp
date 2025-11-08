#include "catl/utils-v1/nudb/catl1-to-nudb-pipeline.h"
#include "catl/core/log-macros.h"
#include "catl/core/logger.h"
#include "catl/nodestore/node_blob.h"
#include "catl/nodestore/node_types.h"
#include "catl/shamap/shamap-innernode.h"
#include "catl/shamap/shamap-leafnode.h"
#include "catl/shamap/shamap-treenode.h"
#include "catl/shamap/shamap.h"  // For walk_nodes_log
#include "catl/utils-v1/nudb/deduplication-strategy.h"
#include "catl/v1/catl-v1-reader.h"  // For map_ops_log
#include "catl/v1/catl-v1-utils.h"
#include "catl/xdata/json-visitor.h"
#include "catl/xdata/parser-context.h"
#include "catl/xdata/parser.h"
#include "catl/xdata/protocol.h"
#include <boost/filesystem.hpp>
#include <boost/json.hpp>
#include <chrono>
#include <fstream>
#include <iostream>
#include <nudb/create.hpp>
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
CatlNudbPipeline::set_compression_threads(int threads)
{
    if (threads <= 0 || threads > 32)
    {
        throw std::invalid_argument(
            "Compression threads must be between 1 and 32");
    }
    if (!compression_workers_.empty())
    {
        throw std::runtime_error(
            "Cannot change compression threads after pipeline started");
    }
    compression_threads_ = threads;
    LOGI("Set compression threads to ", threads);
}

void
CatlNudbPipeline::set_max_write_queue_mb(uint32_t mb)
{
    if (mb == 0)
    {
        throw std::invalid_argument(
            "Max write queue MB must be greater than 0");
    }
    if (!compression_workers_.empty())
    {
        throw std::runtime_error(
            "Cannot change max write queue size after pipeline started");
    }
    max_write_queue_bytes_ = static_cast<uint64_t>(mb) * 1024 * 1024;
    LOGI(
        "Set max write queue size to ",
        mb,
        " MB (",
        max_write_queue_bytes_,
        " bytes)");
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

void
CatlNudbPipeline::set_dedupe_strategy(const std::string& strategy)
{
    dedupe_strategy_ = strategy;
    LOGI("Deduplication strategy set to: ", strategy);
}

void
CatlNudbPipeline::set_mock_mode(const std::string& mode)
{
    mock_mode_ = mode;
    if (mode == "noop" || mode == "memory")
    {
        LOGI("Mock mode: ", mode, " - skipping all I/O operations");
    }
    else if (mode == "disk")
    {
        LOGI("Mock mode: disk - buffered append-only file writes");
    }
}

bool
CatlNudbPipeline::create_database(
    const std::string& path,
    uint32_t key_size,
    uint32_t block_size,
    double load_factor)
{
    namespace fs = boost::filesystem;

    db_path_ = path;

    // Store parameters for later use (e.g., in open_database for verification)
    key_size_ = key_size;
    block_size_ = block_size;
    load_factor_ = load_factor;

    // Handle mock modes
    if (!mock_mode_.empty())
    {
        if (mock_mode_ == "noop" || mock_mode_ == "memory")
        {
            LOGI("Mock mode (", mock_mode_, "): skipping database creation");

            // Start compression pipeline
            start_compression_pipeline();

            return true;
        }
        else if (mock_mode_ == "disk")
        {
            // Create directory if needed
            fs::path dir(path);
            if (!fs::exists(dir))
            {
                LOGI("Creating mock disk directory: ", path);
                fs::create_directories(dir);
            }

            // Open buffered append-only file
            fs::path mock_file = dir / "mock_disk.bin";
            LOGI(
                "Mock mode (disk): creating buffered file at ",
                mock_file.string());

            // Remove existing file if it exists
            if (fs::exists(mock_file))
            {
                fs::remove(mock_file);
            }

            mock_disk_file_ = std::make_unique<std::ofstream>(
                mock_file.string(), std::ios::binary | std::ios::app);

            if (!mock_disk_file_->is_open())
            {
                LOGE("Failed to open mock disk file: ", mock_file.string());
                return false;
            }

            // Set large buffer for better performance
            const size_t buffer_size = 1024 * 1024;  // 1MB buffer
            mock_disk_file_->rdbuf()->pubsetbuf(nullptr, buffer_size);

            LOGI("Mock disk file opened with 1MB buffer");

            // Start compression pipeline
            start_compression_pipeline();

            return true;
        }
        else if (mock_mode_ == "nudb")
        {
            // Use regular NuDB (not bulk writer) for testing
            LOGI(
                "Mock mode (nudb): using regular NuDB inserts (no bulk "
                "writer)");

            fs::path dir(path);

            // Create directory if needed
            if (!fs::exists(dir))
            {
                LOGI("Creating directory: ", dir.string());
                boost::system::error_code fs_ec;
                fs::create_directories(dir, fs_ec);
                if (fs_ec)
                {
                    LOGE("Failed to create directory: ", fs_ec.message());
                    return false;
                }
            }

            // Get absolute paths and normalize (remove ./ and other cruft)
            fs::path abs_dat =
                fs::absolute(dir / "nudb.dat").lexically_normal();
            fs::path abs_key =
                fs::absolute(dir / "nudb.key").lexically_normal();
            fs::path abs_log =
                fs::absolute(dir / "nudb.log").lexically_normal();

            std::string dat_path = abs_dat.string();
            std::string key_path = abs_key.string();
            std::string log_path = abs_log.string();

            // Delete existing database
            ::nudb::error_code ec;
            ::nudb::native_file::erase(dat_path, ec);
            if (ec)
                LOGI(
                    "Erase dat: ", ec.message(), " (ok if file doesn't exist)");
            ec = {};
            ::nudb::native_file::erase(key_path, ec);
            if (ec)
                LOGI(
                    "Erase key: ", ec.message(), " (ok if file doesn't exist)");
            ec = {};
            ::nudb::native_file::erase(log_path, ec);
            if (ec)
                LOGI(
                    "Erase log: ", ec.message(), " (ok if file doesn't exist)");
            ec = {};

            // Verify directory exists
            if (!fs::exists(dir))
            {
                LOGE("ERROR: Directory disappeared! ", dir.string());
                return false;
            }
            LOGI("Directory verified: ", dir.string());

            // Create new NuDB database
            LOGI("Creating NuDB database:");
            LOGI("  dat: ", dat_path);
            LOGI("  key: ", key_path);
            LOGI("  log: ", log_path);
            LOGI("  key_size: ", key_size);
            LOGI("  block_size: ", block_size);
            LOGI("  load_factor: ", load_factor);

            ::nudb::create<::nudb::xxhasher>(
                dat_path,
                key_path,
                log_path,
                1,  // appnum
                ::nudb::make_uid(),
                ::nudb::make_salt(),
                key_size,
                block_size,
                load_factor,
                ec);

            if (ec)
            {
                LOGE("Failed to create NuDB: ", ec.message());
                LOGE("Error code: ", ec.value());
                LOGE("Error category: ", ec.category().name());

                // Clean up any partial files
                ::nudb::native_file::erase(dat_path, ec);
                ::nudb::native_file::erase(key_path, ec);
                ::nudb::native_file::erase(log_path, ec);

                return false;
            }

            // Open database for regular inserts
            db_ = std::make_unique<::nudb::store>();
            db_->open(dat_path, key_path, log_path, ec);

            if (ec)
            {
                LOGE("Failed to open NuDB: ", ec.message());
                return false;
            }

            LOGI("NuDB opened successfully (regular insert mode)");
            start_compression_pipeline();
            return true;
        }
    }

    // Real NuDB mode - use bulk writer for optimal performance

    // Create directory if needed
    fs::path dir(path);
    if (!fs::exists(dir))
    {
        LOGI("Creating NuDB directory: ", path);
        fs::create_directories(dir);
    }

    // NuDB file paths
    fs::path dat_path = dir / "nudb.dat";
    fs::path key_path = dir / "nudb.key";
    fs::path log_path = dir / "nudb.log";

    LOGI("Using NuDB bulk writer (optimized for bulk import)");
    LOGI("  key_size: ", key_size, " bytes");
    LOGI("  block_size: ", block_size);
    LOGI("  load_factor: ", load_factor);

    // Create deduplication strategy based on user choice
    // The "brain" strategy is created first, then assigned based on threading
    // mode
    std::unique_ptr<DeduplicationStrategy> dedupe_brain_strategy;

    if (dedupe_strategy_ == "none")
    {
        LOGI("Deduplication: NONE (fastest, duplicates written to .dat)");
        dedupe_brain_strategy = std::make_unique<NoDeduplicationStrategy>();
    }
    else if (dedupe_strategy_ == "cuckoo-rocks")
    {
        LOGI("Deduplication: Cuckoo+Rocks (hybrid filter + disk-backed)");
        fs::path rocks_dedup_path = dir / "dedup-rocks";
        LOGI("  💾 RocksDB path: ", fs::absolute(rocks_dedup_path).string());
        LOGI("     (You can monitor this directory during import!)");
        // CuckooRocksStrategy: Fast in-memory cuckoo filter + RocksDB ground
        // truth Default: 100M expected items, ~150MB cuckoo + 1GB RocksDB cache
        // = ~1.3GB RAM
        dedupe_brain_strategy = std::make_unique<CuckooRocksStrategy>(
            rocks_dedup_path.string(), false);
    }
    else
    {
        LOGE("Unknown dedupe strategy: ", dedupe_strategy_);
        throw std::runtime_error(
            "Unknown dedupe strategy: " + dedupe_strategy_);
    }

    // Assign strategy based on threading mode
    std::unique_ptr<DeduplicationStrategy> bulk_writer_dedupe_strategy;

    if (use_dedupe_thread_)
    {
        // PARALLEL MODE: Pipeline owns the "brain", bulk_writer gets no-op
        LOGI("🔀 Parallel dedupe mode: dedupe runs in separate thread");
        pipeline_dedup_strategy_ = std::move(dedupe_brain_strategy);
        bulk_writer_dedupe_strategy =
            std::make_unique<NoDeduplicationStrategy>();
    }
    else
    {
        // SEQUENTIAL MODE: bulk_writer owns the "brain" (current behavior)
        LOGI("🔁 Sequential dedupe mode: dedupe runs in writer thread");
        bulk_writer_dedupe_strategy = std::move(dedupe_brain_strategy);
    }

    // Create bulk writer
    bulk_writer_ = std::make_unique<NudbBulkWriter>(
        dat_path.string(),
        key_path.string(),
        log_path.string(),
        key_size,
        std::move(bulk_writer_dedupe_strategy));

    // Suppress bulk_writer stats if using parallel dedupe mode
    // (the pipeline will print stats from the real strategy instead)
    if (use_dedupe_thread_)
    {
        bulk_writer_->set_suppress_stats(true);
    }

    // Open bulk writer (this creates the files and prepares for writing)
    if (!bulk_writer_->open(block_size, load_factor))
    {
        LOGE("Failed to open bulk writer");
        bulk_writer_.reset();
        return false;
    }

    LOGI("Bulk writer opened successfully");

    // Start compression pipeline
    start_compression_pipeline();

    return true;
}

bool
CatlNudbPipeline::open_database(const std::string& path)
{
    namespace fs = boost::filesystem;

    // For nudb mock mode, we need to delete old files and reopen for
    // verification
    if (mock_mode_ == "nudb")
    {
        LOGI("Mock mode (nudb): deleting old database files and reopening...");

        db_path_ = path;
        fs::path dir(path);

        // Ensure directory exists
        if (!fs::exists(dir))
        {
            LOGI("Creating directory for verification: ", dir.string());
            boost::system::error_code fs_ec;
            fs::create_directories(dir, fs_ec);
            if (fs_ec)
            {
                LOGE("Failed to create directory: ", fs_ec.message());
                return false;
            }
        }

        // Get absolute paths and normalize (remove ./ and other cruft)
        fs::path abs_dat = fs::absolute(dir / "nudb.dat").lexically_normal();
        fs::path abs_key = fs::absolute(dir / "nudb.key").lexically_normal();
        fs::path abs_log = fs::absolute(dir / "nudb.log").lexically_normal();

        std::string dat_path = abs_dat.string();
        std::string key_path = abs_key.string();
        std::string log_path = abs_log.string();

        // Delete old files
        ::nudb::error_code ec;
        ::nudb::native_file::erase(dat_path, ec);
        if (ec)
            LOGI("Erase dat: ", ec.message(), " (ok if file doesn't exist)");
        ec = {};
        ::nudb::native_file::erase(key_path, ec);
        if (ec)
            LOGI("Erase key: ", ec.message(), " (ok if file doesn't exist)");
        ec = {};
        ::nudb::native_file::erase(log_path, ec);
        if (ec)
            LOGI("Erase log: ", ec.message(), " (ok if file doesn't exist)");
        ec = {};

        // Verify directory exists
        if (!fs::exists(dir))
        {
            LOGE("ERROR: Directory disappeared! ", dir.string());
            return false;
        }
        LOGI("Directory verified: ", dir.string());

        LOGI("Creating fresh NuDB for verification:");
        LOGI("  dat: ", dat_path);
        LOGI("  key: ", key_path);
        LOGI("  log: ", log_path);
        LOGI("  key_size: ", key_size_);
        LOGI("  block_size: ", block_size_);
        LOGI("  load_factor: ", load_factor_);

        // Create new database
        ::nudb::create<::nudb::xxhasher>(
            dat_path,
            key_path,
            log_path,
            1,  // appnum
            ::nudb::make_uid(),
            ::nudb::make_salt(),
            key_size_,
            block_size_,
            load_factor_,
            ec);

        if (ec)
        {
            LOGE("Failed to create NuDB for verification: ", ec.message());
            LOGE("Error code: ", ec.value());
            LOGE("Error category: ", ec.category().name());

            // Clean up any partial files
            ::nudb::native_file::erase(dat_path, ec);
            ::nudb::native_file::erase(key_path, ec);
            ::nudb::native_file::erase(log_path, ec);

            return false;
        }

        // Open it
        db_ = std::make_unique<::nudb::store>();
        db_->open(dat_path, key_path, log_path, ec);

        if (ec)
        {
            LOGE("Failed to open NuDB for verification: ", ec.message());
            db_.reset();
            return false;
        }

        LOGI("Created and opened fresh NuDB database for verification");
        return true;
    }

    // Skip database operations in other mock modes
    if (!mock_mode_.empty())
    {
        LOGI("Mock mode (", mock_mode_, "): skipping database open");
        return true;
    }

    db_path_ = path;

    // NuDB file paths
    fs::path dir(path);
    fs::path dat_path = dir / "nudb.dat";
    fs::path key_path = dir / "nudb.key";
    fs::path log_path = dir / "nudb.log";

    // Verify essential files exist (dat and key)
    // Note: log file may not exist after clean close (it's only for crash
    // recovery)
    if (!fs::exists(dat_path) || !fs::exists(key_path))
    {
        LOGE("NuDB database files not found at: ", path);
        LOGE("  dat exists: ", fs::exists(dat_path));
        LOGE("  key exists: ", fs::exists(key_path));
        LOGE("  log exists: ", fs::exists(log_path), " (optional)");
        return false;
    }

    // Open the existing database
    db_ = std::make_unique<
        ::nudb::basic_store<::nudb::xxhasher, ::nudb::posix_file>>();
    ::nudb::error_code ec;
    db_->open(dat_path.string(), key_path.string(), log_path.string(), ec);

    if (ec)
    {
        LOGE("Failed to open NuDB database: ", ec.message());
        db_.reset();
        return false;
    }

    LOGI("Opened existing NuDB database at: ", path);
    return true;
}

bool
CatlNudbPipeline::close_database()
{
    // Stop compression pipeline first (wait for all workers to finish)
    stop_compression_pipeline();

    // Handle mock mode closing
    if (!mock_mode_.empty())
    {
        if (mock_mode_ == "nudb" && db_)
        {
            LOGI("Mock mode (nudb): closing NuDB database...");
            ::nudb::error_code ec;
            db_->close(ec);

            if (ec)
            {
                LOGE("Failed to close NuDB: ", ec.message());
                return false;
            }

            LOGI("✅ NuDB closed successfully");
            return true;
        }
        else if (mock_mode_ == "disk" && mock_disk_file_)
        {
            LOGI("Mock mode (disk): closing and flushing file...");
            mock_disk_file_->flush();
            mock_disk_file_->close();
            LOGI("✅ Mock disk file closed successfully");
            return true;
        }
        else
        {
            LOGI("Mock mode (", mock_mode_, "): skipping database close");
            return true;
        }
    }

    // Close bulk writer (this runs rekey to build the index!)
    if (bulk_writer_)
    {
        LOGI("Closing bulk writer (will run rekey to build index)...");

        // This flushes .dat file and runs rekey to build .key file
        if (!bulk_writer_->close(
                1024ULL * 1024 * 1024))  // 1GB buffer for rekey
        {
            LOGE("FATAL: Bulk writer close/rekey failed!");
            bulk_writer_.reset();
            return false;
        }

        LOGI("✅ Bulk import complete (index built successfully)");
        bulk_writer_.reset();
        return true;
    }

    // Close regular database (for verification reopens)
    if (db_)
    {
        // Close database - this flushes the final in-memory pool to disk
        LOGI("Closing NuDB database...");
        ::nudb::error_code ec;
        db_->close(ec);

        if (ec)
        {
            LOGE("FATAL: Error closing NuDB database: ", ec.message());
            db_.reset();
            return false;
        }

        LOGI("✅ Closed NuDB database successfully");
        db_.reset();
        return true;
    }

    // Database was never opened
    return true;
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

// Write a node to NuDB
bool
CatlNudbPipeline::flush_node(
    const Hash256& key,
    const uint8_t* data,
    size_t size,
    uint8_t node_type)
{
    // In mock mode or bulk writer mode, db_ won't be initialized - this is
    // expected
    if (mock_mode_.empty() && !bulk_writer_ && !db_)
    {
        LOGE("Cannot flush - database not open");
        return false;
    }

    static size_t total_attempts = 0;
    static size_t total_inserts = 0;
    static size_t duplicates = 0;

    total_attempts++;

    // Map simple node_type to nodestore::node_type
    // 0 = inner node (hot_unknown)
    // 1 = leaf node (hot_account_node for now - TODO: distinguish account vs
    // tx)
    nodestore::node_type ns_type = (node_type == 0)
        ? nodestore::node_type::hot_unknown
        : nodestore::node_type::hot_account_node;

    // Compress the serialized data using nodestore codec
    std::span<const uint8_t> data_span(data, size);
    nodestore::node_blob compressed_blob =
        nodestore::nodeobject_compress(ns_type, data_span);

    // The compressed blob includes 9-byte header + compressed payload
    const uint8_t* compressed_data = compressed_blob.data.data();
    size_t compressed_size = compressed_blob.data.size();

    // Track bytes for stats
    total_bytes_written_ += compressed_size;  // Compressed
    total_bytes_uncompressed_ += size;        // Uncompressed (input size)

    // Track node counts
    if (node_type == 0)
        total_inner_nodes_++;
    else
        total_leaf_nodes_++;

    bool inserted = false;

    // Handle different modes
    if (mock_mode_.empty())
    {
        // Real NuDB mode - use bulk writer for optimal performance
        if (bulk_writer_)
        {
            inserted = bulk_writer_->insert(
                key, compressed_data, compressed_size, node_type);
            if (inserted)
            {
                total_inserts++;
            }
            else
            {
                duplicates++;
            }

            // Log progress every 10000 successful inserts
            if (total_inserts % 10000 == 0)
            {
                LOGD(
                    "Bulk wrote ",
                    total_inserts,
                    " nodes (",
                    total_bytes_written_.load() / 1024,
                    " KB, ",
                    duplicates,
                    " dups, ",
                    (duplicates * 100 / total_attempts),
                    "%)");
            }
        }
        else
        {
            LOGE("Bulk writer not initialized!");
            throw std::runtime_error("Bulk writer not initialized");
        }
    }
    else if (mock_mode_ == "nudb")
    {
        // Mock NuDB mode - use regular NuDB inserts (not bulk writer)
        if (db_)
        {
            ::nudb::error_code ec;
            db_->insert(key.data(), compressed_data, compressed_size, ec);

            if (!ec)
            {
                total_inserts++;
                inserted = true;
            }
            else if (ec == ::nudb::error::key_exists)
            {
                duplicates++;
                inserted = false;
            }
            else
            {
                LOGE("NuDB insert failed: ", ec.message());
                throw std::runtime_error("NuDB insert failed: " + ec.message());
            }

            // Log progress
            if (total_inserts % 10000 == 0)
            {
                LOGD(
                    "NuDB wrote ",
                    total_inserts,
                    " nodes (",
                    total_bytes_written_.load() / 1024,
                    " KB, ",
                    duplicates,
                    " dups)");
            }
        }
        else
        {
            LOGE("NuDB database not initialized!");
            throw std::runtime_error("NuDB database not initialized");
        }
    }
    else if (mock_mode_ == "disk")
    {
        // Mock disk mode - write key (32 bytes) + size (4 bytes) + compressed
        // data to file (no dedup tracking - handled by dedup marker thread)
        if (mock_disk_file_ && mock_disk_file_->is_open())
        {
            // Write key (32 bytes)
            mock_disk_file_->write(
                reinterpret_cast<const char*>(key.data()), 32);

            // Write compressed size (4 bytes, little-endian)
            uint32_t size32 = static_cast<uint32_t>(compressed_size);
            mock_disk_file_->write(
                reinterpret_cast<const char*>(&size32), sizeof(size32));

            // Write compressed data
            mock_disk_file_->write(
                reinterpret_cast<const char*>(compressed_data),
                compressed_size);

            // Check for write errors
            if (!mock_disk_file_->good())
            {
                LOGE("Failed to write to mock disk file");
                throw std::runtime_error("Mock disk write failed");
            }

            total_inserts++;
            inserted = true;
        }
    }
    else
    {
        // noop/memory mode - no tracking needed
        total_inserts++;
        inserted = true;
    }

    return inserted;
}

// ===== Compression Pipeline Implementation =====

void
CatlNudbPipeline::start_compression_pipeline()
{
    if (!compression_workers_.empty())
    {
        LOGW("Compression pipeline already started");
        return;
    }

    shutdown_.store(false);

    // Start hasher thread
    LOGI("Starting hasher thread");
    hasher_thread_ = std::thread([this]() { hasher_worker(); });

    // Start compression workers
    LOGI("Starting ", compression_threads_, " compression worker threads");
    for (int i = 0; i < compression_threads_; ++i)
    {
        compression_workers_.emplace_back([this]() { compression_worker(); });
    }

    // Start dedupe worker (if parallel mode enabled)
    if (use_dedupe_thread_)
    {
        LOGI("Starting parallel dedupe worker thread");
        dedupe_worker_ = std::thread([this]() { dedupe_worker(); });
    }

    // Start writer thread
    LOGI("Starting writer thread");
    writer_thread_ = std::thread([this]() { writer_worker(); });
}

void
CatlNudbPipeline::stop_compression_pipeline()
{
    // Check if already stopped (idempotent)
    if (pipeline_stopped_.load())
    {
        LOGI("Pipeline already stopped, skipping");
        return;
    }

    LOGI("Stopping compression pipeline");

    // Wait for hasher queue to drain (stops new work entering the fork)
    size_t hasher_depth = hasher_queue_depth_.load();
    if (hasher_depth > 0)
    {
        LOGI(
            "Waiting for hasher queue to drain (",
            hasher_depth,
            " ledgers remaining)...");

        std::unique_lock<std::mutex> lock(hasher_queue_cv_mutex_);
        hasher_queue_cv_.wait(
            lock, [this]() { return hasher_queue_depth_.load() == 0; });

        LOGI("Hasher queue drained");
    }

    // Wait for BOTH parallel branches to complete:
    // Pipeline draining order:
    // 1. Compression: nodes → compressed batches
    // 2. Writer: compressed batches → NuDB

    // Wait for compression queue to drain
    {
        std::unique_lock<std::mutex> lock(compression_queue_mutex_);
        size_t queue_depth = compression_queue_.size();

        if (queue_depth > 0)
        {
            LOGI(
                "Waiting for compression queue to drain (",
                queue_depth,
                " ledgers remaining)...");

            compression_queue_cv_.wait(
                lock, [this]() { return compression_queue_.empty(); });

            LOGI("Compression queue drained");
        }
    }

    // Wait for write queue to drain
    if (mock_mode_.empty() || mock_mode_ == "disk")
    {
        size_t write_depth = write_queue_nodes_.load();

        if (write_depth > 0)
        {
            LOGI(
                "Waiting for write queue to drain (",
                write_depth,
                " nodes in batches remaining)...");

            std::unique_lock<std::mutex> lock(write_queue_cv_mutex_);
            write_queue_cv_.wait(
                lock, [this]() { return write_queue_nodes_.load() == 0; });

            LOGI("Write queue drained");
        }
    }

    // NOW signal shutdown
    shutdown_.store(true);

    // Wake up all threads so they see the shutdown signal
    hasher_queue_cv_.notify_all();
    compression_queue_cv_.notify_all();
    write_queue_cv_.notify_all();
    dedupe_queue_cv_.notify_all();     // Wake up dedupe worker
    writer_assembly_cv_.notify_all();  // Wake up writer (assembly mode)

    // Wait for hasher thread
    if (hasher_thread_.joinable())
    {
        hasher_thread_.join();
    }

    // Wait for compression workers
    for (auto& worker : compression_workers_)
    {
        if (worker.joinable())
        {
            worker.join();
        }
    }
    compression_workers_.clear();

    // Wait for dedupe worker (if running)
    if (dedupe_worker_.joinable())
    {
        dedupe_worker_.join();
    }

    // Wait for writer thread
    if (writer_thread_.joinable())
    {
        writer_thread_.join();
    }

    // Mark pipeline as stopped
    pipeline_stopped_.store(true);

    LOGI("Compression pipeline stopped");
}

void
CatlNudbPipeline::hasher_worker()
{
    while (!shutdown_.load())
    {
        LedgerSnapshot snapshot;

        // Pull snapshot from hasher queue (lock-free pop)
        bool got_snapshot = false;
        while (!got_snapshot)
        {
            // Try to pop from lock-free queue
            if (hasher_queue_.pop(snapshot))
            {
                hasher_queue_depth_--;
                got_snapshot = true;
                break;
            }

            // Queue empty, wait for notification
            if (shutdown_.load())
            {
                break;
            }

            std::unique_lock<std::mutex> lock(hasher_queue_cv_mutex_);
            hasher_queue_cv_.wait_for(lock, std::chrono::milliseconds(100), [this]() {
                return hasher_queue_depth_.load() > 0 || shutdown_.load();
            });
        }

        if (shutdown_.load() && !got_snapshot)
        {
            break;
        }

        if (!got_snapshot)
        {
            continue;
        }

        // Notify that we made space (wake up producer if waiting)
        {
            std::lock_guard<std::mutex> lock(hasher_queue_cv_mutex_);
        }
        hasher_queue_cv_.notify_all();

        // Hash the ledger (may use multiple threads if hasher_threads_ > 1)
        LOGD("Hashing ledger ", snapshot.info.seq);
        HashedLedger hashed = hash_and_verify(std::move(snapshot));

        // FORK: If parallel dedupe mode, extract hashes and send to dedupe
        // worker
        if (use_dedupe_thread_)
        {
            DedupeWork dedupe_work;
            dedupe_work.ledger_seq = hashed.info.seq;

            // Reusable lambda to extract hash from any node
            auto extract_hash =
                [&](const boost::intrusive_ptr<catl::shamap::SHAMapTreeNode>&
                        node) {
                    dedupe_work.hashes.push_back(node->get_hash(map_options_));
                    return true;  // Continue walking
                };

            // Extract hashes from state_snapshot
            if (hashed.state_snapshot)
            {
                hashed.state_snapshot->walk_new_nodes(
                    extract_hash, hashed.processing_version);
            }

            // Extract hashes from tx_map
            if (hashed.tx_map)
            {
                hashed.tx_map->walk_new_nodes(extract_hash);
            }

            // Enqueue dedupe work with backpressure (memory-safe: just hashes,
            // not trees)
            const size_t MAX_DEDUPE_QUEUE = 500;  // Same as compression

            // Apply backpressure if dedupe queue is too deep
            if (dedupe_queue_depth_.load() > MAX_DEDUPE_QUEUE)
            {
                static std::atomic<size_t> dedupe_backpressure_count{0};
                dedupe_backpressure_count++;

                // Only log every 100 times to avoid spam
                if (dedupe_backpressure_count % 100 == 1)
                {
                    LOGW(
                        "Dedupe queue deep (",
                        dedupe_queue_depth_.load(),
                        "), waiting for dedupe worker... (logged ",
                        dedupe_backpressure_count.load(),
                        " times)");
                }

                // Wait until queue has space
                std::unique_lock<std::mutex> lock(dedupe_queue_cv_mutex_);
                dedupe_queue_cv_.wait(lock, [this]() {
                    return dedupe_queue_depth_.load() <=
                        250;  // MAX_DEDUPE_QUEUE / 2
                });

                // Only log resolution occasionally
                if (dedupe_backpressure_count % 100 == 1)
                {
                    LOGI("Dedupe queue drained, continuing");
                }
            }

            // Lock-free push (spin if full)
            while (!dedupe_queue_.push(dedupe_work))
            {
                std::this_thread::yield();
            }
            dedupe_queue_depth_++;

            // Wake up dedupe worker
            {
                std::lock_guard<std::mutex> lock(dedupe_queue_cv_mutex_);
            }
            dedupe_queue_cv_.notify_one();
        }

        // Enqueue to compression queue with backpressure
        {
            std::unique_lock<std::mutex> lock(compression_queue_mutex_);

            const size_t MAX_COMPRESSION_QUEUE = 500;  // Backpressure threshold

            // Apply backpressure if queue is too deep
            if (compression_queue_.size() > MAX_COMPRESSION_QUEUE)
            {
                static std::atomic<size_t> compression_backpressure_count{0};
                compression_backpressure_count++;

                // Only log every 100 times to avoid spam
                if (compression_backpressure_count % 100 == 1)
                {
                    LOGW(
                        "Compression queue deep (",
                        compression_queue_.size(),
                        "), waiting for space... (logged ",
                        compression_backpressure_count.load(),
                        " times)");
                }

                // Wait until queue has space (uses condition variable
                // efficiently)
                compression_queue_cv_.wait(lock, [this]() {
                    return compression_queue_.size() <=
                        250;  // MAX_COMPRESSION_QUEUE / 2
                });

                // Only log resolution occasionally
                if (compression_backpressure_count % 100 == 1)
                {
                    LOGI("Compression queue drained, continuing");
                }
            }

            compression_queue_.push(std::move(hashed));
            compression_queue_depth_++;
        }
        compression_queue_cv_.notify_all();
    }
}

void
CatlNudbPipeline::compression_worker()
{
    while (!shutdown_.load())
    {
        HashedLedger job;

        // Pull job from priority queue
        {
            std::unique_lock<std::mutex> lock(compression_queue_mutex_);
            compression_queue_cv_.wait(lock, [this]() {
                return !compression_queue_.empty() || shutdown_.load();
            });

            if (shutdown_.load() && compression_queue_.empty())
            {
                break;
            }

            if (compression_queue_.empty())
            {
                continue;
            }

            job =
                std::move(const_cast<HashedLedger&>(compression_queue_.top()));
            compression_queue_.pop();
            compression_queue_depth_--;
        }

        // Notify that we made space in the queue (for backpressure)
        compression_queue_cv_.notify_all();

        // Process the entire ledger - accumulate all nodes into a batch
        LOGD("Compressing ledger ", job.info.seq);

        // Accumulate all compressed nodes for this ledger (no locking during
        // walk)
        std::vector<CompressedNode> batch;

        // Compress and collect state_snapshot nodes
        if (job.state_snapshot)
        {
            job.state_snapshot->walk_new_nodes(
                [&](const boost::intrusive_ptr<catl::shamap::SHAMapTreeNode>&
                        node) {
                    if (node->is_inner())
                    {
                        auto inner = boost::static_pointer_cast<
                            catl::shamap::SHAMapInnerNode>(node);

                        // Use concept-based compression for inner nodes
                        auto blob = nodestore::nodeobject_compress(*inner);

                        batch.push_back(CompressedNode{
                            job.info.seq,
                            inner->get_node_source_hash(),
                            std::move(blob.data),
                            512,  // Inner nodes are always 512 bytes (16 * 32)
                            0     // inner = 0
                        });
                    }
                    else
                    {
                        auto leaf = boost::static_pointer_cast<
                            catl::shamap::SHAMapLeafNode>(node);

                        // Serialize leaf data
                        size_t size = leaf->serialized_size();
                        std::vector<uint8_t> data(size);
                        leaf->write_to_buffer(data.data());

                        // Compress using LZ4
                        std::span<const uint8_t> data_span(data.data(), size);
                        auto blob = nodestore::nodeobject_compress(
                            nodestore::node_type::hot_account_node, data_span);

                        batch.push_back(CompressedNode{
                            job.info.seq,
                            leaf->get_hash(map_options_),
                            std::move(blob.data),
                            size,  // Uncompressed leaf data size
                            1      // leaf = 1
                        });
                    }

                    return true;  // Continue walking
                },
                job.processing_version);
        }

        // Compress and collect tx_map nodes (if present)
        if (job.tx_map)
        {
            job.tx_map->walk_new_nodes(
                [&](const boost::intrusive_ptr<catl::shamap::SHAMapTreeNode>&
                        node) {
                    if (node->is_inner())
                    {
                        auto inner = boost::static_pointer_cast<
                            catl::shamap::SHAMapInnerNode>(node);

                        auto blob = nodestore::nodeobject_compress(*inner);

                        batch.push_back(CompressedNode{
                            job.info.seq,
                            inner->get_node_source_hash(),
                            std::move(blob.data),
                            512,  // Inner nodes are always 512 bytes
                            0});
                    }
                    else
                    {
                        auto leaf = boost::static_pointer_cast<
                            catl::shamap::SHAMapLeafNode>(node);

                        size_t size = leaf->serialized_size();
                        std::vector<uint8_t> data(size);
                        leaf->write_to_buffer(data.data());

                        std::span<const uint8_t> data_span(data.data(), size);
                        auto blob = nodestore::nodeobject_compress(
                            nodestore::node_type::hot_transaction_node,
                            data_span);

                        batch.push_back(CompressedNode{
                            job.info.seq,
                            leaf->get_hash(map_options_),
                            std::move(blob.data),
                            size,  // Uncompressed leaf data size
                            1});
                    }

                    return true;  // Continue walking
                });
        }

        LOGD(
            "Finished compressing ledger ",
            job.info.seq,
            " - batch of ",
            batch.size(),
            " nodes");

        // Deliver results based on threading mode
        if (use_dedupe_thread_)
        {
            // PARALLEL MODE: Deliver to assembly station
            // NOTE: Dedupe worker is processing the SAME ledger in parallel!
            {
                std::lock_guard<std::mutex> lock(writer_assembly_mutex_);
                auto [it, inserted] =
                    writer_assembly_map_.try_emplace(job.info.seq);
                it->second.compressed_batch = std::move(batch);
                it->second.compression_done = true;
                if (inserted)
                {
                    assembly_station_depth_++;
                }
            }
            writer_assembly_cv_.notify_one();  // Wake up writer
        }
        else
        {
            // SEQUENTIAL MODE: Enqueue to write queue (current behavior)
            enqueue_compressed_batch(std::move(batch));
        }

        // job goes out of scope → maps destruct → nodes cleanup
    }
}

void
CatlNudbPipeline::writer_worker()
{
    while (!shutdown_.load())
    {
        WriterJob current_job;

        if (use_dedupe_thread_)
        {
            // PARALLEL MODE: Wait for assembly station
            // Wait for BOTH compression AND dedupe to complete for the next
            // ledger
            {
                std::unique_lock<std::mutex> lock(writer_assembly_mutex_);
                writer_assembly_cv_.wait(lock, [this]() {
                    if (shutdown_.load())
                        return true;
                    auto it = writer_assembly_map_.find(next_ledger_to_write_);
                    // Wait if job not found, or if it's not fully done
                    return it != writer_assembly_map_.end() &&
                        it->second.compression_done && it->second.dedupe_done;
                });

                if (shutdown_.load())
                    break;

                // We have the complete job for the next ledger, pull it
                current_job =
                    std::move(writer_assembly_map_[next_ledger_to_write_]);
                writer_assembly_map_.erase(next_ledger_to_write_);
                assembly_station_depth_--;
                next_ledger_to_write_++;
            }  // Lock released

            // Process the job (NO RocksDB I/O - dedupe already done!)
            for (auto& node : current_job.compressed_batch)
            {
                // Track bytes for stats
                total_bytes_written_ += node.blob.size();
                total_bytes_uncompressed_ += node.uncompressed_size;

                // Track node counts
                if (node.node_type == 0)
                    total_inner_nodes_++;
                else
                    total_leaf_nodes_++;

                // Fast in-memory check against duplicate set
                if (current_job.duplicate_set.find(node.hash) ==
                    current_job.duplicate_set.end())
                {
                    // Not a duplicate, write it
                    // bulk_writer_ is in "NoDedupe" mode, so this is just a raw
                    // write
                    if (bulk_writer_)
                    {
                        bulk_writer_->insert(
                            node.hash,
                            node.blob.data(),
                            node.blob.size(),
                            node.node_type);
                    }
                }
            }
        }
        else
        {
            // SEQUENTIAL MODE: Use write queue (current behavior)
            std::vector<CompressedNode>* batch_ptr = nullptr;

            // Pull ONE batch from queue (lock-free pop)
            bool got_batch = false;
            while (!got_batch)
            {
                // Try to pop from lock-free queue
                if (write_queue_.pop(batch_ptr))
                {
                    got_batch = true;
                    break;
                }

                // Queue empty, wait for notification
                if (shutdown_.load())
                {
                    break;
                }

                std::unique_lock<std::mutex> lock(write_queue_cv_mutex_);
                write_queue_cv_.wait_for(
                    lock, std::chrono::milliseconds(100), [this]() {
                        return write_queue_nodes_.load() > 0 || shutdown_.load();
                    });
            }

            if (shutdown_.load() && !got_batch)
            {
                break;
            }

            if (!got_batch)
            {
                continue;
            }

            // Decrement counters for entire batch
            uint64_t batch_bytes = 0;
            for (const auto& node : *batch_ptr)
            {
                batch_bytes += node.blob.size();
            }
            write_queue_bytes_ -= batch_bytes;
            write_queue_nodes_ -= batch_ptr->size();

            // Notify that we made space
            {
                std::lock_guard<std::mutex> lock(write_queue_cv_mutex_);
            }
            write_queue_cv_.notify_all();

            // Process batch WITHOUT holding lock
            for (auto& node : *batch_ptr)
            {
                // Track bytes for stats
                total_bytes_written_ += node.blob.size();  // Compressed
                total_bytes_uncompressed_ +=
                    node.uncompressed_size;  // Uncompressed

                // Track node counts
                if (node.node_type == 0)
                    total_inner_nodes_++;
                else
                    total_leaf_nodes_++;

                // Handle different modes
                if (mock_mode_.empty())
                {
                    // Real NuDB mode
                    if (bulk_writer_)
                    {
                        bulk_writer_->insert(
                            node.hash,
                            node.blob.data(),
                            node.blob.size(),
                            node.node_type);
                    }
                }
                else if (mock_mode_ == "nudb" && db_)
                {
                    // Mock NuDB mode - use regular NuDB inserts (no dedup
                    // tracking)
                    ::nudb::error_code ec;
                    db_->insert(
                        node.hash.data(),
                        node.blob.data(),
                        node.blob.size(),
                        ec);

                    // Only error if it's not a duplicate
                    if (ec && ec != ::nudb::error::key_exists)
                    {
                        LOGE("NuDB insert failed: ", ec.message());
                        throw std::runtime_error(
                            "NuDB insert failed: " + ec.message());
                    }
                }
                else if (
                    mock_mode_ == "disk" && mock_disk_file_ &&
                    mock_disk_file_->is_open())
                {
                    // Mock disk mode - just write
                    mock_disk_file_->write(
                        reinterpret_cast<const char*>(node.hash.data()), 32);
                    uint32_t size32 = static_cast<uint32_t>(node.blob.size());
                    mock_disk_file_->write(
                        reinterpret_cast<const char*>(&size32), sizeof(size32));
                    mock_disk_file_->write(
                        reinterpret_cast<const char*>(node.blob.data()),
                        node.blob.size());

                    if (!mock_disk_file_->good())
                    {
                        LOGE("Failed to write to mock disk file");
                        throw std::runtime_error("Mock disk write failed");
                    }
                }
                // else: noop/memory mode - no tracking needed
            }

            // Flush deduplication strategy batch after processing each ledger
            // This commits all the RocksDB Puts from this ledger in one write
            if (bulk_writer_)
            {
                bulk_writer_->flush_dedupe_batch();
            }

            // Delete the batch (allocated in enqueue_compressed_batch)
            delete batch_ptr;
        }  // End sequential mode
    }      // End while loop
}

void
CatlNudbPipeline::dedupe_worker()
{
    LOGI("Dedupe worker thread started");

    uint64_t ledgers_processed = 0;
    uint64_t total_hashes_checked = 0;

    while (!shutdown_.load())
    {
        DedupeWork work;

        // 1. Pull a DedupeWork job from dedupe_queue_ (lock-free pop)
        bool got_work = false;
        while (!got_work)
        {
            // Try to pop from lock-free queue
            if (dedupe_queue_.pop(work))
            {
                dedupe_queue_depth_--;
                got_work = true;
                break;
            }

            // Queue empty, wait for notification
            if (shutdown_.load())
            {
                break;
            }

            std::unique_lock<std::mutex> lock(dedupe_queue_cv_mutex_);
            dedupe_queue_cv_.wait_for(lock, std::chrono::milliseconds(100), [this]() {
                return dedupe_queue_depth_.load() > 0 || shutdown_.load();
            });
        }

        if (shutdown_.load() && !got_work)
        {
            break;
        }

        if (!got_work)
        {
            continue;
        }

        // Notify hasher that we made space (for backpressure)
        {
            std::lock_guard<std::mutex> lock(dedupe_queue_cv_mutex_);
        }
        dedupe_queue_cv_.notify_all();

        // 2. Prepare duplicate set for this ledger
        std::unordered_set<Hash256, Hash256Hasher> duplicates_for_this_ledger;

        // 3. Run the "brain" - check each hash
        size_t hashes_in_this_ledger = work.hashes.size();
        for (const auto& hash : work.hashes)
        {
            // Use the pipeline's strategy (CuckooRocks, etc.)
            // This calls WriteBatch.Put() internally for new keys
            // Note: We use dummy values for size and node_type since they're
            // not used by the strategy
            bool is_duplicate =
                pipeline_dedup_strategy_->check_and_mark(hash, 0, 0);

            if (is_duplicate)
            {
                duplicates_for_this_ledger.insert(hash);
            }
        }

        // 4. Commit the batch - ONE big I/O write for this ledger
        // This calls db->Write(), committing all RocksDB Puts at once
        pipeline_dedup_strategy_->flush_batch();

        // 5. Update stats
        ledgers_processed++;
        total_hashes_checked += hashes_in_this_ledger;

        // 6. Periodic logging (commented out - use assembly station depth
        // instead) if (ledgers_processed % 1000 == 0)
        // {
        //     uint64_t dup_count =
        //     pipeline_dedup_strategy_->get_duplicate_count(); double dup_rate
        //     = total_hashes_checked > 0
        //         ? (dup_count * 100.0 / total_hashes_checked)
        //         : 0.0;
        //     LOGI(
        //         "📊 Dedupe worker: ",
        //         ledgers_processed,
        //         " ledgers, ",
        //         total_hashes_checked,
        //         " hashes checked, ",
        //         dup_count,
        //         " duplicates (",
        //         std::fixed,
        //         std::setprecision(2),
        //         dup_rate,
        //         "%)");
        // }

        // 7. Deliver the result to the "assembly station"
        {
            std::lock_guard<std::mutex> lock(writer_assembly_mutex_);
            auto [it, inserted] =
                writer_assembly_map_.try_emplace(work.ledger_seq);
            it->second.duplicate_set = std::move(duplicates_for_this_ledger);
            it->second.dedupe_done = true;
            if (inserted)
            {
                assembly_station_depth_++;
            }
            writer_assembly_cv_.notify_one();  // Wake up the writer
        }
    }

    LOGI("Dedupe worker thread stopped");
    LOGI(
        "  Final: ",
        ledgers_processed,
        " ledgers processed, ",
        total_hashes_checked,
        " hashes checked");
}

void
CatlNudbPipeline::enqueue_compressed_batch(std::vector<CompressedNode>&& batch)
{
    if (batch.empty())
    {
        return;  // Nothing to enqueue
    }

    // Calculate total bytes in batch
    uint64_t batch_bytes = 0;
    for (const auto& node : batch)
    {
        batch_bytes += node.blob.size();
    }

    // Apply backpressure if write queue exceeds byte limit
    uint64_t queue_bytes = write_queue_bytes_.load();

    if (queue_bytes > max_write_queue_bytes_)
    {
        LOGW(
            "Write queue full (",
            queue_bytes / 1024 / 1024,
            " MB / ",
            max_write_queue_bytes_ / 1024 / 1024,
            " MB), waiting for writer...");

        std::unique_lock<std::mutex> lock(write_queue_cv_mutex_);
        write_queue_cv_.wait(lock, [this]() {
            return write_queue_bytes_.load() <= max_write_queue_bytes_ / 2;
        });

        LOGI(
            "Write queue drained (",
            write_queue_bytes_.load() / 1024 / 1024,
            " MB), compression can continue");
    }

    // Push entire batch as one item (lock-free queue uses pointers)
    size_t batch_size = batch.size();
    auto* batch_ptr = new std::vector<CompressedNode>(std::move(batch));

    // Lock-free push (spin if full)
    while (!write_queue_.push(batch_ptr))
    {
        std::this_thread::yield();
    }

    write_queue_bytes_ += batch_bytes;
    write_queue_nodes_ += batch_size;

    // Wake up writer
    {
        std::lock_guard<std::mutex> lock(write_queue_cv_mutex_);
    }
    write_queue_cv_.notify_one();
}

void
CatlNudbPipeline::enqueue_to_hasher(LedgerSnapshot snapshot)
{
    // Initialize next_ledger_to_write_ with the first ledger (for parallel
    // mode)
    if (use_dedupe_thread_ && next_ledger_to_write_ == 0)
    {
        next_ledger_to_write_ = snapshot.info.seq;
    }

    const size_t MAX_HASHER_QUEUE =
        500;  // Backpressure threshold (same as compression)
    static std::atomic<size_t> backpressure_count{0};

    // Apply backpressure if queue is too deep (use atomic counter)
    if (hasher_queue_depth_.load() > MAX_HASHER_QUEUE)
    {
        backpressure_count++;

        // Only log every 100 times to avoid spam
        if (backpressure_count % 100 == 1)
        {
            LOGW(
                "Hasher queue deep (",
                hasher_queue_depth_.load(),
                "), waiting for space... (logged ",
                backpressure_count.load(),
                " times)");
        }

        // Wait until queue has space (using cv)
        std::unique_lock<std::mutex> lock(hasher_queue_cv_mutex_);
        hasher_queue_cv_.wait(lock, [this]() {
            return hasher_queue_depth_.load() <= MAX_HASHER_QUEUE / 2;
        });

        // Only log resolution occasionally
        if (backpressure_count % 100 == 1)
        {
            LOGI("Hasher queue drained, continuing");
        }
    }

    // Lock-free push (spin if full, which should be rare with backpressure)
    while (!hasher_queue_.push(snapshot))
    {
        // Queue full, yield and retry
        std::this_thread::yield();
    }
    hasher_queue_depth_++;

    // Wake up hasher thread
    {
        std::lock_guard<std::mutex> lock(hasher_queue_cv_mutex_);
    }
    hasher_queue_cv_.notify_one();
}

bool
CatlNudbPipeline::flush_to_nudb(HashedLedger hashed)
{
    if (!hashed.verified)
    {
        LOGE("Cannot flush unverified ledger ", hashed.info.seq);
        return false;
    }

    // If compression pipeline is running, queue the job
    if (!compression_workers_.empty())
    {
        const size_t MAX_COMPRESSION_QUEUE = 500;  // Backpressure threshold

        LOGD("Queueing ledger ", hashed.info.seq, " for compression");

        {
            std::unique_lock<std::mutex> lock(compression_queue_mutex_);

            // Apply backpressure if queue is too deep
            if (compression_queue_.size() > MAX_COMPRESSION_QUEUE)
            {
                LOGW(
                    "Compression queue deep (",
                    compression_queue_.size(),
                    "), waiting for space...");

                // Wait until queue has space (uses condition variable
                // efficiently)
                compression_queue_cv_.wait(lock, [this]() {
                    return compression_queue_.size() <=
                        250;  // MAX_COMPRESSION_QUEUE / 2
                });

                LOGI("Compression queue drained, continuing");
            }

            compression_queue_.push(std::move(hashed));
        }
        compression_queue_cv_.notify_one();

        return true;
    }

    // Synchronous path disabled - should never reach here!
    LOGE("FATAL: Compression pipeline not running! Ledger ", hashed.info.seq);
    throw std::runtime_error(
        "Synchronous flush path disabled - pipeline not started");

    // Otherwise fall back to synchronous processing (for backwards
    // compatibility)
    LOGD("Flushing ledger ", hashed.info.seq, " to NuDB (synchronous)");

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
    size_t empty_inner_count = 0;

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
                    // Inner node: 512 bytes or 0 if empty (no children)
                    size_t size = node->serialized_size();
                    if (size == 0)
                    {
                        // Empty inner node - skip it (all empty nodes hash to
                        // zero)
                        empty_inner_count++;
                        return true;
                    }

                    std::array<uint8_t, 512> buffer;
                    size_t written = node->write_to_buffer(buffer.data());
                    state_inner_count++;
                    flush_node(
                        node->get_hash(map_options_),
                        buffer.data(),
                        written,
                        0);  // 0 = inner node
                }
                else if (node->is_leaf())
                {
                    // Leaf node: variable size based on item data
                    size_t size = node->serialized_size();
                    if (size == 0)
                    {
                        LOGE(
                            "    WARNING: Leaf #",
                            state_leaf_count + 1,
                            " has 0 serialized size!");
                        return true;  // Continue walking
                    }

                    // Allocate and serialize
                    std::vector<uint8_t> buffer(size);
                    size_t written = node->write_to_buffer(buffer.data());
                    state_leaf_count++;
                    bool inserted = flush_node(
                        node->get_hash(map_options_),
                        buffer.data(),
                        written,
                        1);  // 1 = leaf node

                    // If duplicate leaf, save JSON for debugging
                    if (!inserted)
                    {
                        static std::vector<std::string> dup_leaf_jsons;
                        static const size_t MAX_SAMPLES = 20;

                        if (dup_leaf_jsons.size() < MAX_SAMPLES)
                        {
                            auto leaf = boost::static_pointer_cast<
                                catl::shamap::SHAMapLeafNode>(node);
                            auto item = leaf->get_item();

                            // Parse to JSON using xdata::JsonVisitor
                            catl::xdata::JsonVisitor visitor(protocol_);
                            catl::xdata::ParserContext ctx(item->slice());
                            catl::xdata::parse_with_visitor(
                                ctx, protocol_, visitor);
                            std::string json_str =
                                boost::json::serialize(visitor.get_result());

                            dup_leaf_jsons.push_back(json_str);

                            // Log samples at the end
                            if (dup_leaf_jsons.size() == MAX_SAMPLES)
                            {
                                LOGI("");
                                LOGI(
                                    "📝 Sample duplicate STATE leaf JSONs "
                                    "(first ",
                                    MAX_SAMPLES,
                                    "):");
                                for (size_t i = 0; i < dup_leaf_jsons.size();
                                     i++)
                                {
                                    LOGI("  [", i + 1, "] ", dup_leaf_jsons[i]);
                                }
                            }
                        }
                    }

                    auto leaf = boost::static_pointer_cast<
                        catl::shamap::SHAMapLeafNode>(node);
                    auto item = leaf->get_item();

                    // Debug: Check if this leaf's key matches the debug key
                    // prefix
                    if (walk_nodes_debug_key_)
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
            " inner nodes, ",
            empty_inner_count,
            " empty (skipped)");

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
                // Inner node: 512 bytes or 0 if empty
                size_t size = node->serialized_size();
                if (size == 0)
                {
                    empty_inner_count++;
                }
                else
                {
                    std::array<uint8_t, 512> buffer;
                    size_t written = node->write_to_buffer(buffer.data());
                    tx_inner_count++;
                    flush_node(
                        node->get_hash(map_options_),
                        buffer.data(),
                        written,
                        0);  // 0 = inner node
                }
            }
            else if (node->is_leaf())
            {
                // Leaf node: variable size
                size_t size = node->serialized_size();
                if (size > 0)
                {
                    std::vector<uint8_t> buffer(size);
                    size_t written = node->write_to_buffer(buffer.data());
                    tx_leaf_count++;
                    bool inserted = flush_node(
                        node->get_hash(map_options_),
                        buffer.data(),
                        written,
                        1);  // 1 = leaf node

                    // If duplicate leaf, save JSON for debugging
                    if (!inserted)
                    {
                        static std::vector<std::string> dup_tx_leaf_jsons;
                        static const size_t MAX_SAMPLES = 20;

                        if (dup_tx_leaf_jsons.size() < MAX_SAMPLES)
                        {
                            auto leaf = boost::static_pointer_cast<
                                catl::shamap::SHAMapLeafNode>(node);
                            auto item = leaf->get_item();

                            // Parse to JSON using xdata::JsonVisitor
                            catl::xdata::JsonVisitor visitor(protocol_);
                            catl::xdata::ParserContext ctx(item->slice());
                            catl::xdata::parse_with_visitor(
                                ctx, protocol_, visitor);
                            std::string json_str =
                                boost::json::serialize(visitor.get_result());

                            dup_tx_leaf_jsons.push_back(json_str);

                            // Log samples at the end
                            if (dup_tx_leaf_jsons.size() == MAX_SAMPLES)
                            {
                                LOGI("");
                                LOGI(
                                    "📝 Sample duplicate TX leaf JSONs (first ",
                                    MAX_SAMPLES,
                                    "):");
                                for (size_t i = 0; i < dup_tx_leaf_jsons.size();
                                     i++)
                                {
                                    LOGI(
                                        "  [",
                                        i + 1,
                                        "] ",
                                        dup_tx_leaf_jsons[i]);
                                }
                            }
                        }
                    }
                }
            }
            return true;  // Continue walking
        });

        LOGD(
            "  Tx map flushed: ",
            tx_leaf_count,
            " leaves, ",
            tx_inner_count,
            " inner nodes (+ ",
            empty_inner_count,
            " empty total)");
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

uint64_t
CatlNudbPipeline::get_duplicate_count() const
{
    // If using parallel dedupe thread, return count from pipeline strategy
    if (use_dedupe_thread_ && pipeline_dedup_strategy_)
    {
        return pipeline_dedup_strategy_->get_duplicate_count();
    }

    // Otherwise, return count from bulk writer's strategy
    if (bulk_writer_)
    {
        return bulk_writer_->get_duplicate_count();
    }

    return 0;
}

void
CatlNudbPipeline::print_dedup_stats() const
{
    if (!pipeline_dedup_strategy_)
    {
        LOGI("📊 Deduplication stats: N/A (sequential mode or no dedup)");
        return;
    }

    // Print stats from the pipeline's dedup strategy
    // This will call CuckooRocksStrategy::print_stats() or similar
    uint64_t unique_written =
        total_inner_nodes_.load() + total_leaf_nodes_.load();
    pipeline_dedup_strategy_->print_stats(unique_written);
}

}  // namespace catl::v1::utils::nudb
