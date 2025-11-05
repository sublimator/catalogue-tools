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

    // Handle mock modes
    if (!mock_mode_.empty())
    {
        if (mock_mode_ == "noop" || mock_mode_ == "memory")
        {
            LOGI("Mock mode (", mock_mode_, "): skipping database creation");
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
            return true;
        }
    }

    // Real NuDB mode

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

    // Remove existing database files if they exist
    if (fs::exists(dat_path) || fs::exists(key_path) || fs::exists(log_path))
    {
        LOGI("Removing existing NuDB database files...");

        if (fs::exists(dat_path))
        {
            fs::remove(dat_path);
        }
        if (fs::exists(key_path))
        {
            fs::remove(key_path);
        }
        if (fs::exists(log_path))
        {
            fs::remove(log_path);
        }
    }

    // Create the database
    ::nudb::error_code ec;
    ::nudb::create<::nudb::xxhasher, ::nudb::posix_file>(
        dat_path.string(),
        key_path.string(),
        log_path.string(),
        1,           // appnum
        0xABADCAFE,  // salt (arbitrary value for now)
        key_size,
        block_size,
        load_factor,
        ec);

    if (ec)
    {
        LOGE("Failed to create NuDB database: ", ec.message());
        return false;
    }

    LOGI("Created NuDB database at: ", path);
    LOGI("  key_size: ", key_size, " bytes");
    LOGI("  block_size: ", block_size);
    LOGI("  load_factor: ", load_factor);

    // Open the database
    db_ = std::make_unique<
        ::nudb::basic_store<::nudb::xxhasher, ::nudb::posix_file>>();
    db_->open(dat_path.string(), key_path.string(), log_path.string(), ec);

    if (ec)
    {
        LOGE("Failed to open NuDB database: ", ec.message());
        db_.reset();
        return false;
    }

    LOGI("Opened NuDB database successfully");
    return true;
}

bool
CatlNudbPipeline::open_database(const std::string& path)
{
    // Skip database operations in mock mode
    if (!mock_mode_.empty())
    {
        LOGI("Mock mode (", mock_mode_, "): skipping database open");
        return true;
    }

    namespace fs = boost::filesystem;

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
    // Handle mock mode closing
    if (!mock_mode_.empty())
    {
        if (mock_mode_ == "disk" && mock_disk_file_)
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

    if (db_)
    {
        // Close database - this flushes the final in-memory pool to disk
        LOGI("Closing NuDB database (flushing final batch)...");
        ::nudb::error_code ec;
        db_->close(ec);

        if (ec)
        {
            LOGE("FATAL: Error closing NuDB database: ", ec.message());
            LOGE("Final batch may not have been flushed to disk!");
            db_.reset();
            return false;
        }

        LOGI("✅ Closed NuDB database successfully (final batch flushed)");
        db_.reset();
        return true;
    }

    // Database was never opened
    return true;
}

bool
CatlNudbPipeline::verify_all_keys(int num_threads)
{
    // Skip verification in mock mode
    if (!mock_mode_.empty())
    {
        LOGI("Mock mode (", mock_mode_, "): skipping key verification");
        return true;
    }

    if (!db_)
    {
        LOGE("Cannot verify - database not open");
        return false;
    }

    size_t total_keys = inserted_keys_with_sizes_.size();
    LOGI(
        "Verifying all ",
        total_keys,
        " inserted keys with ",
        num_threads,
        " threads...");

    // Convert unordered_map to vector of (key, size) pairs for easy
    // partitioning
    LOGI("Converting key map to vector for partitioning...");
    std::vector<std::pair<Hash256, size_t>> keys_vec;
    keys_vec.reserve(inserted_keys_with_sizes_.size());
    for (const auto& [key, size] : inserted_keys_with_sizes_)
    {
        keys_vec.emplace_back(key, size);
    }
    LOGI("Converted ", keys_vec.size(), " keys with sizes");

    // Atomic counters for thread-safe tracking
    std::atomic<size_t> verified_count{0};
    std::atomic<size_t> missing_count{0};
    std::atomic<size_t> size_mismatch_count{0};
    std::atomic<size_t> progress_count{0};
    std::atomic<uint64_t> total_bytes_verified{0};

    auto start_time = std::chrono::steady_clock::now();

    // Lambda for each thread to verify its chunk
    auto verify_chunk = [&](size_t start_idx, size_t end_idx, int thread_id) {
        size_t local_verified = 0;
        size_t local_missing = 0;
        size_t local_size_mismatch = 0;
        uint64_t local_bytes = 0;

        for (size_t i = start_idx; i < end_idx; ++i)
        {
            const Hash256& key = keys_vec[i].first;
            size_t expected_size = keys_vec[i].second;

            // Try to fetch this key from NuDB and check size
            ::nudb::error_code ec;
            size_t actual_size = 0;
            db_->fetch(
                key.data(),
                [&actual_size](void const* /*data*/, std::size_t size) {
                    actual_size = size;
                },
                ec);

            if (ec)
            {
                if (ec == ::nudb::error::key_not_found)
                {
                    LOGE(
                        "[Thread ",
                        thread_id,
                        "] Key NOT FOUND: ",
                        key.hex().substr(0, 16),
                        "...");
                }
                else
                {
                    LOGE(
                        "[Thread ",
                        thread_id,
                        "] Error fetching key ",
                        key.hex().substr(0, 16),
                        "...: ",
                        ec.message());
                }
                local_missing++;
            }
            else if (actual_size != expected_size)
            {
                LOGE(
                    "[Thread ",
                    thread_id,
                    "] SIZE MISMATCH for key ",
                    key.hex().substr(0, 16),
                    "... expected ",
                    expected_size,
                    " bytes, got ",
                    actual_size,
                    " bytes");
                local_size_mismatch++;
            }
            else
            {
                local_verified++;
                local_bytes += expected_size;
            }

            // Update progress every 50k keys per thread
            if ((local_verified + local_missing + local_size_mismatch) %
                    50000 ==
                0)
            {
                size_t current_progress =
                    progress_count.fetch_add(50000) + 50000;
                if (current_progress % 100000 == 0)
                {
                    LOGI(
                        "Progress: ",
                        current_progress,
                        " / ",
                        total_keys,
                        " keys verified");
                }
            }
        }

        // Add local counts to global atomics
        verified_count.fetch_add(local_verified);
        missing_count.fetch_add(local_missing);
        size_mismatch_count.fetch_add(local_size_mismatch);
        total_bytes_verified.fetch_add(local_bytes);
    };

    // Partition work across threads
    std::vector<std::thread> threads;
    size_t keys_per_thread = total_keys / num_threads;
    size_t remainder = total_keys % num_threads;

    LOGI("Launching ", num_threads, " verification threads...");
    LOGI("  ~", keys_per_thread, " keys per thread");

    for (int i = 0; i < num_threads; ++i)
    {
        size_t i_size = static_cast<size_t>(i);
        size_t start_idx =
            i_size * keys_per_thread + std::min(i_size, remainder);
        size_t end_idx =
            start_idx + keys_per_thread + (i_size < remainder ? 1 : 0);

        threads.emplace_back(verify_chunk, start_idx, end_idx, i);
    }

    // Wait for all threads to complete
    for (auto& thread : threads)
    {
        thread.join();
    }

    auto end_time = std::chrono::steady_clock::now();
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          end_time - start_time)
                          .count();
    double keys_per_sec = elapsed_ms > 0
        ? static_cast<double>(total_keys) * 1000.0 / elapsed_ms
        : 0;
    double bytes_per_sec = elapsed_ms > 0
        ? static_cast<double>(total_bytes_verified.load()) * 1000.0 / elapsed_ms
        : 0;

    LOGI("========================================");
    LOGI("Verification Complete:");
    LOGI("  - Keys verified: ", verified_count.load());
    LOGI("  - Keys missing: ", missing_count.load());
    LOGI("  - Size mismatches: ", size_mismatch_count.load());
    LOGI("  - Threads used: ", num_threads);
    LOGI(
        "  - Time: ",
        std::fixed,
        std::setprecision(3),
        elapsed_ms / 1000.0,
        " seconds");
    LOGI(
        "  - Speed: ",
        std::fixed,
        std::setprecision(2),
        keys_per_sec,
        " keys/sec, ",
        bytes_per_sec / 1024 / 1024,
        " MB/sec");
    LOGI("  - Total data: ", total_bytes_verified.load() / 1024 / 1024, " MB");
    LOGI("========================================");

    size_t total_errors = missing_count.load() + size_mismatch_count.load();
    if (total_errors > 0)
    {
        LOGE("⚠️  VERIFICATION FAILED - ", total_errors, " errors found!");
        if (missing_count.load() > 0)
        {
            LOGE("  - ", missing_count.load(), " keys missing");
        }
        if (size_mismatch_count.load() > 0)
        {
            LOGE("  - ", size_mismatch_count.load(), " size mismatches");
        }
        return false;
    }

    LOGI("✅ All keys verified successfully (existence + size)!");
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
void
CatlNudbPipeline::flush_node(
    const Hash256& key,
    const uint8_t* data,
    size_t size)
{
    // In mock mode, db_ won't be initialized - this is expected
    if (mock_mode_.empty() && !db_)
    {
        LOGE("Cannot flush - database not open");
        return;
    }

    static size_t total_attempts = 0;
    static size_t total_inserts = 0;
    static size_t duplicates = 0;

    total_attempts++;

    // Check if we've already inserted this key
    auto it = inserted_keys_with_sizes_.find(key);
    if (it != inserted_keys_with_sizes_.end())
    {
        // Already inserted - skip it
        duplicates++;
        return;
    }

    // Track for stats
    total_bytes_written_ += size;

    // Handle different modes
    if (mock_mode_.empty())
    {
        // Real NuDB mode - insert into database
        ::nudb::error_code ec;
        db_->insert(
            key.data(), data, size, ec, ::nudb::insert_flags::no_check_exists);

        if (ec)
        {
            LOGE(
                "Failed to insert node - key: ",
                key.hex().substr(0, 16),
                "... size: ",
                size,
                " error: ",
                ec.message());
            throw std::runtime_error("NuDB insert failed: " + ec.message());
        }
    }
    else if (mock_mode_ == "disk")
    {
        // Mock disk mode - write key (32 bytes) + size (4 bytes) + data to file
        if (mock_disk_file_ && mock_disk_file_->is_open())
        {
            // Write key (32 bytes)
            mock_disk_file_->write(
                reinterpret_cast<const char*>(key.data()), 32);

            // Write size (4 bytes, little-endian)
            uint32_t size32 = static_cast<uint32_t>(size);
            mock_disk_file_->write(
                reinterpret_cast<const char*>(&size32), sizeof(size32));

            // Write data
            mock_disk_file_->write(reinterpret_cast<const char*>(data), size);

            // Check for write errors
            if (!mock_disk_file_->good())
            {
                LOGE("Failed to write to mock disk file");
                throw std::runtime_error("Mock disk write failed");
            }
        }
    }
    // else: noop/memory mode - do nothing

    // Success - add key and size to our tracking map
    inserted_keys_with_sizes_[key] = size;
    total_inserts++;

    // Log progress every 10000 successful inserts
    if (total_inserts % 10000 == 0)
    {
        LOGD(
            "Flushed ",
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
                        node->get_hash(map_options_), buffer.data(), written);
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
                    flush_node(
                        node->get_hash(map_options_), buffer.data(), written);

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
                        node->get_hash(map_options_), buffer.data(), written);
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
                    flush_node(
                        node->get_hash(map_options_), buffer.data(), written);
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

}  // namespace catl::v1::utils::nudb
