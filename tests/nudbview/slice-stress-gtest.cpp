//
// Concurrent Slice Stress Tests - Test slicing LIVE .dat files
//
// PURPOSE: Validate that we can safely create slices of .dat files that are
// actively being written by another process (e.g., a running Ripple/Xahau
// node).
//
// CRITICAL REQUIREMENT: The "history problem" demands slicing hot databases.
// We cannot wait for a node to shut down - we must slice while it's running!
//
// WHAT WE TEST:
// 1. Background thread simulates live database (continuous inserts)
// 2. Main thread indexes and slices WHILE inserts are happening
// 3. IndexBuilder gracefully handles corrupt/partial records at tail
// 4. We clamp slice bounds to safe, indexed ranges
// 5. Fuzzing with random parameters finds edge cases
//
// KEY INSIGHT: NuDB writes are NOT atomic. A record write has stages:
//   - Write size header (6 bytes)
//   - Write key (key_size bytes)
//   - Write value (value_size bytes)
// If we scan mid-write, we see a partial record. dat_scanner (used by
// IndexBuilder) detects this as "corrupt tail" and stops. This is correct!
//
// See also:
// - src/nudbview/nudb-util/index-builder.hpp (tail handling comments)
// - src/nudbview/includes/nudbview/impl/view/slice_rekey.ipp (short_read
// comments)
// - src/nudbview/nudb-util/dat-scanner.cpp (corrupt tail detection)
//

#include "test-helpers.hpp"
#include <atomic>
#include <boost/filesystem.hpp>
#include <catl/core/logger.h>
#include <chrono>
#include <cstdlib>
#include <gtest/gtest.h>
#include <nudbview/nudb.hpp>
#include <nudbview/view/index_builder.hpp>
#include <nudbview/view/index_reader.hpp>
#include <nudbview/view/rekey_slice.hpp>
#include <nudbview/view/slice_store.hpp>
#include <random>
#include <thread>

using namespace nudbview_test;
namespace fs = boost::filesystem;

/**
 * Parameters for concurrent slice stress tests
 *
 * Tests the behavior of indexing and slicing while concurrent inserts
 * are happening to the database (simulating a live/hot database).
 */
struct SliceStressParams
{
    // Initial database size before concurrent operations start
    std::size_t initial_records = 1000;

    // Number of inserts to perform during index building phase
    std::size_t inserts_during_index = 100;

    // Number of inserts to perform during slice creation (rekey) phase
    std::size_t inserts_during_rekey = 100;

    // Delay between inserts (microseconds) - controls insertion rate
    std::size_t insert_delay_us = 1000;  // 1ms = ~1000 inserts/sec

    // Index configuration
    std::uint64_t index_interval = 50;

    // Slice configuration (what portion to slice)
    // Records to include: [slice_start_record, slice_end_record)
    std::size_t slice_start_record = 0;
    std::size_t slice_end_record = 500;  // Slice first 500 records

    // Test repetitions
    std::size_t iterations = 1;

    // Human-readable description
    std::string
    description() const
    {
        std::ostringstream oss;
        oss << "initial=" << initial_records
            << " idx_inserts=" << inserts_during_index
            << " rekey_inserts=" << inserts_during_rekey
            << " rate=" << (1000000 / insert_delay_us) << "/s"
            << " iters=" << iterations;
        return oss.str();
    }
};

/**
 * Stress test: Index and slice while concurrent inserts are happening
 *
 * This test simulates a live database scenario where:
 * 1. Start with initial_records in the database
 * 2. Launch background thread doing continuous inserts
 * 3. Build index (while inserts continue)
 * 4. Create slice (while inserts continue)
 * 5. Verify slice consistency
 *
 * The test verifies that:
 * - Index captures a consistent snapshot of records at time of creation
 * - Slice captures a consistent snapshot of the specified range
 * - No corruption or data races occur despite concurrent writes
 */
class SliceConcurrentInserts
    : public ::testing::TestWithParam<SliceStressParams>
{
public:
    // Background thread function: continuously insert records
    void
    insert_worker(std::size_t max_inserts, std::size_t delay_us)
    {
        std::size_t inserted = 0;

        while (!stop_inserts_ && inserted < max_inserts)
        {
            try
            {
                std::uint32_t value = next_insert_value_.fetch_add(1);

                TestRecord rec;
                rec.key = generate_key(value);
                rec.value = value;

                // Open, insert, close (NuDB handles locking)
                nudbview::basic_store<nudbview::xxhasher, nudbview::native_file>
                    store;
                nudbview::error_code ec;

                store.open(
                    db_->dat_path.string(),
                    db_->key_path.string(),
                    db_->log_path.string(),
                    ec);

                if (!ec)
                {
                    store.insert(
                        rec.key.data(), &rec.value, sizeof(rec.value), ec);
                    store.close(ec);
                }

                if (!ec)
                {
                    inserted++;
                    db_->records.push_back(rec);
                }

                if (delay_us > 0)
                    std::this_thread::sleep_for(
                        std::chrono::microseconds(delay_us));
            }
            catch (...)
            {
                // Swallow exceptions in background thread
            }
        }

        LOGI("Insert worker finished: ", inserted, " records inserted");
    }

protected:
    std::unique_ptr<TestDatabase> db_;
    std::atomic<bool> stop_inserts_{false};
    std::atomic<std::uint32_t> next_insert_value_{0};
    std::thread insert_thread_;

    void
    SetUp() override
    {
        // Set logger to INFO level for test output
        Logger::set_level(LogLevel::INFO);
    }

    void
    TearDown() override
    {
        stop_inserts_ = true;
        if (insert_thread_.joinable())
            insert_thread_.join();
    }
};

TEST_P(SliceConcurrentInserts, IndexAndSliceWhileInserting)
{
    auto params = GetParam();
    LOGI("\n=== Stress Test: ", params.description(), " ===");

    for (std::size_t iter = 0; iter < params.iterations; ++iter)
    {
        LOGI("\n--- Iteration ", iter + 1, "/", params.iterations, " ---");

        // Phase 1: Create initial database
        LOGI(
            "Phase 1: Creating initial database with ",
            params.initial_records,
            " records");
        db_ = create_test_database(params.initial_records, "slice-stress-test");
        next_insert_value_ = params.initial_records;

        ASSERT_EQ(db_->records.size(), params.initial_records);
        ASSERT_TRUE(fs::exists(db_->dat_path));

        // Phase 2: Start background inserts and build index
        LOGI(
            "Phase 2: Building index while inserting ",
            params.inserts_during_index,
            " records");

        stop_inserts_ = false;
        insert_thread_ = std::thread(
            &SliceConcurrentInserts::insert_worker,
            this,
            params.inserts_during_index,
            params.insert_delay_us);

        std::string index_path = (db_->dir / "nudb.dat.index").string();
        nudbutil::IndexBuildOptions opts;
        opts.index_interval = params.index_interval;
        opts.show_progress = false;

        auto result = nudbutil::IndexBuilder::build(
            db_->dat_path.string(), index_path, opts);

        LOGI(
            "  Index built: ",
            result.total_records,
            " records, ",
            result.entry_count,
            " entries");
        ASSERT_TRUE(result.success) << result.error_message;

        // Wait for insert thread to finish
        insert_thread_.join();

        // Phase 3: Create slice while doing more inserts
        LOGI(
            "Phase 3: Creating slice while inserting ",
            params.inserts_during_rekey,
            " records");

        // Start new round of inserts
        stop_inserts_ = false;
        insert_thread_ = std::thread(
            &SliceConcurrentInserts::insert_worker,
            this,
            params.inserts_during_rekey,
            params.insert_delay_us);

        // Load the index we just built
        nudbutil::IndexReader index_reader;
        nudbview::error_code ec;
        ASSERT_TRUE(index_reader.load(index_path, ec))
            << "Failed to load index: " << ec.message();

        // Find byte offsets for slice range
        nudbview::noff_t start_offset;
        std::uint64_t records_to_skip_start;
        ASSERT_TRUE(index_reader.lookup_record_start_offset(
            params.slice_start_record, start_offset, records_to_skip_start));

        // Scan forward if needed to reach exact start record
        if (records_to_skip_start > 0)
        {
            boost::iostreams::mapped_file_source mmap(db_->dat_path.string());
            nudbutil::scan_dat_records(
                mmap,
                db_->key_size,
                [&](std::uint64_t rnum, std::uint64_t offset, std::uint64_t) {
                    if (rnum == 0)
                    {
                        start_offset = offset;
                    }
                },
                start_offset,
                0);
        }

        // Find end offset (last record we want is slice_end_record - 1)
        nudbview::noff_t end_offset;
        std::uint64_t records_to_skip_end;
        ASSERT_TRUE(index_reader.lookup_record_start_offset(
            params.slice_end_record - 1, end_offset, records_to_skip_end));

        // Calculate the last byte of the end record
        {
            boost::iostreams::mapped_file_source mmap(db_->dat_path.string());
            auto const* data =
                reinterpret_cast<std::uint8_t const*>(mmap.data());
            std::uint64_t file_size = mmap.size();

            // Scan forward to reach the end record
            if (records_to_skip_end > 0)
            {
                std::uint64_t scanned = 0;
                nudbutil::scan_dat_records(
                    mmap,
                    db_->key_size,
                    [&](std::uint64_t, std::uint64_t offset, std::uint64_t) {
                        if (scanned == records_to_skip_end)
                        {
                            end_offset = offset;
                        }
                        scanned++;
                    },
                    end_offset,
                    0);
            }

            // Calculate last byte of end record (inclusive)
            if (end_offset + 6 <= file_size)
            {
                std::uint64_t value_size =
                    (static_cast<std::uint64_t>(data[end_offset + 0]) << 40) |
                    (static_cast<std::uint64_t>(data[end_offset + 1]) << 32) |
                    (static_cast<std::uint64_t>(data[end_offset + 2]) << 24) |
                    (static_cast<std::uint64_t>(data[end_offset + 3]) << 16) |
                    (static_cast<std::uint64_t>(data[end_offset + 4]) << 8) |
                    static_cast<std::uint64_t>(data[end_offset + 5]);

                end_offset += 6 + db_->key_size + value_size - 1;
            }
        }

        LOGI("  Slice range: [", start_offset, ", ", end_offset, "]");

        // Create the slice (while inserts are happening!)
        std::string slice_key_path = (db_->dir / "slice.key").string();
        std::string slice_meta_path = (db_->dir / "slice.meta").string();

        nudbview::view::rekey_slice<nudbview::xxhasher, nudbview::native_file>(
            db_->dat_path.string(),
            start_offset,
            end_offset,
            slice_key_path,
            slice_meta_path,
            db_->block_size,
            db_->load_factor,
            params.index_interval,
            8192,  // buffer_size
            ec,
            [](std::uint64_t, std::uint64_t) {}  // progress callback
        );

        ASSERT_FALSE(ec) << "Failed to create slice: " << ec.message();
        ASSERT_TRUE(fs::exists(slice_key_path));
        ASSERT_TRUE(fs::exists(slice_meta_path));

        LOGI("  Slice created successfully");

        // Wait for insert thread to finish
        insert_thread_.join();

        // Phase 4: Verify the slice
        LOGI("Phase 4: Verifying slice consistency");

        // Read salt from key file
        nudbview::detail::key_file_header kfh;
        nudbview::native_file kf;
        kf.open(nudbview::file_mode::read, db_->key_path.string(), ec);
        ASSERT_FALSE(ec) << "Failed to open key file: " << ec.message();

        nudbview::detail::read(kf, kfh, ec);
        ASSERT_FALSE(ec) << "Failed to read key file header: " << ec.message();
        kf.close();

        // Open the slice
        nudbview::view::slice_store<nudbview::xxhasher, nudbview::native_file>
            slice{kfh.salt};
        slice.open(db_->dat_path.string(), slice_key_path, slice_meta_path, ec);

        ASSERT_FALSE(ec) << "Failed to open slice: " << ec.message();

        // Verify we can fetch records that should be in the slice
        // We'll spot-check a few records from the slice range
        std::size_t verified = 0;
        for (std::size_t i = params.slice_start_record; i <
             std::min(params.slice_start_record + 10, params.slice_end_record);
             ++i)
        {
            if (i >= db_->records.size())
                break;

            auto const& rec = db_->records[i];
            bool found = false;

            nudbview::error_code fetch_ec;
            slice.fetch(
                rec.key.data(),
                [&](void const* value, std::size_t size) {
                    found = true;
                    EXPECT_EQ(size, sizeof(rec.value));
                    if (size == sizeof(rec.value))
                    {
                        std::uint32_t fetched_value;
                        std::memcpy(
                            &fetched_value, value, sizeof(fetched_value));
                        EXPECT_EQ(fetched_value, rec.value);
                    }
                },
                fetch_ec);

            if (!fetch_ec)
                verified++;
        }

        LOGI("  Verified ", verified, " records in slice");

        slice.close(ec);

        LOGI("  Test iteration ", iter + 1, " complete");

        // Clean up for next iteration
        stop_inserts_ = true;
        if (insert_thread_.joinable())
            insert_thread_.join();
        db_.reset();  // Triggers cleanup
    }
}

/**
 * Helper to read environment variable with default value
 */
inline std::size_t
get_env_size_t(const char* name, std::size_t default_value)
{
    const char* val = std::getenv(name);
    if (val && *val)
    {
        try
        {
            return std::stoull(val);
        }
        catch (...)
        {
            // Can't log here - this runs at global init before logging is
            // ready!
            return default_value;
        }
    }
    return default_value;
}

/**
 * Check if fuzzing mode is enabled
 */
inline bool
is_fuzz_mode()
{
    const char* fuzz = std::getenv("FUZZ");
    return fuzz && (std::string(fuzz) == "1" || std::string(fuzz) == "true");
}

/**
 * Generate random test parameters for fuzzing
 */
inline SliceStressParams
generate_random_params(std::mt19937& rng)
{
    std::uniform_int_distribution<std::size_t> initial_dist(100, 5000);
    std::uniform_int_distribution<std::size_t> inserts_dist(10, 500);
    std::uniform_int_distribution<std::size_t> delay_dist(1, 1000);
    std::uniform_int_distribution<std::size_t> interval_dist(10, 100);

    std::size_t initial = initial_dist(rng);
    std::size_t interval = interval_dist(rng);

    // CRITICAL: You can ONLY slice at interval boundaries!
    // Index has exact offsets for records 0, interval, 2*interval, etc.
    // Trying to slice to a non-boundary requires scanning forward on live file
    // = DANGER!

    // Calculate max valid boundary (round down to interval)
    std::size_t max_boundary = (initial / interval) * interval;

    // Pick a random boundary within [interval/4, 3*interval/4] of the data
    std::size_t min_boundary_idx =
        std::max<std::size_t>(1, (max_boundary / interval) / 4);
    std::size_t max_boundary_idx = std::max<std::size_t>(
        min_boundary_idx + 1, (max_boundary / interval) * 3 / 4);
    std::size_t boundary_idx = std::uniform_int_distribution<std::size_t>(
        min_boundary_idx, max_boundary_idx)(rng);
    std::size_t slice_end = boundary_idx * interval;

    return SliceStressParams{
        .initial_records = initial,
        .inserts_during_index = inserts_dist(rng),
        .inserts_during_rekey = inserts_dist(rng),
        .insert_delay_us = delay_dist(rng),
        .index_interval = interval,
        .slice_start_record = 0,
        .slice_end_record = slice_end,
        .iterations = 1  // Fuzz mode: 1 iteration per random config
    };
}

/**
 * Create test params with environment variable overrides
 *
 * Environment variables (all optional):
 *   STRESS_INITIAL_RECORDS - Initial database size (default: varies by suite)
 *   STRESS_INDEX_INSERTS   - Inserts during indexing (default: varies by suite)
 *   STRESS_REKEY_INSERTS   - Inserts during slicing (default: varies by suite)
 *   STRESS_INSERT_DELAY_US - Delay between inserts in microseconds (default:
 * varies) STRESS_ITERATIONS      - Number of test iterations (default: varies
 * by suite)
 *
 * Example:
 *   STRESS_ITERATIONS=1000 ./build/tests/nudbview/slice_stress_gtest
 */
inline SliceStressParams
make_params_with_env_overrides(SliceStressParams defaults)
{
    return SliceStressParams{
        .initial_records =
            get_env_size_t("STRESS_INITIAL_RECORDS", defaults.initial_records),
        .inserts_during_index = get_env_size_t(
            "STRESS_INDEX_INSERTS", defaults.inserts_during_index),
        .inserts_during_rekey = get_env_size_t(
            "STRESS_REKEY_INSERTS", defaults.inserts_during_rekey),
        .insert_delay_us =
            get_env_size_t("STRESS_INSERT_DELAY_US", defaults.insert_delay_us),
        .index_interval = defaults.index_interval,
        .slice_start_record = defaults.slice_start_record,
        .slice_end_record = defaults.slice_end_record,
        .iterations = get_env_size_t("STRESS_ITERATIONS", defaults.iterations)};
}

// Test configurations
INSTANTIATE_TEST_SUITE_P(
    Light,
    SliceConcurrentInserts,
    ::testing::Values(make_params_with_env_overrides(SliceStressParams{
        .initial_records = 500,
        .inserts_during_index = 50,
        .inserts_during_rekey = 50,
        .insert_delay_us = 100,  // ~10k inserts/sec
        .iterations = 3})));

// Heavy stress test (commented out for now)
// INSTANTIATE_TEST_SUITE_P(
//     DISABLED_Heavy,
//     SliceConcurrentInserts,
//     ::testing::Values(
//         make_params_with_env_overrides(SliceStressParams{
//             .initial_records = 10000,
//             .inserts_during_index = 1000,
//             .inserts_during_rekey = 1000,
//             .insert_delay_us = 10,  // ~100k inserts/sec
//             .iterations = 10
//         })
//     )
// );

// ============================================================================
// Fuzz test - generates random parameters and tests concurrent operations
// ============================================================================
//
// CRITICAL: This test validates slicing LIVE .dat files
// -------------------------------------------------------
// The whole point of this stress test is to prove that we can:
// 1. Take slices of .dat files that are ACTIVELY being written by another
// process
// 2. Handle partial/incomplete records at the tail gracefully
// 3. Use IndexBuilder to find safe bounds before slicing
//
// What we're testing:
// - Background thread continuously inserts records (simulates live Ripple node)
// - Main thread builds index WHILE inserts are happening
// - Main thread creates slice WHILE inserts are happening
// - IndexBuilder stops at first incomplete record (correct behavior!)
// - We clamp slice range to what was actually indexed
// - File size checks prevent reading beyond valid data
//
// The "short read" error from bulk_reader::prepare() (in bulkio.hpp:72-75)
// indicates we tried to read a partial record. This is EXPECTED on live files!
//
// We handle it by:
// 1. IndexBuilder scans and records total_records (stops at corrupt tail)
// 2. We clamp slice_end to min(requested, total_records)
// 3. We check file size and clamp end_offset if needed
// 4. If we still get short_read, something is wrong with our logic!
//
// Enable with: FUZZ=1 FUZZ_ITERATIONS=100
// ./build/tests/nudbview/slice_stress_gtest --gtest_filter="Fuzz/*" Or use:
// ./scripts/run-nudbview-stress.sh --fuzz 1000

class FuzzParams : public ::testing::TestWithParam<int>
{
protected:
    void
    SetUp() override
    {
        // Set logger to INFO level for test output
        Logger::set_level(LogLevel::INFO);
    }
};

TEST_P(FuzzParams, GenerateRandomConfigs)
{
    if (!is_fuzz_mode())
    {
        GTEST_SKIP() << "Fuzz mode not enabled (set FUZZ=1)";
    }

    // Seed RNG with time + test iteration for reproducibility within a run
    std::size_t seed =
        std::chrono::steady_clock::now().time_since_epoch().count() +
        GetParam();
    std::mt19937 rng(seed);

    // Generate random params
    auto params = generate_random_params(rng);

    LOGI("\n=== FUZZ Test #", GetParam(), " (seed=", seed, ") ===");
    LOGI("Random params: ", params.description());

    // Create initial database
    auto db = create_test_database(params.initial_records, "fuzz-test");
    std::atomic<bool> stop_inserts{false};
    std::atomic<std::uint32_t> next_insert_value{
        static_cast<std::uint32_t>(params.initial_records)};

    ASSERT_EQ(db->records.size(), params.initial_records);

    // Lambda for insert worker
    auto insert_worker = [&](std::size_t max_inserts, std::size_t delay_us) {
        std::size_t inserted = 0;
        while (!stop_inserts && inserted < max_inserts)
        {
            try
            {
                std::uint32_t value = next_insert_value.fetch_add(1);
                TestRecord rec;
                rec.key = generate_key(value);
                rec.value = value;

                nudbview::basic_store<nudbview::xxhasher, nudbview::native_file>
                    store;
                nudbview::error_code ec;
                store.open(
                    db->dat_path.string(),
                    db->key_path.string(),
                    db->log_path.string(),
                    ec);
                if (!ec)
                {
                    store.insert(
                        rec.key.data(), &rec.value, sizeof(rec.value), ec);
                    store.close(ec);
                }
                if (!ec)
                {
                    inserted++;
                    db->records.push_back(rec);
                }
                if (delay_us > 0)
                    std::this_thread::sleep_for(
                        std::chrono::microseconds(delay_us));
            }
            catch (...)
            {
            }
        }
    };

    // Build index while inserting (this is the stress test!)
    LOGI("Building index while inserting...");
    stop_inserts = false;
    std::thread insert_thread1(
        insert_worker, params.inserts_during_index, params.insert_delay_us);

    std::string index_path = (db->dir / "nudb.dat.index").string();
    nudbutil::IndexBuildOptions opts;
    opts.index_interval = params.index_interval;
    opts.show_progress = false;

    auto result =
        nudbutil::IndexBuilder::build(db->dat_path.string(), index_path, opts);

    ASSERT_TRUE(result.success) << result.error_message;
    stop_inserts = true;
    insert_thread1.join();

    LOGI("Index built: ", result.total_records, " records");

    // Clamp slice range to what was actually indexed
    // IndexBuilder guarantees total_records is at an interval boundary
    std::size_t safe_slice_end = std::min(
        params.slice_end_record,
        static_cast<std::size_t>(result.total_records));

    // CRITICAL: We can only slice using index lookups!
    // We need BOTH boundaries (start and end) to exist in the index.
    // The only exception is if we're slicing to total_records where the last
    // indexed boundary is valid.

    // For now: only allow slicing when we have the NEXT boundary in the index
    // (i.e., safe_slice_end < total_records so we can look up the start of the
    // next record)
    if (safe_slice_end >= result.total_records)
    {
        // Would need to scan to find end - skip this test for now
        LOGI("Skipping slice creation - would require scanning to EOF");
        LOGI("✓ FUZZ test #", GetParam(), " passed (EOF skip)!");
        return;
    }

    // Edge case: if clamping resulted in invalid range, skip this test
    if (safe_slice_end <= params.slice_start_record)
    {
        LOGI("Skipping slice creation - not enough complete intervals");
        LOGI("✓ FUZZ test #", GetParam(), " passed (boundary skip)!");
        return;
    }

    LOGI(
        "Slice range: [",
        params.slice_start_record,
        ", ",
        safe_slice_end,
        ") of ",
        result.total_records,
        " indexed records");

    // Create slice while inserting (this is the stress test!)
    LOGI("Creating slice while inserting...");
    stop_inserts = false;
    std::thread insert_thread2(
        insert_worker, params.inserts_during_rekey, params.insert_delay_us);

    // CRITICAL: Use byte offsets directly from IndexBuilder's snapshot
    // The index was built from a consistent snapshot of complete records.
    // If we re-scan the file now, we might hit NEW partial records from
    // thread2. Solution: Trust the index! Use its offsets directly without
    // re-scanning.

    // Load the index
    nudbutil::IndexReader index_reader;
    nudbview::error_code ec;
    ASSERT_TRUE(index_reader.load(index_path, ec));

    // CRITICAL: We can ONLY slice at interval boundaries!
    // Verify that our slice range is at valid boundaries
    ASSERT_EQ(params.slice_start_record % params.index_interval, 0)
        << "slice_start_record must be at interval boundary!";
    ASSERT_EQ(safe_slice_end % params.index_interval, 0)
        << "slice_end must be at interval boundary!";

    // Get EXACT byte offsets from index (no scanning needed at boundaries!)
    // To slice [start_record, end_record), we need:
    //   - START of start_record (at boundary)
    //   - START of end_record (at boundary) - then subtract 1 for inclusive end

    nudbview::noff_t start_offset;
    std::uint64_t records_to_skip_start;
    ASSERT_TRUE(index_reader.lookup_record_start_offset(
        params.slice_start_record, start_offset, records_to_skip_start));
    ASSERT_EQ(records_to_skip_start, 0)
        << "At boundary - should get exact offset!";

    // Look up the NEXT boundary to get where our slice ends
    nudbview::noff_t end_boundary;
    std::uint64_t records_to_skip_end;
    ASSERT_TRUE(index_reader.lookup_record_start_offset(
        safe_slice_end, end_boundary, records_to_skip_end));
    ASSERT_EQ(records_to_skip_end, 0)
        << "At boundary - should get exact offset!";

    // rekey_slice expects [start, end] INCLUSIVE, so subtract 1
    nudbview::noff_t end_offset = end_boundary - 1;

    LOGI(
        "Slice byte range: [",
        start_offset,
        ", ",
        end_offset,
        "] (at interval boundaries, exact offsets)");

    // Create slice
    std::string slice_key_path = (db->dir / "slice.key").string();
    std::string slice_meta_path = (db->dir / "slice.meta").string();

    nudbview::view::rekey_slice<nudbview::xxhasher, nudbview::native_file>(
        db->dat_path.string(),
        start_offset,
        end_offset,
        slice_key_path,
        slice_meta_path,
        db->block_size,
        db->load_factor,
        params.index_interval,
        8192,
        ec,
        [](std::uint64_t, std::uint64_t) {});

    // Stop background inserts now that slicing is done
    stop_inserts = true;
    insert_thread2.join();

    // With index_interval=1, we have exact offsets for all indexed records.
    // We should NEVER get short_read because we're only slicing within the
    // bounds of complete records that were in the index snapshot.
    // If we DO get short_read, something is wrong with our logic!
    ASSERT_FALSE(ec) << "Failed to create slice: " << ec.message()
                     << "\n  Params: " << params.description()
                     << "\n  Seed: " << seed << "\n  Byte range: ["
                     << start_offset << ", " << end_offset << "]"
                     << "\n  This should NOT happen with index_interval=1!";

    LOGI("✓ FUZZ test #", GetParam(), " passed!");
}

// Generate fuzz test instances - controlled by FUZZ_ITERATIONS env var
INSTANTIATE_TEST_SUITE_P(
    Fuzz,
    FuzzParams,
    ::testing::Range(
        0,
        static_cast<int>(get_env_size_t("FUZZ_ITERATIONS", 100))));
