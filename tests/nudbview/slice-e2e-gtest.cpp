#include "test-helpers.hpp"
#include <boost/filesystem.hpp>
#include <catl/core/logger.h>
#include <gtest/gtest.h>
#include <nudbview/nudb.hpp>
#include <nudbview/view/dat_scanner.hpp>
#include <nudbview/view/index_builder.hpp>
#include <nudbview/view/index_reader.hpp>
#include <nudbview/view/slice_rekey.hpp>
#include <nudbview/view/slice_store.hpp>

using namespace nudbview_test;
namespace fs = boost::filesystem;

// End-to-end test: Create DB, index it, slice it, verify slice works correctly
TEST(SliceE2E, CreateIndexSliceAndQuery)
{
    // Phase 1: Create test database with 1000 records
    auto db = create_test_database(1000, "slice-e2e-test");

    LOGI("\n=== Phase 1: Created test database with 1000 records ===");
    ASSERT_EQ(db->records.size(), 1000);
    ASSERT_TRUE(fs::exists(db->dat_path));
    ASSERT_TRUE(fs::exists(db->key_path));

    // Phase 2: Build index with interval=50
    std::string index_path = (db->dir / "nudb.dat.index").string();

    nudbutil::IndexBuildOptions opts;
    opts.index_interval = 50;
    opts.show_progress = false;

    auto result =
        nudbutil::IndexBuilder::build(db->dat_path.string(), index_path, opts);

    LOGI("=== Phase 2: Built index ===");
    LOGI("  Total records: ", result.total_records);
    LOGI("  Index entries: ", result.entry_count);

    ASSERT_TRUE(result.success) << result.error_message;
    ASSERT_EQ(result.total_records, 1000);
    ASSERT_EQ(result.entry_count, 20);  // 1000 / 50

    // Phase 3: Use IndexReader to find byte offsets for records 0 and 500
    LOGI("=== Phase 3: Looking up byte offsets ===");

    nudbutil::IndexReader index_reader;
    nudbview::error_code ec;
    ASSERT_TRUE(index_reader.load(index_path, ec))
        << "Failed to load index: " << ec.message();

    // Find offset for record 0 (start)
    nudbview::noff_t start_offset;
    std::uint64_t records_to_skip_start;
    ASSERT_TRUE(
        index_reader.lookup_record(0, start_offset, records_to_skip_start));

    // Scan forward if needed
    if (records_to_skip_start > 0)
    {
        boost::iostreams::mapped_file_source mmap(db->dat_path.string());
        nudbutil::scan_dat_records(
            mmap,
            db->key_size,
            [&](std::uint64_t rnum, std::uint64_t offset, std::uint64_t) {
                if (rnum == 0)
                {
                    start_offset = offset;
                }
            },
            start_offset,
            0);
    }

    // Find offset for record 499 (last record we want in slice)
    nudbview::noff_t end_offset;
    std::uint64_t records_to_skip_end;
    ASSERT_TRUE(
        index_reader.lookup_record(499, end_offset, records_to_skip_end));

    // Scan forward to find record 499, then calculate its last byte
    {
        boost::iostreams::mapped_file_source mmap(db->dat_path.string());
        auto const* data = reinterpret_cast<std::uint8_t const*>(mmap.data());
        std::uint64_t file_size = mmap.size();

        // Scan forward records_to_skip_end records to reach record 499
        if (records_to_skip_end > 0)
        {
            std::uint64_t scanned = 0;
            nudbutil::scan_dat_records(
                mmap,
                db->key_size,
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

        // Now end_offset points to start of record 499
        // Record format: 6 bytes (size) + key_size bytes (key) + value_size
        // bytes (value) We want the LAST byte of record 499 (inclusive)
        if (end_offset + 6 <= file_size)
        {
            std::uint64_t value_size =
                (static_cast<std::uint64_t>(data[end_offset + 0]) << 40) |
                (static_cast<std::uint64_t>(data[end_offset + 1]) << 32) |
                (static_cast<std::uint64_t>(data[end_offset + 2]) << 24) |
                (static_cast<std::uint64_t>(data[end_offset + 3]) << 16) |
                (static_cast<std::uint64_t>(data[end_offset + 4]) << 8) |
                static_cast<std::uint64_t>(data[end_offset + 5]);

            // Point to last byte of record 499 (inclusive)
            end_offset += 6 + db->key_size + value_size - 1;
        }
    }

    // Check dat file size for debugging
    std::uint64_t dat_file_size = fs::file_size(db->dat_path);

    LOGI("  Start offset: ", start_offset);
    LOGI("  End offset: ", end_offset);
    LOGI("  Dat file size: ", dat_file_size);
    LOGI("  Header size: ", nudbview::detail::dat_file_header::size);

    // Phase 4: Create slice using byte offsets
    std::string slice_key_path = (db->dir / "slice-0-500.key").string();
    std::string slice_meta_path = (db->dir / "slice-0-500.meta").string();

    LOGI("=== Phase 4: Creating slice ===");

    nudbview::view::rekey_slice<nudbview::xxhasher, nudbview::native_file>(
        db->dat_path.string(),
        start_offset,
        end_offset,
        slice_key_path,
        slice_meta_path,
        4096,  // block_size
        0.5f,  // load_factor
        50,    // index_interval (same as main index)
        8192,  // buffer_size
        ec,
        [](std::uint64_t, std::uint64_t) {}  // progress callback (no-op)
    );

    ASSERT_FALSE(ec) << "Failed to create slice: " << ec.message();
    ASSERT_TRUE(fs::exists(slice_key_path));
    ASSERT_TRUE(fs::exists(slice_meta_path));

    LOGI("  Slice created successfully");

    // Phase 5: Read salt from original database's key file
    LOGI("=== Phase 5: Reading salt from original key file ===");

    nudbview::detail::key_file_header kfh;
    nudbview::native_file kf;
    kf.open(nudbview::file_mode::read, db->key_path.string(), ec);
    ASSERT_FALSE(ec) << "Failed to open key file: " << ec.message();

    nudbview::detail::read(kf, kfh, ec);
    ASSERT_FALSE(ec) << "Failed to read key file header: " << ec.message();
    kf.close();

    LOGI("  Salt: ", kfh.salt);

    // Phase 6: Open slice store
    LOGI("=== Phase 6: Opening slice store ===");

    nudbview::view::slice_store<nudbview::xxhasher, nudbview::native_file>
        slice{kfh.salt};
    slice.open(db->dat_path.string(), slice_key_path, slice_meta_path, ec);

    ASSERT_FALSE(ec) << "Failed to open slice: " << ec.message();
    LOGI("  Slice opened successfully");

    // Phase 7: Visit all keys in original database and check against slice
    // Visit traverses the .dat file in physical order (sorted key order)
    // So the first 500 visited keys should be in our slice
    LOGI("=== Phase 7: Visiting original DB and checking slice ===");

    nudbview::basic_store<nudbview::xxhasher, nudbview::native_file>
        original_store;
    original_store.open(
        db->dat_path.string(),
        db->key_path.string(),
        db->log_path.string(),
        ec);

    ASSERT_FALSE(ec) << "Failed to open original store: " << ec.message();

    std::size_t visit_count = 0;
    std::size_t found_in_slice = 0;
    std::size_t not_found_in_slice = 0;

    // Visit all keys in original database
    // IMPORTANT: visit() iterates in physical file order (sorted key order)
    nudbview::visit(
        db->dat_path.string(),
        [&](void const* key,
            std::size_t key_size,
            void const* value,
            std::size_t value_size,
            nudbview::error_code& visit_ec) -> bool {
            visit_count++;

            // Try to fetch this key from the slice
            bool found = false;
            nudbview::error_code fetch_ec;
            slice.fetch(
                key,
                [&](void const* slice_value, std::size_t slice_value_size) {
                    found = true;
                    // Verify the values match
                    EXPECT_EQ(slice_value_size, value_size);
                    EXPECT_EQ(0, std::memcmp(slice_value, value, value_size))
                        << "Value mismatch for record " << visit_count;
                },
                fetch_ec);

            // "key not found" is expected for keys outside the slice - not an
            // error
            if (fetch_ec && fetch_ec != nudbview::error::key_not_found)
            {
                visit_ec = fetch_ec;  // Propagate real errors
                return false;         // Stop visiting
            }

            if (found)
            {
                found_in_slice++;
                // First 500 visited records should be in slice
                EXPECT_LE(visit_count, 500)
                    << "Found record " << visit_count
                    << " in slice, but only first 500 should be present";
            }
            else
            {
                not_found_in_slice++;
                // Records after 500 should NOT be in slice
                EXPECT_GT(visit_count, 500)
                    << "Record " << visit_count
                    << " not in slice, but first 500 should be present";
            }

            return true;  // Continue visiting
        },
        [](std::uint64_t, std::uint64_t) {},  // Progress callback (no-op)
        ec);

    ASSERT_FALSE(ec) << "Visit failed: " << ec.message();

    LOGI("  Visited ", visit_count, " records");
    LOGI("  Found in slice: ", found_in_slice);
    LOGI("  Not in slice: ", not_found_in_slice);

    // Verify we visited all 1000 records
    EXPECT_EQ(visit_count, 1000);

    // Verify exactly first 500 were in slice
    EXPECT_EQ(found_in_slice, 500);
    EXPECT_EQ(not_found_in_slice, 500);

    // Phase 8: Clean up
    original_store.close(ec);
    slice.close(ec);

    LOGI("=== Test Complete ===");
}
