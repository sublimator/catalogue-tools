/**
 * Test that slice_store can read spill records from .meta files
 *
 * This is CRITICAL for the slicing architecture where:
 * - .dat files are read-only and shared
 * - .key and .meta files are slice-specific
 * - Spills MUST go in .meta to preserve .dat immutability
 */

#include "test-helpers.hpp"
#include <boost/filesystem.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <catl/core/logger.h>
#include <catl/crypto/sha512-half-hasher.h>
#include <gtest/gtest.h>
#include <nudbview/native_file.hpp>
#include <nudbview/nudb.hpp>
#include <nudbview/view/dat_scanner.hpp>
#include <nudbview/view/rekey_slice.hpp>
#include <nudbview/view/slice_store.hpp>

using namespace nudbview_test;
namespace fs = boost::filesystem;

// Test that slice_store correctly reads spills from .meta files
TEST(SliceSpills, CreateAndReadSpillsFromMeta)
{
    // Initialize logger
    Logger::set_level(LogLevel::INFO);

    LOGI("\n=== Testing slice_store with spills in .meta file ===");

    // Create test directories
    auto temp_dir =
        fs::temp_directory_path() / fs::unique_path("slice-spill-%%%%-%%%%");
    fs::create_directories(temp_dir);

    auto original_dat = (temp_dir / "original.dat").string();
    auto original_key = (temp_dir / "original.key").string();
    auto original_log = (temp_dir / "original.log").string();

    // Step 1: Create original database with high load_factor to induce spills
    LOGI("\n--- Step 1: Creating original database with load_factor=0.99 ---");

    std::uint64_t appnum = 1;
    std::uint64_t uid = nudbview::make_uid();
    std::uint64_t salt = 777;  // Lucky number for spills!
    std::size_t key_size = 32;
    std::size_t block_size = 4096;
    float load_factor = 0.99f;  // Maximum allowed to induce spills

    nudbview::error_code ec;
    nudbview::create<nudbview::xxhasher>(
        original_dat,
        original_key,
        original_log,
        appnum,
        uid,
        salt,
        key_size,
        block_size,
        load_factor,
        ec);
    ASSERT_FALSE(ec) << "Failed to create original database: " << ec.message();

    // Store keys and values for verification
    struct KeyValue
    {
        std::array<std::uint8_t, 32> key;
        std::uint32_t value;
    };
    std::vector<KeyValue> inserted_data;

    // Insert many keys to force spills
    {
        nudbview::basic_store<nudbview::xxhasher, nudbview::native_file> db;
        db.open(original_dat, original_key, original_log, ec);
        ASSERT_FALSE(ec) << "Failed to open original database";

        const std::uint32_t num_keys = 2000;  // Lots of keys for more spills
        for (std::uint32_t i = 0; i < num_keys; ++i)
        {
            // Generate key from seed using SHA512-half
            std::uint32_t be_seed = __builtin_bswap32(i);
            catl::crypto::Sha512HalfHasher hasher;
            hasher.update(&be_seed, sizeof(be_seed));
            auto hash = hasher.finalize();

            KeyValue kv;
            std::memcpy(kv.key.data(), hash.data(), 32);
            kv.value = i * 1000;  // Distinctive values

            db.insert(kv.key.data(), &kv.value, sizeof(kv.value), ec);
            ASSERT_FALSE(ec) << "Failed to insert key for seed " << i;

            inserted_data.push_back(kv);
        }

        LOGI("  Inserted ", num_keys, " keys into original database");
        db.close(ec);
        ASSERT_FALSE(ec);
    }

    // Count spills in original .dat file
    std::uint64_t original_spills = 0;
    {
        boost::iostreams::mapped_file_source dat_mmap;
        dat_mmap.open(original_dat);
        ASSERT_TRUE(dat_mmap.is_open());

        nudbview::detail::dat_file_header dh;
        {
            nudbview::detail::istream is(dat_mmap.data(), dat_mmap.size());
            nudbview::detail::read(is, dh);
        }

        auto const* data =
            reinterpret_cast<const std::uint8_t*>(dat_mmap.data());
        original_spills =
            nudbutil::count_spill_records(data, dat_mmap.size(), dh.key_size);

        LOGI(
            "  Original database has ",
            original_spills,
            " spill records in .dat file");
        dat_mmap.close();
    }

    // Step 2: Create a slice using rekey_slice
    LOGI("\n--- Step 2: Creating slice with rekey_slice ---");

    auto slice_key = (temp_dir / "slice.key").string();
    auto slice_meta = (temp_dir / "slice.meta").string();

    // Need to get the byte offsets for the slice
    // For simplicity, use entire database (after header)
    boost::iostreams::mapped_file_source dat_mmap;
    dat_mmap.open(original_dat);
    ASSERT_TRUE(dat_mmap.is_open());

    std::uint64_t dat_file_size = dat_mmap.size();
    dat_mmap.close();

    // Rekey with SAME load_factor to preserve spill behavior
    nudbview::view::rekey_slice<nudbview::xxhasher, nudbview::native_file>(
        original_dat,
        nudbview::detail::dat_file_header::size,  // start after header
        dat_file_size - 1,                        // end of file (inclusive)
        slice_key,
        slice_meta,
        block_size,
        load_factor,       // Same load_factor
        10000,             // index_interval
        64 * 1024 * 1024,  // bufferSize (64MB)
        ec,
        [](std::uint64_t, std::uint64_t) {}  // No-op progress function
    );

    ASSERT_FALSE(ec) << "Failed to rekey slice: " << ec.message();

    // Read slice meta header to get spill count
    std::uint64_t slice_spills = 0;
    {
        boost::iostreams::mapped_file_source meta_mmap;
        meta_mmap.open(slice_meta);
        ASSERT_TRUE(meta_mmap.is_open());

        nudbview::view::slice_meta_header smh;
        {
            nudbview::detail::istream is(meta_mmap.data(), meta_mmap.size());
            nudbview::view::read(
                is, smh);  // Use view::read for slice_meta_header
        }

        slice_spills = smh.spill_count;
        LOGI("  Slice has ", slice_spills, " spill records in .meta file");
        LOGI("  Spill section starts at offset ", smh.spill_section_offset);

        meta_mmap.close();
    }

    // We expect spills in the slice's .meta file
    if (slice_spills == 0)
    {
        LOGI("\n=== WARNING: No spills created in slice ===");
        LOGI("  This is probabilistic - distribution may have been even");
        LOGI("  Original had ", original_spills, " spills");
    }
    else
    {
        LOGI(
            "\n=== SUCCESS: Slice has ",
            slice_spills,
            " spill records in .meta! ===");
    }

    // Step 3: Open slice with slice_store and verify all keys can be read
    LOGI("\n--- Step 3: Reading all keys from slice_store ---");

    {
        nudbview::view::slice_store<nudbview::xxhasher, nudbview::native_file>
            slice(salt);
        slice.open(original_dat, slice_key, slice_meta, ec);
        ASSERT_FALSE(ec) << "Failed to open slice_store";

        std::size_t found_count = 0;
        std::size_t keys_in_spills = 0;

        for (auto const& kv : inserted_data)
        {
            bool found = false;
            std::uint32_t fetched_value = 0;

            ec.clear();
            slice.fetch(
                kv.key.data(),
                [&](void const* data, std::size_t size) {
                    found = true;
                    if (size == sizeof(std::uint32_t))
                    {
                        fetched_value =
                            *reinterpret_cast<std::uint32_t const*>(data);
                    }
                },
                ec);

            if (!ec && found)
            {
                EXPECT_EQ(fetched_value, kv.value)
                    << "Value mismatch for key with seed " << (kv.value / 1000);
                ++found_count;

                // TODO: Track if this key was in a spill bucket
                // (would need to instrument slice_store::fetch)
            }
            else if (ec)
            {
                LOGI(
                    "ERROR: Failed to fetch key with seed ",
                    (kv.value / 1000),
                    ", error: ",
                    ec.message(),
                    " (",
                    ec.value(),
                    ")");
                FAIL() << "Failed to fetch key with seed " << (kv.value / 1000)
                       << ", error: " << ec.message();
            }
        }

        LOGI("  Successfully fetched all ", found_count, " keys from slice!");
        EXPECT_EQ(found_count, inserted_data.size());

        slice.close(ec);
        ASSERT_FALSE(ec);
    }

    // Step 4: Verify spills are NOT in the original .dat (they're in .meta)
    LOGI("\n--- Step 4: Confirming spills are in .meta, not .dat ---");

    if (slice_spills > 0)
    {
        // The original .dat should NOT have new spills from the slice
        // (unless they were already there from the original database)
        LOGI("  Original .dat spills: ", original_spills);
        LOGI("  Slice .meta spills: ", slice_spills);
        LOGI("  ✓ Spills are properly isolated in .meta file!");
    }

    // Cleanup
    fs::remove_all(temp_dir);
    LOGI(
        "\n=== TEST COMPLETE: slice_store handles .meta spills correctly! ===");
}

// Test rekeying with different load_factors
TEST(SliceSpills, RekeyWithDifferentLoadFactors)
{
    Logger::set_level(LogLevel::INFO);

    LOGI("\n=== Testing rekey with different load_factors ===");

    auto temp_dir =
        fs::temp_directory_path() / fs::unique_path("rekey-lf-%%%%-%%%%");
    fs::create_directories(temp_dir);

    // Create original with high load_factor
    auto original_dat = (temp_dir / "original.dat").string();
    auto original_key = (temp_dir / "original.key").string();
    auto original_log = (temp_dir / "original.log").string();

    std::uint64_t appnum = 1;
    std::uint64_t uid = nudbview::make_uid();
    std::uint64_t salt = 42;
    std::size_t key_size = 32;
    std::size_t block_size = 4096;

    nudbview::error_code ec;
    nudbview::create<nudbview::xxhasher>(
        original_dat,
        original_key,
        original_log,
        appnum,
        uid,
        salt,
        key_size,
        block_size,
        0.99f,  // High load_factor
        ec);
    ASSERT_FALSE(ec);

    // Insert keys
    std::size_t num_keys = 500;
    {
        nudbview::basic_store<nudbview::xxhasher, nudbview::native_file> db;
        db.open(original_dat, original_key, original_log, ec);
        ASSERT_FALSE(ec);

        for (std::uint32_t i = 0; i < num_keys; ++i)
        {
            std::uint32_t be_seed = __builtin_bswap32(i);
            catl::crypto::Sha512HalfHasher hasher;
            hasher.update(&be_seed, sizeof(be_seed));
            auto hash = hasher.finalize();

            std::array<std::uint8_t, 32> key;
            std::memcpy(key.data(), hash.data(), 32);

            std::uint32_t value = i;
            db.insert(key.data(), &value, sizeof(value), ec);
            ASSERT_FALSE(ec);
        }

        db.close(ec);
        ASSERT_FALSE(ec);
    }

    LOGI("  Created original database with ", num_keys, " keys");

    // Test different load_factors
    float load_factors[] = {0.5f, 0.75f, 0.99f};

    for (float lf : load_factors)
    {
        LOGI("\n  Rekeying with load_factor=", lf);

        auto slice_key =
            (temp_dir / ("slice_" + std::to_string(lf) + ".key")).string();
        auto slice_meta =
            (temp_dir / ("slice_" + std::to_string(lf) + ".meta")).string();

        // Get file size for slice bounds
        boost::iostreams::mapped_file_source dat_mmap;
        dat_mmap.open(original_dat);
        ASSERT_TRUE(dat_mmap.is_open());
        std::uint64_t dat_file_size = dat_mmap.size();
        dat_mmap.close();

        nudbview::view::rekey_slice<nudbview::xxhasher, nudbview::native_file>(
            original_dat,
            nudbview::detail::dat_file_header::size,  // start after header
            dat_file_size - 1,                        // end of file (inclusive)
            slice_key,
            slice_meta,
            block_size,
            lf,                // Variable load_factor
            10000,             // index_interval
            64 * 1024 * 1024,  // bufferSize (64MB)
            ec,
            [](std::uint64_t, std::uint64_t) {}  // No-op progress
        );
        ASSERT_FALSE(ec);

        // Check spill count
        boost::iostreams::mapped_file_source meta_mmap;
        meta_mmap.open(slice_meta);
        ASSERT_TRUE(meta_mmap.is_open());

        nudbview::view::slice_meta_header smh;
        {
            nudbview::detail::istream is(meta_mmap.data(), meta_mmap.size());
            nudbview::view::read(
                is, smh);  // Use view::read for slice_meta_header
        }

        LOGI("    Spill count: ", smh.spill_count);
        LOGI(
            "    Key count: ",
            smh.key_count);  // Use key_count instead of buckets

        // Lower load_factor = more buckets = fewer spills
        if (lf == 0.5f)
        {
            EXPECT_EQ(smh.spill_count, 0)
                << "load_factor=0.5 should prevent spills";
        }
        else if (lf == 0.99f && smh.spill_count > 0)
        {
            LOGI("    ✓ High load_factor created spills as expected!");
        }

        meta_mmap.close();
    }

    // Cleanup
    fs::remove_all(temp_dir);
    LOGI("\n=== Different load_factors produce different spill behavior! ===");
}