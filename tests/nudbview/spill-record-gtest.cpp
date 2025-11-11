#include "test-helpers.hpp"
#include <boost/filesystem.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <catl/core/logger.h>
#include <catl/crypto/sha512-half-hasher.h>
#include <fstream>
#include <gtest/gtest.h>
#include <nudbview/native_file.hpp>
#include <nudbview/nudb.hpp>
#include <nudbview/view/dat_scanner.hpp>

using namespace nudbview_test;
namespace fs = boost::filesystem;

// Test that load_factor=0.99 creates spill records in the .dat file
TEST(SpillRecords, LoadFactorNearMaxCreatesSpills)
{
    // Initialize logger to INFO level so LOGI works
    Logger::set_level(LogLevel::INFO);

    // Phase 1: Generate many keys to insert
    // With load_factor=0.99 (max allowed is <1.0), the database delays splits
    // until buckets are 99% full on average. This means some buckets can
    // accumulate 20+ keys before splitting, exceeding the 16-key capacity
    // and forcing spill records.
    std::vector<std::uint32_t> seeds;
    const std::uint32_t num_keys = 500;  // More keys = higher chance of spills
    for (std::uint32_t i = 0; i < num_keys; ++i)
    {
        seeds.push_back(i);
    }

    ASSERT_GT(seeds.size(), 16)
        << "Need more than 16 keys to test spill behavior";
    LOGI(
        "\n=== Phase 1: Using ",
        seeds.size(),
        " keys with load_factor=0.99 ===");

    // Phase 2: Create test database with collision keys
    auto temp_dir =
        fs::temp_directory_path() / fs::unique_path("spill-test-%%%%-%%%%");
    fs::create_directories(temp_dir);

    auto dat_path = (temp_dir / "nudb.dat").string();
    auto key_path = (temp_dir / "nudb.key").string();
    auto log_path = (temp_dir / "nudb.log").string();

    // Database parameters
    // IMPORTANT: NuDB starts with 1 bucket and grows dynamically
    // Use load_factor=0.99 to delay splits and increase chance of spills!
    std::uint64_t appnum = 1;
    std::uint64_t uid = nudbview::make_uid();
    std::uint64_t salt = 1;
    std::size_t key_size = 32;
    std::size_t block_size = 4096;
    float load_factor = 0.99f;  // Near-MAX load factor to delay splits!

    nudbview::error_code ec;
    nudbview::create<nudbview::xxhasher>(
        dat_path,
        key_path,
        log_path,
        appnum,
        uid,
        salt,
        key_size,
        block_size,
        load_factor,
        ec);
    ASSERT_FALSE(ec) << "Failed to create database: " << ec.message();

    LOGI("=== Phase 2: Creating database with load_factor=0.99 ===");
    LOGI("  Database will delay splits until 99% full on average");
    LOGI("  Some buckets may accumulate 20+ keys before splitting");

    nudbview::basic_store<nudbview::xxhasher, nudbview::native_file> db;
    db.open(dat_path, key_path, log_path, ec);
    ASSERT_FALSE(ec) << "Failed to open database: " << ec.message();

    // Insert all collision keys
    for (auto seed : seeds)
    {
        // Generate key from seed using SHA512-half (must match
        // find-collisions.cpp logic)
        std::uint32_t be_seed =
            __builtin_bswap32(seed);  // Convert to big-endian

        catl::crypto::Sha512HalfHasher hasher;
        hasher.update(&be_seed, sizeof(be_seed));
        auto hash = hasher.finalize();

        std::array<std::uint8_t, 32> key;
        std::memcpy(key.data(), hash.data(), 32);

        db.insert(key.data(), &seed, sizeof(seed), ec);
        ASSERT_FALSE(ec) << "Failed to insert key for seed " << seed << ": "
                         << ec.message();
    }

    LOGI("  Inserted ", seeds.size(), " keys");

    // Close to flush
    db.close(ec);
    ASSERT_FALSE(ec);

    // Phase 3: Scan .dat file and count spill records
    LOGI("=== Phase 3: Scanning .dat file for spill records ===");

    boost::iostreams::mapped_file_source dat_mmap;
    dat_mmap.open(dat_path);
    ASSERT_TRUE(dat_mmap.is_open()) << "Failed to mmap .dat file";

    // Read dat file header to get key_size
    nudbview::detail::dat_file_header dh;
    {
        nudbview::detail::istream is(dat_mmap.data(), dat_mmap.size());
        nudbview::detail::read(is, dh);
        // Note: In production we'd verify the header, but for this test we
        // trust it
    }

    auto const* data = reinterpret_cast<const std::uint8_t*>(dat_mmap.data());
    std::uint64_t file_size = dat_mmap.size();

    // Count total data records
    std::uint64_t data_record_count = nudbutil::scan_dat_records(
        dat_mmap, dh.key_size, [](std::uint64_t, std::uint64_t, std::uint64_t) {
            // Just counting, no action needed
        });

    LOGI("  Total data records: ", data_record_count);

    // Count spill records
    std::uint64_t spill_count = 0;
    std::uint64_t total_spill_bytes = 0;

    spill_count = nudbutil::count_spill_records(data, file_size, dh.key_size);

    // Also visit spill records to get more details
    nudbutil::visit_spill_records(
        data,
        file_size,
        dh.key_size,
        [&](std::uint64_t offset, std::uint16_t bucket_size) {
            LOGI(
                "  Spill record at offset ",
                offset,
                ", bucket size: ",
                bucket_size,
                " bytes");
            total_spill_bytes += bucket_size;
        });

    LOGI("  Total spill records: ", spill_count);
    LOGI("  Total spill data: ", total_spill_bytes, " bytes");

    // Verify expectations
    EXPECT_EQ(data_record_count, seeds.size())
        << "Number of data records should match number of keys inserted";

    // With load_factor=0.99 and 500 keys, we expect SOME spill records
    // The exact count depends on hash distribution, but with load_factor=0.99,
    // buckets won't split until the database is 99% full on average.
    // Some buckets will accumulate > 16 keys, forcing spills.
    if (spill_count > 0)
    {
        LOGI(
            "\n=== SUCCESS: load_factor=0.99 created ",
            spill_count,
            " spill records! ===");
    }
    else
    {
        LOGI("\n=== WARNING: No spill records created ===");
        LOGI("  This can happen if keys distributed evenly across buckets");
        LOGI(
            "  Try: 1) More keys, 2) Different salt value, 3) Collision seeds");

        // This is not necessarily a failure - spills are probabilistic
        // But log a warning so we know to investigate
        EXPECT_GT(spill_count, 0)
            << "Expected spill records with load_factor=0.99 and "
            << seeds.size()
            << " keys, but none were created. This is probabilistic - try "
               "running again.";
    }

    // Cleanup
    dat_mmap.close();
    fs::remove_all(temp_dir);
}

// Test reading from a database with spill records
TEST(SpillRecords, ReadFromDatabaseWithSpills)
{
    // Initialize logger
    Logger::set_level(LogLevel::INFO);

    LOGI("\n=== Testing fetch() from database with potential spills ===");

    // Create test database
    auto temp_dir =
        fs::temp_directory_path() / fs::unique_path("spill-read-%%%%-%%%%");
    fs::create_directories(temp_dir);

    auto dat_path = (temp_dir / "nudb.dat").string();
    auto key_path = (temp_dir / "nudb.key").string();
    auto log_path = (temp_dir / "nudb.log").string();

    // Use load_factor=0.99 for maximum chance of spills
    std::uint64_t appnum = 1;
    std::uint64_t uid = nudbview::make_uid();
    std::uint64_t salt = 42;  // Different salt for variety
    std::size_t key_size = 32;
    std::size_t block_size = 4096;
    float load_factor = 0.99f;

    nudbview::error_code ec;
    nudbview::create<nudbview::xxhasher>(
        dat_path,
        key_path,
        log_path,
        appnum,
        uid,
        salt,
        key_size,
        block_size,
        load_factor,
        ec);
    ASSERT_FALSE(ec) << "Failed to create database: " << ec.message();

    // Store keys and values for verification
    struct KeyValue
    {
        std::array<std::uint8_t, 32> key;
        std::uint32_t value;
    };
    std::vector<KeyValue> inserted_data;

    // Insert many keys
    {
        nudbview::basic_store<nudbview::xxhasher, nudbview::native_file> db;
        db.open(dat_path, key_path, log_path, ec);
        ASSERT_FALSE(ec) << "Failed to open database: " << ec.message();

        const std::uint32_t num_keys =
            1000;  // Even more keys for higher spill chance
        for (std::uint32_t i = 0; i < num_keys; ++i)
        {
            // Generate key from seed
            std::uint32_t be_seed = __builtin_bswap32(i);
            catl::crypto::Sha512HalfHasher hasher;
            hasher.update(&be_seed, sizeof(be_seed));
            auto hash = hasher.finalize();

            KeyValue kv;
            std::memcpy(kv.key.data(), hash.data(), 32);
            kv.value = i;

            db.insert(kv.key.data(), &kv.value, sizeof(kv.value), ec);
            ASSERT_FALSE(ec) << "Failed to insert key for seed " << i;

            inserted_data.push_back(kv);
        }

        LOGI("  Inserted ", num_keys, " keys");
        db.close(ec);
        ASSERT_FALSE(ec);
    }

    // Check for spill records
    {
        boost::iostreams::mapped_file_source dat_mmap;
        dat_mmap.open(dat_path);
        ASSERT_TRUE(dat_mmap.is_open());

        nudbview::detail::dat_file_header dh;
        {
            nudbview::detail::istream is(dat_mmap.data(), dat_mmap.size());
            nudbview::detail::read(is, dh);
        }

        auto const* data =
            reinterpret_cast<const std::uint8_t*>(dat_mmap.data());
        std::uint64_t file_size = dat_mmap.size();
        std::uint64_t spill_count =
            nudbutil::count_spill_records(data, file_size, dh.key_size);

        LOGI("  Database has ", spill_count, " spill records");
        dat_mmap.close();
    }

    // Now verify we can read all keys back (even with spills)
    {
        nudbview::basic_store<nudbview::xxhasher, nudbview::native_file> db;
        db.open(dat_path, key_path, log_path, ec);
        ASSERT_FALSE(ec) << "Failed to reopen database";

        LOGI("  Verifying all keys can be fetched...");

        std::size_t found_count = 0;
        for (auto const& kv : inserted_data)
        {
            bool found = false;
            std::uint32_t fetched_value = 0;

            ec.clear();
            db.fetch(
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
                    << "Value mismatch for key with seed " << kv.value;
                ++found_count;
            }
            else if (ec)
            {
                FAIL() << "Failed to fetch key with seed " << kv.value
                       << ", error: " << ec.message();
            }
        }

        LOGI("  Successfully fetched all ", found_count, " keys!");
        EXPECT_EQ(found_count, inserted_data.size());

        db.close(ec);
        ASSERT_FALSE(ec);
    }

    // Cleanup
    fs::remove_all(temp_dir);
    LOGI("\n=== SUCCESS: Database with spills works correctly! ===");
}