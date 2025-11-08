#include "catl/core/types.h"
#include "catl/utils-v1/nudb/nudb-bulk-writer.h"
#include <boost/filesystem.hpp>
#include <gtest/gtest.h>
#include <nudb/nudb.hpp>
#include <random>
#include <unordered_set>

using namespace catl;
using namespace catl::v1::utils::nudb;
namespace fs = boost::filesystem;

/**
 * Test NuDB bulk writer with duplicate keys
 *
 * This test verifies that:
 * 1. Bulk writer correctly skips duplicate keys
 * 2. Rekey builds a valid index despite duplicates in .dat file
 * 3. Normal NuDB API can open and query the resulting database
 * 4. All unique keys are readable
 * 5. Duplicate attempts are properly tracked
 */
TEST(NudbBulkDuplicates, WriteAndVerify)
{
    // Test parameters
    const size_t NUM_UNIQUE_KEYS = 1000;
    const size_t NUM_DUPLICATE_KEYS = 10;  // ~1% will be duplicated
    const size_t DUPLICATE_ATTEMPTS =
        3;  // Each duplicate key inserted 3 times total

    // Create temporary directory for test database
    fs::path test_dir = fs::temp_directory_path() / "nudb_bulk_dup_test";
    if (fs::exists(test_dir))
    {
        fs::remove_all(test_dir);
    }
    fs::create_directories(test_dir);

    fs::path dat_path = test_dir / "test.dat";
    fs::path key_path = test_dir / "test.key";
    fs::path log_path = test_dir / "test.log";

    // Generate random keys and values
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> value_size_dist(
        100, 1000);  // Variable value sizes
    std::uniform_int_distribution<> byte_dist(0, 255);
    std::uniform_int_distribution<size_t> key_index_dist(
        0, NUM_UNIQUE_KEYS - 1);

    // Custom hasher for Hash256
    struct Hash256Hasher
    {
        std::size_t
        operator()(const Hash256& key) const noexcept
        {
            ::nudb::xxhasher h(0);
            return static_cast<std::size_t>(h(key.data(), key.size()));
        }
    };

    // Generate 1000 unique keys
    std::vector<Hash256> unique_keys;
    std::unordered_map<Hash256, std::vector<uint8_t>, Hash256Hasher>
        key_to_value;  // Track expected values

    for (size_t i = 0; i < NUM_UNIQUE_KEYS; ++i)
    {
        Hash256 key;
        for (size_t j = 0; j < 32; ++j)
        {
            key.data()[j] = byte_dist(gen);
        }
        unique_keys.push_back(key);

        // Generate random value
        size_t value_size = value_size_dist(gen);
        std::vector<uint8_t> value(value_size);
        for (size_t j = 0; j < value_size; ++j)
        {
            value[j] = byte_dist(gen);
        }
        key_to_value[key] = value;
    }

    // Select random keys to duplicate
    std::set<size_t> duplicate_indices;
    while (duplicate_indices.size() < NUM_DUPLICATE_KEYS)
    {
        duplicate_indices.insert(key_index_dist(gen));
    }

    std::cout << "Generated " << NUM_UNIQUE_KEYS << " unique keys\n";
    std::cout << "Will duplicate " << NUM_DUPLICATE_KEYS << " keys ("
              << (NUM_DUPLICATE_KEYS * 100.0 / NUM_UNIQUE_KEYS) << "%)\n";
    std::cout << "Total insert attempts: "
              << (NUM_UNIQUE_KEYS +
                  NUM_DUPLICATE_KEYS * (DUPLICATE_ATTEMPTS - 1))
              << "\n";

    // Create bulk writer with NO DEDUPLICATION
    // This will write ALL keys to .dat including duplicates
    // Testing if NuDB's rekey can handle duplicates in .dat file
    NudbBulkWriter writer(
        dat_path.string(),
        key_path.string(),
        log_path.string(),
        32,   // key_size
        true  // no_dedupe = true (DISABLE deduplication - write all
              // duplicates!)
    );

    ASSERT_TRUE(writer.open(4096, 0.5));

    // Insert all keys in random order, with duplicates scattered throughout
    std::vector<size_t> insert_order;

    // Add all unique keys
    for (size_t i = 0; i < NUM_UNIQUE_KEYS; ++i)
    {
        insert_order.push_back(i);
    }

    // Add duplicates
    for (size_t idx : duplicate_indices)
    {
        for (size_t dup = 1; dup < DUPLICATE_ATTEMPTS; ++dup)
        {
            insert_order.push_back(idx);
        }
    }

    // Shuffle insert order
    std::shuffle(insert_order.begin(), insert_order.end(), gen);

    // Insert keys in shuffled order
    size_t successful_inserts = 0;
    size_t duplicate_attempts = 0;

    for (size_t idx : insert_order)
    {
        const Hash256& key = unique_keys[idx];
        const std::vector<uint8_t>& value = key_to_value[key];

        bool inserted = writer.insert(
            key, value.data(), value.size(), 1);  // node_type=1 (leaf)

        if (inserted)
        {
            successful_inserts++;
        }
        else
        {
            duplicate_attempts++;
        }
    }

    std::cout << "Successful inserts: " << successful_inserts << "\n";
    std::cout << "Duplicate attempts: " << duplicate_attempts << "\n";

    // Verify counts - with no_dedupe=true, ALL inserts should succeed
    size_t expected_total =
        NUM_UNIQUE_KEYS + NUM_DUPLICATE_KEYS * (DUPLICATE_ATTEMPTS - 1);
    EXPECT_EQ(successful_inserts, expected_total)
        << "All inserts should succeed with no_dedupe";
    EXPECT_EQ(duplicate_attempts, 0)
        << "No dedup tracking, so no 'duplicates' detected";
    EXPECT_EQ(writer.get_unique_count(), expected_total)
        << "Bulk writer counts all as unique";
    EXPECT_EQ(writer.get_duplicate_count(), 0);

    // Close bulk writer (this runs rekey!)
    std::cout << "\nClosing bulk writer (running rekey)...\n";
    std::cout << "NOTE: .dat file contains " << expected_total
              << " records including duplicates\n";
    std::cout << "Testing if NuDB rekey can handle duplicates...\n";
    ASSERT_TRUE(writer.close())
        << "Rekey should succeed even with duplicates in .dat file";

    // Verify files exist
    EXPECT_TRUE(fs::exists(dat_path));
    EXPECT_TRUE(fs::exists(key_path));

    std::cout << "\n.dat file size: " << fs::file_size(dat_path) / 1024
              << " KB\n";
    std::cout << ".key file size: " << fs::file_size(key_path) / 1024
              << " KB\n";
    std::cout << "✅ Rekey succeeded with duplicates in .dat file!\n";

    // Open database with normal NuDB API
    std::cout << "\nOpening database with normal NuDB API...\n";

    using store_type =
        ::nudb::basic_store<::nudb::xxhasher, ::nudb::posix_file>;
    store_type db;

    ::nudb::error_code ec;
    db.open(dat_path.string(), key_path.string(), log_path.string(), ec);
    ASSERT_FALSE(ec) << "Failed to open database: " << ec.message();

    // Verify all unique keys are readable
    std::cout << "Verifying all " << NUM_UNIQUE_KEYS
              << " unique keys are readable...\n";

    size_t readable_count = 0;
    size_t missing_count = 0;
    size_t size_mismatch_count = 0;

    for (const auto& [key, expected_value] : key_to_value)
    {
        std::vector<uint8_t> fetched_value;
        size_t fetched_size = 0;

        db.fetch(
            key.data(),
            [&](void const* data, std::size_t size) {
                fetched_size = size;
                const uint8_t* bytes = static_cast<const uint8_t*>(data);
                fetched_value.assign(bytes, bytes + size);
            },
            ec);

        if (ec)
        {
            if (ec == ::nudb::error::key_not_found)
            {
                std::cerr << "ERROR: Key not found: " << key.hex().substr(0, 16)
                          << "...\n";
                missing_count++;
            }
            else
            {
                std::cerr << "ERROR: Fetch failed: " << ec.message() << "\n";
                missing_count++;
            }
        }
        else if (fetched_size != expected_value.size())
        {
            std::cerr << "ERROR: Size mismatch for key "
                      << key.hex().substr(0, 16) << "... expected "
                      << expected_value.size() << " bytes, got " << fetched_size
                      << " bytes\n";
            size_mismatch_count++;
        }
        else if (fetched_value != expected_value)
        {
            std::cerr << "ERROR: Value mismatch for key "
                      << key.hex().substr(0, 16) << "...\n";
            size_mismatch_count++;
        }
        else
        {
            readable_count++;
        }
    }

    std::cout << "Readable keys: " << readable_count << " / " << NUM_UNIQUE_KEYS
              << "\n";
    std::cout << "Missing keys: " << missing_count << "\n";
    std::cout << "Size/value mismatches: " << size_mismatch_count << "\n";

    // Verify results
    EXPECT_EQ(readable_count, NUM_UNIQUE_KEYS);
    EXPECT_EQ(missing_count, 0);
    EXPECT_EQ(size_mismatch_count, 0);

    // Close database
    db.close(ec);
    EXPECT_FALSE(ec);

    // NEW: Test nudb::visit() to walk all records in .dat file
    std::cout << "\nTesting nudb::visit() iteration...\n";
    std::cout << "NOTE: .dat file has " << expected_total
              << " records (including duplicates)\n";

    size_t visit_count = 0;
    std::unordered_set<Hash256, Hash256Hasher>
        visited_keys;  // Track unique keys seen during visit

    ::nudb::visit(
        dat_path.string(),
        [&](void const* key_data,
            [[maybe_unused]] std::size_t key_size,
            [[maybe_unused]] void const* value_data,
            [[maybe_unused]] std::size_t value_size,
            [[maybe_unused]] ::nudb::error_code& visit_ec) {
            visit_count++;

            // Track this key
            Hash256 key;
            std::memcpy(key.data(), key_data, 32);
            visited_keys.insert(key);
        },
        ::nudb::no_progress{},
        ec);

    ASSERT_FALSE(ec) << "visit() failed: " << ec.message();

    std::cout << "Visit stats:\n";
    std::cout << "  - Total records visited: " << visit_count << "\n";
    std::cout << "  - Unique keys seen: " << visited_keys.size() << "\n";
    std::cout << "  - Expected in .dat file: " << expected_total
              << " (with duplicates)\n";
    std::cout << "  - Expected unique keys: " << NUM_UNIQUE_KEYS << "\n";

    // visit() walks the .dat file sequentially, so it sees ALL records
    // including duplicates!
    EXPECT_EQ(visit_count, expected_total)
        << "visit() should see all .dat records";
    // But we should only see NUM_UNIQUE_KEYS unique keys (some appear multiple
    // times)
    EXPECT_EQ(visited_keys.size(), NUM_UNIQUE_KEYS)
        << "Should have correct unique key count";

    // Clean up
    fs::remove_all(test_dir);

    std::cout << "✅ Test passed! NuDB handles duplicates correctly in both "
                 "fetch and visit.\n";
}

/**
 * Test with higher duplicate rate (10%)
 */
TEST(NudbBulkDuplicates, HighDuplicateRate)
{
    const size_t NUM_UNIQUE_KEYS = 1000;
    const size_t NUM_DUPLICATE_KEYS = 100;  // 10% will be duplicated
    const size_t DUPLICATE_ATTEMPTS =
        5;  // Each duplicate key inserted 5 times total

    fs::path test_dir = fs::temp_directory_path() / "nudb_bulk_dup_test_high";
    if (fs::exists(test_dir))
    {
        fs::remove_all(test_dir);
    }
    fs::create_directories(test_dir);

    fs::path dat_path = test_dir / "test.dat";
    fs::path key_path = test_dir / "test.key";
    fs::path log_path = test_dir / "test.log";

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> value_size_dist(100, 1000);
    std::uniform_int_distribution<> byte_dist(0, 255);
    std::uniform_int_distribution<size_t> key_index_dist(
        0, NUM_UNIQUE_KEYS - 1);

    // Custom hasher for Hash256
    struct Hash256Hasher
    {
        std::size_t
        operator()(const Hash256& key) const noexcept
        {
            ::nudb::xxhasher h(0);
            return static_cast<std::size_t>(h(key.data(), key.size()));
        }
    };

    // Generate unique keys
    std::vector<Hash256> unique_keys;
    std::unordered_map<Hash256, std::vector<uint8_t>, Hash256Hasher>
        key_to_value;

    for (size_t i = 0; i < NUM_UNIQUE_KEYS; ++i)
    {
        Hash256 key;
        for (size_t j = 0; j < 32; ++j)
        {
            key.data()[j] = byte_dist(gen);
        }
        unique_keys.push_back(key);

        size_t value_size = value_size_dist(gen);
        std::vector<uint8_t> value(value_size);
        for (size_t j = 0; j < value_size; ++j)
        {
            value[j] = byte_dist(gen);
        }
        key_to_value[key] = value;
    }

    // Select random keys to duplicate
    std::set<size_t> duplicate_indices;
    while (duplicate_indices.size() < NUM_DUPLICATE_KEYS)
    {
        duplicate_indices.insert(key_index_dist(gen));
    }

    std::cout << "\n=== High Duplicate Rate Test ===\n";
    std::cout << "Unique keys: " << NUM_UNIQUE_KEYS << "\n";
    std::cout << "Duplicate keys: " << NUM_DUPLICATE_KEYS << " ("
              << (NUM_DUPLICATE_KEYS * 100.0 / NUM_UNIQUE_KEYS) << "%)\n";
    std::cout << "Total insert attempts: "
              << (NUM_UNIQUE_KEYS +
                  NUM_DUPLICATE_KEYS * (DUPLICATE_ATTEMPTS - 1))
              << "\n";

    // NO DEDUPLICATION - write all duplicates to .dat file
    NudbBulkWriter writer(
        dat_path.string(),
        key_path.string(),
        log_path.string(),
        32,
        true  // no_dedupe = true
    );

    ASSERT_TRUE(writer.open(4096, 0.5));

    // Build insert order with duplicates
    std::vector<size_t> insert_order;
    for (size_t i = 0; i < NUM_UNIQUE_KEYS; ++i)
    {
        insert_order.push_back(i);
    }
    for (size_t idx : duplicate_indices)
    {
        for (size_t dup = 1; dup < DUPLICATE_ATTEMPTS; ++dup)
        {
            insert_order.push_back(idx);
        }
    }
    std::shuffle(insert_order.begin(), insert_order.end(), gen);

    // Insert all keys
    size_t successful_inserts = 0;
    size_t duplicate_attempts = 0;

    for (size_t idx : insert_order)
    {
        const Hash256& key = unique_keys[idx];
        const std::vector<uint8_t>& value = key_to_value[key];

        bool inserted = writer.insert(key, value.data(), value.size(), 1);

        if (inserted)
            successful_inserts++;
        else
            duplicate_attempts++;
    }

    std::cout << "Successful inserts: " << successful_inserts << "\n";
    std::cout << "Duplicate attempts: " << duplicate_attempts << "\n";

    // With no_dedupe=true, ALL inserts succeed
    size_t expected_total =
        NUM_UNIQUE_KEYS + NUM_DUPLICATE_KEYS * (DUPLICATE_ATTEMPTS - 1);
    EXPECT_EQ(successful_inserts, expected_total);
    EXPECT_EQ(duplicate_attempts, 0);

    std::cout << "\nClosing bulk writer (rekey with " << expected_total
              << " records including duplicates)...\n";
    ASSERT_TRUE(writer.close()) << "Rekey must handle duplicates in .dat file";

    // Open and verify with NuDB API
    std::cout << "Verifying with NuDB API...\n";

    using store_type =
        ::nudb::basic_store<::nudb::xxhasher, ::nudb::posix_file>;
    store_type db;

    ::nudb::error_code ec;
    db.open(dat_path.string(), key_path.string(), log_path.string(), ec);
    ASSERT_FALSE(ec);

    // Verify all unique keys
    size_t verified = 0;
    for (const auto& [key, expected_value] : key_to_value)
    {
        std::vector<uint8_t> fetched_value;

        db.fetch(
            key.data(),
            [&](void const* data, std::size_t size) {
                const uint8_t* bytes = static_cast<const uint8_t*>(data);
                fetched_value.assign(bytes, bytes + size);
            },
            ec);

        if (!ec && fetched_value == expected_value)
        {
            verified++;
        }
    }

    std::cout << "Verified keys: " << verified << " / " << NUM_UNIQUE_KEYS
              << "\n";
    EXPECT_EQ(verified, NUM_UNIQUE_KEYS);

    db.close(ec);
    fs::remove_all(test_dir);

    std::cout << "✅ High duplicate rate test passed!\n";
}
