#include "catl/core/logger.h"
#include "catl/shamap/shamap-nodetype.h"
#include "catl/shamap/shamap-options.h"
#include <boost/json/array.hpp>
#include <boost/json/value.hpp>
#include <boost/json/value_to.hpp>
#include <cstddef>
#include <exception>
#include <gtest/gtest.h>
#include <iostream>
#include <optional>
#include <string>

#include "shamap-test-utils.h"

using namespace catl::shamap;

// Test using the fixture with source-relative paths
TEST_F(AccountStateFixture, JsonFileOperations)
{
    try
    {
        // Get path to the test data file relative to this source file
        std::string file_path = get_fixture_path("op-adds.json");
        std::cout << "Loading JSON from: " << file_path << std::endl;

        // Load JSON from file
        boost::json::value operations = load_json_from_file(file_path);
        boost::json::array& ops = operations.as_array();

        // Apply each operation from the JSON
        for (const auto& op : ops)
        {
            auto operation = boost::json::value_to<std::string>(op.at("op"));
            auto key_hex = boost::json::value_to<std::string>(op.at("key"));
            auto expected_hash =
                boost::json::value_to<std::string>(op.at("map_hash"));

            if (operation == "add")
            {
                EXPECT_EQ(
                    add_item_from_hex(key_hex, std::nullopt), SetResult::ADD);
            }
            else if (operation == "remove")
            {
                EXPECT_EQ(remove_item_from_hex(key_hex), true);
            }
            EXPECT_EQ(map.get_hash().hex(), expected_hash)
                << "Hash mismatch after adding key: " << key_hex;

            // Add handling for other operations if needed (remove, etc.)
        }
    }
    catch (const std::exception& e)
    {
        FAIL() << "Exception: " << e.what();
    }
}

// Basic test for SHAMap functionality
TEST(ShaMapTest, BasicOperations)
{
    auto map = SHAMap(tnACCOUNT_STATE);
    EXPECT_EQ(
        map.get_hash().hex(),
        "0000000000000000000000000000000000000000000000000000000000000000");

    TestMmapItems items;
    auto item = items.make(
        "0000000000000000000000000000000000000000000000000000000000000000");
    map.set_item(item);
    EXPECT_EQ(
        map.get_hash().hex(),
        "B992A0C0480B32A2F32308EA2D64E85586A3DAF663F7B383806B5C4CEA84D8BF");
}

// Test for the add_item_only functionality
TEST(ShaMapTest, AddItemOnly)
{
    auto map = SHAMap(tnACCOUNT_STATE);
    TestMmapItems items;

    // Create two test items with different keys
    auto item1 = items.make(
        "0000000000000000000000000000000000000000000000000000000000000001");
    auto item2 = items.make(
        "0000000000000000000000000000000000000000000000000000000000000002");

    // First add should succeed
    EXPECT_EQ(map.add_item(item1), SetResult::ADD);

    // Adding it again should fail with add_item (add-only semantics)
    EXPECT_EQ(map.add_item(item1), SetResult::FAILED);

    // But adding a different item should succeed
    EXPECT_EQ(map.add_item(item2), SetResult::ADD);
}

// Test for the update_item_only functionality
TEST(ShaMapTest, UpdateItemOnly)
{
    auto map = SHAMap(tnACCOUNT_STATE);
    TestMmapItems items;

    // Create two items with the same key
    auto item1 = items.make(
        "0000000000000000000000000000000000000000000000000000000000000001");
    auto item2 = items.make(
        "0000000000000000000000000000000000000000000000000000000000000001");

    // Update should fail since the item doesn't exist yet
    EXPECT_EQ(map.update_item(item1), SetResult::FAILED);

    // Add it first
    EXPECT_EQ(map.set_item(item1), SetResult::ADD);

    // Now update should succeed
    EXPECT_EQ(map.update_item(item2), SetResult::UPDATE);
}

// Test for the set_item with different modes
TEST(ShaMapTest, SetItemModes)
{
    auto map = SHAMap(tnACCOUNT_STATE);
    TestMmapItems items;

    // Create items with the same key but different content
    auto item1 = items.make(
        "0000000000000000000000000000000000000000000000000000000000000001");
    auto item2 = items.make(
        "0000000000000000000000000000000000000000000000000000000000000001");

    // Add mode
    EXPECT_EQ(map.set_item(item1, SetMode::ADD_ONLY), SetResult::ADD);
    EXPECT_EQ(map.set_item(item2, SetMode::ADD_ONLY), SetResult::FAILED);

    // Update mode
    auto item3 = items.make(
        "0000000000000000000000000000000000000000000000000000000000000002");
    EXPECT_EQ(map.set_item(item3, SetMode::UPDATE_ONLY), SetResult::FAILED);

    // Add or update mode
    EXPECT_EQ(map.set_item(item2, SetMode::ADD_OR_UPDATE), SetResult::UPDATE);
    EXPECT_EQ(map.set_item(item3, SetMode::ADD_OR_UPDATE), SetResult::ADD);
}

// Test for adding ledger transaction data one by one
TEST_F(TransactionFixture, Ledger29952TransactionAddTest)
{
    try
    {
        // Get path to the test data file
        std::string file_path = get_fixture_path("ledger-29952-txns.json");
        std::cout << "Loading transaction data from: " << file_path
                  << std::endl;

        // Load JSON from file
        boost::json::value transactions = load_json_from_file(file_path);
        boost::json::array& txns = transactions.as_array();

        std::cout << "Found " << txns.size() << " transactions to process"
                  << std::endl;

        // Process each transaction from the JSON array
        for (size_t i = 0; i < txns.size(); ++i)
        {
            const auto& txn = txns[i];
            auto key_hex = boost::json::value_to<std::string>(txn.at("key"));
            auto data_hex = boost::json::value_to<std::string>(txn.at("data"));

            // Add item to the map
            std::cout << "Adding transaction " << (i + 1)
                      << " with key: " << key_hex << std::endl;
            if (i + 1 == 10)
            {
                Logger::set_level(LogLevel::DEBUG);
            }
            else
            {
                Logger::set_level(LogLevel::INFO);
            }

            EXPECT_EQ(add_item_from_hex(key_hex, data_hex), SetResult::ADD);

            // For additional verification, you could calculate the expected
            // hash after each addition and compare it to the actual hash, but
            // this would require precomputed hashes
            Hash256 current_hash = map.get_hash();
            std::cout << "Map hash after adding: " << current_hash.hex()
                      << std::endl;

            std::cout << "Map trie JSON: ";
            std::cout << map.trie_json_string({.key_as_hash = true})
                      << std::endl;
        }

        // Final hash check if you have an expected final hash value
        Hash256 final_hash = map.get_hash();
        std::cout << "Final map hash: " << final_hash.hex() << std::endl;
        EXPECT_EQ(
            final_hash.hex(),
            "9138DB29694D9B7F125F56FE42520CAFF3C9870F28C4161A69E0C8597664C951");
    }
    catch (const std::exception& e)
    {
        FAIL() << "Exception: " << e.what();
    }
}

TEST_F(TransactionFixture, Ledger81920TransactionAddTest)
{
    try
    {
        // Get path to the test data file
        std::string file_path = get_fixture_path("ledger-81920-txns.json");
        std::cout << "Loading transaction data from: " << file_path
                  << std::endl;

        // Load JSON from file
        boost::json::value transactions = load_json_from_file(file_path);
        boost::json::array& txns = transactions.as_array();

        std::cout << "Found " << txns.size() << " transactions to process"
                  << std::endl;

        // Process each transaction from the JSON array
        for (size_t i = 0; i < txns.size(); ++i)
        {
            const auto& txn = txns[i];
            auto key_hex = boost::json::value_to<std::string>(txn.at("key"));
            auto data_hex = boost::json::value_to<std::string>(txn.at("data"));

            size_t txn_n = i + 1;
            // Add item to the map
            std::cout << "Adding transaction " << txn_n
                      << " with key: " << key_hex << std::endl;

            EXPECT_EQ(add_item_from_hex(key_hex, data_hex), SetResult::ADD);

            // For additional verification, you could calculate the expected
            // hash after each addition and compare it to the actual hash, but
            // this would require precomputed hashes
            Hash256 current_hash = map.get_hash();
            std::cout << "Map hash after adding: " << current_hash.hex()
                      << std::endl;

            auto map_trie = map.trie_json_string({.key_as_hash = true});
            std::cout << "Map trie JSON: " << map_trie << std::endl;

            {
                // Create a map with no collapsing for comparison
                auto map_ = SHAMap(
                    tnTRANSACTION_MD,
                    {.tree_collapse_impl = TreeCollapseImpl::leafs_only});
                TestMmapItems items;  // Local items manager
                for (size_t j = 0; j < i + 1; ++j)
                {
                    const auto& txn_ = txns[j];
                    auto key_str =
                        boost::json::value_to<std::string>(txn_.at("key"));
                    auto data_str =
                        boost::json::value_to<std::string>(txn_.at("data"));
                    auto item = items.make(key_str, data_str);
                    map_.add_item(item);
                }
                map_.collapse_tree();
                auto canonical_trie =
                    map_.trie_json_string({.key_as_hash = true});
                std::cout << "Canonical Collapsed Map trie JSON: "
                          << canonical_trie << std::endl;
                // EXPECT_EQ(map_trie, canonical_trie);
            }
        }

        // Final hash check if you have an expected final hash value
        Hash256 final_hash = map.get_hash();
        std::cout << "Final map hash: " << final_hash.hex() << std::endl;
        // https://xahscan.com/ledger/81920
        EXPECT_EQ(
            final_hash.hex(),
            "39460E5964D942A0E8A7A2C4E86EEF40B6C8FDF707BDA3874BE3CEE7D917D103");
    }
    catch (const std::exception& e)
    {
        FAIL() << "Exception: " << e.what();
    }
}
