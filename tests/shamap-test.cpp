#include <gtest/gtest.h>
#include <boost/json.hpp>
#include <iostream>
#include <utility>
#include "test-utils.h"
#include "../src/shamap/src/pretty-print-json.h"
#include "catl/core/logger.h"
#include "catl/shamap/shamap-impl.h"

// Test using the fixture with source-relative paths
TEST_F(AccountStateFixture, JsonFileOperations) {
    try {
        // Get path to the test data file relative to this source file
        std::string filePath = getFixturePath("op-adds.json");
        std::cout << "Loading JSON from: " << filePath << std::endl;

        // Load JSON from file
        boost::json::value operations = loadJsonFromFile(filePath);
        boost::json::array &ops = operations.as_array();

        // Apply each operation from the JSON
        for (const auto &op: ops) {
            auto operation = boost::json::value_to<std::string>(op.at("op"));
            auto keyHex = boost::json::value_to<std::string>(op.at("key"));
            auto expectedHash = boost::json::value_to<std::string>(op.at("map_hash"));

            if (operation == "add") {
                EXPECT_EQ(addItemFromHex(keyHex,std::nullopt), SetResult::ADD);
            } else if (operation == "remove") {
                EXPECT_EQ(removeItemFromHex(keyHex), true);
            }
            EXPECT_EQ(map.get_hash().hex(), expectedHash)
                << "Hash mismatch after adding key: " << keyHex;

            // Add handling for other operations if needed (remove, etc.)
        }
    } catch (const std::exception &e) {
        FAIL() << "Exception: " << e.what();
    }
}

// Simple test to verify our path resolution works
TEST(FilePathTest, FindTestDataFile) {
    // Get the path to the test data file relative to this source file
    std::string filePath = TestDataPath::getPath("fixture/op-adds.json");
    std::cout << "Test data path: " << filePath << std::endl;

    // Verify the file exists
    std::ifstream file(filePath);
    EXPECT_TRUE(file.good()) << "Could not find test data file at: " << filePath
        << "\nMake sure to create a 'fixture' directory next to this source file.";
}

// This will print the current source directory for debugging
TEST(FilePathTest, PrintSourceDirectory) {
    std::cout << "Current source directory: " << CURRENT_SOURCE_DIR << std::endl;
}

// Basic test for SHAMap functionality
TEST(ShaMapTest, BasicOperations) {
    auto map = SHAMap(tnACCOUNT_STATE);
    EXPECT_EQ(map.get_hash().hex(), "0000000000000000000000000000000000000000000000000000000000000000");

    auto [data, item] = getItemFromHex("0000000000000000000000000000000000000000000000000000000000000000");
    map.set_item(item);
    EXPECT_EQ(map.get_hash().hex(), "B992A0C0480B32A2F32308EA2D64E85586A3DAF663F7B383806B5C4CEA84D8BF");
}

// Test for the add_item_only functionality
TEST(ShaMapTest, AddItemOnly) {
    auto map = SHAMap(tnACCOUNT_STATE);

    // Create two test items with different keys
    auto [data1, item1] = getItemFromHex("0000000000000000000000000000000000000000000000000000000000000001");
    auto [data2, item2] = getItemFromHex("0000000000000000000000000000000000000000000000000000000000000002");

    // First add should succeed
    EXPECT_EQ(map.add_item(item1), SetResult::ADD);

    // Adding it again should fail with add_item (add-only semantics)
    EXPECT_EQ(map.add_item(item1), SetResult::FAILED);

    // But adding a different item should succeed
    EXPECT_EQ(map.add_item(item2), SetResult::ADD);
}

// Test for the update_item_only functionality
TEST(ShaMapTest, UpdateItemOnly) {
    auto map = SHAMap(tnACCOUNT_STATE);

    // Create two items with the same key
    auto [data1, item1] = getItemFromHex("0000000000000000000000000000000000000000000000000000000000000001");
    auto [data2, item2] = getItemFromHex("0000000000000000000000000000000000000000000000000000000000000001");

    // Update should fail since the item doesn't exist yet
    EXPECT_EQ(map.update_item(item1), SetResult::FAILED);

    // Add it first
    EXPECT_EQ(map.set_item(item1), SetResult::ADD);

    // Now update should succeed
    EXPECT_EQ(map.update_item(item2), SetResult::UPDATE);
}

// Test for the set_item with different modes
TEST(ShaMapTest, SetItemModes) {
    auto map = SHAMap(tnACCOUNT_STATE);

    // Create items with the same key but different content
    auto [data1, item1] = getItemFromHex("0000000000000000000000000000000000000000000000000000000000000001");
    auto [data2, item2] = getItemFromHex("0000000000000000000000000000000000000000000000000000000000000001");

    // Add mode
    EXPECT_EQ(map.set_item(item1, SetMode::ADD_ONLY), SetResult::ADD);
    EXPECT_EQ(map.set_item(item2, SetMode::ADD_ONLY), SetResult::FAILED);

    // Update mode
    auto [data3, item3] = getItemFromHex("0000000000000000000000000000000000000000000000000000000000000002");
    EXPECT_EQ(map.set_item(item3, SetMode::UPDATE_ONLY), SetResult::FAILED);

    // Add or update mode
    EXPECT_EQ(map.set_item(item2, SetMode::ADD_OR_UPDATE), SetResult::UPDATE);
    EXPECT_EQ(map.set_item(item3, SetMode::ADD_OR_UPDATE), SetResult::ADD);
}

// Test for node collapsing behavior, particularly with shallow trees
TEST(ShaMapTest, CollapsePathWithSkips) {
    // Logger::setLevel(LogLevel::DEBUG);
    // Create a transaction-like tree (shallow)
    // Add a series of items that will create a specific structure
    // Force collisions to create deeper structures first
    // Keepalive the data

    std::vector<std::shared_ptr<uint8_t[]> > buffers;
    auto get_item = [&buffers](const std::string &hexString, std::optional<std::string> hexData = std::nullopt) {
        auto [data, item] = getItemFromHex(hexString, std::move(hexData));
        std::ranges::copy(data, std::back_inserter(buffers));
        return item;
    };

    auto item1 = get_item("0000000000000000000000000000000000000000000000000000000000010000");
    auto item2 = get_item("0000000000000000000000000000000000000000000000000000000000010100");
    auto item3 = get_item("0000000000500000000000000000000000000000000000000000000000010100");
    auto item4 = get_item("0000000000600000000000000000000000000000000000000000000000010100");

    auto dump_json = [](const SHAMap &map) {
        std::cout << map.trie_json_string({.key_as_hash = true}) << std::endl;
    };

    {
        auto do_collapse = true;
        auto map = SHAMap(tnTRANSACTION_MD, {
                              .tree_collapse_impl = do_collapse
                                                        ? TreeCollapseImpl::leafs_and_inners
                                                        : TreeCollapseImpl::leafs_only
                          });


        auto add_item = [&map, do_collapse, &dump_json](boost::intrusive_ptr<MmapItem> &item) {
            map.add_item(item);
            if (do_collapse) {
                dump_json(map);
            }
        };

        add_item(item1);
        add_item(item2);
        Logger::set_level(LogLevel::DEBUG);
        add_item(item3);
        Logger::set_level(LogLevel::INFO);
        add_item(item4);
        // dump_json(map);
    }

    // {
    //     auto map = SHAMap(tnTRANSACTION_MD, options);
    //     map.add_item(item1);
    //     auto snapshot = map.snapshot();
    //     snapshot->add_item(item2);
    //     auto hash = snapshot->get_hash();
    //     EXPECT_EQ(hash.hex(), "C11AECD806E48EFF26D1A036B3EC6428C7C727895331135E44322F506616ADB5");
    // }
}


// Test for adding ledger transaction data one by one
TEST_F(TransactionFixture, Ledger29952TransactionAddTest) {
    try {
        // Get path to the test data file
        std::string filePath = getFixturePath("ledger-29952-txns.json");
        std::cout << "Loading transaction data from: " << filePath << std::endl;

        // Load JSON from file
        boost::json::value transactions = loadJsonFromFile(filePath);
        boost::json::array &txns = transactions.as_array();

        std::cout << "Found " << txns.size() << " transactions to process" << std::endl;

        // Process each transaction from the JSON array
        for (size_t i = 0; i < txns.size(); ++i) {
            const auto &txn = txns[i];
            auto keyHex = boost::json::value_to<std::string>(txn.at("key"));
            auto dataHex = boost::json::value_to<std::string>(txn.at("data"));

            // Add item to the map
            std::cout << "Adding transaction " << (i + 1) << " with key: " << keyHex << std::endl;
            if (i + 1 == 10) {
                Logger::set_level(LogLevel::DEBUG);
            } else {
                Logger::set_level(LogLevel::INFO);
            }

            EXPECT_EQ(addItemFromHex(keyHex,dataHex), SetResult::ADD);


            // For additional verification, you could calculate the expected hash after each addition
            // and compare it to the actual hash, but this would require precomputed hashes
            Hash256 currentHash = map.get_hash();
            std::cout << "Map hash after adding: " << currentHash.hex() << std::endl;

            // map.collapse_tree();


            std::cout << "Map trie JSON: ";
            map.trie_json(std::cout);
            std::cout << std::endl;
        }


        // Final hash check if you have an expected final hash value
        Hash256 finalHash = map.get_hash();
        std::cout << "Final map hash: " << finalHash.hex() << std::endl;
        EXPECT_EQ(finalHash.hex(), "9138DB29694D9B7F125F56FE42520CAFF3C9870F28C4161A69E0C8597664C951");
    } catch (const std::exception &e) {
        FAIL() << "Exception: " << e.what();
    }
}

TEST_F(TransactionFixture, Ledger81920TransactionAddTest) {
    try {
        // Get path to the test data file
        std::string filePath = getFixturePath("ledger-81920-txns.json");
        std::cout << "Loading transaction data from: " << filePath << std::endl;

        // Load JSON from file
        boost::json::value transactions = loadJsonFromFile(filePath);
        boost::json::array &txns = transactions.as_array();

        std::cout << "Found " << txns.size() << " transactions to process" << std::endl;

        // Process each transaction from the JSON array
        for (size_t i = 0; i < txns.size(); ++i) {
            const auto &txn = txns[i];
            auto keyHex = boost::json::value_to<std::string>(txn.at("key"));
            auto dataHex = boost::json::value_to<std::string>(txn.at("data"));

            size_t txn_n = i + 1;
            // Add item to the map
            std::cout << "Adding transaction " << txn_n << " with key: " << keyHex << std::endl;
            // if (txn_n >= 4 && txn_n <= 7) {
            //     Logger::set_level(LogLevel::DEBUG);
            // } else {
            //     Logger::set_level(LogLevel::INFO);
            // }

            EXPECT_EQ(addItemFromHex(keyHex,dataHex), SetResult::ADD);

            // map.collapse_tree();


            // For additional verification, you could calculate the expected hash after each addition
            // and compare it to the actual hash, but this would require precomputed hashes
            Hash256 currentHash = map.get_hash();
            std::cout << "Map hash after adding: " << currentHash.hex() << std::endl;

            std::cout << "Map trie JSON: ";
            map.trie_json(std::cout);
            std::cout << std::endl; {
                auto map_ = SHAMap(tnTRANSACTION_MD, {.tree_collapse_impl = TreeCollapseImpl::leafs_only});
                std::vector<std::shared_ptr<uint8_t[]> > buffers; // Keep alive
                for (size_t j = 0; j < i + 1; ++j) {
                    const auto &txn_ = txns[j];
                    auto key_str = boost::json::value_to<std::string>(txn_.at("key"));
                    auto data_str = boost::json::value_to<std::string>(txn_.at("data"));
                    auto [data, item] = getItemFromHex(key_str, data_str);
                    std::ranges::copy(data, std::back_inserter(buffers));
                    map_.add_item(item);
                }
                map_.collapse_tree();
                std::cout << "Canonical Collapsed Map trie JSON: ";
                map_.trie_json(std::cout, {.key_as_hash = true});
                std::cout << std::endl;
            }
        }


        // Final hash check if you have an expected final hash value
        Hash256 finalHash = map.get_hash();
        std::cout << "Final map hash: " << finalHash.hex() << std::endl;
        EXPECT_EQ(finalHash.hex(), "39460E5964D942A0E8A7A2C4E86EEF40B6C8FDF707BDA3874BE3CEE7D917D103");
    } catch (const std::exception &e) {
        FAIL() << "Exception: " << e.what();
    }
}


int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
