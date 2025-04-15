#include <gtest/gtest.h>
#include <boost/json.hpp>
#include <iostream>
#include "test-utils.h"
#include "catl/core/logger.h"

// Test using the fixture with source-relative paths
TEST_F(ShaMapFixture, JsonFileOperations) {
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
                EXPECT_EQ(addItemFromHex(keyHex), SetResult::ADD);
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
    auto map = SHAMap(tnTRANSACTION_MD);

    // Add a series of items that will create a specific structure
    // Force collisions to create deeper structures first
    auto [data1, item1] = getItemFromHex("0000000000000000000000000000000000000000000000000000000000000100");
    auto [data2, item2] = getItemFromHex("0000000000000000000000000000000000000000000000000000000000000101");


    map.add_item(item1);
    map.add_item(item2);
}


int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}