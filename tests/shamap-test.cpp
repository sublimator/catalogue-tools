#include <gtest/gtest.h>
#include <boost/json.hpp>
#include <iostream>
#include "test-utils.h"

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
                addItemFromHex(keyHex);
            } else if (operation == "remove") {
                removeItemFromHex(keyHex);
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
    map.add_item(item);
    EXPECT_EQ(map.get_hash().hex(), "B992A0C0480B32A2F32308EA2D64E85586A3DAF663F7B383806B5C4CEA84D8BF");
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}