#include <gtest/gtest.h>
#include <boost/json.hpp>
#include <boost/json/src.hpp>
#include <boost/filesystem.hpp>
#include <fstream>
#include <sstream>
#include <string>
#include "catl/shamap/shamap.h"

// Define this macro in each test file to get the directory of the current source file
#define CURRENT_SOURCE_DIR std::string(__FILE__).substr(0, std::string(__FILE__).find_last_of("/\\"))

// Helper class to manage file paths relative to the source file
class TestDataPath {
public:
    static std::string getPath(const std::string &relativePath) {
        // Get the directory of the current source file
        std::string sourceDir = CURRENT_SOURCE_DIR;

        // Combine with the relative path
        boost::filesystem::path fullPath = boost::filesystem::path(sourceDir) / relativePath;

        return fullPath.string();
    }
};

// Test fixture for SHAMap tests
class ShaMapFixture : public ::testing::Test {
protected:
    ShaMapFixture() : map(tnACCOUNT_STATE) {
    }

    void SetUp() override {
        // Test data location relative to this source file
        fixtureDir = "fixture";

        // Verify empty map hash
        EXPECT_EQ(map.get_hash().hex(), "0000000000000000000000000000000000000000000000000000000000000000");
    }

    std::string getFixturePath(const std::string &filename) {
        return TestDataPath::getPath(fixtureDir + "/" + filename);
    }

    // Helper method to add an item from hex string
    void addItemFromHex(const std::string &hexString) {
        auto [data, item] = getItemFromHex(hexString);
        map.add_item(item);
        buffers.push_back(std::move(data)); // Keep buffer alive
    }

    // Hex parsing helper
    static std::pair<std::unique_ptr<uint8_t[]>, boost::intrusive_ptr<MmapItem> > getItemFromHex(
        const std::string &hexString) {
        if (hexString.length() < 64) {
            throw std::invalid_argument("Hex string must be at least 64 characters");
        }

        // Allocate memory that will persist after the function returns
        auto keyData = std::make_unique<uint8_t[]>(32);

        // Parse hex string into bytes for the key
        for (int i = 0; i < 32; i++) {
            std::string byteStr = hexString.substr(i * 2, 2);
            keyData[i] = static_cast<uint8_t>(std::stoi(byteStr, nullptr, 16));
        }

        // Create the MmapItem using the allocated memory
        auto item = boost::intrusive_ptr<MmapItem>(new MmapItem(keyData.get(), keyData.get(), 32));

        // Return both the data buffer and the item
        return std::make_pair(std::move(keyData), item);
    }

    // JSON loading helper
    static boost::json::value loadJsonFromFile(const std::string &filePath) {
        std::ifstream file(filePath);
        if (!file.is_open()) {
            throw std::runtime_error("Could not open file: " + filePath);
        }

        std::stringstream buffer;
        buffer << file.rdbuf();
        std::string jsonStr = buffer.str();

        boost::system::error_code ec;
        boost::json::value json = boost::json::parse(jsonStr, ec);
        if (ec) {
            throw std::runtime_error("Failed to parse JSON: " + ec.message());
        }

        return json;
    }

    // Member variables
    SHAMap map;
    std::vector<std::unique_ptr<uint8_t[]> > buffers;
    std::string fixtureDir;
};

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
            }
            // Verify hash after operation
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
    std::string filePath = TestDataPath::getPath("fixture/paste.txt");
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

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
