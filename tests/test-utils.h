#pragma once

#include <gtest/gtest.h>
#include <boost/json.hpp>
#include <boost/filesystem.hpp>
#include <fstream>
#include <sstream>
#include <string>
#include <memory>
#include <vector>
#include "catl/shamap/shamap.h"

// Define this macro to get the directory of the current source file
#define CURRENT_SOURCE_DIR std::string(__FILE__).substr(0, std::string(__FILE__).find_last_of("/\\"))

// Helper class to manage file paths relative to the source file
class TestDataPath {
public:
    static std::string getPath(const std::string &relativePath);
};

// Hex parsing helper
std::pair<std::unique_ptr<uint8_t[]>, boost::intrusive_ptr<MmapItem>> getItemFromHex(const std::string &hexString);

// JSON loading helper
boost::json::value loadJsonFromFile(const std::string &filePath);

// Test fixture for SHAMap tests
class ShaMapFixture : public ::testing::Test {
protected:
    ShaMapFixture();

    void SetUp() override;

    std::string getFixturePath(const std::string &filename);

    // Helper method to add an item from hex string
    SetResult addItemFromHex(const std::string &hexString);

    // Helper method to add an item from hex string
    bool removeItemFromHex(const std::string &hexString);

    // Member variables
    SHAMap map;
    std::vector<std::unique_ptr<uint8_t[]>> buffers;
    std::string fixtureDir;
};