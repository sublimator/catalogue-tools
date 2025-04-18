// test-utils.h
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
    static std::string get_path(const std::string &relative_path);
};

// Convert hex string to byte vector
std::vector<uint8_t> hex_to_vector(const std::string &hex_string);

// Class to manage test items and their memory
class TestItems {
private:
    // Store vectors for memory management
    std::vector<std::vector<uint8_t>> buffers;

public:
    // Create an item from hex strings
    boost::intrusive_ptr<MmapItem> make(
        const std::string &hex_string, 
        std::optional<std::string> hex_data = std::nullopt);
    
    // Helper to clear buffers if needed
    void clear();
};

// JSON loading helper
boost::json::value load_json_from_file(const std::string &file_path);

class ShaMapFixture : public ::testing::Test {
protected:
    ShaMapFixture();

    void SetUp() override;

    // Virtual methods for customization
    virtual SHAMapNodeType get_node_type();
    virtual std::optional<SHAMapOptions> get_map_options();
    virtual std::string get_fixture_directory();

    std::string get_fixture_path(const std::string &filename) const;

    SetResult add_item_from_hex(const std::string &hex_string, std::optional<std::string> hex_data = std::nullopt);
    bool remove_item_from_hex(const std::string &hex_string);

    // Member variables
    SHAMap map;
    TestItems items;
    std::string fixture_dir;
};

class TransactionFixture : public ShaMapFixture {
protected:
    SHAMapNodeType get_node_type() override {
        return tnTRANSACTION_MD;
    }
};

class AccountStateFixture : public ShaMapFixture {
protected:
    SHAMapNodeType get_node_type() override {
        return tnACCOUNT_STATE;
    }
};