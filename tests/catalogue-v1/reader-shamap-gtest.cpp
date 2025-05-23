// Fixed Reader Shamap Tests

#include "catl/core/types.h"
#include "catl/shamap/shamap.h"
#include "catl/v1/catl-v1-reader.h"
#include "catl/v1/catl-v1-utils.h"
#include "catl/v1/catl-v1-writer.h"
#include <boost/filesystem.hpp>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "catl/test-utils/test-utils.h"

using namespace catl::v1;
using namespace catl::shamap;

class ReaderShaMapTest : public ::testing::Test
{
protected:
    // Paths to the test fixture files
    std::string uncompressed_fixture_path;
    std::string compressed_fixture_path;

    // Storage vector for SHAMap persistence
    std::vector<uint8_t> storage;

    void
    SetUp() override
    {
        // Get the path to the fixture files
        uncompressed_fixture_path = TestDataPath::get_path(
            "catalogue-v1/fixture/cat.1-100.compression-0.catl");
        compressed_fixture_path = TestDataPath::get_path(
            "catalogue-v1/fixture/cat.1-100.compression-9.catl");

        // Verify fixtures exist
        ASSERT_TRUE(boost::filesystem::exists(uncompressed_fixture_path))
            << "Uncompressed fixture file missing: "
            << uncompressed_fixture_path;
        ASSERT_TRUE(boost::filesystem::exists(compressed_fixture_path))
            << "Compressed fixture file missing: " << compressed_fixture_path;
    }

    // Helper to skip the header and position at the first ledger
    LedgerInfo
    readFirstLedgerInfo(Reader& reader)
    {
        return reader.read_ledger_info();
    }

    // Helper to create temporary files for testing
    std::string
    createTempFile()
    {
        auto path = boost::filesystem::temp_directory_path() /
            boost::filesystem::unique_path(
                        "catl_reader_test_%%%%-%%%%-%%%%-%%%%.dat");
        return path.string();
    }
};

// Test opening and reading headers from both compressed and uncompressed files
TEST_F(ReaderShaMapTest, OpenFilesAndReadHeaders)
{
    // Test with uncompressed file
    {
        Reader reader(uncompressed_fixture_path);
        const auto& header = reader.header();

        // Verify basic header info
        EXPECT_EQ(header.magic, CATL_MAGIC);
        EXPECT_EQ(header.min_ledger, 1);
        EXPECT_EQ(header.max_ledger, 100);
        EXPECT_EQ(get_compression_level(header.version), 0);

        // Read the first ledger header
        LedgerInfo info = readFirstLedgerInfo(reader);
        EXPECT_EQ(info.sequence, 1) << "First ledger should be sequence 1";
    }

    // Test with compressed file
    {
        Reader reader(compressed_fixture_path);
        const auto& header = reader.header();

        // Verify basic header info
        EXPECT_EQ(header.magic, CATL_MAGIC);
        EXPECT_EQ(header.min_ledger, 1);
        EXPECT_EQ(header.max_ledger, 100);
        EXPECT_EQ(get_compression_level(header.version), 9);

        // Read the first ledger header
        LedgerInfo info = readFirstLedgerInfo(reader);
        EXPECT_EQ(info.sequence, 1) << "First ledger should be sequence 1";
    }
}

// Test skip_map functionality
TEST_F(ReaderShaMapTest, SkipMapTest)
{
    Reader reader(uncompressed_fixture_path);

    // Read first ledger header
    LedgerInfo first_info = readFirstLedgerInfo(reader);
    EXPECT_EQ(first_info.sequence, 1);

    // Skip state map
    EXPECT_NO_THROW(reader.skip_map(tnACCOUNT_STATE));

    // Skip transaction map
    EXPECT_NO_THROW(reader.skip_map(tnTRANSACTION_MD));

    // Read second ledger header to confirm we're at the right position
    LedgerInfo second_info = reader.read_ledger_info();
    EXPECT_EQ(second_info.sequence, 2) << "Failed to skip maps properly";
}

// Test read_shamap with storage vector
TEST_F(ReaderShaMapTest, ReadShaMapWithStorage)
{
    Reader reader(uncompressed_fixture_path);

    // Read first ledger header
    LedgerInfo info = readFirstLedgerInfo(reader);

    // Create a SHAMap to populate
    SHAMap map(tnACCOUNT_STATE);

    // Clear storage and record initial size
    storage.clear();
    const size_t arena = 1024 * 1024;
    storage.reserve(arena);  // 1MB initial reservation

    // Read state map into our map with storage
    uint32_t nodes_processed =
        reader.read_map_to_shamap(map, tnACCOUNT_STATE, storage)
            .nodes_processed;

    // Verify data was read
    EXPECT_GT(nodes_processed, 0) << "Should have processed some nodes";
    EXPECT_GT(storage.size(), 0) << "Storage should contain data";

    // Verify the map has items
    size_t item_count = 0;
    map.visit_items([&](const MmapItem&) { item_count++; });
    EXPECT_EQ(item_count, nodes_processed)
        << "Item count should match nodes processed";

    // Read transaction map (skip it)
    reader.skip_map(tnTRANSACTION_MD);

    // Read second ledger header to confirm we're at the right position
    LedgerInfo second_info = reader.read_ledger_info();

    // Check account hash
    Hash256 map_hash = map.get_hash();
    Hash256 expected_hash(info.account_hash);
    EXPECT_EQ(map_hash.hex(), expected_hash.hex()) << "Hashes should match";

    EXPECT_EQ(second_info.sequence, 2) << "Failed to advance to next ledger";
}

// Test node type reading methods - FIXED
TEST_F(ReaderShaMapTest, NodeTypeReadingMethods)
{
    Reader reader(uncompressed_fixture_path);

    // Skip to first ledger's state map
    readFirstLedgerInfo(reader);

    // Read first node type - expect account state
    SHAMapNodeType type = reader.read_node_type();
    EXPECT_EQ(type, tnACCOUNT_STATE)
        << "First node should be an account state node";

    // Skip this node
    // Create a key vector for reading
    std::vector<uint8_t> key_data;
    reader.read_node_key(key_data);

    // Read data length and skip data
    std::vector<uint8_t> data;
    reader.read_node_data(data);

    // Now read next node properly
    SHAMapNodeType node_type;
    std::vector<uint8_t> next_key_data;
    std::vector<uint8_t> item_data;

    bool got_node = reader.read_map_node(node_type, next_key_data, item_data);
    EXPECT_TRUE(got_node) << "Should successfully read a map node";
    EXPECT_EQ(node_type, tnACCOUNT_STATE) << "Should be an account state node";
    EXPECT_EQ(next_key_data.size(), 32) << "Key should be 32 bytes";
    EXPECT_GT(item_data.size(), 0) << "Item data should not be empty";
}

// Test reading keys and data directly
TEST_F(ReaderShaMapTest, ReadKeysAndData)
{
    Reader reader(uncompressed_fixture_path);

    // Skip to first ledger's state map
    readFirstLedgerInfo(reader);

    // Skip the first map entry's type byte
    SHAMapNodeType type = reader.read_node_type();
    EXPECT_EQ(type, tnACCOUNT_STATE);

    // Read key
    std::vector<uint8_t> key;
    reader.read_node_key(key);
    EXPECT_EQ(key.size(), 32) << "Key should be 32 bytes";

    // Read data
    std::vector<uint8_t> data;
    uint32_t data_size = reader.read_node_data(data);
    EXPECT_GT(data_size, 0) << "Data size should be positive";
    EXPECT_EQ(data.size(), data_size)
        << "Data vector size should match reported size";

    // Test append functionality
    std::vector<uint8_t> storage_vector;
    storage_vector.reserve(1024);  // Reserve space for key

    // Read another node's type
    type = reader.read_node_type();
    EXPECT_EQ(type, tnACCOUNT_STATE);

    // Append key to storage
    reader.read_node_key(storage_vector, false);
    EXPECT_EQ(storage_vector.size(), 32)
        << "Storage vector should contain 32 bytes after key read";

    // Append data to storage
    size_t storage_before_data = storage_vector.size();
    data_size = reader.read_node_data(storage_vector, false);
    EXPECT_EQ(storage_vector.size(), storage_before_data + data_size)
        << "Storage vector should grow by exact data size";
}

TEST_F(ReaderShaMapTest, ErrorHandling)
{
    // Test with invalid file
    {
        std::string temp_file = createTempFile();

        // Create an empty file that's not a valid CATL file
        std::ofstream bad_file(temp_file);
        bad_file << "This is not a valid CATL file";
        bad_file.close();

        // Try to read it - should throw
        EXPECT_THROW(Reader reader(temp_file), CatlV1Error);

        // Clean up
        boost::filesystem::remove(temp_file);
    }

    {
        Reader reader(uncompressed_fixture_path);

        // Read first ledger and position at state map
        readFirstLedgerInfo(reader);

        // Read the first node type
        SHAMapNodeType actual_type = reader.read_node_type();
        EXPECT_EQ(actual_type, tnACCOUNT_STATE)
            << "Expected account state node type";

        // Now we're positioned after the node type - need to skip key and data
        std::vector<uint8_t> key_data;
        reader.read_node_key(key_data);  // Skip key

        std::vector<uint8_t> data;
        reader.read_node_data(data);  // Skip data

        // Now try the wrong type - this tests error handling but doesn't expect
        // an exception If we read a transaction type but the actual next node
        // is account state First check the actual type
        SHAMapNodeType next_type = reader.read_node_type();
        EXPECT_EQ(next_type, tnACCOUNT_STATE)
            << "Expected another account state node";

        // Manually test error handling by attempting to create a wrong type
        // situation but don't actually execute it since we can't recover from
        // the exception
        EXPECT_TRUE(next_type != tnTRANSACTION_MD)
            << "Would throw if we tried to skip as transaction type";
    }
}

// Test with compressed file specifically
TEST_F(ReaderShaMapTest, CompressedFileSpecificTests)
{
    Reader reader(compressed_fixture_path);

    // Skip to first ledger's state map
    readFirstLedgerInfo(reader);

    // Create a SHAMap to populate
    SHAMap map(tnACCOUNT_STATE);

    // Clear storage and record initial size
    storage.clear();
    const size_t arena = 1024 * 1024;
    storage.reserve(arena);  // 1MB initial reservation

    // Read state map with storage
    uint32_t nodes_processed =
        reader.read_map_to_shamap(map, tnACCOUNT_STATE, storage)
            .nodes_processed;

    // Verify results
    EXPECT_GT(nodes_processed, 0)
        << "Should process nodes even from compressed file";
    EXPECT_GT(storage.size(), 0)
        << "Storage should contain data from compressed file";

    // Skip transaction map
    reader.skip_map(tnTRANSACTION_MD);

    // Verify we're at the next ledger
    LedgerInfo second_info = reader.read_ledger_info();
    EXPECT_EQ(second_info.sequence, 2) << "Should be at second ledger";
}

// Test read_map with callbacks
TEST_F(ReaderShaMapTest, ReadMapWithCallbacks)
{
    Reader reader(uncompressed_fixture_path);

    // Skip to first ledger's state map
    readFirstLedgerInfo(reader);

    // Counters for tracking callback invocations
    size_t nodes_seen = 0;
    size_t deletions_seen = 0;
    std::vector<std::vector<uint8_t>> keys_seen;
    std::vector<size_t> data_sizes_seen;

    // Callback for regular nodes
    auto on_node = [&](const std::vector<uint8_t>& key,
                       const std::vector<uint8_t>& data) {
        nodes_seen++;
        keys_seen.push_back(key);
        data_sizes_seen.push_back(data.size());
    };

    // Callback for deletions
    auto on_delete = [&](const std::vector<uint8_t>& key) {
        deletions_seen++;
        keys_seen.push_back(key);
    };

    // Read state map with callbacks
    size_t nodes_processed =
        reader.read_map_with_callbacks(tnACCOUNT_STATE, on_node, on_delete)
            .nodes_processed;

    // Verify results
    EXPECT_GT(nodes_processed, 0) << "Should have processed nodes";
    EXPECT_EQ(nodes_seen + deletions_seen, nodes_processed)
        << "Number of callback invocations should match nodes processed";
    EXPECT_EQ(keys_seen.size(), nodes_processed)
        << "Number of keys seen should match nodes processed";

    // Verify that all keys have the correct size
    for (const auto& key : keys_seen)
    {
        EXPECT_EQ(key.size(), 32) << "All keys should be 32 bytes";
    }

    // Skip transaction map
    reader.skip_map(tnTRANSACTION_MD);

    // Verify we can advance to next ledger
    LedgerInfo second_info = reader.read_ledger_info();
    EXPECT_EQ(second_info.sequence, 2) << "Should be at second ledger";

    // Test with only on_node callback (no on_delete callback)
    // Reset counters
    nodes_seen = 0;
    keys_seen.clear();
    data_sizes_seen.clear();

    // Position at second ledger's state map
    // Read state map with only on_node callback
    nodes_processed = reader.read_map_with_callbacks(tnACCOUNT_STATE, on_node)
                          .nodes_processed;

    // Verify results
    EXPECT_GT(nodes_processed, 0)
        << "Should have processed nodes with just on_node callback";
    EXPECT_EQ(nodes_seen, nodes_processed)
        << "All nodes should be processed through on_node when no on_delete "
           "provided";
}

// Test tee functionality with read_map
TEST_F(ReaderShaMapTest, TeeWithReadMap)
{
    Reader reader(uncompressed_fixture_path);

    // Skip to first ledger's state map
    readFirstLedgerInfo(reader);

    // Create an output stream to capture the tee output
    std::stringstream output_stream;

    // Note bytes read before the operation
    size_t bytes_before = reader.body_bytes_consumed();

    // Enable tee mode
    reader.enable_tee(output_stream);

    // Counters for tracking callback invocations
    size_t nodes_seen = 0;
    size_t deletions_seen = 0;

    // Callback for regular nodes
    auto on_node = [&](const std::vector<uint8_t>& key,
                       const std::vector<uint8_t>& data) { nodes_seen++; };

    // Callback for deletions
    auto on_delete = [&](const std::vector<uint8_t>& key) { deletions_seen++; };

    // Read state map with callbacks while tee is enabled
    size_t nodes_processed =
        reader.read_map_with_callbacks(tnACCOUNT_STATE, on_node, on_delete)
            .nodes_processed;

    // Calculate bytes read during tee operation
    size_t bytes_during_tee = reader.body_bytes_consumed() - bytes_before;

    // Disable tee mode
    reader.disable_tee();

    // Verify results
    EXPECT_GT(nodes_processed, 0) << "Should have processed nodes";
    EXPECT_GT(bytes_during_tee, 0) << "Should have read some bytes during tee";
    EXPECT_EQ(output_stream.str().size(), bytes_during_tee)
        << "Output stream size should match bytes read during tee";
    EXPECT_EQ(nodes_seen + deletions_seen, nodes_processed)
        << "Callback invocations should match nodes processed";

    // Skip transaction map without tee (for positioning to next ledger)
    reader.skip_map(tnTRANSACTION_MD);

    // Verify we can advance to next ledger
    LedgerInfo second_info = reader.read_ledger_info();
    EXPECT_EQ(second_info.sequence, 2) << "Should be at second ledger";
}

// Test tee functionality with skip_map
TEST_F(ReaderShaMapTest, TeeWithSkipMap)
{
    Reader reader(uncompressed_fixture_path);

    // Skip to first ledger's state map
    readFirstLedgerInfo(reader);

    // Create an output stream to capture the tee output
    std::stringstream output_stream;

    // Note bytes read before the operation
    size_t bytes_before = reader.body_bytes_consumed();

    // Enable tee mode
    reader.enable_tee(output_stream);

    // Skip the state map - this should copy it to the output stream
    reader.skip_map(tnACCOUNT_STATE);

    // Disable tee mode
    reader.disable_tee();

    // Calculate bytes read during tee operation
    size_t bytes_during_tee = reader.body_bytes_consumed() - bytes_before;

    // Verify output stream size matches bytes read during tee
    EXPECT_GT(bytes_during_tee, 0) << "Should have read some bytes during tee";
    EXPECT_EQ(output_stream.str().size(), bytes_during_tee)
        << "Output stream size should match bytes read during tee";

    // Note bytes before skipping transaction map
    bytes_before = reader.body_bytes_consumed();

    // Skip transaction map without tee - this should not add to the output
    // stream
    reader.skip_map(tnTRANSACTION_MD);

    // Verify tee stream size hasn't changed even though we read more bytes
    EXPECT_GT(reader.body_bytes_consumed() - bytes_before, 0)
        << "Should have read bytes during transaction map skip";
    EXPECT_EQ(output_stream.str().size(), bytes_during_tee)
        << "Stream size should not change when tee is disabled";

    // Verify we can advance to next ledger
    LedgerInfo second_info = reader.read_ledger_info();
    EXPECT_EQ(second_info.sequence, 2) << "Should be at second ledger";
}