#include "catl/core/types.h"
#include "catl/crypto/sha512-hasher.h"  // For hash verification
#include "catl/shamap/shamap.h"
#include "catl/v1/catl-v1-mmap-reader.h"
#include "catl/v1/catl-v1-utils.h"  // For get_compression_level
#include "catl/v1/catl-v1-writer.h"
#include <boost/filesystem.hpp>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>

using namespace catl::v1;

class WriterTest : public ::testing::Test
{
protected:
    std::vector<std::vector<uint8_t>> data_;

    // Create a temporary directory for test files
    void
    SetUp() override
    {
        test_dir_ = boost::filesystem::temp_directory_path() / "writer_test";
        boost::filesystem::create_directory(test_dir_);
    }

    // Clean up temporary files
    void
    TearDown() override
    {
        boost::filesystem::remove_all(test_dir_);
    }

    // Helper to create a test SHAMap with some data
    std::shared_ptr<SHAMap>
    createTestMap(SHAMapNodeType nodeType, int itemCount = 10)
    {
        auto map = std::make_shared<SHAMap>(nodeType);

        // Add some test items
        for (int i = 0; i < itemCount; i++)
        {
            // Create a key with pattern based on i
            uint8_t keyData[32] = {0};
            keyData[0] = static_cast<uint8_t>(i & 0xFF);
            keyData[1] = static_cast<uint8_t>((i >> 8) & 0xFF);
            Key key(keyData);

            // Create some simple test data
            std::vector<uint8_t> data(64, static_cast<uint8_t>(i));
            data_.push_back(data);  // Keep it in scope

            // Create item and add to map
            auto item = boost::intrusive_ptr(
                new MmapItem(key.data(), data.data(), data.size()));
            map->set_item(item);
        }

        return map;
    }

    boost::filesystem::path test_dir_;
};

// Test just the basic writing and header verification of uncompressed and
// compressed files
TEST_F(WriterTest, BasicWriteTest)
{
    // Case 1: Uncompressed file
    {
        // Create a test file path
        auto test_file = test_dir_ / "uncompressed.catl";

        // Create writer with compression level 0 (uncompressed)
        WriterOptions options;
        options.compression_level = 0;
        options.network_id = 123;

        // Create writer
        auto writer = Writer::for_file(test_file.string(), options);

        // Write header
        uint32_t min_ledger = 1000;
        uint32_t max_ledger = 1010;
        EXPECT_NO_THROW(writer->write_header(min_ledger, max_ledger));

        // Create test maps
        auto stateMap = createTestMap(tnACCOUNT_STATE);
        auto txMap = createTestMap(tnTRANSACTION_MD);

        // Create a test ledger header
        LedgerInfo header{};
        header.sequence = min_ledger;
        header.close_time = 12345;

        // Copy hash data into the appropriate fields
        Hash256 stateHash = stateMap->get_hash();
        Hash256 txHash = txMap->get_hash();
        std::memcpy(header.account_hash, stateHash.data(), Hash256::size());
        std::memcpy(header.tx_hash, txHash.data(), Hash256::size());

        // Write ledger
        EXPECT_NO_THROW(writer->write_ledger(header, *stateMap, *txMap));

        // Finalize the file
        EXPECT_NO_THROW(writer->finalize());

        // Done with writer
        writer.reset();

        // Now read back the file with MmapReader to verify the header
        MmapReader reader(test_file.string());

        // Verify header fields
        const auto& readHeader = reader.header();
        EXPECT_EQ(readHeader.magic, CATL_MAGIC);
        EXPECT_EQ(readHeader.min_ledger, min_ledger);
        EXPECT_EQ(readHeader.max_ledger, max_ledger);
        EXPECT_EQ(readHeader.network_id, options.network_id);
        EXPECT_EQ(get_compression_level(readHeader.version), 0);

        // Verify file was successfully created
        EXPECT_TRUE(boost::filesystem::exists(test_file));
        size_t uncompressed_size = boost::filesystem::file_size(test_file);
        std::cout << "Uncompressed file size: " << uncompressed_size << " bytes"
                  << std::endl;

        // Verify the file hash using our exception-based method
        EXPECT_NO_THROW(reader.verify_file_hash())
            << "File hash verification failed";
    }

    // Case 2: Compressed file with Zlib
    // We can only test the writing portion here since MmapReader cannot read
    // compressed files
    {
        // Create a test file path
        auto test_file = test_dir_ / "compressed.catl";

        // Create writer with compression level 6 (default zlib compression)
        WriterOptions options;
        options.compression_level = 6;
        options.network_id = 456;

        // Create writer
        auto writer = Writer::for_file(test_file.string(), options);

        // Write header
        uint32_t min_ledger = 2000;
        uint32_t max_ledger = 2020;
        EXPECT_NO_THROW(writer->write_header(min_ledger, max_ledger));

        // Create test maps with more data to see compression in action
        auto stateMap = createTestMap(tnACCOUNT_STATE, 100);
        auto txMap = createTestMap(tnTRANSACTION_MD, 50);

        // Create a test ledger header
        LedgerInfo header{};
        header.sequence = min_ledger;
        header.close_time = 23456;

        // Copy hash data into the appropriate fields
        Hash256 stateHash = stateMap->get_hash();
        Hash256 txHash = txMap->get_hash();
        std::memcpy(header.account_hash, stateHash.data(), Hash256::size());
        std::memcpy(header.tx_hash, txHash.data(), Hash256::size());

        // Write several ledgers to generate more data
        for (uint32_t i = 0; i < 5; i++)
        {
            header.sequence = min_ledger + i;
            EXPECT_NO_THROW(writer->write_ledger(header, *stateMap, *txMap));
        }

        // Finalize the file
        EXPECT_NO_THROW(writer->finalize());

        // Done with writer
        writer.reset();

        // Check if file exists
        EXPECT_TRUE(boost::filesystem::exists(test_file));

        // Get file size
        size_t compressed_size = boost::filesystem::file_size(test_file);
        std::cout << "Compressed file size: " << compressed_size << " bytes"
                  << std::endl;

        // We can't use MmapReader to verify compressed files as it fails with
        // "File size does not match header value" because in compressed files
        // the filesize in the header is the uncompressed size while the actual
        // file on disk is smaller due to compression.
        //
        // We could use catl-decomp tool to decompress and then verify,
        // but that's beyond the scope of a unit test.
    }
}

// Test a simplified map read/write to diagnose issues
TEST_F(WriterTest, SimpleMapReadTest)
{
    // Create a test file path with a unique name
    auto test_file = test_dir_ / "simple_map.catl";

    // Create writer with compression level 0 (uncompressed)
    WriterOptions options;
    options.compression_level = 0;

    // Create writer
    auto writer = Writer::for_file(test_file.string(), options);

    // Write header
    uint32_t min_ledger = 9000;
    uint32_t max_ledger = 9010;
    EXPECT_NO_THROW(writer->write_header(min_ledger, max_ledger));

    // Create a test map with only ONE item for simplicity
    auto stateMap = std::make_shared<SHAMap>(tnACCOUNT_STATE);

    // Create a single item
    uint8_t keyData[32] = {0xAA, 0xBB, 0xCC};
    Key key(keyData);
    std::vector<uint8_t> itemData(64, 0x42);  // Fill with 'B' (0x42)
    data_.push_back(itemData);                // Keep data alive

    // Create MmapItem
    auto item = boost::intrusive_ptr(
        new MmapItem(key.data(), data_.back().data(), data_.back().size()));
    ASSERT_TRUE(stateMap->set_item(item) != SetResult::FAILED);

    // Verify we have 1 item
    size_t count = 0;
    stateMap->visit_items([&](const MmapItem&) { count++; });
    ASSERT_EQ(count, 1) << "Map should have exactly 1 item";

    // Get hash
    Hash256 stateHash = stateMap->get_hash();
    std::cout << "Original map hash: " << stateHash.hex() << std::endl;

    // Create minimum transaction map
    auto txMap = std::make_shared<SHAMap>(tnTRANSACTION_MD);
    Hash256 txHash = txMap->get_hash();

    // Create ledger header
    LedgerInfo header{};
    header.sequence = min_ledger;

    // Set hashes
    std::memcpy(header.account_hash, stateHash.data(), Hash256::size());
    std::memcpy(header.tx_hash, txHash.data(), Hash256::size());

    // Write the ledger
    EXPECT_NO_THROW(writer->write_ledger(header, *stateMap, *txMap));

    // Finalize file
    EXPECT_NO_THROW(writer->finalize());
    writer.reset();

    // Now read back the file
    MmapReader reader(test_file.string());

    // Skip to after header
    reader.set_position(sizeof(CatlHeader));

    // Skip ledger header
    reader.read_ledger_info();

    // Read the state map
    SHAMap readStateMap(tnACCOUNT_STATE);
    uint32_t nodesRead = reader.read_shamap(readStateMap, tnACCOUNT_STATE);

    // Check how many nodes were read
    std::cout << "Nodes read: " << nodesRead << std::endl;

    // Verify the hash
    Hash256 readHash = readStateMap.get_hash();
    std::cout << "Read map hash: " << readHash.hex() << std::endl;

    EXPECT_EQ(readHash, stateHash) << "Hash mismatch!";

    // Count how many items are in the read map
    size_t readCount = 0;
    readStateMap.visit_items([&](const MmapItem&) { readCount++; });

    std::cout << "Items in read map: " << readCount << std::endl;
    EXPECT_EQ(readCount, 1) << "Should read exactly 1 item";

    // Verify the item content
    bool found_item = false;
    readStateMap.visit_items([&](const MmapItem& readItem) {
        found_item = true;

        // Verify key
        EXPECT_EQ(std::memcmp(readItem.key().data(), keyData, Key::size()), 0)
            << "Key mismatch";

        // Verify data size
        EXPECT_EQ(readItem.slice().size(), itemData.size())
            << "Data size mismatch";

        // Verify data content
        if (readItem.slice().size() == itemData.size())
        {
            bool data_match = true;
            for (size_t i = 0; i < itemData.size(); i++)
            {
                if (readItem.slice().data()[i] != itemData[i])
                {
                    data_match = false;
                    EXPECT_EQ(readItem.slice().data()[i], itemData[i])
                        << "Data mismatch at position " << i;
                    break;
                }
            }
            EXPECT_TRUE(data_match) << "Data content doesn't match";
        }
    });

    EXPECT_TRUE(found_item) << "Item was not found in read map";

    // Read tx map
    SHAMap readTxMap(tnTRANSACTION_MD);
    reader.read_shamap(readTxMap, tnTRANSACTION_MD);

    // Should be at end of file
    EXPECT_TRUE(reader.eof());
}

// Test reading back a map and verifying its contents in detail
TEST_F(WriterTest, ReadAndVerifyMapTest)
{
    // Create a test file path
    auto test_file = test_dir_ / "verify_map.catl";

    // Create writer with compression level 0 (uncompressed)
    WriterOptions options;
    options.compression_level = 0;

    // Create writer
    auto writer = Writer::for_file(test_file.string(), options);

    // Write header
    uint32_t min_ledger = 5000;
    uint32_t max_ledger = 5010;
    EXPECT_NO_THROW(writer->write_header(min_ledger, max_ledger));

    // Create a test map with some specific data patterns
    auto stateMap = std::make_shared<SHAMap>(tnACCOUNT_STATE);

    // Create a set of test data with different sizes and patterns
    std::map<std::string, std::vector<uint8_t>>
        original_items;  // Use string key for comparison
    std::vector<std::unique_ptr<uint8_t[]>> key_buffers;  // Store key data

    // Add 10 items with different sizes and patterns
    for (int i = 0; i < 10; i++)
    {
        // Create a unique key (allocated on heap to avoid stack issues)
        auto key_buffer = std::make_unique<uint8_t[]>(32);
        std::memset(key_buffer.get(), 0, 32);
        key_buffer[0] = static_cast<uint8_t>(i & 0xFF);
        key_buffer[1] = static_cast<uint8_t>((i >> 8) & 0xFF);
        key_buffer[2] = 0xAA;  // Make it more recognizable

        Key key(key_buffer.get());
        std::string key_hex =
            key.hex();  // Store hex representation for comparison

        // Create data with varying sizes and patterns
        std::vector<uint8_t> data(20 + i * 10);  // Different size for each item

        // Fill with a recognizable pattern
        for (size_t j = 0; j < data.size(); j++)
        {
            data[j] = static_cast<uint8_t>((i * j) & 0xFF);
        }

        // Store in map of original items using hex key for comparison
        original_items[key_hex] = data;

        // Store data in class member to keep it alive
        data_.push_back(data);

        // Add to SHAMap
        auto item = boost::intrusive_ptr(new MmapItem(
            key_buffer.get(), data_.back().data(), data_.back().size()));
        auto result = stateMap->set_item(item);

        ASSERT_TRUE(result != SetResult::FAILED)
            << "Failed to add item " << i
            << " to map: result=" << static_cast<int>(result);

        // Keep key buffer alive for the duration of the test
        key_buffers.push_back(std::move(key_buffer));
    }

    // Verify we actually added 10 items to the map
    size_t original_count = 0;
    stateMap->visit_items([&](const MmapItem&) { original_count++; });
    ASSERT_EQ(original_count, 10) << "Original map doesn't have 10 items!";

    std::cout << "Original map hash: " << stateMap->get_hash().hex()
              << std::endl;

    // Get the hash for the ledger header
    Hash256 stateHash = stateMap->get_hash();

    // Create a minimal dummy transaction map (not the focus of this test)
    auto txMap = createTestMap(tnTRANSACTION_MD, 1);
    Hash256 txHash = txMap->get_hash();

    // Create a test ledger header
    LedgerInfo header{};
    header.sequence = min_ledger;
    header.close_time = 12345;

    // Copy hash data into the appropriate fields
    std::memcpy(header.account_hash, stateHash.data(), Hash256::size());
    std::memcpy(header.tx_hash, txHash.data(), Hash256::size());

    // Write ledger
    EXPECT_NO_THROW(writer->write_ledger(header, *stateMap, *txMap));

    // Finalize the file
    EXPECT_NO_THROW(writer->finalize());
    writer.reset();

    // Now read back the file with MmapReader
    MmapReader reader(test_file.string());

    // Skip to after the header
    reader.set_position(sizeof(CatlHeader));

    // Skip ledger header
    reader.read_ledger_info();

    // Read the state map
    SHAMap readStateMap(tnACCOUNT_STATE);
    uint32_t nodesRead = reader.read_shamap(readStateMap, tnACCOUNT_STATE);

    // Print information about what was read
    std::cout << "Nodes read from file: " << nodesRead << std::endl;

    // Count items in the read map
    size_t readItems = 0;
    readStateMap.visit_items([&](const MmapItem&) { readItems++; });
    std::cout << "Items in read map: " << readItems << std::endl;

    // Print hash of the read map
    Hash256 readHash = readStateMap.get_hash();
    std::cout << "Read map hash: " << readHash.hex() << std::endl;

    // Verify node count
    EXPECT_EQ(readItems, 10) << "Expected 10 items in the read map";

    // Verify hash matches
    EXPECT_EQ(readStateMap.get_hash(), stateHash);

    // Now verify each item in detail
    size_t items_verified = 0;

    // Use the visitor pattern to iterate all items
    readStateMap.visit_items([&](const MmapItem& item) {
        const Key& key = item.key();
        std::string key_hex = key.hex();  // Get hex representation for lookup
        Slice data_slice = item.slice();

        std::cout << "Found item with key: " << key_hex
                  << ", size: " << data_slice.size() << std::endl;

        // Check if this key exists in our original items
        auto it = original_items.find(key_hex);
        ASSERT_TRUE(it != original_items.end())
            << "Key not found in original items: " << key_hex;

        const std::vector<uint8_t>& expected_data = it->second;

        // Verify size matches
        EXPECT_EQ(data_slice.size(), expected_data.size())
            << "Size mismatch for key: " << key_hex;

        // Verify content matches
        bool content_matches = true;
        if (data_slice.size() == expected_data.size())
        {
            for (size_t i = 0; i < data_slice.size(); i++)
            {
                if (data_slice.data()[i] != expected_data[i])
                {
                    content_matches = false;
                    EXPECT_EQ(data_slice.data()[i], expected_data[i])
                        << "Data mismatch at position " << i
                        << " for key: " << key_hex;
                    break;
                }
            }
        }

        EXPECT_TRUE(content_matches) << "Content mismatch for key: " << key_hex;
        items_verified++;
    });

    // Verify we checked all items
    EXPECT_EQ(items_verified, original_items.size());

    // Skip tx map (not testing it in detail)
    SHAMap readTxMap(tnTRANSACTION_MD);
    reader.read_shamap(readTxMap, tnTRANSACTION_MD);

    // Should be at end of file
    EXPECT_TRUE(reader.eof());
}

// Test writing map deltas
TEST_F(WriterTest, MapDeltaWriteTest)
{
    // Create a test file path
    auto test_file = test_dir_ / "delta.catl";

    // Create writer with compression level 0 (uncompressed for ease of testing)
    WriterOptions options;
    options.compression_level = 0;

    // Create writer
    auto writer = Writer::for_file(test_file.string(), options);

    // Write header
    uint32_t min_ledger = 3000;
    uint32_t max_ledger = 3010;
    EXPECT_NO_THROW(writer->write_header(min_ledger, max_ledger));

    // Create an initial state map
    auto stateMap1 =
        createTestMap(tnACCOUNT_STATE, 5);  // Smaller map for simpler test
    auto txMap1 = createTestMap(tnTRANSACTION_MD, 3);

    // Create a modified state map by adding/removing items
    auto stateMap2 = std::make_shared<SHAMap>(*stateMap1);  // Copy constructor

    // Add a new item
    uint8_t newKeyData[32] = {0xFF, 0xFF};
    Key newKey(newKeyData);
    std::vector<uint8_t> newData(64, 0xAA);
    auto newItem = boost::intrusive_ptr(
        new MmapItem(newKey.data(), newData.data(), newData.size()));
    stateMap2->set_item(newItem);

    // Remove an existing item (remove the first item)
    uint8_t removeKeyData[32] = {0};
    Key removeKey(removeKeyData);
    stateMap2->remove_item(removeKey);

    // Write the first ledger with full maps
    LedgerInfo header1{};
    header1.sequence = min_ledger;

    // Copy hash data into the appropriate fields
    Hash256 stateHash1 = stateMap1->get_hash();
    Hash256 txHash1 = txMap1->get_hash();
    std::memcpy(header1.account_hash, stateHash1.data(), Hash256::size());
    std::memcpy(header1.tx_hash, txHash1.data(), Hash256::size());

    EXPECT_NO_THROW(writer->write_ledger(header1, *stateMap1, *txMap1));

    // Write the second ledger using deltas for state map
    LedgerInfo header2{};
    header2.sequence = min_ledger + 1;

    // Copy hash data into the appropriate fields
    Hash256 stateHash2 = stateMap2->get_hash();
    // Reuse txHash1 since it's the same map
    std::memcpy(header2.account_hash, stateHash2.data(), Hash256::size());
    std::memcpy(
        header2.tx_hash, txHash1.data(), Hash256::size());  // Same tx map

    // Write ledger header
    EXPECT_NO_THROW(writer->write_ledger_header(header2));

    // Write state map as delta
    EXPECT_NO_THROW(
        writer->write_map_delta(*stateMap1, *stateMap2, tnACCOUNT_STATE));

    // Write tx map as full map
    EXPECT_NO_THROW(writer->write_map(*txMap1, tnTRANSACTION_MD));

    // Finalize the file
    EXPECT_NO_THROW(writer->finalize());

    // Done with writer
    writer.reset();

    // Verify file was written
    EXPECT_TRUE(boost::filesystem::exists(test_file));
    size_t file_size = boost::filesystem::file_size(test_file);
    std::cout << "Delta file size: " << file_size << " bytes" << std::endl;

    // Verify header can be read back correctly
    MmapReader reader(test_file.string());
    const auto& readHeader = reader.header();
    EXPECT_EQ(readHeader.min_ledger, min_ledger);
    EXPECT_EQ(readHeader.max_ledger, max_ledger);
}
