#include "catl/core/types.h"
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
        ASSERT_TRUE(writer->writeHeader(min_ledger, max_ledger));

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
        ASSERT_TRUE(writer->writeLedger(header, *stateMap, *txMap));

        // Finalize the file
        ASSERT_TRUE(writer->finalize());

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
        ASSERT_TRUE(writer->writeHeader(min_ledger, max_ledger));

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
            ASSERT_TRUE(writer->writeLedger(header, *stateMap, *txMap));
        }

        // Finalize the file
        ASSERT_TRUE(writer->finalize());

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
    ASSERT_TRUE(writer->writeHeader(min_ledger, max_ledger));

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

    ASSERT_TRUE(writer->writeLedger(header1, *stateMap1, *txMap1));

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
    ASSERT_TRUE(writer->writeLedgerHeader(header2));

    // Write state map as delta
    ASSERT_TRUE(writer->writeMapDelta(*stateMap1, *stateMap2, tnACCOUNT_STATE));

    // Write tx map as full map
    ASSERT_TRUE(writer->writeMap(*txMap1, tnTRANSACTION_MD));

    // Finalize the file
    ASSERT_TRUE(writer->finalize());

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
