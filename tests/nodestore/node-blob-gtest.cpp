#include "catl/nodestore/node_blob.h"
#include <array>
#include <cstring>
#include <gtest/gtest.h>
#include <string>
#include <vector>

using namespace catl::nodestore;

// Helper to create test hash
Hash256
make_test_hash(uint8_t value)
{
    std::array<uint8_t, 32> data;
    data.fill(value);
    return Hash256(data);
}

TEST(NodeBlobTest, StructGetType)
{
    // Create a blob with header
    node_blob blob;
    blob.data.resize(9);
    std::memset(blob.data.data(), 0, 8);
    blob.data[8] = static_cast<uint8_t>(node_type::hot_account_node);

    EXPECT_EQ(blob.get_type(), node_type::hot_account_node);
}

TEST(NodeBlobTest, StructPayload)
{
    // Create a blob with header + payload
    node_blob blob;
    blob.data = {0, 0, 0, 0, 0, 0, 0, 0, 1, 'H', 'e', 'l', 'l', 'o'};
    // Bytes 0-7: unused
    // Byte 8: type (1 = hot_ledger)
    // Bytes 9+: payload "Hello"

    auto payload = blob.payload();
    EXPECT_EQ(payload.size(), 5);
    EXPECT_EQ(std::memcmp(payload.data(), "Hello", 5), 0);
}

TEST(NodeBlobTest, CompressGenericData)
{
    // Compress some simple data
    std::string original = "Hello, World! This is test data.";
    std::vector<uint8_t> data(original.begin(), original.end());

    node_blob compressed =
        nodeobject_compress(node_type::hot_account_node, data);

    // Should have 9-byte header
    EXPECT_GE(compressed.data.size(), 9);

    // Type should be extractable
    EXPECT_EQ(compressed.get_type(), node_type::hot_account_node);

    // Payload should contain compression type varint + LZ4 data
    auto payload = compressed.payload();
    EXPECT_GT(payload.size(), 0);

    // First byte of payload should be compression type varint (1 = LZ4)
    std::size_t comp_type;
    auto vn = read_varint(payload.data(), payload.size(), comp_type);
    EXPECT_GT(vn, 0);
    EXPECT_EQ(comp_type, static_cast<std::size_t>(compression_type::lz4));
}

TEST(NodeBlobTest, RoundTripCompressDecompress)
{
    std::string original = "This is test data that will be compressed.";
    std::vector<uint8_t> data(original.begin(), original.end());

    // Compress
    node_blob compressed =
        nodeobject_compress(node_type::hot_transaction_node, data);

    // Decompress
    node_blob decompressed = nodeobject_decompress(compressed);

    // Check type preserved
    EXPECT_EQ(decompressed.get_type(), node_type::hot_transaction_node);

    // Check payload matches original
    auto payload = decompressed.payload();
    EXPECT_EQ(payload.size(), original.size());
    EXPECT_EQ(std::memcmp(payload.data(), original.data(), original.size()), 0);
}

TEST(NodeBlobTest, CompressLargeData)
{
    // Create large buffer with pattern
    std::vector<uint8_t> original(10000);
    for (size_t i = 0; i < original.size(); ++i)
    {
        original[i] = static_cast<uint8_t>(i % 256);
    }

    // Compress
    node_blob compressed =
        nodeobject_compress(node_type::hot_account_node, original);

    // Should compress due to pattern
    EXPECT_LT(compressed.data.size(), original.size() + 9);

    // Decompress
    node_blob decompressed = nodeobject_decompress(compressed);

    // Check payload matches
    auto payload = decompressed.payload();
    EXPECT_EQ(payload.size(), original.size());
    EXPECT_EQ(std::memcmp(payload.data(), original.data(), original.size()), 0);
}

TEST(NodeBlobTest, CompressZeros)
{
    // All zeros should compress very well
    std::vector<uint8_t> original(1000, 0);

    node_blob compressed =
        nodeobject_compress(node_type::hot_account_node, original);

    // Should compress well
    EXPECT_LT(compressed.data.size(), original.size() / 10);

    // Decompress
    node_blob decompressed = nodeobject_decompress(compressed);

    // Check all zeros
    auto payload = decompressed.payload();
    EXPECT_EQ(payload.size(), 1000);
    for (auto byte : payload)
    {
        EXPECT_EQ(byte, 0);
    }
}

TEST(NodeBlobTest, EmptyPayloadThrows)
{
    // Empty payloads are not supported - LZ4 will throw
    // This matches Xahau behavior where lz4_decompress throws on outSize <= 0
    std::vector<uint8_t> original;

    node_blob compressed =
        nodeobject_compress(node_type::hot_account_node, original);

    // Decompressing empty payload should throw
    EXPECT_THROW(nodeobject_decompress(compressed), std::runtime_error);
}

TEST(NodeBlobTest, TypePreservation)
{
    // Test that hot types are preserved correctly
    // NOTE: Only hot_* types (0-255) are ever serialized.
    // Pinned types are runtime-only and get downgraded before storage.
    std::vector<node_type> types = {
        node_type::hot_unknown,
        node_type::hot_ledger,
        node_type::hot_account_node,
        node_type::hot_transaction_node};

    std::vector<uint8_t> data = {1, 2, 3, 4, 5};

    for (auto type : types)
    {
        node_blob compressed = nodeobject_compress(type, data);
        node_blob decompressed = nodeobject_decompress(compressed);

        EXPECT_EQ(decompressed.get_type(), type);
        auto payload = decompressed.payload();
        EXPECT_EQ(payload.size(), data.size());
        EXPECT_EQ(std::memcmp(payload.data(), data.data(), data.size()), 0);
    }
}

// Test mock inner node source
struct mock_inner_node
{
    std::array<Hash256, 16> branches;
    uint16_t mask = 0;

    Hash256 const&
    get_node_source_child_hash(int branch) const
    {
        if (branch < 0 || branch >= 16)
            throw std::runtime_error("invalid branch");
        return branches[branch];
    }

    uint16_t
    get_node_source_branch_mask() const
    {
        return mask;
    }

    Hash256 const&
    get_node_source_hash() const
    {
        static Hash256 dummy_hash = make_test_hash(0x42);
        return dummy_hash;
    }
};

// Verify the concept works
static_assert(inner_node_source<mock_inner_node>);

TEST(NodeBlobTest, InnerNodeSourceConceptSparse)
{
    // Create mock inner node with sparse branches (3 populated)
    mock_inner_node node;
    for (auto& b : node.branches)
        b = Hash256::zero();

    node.branches[0] = make_test_hash(0xAA);
    node.branches[8] = make_test_hash(0xBB);
    node.branches[15] = make_test_hash(0xCC);
    // Mask bit encoding: branch i → bit (15 - i)
    node.mask = (1 << 15) | (1 << 7) | (1 << 0);

    // Compress using concept
    node_blob compressed = nodeobject_compress(node);

    // Should use type 2 (compressed) because sparse (3 < 16)
    auto payload = compressed.payload();
    std::size_t comp_type;
    auto vn = read_varint(payload.data(), payload.size(), comp_type);
    EXPECT_GT(vn, 0);
    EXPECT_EQ(
        comp_type,
        static_cast<std::size_t>(compression_type::inner_node_compressed));

    // Decompress
    node_blob decompressed = nodeobject_decompress(compressed);

    // Should be inner node type
    EXPECT_EQ(decompressed.get_type(), node_type::hot_unknown);

    // Payload should be 512 bytes (16 hashes)
    auto dec_payload = decompressed.payload();
    EXPECT_EQ(dec_payload.size(), 512);

    // Check the 3 populated hashes
    EXPECT_EQ(
        std::memcmp(dec_payload.data() + (0 * 32), node.branches[0].data(), 32),
        0);
    EXPECT_EQ(
        std::memcmp(dec_payload.data() + (8 * 32), node.branches[8].data(), 32),
        0);
    EXPECT_EQ(
        std::memcmp(
            dec_payload.data() + (15 * 32), node.branches[15].data(), 32),
        0);

    // Check zero hashes for unpopulated branches
    Hash256 zero = Hash256::zero();
    EXPECT_EQ(std::memcmp(dec_payload.data() + (1 * 32), zero.data(), 32), 0);
}

TEST(NodeBlobTest, InnerNodeSourceConceptFull)
{
    // Create mock inner node with all 16 branches populated
    mock_inner_node node;
    for (std::size_t i = 0; i < 16; ++i)
    {
        node.branches[i] = make_test_hash(static_cast<uint8_t>(i + 1));
    }
    node.mask = 0xFFFF;  // All bits set

    // Compress using concept
    node_blob compressed = nodeobject_compress(node);

    // Should use type 3 (full) because all branches populated
    auto payload = compressed.payload();
    std::size_t comp_type;
    auto vn = read_varint(payload.data(), payload.size(), comp_type);
    EXPECT_GT(vn, 0);
    EXPECT_EQ(
        comp_type, static_cast<std::size_t>(compression_type::inner_node_full));

    // Decompress
    node_blob decompressed = nodeobject_decompress(compressed);

    // Payload should be 512 bytes (16 hashes)
    auto dec_payload = decompressed.payload();
    EXPECT_EQ(dec_payload.size(), 512);

    // Check all 16 hashes
    for (std::size_t i = 0; i < 16; ++i)
    {
        EXPECT_EQ(
            std::memcmp(
                dec_payload.data() + (i * 32), node.branches[i].data(), 32),
            0);
    }
}

TEST(NodeBlobTest, InnerNodeSourceEmpty)
{
    // Create mock inner node with no branches
    mock_inner_node node;
    for (auto& b : node.branches)
        b = Hash256::zero();
    node.mask = 0;  // No bits set

    // Compress using concept
    node_blob compressed = nodeobject_compress(node);

    // Should use type 2 (compressed) with 0 branches
    auto payload = compressed.payload();
    std::size_t comp_type;
    auto vn = read_varint(payload.data(), payload.size(), comp_type);
    EXPECT_GT(vn, 0);
    EXPECT_EQ(
        comp_type,
        static_cast<std::size_t>(compression_type::inner_node_compressed));

    // Decompress
    node_blob decompressed = nodeobject_decompress(compressed);

    // All hashes should be zero
    auto dec_payload = decompressed.payload();
    EXPECT_EQ(dec_payload.size(), 512);
    Hash256 zero = Hash256::zero();
    for (std::size_t i = 0; i < 16; ++i)
    {
        EXPECT_EQ(
            std::memcmp(dec_payload.data() + (i * 32), zero.data(), 32), 0);
    }
}

TEST(NodeBlobTest, InvalidDecompressTooSmall)
{
    // Create blob that's too small (no header)
    node_blob blob;
    blob.data = {1, 2, 3};

    EXPECT_THROW(nodeobject_decompress(blob), std::runtime_error);
}

TEST(NodeBlobTest, InvalidDecompressBadVarint)
{
    // Create blob with header but invalid varint
    node_blob blob;
    blob.data.resize(9);
    std::memset(blob.data.data(), 0, 8);
    blob.data[8] = static_cast<uint8_t>(node_type::hot_unknown);
    // Payload is empty - no varint

    EXPECT_THROW(nodeobject_decompress(blob), std::runtime_error);
}

TEST(NodeBlobTest, InvalidDecompressUnknownType)
{
    // Create blob with invalid compression type
    node_blob blob;
    blob.data.resize(10);
    std::memset(blob.data.data(), 0, 8);
    blob.data[8] = static_cast<uint8_t>(node_type::hot_unknown);
    blob.data[9] = 99;  // Invalid compression type

    EXPECT_THROW(nodeobject_decompress(blob), std::runtime_error);
}
