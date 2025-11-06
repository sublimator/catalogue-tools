#include "catl/nodestore/nodeobject_codec.h"
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

// Helper to create a v1 inner node (525 bytes)
std::vector<uint8_t>
make_inner_node(std::array<Hash256, 16> const& branches)
{
    std::vector<uint8_t> node(format::INNER_NODE_V1_SIZE);

    // Header: 8 unused + 1 type
    std::memset(node.data(), 0, 8);
    node[8] = static_cast<uint8_t>(node_type::hot_unknown);

    // Prefix
    std::uint32_t prefix = inner_node::HASH_PREFIX_INNER_NODE;
    std::memcpy(node.data() + 9, &prefix, 4);

    // Branches
    for (std::size_t i = 0; i < 16; ++i)
    {
        std::memcpy(node.data() + 13 + (i * 32), branches[i].data(), 32);
    }

    return node;
}

TEST(NodeobjectCodecTest, DecompressType0Uncompressed)
{
    // Type 0: uncompressed data
    std::vector<uint8_t> data = {0, 'H', 'e', 'l', 'l', 'o'};
    std::vector<uint8_t> output;

    auto [decompressed, size] = nodeobject_decompress(
        data.data(), data.size(), make_vector_factory(output));

    EXPECT_EQ(size, 5);  // "Hello"
    EXPECT_EQ(std::memcmp(decompressed, "Hello", 5), 0);
}

TEST(NodeobjectCodecTest, DecompressType1Lz4)
{
    std::string original = "Hello, World! This is a test message.";
    std::vector<uint8_t> compressed_buf;

    // Compress with nodeobject_compress (which will use LZ4)
    auto [compressed, size] = nodeobject_compress(
        original.data(), original.size(), make_vector_factory(compressed_buf));

    // Verify it's type 1
    std::size_t type;
    read_varint(compressed_buf.data(), compressed_buf.size(), type);
    EXPECT_EQ(type, 1);

    // Decompress via nodeobject codec
    std::vector<uint8_t> output;
    auto [decompressed, decompressed_size] =
        nodeobject_decompress(compressed, size, make_vector_factory(output));

    EXPECT_EQ(decompressed_size, original.size());
    EXPECT_EQ(std::memcmp(decompressed, original.data(), decompressed_size), 0);
}

TEST(NodeobjectCodecTest, DecompressType2CompressedInnerNode)
{
    // Create sparse inner node
    std::array<Hash256, 16> branches;
    for (auto& b : branches)
        b = Hash256::zero();
    branches[0] = make_test_hash(0x11);
    branches[7] = make_test_hash(0x77);
    branches[15] = make_test_hash(0xFF);

    // Encode as compressed inner node
    std::vector<uint8_t> encoded(2 + 3 * 32);
    auto size = inner_node::encode_compressed(branches, encoded.data());
    encoded.resize(size);

    // Prepend type varint
    std::vector<uint8_t> with_type;
    with_type.push_back(2);  // Type 2
    with_type.insert(with_type.end(), encoded.begin(), encoded.end());

    // Decompress
    std::vector<uint8_t> output;
    auto [decompressed, out_size] = nodeobject_decompress(
        with_type.data(), with_type.size(), make_vector_factory(output));

    EXPECT_EQ(out_size, format::INNER_NODE_V1_SIZE);

    // Verify reconstructed inner node
    auto const* bytes = static_cast<uint8_t const*>(decompressed);

    // Check prefix
    std::uint32_t prefix;
    std::memcpy(&prefix, bytes + 9, 4);
    EXPECT_EQ(prefix, inner_node::HASH_PREFIX_INNER_NODE);

    // Check branches
    EXPECT_EQ(std::memcmp(bytes + 13 + (0 * 32), branches[0].data(), 32), 0);
    EXPECT_EQ(std::memcmp(bytes + 13 + (7 * 32), branches[7].data(), 32), 0);
    EXPECT_EQ(std::memcmp(bytes + 13 + (15 * 32), branches[15].data(), 32), 0);
}

TEST(NodeobjectCodecTest, DecompressType3FullInnerNode)
{
    // Create full inner node
    std::array<Hash256, 16> branches;
    for (std::size_t i = 0; i < 16; ++i)
        branches[i] = make_test_hash(static_cast<uint8_t>(i));

    // Encode as full inner node
    std::vector<uint8_t> encoded(512);
    inner_node::encode_full(branches, encoded.data());

    // Prepend type varint
    std::vector<uint8_t> with_type;
    with_type.push_back(3);  // Type 3
    with_type.insert(with_type.end(), encoded.begin(), encoded.end());

    // Decompress
    std::vector<uint8_t> output;
    auto [decompressed, out_size] = nodeobject_decompress(
        with_type.data(), with_type.size(), make_vector_factory(output));

    EXPECT_EQ(out_size, format::INNER_NODE_V1_SIZE);

    // Verify reconstructed inner node
    auto const* bytes = static_cast<uint8_t const*>(decompressed);

    // Check all branches
    for (std::size_t i = 0; i < 16; ++i)
    {
        EXPECT_EQ(
            std::memcmp(bytes + 13 + (i * 32), branches[i].data(), 32), 0);
    }
}

TEST(NodeobjectCodecTest, CompressRegularDataUsesLz4)
{
    std::string original = "Regular data that is not an inner node.";
    std::vector<uint8_t> compressed_buf;

    auto [compressed, size] = nodeobject_compress(
        original.data(), original.size(), make_vector_factory(compressed_buf));

    // Should start with type 1 (LZ4)
    std::size_t type;
    read_varint(compressed_buf.data(), compressed_buf.size(), type);
    EXPECT_EQ(type, 1);

    // Should be able to decompress
    std::vector<uint8_t> output;
    auto [decompressed, out_size] =
        nodeobject_decompress(compressed, size, make_vector_factory(output));

    EXPECT_EQ(out_size, original.size());
    EXPECT_EQ(std::memcmp(decompressed, original.data(), out_size), 0);
}

TEST(NodeobjectCodecTest, CompressSparseInnerNodeUsesType2)
{
    // Create sparse inner node (3 branches)
    std::array<Hash256, 16> branches;
    for (auto& b : branches)
        b = Hash256::zero();
    branches[0] = make_test_hash(0xAA);
    branches[8] = make_test_hash(0xBB);
    branches[15] = make_test_hash(0xCC);

    auto node_data = make_inner_node(branches);
    std::vector<uint8_t> compressed_buf;

    nodeobject_compress(
        node_data.data(),
        node_data.size(),
        make_vector_factory(compressed_buf));

    // Should start with type 2 (compressed inner node)
    std::size_t type;
    read_varint(compressed_buf.data(), compressed_buf.size(), type);
    EXPECT_EQ(type, 2);
}

TEST(NodeobjectCodecTest, CompressFullInnerNodeUsesType3)
{
    // Create full inner node (all 16 branches)
    std::array<Hash256, 16> branches;
    for (std::size_t i = 0; i < 16; ++i)
        branches[i] = make_test_hash(static_cast<uint8_t>(i + 1));

    auto node_data = make_inner_node(branches);
    std::vector<uint8_t> compressed_buf;

    nodeobject_compress(
        node_data.data(),
        node_data.size(),
        make_vector_factory(compressed_buf));

    // Should start with type 3 (full inner node)
    std::size_t type;
    read_varint(compressed_buf.data(), compressed_buf.size(), type);
    EXPECT_EQ(type, 3);
}

TEST(NodeobjectCodecTest, RoundTripRegularData)
{
    std::string original =
        "This is test data that should survive compression and decompression.";
    std::vector<uint8_t> compressed_buf;
    std::vector<uint8_t> decompressed_buf;

    // Compress
    auto [compressed, compressed_size] = nodeobject_compress(
        original.data(), original.size(), make_vector_factory(compressed_buf));

    // Decompress
    auto [decompressed, decompressed_size] = nodeobject_decompress(
        compressed, compressed_size, make_vector_factory(decompressed_buf));

    EXPECT_EQ(decompressed_size, original.size());
    EXPECT_EQ(std::memcmp(decompressed, original.data(), original.size()), 0);
}

TEST(NodeobjectCodecTest, RoundTripSparseInnerNode)
{
    // Create sparse inner node
    std::array<Hash256, 16> branches;
    for (auto& b : branches)
        b = Hash256::zero();
    branches[0] = make_test_hash(0x10);
    branches[3] = make_test_hash(0x30);
    branches[9] = make_test_hash(0x90);

    auto original = make_inner_node(branches);
    std::vector<uint8_t> compressed_buf;
    std::vector<uint8_t> decompressed_buf;

    // Compress
    auto [compressed, compressed_size] = nodeobject_compress(
        original.data(), original.size(), make_vector_factory(compressed_buf));

    // Decompress
    auto [decompressed, decompressed_size] = nodeobject_decompress(
        compressed, compressed_size, make_vector_factory(decompressed_buf));

    EXPECT_EQ(decompressed_size, format::INNER_NODE_V1_SIZE);
    EXPECT_EQ(std::memcmp(decompressed, original.data(), original.size()), 0);
}

TEST(NodeobjectCodecTest, RoundTripFullInnerNode)
{
    // Create full inner node
    std::array<Hash256, 16> branches;
    for (std::size_t i = 0; i < 16; ++i)
        branches[i] = make_test_hash(static_cast<uint8_t>(0x10 + i));

    auto original = make_inner_node(branches);
    std::vector<uint8_t> compressed_buf;
    std::vector<uint8_t> decompressed_buf;

    // Compress
    auto [compressed, compressed_size] = nodeobject_compress(
        original.data(), original.size(), make_vector_factory(compressed_buf));

    // Decompress
    auto [decompressed, decompressed_size] = nodeobject_decompress(
        compressed, compressed_size, make_vector_factory(decompressed_buf));

    EXPECT_EQ(decompressed_size, format::INNER_NODE_V1_SIZE);
    EXPECT_EQ(std::memcmp(decompressed, original.data(), original.size()), 0);
}

TEST(NodeobjectCodecTest, DecompressInvalidType)
{
    std::vector<uint8_t> data = {99};  // Invalid type
    std::vector<uint8_t> output;

    EXPECT_THROW(
        nodeobject_decompress(
            data.data(), data.size(), make_vector_factory(output)),
        std::runtime_error);
}

TEST(NodeobjectCodecTest, DecompressInvalidVarint)
{
    std::vector<uint8_t> data;  // Empty
    std::vector<uint8_t> output;

    EXPECT_THROW(
        nodeobject_decompress(
            data.data(), data.size(), make_vector_factory(output)),
        std::runtime_error);
}

TEST(NodeobjectCodecTest, BufferFactoryConcept)
{
    // Test that the concept works with lambda
    std::vector<uint8_t> buffer;
    auto factory = [&buffer](std::size_t size) {
        buffer.resize(size);
        return buffer.data();
    };

    static_assert(buffer_factory<decltype(factory)>);

    // Test with make_vector_factory helper
    auto factory2 = make_vector_factory(buffer);
    static_assert(buffer_factory<decltype(factory2)>);
}
