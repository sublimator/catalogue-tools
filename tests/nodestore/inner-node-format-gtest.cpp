#include "catl/nodestore/inner_node_format.h"
#include <array>
#include <cstring>
#include <gtest/gtest.h>
#include <vector>

using namespace catl::nodestore;

// Helper to create a test hash with a specific byte value
Hash256
make_test_hash(uint8_t value)
{
    std::array<uint8_t, 32> data;
    data.fill(value);
    return Hash256(data);
}

TEST(InnerNodeFormatTest, DecodeCompressedEmpty)
{
    // Empty inner node: mask = 0x0000, no hashes
    std::vector<uint8_t> data = {0x00, 0x00};

    std::array<Hash256, format::INNER_NODE_BRANCH_COUNT> branches;
    bool result =
        inner_node::decode_compressed(data.data(), data.size(), branches);

    EXPECT_TRUE(result);

    // All branches should be zero
    for (auto const& branch : branches)
    {
        EXPECT_EQ(branch, Hash256::zero());
    }
}

TEST(InnerNodeFormatTest, DecodeCompressedSingleBranch)
{
    // Single branch at position 0: mask = 0x8000, one hash
    std::vector<uint8_t> data(2 + 32);
    data[0] = 0x80;  // High byte of mask
    data[1] = 0x00;  // Low byte of mask

    // Fill hash with 0xFF
    std::fill(data.begin() + 2, data.end(), 0xFF);

    std::array<Hash256, format::INNER_NODE_BRANCH_COUNT> branches;
    bool result =
        inner_node::decode_compressed(data.data(), data.size(), branches);

    EXPECT_TRUE(result);

    // First branch should be filled
    EXPECT_EQ(branches[0], make_test_hash(0xFF));

    // Rest should be zero
    for (std::size_t i = 1; i < format::INNER_NODE_BRANCH_COUNT; ++i)
    {
        EXPECT_EQ(branches[i], Hash256::zero());
    }
}

TEST(InnerNodeFormatTest, DecodeCompressedMultipleBranches)
{
    // Branches at positions 0, 5, 15: mask = 0x8420
    // 0x8000 (pos 0) | 0x0400 (pos 5) | 0x0001 (pos 15) = 0x8401
    std::vector<uint8_t> data(2 + 3 * 32);
    data[0] = 0x84;
    data[1] = 0x01;

    // Fill first hash with 0x11
    std::fill(data.begin() + 2, data.begin() + 2 + 32, 0x11);
    // Fill second hash with 0x22
    std::fill(data.begin() + 2 + 32, data.begin() + 2 + 64, 0x22);
    // Fill third hash with 0x33
    std::fill(data.begin() + 2 + 64, data.end(), 0x33);

    std::array<Hash256, format::INNER_NODE_BRANCH_COUNT> branches;
    bool result =
        inner_node::decode_compressed(data.data(), data.size(), branches);

    EXPECT_TRUE(result);

    EXPECT_EQ(branches[0], make_test_hash(0x11));
    EXPECT_EQ(branches[5], make_test_hash(0x22));
    EXPECT_EQ(branches[15], make_test_hash(0x33));

    // Check zeros
    for (std::size_t i = 1; i < 5; ++i)
        EXPECT_EQ(branches[i], Hash256::zero());
    for (std::size_t i = 6; i < 15; ++i)
        EXPECT_EQ(branches[i], Hash256::zero());
}

TEST(InnerNodeFormatTest, DecodeCompressedInvalidSize)
{
    // Invalid: mask indicates 2 branches but only 1 hash provided
    std::vector<uint8_t> data(2 + 32);  // Should be 2 + 64
    data[0] = 0xC0;                     // Mask = 0xC000 (2 branches)
    data[1] = 0x00;

    std::array<Hash256, format::INNER_NODE_BRANCH_COUNT> branches;
    bool result =
        inner_node::decode_compressed(data.data(), data.size(), branches);

    EXPECT_FALSE(result);
}

TEST(InnerNodeFormatTest, EncodeCompressedEmpty)
{
    std::array<Hash256, format::INNER_NODE_BRANCH_COUNT> branches;
    for (auto& branch : branches)
        branch = Hash256::zero();

    std::vector<uint8_t> buffer(2 + 16 * 32);  // Max possible size
    auto size = inner_node::encode_compressed(branches, buffer.data());

    EXPECT_EQ(size, 2);  // Just the mask
    EXPECT_EQ(buffer[0], 0x00);
    EXPECT_EQ(buffer[1], 0x00);
}

TEST(InnerNodeFormatTest, EncodeCompressedSingleBranch)
{
    std::array<Hash256, format::INNER_NODE_BRANCH_COUNT> branches;
    for (auto& branch : branches)
        branch = Hash256::zero();

    branches[0] = make_test_hash(0xAA);

    std::vector<uint8_t> buffer(2 + 16 * 32);
    auto size = inner_node::encode_compressed(branches, buffer.data());

    EXPECT_EQ(size, 2 + 32);     // Mask + 1 hash
    EXPECT_EQ(buffer[0], 0x80);  // High byte of mask
    EXPECT_EQ(buffer[1], 0x00);  // Low byte of mask

    // Check hash
    for (size_t i = 2; i < 34; ++i)
    {
        EXPECT_EQ(buffer[i], 0xAA);
    }
}

TEST(InnerNodeFormatTest, RoundTripCompressed)
{
    // Create original branches
    std::array<Hash256, format::INNER_NODE_BRANCH_COUNT> original;
    for (auto& branch : original)
        branch = Hash256::zero();

    original[0] = make_test_hash(0x11);
    original[7] = make_test_hash(0x77);
    original[15] = make_test_hash(0xFF);

    // Encode
    std::vector<uint8_t> buffer(2 + 16 * 32);
    auto size = inner_node::encode_compressed(original, buffer.data());

    // Decode
    std::array<Hash256, format::INNER_NODE_BRANCH_COUNT> decoded;
    bool result = inner_node::decode_compressed(buffer.data(), size, decoded);

    EXPECT_TRUE(result);

    // Compare
    for (std::size_t i = 0; i < format::INNER_NODE_BRANCH_COUNT; ++i)
    {
        EXPECT_EQ(decoded[i], original[i]);
    }
}

TEST(InnerNodeFormatTest, DecodeFullValid)
{
    std::vector<uint8_t> data(512);

    // Fill each 32-byte segment with its index value
    for (std::size_t i = 0; i < 16; ++i)
    {
        std::fill(
            data.begin() + (i * 32),
            data.begin() + ((i + 1) * 32),
            static_cast<uint8_t>(i));
    }

    std::array<Hash256, format::INNER_NODE_BRANCH_COUNT> branches;
    bool result = inner_node::decode_full(data.data(), data.size(), branches);

    EXPECT_TRUE(result);

    for (std::size_t i = 0; i < format::INNER_NODE_BRANCH_COUNT; ++i)
    {
        EXPECT_EQ(branches[i], make_test_hash(static_cast<uint8_t>(i)));
    }
}

TEST(InnerNodeFormatTest, DecodeFullInvalidSize)
{
    std::vector<uint8_t> data(256);  // Wrong size (should be 512)

    std::array<Hash256, format::INNER_NODE_BRANCH_COUNT> branches;
    bool result = inner_node::decode_full(data.data(), data.size(), branches);

    EXPECT_FALSE(result);
}

TEST(InnerNodeFormatTest, EncodeFull)
{
    std::array<Hash256, format::INNER_NODE_BRANCH_COUNT> branches;

    for (std::size_t i = 0; i < format::INNER_NODE_BRANCH_COUNT; ++i)
    {
        branches[i] = make_test_hash(static_cast<uint8_t>(i * 16));
    }

    std::vector<uint8_t> buffer(512);
    auto size = inner_node::encode_full(branches, buffer.data());

    EXPECT_EQ(size, 512);

    // Verify each hash
    for (std::size_t i = 0; i < format::INNER_NODE_BRANCH_COUNT; ++i)
    {
        for (std::size_t j = 0; j < 32; ++j)
        {
            EXPECT_EQ(buffer[i * 32 + j], static_cast<uint8_t>(i * 16));
        }
    }
}

TEST(InnerNodeFormatTest, RoundTripFull)
{
    // Create original branches
    std::array<Hash256, format::INNER_NODE_BRANCH_COUNT> original;
    for (std::size_t i = 0; i < format::INNER_NODE_BRANCH_COUNT; ++i)
    {
        original[i] = make_test_hash(static_cast<uint8_t>(i + 100));
    }

    // Encode
    std::vector<uint8_t> buffer(512);
    auto size = inner_node::encode_full(original, buffer.data());

    // Decode
    std::array<Hash256, format::INNER_NODE_BRANCH_COUNT> decoded;
    bool result = inner_node::decode_full(buffer.data(), size, decoded);

    EXPECT_TRUE(result);

    // Compare
    for (std::size_t i = 0; i < format::INNER_NODE_BRANCH_COUNT; ++i)
    {
        EXPECT_EQ(decoded[i], original[i]);
    }
}

TEST(InnerNodeFormatTest, CountBranches)
{
    std::array<Hash256, format::INNER_NODE_BRANCH_COUNT> branches;
    for (auto& branch : branches)
        branch = Hash256::zero();

    EXPECT_EQ(inner_node::count_branches(branches), 0);

    branches[0] = make_test_hash(0x11);
    EXPECT_EQ(inner_node::count_branches(branches), 1);

    branches[5] = make_test_hash(0x55);
    branches[15] = make_test_hash(0xFF);
    EXPECT_EQ(inner_node::count_branches(branches), 3);

    for (auto& branch : branches)
        branch = make_test_hash(0xFF);
    EXPECT_EQ(inner_node::count_branches(branches), 16);
}

TEST(InnerNodeFormatTest, Zero32Helper)
{
    void const* zero_ptr = inner_node::zero32();
    EXPECT_NE(zero_ptr, nullptr);

    // Verify it's actually 32 zeros
    auto const* bytes = static_cast<uint8_t const*>(zero_ptr);
    for (std::size_t i = 0; i < 32; ++i)
    {
        EXPECT_EQ(bytes[i], 0);
    }
}
