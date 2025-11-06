#include "catl/nodestore/lz4_codec.h"
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <gtest/gtest.h>
#include <string>
#include <vector>

using namespace catl::nodestore;

TEST(Lz4CodecTest, CompressDecompressSmall)
{
    std::string original = "Hello, World!";
    std::vector<uint8_t> compressed_buf;
    std::vector<uint8_t> decompressed_buf;

    // Compress
    auto [compressed_data, compressed_size] = lz4_compress(
        original.data(), original.size(), [&compressed_buf](size_t size) {
            compressed_buf.resize(size);
            return compressed_buf.data();
        });

    EXPECT_GT(compressed_size, 0);
    EXPECT_LE(compressed_size, compressed_buf.size());

    // Decompress
    auto [decompressed_data, decompressed_size] = lz4_decompress(
        compressed_data, compressed_size, [&decompressed_buf](size_t size) {
            decompressed_buf.resize(size);
            return decompressed_buf.data();
        });

    EXPECT_EQ(decompressed_size, original.size());
    EXPECT_EQ(
        std::memcmp(decompressed_data, original.data(), original.size()), 0);
}

TEST(Lz4CodecTest, CompressDecompressLarge)
{
    // Create a large buffer with pattern
    std::vector<uint8_t> original(10000);
    for (size_t i = 0; i < original.size(); ++i)
    {
        original[i] = static_cast<uint8_t>(i % 256);
    }

    std::vector<uint8_t> compressed_buf;
    std::vector<uint8_t> decompressed_buf;

    // Compress
    auto [compressed_data, compressed_size] = lz4_compress(
        original.data(), original.size(), [&compressed_buf](size_t size) {
            compressed_buf.resize(size);
            return compressed_buf.data();
        });

    EXPECT_GT(compressed_size, 0);
    EXPECT_LT(
        compressed_size,
        original.size());  // Should compress due to pattern

    // Decompress
    auto [decompressed_data, decompressed_size] = lz4_decompress(
        compressed_data, compressed_size, [&decompressed_buf](size_t size) {
            decompressed_buf.resize(size);
            return decompressed_buf.data();
        });

    EXPECT_EQ(decompressed_size, original.size());
    EXPECT_EQ(
        std::memcmp(decompressed_data, original.data(), original.size()), 0);
}

TEST(Lz4CodecTest, CompressDecompressZeros)
{
    // All zeros should compress very well
    std::vector<uint8_t> original(1000, 0);
    std::vector<uint8_t> compressed_buf;
    std::vector<uint8_t> decompressed_buf;

    // Compress
    auto [compressed_data, compressed_size] = lz4_compress(
        original.data(), original.size(), [&compressed_buf](size_t size) {
            compressed_buf.resize(size);
            return compressed_buf.data();
        });

    EXPECT_GT(compressed_size, 0);
    EXPECT_LT(compressed_size, original.size() / 10);  // Should compress well

    // Decompress
    auto [decompressed_data, decompressed_size] = lz4_decompress(
        compressed_data, compressed_size, [&decompressed_buf](size_t size) {
            decompressed_buf.resize(size);
            return decompressed_buf.data();
        });

    EXPECT_EQ(decompressed_size, original.size());
    EXPECT_TRUE(std::all_of(
        decompressed_buf.begin(), decompressed_buf.end(), [](uint8_t b) {
            return b == 0;
        }));
}

TEST(Lz4CodecTest, CompressDecompressRandom)
{
    // Random data should not compress well
    std::vector<uint8_t> original(1000);
    for (size_t i = 0; i < original.size(); ++i)
    {
        original[i] = static_cast<uint8_t>((i * 7919) % 256);
    }

    std::vector<uint8_t> compressed_buf;
    std::vector<uint8_t> decompressed_buf;

    // Compress
    auto [compressed_data, compressed_size] = lz4_compress(
        original.data(), original.size(), [&compressed_buf](size_t size) {
            compressed_buf.resize(size);
            return compressed_buf.data();
        });

    EXPECT_GT(compressed_size, 0);

    // Decompress
    auto [decompressed_data, decompressed_size] = lz4_decompress(
        compressed_data, compressed_size, [&decompressed_buf](size_t size) {
            decompressed_buf.resize(size);
            return decompressed_buf.data();
        });

    EXPECT_EQ(decompressed_size, original.size());
    EXPECT_EQ(
        std::memcmp(decompressed_data, original.data(), original.size()), 0);
}

TEST(Lz4CodecTest, DecompressInvalidData)
{
    std::vector<uint8_t> invalid_data = {0xFF, 0xFF, 0xFF, 0xFF};
    std::vector<uint8_t> decompressed_buf;

    // Should throw on invalid data
    EXPECT_THROW(
        lz4_decompress(
            invalid_data.data(),
            invalid_data.size(),
            [&decompressed_buf](size_t size) {
                decompressed_buf.resize(size);
                return decompressed_buf.data();
            }),
        std::runtime_error);
}

TEST(Lz4CodecTest, DecompressEmptyData)
{
    std::vector<uint8_t> empty_data;
    std::vector<uint8_t> decompressed_buf;

    // Should throw on empty data
    EXPECT_THROW(
        lz4_decompress(
            empty_data.data(),
            empty_data.size(),
            [&decompressed_buf](size_t size) {
                decompressed_buf.resize(size);
                return decompressed_buf.data();
            }),
        std::runtime_error);
}

TEST(Lz4CodecTest, CompressIncludesVarint)
{
    std::string original = "Test data";
    std::vector<uint8_t> compressed_buf;

    // Compress
    lz4_compress(
        original.data(), original.size(), [&compressed_buf](size_t size) {
            compressed_buf.resize(size);
            return compressed_buf.data();
        });

    // The compressed data should start with a varint encoding the original size
    std::size_t decoded_size = 0;
    auto varint_bytes =
        read_varint(compressed_buf.data(), compressed_buf.size(), decoded_size);

    EXPECT_GT(varint_bytes, 0);
    EXPECT_EQ(decoded_size, original.size());
}
