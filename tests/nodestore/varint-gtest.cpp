#include "catl/nodestore/varint.h"
#include <array>
#include <cstdint>
#include <gtest/gtest.h>
#include <vector>

using namespace catl::nodestore;

TEST(VarintTest, WriteReadZero)
{
    std::array<std::uint8_t, 10> buf{};
    std::size_t value = 0;

    auto written = write_varint(buf.data(), value);
    EXPECT_EQ(written, 1);

    std::size_t decoded = 0;
    auto read = read_varint(buf.data(), written, decoded);
    EXPECT_EQ(read, written);
    EXPECT_EQ(decoded, value);
}

TEST(VarintTest, WriteReadSmall)
{
    std::array<std::uint8_t, 10> buf{};
    std::size_t value = 42;

    auto written = write_varint(buf.data(), value);
    EXPECT_GT(written, 0);
    EXPECT_LE(written, buf.size());

    std::size_t decoded = 0;
    auto read = read_varint(buf.data(), written, decoded);
    EXPECT_EQ(read, written);
    EXPECT_EQ(decoded, value);
}

TEST(VarintTest, WriteReadLarge)
{
    std::array<std::uint8_t, 10> buf{};
    std::size_t value = 1234567890;

    auto written = write_varint(buf.data(), value);
    EXPECT_GT(written, 0);
    EXPECT_LE(written, buf.size());

    std::size_t decoded = 0;
    auto read = read_varint(buf.data(), written, decoded);
    EXPECT_EQ(read, written);
    EXPECT_EQ(decoded, value);
}

TEST(VarintTest, WriteReadMax)
{
    std::array<std::uint8_t, varint_traits<std::size_t>::max> buf{};
    std::size_t value = std::numeric_limits<std::size_t>::max();

    auto written = write_varint(buf.data(), value);
    EXPECT_GT(written, 0);
    EXPECT_LE(written, buf.size());

    std::size_t decoded = 0;
    auto read = read_varint(buf.data(), written, decoded);
    EXPECT_EQ(read, written);
    EXPECT_EQ(decoded, value);
}

TEST(VarintTest, SizeVarint)
{
    // Base-127 varint encoding boundaries:
    // 1 byte: 0 to 126 (127^1 - 1)
    // 2 bytes: 127 to 16,128 (127^2 - 1)
    // 3 bytes: 16,129 to 2,048,382 (127^3 - 1)
    EXPECT_EQ(size_varint(0UL), 1);
    EXPECT_EQ(size_varint(1UL), 1);
    EXPECT_EQ(size_varint(126UL), 1);
    EXPECT_EQ(size_varint(127UL), 2);
    EXPECT_EQ(size_varint(128UL), 2);
    EXPECT_EQ(size_varint(16128UL), 2);
    EXPECT_EQ(size_varint(16129UL), 3);
    EXPECT_EQ(size_varint(16383UL), 3);
}

TEST(VarintTest, ReadBufferTooSmall)
{
    std::array<std::uint8_t, 10> buf{};
    std::size_t value = 1234567890;
    auto written = write_varint(buf.data(), value);

    std::size_t decoded = 0;
    // Try to read with truncated buffer
    auto read = read_varint(buf.data(), written - 1, decoded);
    EXPECT_EQ(read, 0);  // Should fail
}

TEST(VarintTest, ReadEmptyBuffer)
{
    std::size_t decoded = 0;
    auto read = read_varint(nullptr, 0, decoded);
    EXPECT_EQ(read, 0);
}

TEST(VarintTest, RoundTripMultipleValues)
{
    std::vector<std::size_t> test_values = {
        0, 1, 127, 128, 255, 256, 16383, 16384, 1000000, 1234567890};

    for (auto value : test_values)
    {
        std::array<std::uint8_t, varint_traits<std::size_t>::max> buf{};

        auto written = write_varint(buf.data(), value);
        EXPECT_EQ(written, size_varint(value));

        std::size_t decoded = 0;
        auto read = read_varint(buf.data(), written, decoded);
        EXPECT_EQ(read, written);
        EXPECT_EQ(decoded, value);
    }
}
