#include "catl/nodestore/compression_types.h"
#include <gtest/gtest.h>

using namespace catl::nodestore;

TEST(CompressionTypesTest, EnumValues)
{
    // Verify the enum values match the codec format
    EXPECT_EQ(static_cast<std::size_t>(compression_type::uncompressed), 0);
    EXPECT_EQ(static_cast<std::size_t>(compression_type::lz4), 1);
    EXPECT_EQ(
        static_cast<std::size_t>(compression_type::inner_node_compressed), 2);
    EXPECT_EQ(static_cast<std::size_t>(compression_type::inner_node_full), 3);
}

TEST(CompressionTypesTest, ToString)
{
    EXPECT_STREQ(
        compression_type_to_string(compression_type::uncompressed),
        "uncompressed");
    EXPECT_STREQ(compression_type_to_string(compression_type::lz4), "lz4");
    EXPECT_STREQ(
        compression_type_to_string(compression_type::inner_node_compressed),
        "inner_node_compressed");
    EXPECT_STREQ(
        compression_type_to_string(compression_type::inner_node_full),
        "inner_node_full");
}

TEST(CompressionTypesTest, UnknownType)
{
    auto type = static_cast<compression_type>(999);
    EXPECT_STREQ(compression_type_to_string(type), "unknown");
}

TEST(CompressionTypesTest, FormatConstants)
{
    // Verify format constants
    EXPECT_EQ(format::INNER_NODE_V1_SIZE, 525);
    EXPECT_EQ(format::INNER_NODE_HASH_ARRAY_SIZE, 512);
    EXPECT_EQ(format::INNER_NODE_HASH_SIZE, 32);
    EXPECT_EQ(format::INNER_NODE_BRANCH_COUNT, 16);
    EXPECT_EQ(format::NODEOBJECT_HEADER_SIZE, 9);

    // Verify relationships
    EXPECT_EQ(
        format::INNER_NODE_HASH_ARRAY_SIZE,
        format::INNER_NODE_HASH_SIZE * format::INNER_NODE_BRANCH_COUNT);
}
