#include "catl/nodestore/node_types.h"
#include <cstdint>
#include <gtest/gtest.h>
#include <string>

using namespace catl::nodestore;

TEST(NodeTypesTest, EnumValues)
{
    // Verify the enum values match the nodestore format
    EXPECT_EQ(static_cast<std::uint32_t>(node_type::hot_unknown), 0);
    EXPECT_EQ(static_cast<std::uint32_t>(node_type::hot_ledger), 1);
    EXPECT_EQ(static_cast<std::uint32_t>(node_type::hot_account_node), 3);
    EXPECT_EQ(static_cast<std::uint32_t>(node_type::hot_transaction_node), 4);
    EXPECT_EQ(static_cast<std::uint32_t>(node_type::hot_dummy), 512);

    // Pinned variants
    EXPECT_EQ(static_cast<std::uint32_t>(node_type::pinned_account_node), 1003);
    EXPECT_EQ(
        static_cast<std::uint32_t>(node_type::pinned_transaction_node), 1004);
    EXPECT_EQ(static_cast<std::uint32_t>(node_type::pinned_ledger), 1005);
}

TEST(NodeTypesTest, ToString)
{
    EXPECT_STREQ(node_type_to_string(node_type::hot_unknown), "hot_unknown");
    EXPECT_STREQ(node_type_to_string(node_type::hot_ledger), "hot_ledger");
    EXPECT_STREQ(
        node_type_to_string(node_type::hot_account_node), "hot_account_node");
    EXPECT_STREQ(
        node_type_to_string(node_type::hot_transaction_node),
        "hot_transaction_node");
    EXPECT_STREQ(node_type_to_string(node_type::hot_dummy), "hot_dummy");
    EXPECT_STREQ(
        node_type_to_string(node_type::pinned_account_node),
        "pinned_account_node");
    EXPECT_STREQ(
        node_type_to_string(node_type::pinned_transaction_node),
        "pinned_transaction_node");
    EXPECT_STREQ(
        node_type_to_string(node_type::pinned_ledger), "pinned_ledger");
}

TEST(NodeTypesTest, CastFromRawValue)
{
    // Test casting from raw byte values (as read from nodestore)
    std::uint8_t raw_type = 3;
    auto type = static_cast<node_type>(raw_type);
    EXPECT_EQ(type, node_type::hot_account_node);
    EXPECT_STREQ(node_type_to_string(type), "hot_account_node");
}

TEST(NodeTypesTest, UnknownType)
{
    // Test with an invalid type value
    auto type = static_cast<node_type>(999);
    EXPECT_STREQ(node_type_to_string(type), "unknown");
}
