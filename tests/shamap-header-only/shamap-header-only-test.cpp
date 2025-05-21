#include <gtest/gtest.h>

// Include the custom traits header
#include "shamap-custom-traits.h"
#include <iostream>

// Tests for the header-only implementation
TEST(SHAMapHeaderOnlyTest, CreateEmptyMap)
{
    CustomSHAMap map;
    EXPECT_TRUE(
        map.get_hash().hex() ==
        "0000000000000000000000000000000000000000000000000000000000000000");
    EXPECT_EQ(map.get_root()->node_offset, 1337);
    std::cout << map.get_root()->node_offset << std::endl;
}
