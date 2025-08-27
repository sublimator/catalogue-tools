#include "catl/core/bit-utils.h"
#include <gtest/gtest.h>
#include <limits>

using namespace catl::core;

TEST(BitUtils, PopcountBasic)
{
    EXPECT_EQ(popcount(0u), 0);
    EXPECT_EQ(popcount(1u), 1);
    EXPECT_EQ(popcount(0b111u), 3);
    EXPECT_EQ(popcount(0b1010u), 2);
    EXPECT_EQ(popcount(0xFF), 8);
    EXPECT_EQ(popcount(0xFFFF), 16);
    EXPECT_EQ(popcount(0xFFFFFFFF), 32);
}

TEST(BitUtils, PopcountPowerOfTwo)
{
    for (int i = 0; i < 32; ++i)
    {
        EXPECT_EQ(popcount(1u << i), 1);
    }
}

TEST(BitUtils, PopcountAlternatingBits)
{
    EXPECT_EQ(popcount(0x55555555), 16); // 01010101...
    EXPECT_EQ(popcount(0xAAAAAAAA), 16); // 10101010...
}

TEST(BitUtils, CtzBasic)
{
    EXPECT_EQ(ctz(1u), 0);
    EXPECT_EQ(ctz(2u), 1);
    EXPECT_EQ(ctz(4u), 2);
    EXPECT_EQ(ctz(8u), 3);
    EXPECT_EQ(ctz(0x80000000), 31);
}

TEST(BitUtils, CtzMultipleBits)
{
    EXPECT_EQ(ctz(0b1100), 2);  // First set bit at position 2
    EXPECT_EQ(ctz(0b11000), 3); // First set bit at position 3
    EXPECT_EQ(ctz(0xFF00), 8);  // First set bit at position 8
}

TEST(BitUtils, CtzAllBitsSet)
{
    EXPECT_EQ(ctz(0xFFFFFFFF), 0); // First bit is set
}

TEST(BitUtils, ClzBasic)
{
    EXPECT_EQ(clz(1u), 31);
    EXPECT_EQ(clz(2u), 30);
    EXPECT_EQ(clz(4u), 29);
    EXPECT_EQ(clz(0x80000000), 0);
    EXPECT_EQ(clz(0x40000000), 1);
    EXPECT_EQ(clz(0x20000000), 2);
}

TEST(BitUtils, ClzMultipleBits)
{
    EXPECT_EQ(clz(0xFF), 24);       // Leading zeros before 0xFF
    EXPECT_EQ(clz(0xFFFF), 16);     // Leading zeros before 0xFFFF
    EXPECT_EQ(clz(0xFFFFFF), 8);    // Leading zeros before 0xFFFFFF
    EXPECT_EQ(clz(0xFFFFFFFF), 0);  // No leading zeros
}

TEST(BitUtils, FirstSetBit)
{
    EXPECT_EQ(first_set_bit(1u), 0);
    EXPECT_EQ(first_set_bit(2u), 1);
    EXPECT_EQ(first_set_bit(4u), 2);
    EXPECT_EQ(first_set_bit(0b1100), 2);
    EXPECT_EQ(first_set_bit(0x80000000), 31);
}

TEST(BitUtils, FirstSetBitEquivalentToCtz)
{
    for (uint32_t i = 1; i < 100; ++i)
    {
        EXPECT_EQ(first_set_bit(i), ctz(i));
    }
}

TEST(BitUtils, PopcountBeforeBasic)
{
    uint32_t mask = 0b11111111;
    EXPECT_EQ(popcount_before(mask, 0), 0);
    EXPECT_EQ(popcount_before(mask, 1), 1);
    EXPECT_EQ(popcount_before(mask, 2), 2);
    EXPECT_EQ(popcount_before(mask, 8), 8);
    EXPECT_EQ(popcount_before(mask, 9), 8);
    EXPECT_EQ(popcount_before(mask, 32), 8);
}

TEST(BitUtils, PopcountBeforeSparse)
{
    uint32_t mask = 0b10101010; // Bits set at positions 1, 3, 5, 7
    EXPECT_EQ(popcount_before(mask, 0), 0);
    EXPECT_EQ(popcount_before(mask, 1), 0);
    EXPECT_EQ(popcount_before(mask, 2), 1);
    EXPECT_EQ(popcount_before(mask, 3), 1);
    EXPECT_EQ(popcount_before(mask, 4), 2);
    EXPECT_EQ(popcount_before(mask, 5), 2);
    EXPECT_EQ(popcount_before(mask, 6), 3);
    EXPECT_EQ(popcount_before(mask, 7), 3);
    EXPECT_EQ(popcount_before(mask, 8), 4);
}

TEST(BitUtils, PopcountBeforeEdgeCases)
{
    EXPECT_EQ(popcount_before(0xFFFFFFFF, -1), 0);
    EXPECT_EQ(popcount_before(0xFFFFFFFF, 0), 0);
    EXPECT_EQ(popcount_before(0xFFFFFFFF, 32), 32);
    EXPECT_EQ(popcount_before(0xFFFFFFFF, 33), 32);
    EXPECT_EQ(popcount_before(0, 16), 0);
}

TEST(BitUtils, PopcountBeforeHighBits)
{
    uint32_t mask = 0xFFFF0000; // Upper 16 bits set
    EXPECT_EQ(popcount_before(mask, 0), 0);
    EXPECT_EQ(popcount_before(mask, 16), 0);
    EXPECT_EQ(popcount_before(mask, 17), 1);
    EXPECT_EQ(popcount_before(mask, 32), 16);
}

TEST(BitUtils, CombinedOperations)
{
    // Test that operations work together correctly
    uint32_t value = 0b11001000;
    
    // Should have 3 bits set
    EXPECT_EQ(popcount(value), 3);
    
    // First set bit at position 3
    EXPECT_EQ(ctz(value), 3);
    EXPECT_EQ(first_set_bit(value), 3);
    
    // Leading zeros
    EXPECT_EQ(clz(value), 24);
}

TEST(BitUtils, RealWorldSparseArray)
{
    // Simulate a sparse array scenario
    uint32_t children_mask = 0b10010110; // Children at indices 1, 2, 4, 7
    
    // Count total children
    EXPECT_EQ(popcount(children_mask), 4);
    
    // Find position of child at logical index 4
    // Should be at physical position 2 (0-based)
    EXPECT_EQ(popcount_before(children_mask, 4), 2);
    
    // Find position of child at logical index 7
    // Should be at physical position 3 (0-based)
    EXPECT_EQ(popcount_before(children_mask, 7), 3);
    
    // Iterate through set bits
    uint32_t remaining = children_mask;
    int expected_indices[] = {1, 2, 4, 7};
    int idx = 0;
    while (remaining)
    {
        int bit_pos = ctz(remaining);
        EXPECT_EQ(bit_pos, expected_indices[idx++]);
        remaining &= remaining - 1; // Clear lowest set bit
    }
}

TEST(BitUtils, MaxValues)
{
    // Test with maximum values
    EXPECT_EQ(popcount(std::numeric_limits<uint32_t>::max()), 32);
    EXPECT_EQ(ctz(std::numeric_limits<uint32_t>::max()), 0);
    EXPECT_EQ(clz(std::numeric_limits<uint32_t>::max()), 0);
}