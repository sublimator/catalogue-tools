#pragma once

#include <cstdint>

#if __cplusplus >= 202002L && __has_include(<bit>)
#include <bit>
#endif

namespace catl::core {

/**
 * Portable bit manipulation utilities
 * 
 * These wrap compiler builtins and C++20 standard functions
 * to provide a consistent interface across compilers.
 */

/**
 * Count the number of set bits (population count)
 * @param x The value to count bits in
 * @return Number of 1 bits
 */
inline int
popcount(std::uint32_t x) noexcept
{
#if __cplusplus >= 202002L && __has_include(<bit>)
    return std::popcount(x);
#elif defined(__GNUC__) || defined(__clang__)
    return __builtin_popcount(x);
#else
    // Fallback implementation (Brian Kernighan's algorithm)
    int count = 0;
    while (x)
    {
        x &= x - 1;  // Clear the lowest set bit
        count++;
    }
    return count;
#endif
}

/**
 * Count trailing zero bits
 * @param x The value to count trailing zeros in (must be non-zero)
 * @return Number of trailing 0 bits
 */
inline int
ctz(std::uint32_t x) noexcept
{
    // Undefined behavior if x == 0, same as builtins
#if __cplusplus >= 202002L && __has_include(<bit>)
    return std::countr_zero(x);
#elif defined(__GNUC__) || defined(__clang__)
    return __builtin_ctz(x);
#else
    // Fallback implementation
    if (x == 0)
        return 32;  // Match builtin behavior for consistency
    int count = 0;
    while ((x & 1) == 0)
    {
        x >>= 1;
        count++;
    }
    return count;
#endif
}

/**
 * Count leading zero bits
 * @param x The value to count leading zeros in (must be non-zero)
 * @return Number of leading 0 bits
 */
inline int
clz(std::uint32_t x) noexcept
{
    // Undefined behavior if x == 0, same as builtins
#if __cplusplus >= 202002L && __has_include(<bit>)
    return std::countl_zero(x);
#elif defined(__GNUC__) || defined(__clang__)
    return __builtin_clz(x);
#else
    // Fallback implementation
    if (x == 0)
        return 32;
    int count = 0;
    std::uint32_t mask = 0x80000000;
    while ((x & mask) == 0)
    {
        mask >>= 1;
        count++;
    }
    return count;
#endif
}

/**
 * Find the index of the first set bit (from LSB)
 * @param x The value to search (must be non-zero)
 * @return 0-based index of first set bit
 */
inline int
first_set_bit(std::uint32_t x) noexcept
{
    return ctz(x);
}

/**
 * Count set bits in a mask up to (but not including) a given position
 * Useful for sparse array indexing
 * @param mask The bitmask
 * @param position The position to count up to
 * @return Number of set bits in positions [0, position)
 */
inline int
popcount_before(std::uint32_t mask, int position) noexcept
{
    if (position <= 0)
        return 0;
    if (position >= 32)
        return popcount(mask);
    std::uint32_t mask_before = mask & ((1u << position) - 1);
    return popcount(mask_before);
}

}  // namespace catl::core