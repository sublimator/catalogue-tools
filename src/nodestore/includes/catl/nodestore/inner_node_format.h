#pragma once

#include "catl/core/types.h"
#include "catl/nodestore/compression_types.h"
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace catl::nodestore {

/**
 * Inner node v1 format constants and helpers.
 *
 * Inner nodes represent 16-way branching in the SHAMap Merkle tree.
 * Two storage formats are supported:
 *
 * Type 2 (compressed): [2-byte bitmask][N * 32-byte hashes]
 *   - Bitmask indicates which of 16 branches are present
 *   - Only non-zero branch hashes are stored
 *   - Most space-efficient for sparse inner nodes
 *
 * Type 3 (full): [16 * 32-byte hashes]
 *   - All 16 branch hashes stored (zero for empty branches)
 *   - Used when 16 branches are present
 */
namespace inner_node {

/**
 * Hash prefix used in inner node headers.
 * From rippled's HashPrefix::innerNode.
 */
constexpr std::uint32_t HASH_PREFIX_INNER_NODE = 0x4D494E53;  // 'MINS'

/**
 * Returns pointer to 32 zero bytes (canonical empty hash).
 */
inline void const*
zero32()
{
    return Hash256::zero().data();
}

/**
 * Decode a compressed inner node (type 2).
 *
 * Format: [2-byte bitmask (big-endian)][N * 32-byte hashes]
 * where N = popcount(bitmask)
 *
 * @param data Pointer to compressed inner node data
 * @param size Size of data
 * @param branches Output array of 16 hashes (zero for empty branches)
 * @return true on success, false on invalid format
 */
inline bool
decode_compressed(
    void const* data,
    std::size_t size,
    std::array<Hash256, format::INNER_NODE_BRANCH_COUNT>& branches)
{
    if (size < 2)
        return false;

    auto const* bytes = static_cast<std::uint8_t const*>(data);

    // Read bitmask (big-endian)
    std::uint16_t mask = (static_cast<std::uint16_t>(bytes[0]) << 8) | bytes[1];

    // Count expected hashes
    auto const hash_count = __builtin_popcount(mask);
    auto const expected_size = 2 + (hash_count * format::INNER_NODE_HASH_SIZE);

    if (size != expected_size)
        return false;

    // Decode branches
    // Canonical format: branch i = bit (15 - i)
    auto const* hash_data = bytes + 2;
    std::size_t hash_index = 0;

    for (std::size_t i = 0; i < format::INNER_NODE_BRANCH_COUNT; ++i)
    {
        std::uint16_t bit = 1u << (15 - i);
        if (mask & bit)
        {
            branches[i] = Hash256(
                hash_data + (hash_index * format::INNER_NODE_HASH_SIZE));
            ++hash_index;
        }
        else
        {
            branches[i] = Hash256::zero();
        }
    }

    return true;
}

/**
 * Encode a compressed inner node (type 2).
 *
 * Builds bitmask and stores only non-zero branch hashes.
 *
 * @param branches Array of 16 branch hashes
 * @param buffer Output buffer (must be at least 2 + N*32 bytes)
 * @return Number of bytes written
 */
inline std::size_t
encode_compressed(
    std::array<Hash256, format::INNER_NODE_BRANCH_COUNT> const& branches,
    void* buffer)
{
    auto* bytes = static_cast<std::uint8_t*>(buffer);

    // Build bitmask
    // Canonical format: branch i = bit (15 - i)
    std::uint16_t mask = 0;
    for (std::size_t i = 0; i < format::INNER_NODE_BRANCH_COUNT; ++i)
    {
        if (branches[i] != Hash256::zero())
            mask |= (1u << (15 - i));
    }

    // Write bitmask (big-endian)
    bytes[0] = static_cast<std::uint8_t>(mask >> 8);
    bytes[1] = static_cast<std::uint8_t>(mask & 0xFF);

    // Write non-zero hashes
    // Canonical format: branch i = bit (15 - i)
    auto* hash_data = bytes + 2;
    std::size_t hash_index = 0;

    for (std::size_t i = 0; i < format::INNER_NODE_BRANCH_COUNT; ++i)
    {
        if (mask & (1u << (15 - i)))
        {
            std::memcpy(
                hash_data + (hash_index * format::INNER_NODE_HASH_SIZE),
                branches[i].data(),
                format::INNER_NODE_HASH_SIZE);
            ++hash_index;
        }
    }

    return 2 + (hash_index * format::INNER_NODE_HASH_SIZE);
}

/**
 * Decode a full inner node (type 3).
 *
 * Format: [16 * 32-byte hashes]
 *
 * @param data Pointer to full inner node data
 * @param size Size of data (must be 512)
 * @param branches Output array of 16 hashes
 * @return true on success, false on invalid format
 */
inline bool
decode_full(
    void const* data,
    std::size_t size,
    std::array<Hash256, format::INNER_NODE_BRANCH_COUNT>& branches)
{
    if (size != format::INNER_NODE_HASH_ARRAY_SIZE)
        return false;

    auto const* hash_data = static_cast<std::uint8_t const*>(data);

    for (std::size_t i = 0; i < format::INNER_NODE_BRANCH_COUNT; ++i)
    {
        branches[i] = Hash256(hash_data + (i * format::INNER_NODE_HASH_SIZE));
    }

    return true;
}

/**
 * Encode a full inner node (type 3).
 *
 * Stores all 16 branch hashes (zero for empty branches).
 *
 * @param branches Array of 16 branch hashes
 * @param buffer Output buffer (must be at least 512 bytes)
 * @return Number of bytes written (always 512)
 */
inline std::size_t
encode_full(
    std::array<Hash256, format::INNER_NODE_BRANCH_COUNT> const& branches,
    void* buffer)
{
    auto* hash_data = static_cast<std::uint8_t*>(buffer);

    for (std::size_t i = 0; i < format::INNER_NODE_BRANCH_COUNT; ++i)
    {
        std::memcpy(
            hash_data + (i * format::INNER_NODE_HASH_SIZE),
            branches[i].data(),
            format::INNER_NODE_HASH_SIZE);
    }

    return format::INNER_NODE_HASH_ARRAY_SIZE;
}

/**
 * Count non-zero branches in a branch array.
 */
inline std::size_t
count_branches(
    std::array<Hash256, format::INNER_NODE_BRANCH_COUNT> const& branches)
{
    std::size_t count = 0;
    for (auto const& branch : branches)
    {
        if (branch != Hash256::zero())
            ++count;
    }
    return count;
}

}  // namespace inner_node

}  // namespace catl::nodestore
