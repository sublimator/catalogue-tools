#pragma once

// Binary trie encoding/decoding for SHAMap abbreviated trees.
//
// Format: 2-bit-per-branch headers (uint32 LE) with depth-first traversal.
// See xpop-2-py SPEC.md section 3.4.
//
// Branch types (2 bits each, branch 0 in bits 0-1, branch 1 in bits 2-3, ...):
//   00 = empty
//   01 = leaf     → [key: 32][data_len: varint][data]
//   10 = inner    → recurse (another branch header + children)
//   11 = hash     → [32 bytes] (pruned placeholder)

#include <cstddef>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <vector>

namespace catl::shamap {

/// Branch types in a binary trie header.
enum class BranchType : uint8_t {
    empty = 0,
    leaf = 1,
    inner = 2,
    hash = 3,
};

/// Encode an unsigned integer as LEB128.
inline void
leb128_encode(std::vector<uint8_t>& out, size_t value)
{
    do
    {
        uint8_t byte = static_cast<uint8_t>(value & 0x7F);
        value >>= 7;
        if (value != 0)
            byte |= 0x80;
        out.push_back(byte);
    } while (value != 0);
}

/// Decode an unsigned LEB128 integer. Advances pos.
inline size_t
leb128_decode(std::span<const uint8_t> data, size_t& pos)
{
    size_t value = 0;
    int shift = 0;
    while (pos < data.size())
    {
        uint8_t byte = data[pos++];
        value |= static_cast<size_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0)
            return value;
        shift += 7;
        if (shift >= 64)
            throw std::runtime_error("leb128: overflow");
    }
    throw std::runtime_error("leb128: unexpected end of data");
}

/// Get branch type from a 4-byte header for branch index i (0-15).
inline BranchType
get_branch_type(uint32_t header, int branch)
{
    return static_cast<BranchType>((header >> (branch * 2)) & 0x03);
}

/// Set branch type in a 4-byte header for branch index i (0-15).
inline void
set_branch_type(uint32_t& header, int branch, BranchType type)
{
    int shift = branch * 2;
    header &= ~(0x03u << shift);
    header |= (static_cast<uint32_t>(type) << shift);
}

/// Write a uint32 in little-endian.
inline void
write_u32_le(std::vector<uint8_t>& out, uint32_t v)
{
    out.push_back(static_cast<uint8_t>(v & 0xFF));
    out.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    out.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    out.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
}

/// Read a uint32 from little-endian bytes. Advances pos.
inline uint32_t
read_u32_le(std::span<const uint8_t> data, size_t& pos)
{
    if (pos + 4 > data.size())
        throw std::runtime_error("binary trie: unexpected end reading header");
    uint32_t v = static_cast<uint32_t>(data[pos]) |
        (static_cast<uint32_t>(data[pos + 1]) << 8) |
        (static_cast<uint32_t>(data[pos + 2]) << 16) |
        (static_cast<uint32_t>(data[pos + 3]) << 24);
    pos += 4;
    return v;
}

}  // namespace catl::shamap
