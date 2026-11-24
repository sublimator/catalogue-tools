#pragma once

// Binary trie encoding/decoding for SHAMap trees.
//
// Compact serialization of 16-way radix tries. Used by XPRV proof format
// for map proofs (tx and state trees). An abbreviated tree with 1 leaf
// and ~20 placeholders typically encodes to ~500-700 bytes.
//
// Format: 2-bit-per-branch headers (uint32 LE) with depth-first traversal.
//
// Branch types (2 bits each, branch 0 in bits 0-1, branch 1 in bits 2-3):
//   00 = empty
//   01 = leaf     → [key: 32][data_len: varint][data: canonical binary]
//   10 = inner    → recurse (another branch header + children)
//   11 = hash     → [32 bytes] (pruned subtree placeholder)
//
// Leaf data is raw canonical XRPL binary — the verifier hashes it directly
// without re-serialization (unlike the JSON trie which requires decode→
// re-encode). This makes binary tries both smaller and faster to verify.
//
// See also: sink_trie_binary() on SHAMapInnerNodeT, trie_binary() /
// from_trie_binary() on SHAMapT.

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

/// Encode an unsigned integer as LEB128. All lengths and counts in the
/// XPRV format fit in uint32; larger values are a caller bug.
inline void
leb128_encode(std::vector<uint8_t>& out, size_t value)
{
    if (value > 0xFFFFFFFFull)
        throw std::runtime_error("leb128: value exceeds uint32");
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
///
/// Strict canonical decoding: rejects non-minimal encodings (redundant
/// trailing continuation bytes), varints longer than 5 bytes, and values
/// that exceed uint32 — one value, one byte sequence, so proofs can be
/// hashed/cached by their bytes.
inline size_t
leb128_decode(std::span<const uint8_t> data, size_t& pos)
{
    uint64_t value = 0;
    int shift = 0;
    size_t count = 0;
    while (pos < data.size())
    {
        uint8_t byte = data[pos++];
        if (++count > 5)
            throw std::runtime_error("leb128: varint longer than 5 bytes");
        value |= static_cast<uint64_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0)
        {
            if (count > 1 && byte == 0)
                throw std::runtime_error("leb128: non-minimal encoding");
            if (value > 0xFFFFFFFFull)
                throw std::runtime_error("leb128: value exceeds uint32");
            return static_cast<size_t>(value);
        }
        shift += 7;
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

// Checks one inner node of a binary trie and everything beneath it, starting
// at `pos`, and returns the number of leaves there. Advances `pos` past the
// node. `path` is the branch nibbles from the root, one per byte.
//
// The rules are the JSON trie's (json-trie.h, XPRV §2.3, §3.7): an inner node
// has a leaf beneath it, a hash placeholder is not all zero, a leaf sits
// where its key puts it, and no inner node lies deeper than 63.
inline size_t
validate_binary_trie_node(
    std::span<const uint8_t> data,
    size_t& pos,
    std::vector<uint8_t>& path)
{
    if (path.size() >= 64)
        throw std::runtime_error("binary trie: depth exceeds maximum (64)");
    auto const header = read_u32_le(data, pos);
    size_t leaves = 0;
    for (int branch = 0; branch < 16; ++branch)
    {
        auto const type = get_branch_type(header, branch);
        if (type == BranchType::empty)
            continue;
        if (type == BranchType::inner)
        {
            path.push_back(static_cast<uint8_t>(branch));
            leaves += validate_binary_trie_node(data, pos, path);
            path.pop_back();
            continue;
        }
        if (pos > data.size() || data.size() - pos < 32)
            throw std::runtime_error("binary trie: unexpected end of data");
        auto const hash = data.subspan(pos, 32);
        pos += 32;
        if (type == BranchType::hash)
        {
            bool zero = true;
            for (auto const byte : hash)
                zero = zero && byte == 0;
            if (zero)
                throw std::runtime_error(
                    "binary trie: placeholder is a zero hash; an empty "
                    "branch is absent");
            continue;
        }
        // A leaf: its key, then its length-prefixed bytes.
        path.push_back(static_cast<uint8_t>(branch));
        for (size_t i = 0; i < path.size(); ++i)
        {
            auto const byte = hash[i / 2];
            auto const nibble = i % 2 == 0 ? byte >> 4 : byte & 0x0F;
            if (nibble != path[i])
                throw std::runtime_error(
                    "binary trie: leaf key does not match its trie path");
        }
        path.pop_back();
        auto const size = leb128_decode(data, pos);
        if (size > data.size() - pos)
            throw std::runtime_error("binary trie: unexpected end of data");
        pos += size;
        ++leaves;
    }
    if (leaves == 0)
        throw std::runtime_error("binary trie: inner node contains no leaves");
    return leaves;
}

/// Checks a whole binary trie payload: one root inner node, consumed exactly.
inline void
validate_binary_trie(std::span<const uint8_t> data)
{
    size_t pos = 0;
    std::vector<uint8_t> path;
    validate_binary_trie_node(data, pos, path);
    if (pos != data.size())
        throw std::runtime_error("binary trie: trailing bytes");
}

}  // namespace catl::shamap
