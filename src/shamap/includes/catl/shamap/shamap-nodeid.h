#pragma once

#include "catl/core/types.h"
#include <cstdint>
#include <cstring>
#include <span>

namespace catl::shamap {

/// Position in a SHAMap tree.
///
/// 33 bytes: 32-byte path (nibbles encoding branch choices from root)
///         + 1-byte depth (0 = root, max 64 = leaf at full depth).
///
/// The path's first `depth` nibbles are meaningful; the rest are zero.
/// Two SHAMapNodeIDs with the same path but different depths refer to
/// different nodes (an inner at depth 3 vs a leaf at depth 5).
class SHAMapNodeID
{
    uint8_t data_[33] = {};

public:
    SHAMapNodeID() = default;

    /// Construct from raw 33-byte wire data.
    explicit SHAMapNodeID(std::span<const uint8_t> raw)
    {
        if (raw.size() >= 33)
            std::memcpy(data_, raw.data(), 33);
    }

    /// Construct from separate path + depth.
    SHAMapNodeID(Hash256 const& path, uint8_t depth)
    {
        std::memcpy(data_, path.data(), 32);
        data_[32] = depth;
    }

    /// Root node (depth 0, all-zero path).
    static SHAMapNodeID
    root()
    {
        return {};
    }

    uint8_t
    depth() const
    {
        return data_[32];
    }

    /// The 32-byte path (first `depth` nibbles are meaningful).
    Key
    path() const
    {
        return Key(data_);
    }

    /// Extract nibble at a given depth (0 = high nibble of byte 0).
    int
    nibble_at(int d) const
    {
        int byte_idx = d / 2;
        if (d % 2 == 0)
            return (data_[byte_idx] >> 4) & 0xF;
        else
            return data_[byte_idx] & 0xF;
    }

    /// Get the child NodeID for a given branch at this node's depth.
    SHAMapNodeID
    child(int branch) const
    {
        SHAMapNodeID c;
        std::memcpy(c.data_, data_, 33);
        int d = depth();
        int byte_idx = d / 2;
        if (d % 2 == 0)
            c.data_[byte_idx] = (c.data_[byte_idx] & 0x0F) | (branch << 4);
        else
            c.data_[byte_idx] = (c.data_[byte_idx] & 0xF0) | (branch & 0xF);
        c.data_[32] = d + 1;
        return c;
    }

    /// Raw 33-byte representation.
    std::span<const uint8_t, 33>
    raw() const
    {
        return std::span<const uint8_t, 33>(data_, 33);
    }

    /// Wire format string (for peer protocol).
    std::string
    to_wire() const
    {
        std::string result;
        result.resize(33);
        std::memcpy(result.data(), data_, 33);
        return result;
    }

    bool
    operator==(SHAMapNodeID const& other) const
    {
        return std::memcmp(data_, other.data_, 33) == 0;
    }

    bool
    operator<(SHAMapNodeID const& other) const
    {
        return std::memcmp(data_, other.data_, 33) < 0;
    }
};

}  // namespace catl::shamap
