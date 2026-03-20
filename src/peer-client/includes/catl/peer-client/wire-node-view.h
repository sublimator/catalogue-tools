#pragma once

#include <catl/core/types.h>

#include <cassert>
#include <cstdint>
#include <cstring>
#include <span>

namespace catl::peer_client {

//------------------------------------------------------------------------------
// Wire type — last byte of every SHAMap node blob on the wire
//------------------------------------------------------------------------------

enum class WireType : uint8_t {
    Transaction = 0,          // tx blob (no metadata)
    AccountState = 1,         // serialized ledger entry (SLE)
    Inner = 2,                // uncompressed: 16 × 32-byte hashes
    CompressedInner = 3,      // compressed: [hash][branch] pairs
    TransactionWithMeta = 4,  // tx blob + metadata
};

//------------------------------------------------------------------------------
// WireInnerView — zero-copy view into an inner node blob
//
// Handles both compressed (type 3) and uncompressed (type 2) formats.
// Borrows from the underlying buffer — caller ensures lifetime.
//------------------------------------------------------------------------------

/// These views are intentionally stateless — no cached/computed fields.
/// They're cheap to construct (just a span) and meant to be short-lived.
/// If you need branch_mask() or child_hash() repeatedly, cache locally:
///
///   auto inner = node.inner();
///   auto mask = inner.branch_mask();  // compute once, reuse
///

class WireInnerView
{
    std::span<const uint8_t> data_;  // includes the wire type byte at end
    WireType type_;

public:
    WireInnerView(std::span<const uint8_t> data, WireType type)
        : data_(data), type_(type)
    {
        assert(type == WireType::Inner || type == WireType::CompressedInner);
    }

    WireType
    wire_type() const
    {
        return type_;
    }

    bool
    is_compressed() const
    {
        return type_ == WireType::CompressedInner;
    }

    /// Get child hash for a branch (0-15).
    /// Returns a Key view (zero-copy for uncompressed, points into blob).
    /// Returns nullptr Key if the branch is empty.
    /// For an owned copy: child_hash(n).to_hash()
    Key
    child_hash(int branch) const
    {
        assert(branch >= 0 && branch < 16);

        if (type_ == WireType::Inner)
        {
            // Uncompressed: 16 × 32 bytes + 1 type byte = 513 bytes
            // Branch i starts at offset i * 32
            return Key(data_.data() + branch * 32);
        }
        else
        {
            // Compressed: [hash0][branch0][hash1][branch1]...[typebyte]
            // Each entry is 33 bytes (32 hash + 1 branch number)
            auto body = data_.subspan(0, data_.size() - 1);
            for (size_t pos = 0; pos + 33 <= body.size(); pos += 33)
            {
                if (body[pos + 32] == static_cast<uint8_t>(branch))
                {
                    return Key(body.data() + pos);
                }
            }
            return Key(Hash256::zero().data());
        }
    }

    /// Bitmask of populated branches (bit N set = branch N has a child).
    uint16_t
    branch_mask() const
    {
        uint16_t mask = 0;
        if (type_ == WireType::Inner)
        {
            static const Hash256 zero{};
            for (int i = 0; i < 16; ++i)
            {
                if (std::memcmp(data_.data() + i * 32, zero.data(), 32) != 0)
                {
                    mask |= (1u << i);
                }
            }
        }
        else
        {
            auto body = data_.subspan(0, data_.size() - 1);
            for (size_t pos = 0; pos + 33 <= body.size(); pos += 33)
            {
                mask |= (1u << body[pos + 32]);
            }
        }
        return mask;
    }

    /// Number of non-empty branches.
    int
    child_count() const
    {
        return __builtin_popcount(branch_mask());
    }

    /// Iterate non-empty branches as (branch_index, Key) pairs.
    /// Calls fn(int branch, Key hash) for each. Key is a zero-copy view.
    template <typename Fn>
    void
    for_each_child(Fn&& fn) const
    {
        if (type_ == WireType::Inner)
        {
            for (int i = 0; i < 16; ++i)
            {
                auto const* p = data_.data() + i * 32;
                if (std::memcmp(p, Hash256::zero().data(), 32) != 0)
                {
                    fn(i, Key(p));
                }
            }
        }
        else
        {
            auto body = data_.subspan(0, data_.size() - 1);
            for (size_t pos = 0; pos + 33 <= body.size(); pos += 33)
            {
                int branch = body[pos + 32];
                fn(branch, Key(body.data() + pos));
            }
        }
    }
};

//------------------------------------------------------------------------------
// WireLeafView — zero-copy view into a leaf node blob
//
// Borrows from the underlying buffer — caller ensures lifetime.
//------------------------------------------------------------------------------

class WireLeafView
{
    std::span<const uint8_t> data_;  // includes wire type byte at end
    WireType type_;

public:
    WireLeafView(std::span<const uint8_t> data, WireType type)
        : data_(data), type_(type)
    {
        assert(
            type == WireType::Transaction ||
            type == WireType::AccountState ||
            type == WireType::TransactionWithMeta);
    }

    WireType
    wire_type() const
    {
        return type_;
    }

    bool
    is_transaction() const
    {
        return type_ == WireType::Transaction;
    }

    bool
    is_account_state() const
    {
        return type_ == WireType::AccountState;
    }

    bool
    is_transaction_with_meta() const
    {
        return type_ == WireType::TransactionWithMeta;
    }

    /// The item data (everything except the trailing wire type byte).
    std::span<const uint8_t>
    data() const
    {
        if (data_.size() < 1)
            return {};
        return data_.subspan(0, data_.size() - 1);
    }
};

//------------------------------------------------------------------------------
// WireNodeView — entry point, determines type and narrows
//------------------------------------------------------------------------------

class WireNodeView
{
    std::span<const uint8_t> data_;

public:
    explicit WireNodeView(std::span<const uint8_t> data) : data_(data) {}

    /// The wire type byte (last byte of the blob).
    WireType
    type() const
    {
        assert(!data_.empty());
        return static_cast<WireType>(data_.back());
    }

    bool
    is_inner() const
    {
        auto t = type();
        return t == WireType::Inner || t == WireType::CompressedInner;
    }

    bool
    is_leaf() const
    {
        return !is_inner();
    }

    /// Narrow to inner node view. Only valid when is_inner().
    WireInnerView
    inner() const
    {
        assert(is_inner());
        return WireInnerView(data_, type());
    }

    /// Narrow to leaf node view. Only valid when is_leaf().
    WireLeafView
    leaf() const
    {
        assert(is_leaf());
        return WireLeafView(data_, type());
    }

    /// Raw data including wire type byte.
    std::span<const uint8_t>
    raw() const
    {
        return data_;
    }
};

}  // namespace catl::peer_client
