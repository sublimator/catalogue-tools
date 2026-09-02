#pragma once

// Peer wire form <-> NodeStore on-disk form.
//
// Peers speak rippled's SHAMapTreeNode::serializeForWire:
//
//   inner, full        [16 x 32 hashes][0x02]
//   inner, compressed  [hash32][branch u8] x n [0x03]
//   account state leaf [item][key32][0x01]
//   tx with meta leaf  [item][key32][0x04]
//   tx, no meta        [item][0x00]            (not stored in ledgers; refused)
//
// The store speaks the on-disk form in node_blob.h. The logical hash of a
// node is the same in both worlds: SHA-512/256 over the decoded payload,
// which for a leaf is [prefix][item][key] and for an inner is
// ["MIN\0"][16 x 32]. Conversion never changes it, and wire_to_ondisk
// returns it so the caller can check it against the hash the parent named
// before storing anything.

#include "catl/crypto/sha512-half-hasher.h"
#include "catl/nodestore/node_blob.h"

#include <array>
#include <cstdint>
#include <cstring>
#include <span>
#include <stdexcept>
#include <vector>

namespace catl::nodestore {

enum class wire_type : std::uint8_t {
    transaction = 0,
    account_state = 1,
    inner = 2,
    compressed_inner = 3,
    transaction_with_meta = 4,
};

/// Which tree a leaf belongs to; decides its hash prefix and node type.
enum class tree_kind { state, transaction };

namespace wire_detail {

inline constexpr std::array<std::uint8_t, 4> kLeafPrefix = {'M', 'L', 'N', 0};
inline constexpr std::array<std::uint8_t, 4> kTxNodePrefix = {'S', 'N', 'D', 0};

inline Hash256
sha512_half(std::span<std::uint8_t const> bytes)
{
    catl::crypto::Sha512HalfHasher h;
    h.update(bytes.data(), bytes.size());
    return h.finalize();
}

/// Sixteen slot hashes from either inner wire layout.
inline std::array<Hash256, 16>
wire_inner_hashes(std::span<std::uint8_t const> wire)
{
    if (wire.empty())
        throw std::runtime_error("wire inner: empty");
    auto const type = static_cast<wire_type>(wire.back());
    auto const body = wire.subspan(0, wire.size() - 1);
    std::array<Hash256, 16> out;
    out.fill(Hash256::zero());
    if (type == wire_type::inner)
    {
        if (body.size() != 512)
            throw std::runtime_error("wire inner: full inner is not 512 bytes");
        for (int i = 0; i < 16; ++i)
            out[static_cast<std::size_t>(i)] = Hash256(body.data() + 32 * i);
        return out;
    }
    if (type != wire_type::compressed_inner)
        throw std::runtime_error("wire inner: not an inner type byte");
    if (body.size() % 33 != 0 || body.empty())
        throw std::runtime_error("wire inner: bad compressed inner size");
    std::uint16_t seen = 0;
    for (std::size_t pos = 0; pos < body.size(); pos += 33)
    {
        auto const branch = body[pos + 32];
        if (branch >= 16)
            throw std::runtime_error("wire inner: branch out of range");
        if (seen & (1u << branch))
            throw std::runtime_error("wire inner: duplicate branch");
        seen |= static_cast<std::uint16_t>(1u << branch);
        Hash256 const h(body.data() + pos);
        if (h == Hash256::zero())
            throw std::runtime_error("wire inner: zero hash in compressed inner");
        out[branch] = h;
    }
    return out;
}

}  // namespace wire_detail

/// A converted node: its on-disk bytes and its logical hash.
struct converted_node
{
    node_blob ondisk;
    Hash256 hash;
    bool is_inner = false;
};

/// True if the wire node is an inner (type byte 2 or 3).
inline bool
is_wire_inner(std::span<std::uint8_t const> wire)
{
    if (wire.empty())
        return false;
    auto const t = static_cast<wire_type>(wire.back());
    return t == wire_type::inner || t == wire_type::compressed_inner;
}

/// Wire node (type byte included) to the on-disk form, plus its hash.
/// `tree` decides a leaf's prefix and node type; inners ignore it.
inline converted_node
wire_to_ondisk(std::span<std::uint8_t const> wire, tree_kind tree)
{
    if (wire.empty())
        throw std::runtime_error("wire_to_ondisk: empty");
    converted_node out;
    auto const type = static_cast<wire_type>(wire.back());
    switch (type)
    {
        case wire_type::inner:
        case wire_type::compressed_inner: {
            detail::hash_array_inner_source src;
            src.branches = wire_detail::wire_inner_hashes(wire);
            out.ondisk = nodeobject_compress(src);
            // Hash over ["MIN\0"][16 x 32]: the decoded payload.
            std::vector<std::uint8_t> payload;
            payload.reserve(4 + 512);
            payload.insert(
                payload.end(),
                inner_node::HASH_PREFIX_INNER_NODE_BYTES.begin(),
                inner_node::HASH_PREFIX_INNER_NODE_BYTES.end());
            for (auto const& h : src.branches)
                payload.insert(payload.end(), h.data(), h.data() + 32);
            out.hash = wire_detail::sha512_half(payload);
            out.is_inner = true;
            return out;
        }
        case wire_type::account_state:
        case wire_type::transaction_with_meta: {
            auto const body = wire.subspan(0, wire.size() - 1);  // [item][key]
            if (body.size() < 32)
                throw std::runtime_error("wire_to_ondisk: leaf shorter than a key");
            auto const& prefix = tree == tree_kind::state
                ? wire_detail::kLeafPrefix
                : wire_detail::kTxNodePrefix;
            auto const ntype = tree == tree_kind::state
                ? node_type::hot_account_node
                : node_type::hot_transaction_node;
            if ((tree == tree_kind::state) !=
                (type == wire_type::account_state))
                throw std::runtime_error(
                    "wire_to_ondisk: leaf wire type does not match the tree");
            std::vector<std::uint8_t> payload;
            payload.reserve(4 + body.size());
            payload.insert(payload.end(), prefix.begin(), prefix.end());
            payload.insert(payload.end(), body.begin(), body.end());
            out.hash = wire_detail::sha512_half(payload);
            out.ondisk = nodeobject_compress(ntype, payload);
            return out;
        }
        case wire_type::transaction:
            throw std::runtime_error(
                "wire_to_ondisk: bare transaction leaves are not ledger nodes");
    }
    throw std::runtime_error("wire_to_ondisk: unknown wire type");
}

/// On-disk form to the wire form a peer would send, type byte included.
/// Inners come out full (type 2) when all sixteen branches are populated
/// and compressed (type 3) otherwise, as rippled serializes them.
inline std::vector<std::uint8_t>
ondisk_to_wire(std::span<std::uint8_t const> ondisk)
{
    auto const decoded = nodeobject_decompress(ondisk);
    auto const payload = decoded.payload();
    std::vector<std::uint8_t> wire;
    if (decoded.get_type() == node_type::hot_unknown &&
        payload.size() == 4 + 512)
    {
        // Inner: ["MIN\0"][16 x 32]
        auto const hashes = payload.subspan(4);
        int populated = 0;
        for (int i = 0; i < 16; ++i)
            if (Hash256(hashes.data() + 32 * i) != Hash256::zero())
                ++populated;
        if (populated == 16)
        {
            wire.assign(hashes.begin(), hashes.end());
            wire.push_back(static_cast<std::uint8_t>(wire_type::inner));
            return wire;
        }
        for (int i = 0; i < 16; ++i)
        {
            auto const* h = hashes.data() + 32 * i;
            if (Hash256(h) == Hash256::zero())
                continue;
            wire.insert(wire.end(), h, h + 32);
            wire.push_back(static_cast<std::uint8_t>(i));
        }
        wire.push_back(static_cast<std::uint8_t>(wire_type::compressed_inner));
        return wire;
    }
    if (payload.size() < 4 + 32)
        throw std::runtime_error("ondisk_to_wire: leaf payload too short");
    std::uint8_t type_byte;
    if (std::memcmp(payload.data(), wire_detail::kLeafPrefix.data(), 4) == 0)
        type_byte = static_cast<std::uint8_t>(wire_type::account_state);
    else if (
        std::memcmp(payload.data(), wire_detail::kTxNodePrefix.data(), 4) == 0)
        type_byte = static_cast<std::uint8_t>(wire_type::transaction_with_meta);
    else
        throw std::runtime_error("ondisk_to_wire: not a state or tx leaf");
    wire.assign(payload.begin() + 4, payload.end());
    wire.push_back(type_byte);
    return wire;
}

}  // namespace catl::nodestore
