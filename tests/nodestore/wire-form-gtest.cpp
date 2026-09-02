#include "catl/nodestore/wire_form.h"
#include <gtest/gtest.h>
#include <string>
#include <vector>

using namespace catl::nodestore;

namespace {

std::vector<uint8_t>
hex_to_bytes(std::string const& hex)
{
    std::vector<uint8_t> out;
    for (size_t i = 0; i + 1 < hex.size(); i += 2)
        out.push_back(
            static_cast<uint8_t>(std::stoul(hex.substr(i, 2), nullptr, 16)));
    return out;
}

}  // namespace

TEST(WireForm, InnerRoundTripsFullAndCompressed)
{
    // Sparse inner: branches 2 and 4.
    detail::hash_array_inner_source src;
    src.branches.fill(Hash256::zero());
    std::array<uint8_t, 32> a{}, b{};
    a.fill(0xA1);
    b.fill(0xB2);
    src.branches[2] = Hash256(a);
    src.branches[4] = Hash256(b);
    auto const ondisk = nodeobject_compress(src);
    EXPECT_EQ(peek_compression_type(ondisk), compression_type::inner_node_compressed);

    auto const wire = ondisk_to_wire(ondisk.data);
    ASSERT_EQ(wire.size(), 2u * 33u + 1u);
    EXPECT_EQ(wire.back(), 3u);  // compressed inner
    EXPECT_EQ(wire[32], 2u);     // branch of first entry
    EXPECT_EQ(wire[65], 4u);
    EXPECT_TRUE(is_wire_inner(wire));

    auto const back = wire_to_ondisk(wire, tree_kind::state);
    EXPECT_TRUE(back.is_inner);
    EXPECT_EQ(back.ondisk.data, ondisk.data);
    // Hash equals SHA-512/256 over the decoded payload.
    auto const decoded = nodeobject_decompress(ondisk);
    EXPECT_EQ(back.hash, wire_detail::sha512_half(decoded.payload()));

    // Full inner: sixteen branches -> wire type 2, 513 bytes.
    for (int i = 0; i < 16; ++i)
    {
        std::array<uint8_t, 32> h{};
        h.fill(static_cast<uint8_t>(0x10 + i));
        src.branches[static_cast<size_t>(i)] = Hash256(h);
    }
    auto const full = nodeobject_compress(src);
    auto const full_wire = ondisk_to_wire(full.data);
    ASSERT_EQ(full_wire.size(), 513u);
    EXPECT_EQ(full_wire.back(), 2u);
    auto const full_back = wire_to_ondisk(full_wire, tree_kind::transaction);
    EXPECT_EQ(full_back.ondisk.data, full.data);
}

TEST(WireForm, LeavesRoundTripWithPrefixAndType)
{
    // Account state leaf: item + key, wire type 1.
    std::vector<uint8_t> item = {0x11, 0x00, 0x61, 0x22, 0x00, 0x00, 0x00, 0x00};
    std::array<uint8_t, 32> key{};
    key.fill(0x5C);
    std::vector<uint8_t> wire(item);
    wire.insert(wire.end(), key.begin(), key.end());
    wire.push_back(1);

    auto const conv = wire_to_ondisk(wire, tree_kind::state);
    EXPECT_FALSE(conv.is_inner);
    EXPECT_EQ(peek_compression_type(conv.ondisk), compression_type::lz4);
    auto const decoded = nodeobject_decompress(conv.ondisk);
    EXPECT_EQ(decoded.get_type(), node_type::hot_account_node);
    auto const payload = decoded.payload();
    ASSERT_EQ(payload.size(), 4u + item.size() + 32u);
    EXPECT_EQ(payload[0], 'M');
    EXPECT_EQ(payload[1], 'L');
    EXPECT_EQ(payload[2], 'N');
    EXPECT_EQ(payload[3], 0);
    EXPECT_EQ(conv.hash, wire_detail::sha512_half(payload));

    auto const back = ondisk_to_wire(conv.ondisk.data);
    EXPECT_EQ(back, wire);

    // Tx with meta: wire type 4, "SND\0", hot_transaction_node.
    wire.back() = 4;
    auto const tx = wire_to_ondisk(wire, tree_kind::transaction);
    auto const tx_decoded = nodeobject_decompress(tx.ondisk);
    EXPECT_EQ(tx_decoded.get_type(), node_type::hot_transaction_node);
    EXPECT_EQ(tx_decoded.payload()[0], 'S');
    EXPECT_EQ(ondisk_to_wire(tx.ondisk.data), wire);

    // A leaf under the wrong tree is refused, and a bare tx leaf always is.
    EXPECT_THROW(wire_to_ondisk(wire, tree_kind::state), std::runtime_error);
    wire.back() = 0;
    EXPECT_THROW(wire_to_ondisk(wire, tree_kind::transaction), std::runtime_error);
}

TEST(WireForm, MalformedInnersRefused)
{
    std::vector<uint8_t> dup(66 + 1, 0xEE);
    dup[32] = 7;
    dup[65] = 7;  // duplicate branch
    dup.back() = 3;
    EXPECT_THROW(wire_to_ondisk(dup, tree_kind::state), std::runtime_error);
    std::vector<uint8_t> oob(33 + 1, 0xEE);
    oob[32] = 16;  // branch out of range
    oob.back() = 3;
    std::vector<uint8_t> zero(33 + 1, 0);  // zero hash in a compressed inner
    zero[32] = 1;
    zero.back() = 3;
    EXPECT_THROW(wire_to_ondisk(zero, tree_kind::state), std::runtime_error);
    EXPECT_THROW(wire_to_ondisk(oob, tree_kind::state), std::runtime_error);
    std::vector<uint8_t> short_full(511 + 1);
    short_full.back() = 2;
    EXPECT_THROW(wire_to_ondisk(short_full, tree_kind::state), std::runtime_error);
}
