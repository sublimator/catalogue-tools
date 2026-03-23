#include "catl/core/types.h"
#include "catl/crypto/sha512-half-hasher.h"
#include "catl/shamap/binary-trie.h"
#include "catl/shamap/shamap-hashprefix.h"
#include "catl/shamap/shamap-nodeid.h"
#include "catl/shamap/shamap-traits.h"
#include "catl/shamap/shamap.h"

#include <boost/json.hpp>
#include <gtest/gtest.h>
#include <iostream>
#include <random>
#include <string>

using namespace catl::shamap;
using AbbrevMap = SHAMapT<AbbreviatedTreeTraits>;

// ─── Helpers ─────────────────────────────────────────────────────

namespace {

struct AbbrevFixture
{
    Hash256 full_hash;
    AbbrevMap abbrev;
    int num_leaves;
    int num_real;
    int num_placeholders;

    AbbrevFixture(AbbrevMap&& m) : abbrev(std::move(m))
    {
    }
};

/// Build a full AbbrevMap with all leaves, then build an abbreviated one
/// with `num_real` real leaves and the rest as placeholders.
/// Uses walk_abbreviated on the full tree to get exact nodeIDs.
AbbrevFixture
make_abbreviated_tree(
    int num_leaves,
    int num_real_leaves,
    SHAMapNodeType tree_type = tnACCOUNT_STATE)
{
    std::mt19937 rng(42 + num_leaves);

    struct LeafInfo
    {
        Hash256 key;
        std::vector<uint8_t> data;
        SHAMapNodeID nid;
        Hash256 leaf_hash;
    };
    std::vector<LeafInfo> leaves;

    // Generate random leaves
    for (int i = 0; i < num_leaves; i++)
    {
        LeafInfo ld;
        for (int j = 0; j < 32; j++)
            ld.key.data()[j] = static_cast<uint8_t>(rng());
        size_t data_size = 8 + (rng() % 25);
        ld.data.resize(data_size);
        for (size_t j = 0; j < data_size; j++)
            ld.data[j] = static_cast<uint8_t>(rng());
        leaves.push_back(std::move(ld));
    }

    // Build full tree as AbbrevMap (same type, just using add_item)
    SHAMapOptions opts;
    opts.tree_collapse_impl = TreeCollapseImpl::leafs_only;
    opts.reference_hash_impl = ReferenceHashImpl::recursive_simple;
    AbbrevMap full(tree_type, opts);
    for (auto& ld : leaves)
    {
        Slice s(ld.data.data(), ld.data.size());
        auto item =
            boost::intrusive_ptr<MmapItem>(OwnedItem::create(ld.key, s));
        full.add_item(item);
    }
    auto full_hash = full.get_hash();

    // Walk the full tree to collect leaf nodeIDs and hashes
    std::map<Hash256, std::pair<SHAMapNodeID, Hash256>> leaf_positions;
    full.walk_abbreviated(
        [&](SHAMapNodeID const& nid,
            Hash256 const& hash,
            SHAMapTreeNodeT<AbbreviatedTreeTraits> const* node) {
            if (node && node->is_leaf())
            {
                auto leaf =
                    static_cast<SHAMapLeafNodeT<AbbreviatedTreeTraits> const*>(
                        node);
                if (leaf->get_item())
                {
                    leaf_positions[leaf->get_item()->key().to_hash()] = {
                        nid, hash};
                }
            }
        });

    // Fill in nodeIDs and leaf hashes on our leaf structs
    for (auto& ld : leaves)
    {
        auto it = leaf_positions.find(ld.key);
        if (it != leaf_positions.end())
        {
            ld.nid = it->second.first;
            ld.leaf_hash = it->second.second;
        }
    }

    // Build abbreviated tree: first N as real, rest as placeholders
    AbbrevMap abbrev(tree_type, opts);

    for (int i = 0; i < num_real_leaves && i < num_leaves; i++)
    {
        Slice s(leaves[i].data.data(), leaves[i].data.size());
        auto item =
            boost::intrusive_ptr<MmapItem>(OwnedItem::create(leaves[i].key, s));
        abbrev.set_item_at(leaves[i].nid, item);
    }

    int placed = 0;
    for (int i = 0; i < num_leaves; i++)
    {
        if (abbrev.needs_placeholder(leaves[i].nid))
        {
            abbrev.set_placeholder(leaves[i].nid, leaves[i].leaf_hash);
            placed++;
        }
    }

    AbbrevFixture result(std::move(abbrev));
    result.full_hash = full_hash;
    result.num_leaves = num_leaves;
    result.num_real = num_real_leaves;
    result.num_placeholders = placed;
    return result;
}

}  // namespace

// ─── LEB128 ─────────────────────────────────────────────────────

TEST(BinaryTrie, Leb128RoundTrip)
{
    std::vector<size_t> values = {
        0,
        1,
        127,
        128,
        255,
        256,
        16383,
        16384,
        65535,
        100000,
        1000000,
        SIZE_MAX / 2};

    for (auto v : values)
    {
        std::vector<uint8_t> buf;
        leb128_encode(buf, v);

        size_t pos = 0;
        auto decoded = leb128_decode(buf, pos);
        EXPECT_EQ(decoded, v) << "LEB128 round-trip failed for " << v;
        EXPECT_EQ(pos, buf.size());
    }
}

TEST(BinaryTrie, BranchHeader)
{
    uint32_t header = 0;
    set_branch_type(header, 0, BranchType::leaf);
    set_branch_type(header, 5, BranchType::inner);
    set_branch_type(header, 10, BranchType::hash);
    set_branch_type(header, 15, BranchType::empty);

    EXPECT_EQ(get_branch_type(header, 0), BranchType::leaf);
    EXPECT_EQ(get_branch_type(header, 5), BranchType::inner);
    EXPECT_EQ(get_branch_type(header, 10), BranchType::hash);
    EXPECT_EQ(get_branch_type(header, 15), BranchType::empty);
    EXPECT_EQ(get_branch_type(header, 1), BranchType::empty);
}

// ─── Synthetic abbreviated tree round-trips ─────────────────────

TEST(BinaryTrie, OneLeafOneReal)
{
    auto f = make_abbreviated_tree(1, 1);
    ASSERT_EQ(f.abbrev.get_hash(), f.full_hash);

    auto binary = f.abbrev.trie_binary();
    auto reconstructed = AbbrevMap::from_trie_binary(binary, tnACCOUNT_STATE);
    EXPECT_EQ(reconstructed.get_hash(), f.full_hash);

    // 1 leaf, 0 placeholders: 4-byte header + 32-byte key + varint + data
    std::cout << "  1 leaf (1 real): " << binary.size() << " bytes\n";
    EXPECT_LT(binary.size(), 80u);
}

TEST(BinaryTrie, TwoLeaves_OneReal_OnePlaceholder)
{
    auto f = make_abbreviated_tree(2, 1);
    ASSERT_EQ(f.abbrev.get_hash(), f.full_hash);
    EXPECT_EQ(f.num_placeholders, 1);

    auto binary = f.abbrev.trie_binary();
    auto reconstructed = AbbrevMap::from_trie_binary(binary, tnACCOUNT_STATE);
    EXPECT_EQ(reconstructed.get_hash(), f.full_hash);

    // Should be: headers + 1 leaf + 1 placeholder hash (32 bytes)
    std::cout << "  2 leaves (1 real, 1 placeholder): " << binary.size()
              << " bytes\n";
}

TEST(BinaryTrie, TwoLeaves_TwoReal)
{
    auto f = make_abbreviated_tree(2, 2);
    ASSERT_EQ(f.abbrev.get_hash(), f.full_hash);

    auto binary = f.abbrev.trie_binary();
    auto reconstructed = AbbrevMap::from_trie_binary(binary, tnACCOUNT_STATE);
    EXPECT_EQ(reconstructed.get_hash(), f.full_hash);

    std::cout << "  2 leaves (2 real): " << binary.size() << " bytes\n";
}

TEST(BinaryTrie, TenLeaves_OneReal)
{
    auto f = make_abbreviated_tree(10, 1);
    ASSERT_EQ(f.abbrev.get_hash(), f.full_hash);

    auto binary = f.abbrev.trie_binary();
    auto reconstructed = AbbrevMap::from_trie_binary(binary, tnACCOUNT_STATE);
    EXPECT_EQ(reconstructed.get_hash(), f.full_hash);

    // ~9 placeholders × 32 bytes = 288 + headers + 1 leaf
    std::cout << "  10 leaves (1 real, " << f.num_placeholders
              << " placeholders): " << binary.size() << " bytes\n";

    // Binary should be smaller than JSON for an abbreviated tree
    auto json_str = f.abbrev.trie_json_string();
    std::cout << "  JSON trie: " << json_str.size() << " bytes\n";
    EXPECT_LT(binary.size(), json_str.size())
        << "Binary trie should be smaller than JSON trie";
}

TEST(BinaryTrie, FiftyLeaves_OneReal)
{
    auto f = make_abbreviated_tree(50, 1);
    ASSERT_EQ(f.abbrev.get_hash(), f.full_hash);

    auto binary = f.abbrev.trie_binary();
    auto reconstructed = AbbrevMap::from_trie_binary(binary, tnACCOUNT_STATE);
    EXPECT_EQ(reconstructed.get_hash(), f.full_hash);

    auto json_str = f.abbrev.trie_json_string();
    std::cout << "  50 leaves (1 real, " << f.num_placeholders
              << " placeholders): binary=" << binary.size()
              << " json=" << json_str.size() << " bytes\n";
    EXPECT_LT(binary.size(), json_str.size());
}

TEST(BinaryTrie, FiftyLeaves_FiveReal)
{
    auto f = make_abbreviated_tree(50, 5);
    ASSERT_EQ(f.abbrev.get_hash(), f.full_hash);

    auto binary = f.abbrev.trie_binary();
    auto reconstructed = AbbrevMap::from_trie_binary(binary, tnACCOUNT_STATE);
    EXPECT_EQ(reconstructed.get_hash(), f.full_hash);

    auto json_str = f.abbrev.trie_json_string();
    std::cout << "  50 leaves (5 real, " << f.num_placeholders
              << " placeholders): binary=" << binary.size()
              << " json=" << json_str.size() << " bytes\n";
    EXPECT_LT(binary.size(), json_str.size());
}

TEST(BinaryTrie, HundredLeaves_OneReal)
{
    auto f = make_abbreviated_tree(100, 1);
    ASSERT_EQ(f.abbrev.get_hash(), f.full_hash);

    auto binary = f.abbrev.trie_binary();
    auto reconstructed = AbbrevMap::from_trie_binary(binary, tnACCOUNT_STATE);
    EXPECT_EQ(reconstructed.get_hash(), f.full_hash);

    auto json_str = f.abbrev.trie_json_string();
    std::cout << "  100 leaves (1 real, " << f.num_placeholders
              << " placeholders): binary=" << binary.size()
              << " json=" << json_str.size() << " bytes\n";
    EXPECT_LT(binary.size(), json_str.size());
}

// ─── TX tree type ───────────────────────────────────────────────

TEST(BinaryTrie, TxTree_TwentyLeaves)
{
    auto f = make_abbreviated_tree(20, 1, tnTRANSACTION_MD);
    ASSERT_EQ(f.abbrev.get_hash(), f.full_hash);

    auto binary = f.abbrev.trie_binary();
    auto reconstructed = AbbrevMap::from_trie_binary(binary, tnTRANSACTION_MD);
    EXPECT_EQ(reconstructed.get_hash(), f.full_hash);

    auto json_str = f.abbrev.trie_json_string();
    std::cout << "  TX tree 20 leaves (1 real, " << f.num_placeholders
              << " placeholders): binary=" << binary.size()
              << " json=" << json_str.size() << " bytes\n";
    EXPECT_LT(binary.size(), json_str.size());
}

// ─── Deterministic byte-level check ─────────────────────────────

TEST(BinaryTrie, ByteLevelStructure)
{
    // Single leaf at branch A: verify exact binary layout
    SHAMapOptions opts;
    opts.tree_collapse_impl = TreeCollapseImpl::leafs_only;
    opts.reference_hash_impl = ReferenceHashImpl::recursive_simple;
    AbbrevMap map(tnACCOUNT_STATE, opts);

    Hash256 key;
    std::memset(key.data(), 0xA0, 32);  // first nibble = A
    uint8_t data[] = {0xDE, 0xAD};
    auto item =
        boost::intrusive_ptr<MmapItem>(OwnedItem::create(key, Slice(data, 2)));
    map.add_item(item);

    auto binary = map.trie_binary();

    // Root header: 4 bytes LE. Branch A (index 10) = leaf (01).
    // All other branches empty (00).
    // Header = 0x01 << (10*2) = 0x00100000
    size_t pos = 0;
    uint32_t header = read_u32_le(binary, pos);
    EXPECT_EQ(get_branch_type(header, 10), BranchType::leaf);
    for (int i = 0; i < 16; i++)
    {
        if (i != 10)
        {
            EXPECT_EQ(get_branch_type(header, i), BranchType::empty);
        }
    }

    // Leaf: 32-byte key
    EXPECT_EQ(pos, 4u);
    EXPECT_EQ(binary[pos], 0xA0);  // first byte of key
    pos += 32;

    // Leaf: varint data length = 2
    EXPECT_EQ(binary[pos], 2);
    pos++;

    // Leaf: data
    EXPECT_EQ(binary[pos], 0xDE);
    EXPECT_EQ(binary[pos + 1], 0xAD);
    pos += 2;

    EXPECT_EQ(pos, binary.size());
    // Total: 4 (header) + 32 (key) + 1 (varint) + 2 (data) = 39 bytes
    EXPECT_EQ(binary.size(), 39u);
}

// ─── Multiple real leaves ───────────────────────────────────────

TEST(BinaryTrie, FiftyLeaves_TenReal)
{
    auto f = make_abbreviated_tree(50, 10);
    ASSERT_EQ(f.abbrev.get_hash(), f.full_hash);

    auto binary = f.abbrev.trie_binary();
    auto reconstructed = AbbrevMap::from_trie_binary(binary, tnACCOUNT_STATE);
    EXPECT_EQ(reconstructed.get_hash(), f.full_hash);

    auto json_str = f.abbrev.trie_json_string();
    std::cout << "  50 leaves (10 real, " << f.num_placeholders
              << " placeholders): binary=" << binary.size()
              << " json=" << json_str.size() << " bytes\n";
    EXPECT_LT(binary.size(), json_str.size());
}

TEST(BinaryTrie, FiftyLeaves_AllReal)
{
    // All leaves real, zero placeholders — full tree as binary
    auto f = make_abbreviated_tree(50, 50);
    ASSERT_EQ(f.abbrev.get_hash(), f.full_hash);
    EXPECT_EQ(f.num_placeholders, 0);

    auto binary = f.abbrev.trie_binary();
    auto reconstructed = AbbrevMap::from_trie_binary(binary, tnACCOUNT_STATE);
    EXPECT_EQ(reconstructed.get_hash(), f.full_hash);

    std::cout << "  50 leaves (all real): binary=" << binary.size()
              << " bytes\n";
}

TEST(BinaryTrie, HundredLeaves_TwentyReal)
{
    auto f = make_abbreviated_tree(100, 20);
    ASSERT_EQ(f.abbrev.get_hash(), f.full_hash);

    auto binary = f.abbrev.trie_binary();
    auto reconstructed = AbbrevMap::from_trie_binary(binary, tnACCOUNT_STATE);
    EXPECT_EQ(reconstructed.get_hash(), f.full_hash);

    auto json_str = f.abbrev.trie_json_string();
    std::cout << "  100 leaves (20 real, " << f.num_placeholders
              << " placeholders): binary=" << binary.size()
              << " json=" << json_str.size() << " bytes\n";
    EXPECT_LT(binary.size(), json_str.size());
}

// ─── Random round-trip stress test ──────────────────────────────

TEST(BinaryTrie, RandomizedRoundTrips)
{
    std::mt19937 rng(999);
    int failures = 0;

    for (int trial = 0; trial < 50; trial++)
    {
        int num_leaves = 2 + (rng() % 99);  // 2-100
        int num_real = 1 + (rng() % std::min(num_leaves, 5));

        auto f = make_abbreviated_tree(num_leaves, num_real);
        if (f.abbrev.get_hash() != f.full_hash)
        {
            failures++;
            continue;
        }

        auto binary = f.abbrev.trie_binary();
        auto reconstructed =
            AbbrevMap::from_trie_binary(binary, tnACCOUNT_STATE);

        if (reconstructed.get_hash() != f.full_hash)
        {
            failures++;
            EXPECT_EQ(reconstructed.get_hash(), f.full_hash)
                << "Trial " << trial << " failed (leaves=" << num_leaves
                << " real=" << num_real
                << " placeholders=" << f.num_placeholders
                << " binary=" << binary.size() << ")";
        }
    }

    EXPECT_EQ(failures, 0) << failures << "/50 trials failed";
}
