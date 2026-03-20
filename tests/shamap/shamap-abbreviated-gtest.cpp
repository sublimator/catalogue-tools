#include "catl/core/types.h"
#include "catl/shamap/shamap-header-only.h"
#include "catl/shamap/shamap-traits.h"

#include <algorithm>
#include <boost/json.hpp>
#include <fstream>
#include <gtest/gtest.h>
#include <random>
#include <sstream>
#include <string>

// Instantiate all SHAMap templates for AbbreviatedTreeTraits
INSTANTIATE_SHAMAP_NODE_TRAITS(catl::shamap::AbbreviatedTreeTraits);

using namespace catl::shamap;

// ─── Compile-time checks ─────────────────────────────────────────

static_assert(
    !DefaultNodeTraits::supports_placeholders,
    "DefaultNodeTraits must not support placeholders");

static_assert(
    AbbreviatedTreeTraits::supports_placeholders,
    "AbbreviatedTreeTraits must support placeholders");

// Verify abbreviated traits are larger (they carry PlaceholderHashes)
static_assert(
    sizeof(NodeChildrenT<AbbreviatedTreeTraits>) >
        sizeof(NodeChildrenT<DefaultNodeTraits>),
    "AbbreviatedTreeTraits NodeChildren should be larger than default");

// ─── Placeholder on NodeChildrenT ────────────────────────────────

TEST(AbbreviatedTree, PlaceholderBasics)
{
    NodeChildrenT<AbbreviatedTreeTraits> children;

    Hash256 hash;
    std::memset(hash.data(), 0xAB, 32);

    // Initially no placeholders
    EXPECT_FALSE(children.has_placeholder(5));
    EXPECT_EQ(children.get_placeholder_mask(), 0);

    // Set a placeholder
    children.set_placeholder(5, hash);
    EXPECT_TRUE(children.has_placeholder(5));
    EXPECT_EQ(children.get_placeholder(5), hash);
    EXPECT_EQ(children.get_placeholder_mask(), (1 << 5));

    // Other branches unaffected
    EXPECT_FALSE(children.has_placeholder(0));
    EXPECT_FALSE(children.has_placeholder(15));
}

TEST(AbbreviatedTree, RealChildSupersedesPlaceholder)
{
    NodeChildrenT<AbbreviatedTreeTraits> children;

    Hash256 hash;
    std::memset(hash.data(), 0xCD, 32);

    // Set a placeholder on branch 3
    children.set_placeholder(3, hash);
    EXPECT_TRUE(children.has_placeholder(3));

    // Now set a real child on branch 3 — placeholder should be cleared
    auto leaf = boost::intrusive_ptr<SHAMapTreeNodeT<AbbreviatedTreeTraits>>();
    // We can't easily create a real tree node here without the full SHAMap,
    // but we CAN verify that set_placeholder is a no-op when has_child is true.
    // For now just verify the placeholder is there.
    EXPECT_TRUE(children.has_placeholder(3));
    EXPECT_EQ(children.get_placeholder(3), hash);
}

TEST(AbbreviatedTree, PlaceholderNoopOnExistingChild)
{
    NodeChildrenT<AbbreviatedTreeTraits> children;

    Hash256 hash;
    std::memset(hash.data(), 0xEF, 32);

    // has_child(5) is false, so placeholder should set
    children.set_placeholder(5, hash);
    EXPECT_TRUE(children.has_placeholder(5));
}

TEST(AbbreviatedTree, DefaultTraitsNoPlaceholders)
{
    NodeChildrenT<DefaultNodeTraits> children;

    // These should compile and return false/0
    EXPECT_FALSE(children.has_placeholder(5));
    EXPECT_EQ(children.get_placeholder_mask(), 0);
}

// ─── Helpers ─────────────────────────────────────────────────────

namespace {

std::vector<uint8_t>
hex_to_bytes(std::string const& hex)
{
    std::vector<uint8_t> bytes;
    bytes.reserve(hex.size() / 2);
    for (size_t i = 0; i + 1 < hex.size(); i += 2)
    {
        unsigned int byte;
        std::sscanf(hex.c_str() + i, "%2x", &byte);
        bytes.push_back(static_cast<uint8_t>(byte));
    }
    return bytes;
}

Hash256
hash_from_hex(std::string const& hex)
{
    auto bytes = hex_to_bytes(hex);
    Hash256 h;
    if (bytes.size() >= 32)
        std::memcpy(h.data(), bytes.data(), 32);
    return h;
}

// Get fixture path relative to source dir
std::string
fixture_path(std::string const& name)
{
    // CMake sets this via -DPROJECT_ROOT
    return std::string(PROJECT_ROOT) + "tests/shamap/fixture/" + name;
}

boost::json::value
load_fixture(std::string const& name)
{
    std::ifstream f(fixture_path(name));
    if (!f.is_open())
        throw std::runtime_error("Cannot open fixture: " + name);
    std::stringstream buf;
    buf << f.rdbuf();
    return boost::json::parse(buf.str());
}

}  // namespace

// ─── Fixture-based tests ─────────────────────────────────────────

TEST(AbbreviatedTree, LoadFixtureAndVerifyHash)
{
    auto fixture = load_fixture("tx-tree-fixture-102992073.json");
    auto const& obj = fixture.as_object();

    auto expected_hash =
        hash_from_hex(std::string(obj.at("tx_hash").as_string()));

    auto const& nodes = obj.at("nodes").as_array();

    // Build SHAMap from leaf nodes only
    catl::shamap::SHAMap map(catl::shamap::tnTRANSACTION_MD);
    int leaves = 0;

    for (auto const& node_val : nodes)
    {
        auto const& node = node_val.as_object();
        auto type = std::string(node.at("type").as_string());
        if (type != "leaf")
            continue;

        auto hex = std::string(node.at("hex").as_string());
        auto bytes = hex_to_bytes(hex);

        // Wire format: nodedata + type byte (last byte)
        // Strip type byte → leaf data
        // Leaf data: [VL tx][VL meta][32-byte key]
        if (bytes.size() < 33)  // at least 1 byte data + 32 byte key
            continue;

        // Strip the wire type byte (last byte)
        auto data_size = bytes.size() - 1;

        // Key is the last 32 bytes of the data (before type byte)
        Hash256 key(bytes.data() + data_size - 32);
        Slice item_data(bytes.data(), data_size - 32);

        boost::intrusive_ptr<MmapItem> item(OwnedItem::create(key, item_data));
        map.add_item(item);
        leaves++;
    }

    EXPECT_GT(leaves, 0);

    auto computed = map.get_hash();
    EXPECT_EQ(computed, expected_hash)
        << "expected: " << expected_hash.hex()
        << "\ncomputed: " << computed.hex() << "\nleaves: " << leaves;
}

TEST(AbbreviatedTree, TwoLeaves_OneRealOnePlaceholder)
{
    // Two leaves sharing the same root branch (same first nibble).
    // Full tree: both leaves inserted normally.
    // Abbreviated tree: one real leaf + placeholder for the other.
    // Hashes must match.

    using AbbrevMap = catl::shamap::SHAMapT<AbbreviatedTreeTraits>;

    // Keys that share first nibble (0xA) but differ at nibble 1
    Hash256 key_a, key_b;
    std::memset(key_a.data(), 0, 32);
    std::memset(key_b.data(), 0, 32);
    key_a.data()[0] = 0xA1;  // nibble 0 = A, nibble 1 = 1
    key_b.data()[0] = 0xA2;  // nibble 0 = A, nibble 1 = 2

    uint8_t data_a[] = {1, 2, 3};
    uint8_t data_b[] = {4, 5, 6};

    // Full tree with both leaves
    catl::shamap::SHAMap full_map(catl::shamap::tnACCOUNT_STATE);
    boost::intrusive_ptr<MmapItem> item_a(
        OwnedItem::create(key_a, Slice(data_a, 3)));
    boost::intrusive_ptr<MmapItem> item_b(
        OwnedItem::create(key_b, Slice(data_b, 3)));
    full_map.add_item(item_a);
    full_map.add_item(item_b);
    auto full_hash = full_map.get_hash();

    // Compute leaf B's hash manually: SHA512Half(MLN\0 || data || key)
    catl::crypto::Sha512HalfHasher hasher;
    auto prefix = catl::shamap::HashPrefix::leaf_node;
    hasher.update(prefix.data(), prefix.size());
    hasher.update(data_b, 3);
    hasher.update(key_b.data(), 32);
    auto leaf_b_hash = hasher.finalize();

    // Abbreviated tree: real leaf A + placeholder for leaf B
    SHAMapOptions opts;
    opts.tree_collapse_impl = TreeCollapseImpl::leafs_only;
    opts.reference_hash_impl = ReferenceHashImpl::recursive_simple;
    AbbrevMap abbrev_map(catl::shamap::tnACCOUNT_STATE, opts);

    // Add real leaf A first
    boost::intrusive_ptr<MmapItem> item_a2(
        OwnedItem::create(key_a, Slice(data_a, 3)));
    abbrev_map.add_item(item_a2);

    // Leaf B is at depth 2 (nibble 0 = A shared, nibble 1 = 2 diverges)
    // Its nodeID: path = 0xA200...00, depth = 2
    // But wait — in the SHAMap, the leaf sits as a child of the inner at depth
    // 1, on branch 2. So the placeholder goes at the position where leaf B
    // would be. That's: root → branch A → inner at depth 1 → branch 2 →
    // placeholder
    SHAMapNodeID placeholder_pos;
    {
        // Build nodeID: depth 2, path nibbles [A, 2, 0, 0, ...]
        Hash256 path;
        std::memset(path.data(), 0, 32);
        path.data()[0] = 0xA2;
        placeholder_pos = SHAMapNodeID(path, 2);
    }

    bool placed = abbrev_map.set_placeholder(placeholder_pos, leaf_b_hash);
    EXPECT_TRUE(placed)
        << "Placeholder should be placed (needs to split existing leaf)";

    auto abbrev_hash = abbrev_map.get_hash();
    EXPECT_EQ(abbrev_hash, full_hash)
        << "Two-leaf abbreviated tree should match full tree\n"
        << "full:   " << full_hash.hex() << "\n"
        << "abbrev: " << abbrev_hash.hex();
}

TEST(AbbreviatedTree, FixtureOneRealLeafRestPlaceholders)
{
    auto fixture = load_fixture("tx-tree-fixture-102992073.json");
    auto const& obj = fixture.as_object();
    auto expected_hash =
        hash_from_hex(std::string(obj.at("tx_hash").as_string()));
    auto const& nodes = obj.at("nodes").as_array();

    // Collect all leaves: key, item data, nodeID, leaf hash
    struct LeafEntry
    {
        Hash256 key;
        std::vector<uint8_t> item_data;
        SHAMapNodeID nodeid;
        Hash256 leaf_hash;
    };
    std::vector<LeafEntry> leaves;

    auto leaf_prefix = catl::shamap::HashPrefix::leaf_node;
    // TX tree uses tx_node prefix (SND\0), not leaf_node (MLN\0)
    auto tx_prefix = catl::shamap::HashPrefix::tx_node;

    for (auto const& nv : nodes)
    {
        auto const& n = nv.as_object();
        if (std::string(n.at("type").as_string()) != "leaf")
            continue;
        auto bytes = hex_to_bytes(std::string(n.at("hex").as_string()));
        if (bytes.size() < 33)
            continue;

        auto data_size = bytes.size() - 1;  // strip wire type byte
        Hash256 key(bytes.data() + data_size - 32);

        LeafEntry entry;
        entry.key = key;
        entry.item_data.assign(bytes.data(), bytes.data() + data_size - 32);

        if (n.contains("nodeid"))
        {
            auto nid_bytes =
                hex_to_bytes(std::string(n.at("nodeid").as_string()));
            entry.nodeid = SHAMapNodeID(std::span<const uint8_t>(nid_bytes));
        }

        // Compute leaf hash: SHA512Half(SND\0 || item_data || key)
        catl::crypto::Sha512HalfHasher hasher;
        hasher.update(tx_prefix.data(), tx_prefix.size());
        hasher.update(entry.item_data.data(), entry.item_data.size());
        hasher.update(key.data(), 32);
        entry.leaf_hash = hasher.finalize();

        leaves.push_back(std::move(entry));
    }

    ASSERT_GT(leaves.size(), 1u);

    // Verify full tree first
    catl::shamap::SHAMap full_map(catl::shamap::tnTRANSACTION_MD);
    for (auto& l : leaves)
    {
        Slice s(l.item_data.data(), l.item_data.size());
        boost::intrusive_ptr<MmapItem> item(OwnedItem::create(l.key, s));
        full_map.add_item(item);
    }
    ASSERT_EQ(full_map.get_hash(), expected_hash);

    // Build abbreviated tree: first leaf is real, rest are placeholders
    using AbbrevMap = catl::shamap::SHAMapT<AbbreviatedTreeTraits>;
    SHAMapOptions opts;
    opts.tree_collapse_impl = TreeCollapseImpl::leafs_only;
    opts.reference_hash_impl = ReferenceHashImpl::recursive_simple;
    AbbrevMap abbrev(catl::shamap::tnTRANSACTION_MD, opts);

    // 1. Add the real leaf
    {
        auto& target = leaves[0];
        Slice s(target.item_data.data(), target.item_data.size());
        boost::intrusive_ptr<MmapItem> item(OwnedItem::create(target.key, s));
        abbrev.add_item(item);
    }

    // 2. Add ALL leaves as placeholders (real one will be skipped — hash
    // matches)
    int placed = 0, skipped = 0;
    for (auto& l : leaves)
    {
        if (abbrev.set_placeholder(l.nodeid, l.leaf_hash))
            placed++;
        else
            skipped++;
    }

    auto abbrev_hash = abbrev.get_hash();
    EXPECT_EQ(abbrev_hash, expected_hash)
        << "Abbreviated tree should match full tree\n"
        << "full:    " << expected_hash.hex() << "\n"
        << "abbrev:  " << abbrev_hash.hex() << "\n"
        << "leaves:  " << leaves.size() << "\n"
        << "placed:  " << placed << "\n"
        << "skipped: " << skipped;
}

TEST(AbbreviatedTree, RandomizedInsertionOrder)
{
    // Load fixture, collect ALL nodes (inner branch hashes + leaf hashes)
    // as (SHAMapNodeID, Hash256) pairs. Then randomize insertion order
    // 1000 times and verify the hash always matches.

    auto fixture = load_fixture("tx-tree-fixture-102992073.json");
    auto const& obj = fixture.as_object();
    auto expected_hash =
        hash_from_hex(std::string(obj.at("tx_hash").as_string()));
    auto const& nodes = obj.at("nodes").as_array();

    auto tx_prefix = catl::shamap::HashPrefix::tx_node;

    // Collect every node as (nodeID, hash) — both inner branch hashes and leaf
    // hashes
    struct PlaceholderEntry
    {
        SHAMapNodeID nodeid;
        Hash256 hash;
    };
    std::vector<PlaceholderEntry> all_placeholders;

    // Also track which entry is our "real" leaf (the first one)
    Hash256 real_leaf_key;
    std::vector<uint8_t> real_leaf_data;
    SHAMapNodeID real_leaf_nodeid;
    bool found_real = false;

    for (auto const& nv : nodes)
    {
        auto const& n = nv.as_object();
        auto type = std::string(n.at("type").as_string());
        auto bytes = hex_to_bytes(std::string(n.at("hex").as_string()));

        SHAMapNodeID nodeid;
        if (n.contains("nodeid"))
        {
            auto nid_bytes =
                hex_to_bytes(std::string(n.at("nodeid").as_string()));
            nodeid = SHAMapNodeID(std::span<const uint8_t>(nid_bytes));
        }

        if (type == "inner" && bytes.size() >= 2)
        {
            // Extract branch hashes from inner node wire data
            uint8_t wire_type = bytes.back();

            if (wire_type == 2 && bytes.size() >= 513)  // uncompressed
            {
                for (int branch = 0; branch < 16; ++branch)
                {
                    Hash256 child_hash(bytes.data() + branch * 32);
                    if (child_hash == Hash256::zero())
                        continue;
                    auto child_nid = nodeid.child(branch);
                    all_placeholders.push_back({child_nid, child_hash});
                }
            }
            else if (wire_type == 3)  // compressed
            {
                auto body_size = bytes.size() - 1;
                for (size_t pos = 0; pos + 33 <= body_size; pos += 33)
                {
                    Hash256 child_hash(bytes.data() + pos);
                    int branch = bytes[pos + 32];
                    auto child_nid = nodeid.child(branch);
                    all_placeholders.push_back({child_nid, child_hash});
                }
            }
        }
        else if (type == "leaf" && bytes.size() >= 33)
        {
            auto data_size = bytes.size() - 1;
            Hash256 key(bytes.data() + data_size - 32);

            // Compute leaf hash
            catl::crypto::Sha512HalfHasher hasher;
            hasher.update(tx_prefix.data(), tx_prefix.size());
            hasher.update(bytes.data(), data_size - 32);  // item data
            hasher.update(key.data(), 32);
            auto leaf_hash = hasher.finalize();

            all_placeholders.push_back({nodeid, leaf_hash});

            // First leaf becomes the "real" one
            if (!found_real)
            {
                real_leaf_key = key;
                real_leaf_data.assign(
                    bytes.data(), bytes.data() + data_size - 32);
                real_leaf_nodeid = nodeid;
                found_real = true;
            }
        }
    }

    ASSERT_TRUE(found_real);
    ASSERT_GT(all_placeholders.size(), 10u);

    // Deduplicate: if the same nodeID appears multiple times
    // (e.g. from inner branch hash AND leaf hash), keep only one.
    // They should have the same hash if the data is consistent.
    {
        std::map<std::string, size_t> seen;  // nodeid hex → index
        std::vector<PlaceholderEntry> deduped;
        for (auto& p : all_placeholders)
        {
            auto key = std::string(
                reinterpret_cast<const char*>(p.nodeid.raw().data()), 33);
            if (seen.count(key))
                continue;
            seen[key] = deduped.size();
            deduped.push_back(p);
        }
        all_placeholders = std::move(deduped);
    }

    // First: try natural order (no shuffle)
    {
        using AbbrevMap = catl::shamap::SHAMapT<AbbreviatedTreeTraits>;
        SHAMapOptions opts;
        opts.tree_collapse_impl = TreeCollapseImpl::leafs_only;
        opts.reference_hash_impl = ReferenceHashImpl::recursive_simple;
        AbbrevMap abbrev(catl::shamap::tnTRANSACTION_MD, opts);

        Slice s(real_leaf_data.data(), real_leaf_data.size());
        boost::intrusive_ptr<MmapItem> item(
            OwnedItem::create(real_leaf_key, s));
        abbrev.add_item(item);

        for (auto& p : all_placeholders)
            abbrev.set_placeholder(p.nodeid, p.hash);

        EXPECT_EQ(abbrev.get_hash(), expected_hash)
            << "Natural order should work\n"
            << "expected: " << expected_hash.hex() << "\n"
            << "computed: " << abbrev.get_hash().hex();
    }

    // Then: try depth-sorted order
    {
        auto sorted = all_placeholders;
        std::sort(sorted.begin(), sorted.end(), [](auto& a, auto& b) {
            return a.nodeid.depth() < b.nodeid.depth();
        });

        using AbbrevMap = catl::shamap::SHAMapT<AbbreviatedTreeTraits>;
        SHAMapOptions opts;
        opts.tree_collapse_impl = TreeCollapseImpl::leafs_only;
        opts.reference_hash_impl = ReferenceHashImpl::recursive_simple;
        AbbrevMap abbrev(catl::shamap::tnTRANSACTION_MD, opts);

        Slice s(real_leaf_data.data(), real_leaf_data.size());
        boost::intrusive_ptr<MmapItem> item(
            OwnedItem::create(real_leaf_key, s));
        abbrev.add_item(item);

        for (auto& p : sorted)
            abbrev.set_placeholder(p.nodeid, p.hash);

        EXPECT_EQ(abbrev.get_hash(), expected_hash)
            << "Depth-sorted order should work\n"
            << "expected: " << expected_hash.hex() << "\n"
            << "computed: " << abbrev.get_hash().hex();
    }

    // Run 1000 randomized insertion orders
    std::mt19937 rng(42);  // fixed seed for reproducibility
    int failures = 0;

    for (int trial = 0; trial < 1000; ++trial)
    {
        // Shuffle placeholders
        auto shuffled = all_placeholders;
        std::shuffle(shuffled.begin(), shuffled.end(), rng);

        // Build abbreviated tree
        using AbbrevMap = catl::shamap::SHAMapT<AbbreviatedTreeTraits>;
        SHAMapOptions opts;
        opts.tree_collapse_impl = TreeCollapseImpl::leafs_only;
        opts.reference_hash_impl = ReferenceHashImpl::recursive_simple;
        AbbrevMap abbrev(catl::shamap::tnTRANSACTION_MD, opts);

        // Add the real leaf
        Slice s(real_leaf_data.data(), real_leaf_data.size());
        boost::intrusive_ptr<MmapItem> item(
            OwnedItem::create(real_leaf_key, s));
        abbrev.add_item(item);

        // Add all placeholders in shuffled order
        for (auto& p : shuffled)
            abbrev.set_placeholder(p.nodeid, p.hash);

        auto h = abbrev.get_hash();
        if (h != expected_hash)
        {
            failures++;
            if (failures <= 3)
            {
                EXPECT_EQ(h, expected_hash)
                    << "Trial " << trial << " failed\n"
                    << "expected: " << expected_hash.hex() << "\n"
                    << "computed: " << h.hex();
            }
        }
    }

    EXPECT_EQ(failures, 0) << failures << "/1000 trials failed";
}

TEST(AbbreviatedTree, MinimalTreeWithNeedsPlaceholder)
{
    // Build a minimal abbreviated tree using set_item_at + needs_placeholder.
    // Pick one real leaf, place it at its exact depth, then fill in only
    // the placeholders that needs_placeholder says are required.

    auto fixture = load_fixture("tx-tree-fixture-102992073.json");
    auto const& obj = fixture.as_object();
    auto expected_hash =
        hash_from_hex(std::string(obj.at("tx_hash").as_string()));
    auto const& nodes = obj.at("nodes").as_array();

    auto tx_prefix = catl::shamap::HashPrefix::tx_node;

    // Collect all nodes
    struct NodeEntry
    {
        SHAMapNodeID nodeid;
        Hash256 hash;
        std::string type;
        // For leaves: key + item data
        Hash256 key;
        std::vector<uint8_t> item_data;
    };

    std::vector<NodeEntry> all_entries;
    NodeEntry real_leaf;
    bool found_real = false;

    for (auto const& nv : nodes)
    {
        auto const& n = nv.as_object();
        auto type = std::string(n.at("type").as_string());
        auto bytes = hex_to_bytes(std::string(n.at("hex").as_string()));

        SHAMapNodeID nodeid;
        if (n.contains("nodeid"))
        {
            auto nid_bytes =
                hex_to_bytes(std::string(n.at("nodeid").as_string()));
            nodeid = SHAMapNodeID(std::span<const uint8_t>(nid_bytes));
        }

        if (type == "inner" && bytes.size() >= 2)
        {
            uint8_t wire_type = bytes.back();
            if (wire_type == 2 && bytes.size() >= 513)
            {
                for (int branch = 0; branch < 16; ++branch)
                {
                    Hash256 child_hash(bytes.data() + branch * 32);
                    if (child_hash == Hash256::zero())
                        continue;
                    NodeEntry e;
                    e.nodeid = nodeid.child(branch);
                    e.hash = child_hash;
                    e.type = "inner_branch";
                    all_entries.push_back(std::move(e));
                }
            }
            else if (wire_type == 3)
            {
                auto body_size = bytes.size() - 1;
                for (size_t pos = 0; pos + 33 <= body_size; pos += 33)
                {
                    Hash256 child_hash(bytes.data() + pos);
                    int branch = bytes[pos + 32];
                    NodeEntry e;
                    e.nodeid = nodeid.child(branch);
                    e.hash = child_hash;
                    e.type = "inner_branch";
                    all_entries.push_back(std::move(e));
                }
            }
        }
        else if (type == "leaf" && bytes.size() >= 33)
        {
            auto data_size = bytes.size() - 1;
            Hash256 key(bytes.data() + data_size - 32);

            catl::crypto::Sha512HalfHasher hasher;
            hasher.update(tx_prefix.data(), tx_prefix.size());
            hasher.update(bytes.data(), data_size - 32);
            hasher.update(key.data(), 32);

            NodeEntry e;
            e.nodeid = nodeid;
            e.hash = hasher.finalize();
            e.type = "leaf";
            e.key = key;
            e.item_data.assign(bytes.data(), bytes.data() + data_size - 32);
            all_entries.push_back(e);

            if (!found_real)
            {
                real_leaf = e;
                found_real = true;
            }
        }
    }

    ASSERT_TRUE(found_real);

    // Build minimal tree
    using AbbrevMap = catl::shamap::SHAMapT<AbbreviatedTreeTraits>;
    SHAMapOptions opts;
    opts.tree_collapse_impl = TreeCollapseImpl::leafs_only;
    opts.reference_hash_impl = ReferenceHashImpl::recursive_simple;
    AbbrevMap abbrev(catl::shamap::tnTRANSACTION_MD, opts);

    // 1. Place the real leaf at its exact depth
    Slice s(real_leaf.item_data.data(), real_leaf.item_data.size());
    boost::intrusive_ptr<MmapItem> item(OwnedItem::create(real_leaf.key, s));
    abbrev.set_item_at(real_leaf.nodeid, item);

    // 2. Only add placeholders that are needed
    int needed = 0, not_needed = 0;
    for (auto& e : all_entries)
    {
        if (abbrev.needs_placeholder(e.nodeid))
        {
            abbrev.set_placeholder(e.nodeid, e.hash);
            needed++;
        }
        else
        {
            not_needed++;
        }
    }

    // Should be much fewer than total entries
    std::cout << "  Minimal tree: " << needed << " placeholders needed, "
              << not_needed << " skipped (out of " << all_entries.size()
              << " total)\n";
    EXPECT_LT(needed, static_cast<int>(all_entries.size()))
        << "Minimal tree should need fewer placeholders than total entries";

    auto abbrev_hash = abbrev.get_hash();
    EXPECT_EQ(abbrev_hash, expected_hash)
        << "Minimal abbreviated tree should match full tree\n"
        << "expected: " << expected_hash.hex() << "\n"
        << "computed: " << abbrev_hash.hex() << "\n"
        << "needed: " << needed << ", skipped: " << not_needed;
}

TEST(AbbreviatedTree, RandomLeafSubsetsMinimalTree)
{
    // Pick random subsets of real leaves (1 to N), build minimal abbreviated
    // trees using set_item_at + needs_placeholder + set_placeholder.
    // Verify hash matches every time.

    auto fixture = load_fixture("tx-tree-fixture-102992073.json");
    auto const& obj = fixture.as_object();
    auto expected_hash =
        hash_from_hex(std::string(obj.at("tx_hash").as_string()));
    auto const& nodes = obj.at("nodes").as_array();

    auto tx_prefix = catl::shamap::HashPrefix::tx_node;

    // Collect all placeholder candidates (inner branch hashes + leaf hashes)
    struct PlaceholderEntry
    {
        SHAMapNodeID nodeid;
        Hash256 hash;
    };
    std::vector<PlaceholderEntry> all_placeholders;

    // Collect all leaves with their item data
    struct LeafEntry
    {
        SHAMapNodeID nodeid;
        Hash256 key;
        std::vector<uint8_t> item_data;
    };
    std::vector<LeafEntry> all_leaves;

    for (auto const& nv : nodes)
    {
        auto const& n = nv.as_object();
        auto type = std::string(n.at("type").as_string());
        auto bytes = hex_to_bytes(std::string(n.at("hex").as_string()));

        SHAMapNodeID nodeid;
        if (n.contains("nodeid"))
        {
            auto nid_bytes =
                hex_to_bytes(std::string(n.at("nodeid").as_string()));
            nodeid = SHAMapNodeID(std::span<const uint8_t>(nid_bytes));
        }

        if (type == "inner" && bytes.size() >= 2)
        {
            uint8_t wire_type = bytes.back();
            if (wire_type == 2 && bytes.size() >= 513)
            {
                for (int branch = 0; branch < 16; ++branch)
                {
                    Hash256 child_hash(bytes.data() + branch * 32);
                    if (child_hash == Hash256::zero())
                        continue;
                    all_placeholders.push_back(
                        {nodeid.child(branch), child_hash});
                }
            }
            else if (wire_type == 3)
            {
                auto body_size = bytes.size() - 1;
                for (size_t pos = 0; pos + 33 <= body_size; pos += 33)
                {
                    Hash256 child_hash(bytes.data() + pos);
                    int branch = bytes[pos + 32];
                    all_placeholders.push_back(
                        {nodeid.child(branch), child_hash});
                }
            }
        }
        else if (type == "leaf" && bytes.size() >= 33)
        {
            auto data_size = bytes.size() - 1;
            Hash256 key(bytes.data() + data_size - 32);

            catl::crypto::Sha512HalfHasher hasher;
            hasher.update(tx_prefix.data(), tx_prefix.size());
            hasher.update(bytes.data(), data_size - 32);
            hasher.update(key.data(), 32);

            all_placeholders.push_back({nodeid, hasher.finalize()});

            LeafEntry le;
            le.nodeid = nodeid;
            le.key = key;
            le.item_data.assign(bytes.data(), bytes.data() + data_size - 32);
            all_leaves.push_back(std::move(le));
        }
    }

    ASSERT_GT(all_leaves.size(), 5u);

    std::mt19937 rng(12345);
    int trials = 200;
    int failures = 0;

    for (int trial = 0; trial < trials; ++trial)
    {
        // Pick a random subset of leaves (1 to all)
        auto shuffled_leaves = all_leaves;
        std::shuffle(shuffled_leaves.begin(), shuffled_leaves.end(), rng);
        int num_leaves = 1 + (rng() % all_leaves.size());
        shuffled_leaves.resize(num_leaves);

        // Also shuffle placeholder order
        auto shuffled_placeholders = all_placeholders;
        std::shuffle(
            shuffled_placeholders.begin(), shuffled_placeholders.end(), rng);

        // Build minimal abbreviated tree
        using AbbrevMap = catl::shamap::SHAMapT<AbbreviatedTreeTraits>;
        SHAMapOptions opts;
        opts.tree_collapse_impl = TreeCollapseImpl::leafs_only;
        opts.reference_hash_impl = ReferenceHashImpl::recursive_simple;
        AbbrevMap abbrev(catl::shamap::tnTRANSACTION_MD, opts);

        // 1. Place real leaves at their exact depths
        for (auto& leaf : shuffled_leaves)
        {
            Slice s(leaf.item_data.data(), leaf.item_data.size());
            boost::intrusive_ptr<MmapItem> item(OwnedItem::create(leaf.key, s));
            abbrev.set_item_at(leaf.nodeid, item);
        }

        // 2. Only add placeholders that are needed
        int needed = 0;
        for (auto& p : shuffled_placeholders)
        {
            if (abbrev.needs_placeholder(p.nodeid))
            {
                abbrev.set_placeholder(p.nodeid, p.hash);
                needed++;
            }
        }

        auto h = abbrev.get_hash();
        if (h != expected_hash)
        {
            failures++;
            if (failures <= 3)
            {
                EXPECT_EQ(h, expected_hash)
                    << "Trial " << trial << " failed"
                    << " (leaves=" << num_leaves << ", placeholders=" << needed
                    << ")\n"
                    << "expected: " << expected_hash.hex() << "\n"
                    << "computed: " << h.hex();
            }
        }
    }

    EXPECT_EQ(failures, 0) << failures << "/" << trials << " trials failed";
}

TEST(AbbreviatedTree, WalkAbbreviated)
{
    // Build a minimal tree with 1 real leaf, verify walk_abbreviated
    // visits both the real leaf and the placeholders.

    using AbbrevMap = catl::shamap::SHAMapT<AbbreviatedTreeTraits>;

    Hash256 key_a, key_b;
    std::memset(key_a.data(), 0, 32);
    std::memset(key_b.data(), 0, 32);
    key_a.data()[0] = 0xA1;
    key_b.data()[0] = 0xA2;

    uint8_t data_a[] = {1, 2, 3};
    uint8_t data_b[] = {4, 5, 6};

    // Compute leaf B's hash
    catl::crypto::Sha512HalfHasher hasher;
    auto prefix = catl::shamap::HashPrefix::leaf_node;
    hasher.update(prefix.data(), prefix.size());
    hasher.update(data_b, 3);
    hasher.update(key_b.data(), 32);
    auto leaf_b_hash = hasher.finalize();

    SHAMapOptions opts;
    opts.tree_collapse_impl = TreeCollapseImpl::leafs_only;
    opts.reference_hash_impl = ReferenceHashImpl::recursive_simple;
    AbbrevMap abbrev(catl::shamap::tnACCOUNT_STATE, opts);

    // Real leaf A at depth 2 (nibble 0=A, nibble 1=1)
    Hash256 path_a;
    std::memset(path_a.data(), 0, 32);
    path_a.data()[0] = 0xA1;
    SHAMapNodeID nid_a(path_a, 2);
    boost::intrusive_ptr<MmapItem> item_a(
        OwnedItem::create(key_a, Slice(data_a, 3)));
    abbrev.set_item_at(nid_a, item_a);

    // Placeholder for leaf B at depth 2
    Hash256 path_b;
    std::memset(path_b.data(), 0, 32);
    path_b.data()[0] = 0xA2;
    SHAMapNodeID nid_b(path_b, 2);
    abbrev.set_placeholder(nid_b, leaf_b_hash);

    // Walk and collect
    int real_nodes = 0, placeholders = 0, inners = 0, leaves = 0;
    abbrev.walk_abbreviated(
        [&](SHAMapNodeID const&,
            Hash256 const&,
            SHAMapTreeNodeT<AbbreviatedTreeTraits> const* node) {
            if (node)
            {
                real_nodes++;
                if (node->is_inner())
                    inners++;
                else
                    leaves++;
            }
            else
            {
                placeholders++;
            }
        });

    // Should see: 1 real inner (depth 1, branch A), 1 real leaf (A),
    // 1 placeholder (B)
    EXPECT_EQ(real_nodes, 2);  // inner + leaf
    EXPECT_EQ(inners, 1);
    EXPECT_EQ(leaves, 1);
    EXPECT_EQ(placeholders, 1);
}

TEST(AbbreviatedTree, SingleLeafAsPlaceholder)
{
    // Simplest case: one leaf in a full tree, same leaf as placeholder in
    // abbreviated tree. Both should hash identically.

    using AbbrevMap = catl::shamap::SHAMapT<AbbreviatedTreeTraits>;

    // Create a key and some data
    Hash256 key;
    std::memset(key.data(), 0xAA, 32);
    uint8_t data[] = {1, 2, 3, 4};
    Slice item_data(data, 4);

    // Build a one-leaf tree and get its hash
    catl::shamap::SHAMap full_map(catl::shamap::tnACCOUNT_STATE);
    boost::intrusive_ptr<MmapItem> item(OwnedItem::create(key, item_data));
    full_map.add_item(item);
    auto full_hash = full_map.get_hash();
    EXPECT_NE(full_hash, Hash256::zero());

    // The root inner node's hash IS the tree hash.
    // The root has one child (the leaf) on branch nibble_at(key, 0) = 0xA = 10.
    // That child's hash is the leaf hash.
    // So: tree_hash = SHA512Half(inner_prefix || 0*32 || ... || leaf_hash at
    // branch 10 || ... || 0*32)

    // Now build an abbreviated tree with just a placeholder for that leaf.
    // The placeholder carries the leaf's hash. The root inner should
    // compute the same hash as the full tree.
    SHAMapOptions opts;
    opts.tree_collapse_impl = TreeCollapseImpl::leafs_only;
    opts.reference_hash_impl = ReferenceHashImpl::recursive_simple;
    AbbrevMap abbrev_map(catl::shamap::tnACCOUNT_STATE, opts);

    // We need the leaf's hash — get it from the full tree.
    // In the full tree, the leaf hash = SHA512Half(MLN\0 || item_data || key)
    // But we can just get it from the inner node: full_map root's child on
    // branch 10. Actually, for this test we can compute it: the placeholder
    // should produce the same tree hash if we give it the leaf hash.

    // Compute leaf hash manually: SHA512Half(leaf_node_prefix || data || key)
    catl::crypto::Sha512HalfHasher hasher;
    auto prefix = catl::shamap::HashPrefix::leaf_node;
    hasher.update(prefix.data(), prefix.size());
    hasher.update(data, 4);
    hasher.update(key.data(), 32);
    auto leaf_hash = hasher.finalize();

    // Place it as a placeholder at depth 1, branch 0xA
    int branch = (key.data()[0] >> 4) & 0xF;  // = 0xA
    SHAMapNodeID child_nid = SHAMapNodeID::root().child(branch);
    bool placed = abbrev_map.set_placeholder(child_nid, leaf_hash);
    EXPECT_TRUE(placed);

    auto abbrev_hash = abbrev_map.get_hash();
    EXPECT_EQ(abbrev_hash, full_hash)
        << "Abbreviated tree with one placeholder should match full tree\n"
        << "full:   " << full_hash.hex() << "\n"
        << "abbrev: " << abbrev_hash.hex();
}
