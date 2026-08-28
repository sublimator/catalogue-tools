#include "catl/test-utils/test-mmap-items.h"
#include "hybrid-shamap-test-helpers.h"
#include <algorithm>
#include <array>
#include <cstring>
#include <fstream>
#include <gtest/gtest.h>
#include <limits>
#include <stdexcept>
#include <vector>

namespace {

catl::v2::TreesHeader
trees_header_at(
    catl::v2::CatlV2Reader const& reader,
    catl::v2::LedgerIndexEntry const& entry)
{
    catl::v2::TreesHeader result{};
    std::memcpy(
        &result,
        reader.data_at(entry.header_offset + sizeof(catl::common::LedgerInfo)),
        sizeof(result));
    return result;
}

std::uint64_t
child_offset_at(
    catl::v2::CatlV2Reader const& reader,
    std::uint64_t parent_offset,
    int branch)
{
    catl::v2::InnerNodeHeader parent{};
    std::memcpy(&parent, reader.data_at(parent_offset), sizeof(parent));
    if (parent.get_child_type(branch) == catl::v2::ChildType::EMPTY)
        throw std::runtime_error("requested CATL2 branch is empty");

    std::size_t sparse_index = 0;
    for (int current = 0; current < branch; ++current)
        if (parent.get_child_type(current) != catl::v2::ChildType::EMPTY)
            ++sparse_index;

    auto const slot = parent_offset + sizeof(parent) +
        sparse_index * sizeof(catl::v2::rel_off_t);
    catl::v2::rel_off_t relative{};
    std::memcpy(&relative, reader.data_at(slot), sizeof(relative));
    if (relative >= 0)
        return slot + static_cast<std::uint64_t>(relative);

    // Avoid negating INT64_MIN even though test fixtures are tiny.
    auto const magnitude = static_cast<std::uint64_t>(-(relative + 1)) + 1;
    if (magnitude > slot)
        throw std::runtime_error("CATL2 child offset precedes the file");
    return slot - magnitude;
}

void
expect_all_children_backward(
    catl::v2::CatlV2Reader const& reader,
    std::uint64_t parent_offset)
{
    catl::v2::InnerNodeHeader parent{};
    std::memcpy(&parent, reader.data_at(parent_offset), sizeof(parent));
    std::size_t sparse_index = 0;
    for (int branch = 0; branch < 16; ++branch)
    {
        if (parent.get_child_type(branch) == catl::v2::ChildType::EMPTY)
            continue;
        auto const slot = parent_offset + sizeof(parent) +
            sparse_index++ * sizeof(catl::v2::rel_off_t);
        catl::v2::rel_off_t relative{};
        std::memcpy(&relative, reader.data_at(slot), sizeof(relative));
        EXPECT_LT(relative, 0) << "branch " << branch;
        EXPECT_LT(child_offset_at(reader, parent_offset, branch), parent_offset)
            << "branch " << branch;
    }
}

}  // namespace

TEST(CatlV2EmptyTree, UsesOneCanonicalPhysicalRootPerPack)
{
    TestMmapItems items;
    auto item = items.make(
        "1111111111111111111111111111111111111111111111111111111111111111",
        "CAFE");

    GoldMap state_map(catl::shamap::tnACCOUNT_STATE);
    state_map.add_item(item);
    GoldMap tx_map(catl::shamap::tnTRANSACTION_MD);

    auto const temp_file = boost::filesystem::temp_directory_path() /
        boost::filesystem::unique_path("empty-tx-tree-%%%%.catl2");

    std::vector<catl::v2::CatlV2Writer::PhysicalNodeObservation>
        zero_hash_nodes;
    {
        catl::v2::CatlV2Writer writer(
            temp_file.string(),
            0,
            [&](catl::v2::CatlV2Writer::PhysicalNodeObservation const& node) {
                if (std::all_of(
                        node.hash.begin(),
                        node.hash.end(),
                        [](std::uint8_t byte) { return byte == 0; }))
                    zero_hash_nodes.push_back(node);
            });
        ASSERT_TRUE(writer.write_ledger(
            fake_ledger(1, state_map.get_hash()), state_map, tx_map));
        ASSERT_TRUE(writer.write_ledger(
            fake_ledger(2, state_map.get_hash()), state_map, tx_map));
        ASSERT_TRUE(writer.finalize());
    }

    auto reader = catl::v2::CatlV2Reader::create(temp_file.string());
    EXPECT_EQ(reader->header().version, catl::v2::CATL_V2_FORMAT_VERSION);
    EXPECT_EQ(
        reader->header().ledger_data_offset,
        sizeof(catl::v2::CatlV2Header) + sizeof(catl::v2::InnerNodeHeader));
    EXPECT_FALSE(reader->has_node_index());
    auto const* first = reader->get_ledger_index().find_ledger(1);
    auto const* second = reader->get_ledger_index().find_ledger(2);
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    EXPECT_NE(first->tx_tree_offset, 0u);
    EXPECT_EQ(second->tx_tree_offset, first->tx_tree_offset);

    ASSERT_EQ(zero_hash_nodes.size(), 1u);
    EXPECT_EQ(zero_hash_nodes.front().offset, sizeof(catl::v2::CatlV2Header));
    EXPECT_EQ(zero_hash_nodes.front().offset, first->tx_tree_offset);
    EXPECT_EQ(
        zero_hash_nodes.front().kind,
        catl::v2::CatlV2Writer::PhysicalNodeKind::inner);

    catl::v2::InnerNodeHeader empty_root{};
    ASSERT_LE(first->tx_tree_offset + sizeof(empty_root), reader->file_size());
    std::memcpy(
        &empty_root,
        reader->data_at(first->tx_tree_offset),
        sizeof(empty_root));
    EXPECT_EQ(empty_root.get_depth(), 0u);
    EXPECT_EQ(empty_root.count_children(), 0);
    EXPECT_EQ(empty_root.overlay_mask, 0u);
    EXPECT_TRUE(std::all_of(
        empty_root.hash.begin(), empty_root.hash.end(), [](std::uint8_t byte) {
            return byte == 0;
        }));

    ASSERT_EQ(reader->read_ledger_info().seq, 1u);
    auto const missing_key = Key(Hash256::zero().data());
    EXPECT_FALSE(reader->lookup_key_in_tx(missing_key).has_value());
    EXPECT_EQ(
        reader->walk_tx_items([](Key const&, Slice const&) { return true; }),
        0u);
    reader->skip_state_map();
    EXPECT_EQ(reader->skip_tx_map(), 0u);

    ASSERT_EQ(reader->read_ledger_info().seq, 2u);
    EXPECT_FALSE(reader->lookup_key_in_tx(missing_key).has_value());
    EXPECT_EQ(
        reader->walk_tx_items([](Key const&, Slice const&) { return true; }),
        0u);
    reader->skip_state_map();
    EXPECT_EQ(reader->skip_tx_map(), 0u);

    boost::filesystem::remove(temp_file);
}

TEST(CatlV2Format, RefusesExperimentalVersionOne)
{
    auto const temp_file = boost::filesystem::temp_directory_path() /
        boost::filesystem::unique_path("catl2-old-version-%%%%.catl2");
    {
        catl::v2::CatlV2Writer writer(temp_file.string());
        ASSERT_TRUE(writer.finalize());
    }

    {
        std::fstream file(
            temp_file.string(),
            std::ios::binary | std::ios::in | std::ios::out);
        ASSERT_TRUE(file.good());
        catl::v2::CatlV2Header header{};
        file.read(reinterpret_cast<char*>(&header), sizeof(header));
        ASSERT_TRUE(file.good());
        header.version = 1;
        file.seekp(0);
        file.write(reinterpret_cast<char const*>(&header), sizeof(header));
        ASSERT_TRUE(file.good());
    }

    EXPECT_THROW(
        catl::v2::CatlV2Reader::create(temp_file.string()), std::runtime_error);
    boost::filesystem::remove(temp_file);
}

TEST(CatlV2Format, RefusesMalformedCanonicalEmptyRoot)
{
    auto const temp_file = boost::filesystem::temp_directory_path() /
        boost::filesystem::unique_path("catl2-bad-empty-root-%%%%.catl2");
    {
        catl::v2::CatlV2Writer writer(temp_file.string());
        ASSERT_TRUE(writer.finalize());
    }

    {
        std::fstream file(
            temp_file.string(),
            std::ios::binary | std::ios::in | std::ios::out);
        ASSERT_TRUE(file.good());
        catl::v2::InnerNodeHeader root{};
        file.seekg(sizeof(catl::v2::CatlV2Header));
        file.read(reinterpret_cast<char*>(&root), sizeof(root));
        ASSERT_TRUE(file.good());
        root.overlay_mask = 1;
        file.seekp(sizeof(catl::v2::CatlV2Header));
        file.write(reinterpret_cast<char const*>(&root), sizeof(root));
        ASSERT_TRUE(file.good());
    }

    EXPECT_THROW(
        catl::v2::CatlV2Reader::create(temp_file.string()), std::runtime_error);
    boost::filesystem::remove(temp_file);
}

TEST(CatlV2Offsets, CheckedBackwardOffsetRoundTrips)
{
    std::array<std::uint8_t, 256> storage{};
    constexpr std::size_t target = 24;
    constexpr std::size_t slot = 192;
    auto const relative = catl::v2::rel_from_abs(target, slot);
    ASSERT_LT(relative, 0);
    std::memcpy(storage.data() + slot, &relative, sizeof(relative));
    EXPECT_EQ(
        catl::v2::resolve_self_relative(storage.data() + slot, 0),
        storage.data() + target);

    auto constexpr signed_limit = static_cast<std::uint64_t>(
        std::numeric_limits<catl::v2::rel_off_t>::max());
    EXPECT_EQ(
        catl::v2::rel_from_abs(0, signed_limit + 1),
        std::numeric_limits<catl::v2::rel_off_t>::min());
    EXPECT_THROW(
        catl::v2::rel_from_abs(0, signed_limit + 2), std::overflow_error);
    EXPECT_THROW(
        catl::v2::rel_from_abs(signed_limit + 1, 0), std::overflow_error);
}

TEST(CatlV2SerializedNode, CopiedNodesResetPublishedOffsets)
{
    TestMmapItems items;
    auto original_item = items.make(
        "1111111111111111111111111111111111111111111111111111111111111111",
        "CAFE");
    auto replacement_item = items.make(
        "1111111111111111111111111111111111111111111111111111111111111111",
        "BABE");

    GoldMap map(catl::shamap::tnACCOUNT_STATE);
    ASSERT_EQ(map.add_item(original_item), catl::shamap::SetResult::ADD);
    auto snapshot = map.snapshot();
    ASSERT_TRUE(snapshot);

    auto old_root = map.get_root();
    auto old_leaf = old_root->get_child(1);
    ASSERT_TRUE(old_leaf);
    old_root->node_offset = 4096;
    old_root->processed = true;
    old_leaf->node_offset = 2048;
    old_leaf->processed = true;

    ASSERT_EQ(
        map.update_item(replacement_item), catl::shamap::SetResult::UPDATE);
    auto new_root = map.get_root();
    auto new_leaf = new_root->get_child(1);
    ASSERT_NE(new_root, old_root);
    ASSERT_NE(new_leaf, old_leaf);
    EXPECT_FALSE(new_root->processed);
    EXPECT_EQ(new_root->node_offset, 0u);
    EXPECT_FALSE(new_leaf->processed);
    EXPECT_EQ(new_leaf->node_offset, 0u);
    EXPECT_TRUE(old_root->processed);
    EXPECT_EQ(old_root->node_offset, 4096u);
    EXPECT_TRUE(old_leaf->processed);
    EXPECT_EQ(old_leaf->node_offset, 2048u);
}

TEST(CatlV2Writer, FinalizedSharedTreesRemainReadableAcrossManyLedgers)
{
    TestMmapItems items;
    auto state_item = items.make(
        "1111111111111111111111111111111111111111111111111111111111111111",
        "CAFE");
    auto tx_item = items.make(
        "A111111111111111111111111111111111111111111111111111111111111111",
        "BABE");
    GoldMap state_map(catl::shamap::tnACCOUNT_STATE);
    GoldMap tx_map(catl::shamap::tnTRANSACTION_MD);
    ASSERT_EQ(state_map.add_item(state_item), catl::shamap::SetResult::ADD);
    ASSERT_EQ(tx_map.add_item(tx_item), catl::shamap::SetResult::ADD);

    auto const temp_file = boost::filesystem::temp_directory_path() /
        boost::filesystem::unique_path("shared-many-%%%%.catl2");
    catl::v2::CatlV2Writer writer(temp_file.string());
    constexpr std::uint32_t ledger_count = 32;
    for (std::uint32_t sequence = 1; sequence <= ledger_count; ++sequence)
    {
        auto ledger = fake_ledger(sequence, state_map.get_hash());
        ledger.tx_hash = tx_map.get_hash();
        ASSERT_TRUE(writer.write_ledger(ledger, state_map, tx_map));
    }
    ASSERT_TRUE(writer.finalize());

    // finalize() closes the publication stream, so the completed pack is
    // readable while the writer object itself is still alive.
    auto reader = catl::v2::CatlV2Reader::create(temp_file.string());
    ASSERT_EQ(reader->header().ledger_count, ledger_count);
    std::uint64_t state_root = 0;
    std::uint64_t tx_root = 0;
    for (std::uint32_t sequence = 1; sequence <= ledger_count; ++sequence)
    {
        auto const* entry = reader->get_ledger_index().find_ledger(sequence);
        ASSERT_NE(entry, nullptr);
        auto const sizes = trees_header_at(*reader, *entry);
        if (sequence == 1)
        {
            state_root = entry->state_tree_offset;
            tx_root = entry->tx_tree_offset;
            EXPECT_GT(sizes.state_tree_size, 0u);
            EXPECT_GT(sizes.tx_tree_size, 0u);
        }
        else
        {
            EXPECT_EQ(entry->state_tree_offset, state_root);
            EXPECT_EQ(entry->tx_tree_offset, tx_root);
            EXPECT_EQ(sizes.state_tree_size, 0u);
            EXPECT_EQ(sizes.tx_tree_size, 0u);
        }

        ASSERT_EQ(reader->read_ledger_info().seq, sequence);
        EXPECT_TRUE(reader->lookup_key_in_state(state_item->key()));
        EXPECT_TRUE(reader->lookup_key_in_tx(tx_item->key()));
        EXPECT_EQ(
            reader->walk_state_items(
                [](Key const&, Slice const&) { return true; }),
            1u);
        EXPECT_EQ(
            reader->walk_tx_items(
                [](Key const&, Slice const&) { return true; }),
            1u);
        reader->skip_state_map();
        reader->skip_tx_map();
    }
    EXPECT_TRUE(reader->at_end_of_ledgers());
    boost::filesystem::remove(temp_file);
}

TEST(CatlV2Writer, ChildFirstDeltaMixesSharedAndNewChildren)
{
    using Observation = catl::v2::CatlV2Writer::PhysicalNodeObservation;

    TestMmapItems items;
    auto first = items.make(
        "1111111111111111111111111111111111111111111111111111111111111111",
        "CAFE");
    auto second = items.make(
        "2111111111111111111111111111111111111111111111111111111111111111",
        "BABE");
    auto added = items.make(
        "3111111111111111111111111111111111111111111111111111111111111111",
        "FACE");
    GoldMap state_map(catl::shamap::tnACCOUNT_STATE);
    GoldMap tx_map(catl::shamap::tnTRANSACTION_MD);
    ASSERT_EQ(state_map.add_item(first), catl::shamap::SetResult::ADD);
    ASSERT_EQ(state_map.add_item(second), catl::shamap::SetResult::ADD);

    auto const temp_file = boost::filesystem::temp_directory_path() /
        boost::filesystem::unique_path("postorder-mixed-%%%%.catl2");
    std::vector<Observation> observations;
    catl::v2::CatlV2Writer writer(
        temp_file.string(), 0, [&](Observation const& observation) {
            observations.push_back(observation);
        });
    ASSERT_TRUE(writer.write_ledger(
        fake_ledger(1, state_map.get_hash()), state_map, tx_map));

    auto first_root = state_map.get_root();
    auto const first_root_offset = first_root->node_offset;
    auto shared_child = first_root->get_child(1);
    ASSERT_TRUE(shared_child);
    auto const shared_child_offset = shared_child->node_offset;
    ASSERT_TRUE(first_root->processed);
    ASSERT_TRUE(shared_child->processed);
    auto const observations_after_first = observations.size();

    auto snapshot = state_map.snapshot();
    ASSERT_TRUE(snapshot);
    ASSERT_EQ(state_map.add_item(added), catl::shamap::SetResult::ADD);
    auto second_root = state_map.get_root();
    ASSERT_NE(second_root, first_root);
    EXPECT_FALSE(second_root->processed);
    EXPECT_EQ(second_root->node_offset, 0u);
    ASSERT_EQ(second_root->get_child(1), shared_child);

    ASSERT_TRUE(writer.write_ledger(
        fake_ledger(2, state_map.get_hash()), state_map, tx_map));
    auto const second_root_offset = second_root->node_offset;
    EXPECT_GT(second_root_offset, first_root_offset);
    EXPECT_EQ(observations.size() - observations_after_first, 2u);
    ASSERT_FALSE(observations.empty());
    EXPECT_EQ(observations.back().offset, second_root_offset);
    EXPECT_EQ(
        std::count_if(
            observations.begin(),
            observations.end(),
            [&](Observation const& observation) {
                return observation.offset == shared_child_offset;
            }),
        1);
    ASSERT_TRUE(writer.finalize());

    auto reader = catl::v2::CatlV2Reader::create(temp_file.string());
    auto const* first_index = reader->get_ledger_index().find_ledger(1);
    auto const* second_index = reader->get_ledger_index().find_ledger(2);
    ASSERT_NE(first_index, nullptr);
    ASSERT_NE(second_index, nullptr);
    EXPECT_EQ(first_index->state_tree_offset, first_root_offset);
    EXPECT_EQ(second_index->state_tree_offset, second_root_offset);
    expect_all_children_backward(*reader, first_root_offset);
    expect_all_children_backward(*reader, second_root_offset);
    EXPECT_EQ(
        child_offset_at(*reader, second_root_offset, 1), shared_child_offset);
    EXPECT_GT(
        first_root_offset,
        first_index->header_offset + sizeof(catl::common::LedgerInfo) +
            sizeof(catl::v2::TreesHeader));

    ASSERT_EQ(reader->read_ledger_info().seq, 1u);
    EXPECT_NE(reader->current_data(), reader->current_state_tree_root());
    EXPECT_TRUE(reader->lookup_key_in_state(first->key()));
    EXPECT_FALSE(reader->lookup_key_in_state(added->key()));
    EXPECT_EQ(
        reader->walk_state_items([](Key const&, Slice const&) { return true; }),
        2u);
    reader->skip_state_map();
    reader->skip_tx_map();

    ASSERT_EQ(reader->read_ledger_info().seq, 2u);
    EXPECT_NE(reader->current_data(), reader->current_state_tree_root());
    EXPECT_TRUE(reader->lookup_key_in_state(first->key()));
    EXPECT_TRUE(reader->lookup_key_in_state(added->key()));
    EXPECT_EQ(
        reader->walk_state_items([](Key const&, Slice const&) { return true; }),
        3u);
    reader->skip_state_map();
    reader->skip_tx_map();

    boost::filesystem::remove(temp_file);
}

TEST(CatlV2Writer, ObserverFailureDoesNotPublishIncompleteInner)
{
    TestMmapItems items;
    auto item = items.make(
        "1111111111111111111111111111111111111111111111111111111111111111",
        "CAFE");
    GoldMap state_map(catl::shamap::tnACCOUNT_STATE);
    GoldMap tx_map(catl::shamap::tnTRANSACTION_MD);
    ASSERT_EQ(state_map.add_item(item), catl::shamap::SetResult::ADD);

    auto const temp_file = boost::filesystem::temp_directory_path() /
        boost::filesystem::unique_path("observer-failure-%%%%.catl2");
    {
        catl::v2::CatlV2Writer writer(
            temp_file.string(),
            0,
            [](catl::v2::CatlV2Writer::PhysicalNodeObservation const& node) {
                if (node.kind ==
                        catl::v2::CatlV2Writer::PhysicalNodeKind::inner &&
                    node.offset != sizeof(catl::v2::CatlV2Header))
                    throw std::runtime_error("injected observer failure");
            });
        EXPECT_FALSE(writer.write_ledger(
            fake_ledger(1, state_map.get_hash()), state_map, tx_map));
        EXPECT_FALSE(state_map.get_root()->processed);
        EXPECT_EQ(state_map.get_root()->node_offset, 0u);
    }
    boost::filesystem::remove(temp_file);
}
