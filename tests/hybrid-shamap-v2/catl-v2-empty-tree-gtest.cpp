#include "catl/test-utils/test-mmap-items.h"
#include "hybrid-shamap-test-helpers.h"
#include <algorithm>
#include <cstring>
#include <fstream>
#include <gtest/gtest.h>
#include <vector>

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
