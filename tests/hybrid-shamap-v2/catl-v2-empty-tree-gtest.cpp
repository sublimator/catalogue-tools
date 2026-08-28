#include "catl/test-utils/test-mmap-items.h"
#include "hybrid-shamap-test-helpers.h"
#include <algorithm>
#include <gtest/gtest.h>

TEST(CatlV2EmptyTree, IsImpliedRatherThanSerialized)
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

    std::size_t zero_hash_nodes = 0;
    {
        catl::v2::CatlV2Writer writer(
            temp_file.string(),
            0,
            [&](catl::v2::CatlV2Writer::PhysicalNodeObservation const& node) {
                if (std::all_of(
                        node.hash.begin(),
                        node.hash.end(),
                        [](std::uint8_t byte) { return byte == 0; }))
                    ++zero_hash_nodes;
            });
        ASSERT_TRUE(writer.write_ledger(
            fake_ledger(1, state_map.get_hash()), state_map, tx_map));
        ASSERT_TRUE(writer.finalize());
    }

    auto reader = catl::v2::CatlV2Reader::create(temp_file.string());
    auto const* entry = reader->get_ledger_index().find_ledger(1);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry->tx_tree_offset, 0u);
    EXPECT_EQ(zero_hash_nodes, 0u);

    ASSERT_EQ(reader->read_ledger_info().seq, 1u);
    auto const missing_key = Key(Hash256::zero().data());
    EXPECT_FALSE(reader->lookup_key_in_tx(missing_key).has_value());
    EXPECT_EQ(
        reader->walk_tx_items([](Key const&, Slice const&) { return true; }),
        0u);
    reader->skip_state_map();
    EXPECT_EQ(reader->skip_tx_map(), 0u);

    boost::filesystem::remove(temp_file);
}
