#include "catl/hybrid-shamap-v2/hmap-innernode.h"
#include "catl/hybrid-shamap-v2/hmap-leafnode.h"
#include "catl/hybrid-shamap-v2/hmap.h"
#include "catl/hybrid-shamap-v2/poly-node-ptr.h"
#include "catl/test-utils/test-utils.h"
#include "catl/v2/catl-v2-memtree.h"
#include "catl/v2/catl-v2-reader.h"
#include "catl/v2/catl-v2-writer.h"
#include <boost/filesystem.hpp>
#include <gtest/gtest.h>
#include <memory>

#include "catl/shamap/shamap.h"
#include "catl/test-utils/test-mmap-items.h"

using namespace catl::hybrid_shamap;
// This is shamap with compile time serialization traits built in
// precompiled header
using GoldMap = catl::v2::serialization::SHAMapS;

// Helper function to create fake ledger info
catl::common::LedgerInfo
fake_ledger(std::uint32_t seq, const Hash256& account_hash)
{
    return catl::common::LedgerInfo{
        .seq = seq,
        .drops = 0,
        .parent_hash = Hash256::zero(),
        .tx_hash = Hash256::zero(),
        .account_hash = account_hash,
        .parent_close_time = 0,
        .close_time = 0,
        .close_time_resolution = 0,
        .close_flags = 0,
        .hash = std::nullopt};
}

/**
 * Test fixture that creates a CATL v2 file with test data
 * and provides both gold standard SHAMap and hybrid map for comparison
 */
class HybridMapTestFixture
{
private:
    boost::filesystem::path temp_file_;
    std::shared_ptr<catl::v2::CatlV2Reader> reader_;
    std::unique_ptr<GoldMap> gold_map_;
    std::unique_ptr<Hmap> hybrid_map_;
    Hash256 expected_hash_;

public:
    /**
     * Create a test fixture with the given items
     * @param items Vector of MmapItems to add to the tree
     * @param seq Ledger sequence number (default 1)
     */
    explicit HybridMapTestFixture(
        const std::vector<boost::intrusive_ptr<MmapItem>>& items,
        uint32_t seq = 1)
    {
        // Create temp file path
        auto temp_dir = boost::filesystem::temp_directory_path();
        temp_file_ =
            temp_dir / boost::filesystem::unique_path("test-%%%%.catl2");

        // Create gold map and add all items
        gold_map_ = std::make_unique<GoldMap>(catl::shamap::tnACCOUNT_STATE);
        for (auto item : items)  // Remove const& to allow passing non-const
        {
            gold_map_->add_item(item);
        }

        // Get the expected hash from gold map
        expected_hash_ = gold_map_->get_hash();

        // Write to CATL v2 file
        {
            catl::v2::CatlV2Writer writer(temp_file_.string(), 0);

            // Create fake ledger info with the hash
            catl::common::LedgerInfo ledger_info =
                fake_ledger(seq, expected_hash_);

            // Empty tx map
            GoldMap tx_map(catl::shamap::tnTRANSACTION_MD);

            // Write ledger and finalize
            writer.write_ledger(ledger_info, *gold_map_, tx_map);
            writer.finalize();
        }

        // Open the file with reader
        reader_ = catl::v2::CatlV2Reader::create(temp_file_.string());
        reader_->read_ledger_info();

        // Create hybrid map from mmap'd data
        hybrid_map_ = std::make_unique<Hmap>(reader_->mmap_holder());
        hybrid_map_->set_root_raw(reader_->current_data());
    }

    ~HybridMapTestFixture()
    {
        // Clean up temp file
        if (boost::filesystem::exists(temp_file_))
        {
            boost::filesystem::remove(temp_file_);
        }
    }

    // Accessors
    GoldMap&
    gold_map()
    {
        return *gold_map_;
    }
    Hmap&
    hybrid_map()
    {
        return *hybrid_map_;
    }
    const Hash256&
    expected_hash() const
    {
        return expected_hash_;
    }
    catl::v2::CatlV2Reader&
    reader()
    {
        return *reader_;
    }
    const uint8_t*
    root_ptr() const
    {
        return reader_->current_data();
    }
};

// Basic sanity test to ensure the test framework is working
TEST(HybridShamapV2Basic, TestItemCreation)
{
    // This keeps the buffers around for the lifetime of the test
    TestMmapItems items;

    auto item1 = items.make(
        "0000000000000000000000000000000000000000000000000000000000000000");
    auto item2 = items.make(
        "0000000000000000000000000000000000000000000000000000000000000001",
        "DEAD");
    auto item3 = items.make(
        "0000000000000000000000000000000000000000000000000000000000000002",
        "BEEF");

    // We can access Key and slice
    Key key = item1->key();
    // Data is just the same as the key
    Slice data = item1->slice();

    EXPECT_EQ(
        key.hex(),
        "0000000000000000000000000000000000000000000000000000000000000000");
    EXPECT_EQ(data.size(), 32);

    // The hex() method returns the hex representation of the data
    EXPECT_EQ(item2->hex(), "DEAD");
    EXPECT_EQ(item3->hex(), "BEEF");

    // 3 keys, and 2 unique buffers for data, the key is just used as data when
    // the optional data is not provide
    EXPECT_EQ(items.get_buffers().size(), 5);

    auto map = GoldMap(catl::shamap::tnACCOUNT_STATE);

    map.add_item(item1);
    map.add_item(item2);
    map.add_item(item3);

    // We can access the hash
    EXPECT_EQ(
        map.get_hash().hex(),
        "344B6460ADF48B9604BB375E31B034EA19DB4B513A55444709641A95572DC24D");
}

// Simple test: hybrid map with raw mmap pointer gives correct hash
TEST(HybridShamapV2Basic, RawPointerHashCorrect)
{
    TestMmapItems items;

    // Create just a few test items
    items.make(
        "1111111111111111111111111111111111111111111111111111111111111111",
        "CAFE");
    items.make(
        "2222222222222222222222222222222222222222222222222222222222222222",
        "BABE");
    items.make(
        "3333333333333333333333333333333333333333333333333333333333333333",
        "FACE");

    // Create test fixture with CATL v2 file
    HybridMapTestFixture fixture(items.get_items());

    // THE CORE TEST: hybrid map with raw mmap pointer should have correct hash
    EXPECT_EQ(fixture.hybrid_map().get_root_hash(), fixture.expected_hash());
}