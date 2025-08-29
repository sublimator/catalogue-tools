#include "catl/hybrid-shamap-v2/hmap-innernode.h"
#include "catl/hybrid-shamap-v2/hmap-leafnode.h"
#include "catl/hybrid-shamap-v2/hmap.h"
#include "catl/hybrid-shamap-v2/poly-node-ptr.h"
#include "catl/test-utils/test-utils.h"
#include <gtest/gtest.h>

#include "catl/shamap/shamap.h"
#include "catl/test-utils/test-mmap-items.h"

using namespace catl::hybrid_shamap;
// This is shamap with compile time serialization traits built in
// precompiled header
using GoldMap = catl::v2::serialization::SHAMapS;

// Hrmmm, actually, we're going to need to use the catl v2 shamap that uses the
// serialization traits using GoldMap = catl::shamap::SHAMap;
//
// TODO: we need a facility to create a fake ledger for testing
// We should create a temporary file/directory for the test
// We ned to make a testing ledger info with the expected account hash
// We need to make a catl v2 file with one of the trees
// CatlV2Writer writer(output_file, network_id);
// GoldMap state_map(catl::shamap::tnACCOUNT_STATE);
// GoldMap tx_map(catl::shamap::tnTRANSACTION_MD);
// add all the test items to the map ...
// then writer.write_ledger(ledger_info, state_map, tx_map);
// when write.finalize() is called, we should have a file with the expected hash
// then open the v2 reader, open the file, and get the state root pointer ...
// Then you can MemTreeOps

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

// We can