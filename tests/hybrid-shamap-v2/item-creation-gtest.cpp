#include "catl/test-utils/test-mmap-items.h"
#include "hybrid-shamap-test-helpers.h"
#include <gtest/gtest.h>

using namespace catl::hybrid_shamap;

// Basic sanity test to ensure the test framework is working
TEST(ItemCreation, TestItemCreation)
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