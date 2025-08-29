#include "catl/test-utils/test-mmap-items.h"
#include "hybrid-shamap-test-helpers.h"
#include <gtest/gtest.h>

using namespace catl::hybrid_shamap;

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

// Simple test: hybrid map with raw mmap pointer gives correct hash
TEST(HybridShamapV2Basic, AddingAndRemovingxFourthItem)
{
    TestMmapItems items;

    // TODO: some kind of reusable way to create base items
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

    auto item4 = items.make(
        "4444444444444444444444444444444444444444444444444444444444444444",
        "BADE");

    // TODO: testing macro to add to both maps
    fixture.gold_map().add_item(item4);
    fixture.hybrid_map().set_item(item4->key(), item4->slice());

    // TODO: testing macro to test both maps have same hash
    EXPECT_EQ(
        fixture.hybrid_map().get_root_hash(), fixture.gold_map().get_hash());

    fixture.hybrid_map().remove_item(item4->key());
    EXPECT_EQ(fixture.hybrid_map().get_root_hash(), fixture.expected_hash());
}
