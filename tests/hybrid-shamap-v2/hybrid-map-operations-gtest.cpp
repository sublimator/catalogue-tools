#include "catl/test-utils/test-mmap-items.h"
#include "hybrid-shamap-test-helpers.h"
#include <gtest/gtest.h>

using namespace catl::hybrid_shamap;

// Simple test: hybrid map with raw mmap pointer gives correct hash
TEST(HybridMapOperations, RawPointerHashCorrect)
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

// Test adding and removing items from hybrid map
TEST(HybridMapOperations, AddingAndRemovingFourthItem)
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