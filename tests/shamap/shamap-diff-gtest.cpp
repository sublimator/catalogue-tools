#include "catl/shamap/shamap-diff.h"
#include "catl/shamap/shamap-nodetype.h"
#include <gtest/gtest.h>
#include <memory>

#include "shamap-test-utils.h"

using namespace catl::shamap;

// Basic test for SHAMapDiff functionality
TEST(SHAMapDiffTest, BasicDiff)
{
    // Create two maps
    auto map1 = std::make_shared<SHAMap>(tnACCOUNT_STATE);
    auto map2 = std::make_shared<SHAMap>(tnACCOUNT_STATE);

    // Test utilities for item creation
    TestItems items;

    // Create test items
    auto item1 = items.make(
        "0000000000000000000000000000000000000000000000000000000000000001");
    auto item2 = items.make(
        "0000000000000000000000000000000000000000000000000000000000000002");
    auto item3 = items.make(
        "0000000000000000000000000000000000000000000000000000000000000003");
    auto item1_modified = items.make(
        "0000000000000000000000000000000000000000000000000000000000000001",
        "AABBCCDDEEFF");  // Same key, different content

    // Add items to the first map
    map1->add_item(item1);
    map1->add_item(item2);

    // Add items to the second map
    map2->add_item(item1_modified);  // Modified
    map2->add_item(item3);           // Added
    // Note: item2 is not in map2 (deleted)

    // Create a diff
    SHAMapDiff diff(map1, map2);
    diff.find();

    // Verify the diff results
    EXPECT_EQ(diff.added().size(), 1);
    EXPECT_EQ(diff.modified().size(), 1);
    EXPECT_EQ(diff.deleted().size(), 1);

    // Check specific keys
    EXPECT_TRUE(diff.added().find(item3->key()) != diff.added().end());
    EXPECT_TRUE(diff.modified().find(item1->key()) != diff.modified().end());
    EXPECT_TRUE(diff.deleted().find(item2->key()) != diff.deleted().end());
}

// Test applying a diff to a map
TEST(SHAMapDiffTest, ApplyDiff)
{
    // Create two maps
    auto map1 = std::make_shared<SHAMap>(tnACCOUNT_STATE);
    auto map2 = std::make_shared<SHAMap>(tnACCOUNT_STATE);

    // Test utilities for item creation
    TestItems items;

    // Create test items
    auto item1 = items.make(
        "0000000000000000000000000000000000000000000000000000000000000001");
    auto item2 = items.make(
        "0000000000000000000000000000000000000000000000000000000000000002");
    auto item3 = items.make(
        "0000000000000000000000000000000000000000000000000000000000000003");
    auto item1_modified = items.make(
        "0000000000000000000000000000000000000000000000000000000000000001",
        "AABBCCDDEEFF");  // Same key, different content

    // Add items to the first map
    map1->add_item(item1);
    map1->add_item(item2);

    // Add items to the second map
    map2->add_item(item1_modified);  // Modified
    map2->add_item(item3);           // Added
    // Note: item2 is not in map2 (deleted)

    // Create a target map with the same content as map1
    auto target = std::make_shared<SHAMap>(tnACCOUNT_STATE);
    target->add_item(item1);
    target->add_item(item2);

    // Create and find diff
    SHAMapDiff diff(map1, map2);
    diff.find();

    // Apply the diff to target
    diff.apply(*target);

    // Verify that target now matches map2
    EXPECT_EQ(target->get_hash(), map2->get_hash());

    // Check specific items - using data comparison instead of hash
    auto resulting_item1 = target->get_item(item1->key());
    auto resulting_item3 = target->get_item(item3->key());
    auto resulting_item2 = target->get_item(item2->key());

    // Compare data contents directly
    EXPECT_NE(nullptr, resulting_item1);  // Item1 exists
    EXPECT_NE(nullptr, resulting_item3);  // Item3 was added
    EXPECT_EQ(nullptr, resulting_item2);  // Item2 was deleted

    // For modified item1, verify slices are different from original but match
    // modified
    if (resulting_item1 && item1 && item1_modified)
    {
        // Compare sizes first (quick check)
        EXPECT_NE(resulting_item1->slice().size(), item1->slice().size());
        EXPECT_EQ(
            resulting_item1->slice().size(), item1_modified->slice().size());

        // Check actual content matches modified
        if (resulting_item1->slice().size() == item1_modified->slice().size())
        {
            EXPECT_EQ(
                0,
                std::memcmp(
                    resulting_item1->slice().data(),
                    item1_modified->slice().data(),
                    resulting_item1->slice().size()));
        }
    }
}

// Test inverting a diff
TEST(SHAMapDiffTest, InvertDiff)
{
    // Create two maps
    auto map1 = std::make_shared<SHAMap>(tnACCOUNT_STATE);
    auto map2 = std::make_shared<SHAMap>(tnACCOUNT_STATE);

    // Test utilities for item creation
    TestItems items;

    // Create test items
    auto item1 = items.make(
        "0000000000000000000000000000000000000000000000000000000000000001");
    auto item2 = items.make(
        "0000000000000000000000000000000000000000000000000000000000000002");
    auto item3 = items.make(
        "0000000000000000000000000000000000000000000000000000000000000003");

    // Add items to the first map
    map1->add_item(item1);

    // Add items to the second map
    map2->add_item(item2);
    map2->add_item(item3);

    // Create and find diff from map1 to map2
    SHAMapDiff diff(map1, map2);
    diff.find();

    // Create the inverse diff from map2 to map1
    auto inverted_diff = diff.inverted();

    // Verify inverted diff
    EXPECT_EQ(inverted_diff->added().size(), diff.deleted().size());
    EXPECT_EQ(inverted_diff->deleted().size(), diff.added().size());
    EXPECT_EQ(inverted_diff->modified().size(), diff.modified().size());

    // Apply inverted diff to map2 copy
    auto map2_copy = std::make_shared<SHAMap>(*map2);
    inverted_diff->apply(*map2_copy);

    // Check that map2_copy now matches map1
    EXPECT_EQ(map2_copy->get_hash(), map1->get_hash());
}
