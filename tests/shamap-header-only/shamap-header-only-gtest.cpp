#include <gtest/gtest.h>

// Include the custom traits header
#include "../test-utils/test-helpers.h"
#include "catl/core/types.h"
#include "shamap-custom-traits.h"
#include <iostream>
#include <vector>

// Tests for the header-only implementation
TEST(SHAMapHeaderOnlyTest, CreateEmptyMap)
{
    CustomSHAMap map;
    EXPECT_TRUE(
        map.get_hash().hex() ==
        "0000000000000000000000000000000000000000000000000000000000000000");
    EXPECT_EQ(map.get_root()->node_offset, 1337);
    std::cout << map.get_root()->node_offset << std::endl;
}

// Test that traits without hooks compile and work fine
TEST(SHAMapHeaderOnlyTest, TraitsWithoutHooks)
{
    CustomSHAMap map(catl::shamap::tnACCOUNT_STATE);

    // Add an item
    auto key_storage = test_helpers::key_from_hex(
        "1111111111111111111111111111111111111111111111111111111111111111");
    auto data_storage = test_helpers::data_from_string("test data");
    Key key(key_storage.data());
    auto item = boost::intrusive_ptr<MmapItem>(
        new MmapItem(key.data(), data_storage.data(), data_storage.size()));

    auto result = map.add_item(item);
    EXPECT_EQ(result, catl::shamap::SetResult::ADD);

    // Create snapshot - should work fine without hooks
    auto snapshot = map.snapshot();
    EXPECT_TRUE(snapshot != nullptr);

    // Verify isolation
    auto key2_storage = test_helpers::key_from_hex(
        "2222222222222222222222222222222222222222222222222222222222222222");
    auto data2_storage = test_helpers::data_from_string("test data 2");
    Key key2(key2_storage.data());
    auto item2 = boost::intrusive_ptr<MmapItem>(
        new MmapItem(key2.data(), data2_storage.data(), data2_storage.size()));

    map.add_item(item2);
    EXPECT_TRUE(map.has_item(key2));
    EXPECT_FALSE(snapshot->has_item(key2));
}

// Test CoW hooks are invoked
TEST(SHAMapHeaderOnlyTest, CoWHooksInvoked)
{
    test_counters::reset();

    HookSHAMap map(catl::shamap::tnACCOUNT_STATE);

    // Initially no copies (hooks haven't been called yet)
    EXPECT_EQ(test_counters::inner_copies.load(), 0);
    EXPECT_EQ(test_counters::leaf_copies.load(), 0);

    // STEP 1: Build initial tree (CoW disabled)
    // This creates nodes but doesn't copy anything
    auto key_storage = test_helpers::key_from_hex(
        "1111111111111111111111111111111111111111111111111111111111111111");
    auto data_storage = test_helpers::data_from_string("test data");
    Key key(key_storage.data());
    auto item = boost::intrusive_ptr<MmapItem>(
        new MmapItem(key.data(), data_storage.data(), data_storage.size()));

    map.add_item(item);

    // Still no copies (nodes were created, not copied)
    EXPECT_EQ(test_counters::inner_copies.load(), 0);
    EXPECT_EQ(test_counters::leaf_copies.load(), 0);

    // STEP 2: Create snapshot - this enables CoW and triggers first copies
    // Inside snapshot():
    // - enable_cow() may copy root if it had do_cow_=false
    // - root->copy(snapshot_version) creates snapshot's root
    // - This triggers on_inner_node_copied() hook
    auto snapshot = map.snapshot();

    // Root inner node should have been copied (at least once for snapshot)
    EXPECT_GT(test_counters::inner_copies.load(), 0);

    // STEP 3: Modify the original tree
    // This forces CoW to copy nodes along the path to the modification
    int inner_copies_before = test_counters::inner_copies.load();

    auto key2_storage = test_helpers::key_from_hex(
        "2222222222222222222222222222222222222222222222222222222222222222");
    auto data2_storage = test_helpers::data_from_string("test data 2");
    Key key2(key2_storage.data());
    auto item2 = boost::intrusive_ptr<MmapItem>(
        new MmapItem(key2.data(), data2_storage.data(), data2_storage.size()));

    // When adding this item:
    // - PathFinder navigates from root to insertion point
    // - Detects version mismatches along the path
    // - Copies each node that needs updating (root → branch → new location)
    // - Each copy triggers on_inner_node_copied() hook
    map.add_item(item2);

    // Should have triggered more inner node copies (path from root to new leaf)
    EXPECT_GT(test_counters::inner_copies.load(), inner_copies_before);
}

// Test that hooks receive correct node information
TEST(SHAMapHeaderOnlyTest, HooksReceiveCorrectInfo)
{
    test_counters::reset();

    HookSHAMap map(catl::shamap::tnACCOUNT_STATE);

    // STEP 1: Build initial tree structure
    // At this point: CoW is DISABLED, all nodes have version=0, do_cow_=false
    auto key_storage = test_helpers::key_from_hex(
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    auto data_storage = test_helpers::data_from_string("test");
    Key key(key_storage.data());
    auto item = boost::intrusive_ptr<MmapItem>(
        new MmapItem(key.data(), data_storage.data(), data_storage.size()));
    map.add_item(item);

    // STEP 2: Create first snapshot to enable CoW
    // This triggers:
    // - enable_cow() which may replace the root if it wasn't created with CoW
    // - Original gets version 1, snapshot gets version 2
    // - Both trees now share nodes (which still have version 0)
    auto snapshot1 = map.snapshot();
    test_counters::reset();  // Reset counters after initial CoW setup

    // STEP 3: Set a specific offset on the current root for testing
    // This root now has CoW enabled and version 1
    map.get_root()->node_offset = 42;

    // STEP 4: Modify the tree - this triggers CoW copying
    // When we add this item, PathFinder will:
    // - Navigate from root to insertion point
    // - Detect version mismatch (root has v1, tree wants current version)
    // - Copy the root node, triggering our hook
    // - The hook will see source->node_offset = 42
    auto key2_storage = test_helpers::key_from_hex(
        "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB");
    auto data2_storage = test_helpers::data_from_string("test2");
    Key key2(key2_storage.data());
    auto item2 = boost::intrusive_ptr<MmapItem>(
        new MmapItem(key2.data(), data2_storage.data(), data2_storage.size()));
    map.add_item(item2);

    // VERIFICATION: The hook should have captured the source offset
    EXPECT_EQ(test_counters::last_source_offset.load(), 42);

    // VERIFICATION: The copy should have a newer version
    EXPECT_GT(test_counters::last_copy_version.load(), 0);
}

// Test that processed flag is reset in hooks
TEST(SHAMapHeaderOnlyTest, ProcessedFlagReset)
{
    HookSHAMap map(catl::shamap::tnACCOUNT_STATE);

    // STEP 1: Build initial tree (CoW disabled, version=0, do_cow_=false)
    auto key_storage = test_helpers::key_from_hex(
        "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB");
    auto data_storage = test_helpers::data_from_string("data");
    Key key(key_storage.data());
    auto item = boost::intrusive_ptr<MmapItem>(
        new MmapItem(key.data(), data_storage.data(), data_storage.size()));
    map.add_item(item);

    // STEP 2: Create first snapshot to enable CoW
    // CRITICAL: This may REPLACE the root if it wasn't created with CoW!
    // - If root has do_cow_=false, it gets copied to a new root with
    // do_cow_=true
    // - Original tree gets version 1, snapshot gets version 2
    auto snapshot1 = map.snapshot();

    // STEP 3: Mark root as "processed" (simulating it was written to disk)
    // This root now has: CoW enabled, version 1, processed=true
    map.get_root()->processed = true;
    auto original_root = map.get_root();  // Save pointer for comparison

    // STEP 4: Modify the tree - this triggers CoW path copying
    // According to CoW rules:
    // - PathFinder navigates to insertion point
    // - Finds root with version 1, but tree needs current version
    // - Must copy root (and path) before modification
    // - The copy() method calls our hook: on_inner_node_copied()
    // - Our hook sets: this->processed = false (resetting serialization state)
    auto key2_storage = test_helpers::key_from_hex(
        "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC");
    auto data2_storage = test_helpers::data_from_string("more data");
    Key key2(key2_storage.data());
    auto item2 = boost::intrusive_ptr<MmapItem>(
        new MmapItem(key2.data(), data2_storage.data(), data2_storage.size()));
    map.add_item(item2);

    // VERIFICATION 1: Root was replaced (CoW created a new root)
    EXPECT_NE(map.get_root(), original_root);

    // VERIFICATION 2: New root has processed=false (reset by our hook)
    // This is the key behavior for incremental serialization:
    // - Old nodes keep processed=true (already on disk)
    // - New/modified nodes get processed=false (need to be written)
    EXPECT_FALSE(map.get_root()->processed);

    // STEP 5: Create another snapshot to verify hook behavior continues
    auto snapshot2 = map.snapshot();

    // VERIFICATION 3: Snapshot's root also has processed=false
    // (The hook was called during root->copy() in snapshot creation)
    EXPECT_FALSE(snapshot2->get_root()->processed);
}