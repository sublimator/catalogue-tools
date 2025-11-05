#include "catl/core/logger.h"
#include "catl/shamap/shamap-innernode.h"
#include "catl/shamap/shamap-leafnode.h"
#include "catl/shamap/shamap.h"
#include "shamap-test-utils.h"
#include <gtest/gtest.h>

using namespace catl::shamap;

// Helper function to create a key from an integer
static std::string
make_key_hex(int seq)
{
    std::ostringstream oss;
    oss << std::hex << std::setfill('0') << std::setw(64) << seq;
    return oss.str();
}

class ShaMapCowVersioningFixture : public ::testing::Test
{
protected:
    void
    SetUp() override
    {
        // Enable DEBUG logging to see everything
        Logger::set_level(LogLevel::DEBUG);
        LOGI("===== Test Setup =====");
    }

    void
    TearDown() override
    {
        LOGI("===== Test Teardown =====");
    }
};

/**
 * Test to understand EXACTLY how versioning works with CoW
 */
TEST_F(ShaMapCowVersioningFixture, TraceVersioningLifecycle)
{
    LOGI("====== Starting TraceVersioningLifecycle ======");

    TestMmapItems items;

    // Step 1: Create a SHAMap WITHOUT CoW enabled (default)
    LOGI("===== Step 1: Create SHAMap without CoW =====");
    SHAMap state_map(tnACCOUNT_STATE);
    LOGI("Initial state map version: ", state_map.get_version());
    LOGI("CoW should not be enabled initially (version is 0)");

    // Step 2: Add first item - what version does it get?
    LOGI("===== Step 2: Add first item =====");
    auto item1 = items.make(make_key_hex(1));
    state_map.add_item(item1);

    // Walk the tree and log versions
    LOGI("After adding first item:");
    auto root = state_map.get_root();
    LOGI("  Root version: ", root->get_version());
    LOGI("  Root has ", root->get_branch_count(), " children");

    // Find the leaf
    for (int i = 0; i < 16; ++i)
    {
        if (root->has_child(i))
        {
            auto child = root->get_child(i);
            if (child->is_leaf())
            {
                auto leaf = boost::static_pointer_cast<SHAMapLeafNode>(child);
                LOGI(
                    "  Leaf at branch ",
                    i,
                    " has version: ",
                    leaf->get_version());
            }
        }
    }

    // Step 3: Update the same item - what happens?
    LOGI("===== Step 3: Update the same item =====");
    auto item1_updated = items.make(make_key_hex(1));
    state_map.update_item(item1_updated);

    LOGI("After updating item:");
    root = state_map.get_root();
    LOGI("  Root version: ", root->get_version());

    for (int i = 0; i < 16; ++i)
    {
        if (root->has_child(i))
        {
            auto child = root->get_child(i);
            if (child->is_leaf())
            {
                auto leaf = boost::static_pointer_cast<SHAMapLeafNode>(child);
                LOGI(
                    "  Leaf at branch ",
                    i,
                    " has version: ",
                    leaf->get_version());
            }
        }
    }

    // Step 4: Add more items to create inner nodes
    LOGI("===== Step 4: Add more items to create inner nodes =====");
    for (int i = 2; i <= 5; ++i)
    {
        auto item = items.make(make_key_hex(i * 1000));  // Spread them out
        state_map.add_item(item);
    }

    LOGI("After adding more items:");
    LOGI("  State map version: ", state_map.get_version());

    // Walk all nodes and log their versions
    std::function<void(const boost::intrusive_ptr<SHAMapTreeNode>&, int)>
        walk_all;
    walk_all = [&walk_all](
                   const boost::intrusive_ptr<SHAMapTreeNode>& node,
                   int depth) {
        if (!node)
            return;

        if (node->is_inner())
        {
            auto inner = boost::static_pointer_cast<SHAMapInnerNode>(node);
            LOGI(
                "  Inner at depth ",
                depth,
                " version: ",
                inner->get_version(),
                " with ",
                inner->get_branch_count(),
                " children");

            for (int i = 0; i < 16; ++i)
            {
                if (inner->has_child(i))
                {
                    walk_all(inner->get_child(i), depth + 1);
                }
            }
        }
        else if (node->is_leaf())
        {
            auto leaf = boost::static_pointer_cast<SHAMapLeafNode>(node);
            LOGI(
                "  Leaf at depth ",
                depth,
                " version: ",
                leaf->get_version(),
                " key: ",
                leaf->get_item()->key().hex().substr(0, 8),
                "...");
        }
    };

    root = state_map.get_root();
    walk_all(boost::static_pointer_cast<SHAMapTreeNode>(root), 0);

    // Step 5: Take a snapshot - this should enable CoW
    LOGI("===== Step 5: Take snapshot (enables CoW) =====");
    auto snapshot1 = state_map.snapshot();

    LOGI("After snapshot:");
    LOGI("  Original map version: ", state_map.get_version());
    LOGI("  Snapshot version: ", snapshot1->get_version());
    LOGI("  CoW should now be enabled (version > 0)");

    // Walk snapshot nodes
    LOGI("Snapshot nodes:");
    auto snap_root = snapshot1->get_root();
    walk_all(boost::static_pointer_cast<SHAMapTreeNode>(snap_root), 0);

    // Step 6: Add item to original AFTER CoW is enabled
    LOGI("===== Step 6: Add item to original after CoW enabled =====");
    auto item6 = items.make(make_key_hex(6));
    state_map.add_item(item6);

    LOGI("After adding item with CoW enabled:");
    root = state_map.get_root();
    LOGI("  Root version: ", root->get_version());

    // Find the new leaf
    for (int i = 0; i < 16; ++i)
    {
        if (root->has_child(i))
        {
            auto child = root->get_child(i);
            if (child->is_leaf())
            {
                auto leaf = boost::static_pointer_cast<SHAMapLeafNode>(child);
                auto key_hex = leaf->get_item()->key().hex();
                if (key_hex == make_key_hex(6))
                {
                    LOGI(
                        "  NEW leaf (key=6) has version: ",
                        leaf->get_version());
                }
            }
        }
    }

    // Step 7: Update an existing item after CoW is enabled
    LOGI("===== Step 7: Update existing item after CoW enabled =====");
    auto item2_updated = items.make(make_key_hex(2000));
    state_map.update_item(item2_updated);

    LOGI("After updating with CoW enabled:");
    root = state_map.get_root();
    walk_all(boost::static_pointer_cast<SHAMapTreeNode>(root), 0);

    LOGI("====== Test Complete ======");
}

/**
 * Test what happens when we track versions through multiple snapshots
 */
TEST_F(ShaMapCowVersioningFixture, MultipleSnapshotVersioning)
{
    LOGI("====== Starting MultipleSnapshotVersioning ======");

    TestMmapItems items;
    SHAMap state_map(tnACCOUNT_STATE);

    // Add items without CoW
    LOGI("===== Adding items without CoW =====");
    for (int i = 1; i <= 3; ++i)
    {
        auto item = items.make(make_key_hex(i));
        state_map.add_item(item);
    }

    LOGI("Version before any snapshot: ", state_map.get_version());

    // First snapshot
    LOGI("===== First snapshot =====");
    auto snap1 = state_map.snapshot();
    LOGI("After first snapshot:");
    LOGI("  Original version: ", state_map.get_version());
    LOGI("  Snap1 version: ", snap1->get_version());

    // Add items to original
    LOGI("===== Add items to original =====");
    for (int i = 4; i <= 6; ++i)
    {
        auto item = items.make(make_key_hex(i));
        state_map.add_item(item);
    }

    // Second snapshot
    LOGI("===== Second snapshot =====");
    auto snap2 = state_map.snapshot();
    LOGI("After second snapshot:");
    LOGI("  Original version: ", state_map.get_version());
    LOGI("  Snap1 version: ", snap1->get_version());
    LOGI("  Snap2 version: ", snap2->get_version());

    // Walk snap2 to find new nodes (same version as root)
    LOGI("===== Walking snap2 for new nodes (same version as root) =====");

    int new_node_count = 0;
    snap2->walk_new_nodes(
        [&](const boost::intrusive_ptr<SHAMapTreeNode>& node) {
            new_node_count++;
            if (node->is_leaf())
            {
                auto leaf = boost::static_pointer_cast<SHAMapLeafNode>(node);
                LOGD(
                    "  Found new leaf with key: ",
                    leaf->get_item()->key().hex().substr(0, 8),
                    "...");
            }
            return true;
        });

    LOGI("Found ", new_node_count, " new nodes in snap2");

    LOGI("====== Test Complete ======");
}