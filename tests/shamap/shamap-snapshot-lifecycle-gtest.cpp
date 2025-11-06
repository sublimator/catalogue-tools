#include "catl/core/logger.h"
#include "catl/shamap/shamap-nodetype.h"
#include "catl/shamap/shamap-options.h"
#include "catl/shamap/shamap.h"
#include <atomic>
#include <gtest/gtest.h>
#include <iomanip>
#include <memory>
#include <mutex>
#include <sstream>
#include <unordered_map>
#include <vector>

#include "shamap-test-utils.h"

using namespace catl::shamap;

// Forward declaration of destructor logging partition from shamap
namespace catl::shamap {
extern LogPartition destructor_log;
}

// Helper function to create a key from an integer
static std::string
make_key_hex(int seq)
{
    std::ostringstream oss;
    oss << std::hex << std::setfill('0') << std::setw(64) << seq;
    return oss.str();
}

// Tracking infrastructure for items
class ItemTracker
{
public:
    struct ItemStats
    {
        std::atomic<int> created{0};
        std::atomic<int> destroyed{0};
        std::mutex mutex;
        std::unordered_map<std::string, int> live_items;
    };

    static ItemStats&
    get_stats()
    {
        static ItemStats stats;
        return stats;
    }

    static void
    reset()
    {
        auto& stats = get_stats();
        stats.created = 0;
        stats.destroyed = 0;
        std::lock_guard<std::mutex> lock(stats.mutex);
        stats.live_items.clear();
    }

    static void
    item_created(const std::string& key_hex)
    {
        auto& stats = get_stats();
        stats.created++;
        std::lock_guard<std::mutex> lock(stats.mutex);
        stats.live_items[key_hex]++;
        // Don't log individual items - too verbose
        // LOGD("ITEM CREATED: ", key_hex.substr(0, 16), "... (count: ",
        // stats.live_items[key_hex], ")");
    }

    static void
    item_destroyed(const std::string& key_hex)
    {
        auto& stats = get_stats();
        stats.destroyed++;
        std::lock_guard<std::mutex> lock(stats.mutex);
        auto it = stats.live_items.find(key_hex);
        if (it != stats.live_items.end())
        {
            it->second--;
            // Don't log individual items - too verbose
            // LOGD("ITEM DESTROYED: ", key_hex.substr(0, 16), "... (remaining:
            // ", it->second, ")");
            if (it->second == 0)
            {
                stats.live_items.erase(it);
            }
        }
        else
        {
            // This is concerning, so keep this warning
            LOGW(
                "ITEM DESTROYED but not tracked: ",
                key_hex.substr(0, 16),
                "...");
        }
    }

    static void
    report()
    {
        auto& stats = get_stats();
        std::lock_guard<std::mutex> lock(stats.mutex);
        LOGI("===== Item Tracking Report =====");
        LOGI("Total created: ", stats.created.load());
        LOGI("Total destroyed: ", stats.destroyed.load());
        LOGI("Currently live: ", stats.live_items.size(), " unique items");
        int total_live = 0;
        size_t items_shown = 0;
        for (const auto& [key, count] : stats.live_items)
        {
            if (count > 0)
            {
                // Only show first few items to avoid clutter
                if (items_shown < 5)
                {
                    LOGI("  ", key.substr(0, 16), "...: ", count, " copies");
                    items_shown++;
                }
                total_live += count;
            }
        }
        if (items_shown < stats.live_items.size())
        {
            LOGI(
                "  ... and ",
                stats.live_items.size() - items_shown,
                " more unique items");
        }
        LOGI("Total live item copies: ", total_live);
        LOGI(
            "LEAK STATUS: ",
            (stats.created.load() == stats.destroyed.load()) ? "NO LEAK"
                                                             : "MEMORY LEAK!");
        LOGI("=================================");
    }
};

// Tracked version of MmapItem
class TrackedMmapItem : public MmapItem
{
private:
    std::string key_hex_;

public:
    TrackedMmapItem(
        const uint8_t* key_ptr,
        const uint8_t* data_ptr,
        size_t data_size,
        const std::string& key_hex)
        : MmapItem(key_ptr, data_ptr, data_size), key_hex_(key_hex)
    {
        ItemTracker::item_created(key_hex_);
    }

    ~TrackedMmapItem()
    {
        LOGI(
            "[TRACKED ITEM DESTRUCTOR] Destroying item with key: ",
            key_hex_.substr(0, 16),
            "...");
        ItemTracker::item_destroyed(key_hex_);
    }
};

// Helper class to create tracked items
class TrackedTestMmapItems
{
private:
    std::vector<std::vector<uint8_t>> buffers;
    // REMOVED test_items vector - it was keeping everything alive!

public:
    boost::intrusive_ptr<MmapItem>
    make_tracked(const std::string& hex_string)
    {
        if (hex_string.length() < 64)
        {
            throw std::invalid_argument(
                "Hex string must be at least 64 characters");
        }

        // Convert hex to bytes
        std::vector<uint8_t> key_bytes;
        for (size_t i = 0; i < 64; i += 2)
        {
            std::string byte_str = hex_string.substr(i, 2);
            key_bytes.push_back(
                static_cast<uint8_t>(std::stoi(byte_str, nullptr, 16)));
        }

        buffers.push_back(std::move(key_bytes));
        const uint8_t* key_ptr = buffers.back().data();

        // Create tracked item - NO LONGER storing it!
        auto item = boost::intrusive_ptr<MmapItem>(new TrackedMmapItem(
            key_ptr, key_ptr, 32, hex_string.substr(0, 64)));
        // REMOVED: test_items.push_back(item);  <-- THIS WAS THE LEAK!
        return item;
    }

    // Create items with specific patterns for tracking
    // Pattern: LLLLLLLL00000000000000000000000000000000000000000000000000IIIIII
    // Where LLLLLLLL = ledger number, IIIIII = item number
    boost::intrusive_ptr<MmapItem>
    make_for_ledger(uint32_t ledger, uint32_t item_num)
    {
        std::ostringstream oss;
        oss << std::hex << std::setfill('0');
        oss << std::setw(8) << ledger;    // 8 hex chars for ledger
        oss << std::setw(48) << 0;        // 48 hex chars of zeros
        oss << std::setw(8) << item_num;  // 8 hex chars for item number
        return make_tracked(oss.str());
    }
};

// Special test fixture that enables debug logging
class ShaMapLifecycleFixture : public ::testing::Test
{
protected:
    void
    SetUp() override
    {
        // Only enable INFO logging for cleaner output
        Logger::set_level(LogLevel::INFO);
        LOGI("===== Test Setup =====");
    }

    void
    TearDown() override
    {
        LOGI("===== Test Teardown =====");
    }
};

/**
 * Test that destructors are called when a simple snapshot goes out of scope
 */
TEST_F(ShaMapLifecycleFixture, SimpleSnapshotLifecycle)
{
    LOGI("Starting SimpleSnapshotLifecycle test");

    TestMmapItems items;

    {
        SHAMap parent_map(tnACCOUNT_STATE);

        // Add some items
        for (int i = 0; i < 5; ++i)
        {
            auto item = items.make(make_key_hex(i));
            ASSERT_EQ(parent_map.add_item(item), SetResult::ADD);
        }

        LOGI("Creating snapshot");
        {
            auto snapshot = parent_map.snapshot();
            LOGI("Snapshot created, will now go out of scope");
        }
        LOGI("Snapshot destroyed - check logs for destructor calls");

        LOGI("Parent map will now go out of scope");
    }
    LOGI("Parent map destroyed - check logs for destructor calls");
}

/**
 * Test multiple snapshots creation and destruction
 */
TEST_F(ShaMapLifecycleFixture, MultipleSnapshotsLifecycle)
{
    LOGI("Starting MultipleSnapshotsLifecycle test");

    TestMmapItems items;
    SHAMap parent_map(tnACCOUNT_STATE);

    // Add initial items
    for (int i = 0; i < 10; ++i)
    {
        auto item = items.make(make_key_hex(i));
        ASSERT_EQ(parent_map.add_item(item), SetResult::ADD);
    }

    LOGI("Creating 3 snapshots");
    {
        auto snapshot1 = parent_map.snapshot();
        LOGI("Snapshot 1 created");

        // Modify parent
        auto item = items.make(make_key_hex(100));
        parent_map.add_item(item);

        auto snapshot2 = parent_map.snapshot();
        LOGI("Snapshot 2 created");

        // Modify parent again
        item = items.make(make_key_hex(101));
        parent_map.add_item(item);

        auto snapshot3 = parent_map.snapshot();
        LOGI("Snapshot 3 created");

        LOGI("All 3 snapshots will now go out of scope");
    }
    LOGI("All snapshots destroyed - check logs for destructor calls");
}

/**
 * Test the memory leak pattern from catl1-to-nudb
 * Create snapshots in a loop, let them go out of scope immediately
 */
TEST_F(ShaMapLifecycleFixture, SnapshotMemoryLeakPattern)
{
    LOGI("Starting SnapshotMemoryLeakPattern test");

    TestMmapItems items;
    SHAMap state_map(
        tnACCOUNT_STATE,
        SHAMapOptions{.tree_collapse_impl = TreeCollapseImpl::leafs_only});

    // Simulate the pipeline pattern
    for (int ledger = 0; ledger < 10; ++ledger)
    {
        LOGI("Processing ledger ", ledger);

        // Add items (simulating ledger deltas)
        for (int i = 0; i < 5; ++i)
        {
            auto item = items.make(make_key_hex(ledger * 100 + i));
            state_map.add_item(item);
        }

        // Update some existing items
        if (ledger > 0)
        {
            for (int i = 0; i < 2; ++i)
            {
                auto item = items.make(make_key_hex((ledger - 1) * 100 + i));
                state_map.set_item(item, SetMode::ADD_OR_UPDATE);
            }
        }

        // Create snapshot that immediately goes out of scope
        {
            auto snapshot = state_map.snapshot();
            LOGD(
                "Snapshot for ledger ",
                ledger,
                " created, refcount on root should be 2");
            // Snapshot immediately destroyed here
        }
        LOGD(
            "Snapshot for ledger ",
            ledger,
            " destroyed, refcount on root should be 1");
    }

    LOGI("All ledgers processed - check if memory is accumulating");
}

/**
 * Test NodeChildren lifecycle when snapshots share structure
 */
TEST_F(ShaMapLifecycleFixture, NodeChildrenSharing)
{
    LOGI("Starting NodeChildrenSharing test");

    TestMmapItems items;
    SHAMap parent_map(tnACCOUNT_STATE);

    // Create a tree with multiple levels
    for (int i = 0; i < 20; ++i)
    {
        auto item = items.make(
            make_key_hex(i * 1000));  // Sparse to create multiple inner nodes
        parent_map.add_item(item);
    }

    LOGI("Creating snapshot to share NodeChildren");
    auto snapshot1 = parent_map.snapshot();

    LOGI("Modifying parent to trigger CoW");
    auto item = items.make(make_key_hex(5000));
    parent_map.set_item(item, SetMode::ADD_OR_UPDATE);

    LOGI("Creating second snapshot");
    auto snapshot2 = parent_map.snapshot();

    LOGI("Letting first snapshot go out of scope");
    snapshot1.reset();
    LOGI(
        "First snapshot destroyed - NodeChildren should still be alive if "
        "shared");

    LOGI("Letting second snapshot go out of scope");
    snapshot2.reset();
    LOGI("Second snapshot destroyed - NodeChildren should now be freed");
}

/**
 * Test that tracks reference counts through logging
 */
TEST_F(ShaMapLifecycleFixture, ReferenceCountTracking)
{
    LOGI("Starting ReferenceCountTracking test");

    TestMmapItems items;

    {
        SHAMap map1(tnACCOUNT_STATE);

        // Add items to create inner nodes with children
        for (int i = 0; i < 10; ++i)
        {
            auto item = items.make(make_key_hex(i));
            map1.add_item(item);
        }

        LOGI("Taking first snapshot");
        auto snapshot1 = map1.snapshot();

        LOGI("Taking second snapshot");
        auto snapshot2 = map1.snapshot();

        LOGI("Taking third snapshot");
        auto snapshot3 = map1.snapshot();

        LOGI("Destroying snapshots in reverse order");
        snapshot3.reset();
        LOGD("Snapshot 3 destroyed");

        snapshot2.reset();
        LOGD("Snapshot 2 destroyed");

        snapshot1.reset();
        LOGD("Snapshot 1 destroyed");

        LOGI("All snapshots destroyed, parent map will go out of scope");
    }

    LOGI("Parent map destroyed - all memory should be freed");
}

/**
 * Test update pattern that causes reference accumulation
 */
TEST_F(ShaMapLifecycleFixture, UpdateCausesReferenceAccumulation)
{
    LOGI("Starting UpdateCausesReferenceAccumulation test");

    TestMmapItems items;
    SHAMap state_map(tnACCOUNT_STATE);

    // Create initial state with one item
    auto key = make_key_hex(42);
    auto item = items.make(key);
    state_map.add_item(item);

    // Pattern that causes accumulation:
    for (int i = 0; i < 5; ++i)
    {
        LOGI("Update iteration ", i);

        // Take snapshot
        auto snapshot = state_map.snapshot();
        LOGD("Snapshot ", i, " created");

        // Update the same item
        auto updated_item = items.make(key);
        state_map.set_item(updated_item, SetMode::UPDATE_ONLY);
        LOGD("Item updated in parent");

        // Snapshot goes out of scope
        // But does the old item get freed?
    }

    LOGI("All iterations complete - checking for leaked references");
}

/**
 * Main test that demonstrates the exact leak from the pipeline
 */
TEST_F(ShaMapLifecycleFixture, PipelineExactPattern)
{
    LOGI("====== Starting PipelineExactPattern - THE MAIN LEAK TEST ======");

    TestMmapItems items;

    // Use same options as pipeline
    SHAMapOptions map_options;
    map_options.tree_collapse_impl = TreeCollapseImpl::leafs_only;

    SHAMap state_map(tnACCOUNT_STATE, map_options);

    // Track snapshots like the pipeline does
    std::vector<std::shared_ptr<SHAMap>> snapshots;

    // Simulate processing multiple ledgers
    for (int ledger = 1; ledger <= 5; ++ledger)
    {
        LOGI("========== Processing Ledger ", ledger, " ==========");

        // Add new items (like account creations)
        for (int i = 0; i < 3; ++i)
        {
            auto item = items.make(make_key_hex(ledger * 1000 + i));
            state_map.add_item(item);
            LOGD("Added new item: ", item->key().hex().substr(0, 8), "...");
        }

        // Update existing items (like balance changes)
        if (ledger > 1)
        {
            for (int i = 0; i < 2; ++i)
            {
                auto item = items.make(make_key_hex((ledger - 1) * 1000 + i));
                state_map.set_item(item, SetMode::ADD_OR_UPDATE);
                LOGD("Updated item: ", item->key().hex().substr(0, 8), "...");
            }
        }

        // Create snapshot (this is where the leak happens!)
        LOGI("Creating snapshot for ledger ", ledger);
        auto snapshot = state_map.snapshot();
        snapshots.push_back(snapshot);

        // In the real pipeline, old snapshots would be processed and freed
        // Let's simulate that by keeping only last 2 snapshots
        if (snapshots.size() > 2)
        {
            LOGI("Releasing old snapshot from ledger ", ledger - 2);
            snapshots.erase(snapshots.begin());
            LOGI("Old snapshot released - memory should be freed");
        }
    }

    LOGI("====== Clearing all remaining snapshots ======");
    snapshots.clear();

    LOGI("====== All snapshots cleared - checking for leaks ======");
    LOGI("If destructors are called properly, we should see:");
    LOGI("  - NodeChildren destructors for each version");
    LOGI("  - InnerNode destructors for modified nodes");
    LOGI("  - No accumulation of references");
}

// Helper class to count destructor calls
class DestructorCounter
{
public:
    static std::atomic<int> shamap_count;
    static std::atomic<int> innernode_count;
    static std::atomic<int> nodechildren_count;

    static void
    reset()
    {
        shamap_count = 0;
        innernode_count = 0;
        nodechildren_count = 0;
    }

    static void
    report()
    {
        LOGI("Destructor call counts:");
        LOGI("  SHAMap: ", shamap_count.load());
        LOGI("  InnerNode: ", innernode_count.load());
        LOGI("  NodeChildren: ", nodechildren_count.load());
    }
};

std::atomic<int> DestructorCounter::shamap_count{0};
std::atomic<int> DestructorCounter::innernode_count{0};
std::atomic<int> DestructorCounter::nodechildren_count{0};

/**
 * Test with instrumented counting (requires modifying destructors to increment
 * counters)
 */
TEST_F(ShaMapLifecycleFixture, CountDestructorCalls)
{
    LOGI("Starting CountDestructorCalls test");
    LOGI(
        "NOTE: This test requires modifying destructors to increment counters");

    DestructorCounter::reset();

    TestMmapItems items;

    {
        SHAMap map(tnACCOUNT_STATE);

        // Add items
        for (int i = 0; i < 10; ++i)
        {
            auto item = items.make(make_key_hex(i));
            map.add_item(item);
        }

        // Create and destroy snapshots
        for (int i = 0; i < 3; ++i)
        {
            auto snapshot = map.snapshot();
            // Snapshot destroyed here
        }
    }

    // Report counts
    // Note: These won't be accurate unless we modify the actual destructors
    // to increment these counters
    DestructorCounter::report();
}

/**
 * Test with tracked items to see exactly what gets leaked
 */
TEST_F(ShaMapLifecycleFixture, TrackedItemLifecycle)
{
    LOGI(
        "====== Starting TrackedItemLifecycle - TRACKING ITEM LIFETIMES "
        "======");

    ItemTracker::reset();
    TrackedTestMmapItems tracked_items;

    {
        SHAMap state_map(tnACCOUNT_STATE);

        LOGI("===== Adding items for Ledger 1 =====");
        for (int i = 0; i < 3; ++i)
        {
            auto item = tracked_items.make_for_ledger(1, i);
            state_map.add_item(item);
        }
        ItemTracker::report();

        LOGI("===== Creating snapshot 1 =====");
        auto snapshot1 = state_map.snapshot();
        ItemTracker::report();

        LOGI("===== Adding items for Ledger 2 =====");
        for (int i = 0; i < 3; ++i)
        {
            auto item = tracked_items.make_for_ledger(2, i);
            state_map.add_item(item);
        }
        ItemTracker::report();

        LOGI("===== Creating snapshot 2 =====");
        auto snapshot2 = state_map.snapshot();
        ItemTracker::report();

        LOGI("===== Updating Ledger 1 items in parent =====");
        for (int i = 0; i < 2; ++i)
        {
            auto item = tracked_items.make_for_ledger(
                1, i + 100);  // Different item number to track updates
            state_map.set_item(item, SetMode::UPDATE_ONLY);
        }
        ItemTracker::report();

        LOGI("===== Destroying snapshot 1 =====");
        snapshot1.reset();
        ItemTracker::report();

        LOGI("===== Destroying snapshot 2 =====");
        snapshot2.reset();
        ItemTracker::report();

        LOGI("===== Parent map will be destroyed =====");
    }

    LOGI("===== All maps destroyed - FINAL REPORT =====");
    ItemTracker::report();

    // Check for leaks
    auto& stats = ItemTracker::get_stats();
    if (stats.created != stats.destroyed)
    {
        LOGE(
            "MEMORY LEAK DETECTED: Created ",
            stats.created.load(),
            " items but only destroyed ",
            stats.destroyed.load());
    }
}

/**
 * Test the exact pipeline pattern with tracked items
 */
TEST_F(ShaMapLifecycleFixture, TrackedPipelinePattern)
{
    LOGI(
        "====== Starting TrackedPipelinePattern - SIMULATING PIPELINE WITH "
        "TRACKING ======");

    ItemTracker::reset();
    TrackedTestMmapItems tracked_items;

    SHAMapOptions map_options;
    map_options.tree_collapse_impl = TreeCollapseImpl::leafs_only;

    SHAMap state_map(tnACCOUNT_STATE, map_options);
    std::vector<std::shared_ptr<SHAMap>> snapshots;

    for (int ledger = 1; ledger <= 5; ++ledger)
    {
        LOGI("========== Processing Ledger ", ledger, " ==========");

        // Add new items
        for (int i = 0; i < 3; ++i)
        {
            auto item = tracked_items.make_for_ledger(ledger, i);
            state_map.add_item(item);
        }

        // Update items from previous ledger
        if (ledger > 1)
        {
            for (int i = 0; i < 2; ++i)
            {
                auto item = tracked_items.make_for_ledger(
                    ledger - 1, i + 1000);  // Mark as update
                state_map.set_item(item, SetMode::ADD_OR_UPDATE);
            }
        }

        // Create snapshot
        LOGI("Creating snapshot for ledger ", ledger);
        auto snapshot = state_map.snapshot();
        snapshots.push_back(snapshot);

        // Keep only last 2 snapshots (simulate pipeline)
        if (snapshots.size() > 2)
        {
            LOGI("Releasing old snapshot from ledger ", ledger - 2);
            snapshots.erase(snapshots.begin());
            ItemTracker::report();
        }
    }

    LOGI("====== Clearing all snapshots ======");
    snapshots.clear();
    ItemTracker::report();

    LOGI("====== Final cleanup ======");
    // state_map goes out of scope here
}

/**
 * Test with destructor partition logging enabled
 */
TEST_F(ShaMapLifecycleFixture, DestructorChainTracking)
{
    LOGI(
        "====== Starting DestructorChainTracking - TRACKING DESTRUCTOR CALLS "
        "======");

    // Enable ONLY the destructor partition - no need to change global log
    // level!
    catl::shamap::destructor_log.enable(LogLevel::DEBUG);
    LOGI(
        "Enabled DESTRUCTOR partition logging at DEBUG level (global remains "
        "at INFO)");

    ItemTracker::reset();

    {
        TrackedTestMmapItems tracked_items;

        {
            SHAMap state_map(tnACCOUNT_STATE);

            LOGI("===== Adding 3 items =====");
            for (int i = 0; i < 3; ++i)
            {
                auto item = tracked_items.make_for_ledger(1, i);
                state_map.add_item(item);
            }

            LOGI("===== Creating snapshot =====");
            auto snapshot = state_map.snapshot();

            LOGI("===== Adding 2 more items to parent =====");
            for (int i = 0; i < 2; ++i)
            {
                auto item = tracked_items.make_for_ledger(2, i);
                state_map.add_item(item);
            }

            LOGI("===== Destroying snapshot =====");
            snapshot.reset();
            LOGI("Snapshot destroyed - check DESTRUCTOR logs");

            LOGI("===== Parent map will be destroyed =====");
        }

        LOGI("===== All maps destroyed =====");
        ItemTracker::report();

        LOGI("===== TrackedTestMmapItems will be destroyed =====");
    }

    LOGI("===== TrackedTestMmapItems destroyed =====");
    ItemTracker::report();

    // Disable the destructor partition
    catl::shamap::destructor_log.disable();
}