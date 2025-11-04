#include "catl/shamap/shamap.h"
#include "catl/shamap/shamap-nodetype.h"
#include "catl/shamap/shamap-options.h"
#include <gtest/gtest.h>
#include <thread>
#include <atomic>
#include <vector>
#include <sstream>
#include <iomanip>

#include "shamap-test-utils.h"

using namespace catl::shamap;

// Helper function to create a key from an integer
static std::string make_key_hex(int seq) {
    std::ostringstream oss;
    oss << std::hex << std::setfill('0') << std::setw(64) << seq;
    return oss.str();
}

/**
 * Test concurrent modification of parent tree while hashing snapshot
 *
 * This reproduces the access pattern from catl1-to-nudb pipeline:
 * - Thread 1 (builder): Creates snapshot, then continues modifying parent
 * - Thread 2 (hasher): Hashes the snapshot while parent is being modified
 */
TEST(ShaMapCoWConcurrency, BasicSnapshotHashingWhileModifying)
{
    SHAMap parent_map(tnACCOUNT_STATE);
    TestMmapItems items;

    // Add some initial items
    for (int i = 0; i < 100; ++i) {
        auto item = items.make(make_key_hex(i));
        ASSERT_EQ(parent_map.add_item(item), SetResult::ADD);
    }

    // Create snapshot
    auto snapshot = parent_map.snapshot();

    std::atomic<bool> error_occurred{false};
    std::atomic<bool> modifier_done{false};

    // Thread 1: Continue modifying parent
    std::thread modifier([&]() {
        try {
            for (int i = 100; i < 200; ++i) {
                if (error_occurred.load()) break;

                auto item = items.make(make_key_hex(i));
                auto result = parent_map.add_item(item);
                EXPECT_EQ(result, SetResult::ADD);
            }
            modifier_done.store(true);
        } catch (const std::exception& e) {
            std::cerr << "Modifier exception: " << e.what() << std::endl;
            error_occurred.store(true);
        }
    });

    // Thread 2: Hash the snapshot
    std::thread hasher([&]() {
        try {
            while (!modifier_done.load()) {
                if (error_occurred.load()) break;

                // This should be safe - snapshot should be immutable
                Hash256 hash = snapshot->get_hash();
                EXPECT_NE(hash.hex(), "0000000000000000000000000000000000000000000000000000000000000000");

                std::this_thread::yield();
            }
        } catch (const std::exception& e) {
            std::cerr << "Hasher exception: " << e.what() << std::endl;
            error_occurred.store(true);
        }
    });

    modifier.join();
    hasher.join();

    EXPECT_FALSE(error_occurred.load()) << "Thread safety violation detected";
}

/**
 * Test the exact pipeline pattern: snapshot → modify → hash
 *
 * This is closer to what happens in catl1-to-nudb where:
 * 1. Builder snapshots after ledger N
 * 2. Builder immediately starts building ledger N+1
 * 3. Hasher hashes ledger N snapshot
 */
TEST(ShaMapCoWConcurrency, PipelinePattern)
{
    SHAMap state_map(tnACCOUNT_STATE,
                     SHAMapOptions{.tree_collapse_impl = TreeCollapseImpl::leafs_only});
    TestMmapItems items;

    std::vector<std::shared_ptr<SHAMap>> snapshots;
    std::atomic<bool> error_occurred{false};
    std::atomic<bool> builder_done{false};
    std::atomic<int> snapshots_ready{0};

    // Builder thread: Create snapshots and continue modifying
    std::thread builder([&]() {
        try {
            for (int ledger = 0; ledger < 100; ++ledger) {
                if (error_occurred.load()) break;

                // Add some items (simulating ledger deltas)
                for (int i = 0; i < 10; ++i) {
                    auto item = items.make(make_key_hex(ledger * 10 + i));
                    state_map.add_item(item);
                }

                // Create snapshot
                auto snapshot = state_map.snapshot();
                snapshots.push_back(snapshot);
                snapshots_ready.fetch_add(1);

                // Immediately continue to next ledger (this is where CoW must work!)
            }
            builder_done.store(true);
        } catch (const std::exception& e) {
            std::cerr << "Builder exception: " << e.what() << std::endl;
            error_occurred.store(true);
            builder_done.store(true);
        }
    });

    // Hasher thread: Hash snapshots as they become available
    std::thread hasher([&]() {
        try {
            int hashed_count = 0;
            while (!builder_done.load() || hashed_count < snapshots_ready.load()) {
                if (error_occurred.load()) break;

                int ready = snapshots_ready.load();
                if (hashed_count < ready) {
                    // Hash the snapshot
                    Hash256 hash = snapshots[hashed_count]->get_hash();
                    EXPECT_NE(hash.hex(), "0000000000000000000000000000000000000000000000000000000000000000");
                    hashed_count++;
                } else {
                    std::this_thread::yield();
                }
            }
        } catch (const std::exception& e) {
            std::cerr << "Hasher exception: " << e.what() << std::endl;
            error_occurred.store(true);
        }
    });

    builder.join();
    hasher.join();

    EXPECT_FALSE(error_occurred.load()) << "Thread safety violation in pipeline pattern";
    EXPECT_EQ(snapshots.size(), 100);
}

/**
 * Test with updates to existing keys (not just additions)
 *
 * This tests CoW when modifying existing nodes, which is what happens
 * when applying deltas to the state map.
 */
TEST(ShaMapCoWConcurrency, UpdatesWithSnapshots)
{
    SHAMap state_map(tnACCOUNT_STATE,
                     SHAMapOptions{.tree_collapse_impl = TreeCollapseImpl::leafs_only});
    TestMmapItems items;

    // Create initial state with 100 items
    std::vector<Key> keys;
    for (int i = 0; i < 100; ++i) {
        auto item = items.make(make_key_hex(i));
        keys.push_back(item->key());
        state_map.add_item(item);
    }

    std::atomic<bool> error_occurred{false};
    std::atomic<bool> updater_done{false};

    // Create initial snapshot
    auto snapshot = state_map.snapshot();

    // Thread 1: Update existing keys
    std::thread updater([&]() {
        try {
            for (int round = 0; round < 50; ++round) {
                if (error_occurred.load()) break;

                // Update each key
                for (const auto& key : keys) {
                    auto item = items.make(key.hex());
                    auto result = state_map.set_item(item, SetMode::UPDATE_ONLY);
                    EXPECT_EQ(result, SetResult::UPDATE);
                }
            }
            updater_done.store(true);
        } catch (const std::exception& e) {
            std::cerr << "Updater exception: " << e.what() << std::endl;
            error_occurred.store(true);
        }
    });

    // Thread 2: Repeatedly hash the snapshot
    std::thread hasher([&]() {
        try {
            Hash256 expected_hash = snapshot->get_hash();

            while (!updater_done.load()) {
                if (error_occurred.load()) break;

                // Snapshot hash should remain constant
                Hash256 hash = snapshot->get_hash();
                EXPECT_EQ(hash.hex(), expected_hash.hex())
                    << "Snapshot hash changed during concurrent updates!";

                std::this_thread::yield();
            }
        } catch (const std::exception& e) {
            std::cerr << "Hasher exception: " << e.what() << std::endl;
            error_occurred.store(true);
        }
    });

    updater.join();
    hasher.join();

    EXPECT_FALSE(error_occurred.load()) << "Thread safety violation with updates";
}

/**
 * Stress test: Multiple snapshots with aggressive modifications
 *
 * This creates high contention to expose any race conditions.
 */
TEST(ShaMapCoWConcurrency, StressTest)
{
    SHAMap state_map(tnACCOUNT_STATE,
                     SHAMapOptions{.tree_collapse_impl = TreeCollapseImpl::leafs_only});
    TestMmapItems items;

    std::vector<std::shared_ptr<SHAMap>> snapshots;
    std::atomic<bool> error_occurred{false};
    std::atomic<bool> builder_done{false};
    std::atomic<int> snapshots_ready{0};

    // Builder: Create many snapshots rapidly
    std::thread builder([&]() {
        try {
            for (int i = 0; i < 1000; ++i) {
                if (error_occurred.load()) break;

                // Add/update some items
                auto item = items.make(make_key_hex(i));
                state_map.add_item(item);

                // Snapshot every 10 modifications
                if (i % 10 == 0) {
                    snapshots.push_back(state_map.snapshot());
                    snapshots_ready.fetch_add(1);
                }
            }
            builder_done.store(true);
        } catch (const std::exception& e) {
            std::cerr << "Builder exception: " << e.what() << std::endl;
            error_occurred.store(true);
        }
    });

    // Multiple hasher threads
    auto hasher_work = [&]() {
        try {
            int last_hashed = 0;
            while (!builder_done.load() || last_hashed < snapshots_ready.load()) {
                if (error_occurred.load()) break;

                int ready = snapshots_ready.load();
                if (last_hashed < ready) {
                    // Hash a snapshot
                    Hash256 hash = snapshots[last_hashed]->get_hash();
                    EXPECT_NE(hash.hex(), "0000000000000000000000000000000000000000000000000000000000000000");
                    last_hashed++;
                } else {
                    std::this_thread::yield();
                }
            }
        } catch (const std::exception& e) {
            std::cerr << "Hasher exception: " << e.what() << std::endl;
            error_occurred.store(true);
        }
    };

    std::thread hasher1(hasher_work);
    std::thread hasher2(hasher_work);

    builder.join();
    hasher1.join();
    hasher2.join();

    EXPECT_FALSE(error_occurred.load()) << "Thread safety violation in stress test";
}

/**
 * Test for race condition between canonicalize (during hash) and copy (during CoW)
 *
 * This targets the specific crash we're seeing where:
 * 1. Thread 1 (hasher) calls get_hash() which triggers canonicalize()
 * 2. Thread 2 (modifier) triggers CoW which calls copy() on NodeChildren
 * 3. Race condition: canonicalize() replaces children while copy() is reading it
 */
TEST(ShaMapCoWConcurrency, CanonicalizeVsCopyRace)
{
    SHAMap state_map(tnACCOUNT_STATE,
                     SHAMapOptions{.tree_collapse_impl = TreeCollapseImpl::leafs_only});
    TestMmapItems items;

    // Fill map with enough items to trigger canonicalization
    // We need sparse nodes (not fully filled) for canonicalize to do something
    for (int i = 0; i < 200; i += 3) {  // Skip some to create sparse nodes
        auto item = items.make(make_key_hex(i));
        state_map.add_item(item);
    }

    std::atomic<bool> error_occurred{false};
    std::atomic<bool> stop_threads{false};
    std::atomic<int> hash_count{0};
    std::atomic<int> modify_count{0};

    // Take initial snapshot
    auto snapshot = state_map.snapshot();

    // Thread 1: Continuously hash the snapshot (triggers canonicalize)
    std::thread hasher([&]() {
        try {
            while (!stop_threads.load()) {
                if (error_occurred.load()) break;

                // This triggers canonicalize() on inner nodes
                Hash256 hash = snapshot->get_hash();
                EXPECT_NE(hash.hex(), "0000000000000000000000000000000000000000000000000000000000000000");

                hash_count.fetch_add(1);

                // Small yield to increase chance of race
                std::this_thread::yield();
            }
        } catch (const std::exception& e) {
            std::cerr << "Hasher exception: " << e.what() << std::endl;
            error_occurred.store(true);
        }
    });

    // Thread 2: Continuously modify the parent (triggers CoW and copy)
    std::thread modifier([&]() {
        try {
            int key_counter = 1000;
            while (!stop_threads.load()) {
                if (error_occurred.load()) break;

                // Add new items to trigger CoW
                auto item = items.make(make_key_hex(key_counter++));
                auto result = state_map.add_item(item);
                EXPECT_EQ(result, SetResult::ADD);

                modify_count.fetch_add(1);

                // Small yield to increase chance of race
                std::this_thread::yield();
            }
        } catch (const std::exception& e) {
            std::cerr << "Modifier exception: " << e.what() << std::endl;
            error_occurred.store(true);
        }
    });

    // Let it run for a bit
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    stop_threads.store(true);

    hasher.join();
    modifier.join();

    EXPECT_FALSE(error_occurred.load()) << "Race condition detected between canonicalize and copy";

    // Make sure both threads actually did work
    EXPECT_GT(hash_count.load(), 0) << "Hasher didn't run";
    EXPECT_GT(modify_count.load(), 0) << "Modifier didn't run";
}

/**
 * Test multiple concurrent snapshots being hashed while parent is modified
 *
 * This is closer to the actual catl1-to-nudb pattern where we have:
 * - Multiple snapshots in flight
 * - Hashing happens on older snapshots while newer ones are created
 * - Parent continues to be modified
 */
TEST(ShaMapCoWConcurrency, MultipleSnapshotsWithCanonicalization)
{
    SHAMap state_map(tnACCOUNT_STATE,
                     SHAMapOptions{
                         .tree_collapse_impl = TreeCollapseImpl::leafs_only
                     });
    TestMmapItems items;

    // Create initial sparse tree
    for (int i = 0; i < 500; i += 7) {  // Sparse to trigger canonicalization
        auto item = items.make(make_key_hex(i));
        state_map.add_item(item);
    }

    std::vector<std::shared_ptr<SHAMap>> snapshots;
    std::mutex snapshots_mutex;
    std::atomic<bool> error_occurred{false};
    std::atomic<bool> stop_threads{false};

    // Thread 1: Create snapshots periodically
    std::thread snapshotter([&]() {
        try {
            while (!stop_threads.load()) {
                if (error_occurred.load()) break;

                auto snapshot = state_map.snapshot();
                {
                    std::lock_guard<std::mutex> lock(snapshots_mutex);
                    snapshots.push_back(snapshot);
                    // Keep only last 10 snapshots
                    if (snapshots.size() > 10) {
                        snapshots.erase(snapshots.begin());
                    }
                }

                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
        } catch (const std::exception& e) {
            std::cerr << "Snapshotter exception: " << e.what() << std::endl;
            error_occurred.store(true);
        }
    });

    // Thread 2: Continuously modify parent
    std::thread modifier([&]() {
        try {
            int key_counter = 10000;
            while (!stop_threads.load()) {
                if (error_occurred.load()) break;

                // Mix of adds and updates
                if (key_counter % 3 == 0) {
                    // Update existing
                    auto item = items.make(make_key_hex(key_counter % 500));
                    state_map.set_item(item);  // Default is UPDATE_OR_ADD
                } else {
                    // Add new
                    auto item = items.make(make_key_hex(key_counter));
                    state_map.add_item(item);
                }
                key_counter++;

                std::this_thread::yield();
            }
        } catch (const std::exception& e) {
            std::cerr << "Modifier exception: " << e.what() << std::endl;
            error_occurred.store(true);
        }
    });

    // Thread 3: Hash all available snapshots
    std::thread hasher([&]() {
        try {
            while (!stop_threads.load()) {
                if (error_occurred.load()) break;

                std::vector<std::shared_ptr<SHAMap>> local_snapshots;
                {
                    std::lock_guard<std::mutex> lock(snapshots_mutex);
                    local_snapshots = snapshots;
                }

                for (const auto& snapshot : local_snapshots) {
                    if (error_occurred.load()) break;

                    // Hash each snapshot - this triggers canonicalize
                    Hash256 hash = snapshot->get_hash();
                    EXPECT_NE(hash.hex(), "0000000000000000000000000000000000000000000000000000000000000000");
                }

                std::this_thread::yield();
            }
        } catch (const std::exception& e) {
            std::cerr << "Hasher exception: " << e.what() << std::endl;
            error_occurred.store(true);
        }
    });

    // Let it run for a bit
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    stop_threads.store(true);

    snapshotter.join();
    modifier.join();
    hasher.join();

    EXPECT_FALSE(error_occurred.load()) << "Race condition in multi-snapshot scenario";
}
