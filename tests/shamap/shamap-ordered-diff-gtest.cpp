#include "catl/shamap/shamap-diff.h"
#include "catl/shamap/shamap-ordered-diff.h"

#include "shamap-test-utils.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <vector>

using namespace catl::shamap;

namespace {

boost::intrusive_ptr<MmapItem>
add_item(
    SHAMap& map,
    TestMmapItems& items,
    std::uint8_t first_byte,
    std::string data)
{
    std::array<std::uint8_t, 32> key{};
    key[0] = first_byte;
    auto item = items.make(Key(key.data()).hex(), std::move(data));
    EXPECT_NE(map.set_item(item), SetResult::FAILED);
    return item;
}

struct Event
{
    std::uint8_t first_byte;
    OrderedDiffOp operation;

    friend bool
    operator==(Event const&, Event const&) = default;
};

std::string
byte_hex(std::uint8_t value)
{
    constexpr char digits[] = "0123456789ABCDEF";
    return {digits[value >> 4], digits[value & 0x0F]};
}

}  // namespace

TEST(SHAMapOrderedDiffTest, EmitsExactChangesInStrictKeyOrder)
{
    auto before = std::make_shared<SHAMap>(tnACCOUNT_STATE);
    auto after = std::make_shared<SHAMap>(tnACCOUNT_STATE);
    TestMmapItems items;

    auto unchanged_10 = add_item(*before, items, 0x10, "10");
    (void)add_item(*before, items, 0x30, "30");
    (void)add_item(*before, items, 0x50, "50B0");
    (void)add_item(*before, items, 0x70, "70");
    auto unchanged_90 = add_item(*before, items, 0x90, "90");

    EXPECT_NE(after->set_item(unchanged_10), SetResult::FAILED);
    (void)add_item(*after, items, 0x20, "20AD");
    (void)add_item(*after, items, 0x50, "50AF");
    (void)add_item(*after, items, 0x60, "60AD");
    EXPECT_NE(after->set_item(unchanged_90), SetResult::FAILED);

    std::vector<Event> events;
    auto const stats = visit_differences_ordered(
        *before,
        *after,
        [&](Key const& key,
            OrderedDiffOp operation,
            MmapItem const* old_item,
            MmapItem const* new_item) {
            EXPECT_EQ(old_item != nullptr, operation != OrderedDiffOp::added);
            EXPECT_EQ(new_item != nullptr, operation != OrderedDiffOp::deleted);
            events.push_back({key.data()[0], operation});
            return true;
        });

    EXPECT_TRUE(stats.complete);
    EXPECT_EQ(stats.added, 2);
    EXPECT_EQ(stats.modified, 1);
    EXPECT_EQ(stats.deleted, 2);
    EXPECT_GT(stats.subtrees_skipped_by_hash, 0);
    EXPECT_EQ(
        events,
        (std::vector<Event>{
            {0x20, OrderedDiffOp::added},
            {0x30, OrderedDiffOp::deleted},
            {0x50, OrderedDiffOp::modified},
            {0x60, OrderedDiffOp::added},
            {0x70, OrderedDiffOp::deleted},
        }));

    SHAMapDiff oracle(before, after);
    oracle.find();
    for (auto const& event : events)
    {
        std::array<std::uint8_t, 32> bytes{};
        bytes[0] = event.first_byte;
        Key const key(bytes.data());
        if (event.operation == OrderedDiffOp::added)
            EXPECT_TRUE(oracle.added().contains(key));
        else if (event.operation == OrderedDiffOp::modified)
            EXPECT_TRUE(oracle.modified().contains(key));
        else
            EXPECT_TRUE(oracle.deleted().contains(key));
    }
}

TEST(SHAMapOrderedDiffTest, AlignsDifferentlyCollapsedTrees)
{
    SHAMapOptions expanded_options{
        .tree_collapse_impl = TreeCollapseImpl::leafs_only,
        .reference_hash_impl = ReferenceHashImpl::use_synthetic_inners,
    };
    SHAMap before(tnTRANSACTION_MD, expanded_options);
    SHAMap after(
        tnTRANSACTION_MD,
        {.tree_collapse_impl = TreeCollapseImpl::leafs_and_inners});
    TestMmapItems items;

    auto const key_1 =
        "0000000000000000000000000000000000000000000000000000000000010000";
    auto const key_2 =
        "0000000000000000000000000000000000000000000000000000000000010100";
    auto const key_3 =
        "0000000000500000000000000000000000000000000000000000000000010100";
    auto const key_4 =
        "0000000000600000000000000000000000000000000000000000000000010100";

    auto before_1 = items.make(key_1, "AA");
    auto before_2 = items.make(key_2, "BB");
    auto before_3 = items.make(key_3, "CC");
    ASSERT_NE(before.set_item(before_1), SetResult::FAILED);
    ASSERT_NE(before.set_item(before_2), SetResult::FAILED);
    ASSERT_NE(before.set_item(before_3), SetResult::FAILED);
    before.collapse_tree();

    auto after_1 = items.make(key_1, "AB");
    auto after_3 = items.make(key_3, "CC");
    auto after_4 = items.make(key_4, "DD");
    ASSERT_NE(after.set_item(after_1), SetResult::FAILED);
    ASSERT_NE(after.set_item(after_3), SetResult::FAILED);
    ASSERT_NE(after.set_item(after_4), SetResult::FAILED);

    std::vector<std::pair<Key, OrderedDiffOp>> events;
    auto const stats = visit_differences_ordered(
        before,
        after,
        [&](Key const& key,
            OrderedDiffOp operation,
            MmapItem const*,
            MmapItem const*) {
            events.emplace_back(key, operation);
            return true;
        });

    ASSERT_TRUE(stats.complete);
    ASSERT_EQ(events.size(), 3);
    EXPECT_EQ(events[0], (std::pair{before_1->key(), OrderedDiffOp::modified}));
    EXPECT_EQ(events[1], (std::pair{before_2->key(), OrderedDiffOp::deleted}));
    EXPECT_EQ(events[2], (std::pair{after_4->key(), OrderedDiffOp::added}));
}

TEST(SHAMapOrderedDiffTest, VisitorCanStopWithoutMaterializingTheRemainder)
{
    SHAMap before(tnACCOUNT_STATE);
    SHAMap after(tnACCOUNT_STATE);
    TestMmapItems items;
    (void)add_item(after, items, 0x10, "10");
    (void)add_item(after, items, 0x20, "20");
    (void)add_item(after, items, 0x30, "30");

    std::size_t calls = 0;
    auto const stats = visit_differences_ordered(
        before,
        after,
        [&](Key const&, OrderedDiffOp, MmapItem const*, MmapItem const*) {
            ++calls;
            return false;
        });

    EXPECT_FALSE(stats.complete);
    EXPECT_EQ(stats.total(), 1);
    EXPECT_EQ(calls, 1);
}

TEST(SHAMapOrderedDiffTest, CowSnapshotMatchesMaterializedDiffOracle)
{
    auto before = std::make_shared<SHAMap>(
        tnACCOUNT_STATE,
        SHAMapOptions{.tree_collapse_impl = TreeCollapseImpl::leafs_only});
    TestMmapItems items;
    std::vector<Key> original_keys;

    for (std::uint8_t i = 0; i < 96; ++i)
    {
        std::array<std::uint8_t, 32> bytes{};
        bytes[0] = static_cast<std::uint8_t>((i % 8) << 4);
        bytes[1] = i;
        bytes[31] = static_cast<std::uint8_t>(i * 37);
        Key key(bytes.data());
        auto item = items.make(key.hex(), byte_hex(i) + byte_hex(i ^ 0x5A));
        ASSERT_NE(before->set_item(item), SetResult::FAILED);
        original_keys.push_back(item->key());
    }

    // snapshot() returns the frozen historical view; the original remains the
    // writable successor and lazily performs CoW on its next mutation.
    auto after = before;
    before = after->snapshot();
    for (std::size_t i = 0; i < original_keys.size(); ++i)
    {
        if ((i + 1) % 10 == 0)
        {
            ASSERT_TRUE(after->remove_item(original_keys[i]));
        }
        else if ((i + 1) % 11 == 0)
        {
            auto replacement = items.make(
                original_keys[i].hex(),
                byte_hex(static_cast<std::uint8_t>(i)) + "FFFF");
            ASSERT_EQ(after->set_item(replacement), SetResult::UPDATE);
        }
    }
    for (std::uint8_t i = 0; i < 24; ++i)
    {
        std::array<std::uint8_t, 32> bytes{};
        bytes[0] = static_cast<std::uint8_t>(0x80 | ((i % 8) << 4));
        bytes[1] = static_cast<std::uint8_t>(0xA0 + i);
        bytes[31] = static_cast<std::uint8_t>(i * 19);
        Key key(bytes.data());
        auto item = items.make(key.hex(), "AD" + byte_hex(i));
        ASSERT_EQ(after->set_item(item), SetResult::ADD);
    }

    SHAMapDiff oracle(before, after);
    oracle.find();

    std::set<Key> added;
    std::set<Key> modified;
    std::set<Key> deleted;
    std::optional<Key> prior;
    auto const stats = visit_differences_ordered(
        *before,
        *after,
        [&](Key const& key,
            OrderedDiffOp operation,
            MmapItem const*,
            MmapItem const*) {
            if (prior)
                EXPECT_LT(*prior, key);
            prior = key;
            if (operation == OrderedDiffOp::added)
                added.insert(key);
            else if (operation == OrderedDiffOp::modified)
                modified.insert(key);
            else
                deleted.insert(key);
            return true;
        });

    EXPECT_TRUE(stats.complete);
    EXPECT_EQ(added, oracle.added());
    EXPECT_EQ(modified, oracle.modified());
    EXPECT_EQ(deleted, oracle.deleted());
    EXPECT_GT(stats.subtrees_skipped_by_identity, 0);
    EXPECT_EQ(
        stats.total(),
        oracle.added().size() + oracle.modified().size() +
            oracle.deleted().size());
}
