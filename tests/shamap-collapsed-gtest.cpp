#include <gtest/gtest.h>
#include <iostream>
#include <utility>
#include "utils/test-utils.h"
#include "../src/shamap/src/pretty-print-json.h"
#include "catl/core/logger.h"

// Test for node collapsing behavior, particularly with shallow trees
TEST(CollapseTest, WithSkips) {
    // Create a transaction-like tree (shallow)
    // Add a series of items that will create a specific structure
    TestItems items;

    auto i1 = items.make("0000000000000000000000000000000000000000000000000000000000010000");
    auto i2 = items.make("0000000000000000000000000000000000000000000000000000000000010100");
    auto i3 = items.make("0000000000500000000000000000000000000000000000000000000000010100");
    auto i4 = items.make("0000000000600000000000000000000000000000000000000000000000010100");

    auto canonical_map = SHAMap(tnTRANSACTION_MD, {
                                    .tree_collapse_impl = TreeCollapseImpl::leafs_only
                                });
    auto map = SHAMap(tnTRANSACTION_MD, {
                          .tree_collapse_impl = TreeCollapseImpl::leafs_and_inners
                      });


    auto add_item = [
                &canonical_map,
                &map](boost::intrusive_ptr<MmapItem> &item) {
        map.add_item(item);
        canonical_map.add_item(item);
        const auto copy = canonical_map.snapshot();
        copy->collapse_tree();
        LOGI("Actual: ", map.trie_json_string({.key_as_hash = true}));
        LOGI("Canonical: ", copy->trie_json_string({.key_as_hash = true}));
    };
    Logger::set_level(LogLevel::INFO);
    add_item(i1);
    add_item(i2);

    // add_item(i3);
    // Logger::set_level(LogLevel::INFO);
    // add_item(i4);
}

TEST(CollapseTest, BasicNoSkips) {
    // Create a transaction-like tree (shallow)
    // Add a series of items that will create a specific structure
    TestItems items;

    auto i1 = items.make("0000000000000000000000000000000000000000000000000000000000010000");
    auto i2 = items.make("1000000000000000000000000000000000000000000000000000000000010100");
    auto i3 = items.make("0000000000000000000000000000000000000000000000000000000000020000");
    auto i4 = items.make("0000000000000000000000000000000000000000000000000000000000020001");

    auto canonical_map = SHAMap(tnTRANSACTION_MD, {
                                    .tree_collapse_impl = TreeCollapseImpl::leafs_only
                                });
    auto map = SHAMap(tnTRANSACTION_MD, {
                          .tree_collapse_impl = TreeCollapseImpl::leafs_and_inners
                      });

    auto add_item = [
                &canonical_map,
                &map](const std::string& name,boost::intrusive_ptr<MmapItem> &item) {
        map.add_item(item);
        canonical_map.add_item(item);
        LOGI("Adding item: ", name, item->key().hex());
        const auto copy = canonical_map.snapshot();
        copy->collapse_tree();
        auto actual = map.trie_json_string({.key_as_hash = true});
        auto canonical = copy->trie_json_string({.key_as_hash = true});
        LOGI("Actual: ", actual);
        LOGI("Canonical: ", canonical);
        EXPECT_EQ(actual, canonical);
    };

    Logger::set_level(LogLevel::INFO);
    add_item("i1", i1);
    add_item("i2", i2);
    add_item("i3", i3);
    add_item("i4", i4);

    // add_item(i3);
    // Logger::set_level(LogLevel::INFO);
    // add_item(i4);
}
