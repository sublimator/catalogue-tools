#include <gtest/gtest.h>
#include <iostream>
#include <utility>
#include "utils/test-utils.h"
#include "../src/shamap/src/pretty-print-json.h"
#include "catl/core/logger.h"

// Test for node collapsing behavior, particularly with shallow trees
TEST(ShaMapTest, CollapsePathWithSkips) {
    // Create a transaction-like tree (shallow)
    // Add a series of items that will create a specific structure
    TestItems items;

    auto i1 = items.make("0000000000000000000000000000000000000000000000000000000000010000");
    auto i2 = items.make("0000000000000000000000000000000000000000000000000000000000010100");
    auto i3 = items.make("0000000000500000000000000000000000000000000000000000000000010100");
    auto i4 = items.make("0000000000600000000000000000000000000000000000000000000000010100");

    auto dump_json = [](const SHAMap &map) {
        LOGD(map.trie_json_string({.key_as_hash = true}));
    };

    {
        auto do_collapse = true;
        auto map = SHAMap(tnTRANSACTION_MD, {
                              .tree_collapse_impl = do_collapse
                                                        ? TreeCollapseImpl::leafs_and_inners
                                                        : TreeCollapseImpl::leafs_only
                          });


        auto add_item = [&map, do_collapse, &dump_json](boost::intrusive_ptr<MmapItem> &item) {
            map.add_item(item);
            if (do_collapse) {
                dump_json(map);
            }
        };

        add_item(i1);
        add_item(i2);
        Logger::set_level(LogLevel::DEBUG);
        add_item(i3);
        Logger::set_level(LogLevel::INFO);
        add_item(i4);
    }
}
