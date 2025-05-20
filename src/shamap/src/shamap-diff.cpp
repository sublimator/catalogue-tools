#include "catl/shamap/shamap-diff.h"
#include "catl/core/logger.h"
#include "catl/shamap/shamap-innernode.h"
#include "catl/shamap/shamap-leafnode.h"
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <memory>
#include <stdexcept>

namespace catl::shamap {
SHAMapDiff::SHAMapDiff(std::shared_ptr<SHAMap> one, std::shared_ptr<SHAMap> two)
    : map_one(std::move(one)), map_two(std::move(two))
{
}

SHAMapDiff&
SHAMapDiff::find()
{
    // Force hash calculation for both maps to ensure we're comparing fully
    // built trees
    map_one->get_hash();
    map_two->get_hash();

    // Start comparison from the root nodes
    auto root_one = map_one->get_root();
    auto root_two = map_two->get_root();

    if (!root_one || !root_two)
    {
        throw std::runtime_error("Cannot diff maps with null roots");
    }

    compare_inner(root_one, root_two);
    return *this;
}

std::unique_ptr<SHAMapDiff>
SHAMapDiff::inverted() const
{
    auto diff = std::make_unique<SHAMapDiff>(map_two, map_one);

    // Invert the change sets
    diff->added_items = deleted_items;
    diff->modified_items = modified_items;
    diff->deleted_items = added_items;

    return diff;
}

// Helper to create a copy of an MmapItem, since we can't use copy constructor
static boost::intrusive_ptr<MmapItem>
make_item_copy(const boost::intrusive_ptr<MmapItem>& original)
{
    return original;
    // // Create a new item with the same key and data
    // const Key& key = original->key();
    // auto data_slice = original->slice();
    //
    // return boost::intrusive_ptr<MmapItem>(
    //     new MmapItem(key.data(), data_slice.data(), data_slice.size()));
}

void
SHAMapDiff::apply(SHAMap& target) const
{
    // Apply modifications first
    for (const auto& key : modified_items)
    {
        auto item = map_two->get_item(key);
        if (!item)
        {
            throw std::runtime_error("Modified item not found in source map");
        }

        auto item_copy = make_item_copy(item);
        if (target.update_item(item_copy) == SetResult::FAILED)
        {
            throw std::runtime_error("Failed to update item in target map");
        }
    }

    // Apply additions
    for (const auto& key : added_items)
    {
        auto item = map_two->get_item(key);
        if (!item)
        {
            throw std::runtime_error("Added item not found in source map");
        }

        auto item_copy = make_item_copy(item);
        if (target.add_item(item_copy) == SetResult::FAILED)
        {
            throw std::runtime_error("Failed to add item to target map");
        }
    }

    // Apply deletions
    for (const auto& key : deleted_items)
    {
        if (!target.remove_item(key))
        {
            throw std::runtime_error("Failed to remove item from target map");
        }
    }
}

// Helper to compare item data without using hash() method
static bool
items_equal(
    const boost::intrusive_ptr<MmapItem>& a,
    const boost::intrusive_ptr<MmapItem>& b)
{
    // Compare key first (quick check)
    if (a->key() != b->key())
    {
        return false;
    }

    // Compare data slices
    const auto& slice_a = a->slice();
    const auto& slice_b = b->slice();

    // Different sizes means different content
    if (slice_a.size() != slice_b.size())
    {
        return false;
    }

    // Compare byte by byte
    return std::memcmp(slice_a.data(), slice_b.data(), slice_a.size()) == 0;
}

void
SHAMapDiff::compare_inner(
    const boost::intrusive_ptr<SHAMapInnerNode>& a,
    const boost::intrusive_ptr<SHAMapInnerNode>& b)
{
    // Get options from the maps for hash calculation
    const SHAMapOptions& options_a = map_one->get_options();
    const SHAMapOptions& options_b = map_two->get_options();

    for (int i = 0; i < 16; i++)
    {
        auto a_child = a->get_child(i);
        auto b_child = b->get_child(i);

        if (!a_child && b_child)
        {
            // Added in B (map_two)
            track_added(b_child);
        }
        else if (a_child && !b_child)
        {
            // Removed from B (map_two)
            track_removed(a_child);
        }
        else if (
            a_child && b_child &&
            a_child->get_hash(options_a) != b_child->get_hash(options_b))
        {
            // Both nodes exist but hashes differ
            bool a_is_leaf = a_child->is_leaf();
            bool b_is_leaf = b_child->is_leaf();

            if (a_is_leaf && b_is_leaf)
            {
                // Both are leaves with different hashes
                auto leaf_a =
                    boost::static_pointer_cast<SHAMapLeafNode>(a_child);
                auto leaf_b =
                    boost::static_pointer_cast<SHAMapLeafNode>(b_child);

                if (leaf_a->get_item()->key() == leaf_b->get_item()->key())
                {
                    // Same key but different content - modified item
                    modified_items.insert(leaf_a->get_item()->key());
                }
                else
                {
                    // Different keys - one deleted and one added
                    deleted_items.insert(leaf_a->get_item()->key());
                    added_items.insert(leaf_b->get_item()->key());
                }
            }
            else if (a_is_leaf && !b_is_leaf)
            {
                // A is leaf, B is inner - the leaf was replaced with a subtree
                auto leaf_a =
                    boost::static_pointer_cast<SHAMapLeafNode>(a_child);
                auto inner_b =
                    boost::static_pointer_cast<SHAMapInnerNode>(b_child);

                // Add all items from the new subtree
                track_added(inner_b);

                // Special handling: if the subtree contains the original leaf's
                // key, we should consider it as modified, not added
                const Key& leaf_key = leaf_a->get_item()->key();
                auto item = map_two->get_item(leaf_key);

                if (item)
                {
                    // Remove from added since track_added would have added it
                    added_items.erase(leaf_key);

                    if (!items_equal(item, leaf_a->get_item()))
                    {
                        // Content differs, so it's modified
                        modified_items.insert(leaf_key);
                    }
                }
                else
                {
                    // The item is genuinely deleted
                    deleted_items.insert(leaf_key);
                }
            }
            else if (!a_is_leaf && b_is_leaf)
            {
                // A is inner, B is leaf - a subtree was replaced with a leaf
                auto inner_a =
                    boost::static_pointer_cast<SHAMapInnerNode>(a_child);
                auto leaf_b =
                    boost::static_pointer_cast<SHAMapLeafNode>(b_child);

                // Remove all items from the old subtree
                track_removed(inner_a);

                // Special handling: if the old subtree contained the new leaf's
                // key, we should consider it as modified, not deleted
                const Key& leaf_key = leaf_b->get_item()->key();
                auto item = map_one->get_item(leaf_key);

                if (item)
                {
                    // Remove from deleted since track_removed would have added
                    // it
                    deleted_items.erase(leaf_key);

                    if (!items_equal(item, leaf_b->get_item()))
                    {
                        // Content differs, so it's modified
                        modified_items.insert(leaf_key);
                    }
                }
                else
                {
                    // The item is genuinely added
                    added_items.insert(leaf_key);
                }
            }
            else
            {
                // Both are inner nodes with different hashes, recursively
                // compare
                auto inner_a =
                    boost::static_pointer_cast<SHAMapInnerNode>(a_child);
                auto inner_b =
                    boost::static_pointer_cast<SHAMapInnerNode>(b_child);
                compare_inner(inner_a, inner_b);
            }
        }
    }
}

void
SHAMapDiff::track_removed(const boost::intrusive_ptr<SHAMapTreeNode>& node)
{
    if (node->is_leaf())
    {
        auto leaf = boost::static_pointer_cast<SHAMapLeafNode>(node);
        deleted_items.insert(leaf->get_item()->key());
    }
    else if (node->is_inner())
    {
        auto inner = boost::static_pointer_cast<SHAMapInnerNode>(node);

        // Recursively process all children
        for (int i = 0; i < 16; i++)
        {
            auto child = inner->get_child(i);
            if (child)
            {
                track_removed(child);
            }
        }
    }
}

void
SHAMapDiff::track_added(const boost::intrusive_ptr<SHAMapTreeNode>& node)
{
    if (node->is_leaf())
    {
        auto leaf = boost::static_pointer_cast<SHAMapLeafNode>(node);
        added_items.insert(leaf->get_item()->key());
    }
    else if (node->is_inner())
    {
        auto inner = boost::static_pointer_cast<SHAMapInnerNode>(node);

        // Recursively process all children
        for (int i = 0; i < 16; i++)
        {
            auto child = inner->get_child(i);
            if (child)
            {
                track_added(child);
            }
        }
    }
}
}  // namespace catl::shamap