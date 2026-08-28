#pragma once

#include "catl/shamap/shamap.h"

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <functional>
#include <stdexcept>
#include <utility>

namespace catl::shamap {

enum class OrderedDiffOp { added, modified, deleted };

struct OrderedDiffStats
{
    std::size_t added = 0;
    std::size_t modified = 0;
    std::size_t deleted = 0;
    std::size_t subtrees_skipped_by_identity = 0;
    std::size_t subtrees_skipped_by_hash = 0;
    bool complete = true;

    [[nodiscard]] std::size_t
    total() const
    {
        return added + modified + deleted;
    }
};

namespace detail {

inline bool
ordered_diff_items_equal(MmapItem const& left, MmapItem const& right)
{
    if (left.key() != right.key())
        return false;

    auto const left_data = left.slice();
    auto const right_data = right.slice();
    if (left_data.size() != right_data.size())
        return false;
    return left_data.empty() ||
        std::memcmp(left_data.data(), right_data.data(), left_data.size()) == 0;
}

}  // namespace detail

/**
 * Visit the logical leaf difference between two SHAMaps in strict key order.
 *
 * Equal Merkle subtrees are skipped. Differently collapsed physical trees are
 * aligned by projecting a deeper inner node onto its logical radix branch.
 * The visitor is called as:
 *
 *   bool(Key const&, OrderedDiffOp, MmapItem const* old_item,
 *        MmapItem const* new_item)
 *
 * Added records have only new_item, deleted records only old_item, and
 * modified records both. Returning false stops immediately and sets
 * OrderedDiffStats::complete to false. Aside from the bounded traversal
 * stack, the operation does not materialize keys or changes.
 *
 * Abbreviated trees are excluded: a placeholder proves a subtree hash but
 * does not contain the leaves required to emit a complete logical diff.
 */
template <typename Traits, typename Callback>
OrderedDiffStats
visit_differences_ordered(
    SHAMapT<Traits> const& before,
    SHAMapT<Traits> const& after,
    Callback&& callback)
    requires(!has_placeholders_v<Traits>)
{
    using TreeNode = SHAMapTreeNodeT<Traits>;
    using InnerNode = SHAMapInnerNodeT<Traits>;
    using LeafNode = SHAMapLeafNodeT<Traits>;
    using TreePtr = boost::intrusive_ptr<TreeNode>;
    using InnerPtr = boost::intrusive_ptr<InnerNode>;

    auto before_root = before.get_root();
    auto after_root = after.get_root();
    if (!before_root || !after_root)
        throw std::logic_error("Cannot diff SHAMaps with null roots");

    // Ensure every subtree hash used as a skip oracle has been committed.
    (void)before.get_hash();
    (void)after.get_hash();

    auto const& before_options = before.get_options();
    auto const& after_options = after.get_options();
    OrderedDiffStats stats;

    auto emit = [&](OrderedDiffOp operation,
                    MmapItem const* old_item,
                    MmapItem const* new_item) -> bool {
        auto const* identity = old_item ? old_item : new_item;
        if (!identity)
            throw std::logic_error("Ordered SHAMap diff emitted no identity");

        switch (operation)
        {
            case OrderedDiffOp::added:
                ++stats.added;
                break;
            case OrderedDiffOp::modified:
                ++stats.modified;
                break;
            case OrderedDiffOp::deleted:
                ++stats.deleted;
                break;
        }

        if (!std::invoke(
                callback, identity->key(), operation, old_item, new_item))
        {
            stats.complete = false;
            return false;
        }
        return true;
    };

    auto walk_leaves = [&](TreePtr const& root, auto&& visitor) -> bool {
        auto walk = [&](auto&& self, TreePtr const& node) -> bool {
            if (!node)
                return true;
            if (node->is_leaf())
            {
                auto leaf = boost::static_pointer_cast<LeafNode>(node);
                auto item = leaf->get_item();
                if (!item)
                    throw std::logic_error(
                        "Ordered SHAMap diff found a leaf without an item");
                return visitor(*item);
            }
            if (!node->is_inner())
                throw std::logic_error(
                    "Ordered SHAMap diff found an unknown node kind");

            auto inner = boost::static_pointer_cast<InnerNode>(node);
            for (int branch = 0; branch < 16; ++branch)
            {
                if (!self(self, inner->get_child(branch)))
                    return false;
            }
            return true;
        };
        return walk(walk, root);
    };

    auto first_item = [&](InnerPtr const& root) -> MmapItem const& {
        TreePtr node = root;
        while (node && node->is_inner())
        {
            auto inner = boost::static_pointer_cast<InnerNode>(node);
            node.reset();
            for (int branch = 0; branch < 16; ++branch)
            {
                if (auto child = inner->get_child(branch))
                {
                    node = std::move(child);
                    break;
                }
            }
        }
        if (!node || !node->is_leaf())
            throw std::logic_error(
                "Ordered SHAMap diff found an empty projected inner");
        auto leaf = boost::static_pointer_cast<LeafNode>(node);
        auto item = leaf->get_item();
        if (!item)
            throw std::logic_error(
                "Ordered SHAMap diff found a leaf without an item");
        return *item;
    };

    auto project_branch = [&](InnerPtr const& node,
                              int target_depth,
                              int branch,
                              int projected_branch) -> TreePtr {
        auto const node_depth = node->get_depth_int();
        if (node_depth < target_depth)
            throw std::logic_error(
                "Ordered SHAMap diff projected above an inner's depth");
        if (node_depth == target_depth)
            return node->get_child(branch);

        auto const selected = projected_branch >= 0
            ? projected_branch
            : select_branch(first_item(node).key(), target_depth);
        if (selected != branch)
            return {};
        return TreePtr(node);
    };

    auto equal_subtree = [&](TreePtr const& left, TreePtr const& right) {
        if (left && left.get() == right.get())
        {
            ++stats.subtrees_skipped_by_identity;
            return true;
        }
        if (!left || !right || left->is_leaf() != right->is_leaf() ||
            left->is_inner() != right->is_inner())
            return false;
        if (left->is_inner())
        {
            auto left_inner = boost::static_pointer_cast<InnerNode>(left);
            auto right_inner = boost::static_pointer_cast<InnerNode>(right);
            if (left_inner->get_depth() != right_inner->get_depth())
                return false;
        }
        if (left->get_hash(before_options) == right->get_hash(after_options))
        {
            ++stats.subtrees_skipped_by_hash;
            return true;
        }
        return false;
    };

    auto merge_old_leaf_new_subtree = [&](MmapItem const& old_item,
                                          InnerPtr const& new_inner) -> bool {
        bool old_consumed = false;
        auto const ok = walk_leaves(
            TreePtr(new_inner), [&](MmapItem const& new_item) -> bool {
                if (!old_consumed && old_item.key() < new_item.key())
                {
                    if (!emit(OrderedDiffOp::deleted, &old_item, nullptr))
                        return false;
                    old_consumed = true;
                }

                if (!old_consumed && old_item.key() == new_item.key())
                {
                    old_consumed = true;
                    if (!detail::ordered_diff_items_equal(old_item, new_item))
                        return emit(
                            OrderedDiffOp::modified, &old_item, &new_item);
                    return true;
                }
                return emit(OrderedDiffOp::added, nullptr, &new_item);
            });
        if (!ok)
            return false;
        return old_consumed || emit(OrderedDiffOp::deleted, &old_item, nullptr);
    };

    auto merge_old_subtree_new_leaf = [&](InnerPtr const& old_inner,
                                          MmapItem const& new_item) -> bool {
        bool new_consumed = false;
        auto const ok = walk_leaves(
            TreePtr(old_inner), [&](MmapItem const& old_item) -> bool {
                if (!new_consumed && new_item.key() < old_item.key())
                {
                    if (!emit(OrderedDiffOp::added, nullptr, &new_item))
                        return false;
                    new_consumed = true;
                }

                if (!new_consumed && new_item.key() == old_item.key())
                {
                    new_consumed = true;
                    if (!detail::ordered_diff_items_equal(old_item, new_item))
                        return emit(
                            OrderedDiffOp::modified, &old_item, &new_item);
                    return true;
                }
                return emit(OrderedDiffOp::deleted, &old_item, nullptr);
            });
        if (!ok)
            return false;
        return new_consumed || emit(OrderedDiffOp::added, nullptr, &new_item);
    };

    auto compare =
        [&](auto&& self, InnerPtr const& left, InnerPtr const& right) -> bool {
        if (equal_subtree(TreePtr(left), TreePtr(right)))
            return true;

        auto const left_depth = left->get_depth_int();
        auto const right_depth = right->get_depth_int();
        auto const aligned_depth = std::min(left_depth, right_depth);
        auto const left_projection = left_depth > aligned_depth
            ? select_branch(first_item(left).key(), aligned_depth)
            : -1;
        auto const right_projection = right_depth > aligned_depth
            ? select_branch(first_item(right).key(), aligned_depth)
            : -1;

        for (int branch = 0; branch < 16; ++branch)
        {
            auto old_node =
                project_branch(left, aligned_depth, branch, left_projection);
            auto new_node =
                project_branch(right, aligned_depth, branch, right_projection);

            if (!old_node && !new_node)
                continue;
            if (equal_subtree(old_node, new_node))
                continue;

            if (!old_node)
            {
                if (!walk_leaves(new_node, [&](MmapItem const& item) {
                        return emit(OrderedDiffOp::added, nullptr, &item);
                    }))
                    return false;
                continue;
            }
            if (!new_node)
            {
                if (!walk_leaves(old_node, [&](MmapItem const& item) {
                        return emit(OrderedDiffOp::deleted, &item, nullptr);
                    }))
                    return false;
                continue;
            }

            if (old_node->is_leaf() && new_node->is_leaf())
            {
                auto old_leaf = boost::static_pointer_cast<LeafNode>(old_node);
                auto new_leaf = boost::static_pointer_cast<LeafNode>(new_node);
                auto old_item = old_leaf->get_item();
                auto new_item = new_leaf->get_item();
                if (!old_item || !new_item)
                    throw std::logic_error(
                        "Ordered SHAMap diff found a leaf without an item");
                if (detail::ordered_diff_items_equal(*old_item, *new_item))
                    continue;
                if (old_item->key() == new_item->key())
                {
                    if (!emit(
                            OrderedDiffOp::modified,
                            old_item.get(),
                            new_item.get()))
                        return false;
                }
                else if (old_item->key() < new_item->key())
                {
                    if (!emit(
                            OrderedDiffOp::deleted, old_item.get(), nullptr) ||
                        !emit(OrderedDiffOp::added, nullptr, new_item.get()))
                        return false;
                }
                else if (
                    !emit(OrderedDiffOp::added, nullptr, new_item.get()) ||
                    !emit(OrderedDiffOp::deleted, old_item.get(), nullptr))
                {
                    return false;
                }
                continue;
            }

            if (old_node->is_leaf() && new_node->is_inner())
            {
                auto old_leaf = boost::static_pointer_cast<LeafNode>(old_node);
                auto new_inner =
                    boost::static_pointer_cast<InnerNode>(new_node);
                auto old_item = old_leaf->get_item();
                if (!old_item ||
                    !merge_old_leaf_new_subtree(*old_item, new_inner))
                    return false;
                continue;
            }

            if (old_node->is_inner() && new_node->is_leaf())
            {
                auto old_inner =
                    boost::static_pointer_cast<InnerNode>(old_node);
                auto new_leaf = boost::static_pointer_cast<LeafNode>(new_node);
                auto new_item = new_leaf->get_item();
                if (!new_item ||
                    !merge_old_subtree_new_leaf(old_inner, *new_item))
                    return false;
                continue;
            }

            if (!old_node->is_inner() || !new_node->is_inner())
                throw std::logic_error(
                    "Ordered SHAMap diff found an unknown node kind");
            if (!self(
                    self,
                    boost::static_pointer_cast<InnerNode>(old_node),
                    boost::static_pointer_cast<InnerNode>(new_node)))
                return false;
        }
        return true;
    };

    (void)compare(compare, before_root, after_root);
    return stats;
}

}  // namespace catl::shamap
