#pragma once

#include "hybrid-shamap.h"
#include <algorithm>
#include <coroutine>
#include <queue>
#include <stack>
#include <utility>
#include <vector>

#include "catl/core/types.h"

namespace catl::hybrid_shamap {

/**
 * Simple generator implementation since std::generator isn't available yet
 */
template <typename T>
class generator
{
public:
    struct promise_type
    {
        T current_value;

        auto
        initial_suspend()
        {
            return std::suspend_always{};
        }
        auto
        final_suspend() noexcept
        {
            return std::suspend_always{};
        }

        generator
        get_return_object()
        {
            return generator{
                std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        void
        unhandled_exception()
        {
            std::terminate();
        }
        void
        return_void()
        {
        }

        auto
        yield_value(T&& value)
        {
            current_value = std::forward<T>(value);
            return std::suspend_always{};
        }

        auto
        yield_value(const T& value)
        {
            current_value = value;
            return std::suspend_always{};
        }
    };

    struct iterator
    {
        std::coroutine_handle<promise_type> handle;

        iterator(std::coroutine_handle<promise_type> h, bool done) : handle(h)
        {
            if (!done && handle)
            {
                handle.resume();
            }
        }

        iterator&
        operator++()
        {
            if (handle && !handle.done())
            {
                handle.resume();
            }
            return *this;
        }

        bool
        operator!=(const iterator& other) const
        {
            // When comparing to end iterator (which has nullptr handle),
            // we're done if our handle is done
            if (!other.handle)
            {
                return handle && !handle.done();
            }
            return handle != other.handle;
        }

        const T&
        operator*() const
        {
            return handle.promise().current_value;
        }
    };

private:
    std::coroutine_handle<promise_type> handle;

public:
    explicit generator(std::coroutine_handle<promise_type> h) : handle(h)
    {
    }

    generator(generator&& other) noexcept
        : handle(std::exchange(other.handle, {}))
    {
    }

    ~generator()
    {
        if (handle)
            handle.destroy();
    }

    generator(const generator&) = delete;
    generator&
    operator=(const generator&) = delete;
    generator&
    operator=(generator&& other) noexcept
    {
        if (this != &other)
        {
            if (handle)
                handle.destroy();
            handle = std::exchange(other.handle, {});
        }
        return *this;
    }

    iterator
    begin()
    {
        return iterator{handle, false};
    }

    iterator
    end()
    {
        return iterator{nullptr, true};
    }
};

/**
 * Node visitor info - what we yield for each node during traversal
 */
struct NodeVisit
{
    PolyNodePtr node;
    int depth;
    int branch;          // Branch taken from parent (-1 for root)
    PolyNodePtr parent;  // Parent node (empty for root)
    int parent_depth;    // Actual depth of parent (-1 for root)

    // Convenience accessors
    [[nodiscard]] bool
    is_leaf() const
    {
        return node.is_leaf();
    }
    [[nodiscard]] bool
    is_inner() const
    {
        return node.is_inner();
    }
    [[nodiscard]] bool
    is_mmap() const
    {
        return node.is_raw_memory();
    }
    [[nodiscard]] bool
    is_materialized() const
    {
        return node.is_materialized();
    }

    [[nodiscard]] Hash256
    get_hash() const
    {
        return node.get_hash();
    }

    [[nodiscard]] bool
    has_depth_skip() const
    {
        return parent_depth >= 0 && (depth - parent_depth) > 1;
    }

    [[nodiscard]] int
    depth_skip_amount() const
    {
        if (parent_depth < 0)
            return 0;
        return std::max(0, (depth - parent_depth) - 1);
    }
};

/**
 * Depth-first tree walker using C++23 generators
 * Yields each node in the tree exactly once
 */
class TreeWalker
{
public:
    /**
     * Walk tree depth-first (pre-order)
     * Yields parent before children
     */
    static generator<NodeVisit>
    walk_depth_first(const PolyNodePtr& root)
    {
        struct StackItem
        {
            PolyNodePtr node;
            int depth;
            int branch;
            PolyNodePtr parent;
            int parent_depth;
        };

        std::stack<StackItem> stack;
        stack.push({root, 0, -1, PolyNodePtr::make_empty(), -1});

        while (!stack.empty())
        {
            auto [node, depth, branch, parent, parent_depth] = stack.top();
            stack.pop();

            if (node.is_empty())
                continue;

            // Yield current node
            co_yield NodeVisit{node, depth, branch, parent, parent_depth};

            // If it's an inner node, push children onto stack (in reverse order
            // for correct traversal)
            if (node.is_inner())
            {
                if (node.is_materialized())
                {
                    // Materialized inner node

                    auto* inner = static_cast<
                        HmapInnerNode*>(  // NOLINT(*-pro-type-static-cast-downcast)
                        node.get_materialized_base());
                    for (int i = 15; i >= 0; --i)
                    {
                        auto child = inner->get_child(i);
                        if (!child.is_empty())
                        {
                            // Get actual depth from child if it's an inner node
                            int child_depth = depth + 1;  // Default
                            if (child.is_inner() && child.is_materialized())
                            {
                                child_depth = child.get_materialized<HmapInnerNode>()->get_depth();
                            }
                            stack.push({child, child_depth, i, node, depth});
                        }
                    }
                }
                else
                {
                    // Mmap inner node
                    const uint8_t* raw = node.get_raw_memory();
                    InnerNodeView view = MemTreeOps::get_inner_node(raw);

                    for (int i = 15; i >= 0; --i)
                    {
                        auto child_type = view.get_child_type(i);
                        if (child_type != catl::v2::ChildType::EMPTY)
                        {
                            const uint8_t* child_ptr = view.get_child_ptr(i);
                            PolyNodePtr child = PolyNodePtr::make_raw_memory(
                                child_ptr, child_type);
                            // Get actual depth from child if it's an inner node
                            int child_depth = depth + 1;  // Default for leaves
                            if (child_type == catl::v2::ChildType::INNER)
                            {
                                InnerNodeView child_view =
                                    MemTreeOps::get_inner_node(child_ptr);
                                child_depth =
                                    child_view.header_ptr->get_depth();
                            }
                            stack.push({child, child_depth, i, node, depth});
                        }
                    }
                }
            }
        }
    }

    /**
     * Walk tree breadth-first (level-order)
     * Yields all nodes at depth N before any at depth N+1
     */
    static generator<NodeVisit>
    walk_breadth_first(const PolyNodePtr& root)
    {
        struct QueueItem
        {
            PolyNodePtr node;
            int depth;
            int branch;
            PolyNodePtr parent;
            int parent_depth;
        };

        std::queue<QueueItem> queue;
        queue.push({root, 0, -1, PolyNodePtr::make_empty(), -1});

        while (!queue.empty())
        {
            auto [node, depth, branch, parent, parent_depth] = queue.front();
            queue.pop();

            if (node.is_empty())
                continue;

            // Yield current node
            co_yield NodeVisit{node, depth, branch, parent, parent_depth};

            // If it's an inner node, enqueue children
            if (node.is_inner())
            {
                if (node.is_materialized())
                {
                    // Materialized inner node
                    auto* inner = node.get_materialized<HmapInnerNode>();
                    for (int i = 0; i < 16; ++i)
                    {
                        auto child = inner->get_child(i);
                        if (!child.is_empty())
                        {
                            queue.push({child, depth + 1, i, node, depth});
                        }
                    }
                }
                else
                {
                    // Mmap inner node
                    const uint8_t* raw = node.get_raw_memory();
                    InnerNodeView view = MemTreeOps::get_inner_node(raw);

                    for (int i = 0; i < 16; ++i)
                    {
                        auto child_type = view.get_child_type(i);
                        if (child_type != catl::v2::ChildType::EMPTY)
                        {
                            const uint8_t* child_ptr = view.get_child_ptr(i);
                            PolyNodePtr child = PolyNodePtr::make_raw_memory(
                                child_ptr, child_type);
                            queue.push({child, depth + 1, i, node, depth});
                        }
                    }
                }
            }
        }
    }

    /**
     * Walk only leaf nodes
     * Useful for iterating through all key-value pairs
     */
    static generator<NodeVisit>
    walk_leaves_only(const PolyNodePtr& root)
    {
        for (auto&& visit : walk_depth_first(root))
        {
            if (visit.is_leaf())
            {
                co_yield visit;
            }
        }
    }

    /**
     * Walk with filtering predicate
     * Only yields nodes that match the predicate
     */
    template <typename Predicate>
    static generator<NodeVisit>
    walk_filtered(const PolyNodePtr& root, Predicate pred)
    {
        for (auto&& visit : walk_depth_first(root))
        {
            if (pred(visit))
            {
                co_yield visit;
            }
        }
    }

    /**
     * Count nodes matching a predicate
     * Example: count_if(root, [](auto& v) { return v.is_mmap(); })
     */
    template <typename Predicate>
    static size_t
    count_if(const PolyNodePtr& root, Predicate pred)
    {
        size_t count = 0;
        for (auto&& visit : walk_depth_first(root))
        {
            if (pred(visit))
            {
                ++count;
            }
        }
        return count;
    }

    /**
     * Collect all nodes matching a predicate into a vector
     */
    template <typename Predicate>
    static std::vector<NodeVisit>
    collect_if(const PolyNodePtr& root, Predicate pred)
    {
        std::vector<NodeVisit> result;
        for (auto&& visit : walk_depth_first(root))
        {
            if (pred(visit))
            {
                result.push_back(visit);
            }
        }
        return result;
    }
};

}  // namespace catl::hybrid_shamap