#include "catl/shamap/shamap.h"

#include "catl/core/types.h"
#include "catl/shamap/shamap-errors.h"
#include "catl/shamap/shamap-innernode.h"
#include "catl/shamap/shamap-leafnode.h"
#include "catl/shamap/shamap-nodetype.h"
#include "catl/shamap/shamap-options.h"
#include "catl/shamap/shamap-pathfinder.h"
#include "catl/shamap/shamap-treenode.h"
#include <atomic>
#include <boost/json/array.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>

#include "catl/core/logger.h"
#include "pretty-print-json.h"
#include <functional>
#include <memory>
#include <ostream>
#include <sstream>
#include <stack>
#include <string>
#include <utility>

namespace catl::shamap {

// Global partition for destructor tracking (not static, so it's visible across
// compilation units)
LogPartition destructor_log("DESTRUCTOR", LogLevel::NONE);  // Start disabled

// LogPartition for walk_new_nodes tracking - disabled by default
// Enable with: catl::shamap::walk_nodes_log.enable(LogLevel::DEBUG)
LogPartition walk_nodes_log("WALK_NODES", LogLevel::NONE);

//----------------------------------------------------------
// SHAMapT Implementation
//----------------------------------------------------------

template <typename Traits>
SHAMapT<Traits>::SHAMapT(SHAMapNodeType type, SHAMapOptions options)
    : node_type_(type)
    , options_(options)
    , version_counter_(std::make_shared<std::atomic<int>>(0))
    , current_version_(0)
    , cow_enabled_(false)
{
    root = boost::intrusive_ptr(
        new SHAMapInnerNodeT<Traits>(0));  // Root has depth 0
    OLOGD("SHAMap created with type: ", static_cast<int>(type));
}

template <typename Traits>
SHAMapT<Traits>::SHAMapT(
    const SHAMapNodeType type,
    boost::intrusive_ptr<SHAMapInnerNodeT<Traits>> rootNode,
    std::shared_ptr<std::atomic<int>> vCounter,
    const int version,
    SHAMapOptions options)
    : root(std::move(rootNode))
    , node_type_(type)
    , options_(options)
    , version_counter_(std::move(vCounter))
    , current_version_(version)
    , cow_enabled_(true)
    , needs_version_bump_on_write_(false)  // Snapshots don't need lazy bump
{
    OLOGD("Created SHAMap snapshot with version ", version);
}

template <typename Traits>
SHAMapT<Traits>::~SHAMapT()
{
    PLOGD(
        destructor_log,
        "~SHAMapT: type=",
        static_cast<int>(node_type_),
        ", version=",
        current_version_,
        ", cow=",
        cow_enabled_,
        ", root=",
        (root ? "yes" : "no"),
        ", root.refcount=",
        (root ? root->ref_count_.load() : 0));
    // root intrusive_ptr will be destroyed here, should decrement refcount
}

template <typename Traits>
void
SHAMapT<Traits>::enable_cow()
{
    if (cow_enabled_)
    {
        OLOGD("Copy-on-Write already enabled");
        return;
    }

    cow_enabled_ = true;

    // Generate a new version if we don't have one
    if (current_version_ == 0)
    {
        new_version(true);
    }

    // Update root node if it exists
    if (root)
    {
        // IMPORTANT: If the root was created without CoW, we need to copy it
        // to avoid shared state corruption
        if (!root->is_cow_enabled())
        {
            OLOGD("Root node created without CoW, creating a copy");
            root = root->copy(current_version_, nullptr);
        }

        root->enable_cow(true);

        // Set version if it's 0
        if (root->get_version() == 0)
        {
            root->set_version(current_version_);
        }
    }

    OLOGD("Copy-on-Write enabled for SHAMap with version ", current_version_);
}

template <typename Traits>
int
SHAMapT<Traits>::new_version(bool in_place)
{
    if (!version_counter_)
    {
        version_counter_ = std::make_shared<std::atomic<int>>(0);
    }
    // Increment shared counter and update current version
    int new_ver = ++(*version_counter_);

    if (in_place)
    {
        current_version_ = new_ver;
    }

    OLOGD("Generated new SHAMap version: ", new_ver);
    return new_ver;
}

template <typename Traits>
std::shared_ptr<SHAMapT<Traits>>
SHAMapT<Traits>::snapshot()
{
    if (!root)
    {
        OLOGW("Attempted to snapshot a SHAMap with null root.");
        return nullptr;
    }

    // Enable CoW if not already enabled
    if (!cow_enabled_)
    {
        enable_cow();
    }

    // ONLY create new version for the snapshot, not the original
    // The original will lazily bump its version on first write
    int snapshot_version = new_version(false);  // Don't update current_version_

    // Mark that original needs version bump on next write
    needs_version_bump_on_write_ = true;

    OLOGD(
        "Creating snapshot: original staying at version ",
        current_version_,
        ", snapshot version ",
        snapshot_version);

    // Create a new SHAMap that shares the same root and version counter
    auto copy = std::make_shared<SHAMapT<Traits>>(SHAMapT<Traits>(
        node_type_,
        root->copy(snapshot_version, nullptr),
        version_counter_,
        snapshot_version,
        options_));

    return copy;
}

namespace json = boost::json;

template <typename Traits>
void
SHAMapT<Traits>::trie_json(std::ostream& os, TrieJsonOptions options) const
{
    auto trie = root->trie_json(options, options_);
    if (options.pretty)
        pretty_print_json(os, trie);
    else
    {
        os << trie;
    }
}

template <typename Traits>
std::string
SHAMapT<Traits>::trie_json_string(TrieJsonOptions options) const
{
    std::ostringstream oss;
    trie_json(oss, options);
    return oss.str();
}

template <typename Traits>
void
SHAMapT<Traits>::visit_items(
    const std::function<void(const MmapItem&)>& visitor) const
{
    if (!visitor)
    {
        OLOGW("visit_items called with an invalid visitor function.");
        return;
    }
    if (!root)
    {
        // Empty tree, nothing to visit
        return;
    }

    // Stack holds const pointers to nodes yet to be processed
    std::stack<boost::intrusive_ptr<const SHAMapTreeNodeT<Traits>>> node_stack;

    // Start traversal from the root node
    node_stack.push(
        boost::static_pointer_cast<const SHAMapTreeNodeT<Traits>>(root));

    while (!node_stack.empty())
    {
        // Get the next node from the stack (LIFO -> depth-first)
        boost::intrusive_ptr<const SHAMapTreeNodeT<Traits>> current_node =
            node_stack.top();
        node_stack.pop();

        // Safety check
        if (!current_node)
            continue;

        // Process based on node type
        if (current_node->is_leaf())
        {
            auto leaf =
                boost::dynamic_pointer_cast<const SHAMapLeafNodeT<Traits>>(
                    current_node);
            if (leaf)
            {
                auto item_ptr = leaf->get_item();
                if (item_ptr)
                {
                    visitor(*item_ptr);  // Execute visitor for the leaf item
                }
                else
                {
                    OLOGW(
                        "Encountered leaf node with null MmapItem during "
                        "visit_items traversal.");
                }
            }
            else
            {
                OLOGE(
                    "Failed to cast node to const SHAMapLeafNodeT despite "
                    "is_leaf() being true.");
            }
        }
        else if (current_node->is_inner())
        {
            auto inner =
                boost::dynamic_pointer_cast<const SHAMapInnerNodeT<Traits>>(
                    current_node);
            if (inner)
            {
                // Push children onto the stack to visit them next.
                // Pushing 0..15 results in processing 15..0 (right-to-left DFS)
                for (int i = 0; i < 16; ++i)
                {
                    boost::intrusive_ptr<const SHAMapTreeNodeT<Traits>> child =
                        inner->get_child(i);
                    if (child)
                    {
                        // Only push valid children
                        node_stack.push(child);
                    }
                }
            }
            else
            {
                OLOGE(
                    "Failed to cast node to const SHAMapInnerNodeT despite "
                    "is_inner() being true.");
            }
        }
        // else: Unknown node type? Log error or ignore.
    }  // End while stack not empty
}

template <typename Traits>
void
SHAMapT<Traits>::invalidate_hash_recursive()
{
    root->invalidate_hash_recursive();
}

template <typename Traits>
boost::json::array
SHAMapT<Traits>::items_json() const
{
    boost::json::array items_array;  // Create the array to be populated

    // Define the visitor lambda that builds the JSON object for each item
    auto json_builder = [&items_array](const MmapItem& item) {
        // Get key hex using Key::hex()
        std::string key_hex = item.key().hex();

        // Get data hex using the existing slice_hex utility
        std::string data_hex;
        slice_hex(item.slice(), data_hex);  // Assumes slice_hex is available

        // Add the object to the array captured by the lambda
        items_array.emplace_back(
            boost::json::object{{"key", key_hex}, {"data", data_hex}});
    };

    // Call the general (now iterative) visitor function, passing the lambda
    this->visit_items(json_builder);

    return items_array;  // Return the populated array
}

template <typename Traits>
SetResult
SHAMapT<Traits>::add_item(boost::intrusive_ptr<MmapItem>& item)
{
    return set_item(item, SetMode::ADD_ONLY);
}

template <typename Traits>
SetResult
SHAMapT<Traits>::update_item(boost::intrusive_ptr<MmapItem>& item)
{
    return set_item(item, SetMode::UPDATE_ONLY);
}

template <typename Traits>
SetResult
SHAMapT<Traits>::set_item(boost::intrusive_ptr<MmapItem>& item, SetMode mode)
{
    if (options_.tree_collapse_impl == TreeCollapseImpl::leafs_and_inners)
    {
        return set_item_collapsed(item, mode);
    }
    else
    {
        return set_item_reference(item, mode);
    }
}

template <typename Traits>
bool
SHAMapT<Traits>::remove_item(const Key& key)
{
    return remove_item_reference(key);
}

template <typename Traits>
bool
SHAMapT<Traits>::has_item(const Key& key) const
{
    return get_item(key) != nullptr;
}

template <typename Traits>
Hash256
SHAMapT<Traits>::get_hash() const
{
    if (!root)
    {
        OLOGW("Attempting to get hash of a null root SHAMap.");
        return Hash256::zero();
    }
    return root->get_hash(options_);
}

template <typename Traits>
std::function<void()>
SHAMapT<Traits>::get_hash_job(int threadId, int totalThreads) const
{
    // Validate parameters
    if (threadId < 0 || threadId >= totalThreads)
    {
        throw std::invalid_argument(
            "Invalid threadId: must be 0 to totalThreads-1");
    }

    // Check totalThreads is power of 2
    if (totalThreads <= 0 || (totalThreads & (totalThreads - 1)) != 0 ||
        totalThreads > 16)
    {
        throw std::invalid_argument(
            "totalThreads must be power of 2 (1, 2, 4, 8, or 16)");
    }

    if (!root)
    {
        // No work to do for empty tree
        return []() {};
    }

    // For single thread, just hash everything
    if (totalThreads == 1)
    {
        return [this]() { root->get_hash(options_); };
    }

    // Calculate which branches this thread handles
    int branchesPerThread = 16 / totalThreads;
    int startBranch = threadId * branchesPerThread;
    int endBranch = startBranch + branchesPerThread;

    // Capture the root and options by value/shared_ptr to avoid lifetime issues
    auto capturedRoot = root;
    auto capturedOptions = options_;

    return [capturedRoot, capturedOptions, startBranch, endBranch]() {
        // Just hash the assigned child branches
        // This will recursively hash those entire subtrees
        LOGD(
            "Hash job executing for branches ",
            startBranch,
            " to ",
            endBranch - 1);

        auto children = capturedRoot->get_children();
        for (int i = startBranch; i < endBranch; ++i)
        {
            if (children->has_child(i))
            {
                auto child = children->get_child(i);
                if (child)
                {
                    // This recursively hashes the entire subtree and caches the
                    // result
                    child->get_hash(capturedOptions);
                }
            }
        }
    };
}

template <typename Traits>
void
SHAMapT<Traits>::handle_path_cow(PathFinderT<Traits>& path_finder)
{
    // If CoW is enabled, handle versioning
    if (cow_enabled_)
    {
        // Lazy version bump: increment ONLY on first write after snapshot
        if (needs_version_bump_on_write_)
        {
            current_version_ = new_version(true);
            needs_version_bump_on_write_ = false;

            OLOGD(
                "Lazy version bump on first write after snapshot to version ",
                current_version_);
        }

        if (current_version_ == 0)
        {
            current_version_ = new_version(true);
        }

        // Apply CoW to path
        auto inner_node = path_finder.dirty_or_copy_inners(current_version_);
        if (!inner_node)
        {
            throw NullNodeException(
                "addItem: CoW failed to return valid inner node");
        }

        // If root was copied, update our reference
        if (path_finder.get_parent_of_terminal() != root)
        {
            root = path_finder.search_root_;
        }
    }
}

template <typename Traits>
boost::intrusive_ptr<MmapItem>
SHAMapT<Traits>::get_item(const Key& key) const
{
    if (!root)
    {
        return nullptr;
    }

    // Create a PathFinder to locate the item
    PathFinderT<Traits> path_finder(
        const_cast<boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>&>(root),
        key,
        options_);
    path_finder.find_path();

    // Check if we found a leaf and the keys match
    if (path_finder.has_leaf() && path_finder.did_leaf_key_match())
    {
        auto leaf = path_finder.get_leaf();
        return leaf->get_item();
    }

    // Item not found
    return nullptr;
}

template <typename Traits>
void
SHAMapT<Traits>::set_new_copied_root()
{
    if (!root)
    {
        OLOGW("Cannot copy null root node");
        return;
    }

    // Create a shallow copy of the root node without CoW machinery
    auto new_root =
        boost::intrusive_ptr(new SHAMapInnerNodeT<Traits>(root->get_depth()));

    // Copy children - this creates a non-canonicalized copy that shares child
    // pointers
    auto root_children = root->get_children();
    new_root->set_children(root_children->copy());

    // Copy hash properties
    new_root->hash = root->hash;
    new_root->hash_valid_ = root->hash_valid_;
    new_root->set_version(root->get_version());
    new_root->enable_cow(root->is_cow_enabled());

    // Replace the root
    root = new_root;

    OLOGD("Created shallow copy of root node without CoW");
}

template <typename Traits>
void
SHAMapT<Traits>::walk_new_nodes(
    const std::function<
        bool(const boost::intrusive_ptr<SHAMapTreeNodeT<Traits>>&)>& visitor,
    int target_version) const
{
    if (!root || !visitor)
    {
        PLOGD(
            walk_nodes_log,
            "walk_new_nodes called with null root or visitor - returning "
            "early");
        return;
    }

    // If target_version is -1, use root's version
    if (target_version == -1)
    {
        target_version = root->get_version();
        PLOGD(
            walk_nodes_log, "Using root's version as target: ", target_version);
    }
    else
    {
        PLOGD(
            walk_nodes_log,
            "Walking nodes with explicit target version: ",
            target_version);
    }

    OLOGD("Walking nodes with version ", target_version);

    // Track statistics
    int nodes_visited = 0;
    int nodes_with_target_version = 0;
    int inner_nodes_visited = 0;
    int leaf_nodes_visited = 0;
    int target_inner_nodes = 0;  // Inner nodes with target version
    int target_leaf_nodes = 0;   // Leaf nodes with target version

    // Helper lambda for recursive walking
    std::function<void(
        const boost::intrusive_ptr<SHAMapTreeNodeT<Traits>>&, int depth)>
        walk_impl;

    walk_impl = [&](const boost::intrusive_ptr<SHAMapTreeNodeT<Traits>>& node,
                    int depth) {
        if (!node)
            return;

        nodes_visited++;

        // Get this node's version
        int node_version = -999;  // Invalid default
        std::string node_type_str;

        if (node->is_inner())
        {
            auto inner =
                boost::static_pointer_cast<SHAMapInnerNodeT<Traits>>(node);
            node_version = inner->get_version();
            node_type_str = "INNER";
            inner_nodes_visited++;
        }
        else if (node->is_leaf())
        {
            auto leaf =
                boost::static_pointer_cast<SHAMapLeafNodeT<Traits>>(node);
            node_version = leaf->get_version();
            node_type_str = "LEAF";
            leaf_nodes_visited++;

            // Log leaf key for debugging
            if (leaf->get_item())
            {
                PLOGD(
                    walk_nodes_log,
                    "  Visiting ",
                    node_type_str,
                    " at depth ",
                    depth,
                    " with version ",
                    node_version,
                    " (target: ",
                    target_version,
                    ")",
                    " key: ",
                    leaf->get_item()->key().hex().substr(0, 16),
                    "...");
            }
            else
            {
                PLOGD(
                    walk_nodes_log,
                    "  Visiting ",
                    node_type_str,
                    " at depth ",
                    depth,
                    " with version ",
                    node_version,
                    " (target: ",
                    target_version,
                    ")",
                    " key: NULL ITEM!");
            }
        }

        // Log inner node visits at higher verbosity
        if (node->is_inner())
        {
            auto inner =
                boost::static_pointer_cast<SHAMapInnerNodeT<Traits>>(node);
            PLOGD(
                walk_nodes_log,
                "  Visiting ",
                node_type_str,
                " at depth ",
                depth,
                " with version ",
                node_version,
                " (target: ",
                target_version,
                ")",
                " children: ",
                inner->get_branch_count());
        }

        // If this node has the target version, visit it
        if (node_version == target_version)
        {
            nodes_with_target_version++;
            // Track breakdown by node type
            if (node->is_inner())
            {
                target_inner_nodes++;
            }
            else if (node->is_leaf())
            {
                target_leaf_nodes++;
            }
            PLOGD(
                walk_nodes_log,
                "    ✓ Version MATCHES - calling visitor for ",
                node_type_str);

            if (!visitor(node))
            {
                PLOGD(
                    walk_nodes_log,
                    "    Visitor returned false - stopping walk");
                return;  // Visitor said stop
            }

            // Recurse into children of inner nodes
            if (node->is_inner())
            {
                auto inner =
                    boost::static_pointer_cast<SHAMapInnerNodeT<Traits>>(node);
                PLOGD(
                    walk_nodes_log,
                    "    Recursing into ",
                    inner->get_branch_count(),
                    " children");

                for (int i = 0; i < 16; ++i)
                {
                    if (inner->has_child(i))
                    {
                        walk_impl(inner->get_child(i), depth + 1);
                    }
                }
            }
        }
        else
        {
            PLOGD(
                walk_nodes_log,
                "    ✗ Version mismatch (",
                node_version,
                " != ",
                target_version,
                ") - skipping subtree");
        }
    };

    PLOGD(walk_nodes_log, "Starting walk from root at depth 0");

    // Start walking from the root
    walk_impl(root, 0);

    // Log summary statistics
    PLOGD(
        walk_nodes_log,
        "Walk complete - visited ",
        nodes_visited,
        " total nodes: ",
        inner_nodes_visited,
        " inner, ",
        leaf_nodes_visited,
        " leaves. ",
        "Found ",
        nodes_with_target_version,
        " nodes (l=",
        target_leaf_nodes,
        ",i=",
        target_inner_nodes,
        ") with target version ",
        target_version);
}

// Explicit template instantiations for default traits
template class SHAMapT<DefaultNodeTraits>;

}  // namespace catl::shamap