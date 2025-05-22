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
{
    OLOGD("Created SHAMap snapshot with version ", version);
}

template <typename Traits>
void
SHAMapT<Traits>::enable_cow()
{
    cow_enabled_ = true;

    // Update root node if it exists
    if (root)
    {
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

    // Create new version for both original and snapshot
    const int original_version = new_version(true);
    int snapshot_version = new_version(false);

    OLOGD(
        "Creating snapshot: original version ",
        original_version,
        ", snapshot version ",
        snapshot_version);

    // Create a new SHAMap that shares the same root and version counter
    auto copy = std::make_shared<SHAMapT<Traits>>(SHAMapT<Traits>(
        node_type_,
        root->copy(snapshot_version),
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
void
SHAMapT<Traits>::handle_path_cow(PathFinderT<Traits>& path_finder)
{
    // If CoW is enabled, handle versioning
    if (cow_enabled_)
    {
        if (current_version_ == 0)
        {
            new_version();
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
    new_root->children_ = root->children_->copy();

    // Copy hash properties
    new_root->hash = root->hash;
    new_root->hash_valid_ = root->hash_valid_;

    // Replace the root
    root = new_root;

    OLOGD("Created shallow copy of root node without CoW");
}

// Explicit template instantiations for default traits
template class SHAMapT<DefaultNodeTraits>;

}  // namespace catl::shamap