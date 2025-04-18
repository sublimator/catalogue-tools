#include "catl/shamap/shamap.h"

#include <boost/json/serialize.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>

#include "catl/core/log-macros.h"
#include "catl/core/logger.h"
#include "catl/shamap/shamap-utils.h"
#include "pretty-print-json.h"

//----------------------------------------------------------
// SHAMap Implementation
//----------------------------------------------------------
SHAMap::SHAMap(SHAMapNodeType type, SHAMapOptions options)
    : node_type_(type)
    , options_(options)
    , version_counter_(std::make_shared<std::atomic<int>>(0))
    , current_version_(0)
    , cow_enabled_(false)
{
    root = boost::intrusive_ptr(new SHAMapInnerNode(0));  // Root has depth 0
    OLOGD("SHAMap created with type: ", static_cast<int>(type));
}

SHAMap::SHAMap(
    const SHAMapNodeType type,
    boost::intrusive_ptr<SHAMapInnerNode> rootNode,
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

void
SHAMap::enable_cow(bool enable)
{
    cow_enabled_ = enable;

    // Update root node if it exists
    if (root && enable)
    {
        root->enable_cow(true);

        // Set version if it's 0
        if (root->get_version() == 0)
        {
            root->set_version(current_version_);
        }
    }

    OLOGD(
        "Copy-on-Write ",
        (enable ? "enabled" : "disabled"),
        " for SHAMap with version ",
        current_version_);
}

int
SHAMap::new_version()
{
    if (!version_counter_)
    {
        version_counter_ = std::make_shared<std::atomic<int>>(0);
    }
    // Increment shared counter and update current version
    int newVer = ++(*version_counter_);
    current_version_ = newVer;
    OLOGD("Generated new SHAMap version: ", newVer);
    return newVer;
}

std::shared_ptr<SHAMap>
SHAMap::snapshot()
{
    if (!root)
    {
        OLOGW("Attempted to snapshot a SHAMap with null root");
        return nullptr;
    }

    // Enable CoW if not already enabled
    if (!cow_enabled_)
    {
        enable_cow(!cow_enabled_);
    }

    // Create new version for both original and snapshot
    const int originalVersion = new_version();
    int snapshotVersion = new_version();

    OLOGI(
        "Creating snapshot: original version ",
        originalVersion,
        ", snapshot version ",
        snapshotVersion);

    // Create a new SHAMap that shares the same root and version counter
    auto copy = std::make_shared<SHAMap>(
        SHAMap(node_type_, root, version_counter_, snapshotVersion, options_));

    return copy;
}

namespace json = boost::json;

void
SHAMap::trie_json(std::ostream& os, TrieJsonOptions options) const
{
    auto trie = root->trie_json(options, options_);
    if (options.pretty)
        pretty_print_json(os, trie);
    else
    {
        os << trie;
    }
}

std::string
SHAMap::trie_json_string(TrieJsonOptions options) const
{
    std::ostringstream oss;
    trie_json(oss, options);
    return oss.str();
}

void
SHAMap::visit_items(const std::function<void(const MmapItem&)>& visitor) const
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
    std::stack<boost::intrusive_ptr<const SHAMapTreeNode>> node_stack;

    // Start traversal from the root node
    node_stack.push(boost::static_pointer_cast<const SHAMapTreeNode>(root));

    while (!node_stack.empty())
    {
        // Get the next node from the stack (LIFO -> depth-first)
        boost::intrusive_ptr<const SHAMapTreeNode> current_node =
            node_stack.top();
        node_stack.pop();

        // Safety check
        if (!current_node)
            continue;

        // Process based on node type
        if (current_node->is_leaf())
        {
            auto leaf =
                boost::dynamic_pointer_cast<const SHAMapLeafNode>(current_node);
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
                    "Failed to cast node to const SHAMapLeafNode despite "
                    "is_leaf() being true.");
            }
        }
        else if (current_node->is_inner())
        {
            auto inner = boost::dynamic_pointer_cast<const SHAMapInnerNode>(
                current_node);
            if (inner)
            {
                // Push children onto the stack to visit them next.
                // Pushing 0..15 results in processing 15..0 (right-to-left DFS)
                for (int i = 0; i < 16; ++i)
                {
                    boost::intrusive_ptr<const SHAMapTreeNode> child =
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
                    "Failed to cast node to const SHAMapInnerNode despite "
                    "is_inner() being true.");
            }
        }
        // else: Unknown node type? Log error or ignore.
    }  // End while stack not empty
}

boost::json::array
SHAMap::items_json() const
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

SetResult
SHAMap::add_item(boost::intrusive_ptr<MmapItem>& item)
{
    return set_item(item, SetMode::ADD_ONLY);
}

SetResult
SHAMap::update_item(boost::intrusive_ptr<MmapItem>& item)
{
    return set_item(item, SetMode::UPDATE_ONLY);
}

SetResult
SHAMap::set_item(boost::intrusive_ptr<MmapItem>& item, SetMode mode)
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

bool
SHAMap::remove_item(const Key& key)
{
    if (options_.tree_collapse_impl == TreeCollapseImpl::leafs_and_inners)
    {
        return remove_item_collapsed(key);
    }
    else
    {
        return remove_item_reference(key);
    }
}

Hash256
SHAMap::get_hash() const
{
    if (!root)
    {
        OLOGW("Attempting to get hash of a null root SHAMap.");
        return Hash256::zero();
    }
    // getHash() inside the node handles lazy calculation
    return root->get_hash(options_);
}

LogPartition SHAMap::log_partition_{"SHAMap", LogLevel::DEBUG};
