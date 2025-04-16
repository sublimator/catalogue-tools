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
SHAMap::SHAMap(SHAMapNodeType type)
    : node_type_(type)
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
    const int version)
    : root(std::move(rootNode))
    , node_type_(type)
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
        LOGW("Attempted to snapshot a SHAMap with null root");
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

    LOGI(
        "Creating snapshot: original version ",
        originalVersion,
        ", snapshot version ",
        snapshotVersion);

    // Create a new SHAMap that shares the same root and version counter
    auto copy = std::make_shared<SHAMap>(
        SHAMap(node_type_, root, version_counter_, snapshotVersion));

    return copy;
}

namespace json = boost::json;

void
SHAMap::trie_json(std::ostream& os) const
{
    auto trie = root->trie_json();
    pretty_print_json(os, trie);
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
                    {  // Only push valid children
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
    if (!item)
    {
        OLOGW("Attempted to add null item to SHAMap.");
        return SetResult::FAILED;
    }
    OLOGD_KEY("Attempting to add item with key: ", item->key());

    try
    {
        PathFinder pathFinder(root, item->key());

        // If CoW is enabled, handle versioning
        if (cow_enabled_)
        {
            // First generate a new version if needed
            if (current_version_ == 0)
            {
                new_version();
            }

            // Apply CoW to path
            auto innerNode = pathFinder.dirty_or_copy_inners(current_version_);
            if (!innerNode)
            {
                throw NullNodeException(
                    "addItem: CoW failed to return valid inner node");
            }

            // If root was copied, update our reference
            if (pathFinder.get_parent_of_terminal() != root)
            {
                root = pathFinder.searchRoot;
            }
        }

        bool itemExists =
            pathFinder.has_leaf() && pathFinder.did_leaf_key_match();

        // Early checks based on mode
        if (itemExists && mode == SetMode::ADD_ONLY)
        {
            OLOGW(
                "Item with key ",
                item->key().hex(),
                " already exists, but ADD_ONLY specified");
            return SetResult::FAILED;
        }

        if (!itemExists && mode == SetMode::UPDATE_ONLY)
        {
            OLOGW(
                "Item with key ",
                item->key().hex(),
                " doesn't exist, but UPDATE_ONLY specified");
            return SetResult::FAILED;
        }

        if (pathFinder.ended_at_null_branch() ||
            (itemExists && mode != SetMode::ADD_ONLY))
        {
            auto parent = pathFinder.get_parent_of_terminal();
            int branch = pathFinder.get_terminal_branch();
            if (!parent)
            {
                throw NullNodeException(
                    "addItem: null parent node (should be root)");
            }

            OLOGD(
                "Adding/Updating leaf at depth ",
                parent->get_depth() + 1,
                " branch ",
                branch);
            auto newLeaf =
                boost::intrusive_ptr(new SHAMapLeafNode(item, node_type_));
            if (cow_enabled_)
            {
                newLeaf->set_version(current_version_);
            }
            parent->set_child(branch, newLeaf);
            // pathFinder.update_path();
            pathFinder.dirty_path();
            pathFinder.collapse_path();  // Add collapsing OLOGic here
            return itemExists ? SetResult::UPDATE : SetResult::ADD;
        }

        if (pathFinder.has_leaf() && !pathFinder.did_leaf_key_match())
        {
            OLOGD_KEY("Handling collision for key: ", item->key());
            auto parent = pathFinder.get_parent_of_terminal();
            int branch = pathFinder.get_terminal_branch();
            if (!parent)
            {
                throw NullNodeException(
                    "addItem collision: null parent node (should be root)");
            }
            auto existingLeaf = pathFinder.get_leaf_mutable();
            auto existingItem = existingLeaf->get_item();
            if (!existingItem)
            {
                throw NullItemException(); /* Should be caught by leaf
                                              constructor */
            }

            boost::intrusive_ptr<SHAMapInnerNode> currentParent = parent;
            int currentBranch = branch;
            uint8_t currentDepth =
                parent->get_depth() + 1;  // Start depth below parent

            // Create first new inner node to replace the leaf
            auto newInner =
                boost::intrusive_ptr(new SHAMapInnerNode(currentDepth));
            if (cow_enabled_)
            {
                newInner->enable_cow(true);
                newInner->set_version(current_version_);
            }
            parent->set_child(currentBranch, newInner);
            currentParent = newInner;

            while (currentDepth < 64)
            {
                // Max depth check
                int existingBranch =
                    select_branch(existingItem->key(), currentDepth);
                int newBranch = select_branch(item->key(), currentDepth);

                if (existingBranch != newBranch)
                {
                    OLOGD(
                        "Collision resolved at depth ",
                        std::to_string(currentDepth),
                        ". Placing leaves at branches ",
                        existingBranch,
                        " and ",
                        newBranch);
                    auto newLeaf = boost::intrusive_ptr(
                        new SHAMapLeafNode(item, node_type_));
                    if (cow_enabled_)
                    {
                        newLeaf->set_version(current_version_);
                        // May need to update existing leaf version as well
                        if (existingLeaf->get_version() != current_version_)
                        {
                            auto copiedLeaf = existingLeaf->copy();
                            copiedLeaf->set_version(current_version_);
                            existingLeaf = copiedLeaf;
                        }
                    }
                    currentParent->set_child(existingBranch, existingLeaf);
                    currentParent->set_child(newBranch, newLeaf);
                    break;  // Done
                }
                else
                {
                    // Collision continues, create another inner node
                    OLOGD(
                        "Collision continues at depth ",
                        std::to_string(currentDepth),
                        ", branch ",
                        std::to_string(existingBranch),
                        ". Descending further.");
                    auto nextInner = boost::intrusive_ptr(
                        new SHAMapInnerNode(currentDepth + 1));
                    if (cow_enabled_)
                    {
                        nextInner->enable_cow(true);
                        nextInner->set_version(current_version_);
                    }
                    currentParent->set_child(existingBranch, nextInner);
                    currentParent = nextInner;
                    currentDepth++;
                }
            }
            if (currentDepth >= 64)
            {
                throw SHAMapException(
                    "Maximum SHAMap depth reached during collision resolution "
                    "for key: " +
                    item->key().hex());
            }

            pathFinder.update_path();
            pathFinder.dirty_path();
            pathFinder.collapse_path();  // Add collapsing OLOGic here
            return SetResult::ADD;
        }

        // Should ideally not be reached if PathFinder OLOGic is correct
        OLOGE(
            "Unexpected state in addItem for key: ",
            item->key().hex(),
            ". PathFinder OLOGic error?");
        throw SHAMapException(
            "Unexpected state in addItem - PathFinder OLOGic error");
    }
    catch (const SHAMapException& e)
    {
        OLOGE("Error adding item with key ", item->key().hex(), ": ", e.what());
        return SetResult::FAILED;
    }
    catch (const std::exception& e)
    {
        OLOGE(
            "Standard exception adding item with key ",
            item->key().hex(),
            ": ",
            e.what());
        return SetResult::FAILED;
    }
}

bool
SHAMap::remove_item(const Key& key)
{
    OLOGD_KEY("Attempting to remove item with key: ", key);
    try
    {
        PathFinder pathFinder(root, key);

        // If CoW is enabled, handle versioning
        if (cow_enabled_)
        {
            // First generate a new version if needed
            if (current_version_ == 0)
            {
                new_version();
            }

            // Apply CoW to path
            auto innerNode = pathFinder.dirty_or_copy_inners(current_version_);
            if (!innerNode)
            {
                throw NullNodeException(
                    "removeItem: CoW failed to return valid inner node");
            }

            // If root was copied, update our reference
            if (pathFinder.get_parent_of_terminal() != root)
            {
                root = pathFinder.searchRoot;
            }
        }

        if (!pathFinder.has_leaf() || !pathFinder.did_leaf_key_match())
        {
            OLOGD_KEY("Item not found for removal, key: ", key);
            return false;  // Item not found
        }

        auto parent = pathFinder.get_parent_of_terminal();
        int branch = pathFinder.get_terminal_branch();
        if (!parent)
        {
            throw NullNodeException(
                "removeItem: null parent node (should be root)");
        }

        OLOGD(
            "Removing leaf at depth ",
            parent->get_depth() + 1,
            " branch ",
            branch);
        parent->set_child(branch, nullptr);  // Remove the leaf
        // pathFinder.update_path();
        pathFinder.dirty_path();
        pathFinder.collapse_path();  // Compress path if possible
        OLOGD_KEY("Item removed successfully, key: ", key);
        return true;
    }
    catch (const SHAMapException& e)
    {
        OLOGE("Error removing item with key ", key.hex(), ": ", e.what());
        return false;
    }
    catch (const std::exception& e)
    {
        OLOGE(
            "Standard exception removing item with key ",
            key.hex(),
            ": ",
            e.what());
        return false;
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
    return root->get_hash();
}

void
SHAMap::collapse_tree()
{
    if (root)
    {
        collapse_inner_node(root);
    }
}

void
SHAMap::collapse_inner_node(boost::intrusive_ptr<SHAMapInnerNode>& node)
{
    if (!node)
    {
        return;
    }

    // First, recursively process all child nodes
    for (int i = 0; i < 16; i++)
    {
        if (node->has_child(i))
        {
            auto child = node->get_child(i);
            if (child && child->is_inner())
            {
                auto innerChild =
                    boost::static_pointer_cast<SHAMapInnerNode>(child);
                collapse_inner_node(innerChild);
                // Update the child reference in case it changed
                node->set_child(i, innerChild);
            }
        }
    }

    // After processing all children, look for a single inner child

    if (boost::intrusive_ptr<SHAMapInnerNode> singleInnerChild =
            find_only_single_inner_child(node))
    {
        OLOGD(
            "Collapsing node at depth ",
            static_cast<int>(node->get_depth()),
            " with single inner child at depth ",
            static_cast<int>(singleInnerChild->get_depth()));
        // throw std::runtime_error("Not implemented");

        // Copy all branches from the child to this node
        for (int i = 0; i < 16; i++)
        {
            // Remove existing branch
            node->set_child(i, nullptr);

            // Set branch from child if it exists
            if (singleInnerChild->has_child(i))
            {
                node->set_child(i, singleInnerChild->get_child(i));
            }
        }

        // Update depth and other properties
        node->depth_ = singleInnerChild->get_depth();

        // If using CoW support, copy relevant properties
        if (cow_enabled_)
        {
            node->set_version(singleInnerChild->get_version());
            node->enable_cow(singleInnerChild->is_cow_enabled());
        }

        // Invalidate the hash since we've changed the structure
        node->invalidate_hash();
    }
}

boost::intrusive_ptr<SHAMapInnerNode>
SHAMap::find_only_single_inner_child(
    boost::intrusive_ptr<SHAMapInnerNode>& node)
{
    boost::intrusive_ptr<SHAMapInnerNode> singleInnerChild = nullptr;
    int innerCount = 0;

    for (int i = 0; i < 16; i++)
    {
        if (node->has_child(i))
        {
            if (auto child = node->get_child(i))
            {
                if (child->is_inner())
                {
                    innerCount++;
                    if (innerCount == 1)
                    {
                        singleInnerChild =
                            boost::static_pointer_cast<SHAMapInnerNode>(child);
                    }
                    else
                    {
                        return nullptr;  // More than one inner child, don't
                                         // collapse
                    }
                }
                else if (child->is_leaf())
                {
                    return nullptr;  // Contains a leaf node, don't collapse
                }
            }
        }
    }

    return singleInnerChild;
}

LogPartition SHAMap::log_partition_{"SHAMap", LogLevel::DEBUG};
