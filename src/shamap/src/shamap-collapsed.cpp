#include "catl/core/log-macros.h"
#include "catl/core/logger.h"
#include "catl/shamap/shamap-errors.h"
#include "catl/shamap/shamap-utils.h"
#include "catl/shamap/shamap.h"

bool
SHAMap::remove_item_collapsed(const Key& key)
{
    OLOGD_KEY("Attempting to remove item with key: ", key);
    try
    {
        PathFinder pathFinder(root, key, options_);

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

SetResult
SHAMap::set_item_collapsed(boost::intrusive_ptr<MmapItem>& item, SetMode mode)
{
    if (!item)
    {
        OLOGW("Attempted to add null item to SHAMap.");
        return SetResult::FAILED;
    }
    OLOGD_KEY("Attempting to add item with key: ", item->key());

    try
    {
        PathFinder pathFinder(root, item->key(), options_);

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
