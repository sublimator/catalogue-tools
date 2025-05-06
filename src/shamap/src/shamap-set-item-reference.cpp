#include "catl/core/log-macros.h"
#include "catl/core/logger.h"
#include "catl/core/types.h"
#include "catl/shamap/shamap-errors.h"
#include "catl/shamap/shamap-leafnode.h"
#include "catl/shamap/shamap-options.h"
#include "catl/shamap/shamap-pathfinder.h"
#include "catl/shamap/shamap-utils.h"
#include "catl/shamap/shamap.h"
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <cstdint>
#include <exception>

SetResult
SHAMap::set_item_reference(boost::intrusive_ptr<MmapItem>& item, SetMode mode)
{
    if (!item)
    {
        OLOGW("Attempted to add null item to SHAMap.");
        return SetResult::FAILED;
    }
    OLOGD_KEY("Attempting to add item with key: ", item->key());

    try
    {
        PathFinder path_finder(root, item->key(), options_);
        path_finder.find_path();
        handle_path_cow(path_finder);

        bool item_exists =
            path_finder.has_leaf() && path_finder.did_leaf_key_match();

        // Early checks based on mode
        if (item_exists && mode == SetMode::ADD_ONLY)
        {
            OLOGW(
                "Item with key ",
                item->key().hex(),
                " already exists, but ADD_ONLY specified");
            return SetResult::FAILED;
        }

        if (!item_exists && mode == SetMode::UPDATE_ONLY)
        {
            OLOGW(
                "Item with key ",
                item->key().hex(),
                " doesn't exist, but UPDATE_ONLY specified");
            return SetResult::FAILED;
        }

        if (path_finder.ended_at_null_branch() ||
            (item_exists && mode != SetMode::ADD_ONLY))
        {
            auto parent = path_finder.get_parent_of_terminal();
            int branch = path_finder.get_terminal_branch();
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
            auto new_leaf =
                boost::intrusive_ptr(new SHAMapLeafNode(item, node_type_));
            if (cow_enabled_)
            {
                new_leaf->set_version(current_version_);
            }
            parent->set_child(branch, new_leaf);
            path_finder.dirty_path();
            path_finder.collapse_path();  // Add collapsing logic here
            return item_exists ? SetResult::UPDATE : SetResult::ADD;
        }

        if (path_finder.has_leaf() && !path_finder.did_leaf_key_match())
        {
            OLOGD_KEY("Handling collision for key: ", item->key());
            auto parent = path_finder.get_parent_of_terminal();
            int branch = path_finder.get_terminal_branch();
            if (!parent)
            {
                throw NullNodeException(
                    "addItem collision: null parent node (should be root)");
            }
            auto existing_leaf = path_finder.get_leaf_mutable();
            auto existing_item = existing_leaf->get_item();
            if (!existing_item)
            {
                throw NullItemException(); /* Should be caught by leaf
                                              constructor */
            }

            boost::intrusive_ptr<SHAMapInnerNode> current_parent = parent;
            int current_branch = branch;
            uint8_t current_depth =
                parent->get_depth() + 1;  // Start depth below parent

            // Create first new inner node to replace the leaf
            auto new_inner =
                boost::intrusive_ptr(new SHAMapInnerNode(current_depth));
            if (cow_enabled_)
            {
                new_inner->enable_cow(true);
                new_inner->set_version(current_version_);
            }
            parent->set_child(current_branch, new_inner);
            current_parent = new_inner;

            while (current_depth < 64)
            {
                // Max depth check
                int existing_branch =
                    select_branch(existing_item->key(), current_depth);
                int new_branch = select_branch(item->key(), current_depth);

                if (existing_branch != new_branch)
                {
                    OLOGD(
                        "Collision resolved at depth ",
                        current_depth,
                        ". Placing leaves at branches ",
                        existing_branch,
                        " and ",
                        new_branch);
                    auto new_leaf = boost::intrusive_ptr(
                        new SHAMapLeafNode(item, node_type_));
                    if (cow_enabled_)
                    {
                        new_leaf->set_version(current_version_);
                        // May need to update existing leaf version as well
                        if (existing_leaf->get_version() != current_version_)
                        {
                            auto copied_leaf = existing_leaf->copy();
                            copied_leaf->set_version(current_version_);
                            existing_leaf = copied_leaf;
                        }
                    }
                    current_parent->set_child(existing_branch, existing_leaf);
                    current_parent->set_child(new_branch, new_leaf);
                    break;  // Done
                }
                else
                {
                    // Collision continues, create another inner node
                    OLOGD(
                        "Collision continues at depth ",
                        current_depth,
                        ", branch ",
                        existing_branch,
                        ". Descending further.");
                    auto next_inner = boost::intrusive_ptr(
                        new SHAMapInnerNode(current_depth + 1));
                    if (cow_enabled_)
                    {
                        next_inner->enable_cow(true);
                        next_inner->set_version(current_version_);
                    }
                    current_parent->set_child(existing_branch, next_inner);
                    current_parent = next_inner;
                    current_depth++;
                }
            }
            if (current_depth >= 64)
            {
                throw SHAMapException(
                    "Maximum SHAMap depth reached during collision resolution "
                    "for key: " +
                    item->key().hex());
            }

            path_finder.dirty_path();
            path_finder.collapse_path();  // Add collapsing logic here
            return SetResult::ADD;
        }

        // Should ideally not be reached if PathFinder logic is correct
        OLOGE(
            "Unexpected state in addItem for key: ",
            item->key().hex(),
            ". PathFinder logic error?");
        throw SHAMapException(
            "Unexpected state in addItem - PathFinder logic error");
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
