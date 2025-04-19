#include "catl/core/log-macros.h"
#include "catl/core/logger.h"
#include "catl/shamap/shamap-errors.h"
#include "catl/shamap/shamap-options.h"
#include "catl/shamap/shamap.h"

SetResult
SHAMap::set_item_collapsed(boost::intrusive_ptr<MmapItem>& item, SetMode mode)
{
    if (!item)
    {
        OLOGW("Attempted to add null item to SHAMap.");
        return SetResult::FAILED;
    }
    OLOGD_KEY("Adding item with canonical collapsing for key: ", item->key());

    try
    {
        PathFinder pathFinder(root, item->key(), options_);
        pathFinder.find_path();
        handle_path_cow(pathFinder);

        // Check mode constraints
        bool item_exists =
            pathFinder.has_leaf() && pathFinder.did_leaf_key_match();
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

        // SIMPLE CASE: Update existing item
        if (item_exists && mode != SetMode::ADD_ONLY)
        {
            auto parent = pathFinder.get_parent_of_terminal();
            auto newLeaf =
                boost::intrusive_ptr(new SHAMapLeafNode(item, node_type_));
            if (cow_enabled_)
            {
                newLeaf->set_version(current_version_);
            }
            parent->set_child(pathFinder.get_terminal_branch(), newLeaf);
            pathFinder.dirty_path();
            return SetResult::UPDATE;
        }

        // DIRECT INSERTION: Insert at null branch
        if (pathFinder.ended_at_null_branch())
        {
            auto parent = pathFinder.get_parent_of_terminal();
            int branch = pathFinder.get_terminal_branch();

            // Create new leaf node
            auto newLeaf =
                boost::intrusive_ptr(new SHAMapLeafNode(item, node_type_));
            if (cow_enabled_)
            {
                newLeaf->set_version(current_version_);
            }

            parent->set_child(branch, newLeaf);
            pathFinder.dirty_path();
            return SetResult::ADD;
        }

        // COLLISION HANDLING: Smart collision resolution with skips
        // consideration
        if (pathFinder.has_leaf() && !pathFinder.did_leaf_key_match())
        {
        }

        throw SHAMapException("Unexpected state in set_item_collapsed");
    }
    catch (const SHAMapException& e)
    {
        OLOGE(
            "Error in set_item_collapsed for key ",
            item->key().hex(),
            ": ",
            e.what());
        return SetResult::FAILED;
    }
    catch (const std::exception& e)
    {
        OLOGE(
            "Standard exception in set_item_collapsed for key ",
            item->key().hex(),
            ": ",
            e.what());
        return SetResult::FAILED;
    }
}
