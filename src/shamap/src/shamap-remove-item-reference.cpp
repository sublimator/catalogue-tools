#include "catl/core/log-macros.h"
#include "catl/core/logger.h"
#include "catl/core/types.h"
#include "catl/shamap/shamap-errors.h"
#include "catl/shamap/shamap-pathfinder.h"
#include "catl/shamap/shamap.h"
#include <exception>

bool
SHAMap::remove_item_reference(const Key& key)
{
    OLOGD_KEY("Attempting to remove item with key: ", key);
    try
    {
        PathFinder pathFinder(root, key, options_);
        pathFinder.find_path();
        handle_path_cow(pathFinder);

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