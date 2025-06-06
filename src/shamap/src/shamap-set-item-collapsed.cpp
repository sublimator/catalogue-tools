#include "catl/core/types.h"
#include "catl/shamap/shamap-leafnode.h"
#include "catl/shamap/shamap-pathfinder.h"
#include "catl/shamap/shamap-utils.h"
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <exception>

#include "catl/core/log-macros.h"
#include "catl/core/logger.h"
#include "catl/shamap/shamap-errors.h"
#include "catl/shamap/shamap-options.h"
#include "catl/shamap/shamap.h"

namespace catl::shamap {
template <typename Traits>
SetResult
SHAMapT<Traits>::set_item_collapsed(
    boost::intrusive_ptr<MmapItem>& item,
    SetMode mode)
{
    if (!item)
    {
        OLOGW("Attempted to add null item to SHAMap.");
        return SetResult::FAILED;
    }
    OLOGD_KEY("Adding item with canonical collapsing for key: ", item->key());

    try
    {
        PathFinderT<Traits> path_finder(
            this->root, item->key(), this->options_);
        path_finder.find_path();
        this->handle_path_cow(path_finder);
        path_finder.add_node_at_divergence();

        // Check mode constraints
        bool item_exists =
            path_finder.has_leaf() && path_finder.did_leaf_key_match();
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
            OLOGD("Updating existing key: ", item->key().hex());
            auto parent = path_finder.get_parent_of_terminal();
            auto new_leaf = boost::intrusive_ptr(
                new SHAMapLeafNodeT<Traits>(item, this->node_type_));
            if (this->cow_enabled_)
            {
                new_leaf->set_version(this->current_version_);
            }
            parent->set_child(path_finder.get_terminal_branch(), new_leaf);
            path_finder.dirty_path();
            return SetResult::UPDATE;
        }

        // DIRECT INSERTION: Insert at null branch
        if (path_finder.ended_at_null_branch())
        {
            OLOGD("ended_at_null_branch Inserting key: ", item->key().hex());
            OLOGD("Pathfinder size: ", path_finder.inners_.size());
            auto parent = path_finder.get_parent_of_terminal();
            int branch = path_finder.get_terminal_branch();

            // Create new leaf node
            auto new_leaf = boost::intrusive_ptr(
                new SHAMapLeafNodeT<Traits>(item, this->node_type_));
            if (this->cow_enabled_)
            {
                new_leaf->set_version(this->current_version_);
            }

            parent->set_child(branch, new_leaf);
            path_finder.dirty_path();
            return SetResult::ADD;
        }

        // COLLISION HANDLING: Smart collision resolution with skips
        // consideration
        if (path_finder.has_leaf() && !path_finder.did_leaf_key_match())
        {
            OLOGD("Handling collision for key: ", item->key().hex());
            auto parent = path_finder.get_parent_of_terminal();
            int parent_depth = parent->get_depth();
            auto other_key = path_finder.get_leaf()->get_item()->key();
            int divergence_depth =
                find_divergence_depth(item->key(), other_key, parent_depth);
            auto new_inner = parent->make_child(divergence_depth);
            parent->set_child(
                parent->select_branch_for_depth(item->key()), new_inner);
            auto new_leaf = boost::intrusive_ptr(
                new SHAMapLeafNodeT<Traits>(item, this->node_type_));
            if (this->cow_enabled_)
            {
                new_leaf->set_version(this->current_version_);
            }
            new_inner->set_child(
                new_inner->select_branch_for_depth(item->key()), new_leaf);
            auto existing_leaf = path_finder.get_leaf_mutable();
            if (this->cow_enabled_)
            {
                existing_leaf = existing_leaf->copy();
                existing_leaf->set_version(this->current_version_);
            }
            // TODO: leaf cow ?
            new_inner->set_child(
                new_inner->select_branch_for_depth(other_key), existing_leaf);
            path_finder.dirty_path();
            return SetResult::ADD;
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
// Explicit template instantiation for default traits
template SetResult
SHAMapT<DefaultNodeTraits>::set_item_collapsed(
    boost::intrusive_ptr<MmapItem>& item,
    SetMode mode);

}  // namespace catl::shamap