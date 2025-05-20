#pragma once

#include "catl/core/logger.h"
#include "catl/core/types.h"
#include "catl/shamap/shamap-innernode.h"
#include "catl/shamap/shamap-leafnode.h"
#include "shamap-options.h"

namespace catl::shamap {
/**
 * Helper class to find paths in the tree with CoW support
 */
template <typename Traits = DefaultNodeTraits>
class PathFinderT
{
private:
    const Key& target_key_;
    const SHAMapOptions options_;
    static LogPartition log_partition_;
    std::vector<boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>> inners_;
    std::vector<int> branches_;
    boost::intrusive_ptr<SHAMapLeafNodeT<Traits>> found_leaf_ = nullptr;
    bool leaf_key_matches_ = false;

    int terminal_branch_ = -1;
    int divergence_depth_ = -1;
    boost::intrusive_ptr<SHAMapInnerNodeT<Traits>> diverged_inner_ = nullptr;

    void
    find_path();

    void
    collapse_path_inners();

protected:
    template <typename T>
    friend class SHAMapT;

    boost::intrusive_ptr<SHAMapInnerNodeT<Traits>> search_root_;

public:
    PathFinderT(
        boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>& root,
        const Key& key,
        SHAMapOptions options);
    bool
    has_leaf() const;
    bool
    did_leaf_key_match() const;
    bool
    ended_at_null_branch() const;
    boost::intrusive_ptr<const SHAMapLeafNodeT<Traits>>
    get_leaf() const;
    boost::intrusive_ptr<SHAMapLeafNodeT<Traits>>
    get_leaf_mutable();

    boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>
    get_parent_of_terminal();
    boost::intrusive_ptr<const SHAMapInnerNodeT<Traits>>
    get_parent_of_terminal() const;

    int
    get_terminal_branch() const;
    void
    dirty_path() const;

    bool
    collapse_path_single_leaf_child();

    void
    collapse_path();

    // CoW support - used by SHAMap operations
    boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>
    dirty_or_copy_inners(int target_version);
    boost::intrusive_ptr<SHAMapLeafNodeT<Traits>>
    invalidated_possibly_copied_leaf_for_updating(int targetVersion);

    void
    add_node_at_divergence();

    static LogPartition&
    get_log_partition()
    {
        return log_partition_;
    }
};

// Define the static log partition for all template instantiations
template <typename Traits>
LogPartition PathFinderT<Traits>::log_partition_("PathFinder");

// Type alias for backward compatibility
using PathFinder = PathFinderT<DefaultNodeTraits>;

}  // namespace catl::shamap