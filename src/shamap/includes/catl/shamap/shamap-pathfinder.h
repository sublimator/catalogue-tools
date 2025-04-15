#pragma once

#include "catl/core/types.h"
#include "catl/shamap/shamap-innernode.h"
#include "catl/shamap/shamap-leafnode.h"
/**
 * Helper class to find paths in the tree with CoW support
 */
class PathFinder
{
private:
    const Key& targetKey;
    std::vector<boost::intrusive_ptr<SHAMapInnerNode>> inners;
    std::vector<int> branches;
    boost::intrusive_ptr<SHAMapLeafNode> foundLeaf = nullptr;
    bool leafKeyMatches = false;
    int terminalBranch = -1;

    void
    find_path(boost::intrusive_ptr<SHAMapInnerNode> root);
    bool
    maybe_copy_on_write() const;

protected:
    friend class SHAMap;
    boost::intrusive_ptr<SHAMapInnerNode> searchRoot;

public:
    PathFinder(boost::intrusive_ptr<SHAMapInnerNode>& root, const Key& key);
    bool
    has_leaf() const;
    bool
    did_leaf_key_match() const;
    bool
    ended_at_null_branch() const;
    boost::intrusive_ptr<const SHAMapLeafNode>
    get_leaf() const;
    boost::intrusive_ptr<SHAMapLeafNode>
    get_leaf_mutable();
    boost::intrusive_ptr<SHAMapInnerNode>
    get_parent_of_terminal();
    boost::intrusive_ptr<const SHAMapInnerNode>
    get_parent_of_terminal() const;
    int
    get_terminal_branch() const;
    void
    dirty_path() const;
    void
    collapse_path();

    // CoW support - used by SHAMap operations
    boost::intrusive_ptr<SHAMapInnerNode>
    dirty_or_copy_inners(int targetVersion);
    boost::intrusive_ptr<SHAMapLeafNode>
    invalidated_possibly_copied_leaf_for_updating(int targetVersion);
};