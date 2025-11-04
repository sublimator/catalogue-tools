#pragma once

#include <array>
#include <atomic>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <memory>
#include <openssl/evp.h>
#include <stdexcept>
#include <string>
#include <vector>

#include "catl/core/logger.h"
#include "catl/core/types.h"
#include "catl/shamap/shamap-errors.h"
#include "catl/shamap/shamap-innernode.h"
#include "catl/shamap/shamap-leafnode.h"
#include "catl/shamap/shamap-nodechildren.h"
#include "catl/shamap/shamap-nodetype.h"
#include "catl/shamap/shamap-options.h"
#include "catl/shamap/shamap-pathfinder.h"
#include "catl/shamap/shamap-traits.h"
#include "catl/shamap/shamap-treenode.h"

namespace catl::shamap {
/**
 * Main SHAMap class implementing a pruned, binary prefix tree
 * with Copy-on-Write support for efficient snapshots
 */
template <typename Traits = DefaultNodeTraits>
class SHAMapT
{
private:
    static LogPartition log_partition_;
    boost::intrusive_ptr<SHAMapInnerNodeT<Traits>> root;
    SHAMapNodeType node_type_;
    SHAMapOptions options_;

    // CoW support - all private
    std::shared_ptr<std::atomic<int>> version_counter_;
    int current_version_ = 0;
    bool cow_enabled_ = false;

    // Private methods
    void
    enable_cow();

    bool
    is_cow_enabled() const
    {
        return cow_enabled_;
    }

    int
    new_version(bool in_place = false);

    /**
     * Recursively collapses inner nodes that have only a single inner child
     * and no leaf children
     *
     * @param node The inner node to process
     */
    void
    collapse_inner_node(boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>& node);

    /**
     * Finds a single inner child if this node has exactly one inner child
     * and no leaf children
     *
     * @param node The inner node to check
     * @return A pointer to the single inner child, or nullptr if condition not
     * met
     */
    boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>
    find_only_single_inner_child(
        boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>& node);

    // Private constructor for creating snapshots
    SHAMapT(
        SHAMapNodeType type,
        boost::intrusive_ptr<SHAMapInnerNodeT<Traits>> rootNode,
        std::shared_ptr<std::atomic<int>> vCounter,
        int version,
        SHAMapOptions options);

protected:
    template <typename T>
    friend class PathFinderT;

    void
    handle_path_cow(PathFinderT<Traits>& path_finder);

    SetResult
    set_item_reference(
        boost::intrusive_ptr<MmapItem>& item,
        SetMode mode = SetMode::ADD_OR_UPDATE);

    SetResult
    set_item_collapsed(
        boost::intrusive_ptr<MmapItem>& item,
        SetMode mode = SetMode::ADD_OR_UPDATE);
    bool
    remove_item_reference(const Key& key);

public:
    explicit SHAMapT(
        SHAMapNodeType type = tnACCOUNT_STATE,
        SHAMapOptions options = SHAMapOptions());

    ~SHAMapT();

    SetResult
    set_item(
        boost::intrusive_ptr<MmapItem>& item,
        SetMode mode = SetMode::ADD_OR_UPDATE);

    SetResult
    add_item(boost::intrusive_ptr<MmapItem>& item);

    SetResult
    update_item(boost::intrusive_ptr<MmapItem>& item);

    bool
    remove_item(const Key& key);

    [[nodiscard]] bool
    has_item(const Key& key) const;

    Hash256
    get_hash() const;

    // Only public CoW method - creates a snapshot
    std::shared_ptr<SHAMapT<Traits>>
    snapshot();

    void
    trie_json(std::ostream& os, TrieJsonOptions = {}) const;

    std::string
    trie_json_string(TrieJsonOptions options = {}) const;

    void
    visit_items(const std::function<void(const MmapItem&)>& visitor) const;

    void
    invalidate_hash_recursive();

    boost::json::array
    items_json() const;

    /**
     * Get an item by its key
     *
     * @param key The key to look up
     * @return The item if found, or nullptr if not found
     */
    boost::intrusive_ptr<MmapItem>
    get_item(const Key& key) const;

    /**
     * Get the root node of the SHAMap
     *
     * @return Root node pointer
     */
    boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>
    get_root() const
    {
        return root;
    }

    /**
     * Get the SHAMap options
     *
     * @return Options structure
     */
    const SHAMapOptions&
    get_options() const
    {
        return options_;
    }

    static LogPartition&
    get_log_partition()
    {
        return log_partition_;
    }

    /**
     * Collapses the entire tree by removing single-child inner nodes
     * where possible to optimize the tree structure
     *
     * This method optimizes the in-memory representation of the SHAMap by
     * collapsing long chains of inner nodes that have only a single child.
     * The optimization preserves the logical structure and hash computation
     * while reducing memory consumption and improving traversal efficiency.
     *
     * Note: When using a collapsed tree, the hash computation process must
     * account for the skipped nodes to maintain the same hash outcome as
     * a non-collapsed tree. This is handled internally by the hash algorithms.
     */
    void
    collapse_tree();

    /**
     * Creates a shallow copy of the root node and replaces the current root
     * without engaging CoW machinery. This is useful for serialization
     * scenarios where you need to modify node traits without affecting the
     * original tree.
     */
    void
    set_new_copied_root();

    int
    get_version() const
    {
        return current_version_;
    }
};

// Define the static log partition declaration for all template instantiations
template <typename Traits>
LogPartition SHAMapT<Traits>::log_partition_("SHAMap");

// Type alias for backward compatibility
using SHAMap = SHAMapT<DefaultNodeTraits>;

}  // namespace catl::shamap