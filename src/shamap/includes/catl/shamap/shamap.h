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
#include "catl/shamap/shamap-treenode.h"

/**
 * Main SHAMap class implementing a pruned, binary prefix tree
 * with Copy-on-Write support for efficient snapshots
 */
class SHAMap
{
private:
    static LogPartition log_partition_;
    boost::intrusive_ptr<SHAMapInnerNode> root;
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
    get_version() const
    {
        return current_version_;
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
    collapse_inner_node(boost::intrusive_ptr<SHAMapInnerNode>& node);

    /**
     * Finds a single inner child if this node has exactly one inner child
     * and no leaf children
     *
     * @param node The inner node to check
     * @return A pointer to the single inner child, or nullptr if condition not
     * met
     */
    boost::intrusive_ptr<SHAMapInnerNode>
    find_only_single_inner_child(boost::intrusive_ptr<SHAMapInnerNode>& node);

    // Private constructor for creating snapshots
    SHAMap(
        SHAMapNodeType type,
        boost::intrusive_ptr<SHAMapInnerNode> rootNode,
        std::shared_ptr<std::atomic<int>> vCounter,
        int version,
        SHAMapOptions options);

protected:
    friend class PathFinder;

    void
    handle_path_cow(PathFinder& path_finder);

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
    explicit SHAMap(
        SHAMapNodeType type = tnACCOUNT_STATE,
        SHAMapOptions options = SHAMapOptions());

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

    Hash256
    get_hash() const;

    // Only public CoW method - creates a snapshot
    std::shared_ptr<SHAMap>
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
    boost::intrusive_ptr<SHAMapInnerNode>
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
     */
    void
    collapse_tree();
};
