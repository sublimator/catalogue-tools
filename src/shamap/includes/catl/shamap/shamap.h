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
#include "catl/shamap/shamap-pathfinder.h"
#include "catl/shamap/shamap-treenode.h"

enum class SetResult {
    FAILED = 0,  // Operation failed
    ADD = 1,     // New item was added
    UPDATE = 2,  // Existing item was updated
};

enum class SetMode {
    ADD_ONLY,      // Fail if the item already exists
    UPDATE_ONLY,   // Fail if the item doesn't exist
    ADD_OR_UPDATE  // Allow either adding or updating
};

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

    // CoW support - all private
    std::shared_ptr<std::atomic<int>> version_counter_;
    int current_version_ = 0;
    bool cow_enabled_ = false;

    // Private methods
    void
    enable_cow(bool enable = true);

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
    new_version();

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
        int version);

public:
    explicit SHAMap(SHAMapNodeType type = tnACCOUNT_STATE);

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

    // Add this getter
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
