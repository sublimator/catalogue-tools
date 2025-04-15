#pragma once

#include <array>
#include <atomic>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <memory>
#include <openssl/evp.h>
#include <stdexcept>
#include <string>
#include <vector>

#include "catl/core/types.h"
#include "catl/shamap/shamap-errors.h"
#include "catl/shamap/shamap-innernode.h"
#include "catl/shamap/shamap-leafnode.h"
#include "catl/shamap/shamap-nodechildren.h"
#include "catl/shamap/shamap-nodetype.h"
#include "catl/shamap/shamap-pathfinder.h"
#include "catl/shamap/shamap-treenode.h"

enum class AddResult {
    arADD,     // New item was added
    arUPDATE,  // Existing item was updated
    arFAILED   // Operation failed
};

/**
 * Main SHAMap class implementing a pruned, binary prefix tree
 * with Copy-on-Write support for efficient snapshots
 */
class SHAMap
{
private:
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

    // Private constructor for creating snapshots
    SHAMap(
        SHAMapNodeType type,
        boost::intrusive_ptr<SHAMapInnerNode> rootNode,
        std::shared_ptr<std::atomic<int>> vCounter,
        int version);

public:
    explicit SHAMap(SHAMapNodeType type = tnACCOUNT_STATE);

    AddResult
    add_item(boost::intrusive_ptr<MmapItem>& item, bool allowUpdate = true);
    bool
    remove_item(const Key& key);
    Hash256
    get_hash() const;

    // Only public CoW method - creates a snapshot
    std::shared_ptr<SHAMap>
    snapshot();
};