#pragma once

#include "catl/core/types.h"
#include "catl/hybrid-shamap-v2/hybrid-shamap-forwards.h"
#include "catl/hybrid-shamap-v2/poly-node-ptr.h"
#include "catl/shamap/shamap-options.h"
#include "catl/v2/catl-v2-reader.h"
#include <memory>
#include <vector>

namespace catl::hybrid_shamap {

class Hmap
{
private:
    PolyNodePtr root_;  // Root can be any type of pointer
    std::vector<std::shared_ptr<v2::MmapHolder>>
        mmap_holders_;  // Keeps all mmap files alive

public:
    Hmap() = default;

    // Constructor with single mmap holder
    explicit Hmap(std::shared_ptr<v2::MmapHolder> holder)
    {
        if (holder)
        {
            mmap_holders_.push_back(std::move(holder));
        }
    }

    // Constructor with vector of mmap holders
    explicit Hmap(std::vector<std::shared_ptr<v2::MmapHolder>> holders)
        : mmap_holders_(std::move(holders))
    {
    }

    // Add an mmap holder to keep a file alive
    void
    add_mmap_holder(std::shared_ptr<v2::MmapHolder> holder)
    {
        if (holder)
        {
            mmap_holders_.push_back(std::move(holder));
        }
    }

    // Initialize with a raw memory root (from mmap)
    void
    set_root_raw(const uint8_t* raw_root)
    {
        root_ = PolyNodePtr::wrap_raw_memory(raw_root);
    }

    // Initialize with a materialized root
    void
    set_root_materialized(HmapInnerNode* node);

    [[nodiscard]] PolyNodePtr
    get_root() const
    {
        return root_;
    }

    [[nodiscard]] const std::vector<std::shared_ptr<v2::MmapHolder>>&
    get_mmap_holders() const
    {
        return mmap_holders_;
    }

    [[nodiscard]] Hash256
    get_root_hash() const;

    /**
     * Set (add or update) an item in the tree
     * Will materialize nodes along the path as needed
     *
     * @param key The key to set
     * @param data The data to store
     * @return true if item was added, false if updated
     */
    shamap::SetResult
    set_item(
        const Key& key,
        const Slice& data,
        shamap::SetMode mode = shamap::SetMode::ADD_OR_UPDATE);

    /**
     * Remove an item from the tree
     *
     * @param key The key to remove
     * @return true if item was removed, false if not found
     */
    bool
    remove_item(const Key& key);
};

}  // namespace catl::hybrid_shamap