#pragma once

#include "catl/core/types.h"
#include "catl/hybrid-shamap-v2/hmap-node.h"
#include <vector>

namespace catl::hybrid_shamap {

/**
 * Leaf node - contains actual data
 */
class HmapLeafNode final : public HMapNode
{
private:
    Key key_;
    std::vector<uint8_t> data_;  // Owned copy of data

public:
    HmapLeafNode(const Key& key, const Slice& data)
        : key_(key), data_(data.data(), data.data() + data.size())
    {
    }

    v2::ChildType
    get_type() const override
    {
        return v2::ChildType::LEAF;
    }

    [[nodiscard]] const Key&
    get_key() const
    {
        return key_;
    }

    [[nodiscard]] Slice
    get_data() const
    {
        return Slice(data_.data(), data_.size());
    }

    void
    set_data(const Slice& data)
    {
        data_.assign(data.data(), data.data() + data.size());
        invalidate_hash();
    }

    std::string
    describe() const override
    {
        return "LeafNode(key=" + key_.hex().substr(0, 8) +
            "..., size=" + std::to_string(data_.size()) + ")";
    }

    void
    update_hash() override;
};

}  // namespace catl::hybrid_shamap