#pragma once

#include "catl/core/types.h"
#include "catl/hybrid-shamap-v2/hmap-node.h"

namespace catl::hybrid_shamap {

/**
 * Placeholder node - just knows the hash, content not loaded yet
 */
class HmapPlaceholder final : public HMapNode
{
private:
    Hash256 hash_;
    uint8_t depth_ = 0;  // Might need to know if it's inner or leaf

public:
    HmapPlaceholder() = default;
    explicit HmapPlaceholder(const Hash256& hash, uint8_t depth = 0)
        : hash_(hash), depth_(depth)
    {
    }

    Type
    get_type() const override
    {
        return Type::PLACEHOLDER;
    }

    [[nodiscard]] const Hash256&
    get_hash() const
    {
        return hash_;
    }

    [[nodiscard]] uint8_t
    get_depth() const
    {
        return depth_;
    }

    std::string
    describe() const override
    {
        return "Placeholder(hash=" + hash_.hex().substr(0, 8) + "...)";
    }

    void
    update_hash() override
    {
        // Placeholder already has its hash
        // Do nothing - hash_ is already set
    }
};

}  // namespace catl::hybrid_shamap