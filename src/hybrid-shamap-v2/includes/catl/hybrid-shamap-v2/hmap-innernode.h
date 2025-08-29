#pragma once

#include "catl/core/types.h"
#include "catl/hybrid-shamap-v2/hmap-node.h"
#include "catl/hybrid-shamap-v2/poly-node-ptr.h"
#include "catl/v2/catl-v2-structs.h"
#include <array>

namespace catl::hybrid_shamap {

/**
 * Inner node - has up to 16 children
 * Stores raw pointers internally and creates PolyNodePtr lazily via
 * get_child(). This means HmapInnerNode must manually manage reference counts
 * in set_child() and its destructor to maintain ownership correctly.
 */
class HmapInnerNode final : public HMapNode
{
private:
    std::array<void*, 16> children_{};  // Just raw pointers, no tagging
    uint32_t child_types_ =
        0;  // 2 bits × 16 children = 32 bits (WHAT: empty/inner/leaf/hash)
    uint16_t materialized_mask_ =
        0;  // 1 bit × 16 children (WHERE: mmap vs heap)
    uint8_t depth_ = 0;

public:
    HmapInnerNode() = default;
    explicit HmapInnerNode(uint8_t depth) : depth_(depth)
    {
    }

    ~HmapInnerNode();

    Type
    get_type() const override
    {
        return Type::INNER;
    }

    // Check if a child is materialized (heap-allocated)
    [[nodiscard]] bool
    is_child_materialized(int branch) const
    {
        assert(branch >= 0 && branch < 16);
        return (materialized_mask_ >> branch) & 1;
    }

    // Get child as PolyNodePtr (lazily generated view)
    [[nodiscard]] PolyNodePtr
    get_child(int branch) const
    {
        assert(branch >= 0 && branch < 16);

        // Lazily construct PolyNodePtr from our storage
        return PolyNodePtr(
            children_[branch],
            get_child_type(branch),
            is_child_materialized(branch));
    }

    [[nodiscard]] v2::ChildType
    get_child_type(int branch) const
    {
        assert(branch >= 0 && branch < 16);
        return static_cast<v2::ChildType>((child_types_ >> (branch * 2)) & 0x3);
    }

    // Set child from PolyNodePtr
    void
    set_child(int branch, const PolyNodePtr& ptr, v2::ChildType type);

    [[nodiscard]] uint8_t
    get_depth() const
    {
        return depth_;
    }
    void
    set_depth(uint8_t d)
    {
        depth_ = d;
    }

    // Count non-empty children
    [[nodiscard]] int
    count_children() const;

    std::string
    describe() const override
    {
        return "InnerNode(depth=" + std::to_string(depth_) +
            ", children=" + std::to_string(count_children()) + ")";
    }

    /**
     * Find the first leaf in depth-first order
     * @return PolyNodePtr to the first leaf found
     * @throws std::runtime_error if no leaf found (malformed tree)
     */
    [[nodiscard]] PolyNodePtr
    first_leaf() const;

    /**
     * Get the key of the first leaf in depth-first order
     * @return Key of the first leaf
     * @throws std::runtime_error if no leaf found
     */
    [[nodiscard]] Key
    first_leaf_key() const;

    void
    update_hash() override;
};

}  // namespace catl::hybrid_shamap