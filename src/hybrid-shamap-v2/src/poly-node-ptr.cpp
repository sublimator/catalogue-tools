#include "catl/hybrid-shamap-v2/poly-node-ptr.h"
#include "catl/hybrid-shamap-v2/hmap-innernode.h"
#include "catl/hybrid-shamap-v2/hmap-leafnode.h"
#include "catl/hybrid-shamap-v2/hmap-node.h"
#include "catl/hybrid-shamap-v2/hmap-placeholder.h"
#include "catl/v2/catl-v2-structs.h"
#include <cstring>

namespace catl::hybrid_shamap {

// Constructor with all fields
PolyNodePtr::PolyNodePtr(void* p, v2::ChildType t, bool m)
    : ptr_(p), type_(t), materialized_(m)
{
    add_ref();
}

// Copy constructor
PolyNodePtr::PolyNodePtr(const PolyNodePtr& other)
    : ptr_(other.ptr_), type_(other.type_), materialized_(other.materialized_)
{
    add_ref();
}

// Move constructor
PolyNodePtr::PolyNodePtr(PolyNodePtr&& other) noexcept
    : ptr_(other.ptr_), type_(other.type_), materialized_(other.materialized_)
{
    other.ptr_ = nullptr;
    other.type_ = v2::ChildType::EMPTY;
    other.materialized_ = false;
}

// Destructor
PolyNodePtr::~PolyNodePtr()
{
    release();
}

// Copy assignment
PolyNodePtr&
PolyNodePtr::operator=(const PolyNodePtr& other)
{
    if (this != &other)
    {
        release();  // Release old
        ptr_ = other.ptr_;
        type_ = other.type_;
        materialized_ = other.materialized_;
        add_ref();  // Add ref to new
    }
    return *this;
}

// Move assignment
PolyNodePtr&
PolyNodePtr::operator=(PolyNodePtr&& other) noexcept
{
    if (this != &other)
    {
        release();  // Release old
        ptr_ = other.ptr_;
        type_ = other.type_;
        materialized_ = other.materialized_;
        other.ptr_ = nullptr;
        other.type_ = v2::ChildType::EMPTY;
        other.materialized_ = false;
    }
    return *this;
}

// Helper to increment ref count if materialized
void
PolyNodePtr::add_ref() const
{
    if (is_materialized() && !is_empty())
    {
        intrusive_ptr_add_ref(get_materialized_base());
    }
}

// Helper to decrement ref count if materialized
void
PolyNodePtr::release() const
{
    if (is_materialized() && !is_empty())
    {
        intrusive_ptr_release(get_materialized_base());
    }
}

// New factory method: Takes ownership and increments ref count
PolyNodePtr
PolyNodePtr::adopt_materialized(HMapNode* node)
{
    if (!node)
        return make_empty();

    // Auto-detect type
    v2::ChildType type;
    switch (node->get_type())
    {
        case HMapNode::Type::INNER:
            type = v2::ChildType::INNER;
            break;
        case HMapNode::Type::LEAF:
            type = v2::ChildType::LEAF;
            break;
        case HMapNode::Type::PLACEHOLDER:
            type = v2::ChildType::PLACEHOLDER;
            break;
        default:
            type = v2::ChildType::EMPTY;
    }

    // Use constructor which WILL increment ref count
    return {node, type, true};
}

// Copy hash to buffer
void
PolyNodePtr::copy_hash_to(uint8_t* dest) const
{
    if (is_empty())
    {
        // Zero hash for empty
        std::memset(dest, 0, Hash256::size());
        return;
    }

    if (is_materialized())
    {
        // Get hash from materialized node
        auto* node = get_materialized_base();
        const auto& hash = node->get_hash();
        std::memcpy(dest, hash.data(), Hash256::size());
    }
    else
    {
        // Get perma-cached hash from mmap header
        if (is_inner())
        {
            std::memcpy(
                dest,
                get_memptr<v2::InnerNodeHeader>()->hash.data(),
                Hash256::size());
        }
        else if (is_leaf())
        {
            std::memcpy(
                dest,
                get_memptr<v2::LeafHeader>()->hash.data(),
                Hash256::size());
        }
        else if (is_placeholder())
        {
            // Placeholder should have its hash stored
            const auto& hash = get_materialized<HmapPlaceholder>()->get_hash();
            std::memcpy(dest, hash.data(), Hash256::size());
        }
        else
        {
            // Unknown type - zero hash
            std::memset(dest, 0, Hash256::size());
        }
    }
}

// Get hash as Hash256
Hash256
PolyNodePtr::get_hash() const
{
    Hash256 result;
    copy_hash_to(result.data());
    return result;
}

}  // namespace catl::hybrid_shamap