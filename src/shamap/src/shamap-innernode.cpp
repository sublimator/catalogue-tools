#include "catl/shamap/shamap-innernode.h"

#include "catl/shamap/shamap-leafnode.h"
#include "catl/shamap/shamap-nodechildren.h"
#include "catl/shamap/shamap-options.h"
#include <boost/json/object.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <cstdint>
#include <memory>

#include "catl/core/logger.h"
#include "catl/shamap/shamap-errors.h"
#include <string>

namespace catl::shamap {
//----------------------------------------------------------
// SHAMapInnerNodeT Implementation
//----------------------------------------------------------

template <typename Traits>
SHAMapInnerNodeT<Traits>::SHAMapInnerNodeT(uint8_t nodeDepth)
    : depth_(nodeDepth), version_(0), do_cow_(false)
{
    children_ = std::make_unique<NodeChildrenT<Traits>>();
}

template <typename Traits>
SHAMapInnerNodeT<Traits>::SHAMapInnerNodeT(
    bool isCopy,
    uint8_t nodeDepth,
    int initialVersion)
    : depth_(nodeDepth), version_(initialVersion), do_cow_(isCopy)
{
    children_ = std::make_unique<NodeChildrenT<Traits>>();
}

template <typename Traits>
bool
SHAMapInnerNodeT<Traits>::is_leaf() const
{
    return false;
}

template <typename Traits>
bool
SHAMapInnerNodeT<Traits>::is_inner() const
{
    return true;
}

// TODO: replace this with an int or something? or add get_depth_int() method
// for debugging
template <typename Traits>
uint8_t
SHAMapInnerNodeT<Traits>::get_depth() const
{
    return depth_;
}

template <typename Traits>
void
SHAMapInnerNodeT<Traits>::set_depth(uint8_t depth)
{
    depth_ = depth;
}

template <typename Traits>
int
SHAMapInnerNodeT<Traits>::get_depth_int() const
{
    return depth_;
}

template <typename Traits>
boost::json::object
SHAMapInnerNodeT<Traits>::trie_json(
    TrieJsonOptions options,
    SHAMapOptions const& shamap_options) const
{
    boost::json::object result;
    result["__depth__"] = depth_;

    for (int i = 0; i < 16; i++)
    {
        if (has_child(i))
        {
            auto child = get_child(i);
            if (child)
            {
                // Convert nibble to hex string
                std::string nibble = (i < 10) ? std::to_string(i)
                                              : std::string(1, 'A' + (i - 10));

                if (child->is_leaf())
                {
                    auto leaf =
                        boost::static_pointer_cast<SHAMapLeafNodeT<Traits>>(
                            child);
                    if (options.key_as_hash)
                    {
                        auto key_hex = leaf->get_item()->key().hex();
                        // wrap [ and ] around the nibble at the inner node's
                        // depth
                        auto wrapped = key_hex.substr(0, depth_) + "[" +
                            key_hex.substr(depth_, 1) + "]" +
                            key_hex.substr(depth_ + 1, 63);
                        result[nibble] = wrapped;
                    }
                    else
                    {
                        result[nibble] = leaf->get_hash(shamap_options).hex();
                    }
                }
                else
                {
                    auto inner =
                        boost::static_pointer_cast<SHAMapInnerNodeT<Traits>>(
                            child);
                    result[nibble] = inner->trie_json(options, shamap_options);
                }
            }
        }
    }

    return result;
}

template <typename Traits>
void
SHAMapInnerNodeT<Traits>::invalidate_hash_recursive()
{
    for (int i = 0; i < 16; i++)
    {
        if (auto child = children_->get_child(i))
        {
            child->invalidate_hash();
            if (child->is_inner())
            {
                auto inner =
                    boost::static_pointer_cast<SHAMapInnerNodeT<Traits>>(child);
                inner->invalidate_hash_recursive();
            }
        }
    }
    this->invalidate_hash();
}

template <typename Traits>
bool
SHAMapInnerNodeT<Traits>::set_child(
    int branch,
    boost::intrusive_ptr<SHAMapTreeNodeT<Traits>> const& child)
{
    if (branch < 0 || branch >= 16)
    {
        throw InvalidBranchException(branch);
    }

    // it should be non-canonicalized
    // Check if node is canonicalized - if yes, we need to make a copy first
    // When the map is mutable, with no copy-on-write, we need to make a copy
    if (children_->is_canonical())
    {
        // Create a non-canonicalized copy of children
        children_ = children_->copy();
    }

    // Now safe to modify
    children_->set_child(branch, child);
    this->invalidate_hash();  // Mark hash as invalid
    return true;
}

template <typename Traits>
boost::intrusive_ptr<SHAMapTreeNodeT<Traits>>
SHAMapInnerNodeT<Traits>::get_child(int branch) const
{
    if (branch < 0 || branch >= 16)
    {
        throw InvalidBranchException(branch);
    }

    return children_->get_child(branch);
}

template <typename Traits>
bool
SHAMapInnerNodeT<Traits>::has_child(int branch) const
{
    if (branch < 0 || branch >= 16)
    {
        throw InvalidBranchException(branch);
    }

    return children_->has_child(branch);
}

template <typename Traits>
int
SHAMapInnerNodeT<Traits>::get_branch_count() const
{
    return children_->get_child_count();
}

template <typename Traits>
uint16_t
SHAMapInnerNodeT<Traits>::get_branch_mask() const
{
    return children_->get_branch_mask();
}

template <typename Traits>
boost::intrusive_ptr<SHAMapLeafNodeT<Traits>>
SHAMapInnerNodeT<Traits>::get_only_child_leaf() const
{
    boost::intrusive_ptr<SHAMapLeafNodeT<Traits>> resultLeaf = nullptr;
    int leaf_count = 0;

    // Iterate through all branches
    for (int i = 0; i < 16; i++)
    {
        if (children_->has_child(i))
        {
            auto child = children_->get_child(i);
            if (child->is_inner())
            {
                return nullptr;  // Found inner node, not a leaf-only node
            }

            leaf_count++;
            if (leaf_count == 1)
            {
                resultLeaf =
                    boost::static_pointer_cast<SHAMapLeafNodeT<Traits>>(child);
            }
            else
            {
                return nullptr;  // More than one leaf
            }
        }
    }

    return resultLeaf;  // Returns leaf if exactly one found, else nullptr
}

template <typename Traits>
boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>
SHAMapInnerNodeT<Traits>::copy(int newVersion) const
{
    // Create a new inner node with same depth
    auto new_node = boost::intrusive_ptr(
        new SHAMapInnerNodeT<Traits>(true, depth_, newVersion));

    // Copy children - this creates a non-canonicalized copy
    new_node->children_ = children_->copy();

    // Copy other properties
    new_node->hash = this->hash;
    new_node->hash_valid_ = this->hash_valid_;

    LOGD(
        "Cloned inner node from version ",
        get_version(),
        " to version ",
        newVersion);

    return new_node;
}

template <typename Traits>
boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>
SHAMapInnerNodeT<Traits>::make_child(int depth) const
{
    // Should at least be one level deeper
    if (depth <= depth_)
    {
        throw InvalidDepthException(depth, 63);
    }
    return boost::intrusive_ptr(
        new SHAMapInnerNodeT<Traits>(do_cow_, depth, version_));
}

// Explicit template instantiation for default traits
template class SHAMapInnerNodeT<DefaultNodeTraits>;

}  // namespace catl::shamap