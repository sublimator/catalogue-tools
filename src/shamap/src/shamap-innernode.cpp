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

// External destructor logging partition
extern LogPartition destructor_log;

//----------------------------------------------------------
// SHAMapInnerNodeT Implementation
//----------------------------------------------------------

template <typename Traits>
SHAMapInnerNodeT<Traits>::SHAMapInnerNodeT(uint8_t nodeDepth)
    : depth_(nodeDepth), version_(0), do_cow_(false)
{
    auto* new_children = new NodeChildrenT<Traits>();
    intrusive_ptr_add_ref(new_children);  // OUR ownership reference
    children_ = new_children;  // Plain assignment - constructor, no concurrency
}

template <typename Traits>
SHAMapInnerNodeT<Traits>::SHAMapInnerNodeT(
    bool isCopy,
    uint8_t nodeDepth,
    int initialVersion)
    : depth_(nodeDepth), version_(initialVersion), do_cow_(isCopy)
{
    auto* new_children = new NodeChildrenT<Traits>();
    intrusive_ptr_add_ref(new_children);  // OUR ownership reference
    children_ = new_children;  // Plain assignment - constructor, no concurrency
}

template <typename Traits>
SHAMapInnerNodeT<Traits>::~SHAMapInnerNodeT()
{
    PLOGD(destructor_log, "~SHAMapInnerNodeT: depth=", static_cast<int>(depth_),
          ", version=", version_,
          ", children=", (children_ ? "yes" : "no"),
          ", this.refcount=", SHAMapTreeNodeT<Traits>::ref_count_.load());
    // No need for lock in destructor - no concurrent access possible
    if (children_) {
        intrusive_ptr_release(children_);  // Release OUR ownership reference
    }
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
    auto children = get_children();
    for (int i = 0; i < 16; i++)
    {
        if (auto child = children->get_child(i))
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

    auto children = get_children();

    // Check if node is canonicalized - if yes, we need to make a copy first
    // When the map is mutable, with no copy-on-write, we need to make a copy
    if (children->is_canonical())
    {
        // Create a non-canonicalized copy of children
        auto new_children = children->copy();
        set_children(new_children);
        children = new_children;  // Use the copy for modification
    }

    // Now safe to modify
    children->set_child(branch, child);
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

    auto children = get_children();
    return children->get_child(branch);
}

template <typename Traits>
bool
SHAMapInnerNodeT<Traits>::has_child(int branch) const
{
    if (branch < 0 || branch >= 16)
    {
        throw InvalidBranchException(branch);
    }

    auto children = get_children();
    return children->has_child(branch);
}

template <typename Traits>
int
SHAMapInnerNodeT<Traits>::get_branch_count() const
{
    auto children = get_children();
    return children->get_child_count();
}

template <typename Traits>
uint16_t
SHAMapInnerNodeT<Traits>::get_branch_mask() const
{
    auto children = get_children();
    return children->get_branch_mask();
}

template <typename Traits>
boost::intrusive_ptr<SHAMapLeafNodeT<Traits>>
SHAMapInnerNodeT<Traits>::get_only_child_leaf() const
{
    auto children = get_children();
    if (!children)
    {
        return nullptr;  // No children object
    }

    boost::intrusive_ptr<SHAMapLeafNodeT<Traits>> resultLeaf = nullptr;
    int leaf_count = 0;

    // Iterate through all branches
    for (int i = 0; i < 16; i++)
    {
        if (children->has_child(i))
        {
            auto child = children->get_child(i);
            if (!child)
            {
                continue;  // Child was removed between has_child and get_child
            }
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
SHAMapInnerNodeT<Traits>::copy(int newVersion, SHAMapInnerNodeT<Traits>* parent)
    const
{
    // Create a new inner node with same depth
    auto new_node = boost::intrusive_ptr(
        new SHAMapInnerNodeT<Traits>(true, depth_, newVersion));

    // Copy children - this creates a non-canonicalized copy
    auto children = get_children();
    new_node->set_children(children->copy());

    // Copy other properties
    new_node->hash = this->hash;
    new_node->hash_valid_ = this->hash_valid_;

    LOGD(
        "Cloned inner node from version ",
        get_version(),
        " to version ",
        newVersion);

    // Invoke CoW hook if present
    if constexpr (requires(Traits & t) {
                      t.on_inner_node_copied(
                          (SHAMapInnerNodeT<Traits>*)nullptr,
                          (const SHAMapInnerNodeT<Traits>*)nullptr,
                          (SHAMapInnerNodeT<Traits>*)nullptr);
                  })
    {
        new_node->on_inner_node_copied(new_node.get(), this, parent);
    }

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