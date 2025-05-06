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

//----------------------------------------------------------
// SHAMapInnerNode Implementation
//----------------------------------------------------------

SHAMapInnerNode::SHAMapInnerNode(uint8_t nodeDepth)
    : depth_(nodeDepth), version_(0), do_cow_(false)
{
    children_ = std::make_unique<NodeChildren>();
}

SHAMapInnerNode::SHAMapInnerNode(
    bool isCopy,
    uint8_t nodeDepth,
    int initialVersion)
    : depth_(nodeDepth), version_(initialVersion), do_cow_(isCopy)
{
    children_ = std::make_unique<NodeChildren>();
}

bool
SHAMapInnerNode::is_leaf() const
{
    return false;
}

bool
SHAMapInnerNode::is_inner() const
{
    return true;
}

// TODO: replace this with an int or something? or add get_depth_int() method
// for debugging
uint8_t
SHAMapInnerNode::get_depth() const
{
    return depth_;
}

void
SHAMapInnerNode::set_depth(uint8_t depth)
{
    depth_ = depth;
}

int
SHAMapInnerNode::get_depth_int() const
{
    return depth_;
}

boost::json::object
SHAMapInnerNode::trie_json(
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
                        boost::static_pointer_cast<SHAMapLeafNode>(child);
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
                        boost::static_pointer_cast<SHAMapInnerNode>(child);
                    result[nibble] = inner->trie_json(options, shamap_options);
                }
            }
        }
    }

    return result;
}

void
SHAMapInnerNode::invalidate_hash_recursive()
{
    for (int i = 0; i < 16; i++)
    {
        if (auto child = children_->get_child(i))
        {
            child->invalidate_hash();
            if (child->is_inner())
            {
                auto inner = boost::static_pointer_cast<SHAMapInnerNode>(child);
                inner->invalidate_hash_recursive();
            }
        }
    }
    invalidate_hash();
}

bool
SHAMapInnerNode::set_child(
    int branch,
    boost::intrusive_ptr<SHAMapTreeNode> const& child)
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
    invalidate_hash();  // Mark hash as invalid
    return true;
}

boost::intrusive_ptr<SHAMapTreeNode>
SHAMapInnerNode::get_child(int branch) const
{
    if (branch < 0 || branch >= 16)
    {
        throw InvalidBranchException(branch);
    }

    return children_->get_child(branch);
}

bool
SHAMapInnerNode::has_child(int branch) const
{
    if (branch < 0 || branch >= 16)
    {
        throw InvalidBranchException(branch);
    }

    return children_->has_child(branch);
}

int
SHAMapInnerNode::get_branch_count() const
{
    return children_->get_child_count();
}

uint16_t
SHAMapInnerNode::get_branch_mask() const
{
    return children_->get_branch_mask();
}

boost::intrusive_ptr<SHAMapLeafNode>
SHAMapInnerNode::get_only_child_leaf() const
{
    boost::intrusive_ptr<SHAMapLeafNode> resultLeaf = nullptr;
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
                resultLeaf = boost::static_pointer_cast<SHAMapLeafNode>(child);
            }
            else
            {
                return nullptr;  // More than one leaf
            }
        }
    }

    return resultLeaf;  // Returns leaf if exactly one found, else nullptr
}

boost::intrusive_ptr<SHAMapInnerNode>
SHAMapInnerNode::copy(int newVersion) const
{
    // Create a new inner node with same depth
    auto new_node =
        boost::intrusive_ptr(new SHAMapInnerNode(true, depth_, newVersion));

    // Copy children - this creates a non-canonicalized copy
    new_node->children_ = children_->copy();

    // Copy other properties
    new_node->hash = hash;
    new_node->hash_valid_ = hash_valid_;

    LOGD(
        "Cloned inner node from version ",
        get_version(),
        " to version ",
        newVersion);

    return new_node;
}

boost::intrusive_ptr<SHAMapInnerNode>
SHAMapInnerNode::make_child(int depth) const
{
    // Should at least be one level deeper
    if (depth <= depth_)
    {
        throw InvalidDepthException(depth, 63);
    }
    return boost::intrusive_ptr(new SHAMapInnerNode(do_cow_, depth, version_));
}

LogPartition SHAMapInnerNode::log_partition_{
    "SHAMapInnerNode",
    LogLevel::DEBUG};
