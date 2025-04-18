#include "catl/shamap/shamap-innernode.h"

#include <openssl/evp.h>
#include <openssl/sha.h>

#include "catl/core/log-macros.h"
#include "catl/core/logger.h"
#include "catl/shamap/shamap-errors.h"
#include "catl/shamap/shamap-hashprefix.h"
#include "catl/shamap/shamap-impl.h"
#include "catl/shamap/shamap-utils.h"

//----------------------------------------------------------
// SHAMapInnerNode Implementation
//----------------------------------------------------------

SHAMapInnerNode::SHAMapInnerNode(uint8_t nodeDepth)
    : depth_(nodeDepth), version(0), do_cow_(false)
{
    children_ = std::make_unique<NodeChildren>();
}

SHAMapInnerNode::SHAMapInnerNode(
    bool isCopy,
    uint8_t nodeDepth,
    int initialVersion)
    : depth_(nodeDepth), version(initialVersion), do_cow_(isCopy)
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
                        result[nibble] = leaf->get_item()->key().hex();
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

bool
SHAMapInnerNode::set_child(
    int branch,
    boost::intrusive_ptr<SHAMapTreeNode> const& child)
{
    if (branch < 0 || branch >= 16)
    {
        throw InvalidBranchException(branch);
    }

    // Check if node is canonicalized - if yes, we need to make a copy first
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
    int leafCount = 0;

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

            leafCount++;
            if (leafCount == 1)
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
    auto newNode =
        boost::intrusive_ptr(new SHAMapInnerNode(true, depth_, newVersion));

    // Copy children - this creates a non-canonicalized copy
    newNode->children_ = children_->copy();

    // Copy other properties
    newNode->hash = hash;
    newNode->hashValid = hashValid;

    LOGD(
        "Cloned inner node from version ",
        get_version(),
        " to version ",
        newVersion);

    return newNode;
}

LogPartition SHAMapInnerNode::log_partition_{
    "SHAMapInnerNode",
    LogLevel::DEBUG};
