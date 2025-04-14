#include "hasher/shamap/shamap-innernode.h"

#include <openssl/evp.h>
#include <openssl/sha.h>

#include "hasher/catalogue-consts.h"
#include "hasher/logger.h"
#include "hasher/shamap/shamap-errors.h"

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

uint8_t
SHAMapInnerNode::get_depth() const
{
    return depth_;
}

void
SHAMapInnerNode::update_hash()
{
    uint16_t branchMask = children_->getBranchMask();

    if (branchMask == 0)
    {
        hash = Hash256::zero();
        hashValid = true;
        return;
    }

    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx)
        throw HashCalculationException("Failed to create EVP_MD_CTX");

    if (EVP_DigestInit_ex(ctx, EVP_sha512(), nullptr) != 1)
    {
        EVP_MD_CTX_free(ctx);
        throw HashCalculationException("Failed to initialize SHA-512 digest");
    }

    auto prefix = HashPrefix::innerNode;
    if (EVP_DigestUpdate(ctx, &prefix, sizeof(HashPrefix::innerNode)) != 1)
    {
        EVP_MD_CTX_free(ctx);
        throw HashCalculationException("Failed to update digest with prefix");
    }

    Hash256 zeroHash = Hash256::zero();
    for (int i = 0; i < 16; i++)
    {
        const uint8_t* hashData = zeroHash.data();
        auto child = children_->getChild(i);
        if (child)
        {
            hashData = child->get_hash().data();  // Recursive hash calculation
        }

        if (EVP_DigestUpdate(ctx, hashData, Hash256::size()) != 1)
        {
            EVP_MD_CTX_free(ctx);
            throw HashCalculationException(
                "Failed to update digest with child data (branch " +
                std::to_string(i) + ")");
        }
    }

    std::array<uint8_t, 64> fullHash{};
    unsigned int hashLen = 0;
    if (EVP_DigestFinal_ex(ctx, fullHash.data(), &hashLen) != 1)
    {
        EVP_MD_CTX_free(ctx);
        throw HashCalculationException("Failed to finalize digest");
    }

    EVP_MD_CTX_free(ctx);
    hash = Hash256(reinterpret_cast<const uint8_t*>(fullHash.data()));
    hashValid = true;

    // Once hash is calculated, canonicalize to save memory
    // After this, the node becomes immutable until explicitly copied
    children_->canonicalize();
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
    if (children_->isCanonical())
    {
        // Create a non-canonicalized copy of children
        children_ = children_->copy();
    }

    // Now safe to modify
    children_->setChild(branch, child);
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

    return children_->getChild(branch);
}

bool
SHAMapInnerNode::has_child(int branch) const
{
    if (branch < 0 || branch >= 16)
    {
        throw InvalidBranchException(branch);
    }

    return children_->hasChild(branch);
}

int
SHAMapInnerNode::get_branch_count() const
{
    return children_->getChildCount();
}

uint16_t
SHAMapInnerNode::get_branch_mask() const
{
    return children_->getBranchMask();
}

boost::intrusive_ptr<SHAMapLeafNode>
SHAMapInnerNode::get_only_child_leaf() const
{
    boost::intrusive_ptr<SHAMapLeafNode> resultLeaf = nullptr;
    int leafCount = 0;

    // Iterate through all branches
    for (int i = 0; i < 16; i++)
    {
        if (children_->hasChild(i))
        {
            auto child = children_->getChild(i);
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