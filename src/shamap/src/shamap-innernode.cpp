#include "catl/shamap/shamap-innernode.h"

#include <openssl/evp.h>
#include <openssl/sha.h>

#include "catl/core/logger.h"
#include "catl/shamap/shamap-errors.h"
#include "catl/shamap/shamap-hashprefix.h"
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

void
SHAMapInnerNode::update_hash()
{
    uint16_t branchMask = children_->get_branch_mask();

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
    if (EVP_DigestUpdate(ctx, prefix.data(), prefix.size()) != 1)
    {
        EVP_MD_CTX_free(ctx);
        throw HashCalculationException("Failed to update digest with prefix");
    }

    Hash256 zeroHash = Hash256::zero();

    for (int i = 0; i < 16; i++)
    {
        const uint8_t* hashData = zeroHash.data();
        auto child = children_->get_child(i);

        if (child)
        {
            if (child->is_inner())
            {
                auto innerChild =
                    boost::static_pointer_cast<SHAMapInnerNode>(child);
                int skips = innerChild->get_depth() - depth_ - 1;

                if (skips > 0)
                {
                    // We have skipped inners - need to find a leaf to determine
                    // the path
                    auto leaf = first_leaf(innerChild);
                    if (!leaf)
                    {
                        // If there's no leaf, just use the regular hash
                        hashData = child->get_hash().data();
                    }
                    else
                    {
                        // We found a leaf to use for the path
                        auto index = leaf->get_item()->key();
                        hashData =
                            compute_skipped_hash(innerChild, index, 1, skips)
                                .data();
                    }
                }
                else
                {
                    // Normal case, no skips
                    hashData = child->get_hash().data();
                }
            }
            else
            {
                // Leaf node
                hashData = child->get_hash().data();
            }
        }

        if (EVP_DigestUpdate(ctx, hashData, Hash256::size()) != 1)
        {
            EVP_MD_CTX_free(ctx);
            throw HashCalculationException(
                "Failed to update digest with child data");
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
    hash = Hash256(fullHash.data());
    hashValid = true;

    // Once hash is calculated, canonicalize to save memory
    children_->canonicalize();
}

// Helper method to find the first leaf in a subtree
boost::intrusive_ptr<SHAMapLeafNode>
SHAMapInnerNode::first_leaf(const boost::intrusive_ptr<SHAMapInnerNode>& inner)
{
    // Create a stack to hold inner nodes to process
    std::stack<boost::intrusive_ptr<SHAMapInnerNode>> nodeStack;
    nodeStack.push(inner);

    while (!nodeStack.empty())
    {
        auto current = nodeStack.top();
        nodeStack.pop();

        // Check all children of this node
        for (int i = 0; i < 16; i++)
        {
            if (!current->has_child(i))
                continue;

            auto child = current->get_child(i);

            if (child->is_leaf())
            {
                // Found a leaf - return it immediately
                return boost::static_pointer_cast<SHAMapLeafNode>(child);
            }

            if (child->is_inner())
            {
                // Push inner node to stack to process its children later
                nodeStack.push(
                    boost::static_pointer_cast<SHAMapInnerNode>(child));
            }
        }
    }

    // No leaf found
    return nullptr;
}

Hash256
SHAMapInnerNode::compute_skipped_hash(
    const boost::intrusive_ptr<SHAMapInnerNode>& inner,
    const Key& index,
    int round,
    int skips) const
{
    // Static zero hash
    static const Hash256 zeroHash = Hash256::zero();

    // Store computed hashes for each level
    std::vector<Hash256> levelHashes(skips - round + 1);

    // Log starting parameters for debugging
    LOGD(
        "compute_skipped_hash: inner depth=",
        static_cast<int>(inner->get_depth()),
        ", this depth=",
        static_cast<int>(depth_),
        ", round=",
        round,
        ", skips=",
        skips);

    // Compute hash for each level, starting from the terminal level
    for (int currentRound = skips; currentRound >= round; currentRound--)
    {
        EVP_MD_CTX* ctx = EVP_MD_CTX_new();
        if (!ctx)
            throw HashCalculationException("Failed to create EVP_MD_CTX");

        if (EVP_DigestInit_ex(ctx, EVP_sha512(), nullptr) != 1)
        {
            EVP_MD_CTX_free(ctx);
            throw HashCalculationException(
                "Failed to initialize SHA-512 digest");
        }

        auto prefix = HashPrefix::innerNode;
        if (EVP_DigestUpdate(ctx, prefix.data(), prefix.size()) != 1)
        {
            EVP_MD_CTX_free(ctx);
            throw HashCalculationException(
                "Failed to update digest with prefix");
        }

        // CRITICAL: Ensure path depth calculation is correct
        // The path depth should be based on the parent node (this) depth
        // plus the current round
        int pathDepth = depth_ + currentRound;
        int selectedBranch = select_branch(index, pathDepth);

        LOGD(
            "compute_skipped_hash: round=",
            currentRound,
            ", pathDepth=",
            pathDepth,
            ", selectedBranch=",
            selectedBranch);

        for (int i = 0; i < 16; i++)
        {
            const uint8_t* hashData;

            if (i == selectedBranch)
            {
                if (currentRound == skips)
                {
                    // Terminal level - use inner's hash
                    hashData = inner->get_hash().data();
                    LOGD(
                        "compute_skipped_hash: terminal level using inner "
                        "hash");
                }
                else
                {
                    // Non-terminal level - use hash from next level
                    hashData = levelHashes[currentRound - round + 1].data();
                    LOGD("compute_skipped_hash: using hash from next level");
                }
            }
            else
            {
                // Branch not on path - use zero hash
                hashData = zeroHash.data();
            }

            if (EVP_DigestUpdate(ctx, hashData, Hash256::size()) != 1)
            {
                EVP_MD_CTX_free(ctx);
                throw HashCalculationException(
                    "Failed to update digest in skipped hash");
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
        levelHashes[currentRound - round] = Hash256(fullHash.data());

        LOGD(
            "compute_skipped_hash: completed round=",
            currentRound,
            ", hash=",
            levelHashes[currentRound - round].hex().substr(0, 8));
    }

    // Return the hash for the initial level
    return levelHashes[0];
}

// Hash256
// SHAMapInnerNode::compute_skipped_hash(
//     const boost::intrusive_ptr<SHAMapInnerNode>& inner,
//     const Key& index,
//     int round,
//     int skips) const
// {
//     // Static zero hash
//     static const Hash256 zeroHash = Hash256::zero();
//
//     // Store computed hashes for each level
//     std::vector<Hash256> levelHashes(skips - round + 1);
//
//     // Compute hash for each level, starting from the terminal level
//     for (int currentRound = skips; currentRound >= round; currentRound--)
//     {
//         EVP_MD_CTX* ctx = EVP_MD_CTX_new();
//         if (!ctx)
//             throw HashCalculationException("Failed to create EVP_MD_CTX");
//
//         if (EVP_DigestInit_ex(ctx, EVP_sha512(), nullptr) != 1)
//         {
//             EVP_MD_CTX_free(ctx);
//             throw HashCalculationException(
//                 "Failed to initialize SHA-512 digest");
//         }
//
//         auto prefix = HashPrefix::innerNode;
//         if (EVP_DigestUpdate(ctx, prefix.data(), prefix.size()) != 1)
//         {
//             EVP_MD_CTX_free(ctx);
//             throw HashCalculationException(
//                 "Failed to update digest with prefix");
//         }
//
//         int pathDepth = depth_ + currentRound;
//         int selectedBranch = select_branch(index, pathDepth);
//
//         for (int i = 0; i < 16; i++)
//         {
//             const uint8_t* hashData;
//
//             if (i == selectedBranch)
//             {
//                 if (currentRound == skips)
//                 {
//                     // Terminal level - use inner's hash
//                     hashData = inner->get_hash().data();
//                 }
//                 else
//                 {
//                     // Non-terminal level - use hash from next level
//                     hashData = levelHashes[currentRound - round + 1].data();
//                 }
//             }
//             else
//             {
//                 // Branch not on path - use zero hash
//                 hashData = zeroHash.data();
//             }
//
//             if (EVP_DigestUpdate(ctx, hashData, Hash256::size()) != 1)
//             {
//                 EVP_MD_CTX_free(ctx);
//                 throw HashCalculationException(
//                     "Failed to update digest in skipped hash");
//             }
//         }
//
//         std::array<uint8_t, 64> fullHash{};
//         unsigned int hashLen = 0;
//         if (EVP_DigestFinal_ex(ctx, fullHash.data(), &hashLen) != 1)
//         {
//             EVP_MD_CTX_free(ctx);
//             throw HashCalculationException("Failed to finalize digest");
//         }
//
//         EVP_MD_CTX_free(ctx);
//         levelHashes[currentRound - round] = Hash256(fullHash.data());
//     }
//
//     // Return the hash for the initial level
//     return levelHashes[0];
// }

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