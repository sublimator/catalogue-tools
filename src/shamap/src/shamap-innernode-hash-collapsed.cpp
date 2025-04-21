#include "catl/shamap/shamap-options.h"
#include <array>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <cstdint>
#include <stack>

#include "catl/shamap/shamap-innernode.h"

#include <openssl/evp.h>

#include "catl/core/log-macros.h"
#include "catl/core/logger.h"
#include "catl/shamap/shamap-errors.h"
#include "catl/shamap/shamap-hashprefix.h"
#include "catl/shamap/shamap-utils.h"
#include <string>
#include <vector>

void
SHAMapInnerNode::update_hash_collapsed(SHAMapOptions const& options)
{
    uint16_t branchMask = children_->get_branch_mask();

    if (branchMask == 0)
    {
        hash = Hash256::zero();
        hash_valid_ = true;
        OLOGD("Empty node (no branches), using zero hash");
        return;
    }

    OLOGD("Calculating hash for node with branch mask ", branchMask);

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

    // Use local variable, not static
    const Hash256 zeroHash = Hash256::zero();

    for (int i = 0; i < 16; i++)
    {
        // Create a local Hash256 for each branch
        Hash256 child_hash = zeroHash;

        if (auto child = children_->get_child(i))
        {
            if (child->is_inner())
            {
                auto innerChild =
                    boost::static_pointer_cast<SHAMapInnerNode>(child);
                int skips = innerChild->get_depth() - depth_ - 1;

                if (skips > 0)
                {
                    OLOGD(
                        "Branch ",
                        i,
                        " has skipped inner nodes: ",
                        skips,
                        " levels");

                    auto leaf = first_leaf(innerChild);
                    if (!leaf)
                    {
                        // No leaf found, use regular hash
                        OLOGD(
                            "No leaf found in branch ",
                            i,
                            ", using regular inner hash");
                        child_hash = child->get_hash(options);
                    }
                    else
                    {
                        // Use leaf key for path
                        auto index = leaf->get_item()->key();
                        OLOGD_KEY(
                            "Found leaf for path in branch " +
                                std::to_string(i) + " with key: ",
                            index);

                        if (options.skipped_inners_hash_impl ==
                            SkippedInnersHashImpl::recursive_simple)
                        {
                            child_hash = compute_skipped_hash_recursive(
                                options, innerChild, index, 1, skips);
                        }
                        else
                        {
                            child_hash = compute_skipped_hash_stack(
                                options, innerChild, index, 1, skips);
                        }
                    }
                }
                else
                {
                    // Normal case, no skips
                    OLOGD("Branch ", i, " has normal inner node (no skips)");
                    child_hash = child->get_hash(options);
                }
            }
            else
            {
                // Leaf node
                OLOGD("Branch ", i, " has leaf node");
                child_hash = child->get_hash(options);
            }
        }
        else
        {
            OLOGD("Branch ", i, " is empty, using zero hash");
            // child_hash already contains zero hash
        }

        // Use this branch's hash for the digest update
        if (EVP_DigestUpdate(ctx, child_hash.data(), Hash256::size()) != 1)
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
    hash_valid_ = true;

    OLOGD("Hash calculation complete: ", hash.hex());

    // Once hash is calculated, canonicalize to save memory
    children_->canonicalize();
}

// Helper method to find the first leaf in a subtree
boost::intrusive_ptr<SHAMapLeafNode>
SHAMapInnerNode::first_leaf(
    const boost::intrusive_ptr<SHAMapInnerNode>& inner) const
{
    OLOGD(
        "Searching for first leaf in inner node at depth ",
        static_cast<int>(inner->get_depth()));

    // Create a stack to hold inner nodes to process
    std::stack<boost::intrusive_ptr<SHAMapInnerNode>> nodeStack;
    nodeStack.push(inner);

    while (!nodeStack.empty())
    {
        auto current = nodeStack.top();
        nodeStack.pop();

        OLOGD(
            "Processing inner node at depth ",
            static_cast<int>(current->get_depth()));

        // Check all children of this node
        for (int i = 0; i < 16; i++)
        {
            if (!current->has_child(i))
                continue;

            auto child = current->get_child(i);
            OLOGD(
                "Checking branch ",
                i,
                " at depth ",
                static_cast<int>(current->get_depth()));

            if (child->is_leaf())
            {
                // Found a leaf - return it immediately
                OLOGD("Found leaf node at branch ", i);
                return boost::static_pointer_cast<SHAMapLeafNode>(child);
            }

            if (child->is_inner())
            {
                // Push inner node to stack to process its children later
                OLOGD(
                    "Found inner node at branch ",
                    i,
                    ", adding to processing stack");
                nodeStack.push(
                    boost::static_pointer_cast<SHAMapInnerNode>(child));
            }
        }
    }

    // No leaf found
    OLOGW("No leaf found in inner node subtree");
    return nullptr;
}

Hash256
SHAMapInnerNode::compute_skipped_hash_recursive(
    const SHAMapOptions& options,
    const boost::intrusive_ptr<SHAMapInnerNode>& inner,
    const Key& index,
    int round,
    int skips) const
{
    // Static zero hash for non-path branches
    static const Hash256 zeroHash = Hash256::zero();

    // Create digest context with proper prefix
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

    // Calculate the path depth - this is the parent depth (this->depth_) plus
    // the current round
    int pathDepth = depth_ + round;

    // Determine which branch we're on at this depth level
    int selectedBranch = select_branch(index, pathDepth);

    OLOGD(
        "Recursive skipped hash - round=",
        round,
        ", depth=",
        pathDepth,
        ", branch=",
        selectedBranch,
        ", terminal=",
        (round == skips));

    // Process all 16 branches
    for (int i = 0; i < 16; i++)
    {
        const uint8_t* hashData = nullptr;

        if (i == selectedBranch)
        {
            // This branch is on our path
            if (round == skips)
            {
                // We're at the terminal level, use the inner node's hash
                // directly
                hashData = inner->get_hash(options).data();
                OLOGD(
                    "Terminal branch ",
                    i,
                    " using hash: ",
                    inner->get_hash(options).hex().substr(0, 16));
            }
            else
            {
                // We need to recurse deeper
                Hash256 nextHash = compute_skipped_hash_recursive(
                    options, inner, index, round + 1, skips);
                hashData = nextHash.data();
                OLOGD(
                    "Non-terminal branch ",
                    i,
                    " using recursive hash: ",
                    nextHash.hex().substr(0, 16));
            }
        }
        else
        {
            // Not on our path - use zero hash
            hashData = zeroHash.data();
            OLOGD("Branch ", i, " not on path, using zero hash");
        }

        if (EVP_DigestUpdate(ctx, hashData, Hash256::size()) != 1)
        {
            EVP_MD_CTX_free(ctx);
            throw HashCalculationException(
                "Failed to update digest in skipped hash");
        }
    }

    // Finalize the hash
    std::array<uint8_t, 64> fullHash{};
    unsigned int hashLen = 0;
    if (EVP_DigestFinal_ex(ctx, fullHash.data(), &hashLen) != 1)
    {
        EVP_MD_CTX_free(ctx);
        throw HashCalculationException("Failed to finalize digest");
    }

    EVP_MD_CTX_free(ctx);
    Hash256 result = Hash256(fullHash.data());

    OLOGD(
        "Completed round ",
        round,
        " recursive hash: ",
        result.hex().substr(0, 16));

    return result;
}

Hash256
SHAMapInnerNode::compute_skipped_hash_stack(
    const SHAMapOptions& options,
    const boost::intrusive_ptr<SHAMapInnerNode>& inner,
    const Key& index,
    int round,
    int skips) const
{
    // Static zero hash
    static const Hash256 zeroHash = Hash256::zero();

    // Store computed hashes for each level
    std::vector<Hash256> levelHashes(skips - round + 1);

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

        int pathDepth = depth_ + currentRound;
        int selectedBranch = select_branch(index, pathDepth);

        for (int i = 0; i < 16; i++)
        {
            const uint8_t* hashData;

            if (i == selectedBranch)
            {
                if (currentRound == skips)
                {
                    // Terminal level - use inner's hash
                    hashData = inner->get_hash(options).data();
                }
                else
                {
                    // Non-terminal level - use hash from next level
                    hashData = levelHashes[currentRound - round + 1].data();
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
    }

    // Return the hash for the initial level
    return levelHashes[0];
}
