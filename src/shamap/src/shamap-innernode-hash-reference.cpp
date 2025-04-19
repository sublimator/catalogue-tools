#include "catl/shamap/shamap-innernode.h"

#include <openssl/evp.h>
#include <openssl/sha.h>

#include "catl/core/log-macros.h"
#include "catl/core/logger.h"
#include "catl/shamap/shamap-errors.h"
#include "catl/shamap/shamap-hashprefix.h"
#include "catl/shamap/shamap-impl.h"
#include "catl/shamap/shamap-utils.h"

// TODO: restore the old update hash function
void
SHAMapInnerNode::update_hash_reference(SHAMapOptions const& options)
{
    if (uint16_t branchMask = children_->get_branch_mask(); branchMask == 0)
    {
        hash = Hash256::zero();
        hash_valid_ = true;
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

    for (int i = 0; i < 16; i++)
    {
        const uint8_t* hashData = Hash256::zero().data();
        if (const auto child = children_->get_child(i))
        {
            hashData = child->get_hash(options).data();
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
    hash_valid_ = true;

    // Once hash is calculated, canonicalize to save memory
    // After this, the node becomes immutable until explicitly copied
    children_->canonicalize();
}
