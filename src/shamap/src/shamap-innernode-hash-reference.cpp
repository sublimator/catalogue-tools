#include "catl/shamap/shamap-innernode.h"

#include "catl/crypto/sha512-half-hasher.h"
#include "catl/shamap/shamap-options.h"
#include <array>
#include <cstdint>

#include "catl/shamap/shamap-errors.h"
#include "catl/shamap/shamap-hashprefix.h"
#include <string>

namespace catl::shamap {
// TODO: restore the old update hash function
template <typename Traits>
void
SHAMapInnerNodeT<Traits>::update_hash_reference(SHAMapOptions const& options)
{
    auto children = this->get_children();
    uint16_t branchMask = children->get_branch_mask();
    uint16_t placeholderMask = children->get_placeholder_mask();
    if (branchMask == 0 && placeholderMask == 0)
    {
        this->hash = Hash256::zero();
        this->hash_valid_ = true;
        return;
    }

    try
    {
        catl::crypto::Sha512HalfHasher hasher;

        // Add the prefix
        auto prefix = HashPrefix::inner_node;
        hasher.update(prefix.data(), prefix.size());

        // Add each branch's hash. Three sources in priority order:
        //   1. Real child → compute/get its hash
        //   2. Placeholder → use precomputed subtree hash (abbreviated trees
        //   only)
        //   3. Neither → zero hash (empty branch)
        for (int i = 0; i < 16; i++)
        {
            const uint8_t* hashData = Hash256::zero().data();
            if (const auto child = children->get_child(i))
            {
                hashData = child->get_hash(options).data();
            }
            else if constexpr (has_placeholders_v<Traits>)
            {
                if (children->has_placeholder(i))
                    hashData = children->get_placeholder(i).data();
            }

            hasher.update(hashData, Hash256::size());
        }

        // Finalize hash and take first 256 bits
        this->hash = hasher.finalize();
        this->hash_valid_ = true;

        // Once hash is calculated, canonicalize to save memory
        // After this, the node becomes immutable until explicitly copied
        if (auto canonical = children->canonicalize())
        {
            this->set_children(canonical);
        }
    }
    catch (const std::exception& e)
    {
        throw HashCalculationException(
            std::string("Hash calculation failed: ") + e.what());
    }
}
// Explicit template instantiation for default traits
template void
SHAMapInnerNodeT<DefaultNodeTraits>::update_hash_reference(
    SHAMapOptions const& options);

}  // namespace catl::shamap