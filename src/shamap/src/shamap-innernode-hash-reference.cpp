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
    if (uint16_t branchMask = children->get_branch_mask();
        branchMask == 0)
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

        // Add each branch's hash (or zero hash for empty branches)
        for (int i = 0; i < 16; i++)
        {
            const uint8_t* hashData = Hash256::zero().data();
            if (const auto child = children->get_child(i))
            {
                hashData = child->get_hash(options).data();
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