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
Hash256
SHAMapInnerNodeT<Traits>::update_hash_reference(SHAMapOptions const& options)
{
    auto children = this->get_children();
    uint16_t branchMask = children->get_branch_mask();
    uint16_t placeholderMask = children->get_placeholder_mask();
    if (branchMask == 0 && placeholderMask == 0)
    {
        Hash256 result = Hash256::zero();
        this->set_children(children->canonicalize(result));
        return result;
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
            Hash256 child_hash = Hash256::zero();
            if (const auto child = children->get_child(i))
            {
                child_hash = child->get_hash(options);
            }
            else if constexpr (has_placeholders_v<Traits>)
            {
                if (children->has_placeholder(i))
                    child_hash = children->get_placeholder(i);
            }

            hasher.update(child_hash.data(), Hash256::size());
        }

        // Finalize hash and take first 256 bits
        Hash256 result = hasher.finalize();

        // Publish immutable children and hash together under set_children().
        this->set_children(children->canonicalize(result));
        return result;
    }
    catch (const std::exception& e)
    {
        throw HashCalculationException(
            std::string("Hash calculation failed: ") + e.what());
    }
}
// Explicit template instantiation for default traits
template Hash256
SHAMapInnerNodeT<DefaultNodeTraits>::update_hash_reference(
    SHAMapOptions const& options);

}  // namespace catl::shamap
