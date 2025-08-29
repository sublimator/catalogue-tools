#include "catl/crypto/sha512-half-hasher.h"
#include "catl/hybrid-shamap-v2/hmap-leafnode.h"
#include "catl/shamap/shamap-hashprefix.h"

namespace catl::hybrid_shamap {

using catl::crypto::Sha512HalfHasher;

/**
 * Update hash for a leaf node
 * Simply hashes: prefix + data + key
 */
void
HmapLeafNode::update_hash()
{
    Sha512HalfHasher hasher;

    // Use leaf_node prefix (we're always dealing with state tree for now)
    auto prefix = catl::shamap::HashPrefix::leaf_node;
    hasher.update(prefix.data(), prefix.size());

    // Add data
    auto data_slice = get_data();
    hasher.update(data_slice.data(), data_slice.size());

    // Add key
    hasher.update(key_.data(), Key::size());

    // Finalize and store
    hash_ = hasher.finalize();
}

}  // namespace catl::hybrid_shamap