#include "catl/core/types.h"
#include "catl/crypto/sha512-half-hasher.h"
#include "catl/shamap/shamap-nodetype.h"
#include "catl/shamap/shamap-options.h"
#include <array>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <cstring>

#include "catl/shamap/shamap-errors.h"
#include "catl/shamap/shamap-leafnode.h"

#include "catl/shamap/shamap-hashprefix.h"
#include <utility>

using catl::crypto::Sha512HalfHasher;

namespace catl::shamap {
//----------------------------------------------------------
// SHAMapLeafNodeT Implementation
//----------------------------------------------------------

template <typename Traits>
SHAMapLeafNodeT<Traits>::SHAMapLeafNodeT(
    boost::intrusive_ptr<MmapItem> i,
    SHAMapNodeType t)
    : item(std::move(i)), type(t)
{
    if (!item)
    {
        throw NullItemException();
    }
}

template <typename Traits>
bool
SHAMapLeafNodeT<Traits>::is_leaf() const
{
    return true;
}

template <typename Traits>
bool
SHAMapLeafNodeT<Traits>::is_inner() const
{
    return false;
}

template <typename Traits>
void
SHAMapLeafNodeT<Traits>::update_hash(SHAMapOptions const&)
{
    std::array<unsigned char, 4> prefix = {0, 0, 0, 0};
    auto set = [&prefix](auto& from) {
        std::memcpy(prefix.data(), from.data(), 4);
    };
    switch (type)
    {
        case tnTRANSACTION_NM:
        case tnTRANSACTION_MD:
            set(HashPrefix::tx_node);
            break;
        case tnACCOUNT_STATE:
        default:
            set(HashPrefix::leaf_node);
            break;
    }

    try
    {
        Sha512HalfHasher hasher;

        // Update with all hash components in order
        hasher.update(prefix.data(), prefix.size());
        hasher.update(item->slice().data(), item->slice().size());
        hasher.update(item->key().data(), Key::size());

        // Finalize hash and take first 256 bits
        this->hash = hasher.finalize();
        this->hash_valid_ = true;
    }
    catch (const std::exception& e)
    {
        throw HashCalculationException(
            std::string("Hash calculation failed: ") + e.what());
    }
}

template <typename Traits>
boost::intrusive_ptr<MmapItem>
SHAMapLeafNodeT<Traits>::get_item() const
{
    return item;
}

template <typename Traits>
SHAMapNodeType
SHAMapLeafNodeT<Traits>::get_type() const
{
    return type;
}

template <typename Traits>
boost::intrusive_ptr<SHAMapLeafNodeT<Traits>>
SHAMapLeafNodeT<Traits>::copy(int newVersion) const
{
    auto new_leaf =
        boost::intrusive_ptr(new SHAMapLeafNodeT<Traits>(item, type));
    new_leaf->hash = this->hash;
    new_leaf->hash_valid_ = this->hash_valid_;
    new_leaf->version = newVersion;
    return new_leaf;
}

// Explicit template instantiations for default traits
template class SHAMapLeafNodeT<DefaultNodeTraits>;

}  // namespace catl::shamap