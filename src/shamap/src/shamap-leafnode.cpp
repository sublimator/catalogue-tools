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
// SHAMapLeafNode Implementation
//----------------------------------------------------------

SHAMapLeafNode::SHAMapLeafNode(
    boost::intrusive_ptr<MmapItem> i,
    SHAMapNodeType t)
    : item(std::move(i)), type(t)
{
    if (!item)
    {
        throw NullItemException();
    }
}

bool
SHAMapLeafNode::is_leaf() const
{
    return true;
}

bool
SHAMapLeafNode::is_inner() const
{
    return false;
}

void
SHAMapLeafNode::update_hash(SHAMapOptions const&)
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
        hash = hasher.finalize();
        hash_valid_ = true;
    }
    catch (const std::exception& e)
    {
        throw HashCalculationException(
            std::string("Hash calculation failed: ") + e.what());
    }
}

boost::intrusive_ptr<MmapItem>
SHAMapLeafNode::get_item() const
{
    return item;
}

SHAMapNodeType
SHAMapLeafNode::get_type() const
{
    return type;
}

boost::intrusive_ptr<SHAMapLeafNode>
SHAMapLeafNode::copy() const
{
    auto new_leaf = boost::intrusive_ptr(new SHAMapLeafNode(item, type));
    new_leaf->hash = hash;
    new_leaf->hash_valid_ = hash_valid_;
    new_leaf->version = version;
    return new_leaf;
}
}  // namespace catl::shamap