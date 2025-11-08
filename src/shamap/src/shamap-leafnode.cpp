#include "catl/core/types.h"
#include "catl/crypto/sha512-half-hasher.h"
#include "catl/shamap/shamap-nodetype.h"
#include "catl/shamap/shamap-options.h"
#include <array>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <cstring>

#include "catl/core/logger.h"
#include "catl/shamap/shamap-errors.h"
#include "catl/shamap/shamap-innernode.h"
#include "catl/shamap/shamap-leafnode.h"

#include "catl/shamap/shamap-hashprefix.h"
#include <utility>

using catl::crypto::Sha512HalfHasher;

namespace catl::shamap {

// External destructor logging partition
extern LogPartition destructor_log;

namespace {
// Helper to get hash prefix for a node type
std::array<unsigned char, 4>
get_node_prefix(SHAMapNodeType type)
{
    switch (type)
    {
        case tnINNER:
            return HashPrefix::inner_node;
        case tnTRANSACTION_NM:
        case tnTRANSACTION_MD:
            return HashPrefix::tx_node;
        case tnACCOUNT_STATE:
            return HashPrefix::leaf_node;
        default:
            throw std::runtime_error(
                "get_node_prefix: unsupported node type " +
                std::to_string(static_cast<int>(type)));
    }
}
}  // anonymous namespace

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
SHAMapLeafNodeT<Traits>::~SHAMapLeafNodeT()
{
    PLOGD(
        destructor_log,
        "~SHAMapLeafNodeT: version=",
        version,
        ", type=",
        static_cast<int>(type),
        ", item=",
        (item ? "yes" : "no"),
        ", item.key=",
        (item ? item->key().hex().substr(0, 16) : "null"),
        ", this.refcount=",
        SHAMapTreeNodeT<Traits>::ref_count_.load());
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
    auto prefix = get_node_prefix(type);

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
SHAMapLeafNodeT<Traits>::copy(int newVersion, SHAMapInnerNodeT<Traits>* parent)
    const
{
    auto new_leaf =
        boost::intrusive_ptr(new SHAMapLeafNodeT<Traits>(item, type));
    new_leaf->hash = this->hash;
    new_leaf->hash_valid_ = this->hash_valid_;
    new_leaf->version = newVersion;

    // Invoke CoW hook if present
    if constexpr (requires(Traits & t) {
                      t.on_leaf_node_copied(
                          (SHAMapLeafNodeT<Traits>*)nullptr,
                          (const SHAMapLeafNodeT<Traits>*)nullptr,
                          (SHAMapInnerNodeT<Traits>*)nullptr);
                  })
    {
        new_leaf->on_leaf_node_copied(new_leaf.get(), this, parent);
    }

    return new_leaf;
}

template <typename Traits>
size_t
SHAMapLeafNodeT<Traits>::serialized_size() const
{
    // Leaf node format matches hash calculation:
    // 4-byte prefix + item data + 32-byte key
    if (!item)
    {
        return 0;
    }
    return 4 + item->slice().size() + Key::size();
}

template <typename Traits>
size_t
SHAMapLeafNodeT<Traits>::write_to_buffer(uint8_t* ptr) const
{
    // Leaf node format matches hash calculation:
    // 4-byte prefix + item data + 32-byte key
    // This ensures the key can be extracted from the last 32 bytes for tree
    // walking
    if (!item)
    {
        // No item - this shouldn't happen, but handle gracefully
        return 0;
    }

    auto prefix = get_node_prefix(type);

    // Write prefix
    std::memcpy(ptr, prefix.data(), 4);
    ptr += 4;

    // Write item data
    const Slice& data = item->slice();
    std::memcpy(ptr, data.data(), data.size());
    ptr += data.size();

    // Write key (32 bytes)
    std::memcpy(ptr, item->key().data(), Key::size());

    return 4 + data.size() + Key::size();
}

// Explicit template instantiations for default traits
template class SHAMapLeafNodeT<DefaultNodeTraits>;

}  // namespace catl::shamap