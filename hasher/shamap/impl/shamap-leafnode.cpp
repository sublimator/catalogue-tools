#include <openssl/evp.h>

#include "hasher/catalogue-consts.h"
#include "hasher/shamap/shamap-errors.h"
#include "hasher/shamap/shamap-leafnode.h"

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
SHAMapLeafNode::update_hash()
{
    std::array<unsigned char, 4> prefix = {0, 0, 0, 0};
    auto set = [&prefix](auto& from) {
        std::memcpy(prefix.data(), from.data(), 4);
    };
    switch (type)
    {
        case tnTRANSACTION_NM:
        case tnTRANSACTION_MD:
            set(HashPrefix::txNode);
            break;
        case tnACCOUNT_STATE:
        default:
            set(HashPrefix::leafNode);
            break;
    }
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (ctx == nullptr)
    {
        throw HashCalculationException("Failed to create EVP_MD_CTX");
    }
    if (EVP_DigestInit_ex(ctx, EVP_sha512(), nullptr) != 1)
    {
        EVP_MD_CTX_free(ctx);
        throw HashCalculationException("Failed to initialize SHA-512 digest");
    }
    if (EVP_DigestUpdate(ctx, &prefix, sizeof(prefix)) != 1)
    {
        EVP_MD_CTX_free(ctx);
        throw HashCalculationException("Failed to update digest with prefix");
    }
    if (EVP_DigestUpdate(ctx, item->slice().data(), item->slice().size()) != 1)
    {
        EVP_MD_CTX_free(ctx);
        throw HashCalculationException(
            "Failed to update digest with item data");
    }
    if (EVP_DigestUpdate(ctx, item->key().data(), Key::size()) != 1)
    {
        EVP_MD_CTX_free(ctx);
        throw HashCalculationException("Failed to update digest with item key");
    }
    std::array<unsigned char, 64> fullHash{};
    unsigned int hashLen = 0;
    if (EVP_DigestFinal_ex(ctx, fullHash.data(), &hashLen) != 1)
    {
        EVP_MD_CTX_free(ctx);
        throw HashCalculationException("Failed to finalize digest");
    }
    EVP_MD_CTX_free(ctx);
    hash = Hash256(fullHash.data());
    hashValid = true;
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
    auto newLeaf = boost::intrusive_ptr(new SHAMapLeafNode(item, type));
    newLeaf->hash = hash;
    newLeaf->hashValid = hashValid;
    newLeaf->version = version;
    return newLeaf;
}
