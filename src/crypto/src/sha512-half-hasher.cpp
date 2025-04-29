#include "catl/crypto/sha512-half-hasher.h"
#include <array>
#include <stdexcept>

namespace catl::crypto {

Sha512HalfHasher::Sha512HalfHasher() = default;

Sha512HalfHasher::~Sha512HalfHasher() = default;

bool
Sha512HalfHasher::update(const void* data, size_t len)
{
    return hasher_.update(data, len);
}

Hash256
Sha512HalfHasher::finalize()
{
    // SHA-512 produces 64 bytes, we'll only use the first 32
    std::array<unsigned char, 64> full_hash;
    unsigned int hash_len = 0;

    if (!hasher_.final(full_hash.data(), &hash_len) || hash_len != 64)
    {
        throw std::runtime_error("Sha512HalfHasher: Failed to finalize hash");
    }

    // Create a Hash256 with just the first half of the SHA-512 result
    return Hash256(full_hash.data());
}

}  // namespace catl::crypto