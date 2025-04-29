#pragma once

#include "catl/core/types.h"
#include "catl/crypto/sha512-hasher.h"

namespace catl::crypto {

/**
 * A specialized hasher that computes SHA-512 and returns only the first 256
 * bits. Useful for SHAMap which only needs the first 256 bits of the SHA-512
 * hash.
 */
class Sha512HalfHasher
{
public:
    Sha512HalfHasher();
    ~Sha512HalfHasher();

    // Not copyable or movable
    Sha512HalfHasher(const Sha512HalfHasher&) = delete;
    Sha512HalfHasher&
    operator=(const Sha512HalfHasher&) = delete;

    /**
     * Update the hash with more data.
     * @param data Pointer to data to hash
     * @param len Length of data in bytes
     * @throws std::runtime_error if the update fails
     */
    void
    update(const void* data, size_t len);

    /**
     * Finalize and return the first 256 bits as a Hash256.
     * After calling this method, the hasher is reset and cannot be used
     * anymore.
     * @return Hash256 object containing the first 256 bits of the SHA-512 hash
     */
    Hash256
    finalize();

private:
    Sha512Hasher hasher_;
};

}  // namespace catl::crypto