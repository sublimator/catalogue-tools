#pragma once

#include <cstddef>

namespace catl::crypto {

// RAII wrapper for OpenSSL SHA-512 EVP API
class Sha512Hasher
{
public:
    Sha512Hasher();
    ~Sha512Hasher();

    // Not copyable or movable
    Sha512Hasher(const Sha512Hasher&) = delete;
    Sha512Hasher&
    operator=(const Sha512Hasher&) = delete;

    /**
     * Update the hash with more data.
     * @param data Pointer to data to hash
     * @param len Length of data in bytes
     * @throws std::runtime_error if the update fails
     */
    void
    update(const void* data, size_t len);

    /**
     * Finalize the hash and write result to buffer.
     * @param out Output buffer for the hash
     * @param out_len Pointer to variable that will receive hash length
     * @throws std::runtime_error if finalization fails
     */
    void
    final(unsigned char* out, unsigned int* out_len);

private:
    // Throws std::runtime_error if context is not valid
    void
    check_context() const;

    void
    cleanup_and_throw(const char* msg);

    void* ctx_;  // opaque pointer to avoid including openssl headers here
};

}  // namespace catl::crypto