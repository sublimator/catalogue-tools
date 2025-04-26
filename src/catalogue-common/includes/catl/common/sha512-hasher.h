#pragma once

#include <cstddef>

namespace catl::common {

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

    bool
    update(const void* data, size_t len);
    bool
    final(unsigned char* out, unsigned int* out_len);

private:
    // Throws std::runtime_error if context is not valid
    void
    check_context() const;
    void* ctx_;  // opaque pointer to avoid including openssl headers here
};

}  // namespace catl::common