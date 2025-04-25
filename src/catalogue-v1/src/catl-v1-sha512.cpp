#include <openssl/evp.h>

#include "catl/v1/catl-v1-utils.h"
using namespace catl::v1;

// RAII wrapper for OpenSSL SHA-512 EVP API
Sha512Hasher::Sha512Hasher() : ctx_(nullptr)
{
    ctx_ = EVP_MD_CTX_new();
    if (!ctx_)
    {
        throw std::runtime_error("Sha512Hasher: EVP_MD_CTX_new() failed");
    }
    if (EVP_DigestInit_ex(
            static_cast<EVP_MD_CTX*>(ctx_), EVP_sha512(), nullptr) != 1)
    {
        EVP_MD_CTX_free(static_cast<EVP_MD_CTX*>(ctx_));
        throw std::runtime_error("Sha512Hasher: EVP_DigestInit_ex() failed");
    }
}

Sha512Hasher::~Sha512Hasher()
{
    if (ctx_)
    {
        EVP_MD_CTX_free(static_cast<EVP_MD_CTX*>(ctx_));
    }
}

void
Sha512Hasher::check_context() const
{
    if (!ctx_)
    {
        throw std::runtime_error("Sha512Hasher: context is not valid");
    }
}

bool
Sha512Hasher::update(const void* data, size_t len)
{
    check_context();
    return EVP_DigestUpdate(static_cast<EVP_MD_CTX*>(ctx_), data, len) == 1;
}

bool
Sha512Hasher::final(unsigned char* out, unsigned int* out_len)
{
    check_context();
    bool result =
        EVP_DigestFinal_ex(static_cast<EVP_MD_CTX*>(ctx_), out, out_len) == 1;
    // Invalidate context after final
    EVP_MD_CTX_free(static_cast<EVP_MD_CTX*>(ctx_));
    ctx_ = nullptr;
    return result;
}
