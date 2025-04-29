#include "catl/crypto/sha512-hasher.h"
#include <openssl/evp.h>
#include <stdexcept>

namespace catl::crypto {

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
        cleanup_and_throw("Sha512Hasher: EVP_DigestInit_ex() failed");
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

void
Sha512Hasher::cleanup_and_throw(const char* msg)
{
    EVP_MD_CTX_free(static_cast<EVP_MD_CTX*>(ctx_));
    ctx_ = nullptr;
    throw std::runtime_error(msg);
}

void
Sha512Hasher::update(const void* data, size_t len)
{
    check_context();
    if (EVP_DigestUpdate(static_cast<EVP_MD_CTX*>(ctx_), data, len) != 1)
    {
        cleanup_and_throw("Sha512Hasher: EVP_DigestUpdate failed");
    }
}

void
Sha512Hasher::final(unsigned char* out, unsigned int* out_len)
{
    check_context();
    if (EVP_DigestFinal_ex(static_cast<EVP_MD_CTX*>(ctx_), out, out_len) != 1)
    {
        cleanup_and_throw("Sha512Hasher: EVP_DigestFinal_ex failed");
    }
    EVP_MD_CTX_free(static_cast<EVP_MD_CTX*>(ctx_));
    ctx_ = nullptr;
}

}  // namespace catl::crypto
