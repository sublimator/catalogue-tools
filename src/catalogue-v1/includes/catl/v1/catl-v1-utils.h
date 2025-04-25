#pragma once

#include "catl/v1/catl-v1-structs.h"
#include <string>

namespace catl::v1 {

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

int
get_compression_level(uint16_t version_field);

int
get_catalogue_version(uint16_t version_field);

// Verifies the hash in the header matches the file content.
// Returns true if the hash matches, false otherwise.
bool
verify_hash(const CatlHeader& header, const std::string& filename);

// Returns true if the version field indicates compression
inline bool
is_compressed(uint16_t version_field)
{
    return get_compression_level(version_field) > 0;
}

// Returns a version field with the given version and compression level
inline uint16_t
make_catalogue_version_field(
    uint8_t catalogue_version,
    uint8_t compression_level = 0)
{
    // Ensure compression level is within valid range (0-9)
    if (compression_level > 9)
        compression_level = 9;
    uint16_t result = catalogue_version & CATALOGUE_VERSION_MASK;
    result |= (compression_level << 8);  // Store level in bits 8-11
    return result;
}

}  // namespace catl::v1