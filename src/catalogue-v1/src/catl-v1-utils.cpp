#include <cstdint>
#include <cstring>
#include <fstream>
#include <vector>

#include <openssl/evp.h>

#include "catl/v1/catl-v1-utils.h"

namespace catl::v1 {

int
get_compression_level(uint16_t version_field)
{
    return (version_field & CATALOGUE_COMPRESS_LEVEL_MASK) >> 8;
}

int
get_catalogue_version(uint16_t version_field)
{
    return version_field & CATALOGUE_VERSION_MASK;
}

bool
verify_hash(const CatlHeader& header, const std::string& filename)
{
    constexpr size_t HASH_OFFSET = offsetof(CatlHeader, hash);
    constexpr size_t HASH_SIZE = sizeof(header.hash);

    std::ifstream file(filename, std::ios::binary);
    if (!file)
    {
        return false;
    }

    Sha512Hasher hasher;
    if (!hasher.update(nullptr, 0))
    {  // Just to check ctx_ is valid
        return false;
    }

    // Hash header up to hash offset
    std::vector<char> buffer(HASH_OFFSET);
    file.read(buffer.data(), buffer.size());
    if (file.gcount() != static_cast<std::streamsize>(buffer.size()))
    {
        return false;
    }
    if (!hasher.update(buffer.data(), buffer.size()))
    {
        return false;
    }

    // Hash 64 zero bytes (the hash field)
    std::array<char, HASH_SIZE> zero_hash = {};
    if (!hasher.update(zero_hash.data(), zero_hash.size()))
    {
        return false;
    }

    // Hash the rest of the file
    file.seekg(HASH_OFFSET + HASH_SIZE, std::ios::beg);
    buffer.resize(64 * 1024);
    while (file)
    {
        file.read(buffer.data(), buffer.size());
        std::streamsize bytes_read = file.gcount();
        if (bytes_read > 0)
        {
            if (!hasher.update(buffer.data(), bytes_read))
            {
                return false;
            }
        }
    }

    // Finalize hash
    std::array<unsigned char, HASH_SIZE> computed_hash;
    unsigned int hash_len = 0;
    if (hasher.final(computed_hash.data(), &hash_len) && hash_len == HASH_SIZE)
    {
        // Compare to header.hash
        return (
            std::memcmp(computed_hash.data(), header.hash.data(), HASH_SIZE) ==
            0);
    }
    return false;
}

}  // namespace catl::v1
