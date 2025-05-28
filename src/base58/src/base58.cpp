#include "catl/base58/base58.h"
#include <algorithm>
#include <cstring>
#include <openssl/sha.h>

namespace catl::base58 {

base58::base58(std::string_view alphabet)
{
    if (alphabet.size() != 58)
    {
        throw std::invalid_argument(
            "Base58 alphabet must be exactly 58 characters");
    }

    std::fill(alphabet_.begin(), alphabet_.end(), 0);
    std::fill(indexes_.begin(), indexes_.end(), -1);

    for (size_t i = 0; i < alphabet.size(); ++i)
    {
        alphabet_[i] = alphabet[i];
        if (static_cast<unsigned char>(alphabet[i]) < 128)
        {
            indexes_[static_cast<unsigned char>(alphabet[i])] =
                static_cast<int8_t>(i);
        }
    }

    encoded_zero_ = alphabet[0];
}

std::string
base58::encode(const uint8_t* input, size_t length) const
{
    if (length == 0)
    {
        return "";
    }

    // Count leading zeros
    size_t zeros = 0;
    while (zeros < length && input[zeros] == 0)
    {
        ++zeros;
    }

    // Allocate enough space in big-endian base58 representation
    size_t size = length * 138 / 100 + 1;  // log(256) / log(58), rounded up
    std::vector<uint8_t> b58(size);

    // Process the bytes
    for (size_t i = zeros; i < length; ++i)
    {
        uint32_t carry = input[i];
        for (auto it = b58.rbegin(); it != b58.rend(); ++it)
        {
            carry += 256 * (*it);
            *it = carry % 58;
            carry /= 58;
        }
    }

    // Skip leading zeros in base58 result
    auto it = b58.begin();
    while (it != b58.end() && *it == 0)
    {
        ++it;
    }

    // Translate the result into a string
    std::string str;
    str.reserve(zeros + (b58.end() - it));
    str.append(zeros, encoded_zero_);
    for (; it != b58.end(); ++it)
    {
        str.push_back(alphabet_[*it]);
    }

    return str;
}

std::string
base58::encode(const std::vector<uint8_t>& data) const
{
    return encode(data.data(), data.size());
}

std::optional<std::vector<uint8_t>>
base58::decode(std::string_view input) const
{
    if (input.empty())
    {
        return std::vector<uint8_t>{};
    }

    // Count leading zeros (encoded as first char of alphabet)
    size_t zeros = 0;
    while (zeros < input.size() && input[zeros] == encoded_zero_)
    {
        ++zeros;
    }

    // Allocate enough space
    size_t size =
        input.size() * 733 / 1000 + 1;  // log(58) / log(256), rounded up
    std::vector<uint8_t> b256(size);

    // Process the characters
    for (size_t i = zeros; i < input.size(); ++i)
    {
        // Check for invalid characters
        if (static_cast<unsigned char>(input[i]) >= 128 ||
            indexes_[static_cast<unsigned char>(input[i])] == -1)
        {
            return std::nullopt;
        }

        uint32_t carry = indexes_[static_cast<unsigned char>(input[i])];
        for (auto it = b256.rbegin(); it != b256.rend(); ++it)
        {
            carry += 58 * (*it);
            *it = carry % 256;
            carry /= 256;
        }
    }

    // Skip leading zeros in b256
    auto it = b256.begin();
    while (it != b256.end() && *it == 0)
    {
        ++it;
    }

    // Copy result
    std::vector<uint8_t> result;
    result.reserve(zeros + (b256.end() - it));
    result.insert(result.end(), zeros, 0);
    result.insert(result.end(), it, b256.end());

    return result;
}

std::string
base58::encode_checked(const uint8_t* data, size_t len) const
{
    // Calculate checksum
    uint8_t hash1[SHA256_DIGEST_LENGTH];
    uint8_t hash2[SHA256_DIGEST_LENGTH];
    SHA256(data, len, hash1);
    SHA256(hash1, SHA256_DIGEST_LENGTH, hash2);

    // Concatenate data and checksum
    std::vector<uint8_t> with_checksum(len + 4);
    std::memcpy(with_checksum.data(), data, len);
    std::memcpy(with_checksum.data() + len, hash2, 4);

    return encode(with_checksum);
}

std::string
base58::encode_checked(const std::vector<uint8_t>& data) const
{
    return encode_checked(data.data(), data.size());
}

std::optional<std::vector<uint8_t>>
base58::decode_checked(std::string_view encoded) const
{
    auto decoded = decode(encoded);
    if (!decoded || decoded->size() < 4)
    {
        return std::nullopt;
    }

    // Split payload and checksum
    size_t payload_len = decoded->size() - 4;

    // Verify checksum
    uint8_t hash1[SHA256_DIGEST_LENGTH];
    uint8_t hash2[SHA256_DIGEST_LENGTH];
    SHA256(decoded->data(), payload_len, hash1);
    SHA256(hash1, SHA256_DIGEST_LENGTH, hash2);

    if (std::memcmp(hash2, decoded->data() + payload_len, 4) != 0)
    {
        return std::nullopt;
    }

    // Return payload only
    decoded->resize(payload_len);
    return decoded;
}

std::string
base58::encode_versioned(const uint8_t* data, size_t len, const version& ver)
    const
{
    if (len != ver.expected_length)
    {
        throw std::invalid_argument(
            "Data length does not match version expected length");
    }

    // Concatenate version and data
    std::vector<uint8_t> versioned;
    versioned.reserve(ver.bytes.size() + len);
    versioned.insert(versioned.end(), ver.bytes.begin(), ver.bytes.end());
    versioned.insert(versioned.end(), data, data + len);

    return encode_checked(versioned);
}

std::string
base58::encode_versioned(const std::vector<uint8_t>& data, const version& ver)
    const
{
    return encode_versioned(data.data(), data.size(), ver);
}

// Helper function for decode_versioned
std::optional<decoded>
decode_versioned_impl(
    const base58& codec,
    std::string_view encoded,
    const version& ver)
{
    auto data = codec.decode_checked(encoded);
    if (!data)
    {
        return std::nullopt;
    }

    // Check version prefix
    if (data->size() < ver.bytes.size() ||
        !std::equal(ver.bytes.begin(), ver.bytes.end(), data->begin()))
    {
        return std::nullopt;
    }

    // Check payload length
    size_t payload_size = data->size() - ver.bytes.size();
    if (payload_size != ver.expected_length)
    {
        return std::nullopt;
    }

    // Extract payload
    std::vector<uint8_t> payload(data->begin() + ver.bytes.size(), data->end());
    return decoded{ver.name, std::move(payload)};
}

// Helper to implement variadic template
namespace detail {
template <typename First, typename... Rest>
std::optional<decoded>
try_decode_versions(
    const base58& codec,
    std::string_view encoded,
    const First& first,
    const Rest&... rest)
{
    auto result = decode_versioned_impl(codec, encoded, first);
    if (result)
    {
        return result;
    }
    if constexpr (sizeof...(rest) > 0)
    {
        return try_decode_versions(codec, encoded, rest...);
    }
    return std::nullopt;
}
}  // namespace detail

// Template implementation for multiple versions
template <typename... Versions>
std::optional<decoded>
base58::decode_versioned(std::string_view encoded, const Versions&... versions)
    const
{
    return detail::try_decode_versions(*this, encoded, versions...);
}

// Explicit instantiations for common use cases
template std::optional<decoded>
base58::decode_versioned<version>(std::string_view encoded, const version& v1)
    const;
template std::optional<decoded>
base58::decode_versioned<version, version>(
    std::string_view encoded,
    const version& v1,
    const version& v2) const;

}  // namespace catl::base58
