#pragma once

#include <catl/core/types.h>

#include <cstdint>
#include <iomanip>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

namespace xprv {

/// Parse 64-char hex string to Hash256.
inline Hash256
hash_from_hex(std::string const& hex)
{
    Hash256 result;
    if (hex.size() != 64)
        throw std::runtime_error(
            "hash_from_hex: expected 64 hex chars, got " +
            std::to_string(hex.size()));
    for (size_t i = 0; i < 32; ++i)
    {
        unsigned int byte;
        std::sscanf(hex.c_str() + i * 2, "%2x", &byte);
        result.data()[i] = static_cast<uint8_t>(byte);
    }
    return result;
}

/// Uppercase hex of a Hash256.
inline std::string
upper_hex(Hash256 const& h)
{
    std::ostringstream oss;
    for (size_t i = 0; i < 32; ++i)
        oss << std::hex << std::uppercase << std::setfill('0') << std::setw(2)
            << static_cast<int>(h.data()[i]);
    return oss.str();
}

/// Bytes to lowercase hex string.
inline std::string
bytes_hex(std::vector<uint8_t> const& v)
{
    std::string hex;
    Slice s(v.data(), v.size());
    slice_hex(s, hex);
    return hex;
}

/// Decode hex string to bytes.
inline std::vector<uint8_t>
from_hex(std::string_view hex)
{
    std::vector<uint8_t> result;
    result.reserve(hex.size() / 2);
    for (size_t i = 0; i + 1 < hex.size(); i += 2)
    {
        unsigned int byte;
        std::sscanf(hex.data() + i, "%2x", &byte);
        result.push_back(static_cast<uint8_t>(byte));
    }
    return result;
}

}  // namespace xprv
