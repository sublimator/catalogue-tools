#pragma once

#include <catl/core/types.h>

#include <cstdint>
#include <iomanip>
#include <stdexcept>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

namespace xprv {

inline uint8_t
hex_byte(char high, char low)
{
    auto const nibble = [](char value) -> int {
        if (value >= '0' && value <= '9')
            return value - '0';
        if (value >= 'a' && value <= 'f')
            return value - 'a' + 10;
        if (value >= 'A' && value <= 'F')
            return value - 'A' + 10;
        return -1;
    };

    auto const hi = nibble(high);
    auto const lo = nibble(low);
    if (hi < 0 || lo < 0)
        throw std::runtime_error("invalid hexadecimal digit");
    return static_cast<uint8_t>((hi << 4) | lo);
}

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
        result.data()[i] = hex_byte(hex[i * 2], hex[i * 2 + 1]);
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
    if (hex.size() % 2 != 0)
        throw std::runtime_error("from_hex: expected an even number of digits");

    std::vector<uint8_t> result;
    result.reserve(hex.size() / 2);
    for (size_t i = 0; i < hex.size(); i += 2)
        result.push_back(hex_byte(hex[i], hex[i + 1]));
    return result;
}

}  // namespace xprv
