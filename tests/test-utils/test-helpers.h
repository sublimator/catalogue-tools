#pragma once

#include <array>
#include <stdexcept>
#include <string>
#include <vector>

namespace test_helpers {

// Helper to create a Key storage array from hex string
inline std::array<uint8_t, 32>
key_from_hex(const std::string& hex)
{
    if (hex.size() != 64)
    {
        throw std::invalid_argument(
            "Key hex string must be exactly 64 characters");
    }

    std::array<uint8_t, 32> storage;
    for (size_t i = 0; i < 32; ++i)
    {
        std::string byte = hex.substr(i * 2, 2);
        storage[i] = static_cast<uint8_t>(std::stoul(byte, nullptr, 16));
    }

    return storage;
}

// Helper to create data storage from string
inline std::vector<uint8_t>
data_from_string(const std::string& str)
{
    return std::vector<uint8_t>(str.begin(), str.end());
}

}  // namespace test_helpers