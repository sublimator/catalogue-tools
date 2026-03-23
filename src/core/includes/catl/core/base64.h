#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace catl {

/// Decode base64 string to raw bytes.
std::vector<uint8_t>
base64_decode(std::string_view input);

/// Encode raw bytes to base64 string.
std::string
base64_encode(std::span<const uint8_t> input);

}  // namespace catl
