#pragma once

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace catl {

/// Decode base64 string to raw bytes. Lenient: every byte outside the
/// alphabet is skipped, padding included, and neither the length nor the
/// trailing bits are checked. Use base64_decode_strict where two spellings
/// of the same bytes must not both be accepted.
std::vector<uint8_t>
base64_decode(std::string_view input);

/// Decode canonical base64 (RFC 4648 section 4): the standard alphabet,
/// padded to a multiple of four characters, zero pad bits, and nothing else.
/// Returns nullopt for any text that base64_encode would not have written.
std::optional<std::vector<uint8_t>>
base64_decode_strict(std::string_view input);

/// Encode raw bytes to base64 string.
std::string
base64_encode(std::span<const uint8_t> input);

}  // namespace catl
