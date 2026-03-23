#pragma once

#include <cstdint>
#include <span>
#include <stdexcept>
#include <string_view>

namespace catl::xdata {

// Decode a single hex character to its 4-bit value.
// Returns 0xFF on invalid input.
inline constexpr uint8_t
hex_nibble(char c)
{
    if (c >= '0' && c <= '9')
        return static_cast<uint8_t>(c - '0');
    if (c >= 'a' && c <= 'f')
        return static_cast<uint8_t>(c - 'a' + 10);
    if (c >= 'A' && c <= 'F')
        return static_cast<uint8_t>(c - 'A' + 10);
    return 0xFF;
}

// Decode two hex characters to one byte.
inline constexpr uint8_t
hex_byte(char hi, char lo)
{
    return static_cast<uint8_t>((hex_nibble(hi) << 4) | hex_nibble(lo));
}

// Decode hex string into a fixed-size output buffer.
// Throws if hex.size() != N*2.
template <size_t N>
void
hex_decode(std::string_view hex, uint8_t (&out)[N])
{
    if (hex.size() != N * 2)
    {
        throw std::runtime_error(
            "hex_decode: expected " + std::to_string(N * 2) +
            " hex chars, got " + std::to_string(hex.size()));
    }
    for (size_t i = 0; i < N; ++i)
    {
        out[i] = hex_byte(hex[i * 2], hex[i * 2 + 1]);
    }
}

// Decode hex string into a span. Throws if sizes don't match.
inline void
hex_decode(std::string_view hex, std::span<uint8_t> out)
{
    if (hex.size() != out.size() * 2)
    {
        throw std::runtime_error(
            "hex_decode: expected " + std::to_string(out.size() * 2) +
            " hex chars, got " + std::to_string(hex.size()));
    }
    for (size_t i = 0; i < out.size(); ++i)
    {
        out[i] = hex_byte(hex[i * 2], hex[i * 2 + 1]);
    }
}

// Stream-decode hex directly into a ByteSink. Zero intermediate allocation.
template <typename Sink>
    requires requires(Sink& s, uint8_t b) { s.add(b); }
void
hex_decode(std::string_view hex, Sink& sink)
{
    for (size_t i = 0; i + 1 < hex.size(); i += 2)
    {
        sink.add(hex_byte(hex[i], hex[i + 1]));
    }
}

// Returns decoded byte count for a hex string (just hex.size() / 2).
inline constexpr size_t
hex_decoded_size(std::string_view hex)
{
    return hex.size() / 2;
}

// Encode bytes to uppercase hex string.
inline std::string
hex_encode(const uint8_t* data, size_t size)
{
    static const char hex_chars[] = "0123456789ABCDEF";
    std::string result;
    result.reserve(size * 2);
    for (size_t i = 0; i < size; ++i)
    {
        result.push_back(hex_chars[data[i] >> 4]);
        result.push_back(hex_chars[data[i] & 0xF]);
    }
    return result;
}

inline std::string
hex_encode(Slice const& s)
{
    return hex_encode(s.data(), s.size());
}

// Check if data is printable ASCII text.
inline bool
is_printable_text(const uint8_t* data, size_t size)
{
    if (size == 0)
        return false;
    for (size_t i = 0; i < size; ++i)
    {
        if (data[i] < 32 || data[i] > 126)
        {
            if (data[i] != '\n' && data[i] != '\r' && data[i] != '\t')
                return false;
        }
    }
    return true;
}

inline bool
is_printable_text(Slice const& s)
{
    return is_printable_text(s.data(), s.size());
}

}  // namespace catl::xdata
