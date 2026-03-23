#pragma once

#include <charconv>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>

namespace catl::xdata {

enum class CodecErrorCode {
    invalid_value,  // wrong JSON type or unparseable value
    missing_field,  // required sub-field absent (e.g. IOU without issuer)
    out_of_range,   // value exceeds type bounds (e.g. negative native amount)
    invalid_encoding,  // bad hex string, bad base58, wrong length
    unknown_field,     // field name not found in Protocol
    unknown_enum,      // string enum value not found (e.g. bad TransactionType)
    malformed_data,    // binary structure corrupt (decode side)
};

class CodecError : public std::runtime_error
{
public:
    CodecErrorCode code;
    std::string type;
    std::string path;

protected:
    CodecError(
        std::string prefix,
        CodecErrorCode code_,
        std::string type_,
        std::string msg,
        std::string path_)
        : std::runtime_error(
              prefix + " " + type_ + ": " + msg +
              (path_.empty() ? "" : " at " + path_))
        , code(code_)
        , type(std::move(type_))
        , path(std::move(path_))
    {
    }
};

class EncodeError : public CodecError
{
public:
    EncodeError(
        CodecErrorCode code_,
        std::string type_,
        std::string msg,
        std::string path_ = {})
        : CodecError(
              "encode",
              code_,
              std::move(type_),
              std::move(msg),
              std::move(path_))
    {
    }
};

class DecodeError : public CodecError
{
public:
    DecodeError(
        CodecErrorCode code_,
        std::string type_,
        std::string msg,
        std::string path_ = {})
        : CodecError(
              "decode",
              code_,
              std::move(type_),
              std::move(msg),
              std::move(path_))
    {
    }
};

// ---------------------------------------------------------------------------
// Parse helpers — from_chars with EncodeError on failure
// ---------------------------------------------------------------------------

inline int64_t
parse_int64(
    std::string_view sv,
    std::string const& type,
    std::string const& path = {})
{
    int64_t val = 0;
    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), val);
    if (ec != std::errc{})
    {
        throw EncodeError(
            CodecErrorCode::invalid_value,
            type,
            "invalid integer: " + std::string(sv),
            path);
    }
    return val;
}

inline uint64_t
parse_uint64(
    std::string_view sv,
    std::string const& type,
    std::string const& path = {})
{
    uint64_t val = 0;
    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), val);
    if (ec != std::errc{})
    {
        throw EncodeError(
            CodecErrorCode::invalid_value,
            type,
            "invalid integer: " + std::string(sv),
            path);
    }
    return val;
}

inline uint64_t
parse_hex_uint64(
    std::string_view sv,
    std::string const& type,
    std::string const& path = {})
{
    uint64_t val = 0;
    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), val, 16);
    if (ec != std::errc{})
    {
        throw EncodeError(
            CodecErrorCode::invalid_value,
            type,
            "invalid hex integer: " + std::string(sv),
            path);
    }
    return val;
}

// Validate hex string length. Throws EncodeError if wrong.
inline void
require_hex_length(
    std::string_view hex,
    size_t expected_chars,
    std::string const& type,
    std::string const& path = {})
{
    if (hex.size() != expected_chars)
    {
        throw EncodeError(
            CodecErrorCode::invalid_encoding,
            type,
            "expected " + std::to_string(expected_chars) + " hex chars, got " +
                std::to_string(hex.size()),
            path);
    }
}

}  // namespace catl::xdata
