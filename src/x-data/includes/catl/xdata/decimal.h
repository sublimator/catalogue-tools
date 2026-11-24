#pragma once

#include "catl/xdata/codec-error.h"
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

namespace catl::xdata {

/// A decimal number as rippled's partsFromString reads it
/// (src/libxrpl/protocol/STNumber.cpp), the one parser behind issued, native
/// and MPT amounts and Number fields:
///
///   [+-]? ( 0 | [1-9][0-9]* ) ( . [0-9]+ )? ( [eE] [+-]? [0-9]+ )?
///
/// The value is mantissa * 10^exponent, negated when `negative`. As in
/// rippled, the integer and fraction digits read together must fit an
/// unsigned 64-bit integer. Text outside the grammar is an error as a whole:
/// a prefix that happens to parse ("100 and something") is not a number.
struct DecimalParts
{
    bool negative = false;
    uint64_t mantissa = 0;
    int64_t exponent = 0;
};

inline DecimalParts
parse_decimal(
    std::string_view text,
    std::string const& type,
    std::string const& path = {})
{
    auto const fail = [&]() -> EncodeError {
        return EncodeError(
            CodecErrorCode::invalid_value,
            type,
            "'" + std::string(text) + "' is not a number",
            path);
    };
    auto const is_digit = [](char c) { return c >= '0' && c <= '9'; };

    DecimalParts parts;
    size_t pos = 0;
    auto const end = text.size();

    if (pos < end && (text[pos] == '+' || text[pos] == '-'))
    {
        parts.negative = text[pos] == '-';
        ++pos;
    }

    // Integer and fraction digits are one unsigned mantissa.
    auto const push_digit = [&](char c) {
        auto const digit = static_cast<uint64_t>(c - '0');
        if (parts.mantissa >
            (std::numeric_limits<uint64_t>::max() - digit) / 10)
            throw fail();
        parts.mantissa = parts.mantissa * 10 + digit;
    };

    // 0, or a digit string with no leading zero.
    if (pos >= end || !is_digit(text[pos]))
        throw fail();
    if (text[pos] == '0')
    {
        ++pos;
        if (pos < end && is_digit(text[pos]))
            throw fail();
    }
    else
    {
        while (pos < end && is_digit(text[pos]))
            push_digit(text[pos++]);
    }

    if (pos < end && text[pos] == '.')
    {
        ++pos;
        if (pos >= end || !is_digit(text[pos]))
            throw fail();
        while (pos < end && is_digit(text[pos]))
        {
            push_digit(text[pos++]);
            --parts.exponent;
        }
    }

    if (pos < end && (text[pos] == 'e' || text[pos] == 'E'))
    {
        ++pos;
        bool exponent_negative = false;
        if (pos < end && (text[pos] == '+' || text[pos] == '-'))
        {
            exponent_negative = text[pos] == '-';
            ++pos;
        }
        if (pos >= end || !is_digit(text[pos]))
            throw fail();
        int64_t written = 0;
        while (pos < end && is_digit(text[pos]))
        {
            written = written * 10 + (text[pos++] - '0');
            // rippled reads the exponent as an int.
            if (written > std::numeric_limits<int32_t>::max())
                throw fail();
        }
        parts.exponent += exponent_negative ? -written : written;
    }

    if (pos != end)
        throw fail();
    return parts;
}

/// The integer `parts` names, for the amounts rippled keeps as integers
/// (native and MPT). rippled refuses a negative exponent there even when the
/// value is whole ("1.0"), and so does this.
inline int64_t
decimal_to_integer(
    DecimalParts const& parts,
    std::string_view text,
    std::string const& type,
    std::string const& path = {})
{
    auto const fail = [&](char const* why) -> EncodeError {
        return EncodeError(
            CodecErrorCode::out_of_range,
            type,
            "'" + std::string(text) + "' " + why,
            path);
    };
    if (parts.exponent < 0)
        throw fail("is not an integer amount");
    uint64_t value = parts.mantissa;
    auto const limit =
        static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
    for (int64_t i = 0; i < parts.exponent && value != 0; ++i)
    {
        if (value > limit / 10)
            throw fail("is out of range");
        value *= 10;
    }
    if (value > limit)
        throw fail("is out of range");
    auto const magnitude = static_cast<int64_t>(value);
    return parts.negative ? -magnitude : magnitude;
}

}  // namespace catl::xdata
