#pragma once

#include "catl/xdata/codec-error.h"
#include "catl/xdata/decimal.h"
#include "catl/xdata/serializer.h"
#include "catl/xdata/types/number.h"
#include <boost/json.hpp>

namespace catl::xdata::codecs {

struct NumberCodec
{
    static constexpr size_t fixed_size = 12;

    // XRPL Number normalization: mantissa in [1e18, 1e19) range
    // (the "large" MantissaRange from rippled's Number class)
    static constexpr uint64_t MIN_MANTISSA =
        1'000'000'000'000'000'000ULL;  // 1e18
    static constexpr uint64_t MAX_MANTISSA =
        9'999'999'999'999'999'999ULL;  // 1e19-1
    // rippled Number::kMinExponent and kMaxExponent
    static constexpr int64_t MIN_EXPONENT = -32768;
    static constexpr int64_t MAX_EXPONENT = 32768;

    static size_t
    encoded_size(boost::json::value const&)
    {
        return 12;
    }

    // Normalize and write mantissa + exponent
    template <ByteSink Sink>
    static void
    encode_normalized(
        Serializer<Sink>& s,
        bool negative,
        uint64_t mantissa,
        int32_t exponent)
    {
        if (mantissa == 0)
        {
            // Zero: mantissa=0, exponent=INT32_MIN (0x80000000)
            s.add_u64(0);
            s.add_u32(static_cast<uint32_t>(INT32_MIN));
            return;
        }

        // Normalize to [1e18, 1e19-1] range (rippled's "large" MantissaRange)
        // then ensure mantissa fits in int64 (divide once more if > INT64_MAX)

        // Grow to at least 1e18
        while (mantissa < MIN_MANTISSA)
        {
            mantissa *= 10;
            --exponent;
        }

        // Shrink to at most 1e19-1
        // Track last dropped digit for rounding
        uint8_t last_dropped = 0;
        while (mantissa > MAX_MANTISSA)
        {
            last_dropped = mantissa % 10;
            mantissa /= 10;
            ++exponent;
        }

        // If mantissa > INT64_MAX, shrink one more (matching rippled's
        // Number::normalize which ensures mantissa fits in int64 for
        // the external mantissa() accessor)
        static constexpr uint64_t MAX_SIGNED = static_cast<uint64_t>(INT64_MAX);
        if (mantissa > MAX_SIGNED)
        {
            last_dropped = mantissa % 10;
            mantissa /= 10;
            ++exponent;
        }

        // Round up if last dropped digit >= 5
        if (last_dropped >= 5)
        {
            ++mantissa;
            // Re-check bounds after rounding
            if (mantissa > MAX_MANTISSA)
            {
                mantissa /= 10;
                ++exponent;
            }
        }

        // Write as signed int64 mantissa + int32 exponent
        int64_t signed_mantissa = negative ? -static_cast<int64_t>(mantissa)
                                           : static_cast<int64_t>(mantissa);
        s.add_u64(static_cast<uint64_t>(signed_mantissa));
        s.add_u32(static_cast<uint32_t>(exponent));
    }

    // From mantissa + exponent (already split)
    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, int64_t mantissa, int32_t exponent)
    {
        bool negative = mantissa < 0;
        uint64_t abs_mantissa = negative ? static_cast<uint64_t>(-mantissa)
                                         : static_cast<uint64_t>(mantissa);
        encode_normalized(s, negative, abs_mantissa, exponent);
    }

    /// Write mantissa * 10^exponent only when the wire form holds it
    /// exactly. rippled's numberFromJson refuses a value its Number would
    /// have to round ("number cannot be represented"), and so does this:
    /// a decimal that is quietly rounded is not the number the JSON showed.
    template <ByteSink Sink>
    static void
    encode_exact(
        Serializer<Sink>& s,
        bool negative,
        uint64_t mantissa,
        int64_t exponent,
        std::string const& path = {})
    {
        auto const refuse = [&]() -> EncodeError {
            return EncodeError(
                CodecErrorCode::out_of_range,
                "Number",
                "number cannot be represented",
                path);
        };

        if (mantissa == 0)
        {
            // Zero: mantissa=0, exponent=INT32_MIN (0x80000000)
            s.add_u64(0);
            s.add_u32(static_cast<uint32_t>(INT32_MIN));
            return;
        }

        // Number::normalize: into [1e18, 1e19 - 1], exponent in range.
        while (mantissa < MIN_MANTISSA && exponent > MIN_EXPONENT)
        {
            mantissa *= 10;
            --exponent;
        }
        // Too small: rippled's Number becomes zero.
        if (mantissa < MIN_MANTISSA || exponent < MIN_EXPONENT)
            throw refuse();
        while (mantissa > MAX_MANTISSA)
        {
            if (mantissa % 10 != 0)
                throw refuse();
            mantissa /= 10;
            ++exponent;
        }
        if (exponent > MAX_EXPONENT)
            throw refuse();

        // The wire mantissa is signed. One above 2^63 - 1 is written a
        // digit shorter, which is exact only when that digit is zero.
        if (mantissa > static_cast<uint64_t>(INT64_MAX))
        {
            if (mantissa % 10 != 0)
                throw refuse();
            mantissa /= 10;
            ++exponent;
        }

        int64_t signed_mantissa = negative ? -static_cast<int64_t>(mantissa)
                                           : static_cast<int64_t>(mantissa);
        s.add_u64(static_cast<uint64_t>(signed_mantissa));
        s.add_u32(static_cast<uint32_t>(static_cast<int32_t>(exponent)));
    }

    // From JSON: an integer, or a decimal string in rippled's number
    // grammar (decimal.h), as rippled's numberFromJson.
    template <ByteSink Sink>
    static void
    encode(
        Serializer<Sink>& s,
        boost::json::value const& v,
        std::string const& path = {})
    {
        if (v.is_int64())
        {
            int64_t const val = v.as_int64();
            bool const negative = val < 0;
            // Negate as unsigned: -INT64_MIN does not fit int64.
            uint64_t const magnitude = negative ? 0 - static_cast<uint64_t>(val)
                                                : static_cast<uint64_t>(val);
            encode_exact(s, negative, magnitude, 0, path);
            return;
        }
        if (v.is_uint64())
        {
            encode_exact(s, false, v.as_uint64(), 0, path);
            return;
        }
        if (!v.is_string())
        {
            throw EncodeError(
                CodecErrorCode::invalid_value,
                "Number",
                "expected a decimal string or an integer",
                path);
        }

        auto const parts =
            parse_decimal(std::string_view(v.as_string()), "Number", path);
        encode_exact(s, parts.negative, parts.mantissa, parts.exponent, path);
    }

    static boost::json::value
    decode(Slice const& data)
    {
        STNumber number = parse_number(data);
        return boost::json::string(number.to_string());
    }
};

}  // namespace catl::xdata::codecs
