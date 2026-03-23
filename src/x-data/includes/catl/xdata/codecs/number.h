#pragma once

#include "catl/xdata/codec-error.h"
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

    // From JSON string (decimal or scientific notation)
    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, boost::json::value const& v)
    {
        if (v.is_int64() || v.is_uint64())
        {
            int64_t val = v.is_int64() ? v.as_int64()
                                       : static_cast<int64_t>(v.as_uint64());
            encode(s, val, 0);
            return;
        }

        auto sv = std::string_view(v.as_string());

        bool negative = false;
        if (!sv.empty() && sv.front() == '-')
        {
            negative = true;
            sv.remove_prefix(1);
        }
        else if (!sv.empty() && sv.front() == '+')
        {
            sv.remove_prefix(1);
        }

        // Parse mantissa digits and decimal point
        uint64_t mantissa = 0;
        int32_t exponent = 0;
        bool seen_dot = false;
        int decimals = 0;
        bool overflow = false;

        size_t i = 0;
        for (; i < sv.size(); ++i)
        {
            char c = sv[i];
            if (c == '.')
            {
                seen_dot = true;
                continue;
            }
            if (c == 'e' || c == 'E')
                break;
            if (c < '0' || c > '9')
                break;

            if (!overflow)
            {
                uint64_t next = mantissa * 10 + static_cast<uint64_t>(c - '0');
                if (next / 10 != mantissa || next < mantissa)
                {
                    // Overflow — round and track remaining digits
                    overflow = true;
                    if (c >= '5')
                        ++mantissa;
                    if (!seen_dot)
                        ++exponent;
                    continue;
                }
                mantissa = next;
                if (seen_dot)
                    ++decimals;
            }
            else
            {
                if (!seen_dot)
                    ++exponent;
            }
        }
        exponent -= decimals;

        // Parse optional exponent part
        if (i < sv.size() && (sv[i] == 'e' || sv[i] == 'E'))
        {
            ++i;
            auto exp_sv = sv.substr(i);
            int32_t exp_val =
                static_cast<int32_t>(parse_int64(exp_sv, "Number"));
            exponent += exp_val;
        }

        encode_normalized(s, negative, mantissa, exponent);
    }

    static boost::json::value
    decode(Slice const& data)
    {
        STNumber number = parse_number(data);
        return boost::json::string(number.to_string());
    }
};

}  // namespace catl::xdata::codecs
