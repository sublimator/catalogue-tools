#pragma once

#include "catl/xdata/serializer.h"
#include <boost/json.hpp>
#include <charconv>

namespace catl::xdata::codecs {

struct NumberCodec
{
    static constexpr size_t fixed_size = 12;

    static size_t
    encoded_size(boost::json::value const&)
    {
        return 12;
    }

    // From mantissa + exponent
    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, int64_t mantissa, int32_t exponent)
    {
        s.add_number(mantissa, exponent);
    }

    // From JSON string (decimal or scientific notation)
    // TODO: proper decimal → mantissa/exponent parsing
    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, boost::json::value const& v)
    {
        auto sv = std::string_view(v.as_string());

        // Check for scientific notation (e.g. "1234e-15")
        auto e_pos = sv.find('e');
        if (e_pos != std::string_view::npos)
        {
            int64_t mantissa = 0;
            int32_t exponent = 0;
            std::from_chars(sv.data(), sv.data() + e_pos, mantissa);
            std::from_chars(
                sv.data() + e_pos + 1, sv.data() + sv.size(), exponent);
            s.add_number(mantissa, exponent);
            return;
        }

        // Decimal notation: parse mantissa and compute exponent
        bool negative = false;
        std::string_view num = sv;
        if (num.front() == '-')
        {
            negative = true;
            num.remove_prefix(1);
        }

        int64_t mantissa = 0;
        int32_t exponent = 0;
        bool seen_dot = false;
        int decimals = 0;

        for (char c : num)
        {
            if (c == '.')
            {
                seen_dot = true;
                continue;
            }
            if (c < '0' || c > '9')
                break;
            mantissa = mantissa * 10 + (c - '0');
            if (seen_dot)
                ++decimals;
        }

        exponent = -decimals;
        if (negative)
            mantissa = -mantissa;

        s.add_number(mantissa, exponent);
    }
};

}  // namespace catl::xdata::codecs
