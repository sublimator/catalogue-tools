#pragma once

#include "catl/xdata/codec-error.h"
#include "catl/xdata/serializer.h"
#include <boost/json.hpp>
#include <cstring>

namespace catl::xdata::codecs {

struct CurrencyCodec
{
    static constexpr size_t fixed_size = 20;

    static size_t
    encoded_size(boost::json::value const&)
    {
        return 20;
    }

    // From raw 20 bytes
    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, std::span<const uint8_t, 20> data)
    {
        s.add_raw(std::span<const uint8_t>{data.data(), 20});
    }

    // From string
    template <ByteSink Sink>
    static void
    encode(
        Serializer<Sink>& s,
        std::string_view currency,
        std::string const& path = {})
    {
        uint8_t buf[20] = {};

        if (currency == "XRP" || currency == "XAH" || currency.empty())
        {
            s.add_raw(std::span<const uint8_t>{buf, 20});
        }
        else if (currency.size() == 40)
        {
            s.add_hex(currency);
        }
        else if (currency.size() == 3)
        {
            buf[12] = static_cast<uint8_t>(currency[0]);
            buf[13] = static_cast<uint8_t>(currency[1]);
            buf[14] = static_cast<uint8_t>(currency[2]);
            s.add_raw(std::span<const uint8_t>{buf, 20});
        }
        else
        {
            throw EncodeError(
                CodecErrorCode::invalid_value,
                "Currency",
                "invalid currency: " + std::string(currency),
                path);
        }
    }

    // From JSON string
    template <ByteSink Sink>
    static void
    encode(
        Serializer<Sink>& s,
        boost::json::value const& v,
        std::string const& path = {})
    {
        if (!v.is_string())
        {
            throw EncodeError(
                CodecErrorCode::invalid_value,
                "Currency",
                "expected string",
                path);
        }
        encode(s, std::string_view(v.as_string()), path);
    }
};

}  // namespace catl::xdata::codecs
