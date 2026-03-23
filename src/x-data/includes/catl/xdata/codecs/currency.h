#pragma once

#include "catl/xdata/codec-error.h"
#include "catl/xdata/hex.h"
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

    // Decode 20-byte currency to JSON string
    static boost::json::value
    decode(Slice const& data)
    {
        if (data.size() != 20)
            return boost::json::string(hex_encode(data));

        // All zeros → native
        bool all_zeros = true;
        for (size_t i = 0; i < 20; ++i)
        {
            if (data.data()[i] != 0)
            {
                all_zeros = false;
                break;
            }
        }
        if (all_zeros)
            return boost::json::string("XRP");

        // Standard 3-char code: 12 zeros + 3 chars + 5 zeros
        bool standard = true;
        for (size_t i = 0; i < 12; ++i)
        {
            if (data.data()[i] != 0)
            {
                standard = false;
                break;
            }
        }
        if (standard)
        {
            for (size_t i = 15; i < 20; ++i)
            {
                if (data.data()[i] != 0)
                {
                    standard = false;
                    break;
                }
            }
        }
        if (standard)
        {
            std::string code;
            for (size_t i = 12; i < 15; ++i)
            {
                if (data.data()[i] != 0)
                    code += static_cast<char>(data.data()[i]);
            }
            return boost::json::string(code);
        }

        return boost::json::string(hex_encode(data));
    }
};

}  // namespace catl::xdata::codecs
