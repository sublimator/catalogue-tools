#pragma once

#include "catl/base58/base58.h"
#include "catl/xdata/codec-error.h"
#include "catl/xdata/serializer.h"
#include <boost/json.hpp>

namespace catl::xdata::codecs {

struct AccountIDCodec
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

    // From base58 string
    template <ByteSink Sink>
    static void
    encode(
        Serializer<Sink>& s,
        std::string_view base58_addr,
        std::string const& path = {})
    {
        auto decoded = base58::decode_account_id(base58_addr);
        if (!decoded || decoded->size() != 20)
        {
            throw EncodeError(
                CodecErrorCode::invalid_encoding,
                "AccountID",
                "invalid base58 address: " + std::string(base58_addr),
                path);
        }
        s.add_raw(
            std::span<const uint8_t>{decoded->data(), decoded->size()});
    }

    // From JSON (base58 string)
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
                "AccountID",
                "expected string",
                path);
        }
        encode(s, std::string_view(v.as_string()), path);
    }
};

}  // namespace catl::xdata::codecs
