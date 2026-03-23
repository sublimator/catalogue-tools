#pragma once

#include "catl/base58/base58.h"
#include "catl/xdata/codec-error.h"
#include "catl/xdata/serializer.h"
#include <boost/json.hpp>

namespace catl::xdata::codecs {

struct AccountIDCodec
{
    static constexpr size_t fixed_size = 20;

    static constexpr std::string_view ZERO_ACCOUNT_B58 =
        "rrrrrrrrrrrrrrrrrrrrrhoLvTp";

    static size_t
    encoded_size(boost::json::value const& v)
    {
        if (v.is_string() && v.as_string() == ZERO_ACCOUNT_B58)
            return 0;
        return 20;
    }

    // From raw 20 bytes
    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, std::span<const uint8_t, 20> data)
    {
        s.add_raw(std::span<const uint8_t>{data.data(), 20});
    }

    // Check if 20 bytes are all zero (default/absent account)
    static bool
    is_zero_account(const uint8_t* data, size_t size)
    {
        if (size != 20)
            return false;
        for (size_t i = 0; i < 20; ++i)
        {
            if (data[i] != 0)
                return false;
        }
        return true;
    }

    // From base58 string.
    // Zero account (rrrrrrrrrrrrrrrrrrrrrhoLvTp) writes nothing — the caller
    // (STObjectCodec) writes VL(0). Non-zero accounts write 20 bytes.
    template <ByteSink Sink>
    static void
    encode(
        Serializer<Sink>& s,
        std::string_view base58_addr,
        std::string const& path = {})
    {
        // Zero account → write nothing (VL prefix will be 0)
        if (base58_addr == ZERO_ACCOUNT_B58)
            return;

        auto decoded = base58::decode_account_id(base58_addr);
        if (!decoded || decoded->size() != 20)
        {
            throw EncodeError(
                CodecErrorCode::invalid_encoding,
                "AccountID",
                "invalid base58 address: " + std::string(base58_addr),
                path);
        }
        s.add_raw(std::span<const uint8_t>{decoded->data(), decoded->size()});
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

    static boost::json::value
    decode(Slice const& data)
    {
        // Empty VL = zero/default account (e.g. pseudo-transactions)
        if (data.empty())
        {
            static const uint8_t zeros[20] = {};
            return boost::json::string(
                base58::encode_account_id(zeros, 20));
        }
        return boost::json::string(
            base58::encode_account_id(data.data(), data.size()));
    }
};

}  // namespace catl::xdata::codecs
