#pragma once

#include "catl/xdata/codec-error.h"
#include "catl/xdata/serializer.h"
#include <boost/json.hpp>
#include <string>

namespace catl::xdata::codecs {

struct UInt8Codec
{
    static constexpr size_t fixed_size = 1;

    static size_t
    encoded_size(boost::json::value const&)
    {
        return 1;
    }

    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, uint8_t v)
    {
        s.add_u8(v);
    }

    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, boost::json::value const& v)
    {
        s.add_u8(static_cast<uint8_t>(
            v.is_uint64() ? v.as_uint64()
                          : static_cast<uint64_t>(v.as_int64())));
    }

    static boost::json::value
    decode(Slice const& data)
    {
        return static_cast<std::uint64_t>(data.data()[0]);
    }
};

struct UInt16Codec
{
    static constexpr size_t fixed_size = 2;

    static size_t
    encoded_size(boost::json::value const&)
    {
        return 2;
    }

    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, uint16_t v)
    {
        s.add_u16(v);
    }

    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, boost::json::value const& v)
    {
        s.add_u16(static_cast<uint16_t>(
            v.is_uint64() ? v.as_uint64()
                          : static_cast<uint64_t>(v.as_int64())));
    }

    // Decode to raw uint16. Enum resolution (TransactionType etc.) is
    // handled by the dispatch layer which has Protocol access.
    static uint16_t
    decode_raw(Slice const& data)
    {
        return (static_cast<uint16_t>(data.data()[0]) << 8) |
            static_cast<uint16_t>(data.data()[1]);
    }

    static boost::json::value
    decode(Slice const& data)
    {
        return static_cast<std::uint64_t>(decode_raw(data));
    }
};

struct UInt32Codec
{
    static constexpr size_t fixed_size = 4;

    static size_t
    encoded_size(boost::json::value const&)
    {
        return 4;
    }

    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, uint32_t v)
    {
        s.add_u32(v);
    }

    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, boost::json::value const& v)
    {
        s.add_u32(static_cast<uint32_t>(
            v.is_uint64() ? v.as_uint64()
                          : static_cast<uint64_t>(v.as_int64())));
    }

    static boost::json::value
    decode(Slice const& data)
    {
        uint32_t v = (static_cast<uint32_t>(data.data()[0]) << 24) |
            (static_cast<uint32_t>(data.data()[1]) << 16) |
            (static_cast<uint32_t>(data.data()[2]) << 8) |
            static_cast<uint32_t>(data.data()[3]);
        return static_cast<std::uint64_t>(v);
    }
};

struct UInt64Codec
{
    static constexpr size_t fixed_size = 8;

    static size_t
    encoded_size(boost::json::value const&)
    {
        return 8;
    }

    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, uint64_t v)
    {
        s.add_u64(v);
    }

    // JSON UInt64 is a hex string (e.g. "5003BAF82D03A000")
    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, boost::json::value const& v)
    {
        s.add_u64(parse_hex_uint64(std::string_view(v.as_string()), "UInt64"));
    }

    // From hex string
    template <ByteSink Sink>
    static void
    encode_hex(Serializer<Sink>& s, std::string_view hex)
    {
        s.add_u64(parse_hex_uint64(hex, "UInt64"));
    }

    // From decimal string (for SPECIAL_FIELDS like MaximumAmount)
    template <ByteSink Sink>
    static void
    encode_decimal(Serializer<Sink>& s, std::string_view decimal)
    {
        s.add_u64(parse_uint64(decimal, "UInt64"));
    }

    // Decode to hex string (matching xrpl-py/xrpl.js)
    static boost::json::value
    decode(Slice const& data)
    {
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i)
        {
            v = (v << 8) | data.data()[i];
        }
        // Return uppercase hex, zero-padded to 16 chars
        char buf[17];
        std::snprintf(
            buf, sizeof(buf), "%016llX", static_cast<unsigned long long>(v));
        return boost::json::string(buf);
    }
};

}  // namespace catl::xdata::codecs
