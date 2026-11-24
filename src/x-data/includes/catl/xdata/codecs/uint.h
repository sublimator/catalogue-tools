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

    // The UInt64 fields rippled marks base-ten (SField::kSmdBaseTen in
    // include/xrpl/protocol/detail/sfields.macro). They are amounts: their
    // JSON text is decimal, where every other UInt64 is hex. Reading one as
    // hex is a different number on the wire, not a different spelling.
    // xahaud marks the first three the same way and defines no others.
    //
    // The set is by name. A field's number belongs to one network's
    // definitions; the name is what the JSON reader sees.
    static constexpr std::string_view base_ten_fields[] = {
        "MaximumAmount",
        "OutstandingAmount",
        "MPTAmount",
        "LockedAmount",
        "ConfidentialOutstandingAmount",
    };

    static bool
    is_base_ten(std::string_view field_name)
    {
        for (auto const name : base_ten_fields)
            if (field_name == name)
                return true;
        return false;
    }

    // From decimal text, as rippled reads a base-ten field: digits and
    // nothing else, at most 2^64 - 1. No sign, no spaces, no exponent, no hex.
    template <ByteSink Sink>
    static void
    encode_decimal(
        Serializer<Sink>& s,
        std::string_view decimal,
        std::string const& path = {})
    {
        uint64_t val = 0;
        bool const digits = !decimal.empty() &&
            decimal.find_first_not_of("0123456789") == std::string_view::npos;
        auto const [ptr, ec] = digits
            ? std::from_chars(
                  decimal.data(), decimal.data() + decimal.size(), val)
            : std::from_chars_result{decimal.data(), std::errc::invalid_argument};
        if (ec != std::errc{} || ptr != decimal.data() + decimal.size())
        {
            throw EncodeError(
                CodecErrorCode::invalid_value,
                "UInt64",
                "invalid decimal integer: " + std::string(decimal),
                path);
        }
        s.add_u64(val);
    }

    template <ByteSink Sink>
    static void
    encode_decimal(
        Serializer<Sink>& s,
        boost::json::value const& v,
        std::string const& path = {})
    {
        if (!v.is_string())
        {
            throw EncodeError(
                CodecErrorCode::invalid_value,
                "UInt64",
                "expected a decimal string",
                path);
        }
        encode_decimal(s, std::string_view(v.as_string()), path);
    }

    // Decode a base-ten field to decimal text, as rippled's STUInt64::getJson.
    static boost::json::value
    decode_decimal(Slice const& data)
    {
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i)
        {
            v = (v << 8) | data.data()[i];
        }
        return boost::json::string(std::to_string(v));
    }

    // Decode to hex text, as rippled's STUInt64::getJson writes a field that
    // is not base ten: std::to_chars in base 16, so lowercase and without
    // leading zeros ("ff"; zero is "0"). xrpl.js and xrpl-py write 16
    // uppercase digits for the same bytes; both spellings are read back.
    static boost::json::value
    decode(Slice const& data)
    {
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i)
        {
            v = (v << 8) | data.data()[i];
        }
        char buf[17];
        std::snprintf(
            buf, sizeof(buf), "%llx", static_cast<unsigned long long>(v));
        return boost::json::string(buf);
    }
};

}  // namespace catl::xdata::codecs
