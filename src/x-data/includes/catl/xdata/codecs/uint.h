#pragma once

#include "catl/xdata/serializer.h"
#include <boost/json.hpp>
#include <charconv>
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

    // JSON can be a number or a string (TransactionType, LedgerEntryType).
    // String resolution requires Protocol — handled by the dispatch layer.
    // This codec handles the numeric case.
    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, boost::json::value const& v)
    {
        s.add_u16(static_cast<uint16_t>(
            v.is_uint64() ? v.as_uint64()
                          : static_cast<uint64_t>(v.as_int64())));
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

    // JSON is a decimal string (too large for JSON number)
    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, boost::json::value const& v)
    {
        auto sv = v.as_string();
        uint64_t val = 0;
        std::from_chars(sv.data(), sv.data() + sv.size(), val);
        s.add_u64(val);
    }

    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, std::string_view decimal)
    {
        uint64_t val = 0;
        std::from_chars(decimal.data(), decimal.data() + decimal.size(), val);
        s.add_u64(val);
    }
};

}  // namespace catl::xdata::codecs
