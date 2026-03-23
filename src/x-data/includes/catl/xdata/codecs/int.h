#pragma once

#include "catl/xdata/serializer.h"
#include <boost/json.hpp>

namespace catl::xdata::codecs {

struct Int32Codec
{
    static constexpr size_t fixed_size = 4;

    static size_t
    encoded_size(boost::json::value const&)
    {
        return 4;
    }

    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, int32_t v)
    {
        s.add_u32(static_cast<uint32_t>(v));
    }

    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, boost::json::value const& v)
    {
        s.add_u32(static_cast<uint32_t>(v.as_int64()));
    }

    static boost::json::value
    decode(Slice const& data)
    {
        int32_t v = static_cast<int32_t>(
            (static_cast<uint32_t>(data.data()[0]) << 24) |
            (static_cast<uint32_t>(data.data()[1]) << 16) |
            (static_cast<uint32_t>(data.data()[2]) << 8) |
            static_cast<uint32_t>(data.data()[3]));
        return static_cast<std::int64_t>(v);
    }
};

struct Int64Codec
{
    static constexpr size_t fixed_size = 8;

    static size_t
    encoded_size(boost::json::value const&)
    {
        return 8;
    }

    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, int64_t v)
    {
        s.add_u64(static_cast<uint64_t>(v));
    }

    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, boost::json::value const& v)
    {
        s.add_u64(static_cast<uint64_t>(v.as_int64()));
    }

    static boost::json::value
    decode(Slice const& data)
    {
        uint64_t u = 0;
        for (int i = 0; i < 8; ++i)
        {
            u = (u << 8) | data.data()[i];
        }
        return static_cast<std::int64_t>(u);
    }
};

}  // namespace catl::xdata::codecs
