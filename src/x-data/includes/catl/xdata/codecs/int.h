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
};

}  // namespace catl::xdata::codecs
