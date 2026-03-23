#pragma once

#include "catl/xdata/serializer.h"
#include <boost/json.hpp>

namespace catl::xdata::codecs {

struct Vector256Codec
{
    // Size from JSON array of hex strings (each 64 chars = 32 bytes)
    static size_t
    encoded_size(boost::json::value const& v)
    {
        return v.as_array().size() * 32;
    }

    // From span of Hash256
    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, std::span<const Hash256> hashes)
    {
        for (auto const& h : hashes)
        {
            s.add_raw(std::span<const uint8_t>{h.data(), 32});
        }
    }

    // From JSON array of hex strings — streams each hash directly
    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, boost::json::value const& v)
    {
        for (auto const& elem : v.as_array())
        {
            s.add_hex(elem.as_string());
        }
    }
};

}  // namespace catl::xdata::codecs
