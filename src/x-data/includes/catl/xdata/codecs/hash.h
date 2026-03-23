#pragma once

#include "catl/xdata/hex.h"
#include "catl/xdata/serializer.h"
#include <boost/json.hpp>

namespace catl::xdata::codecs {

// Generic fixed-size hash/uint codec. Works for:
// Hash128(16), Hash160(20), Hash192(24), Hash256(32),
// UInt96(12), UInt384(48), UInt512(64)
template <size_t N>
struct FixedHashCodec
{
    static constexpr size_t fixed_size = N;

    static size_t
    encoded_size(boost::json::value const&)
    {
        return N;
    }

    // From raw bytes
    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, std::span<const uint8_t, N> data)
    {
        s.add_raw(std::span<const uint8_t>{data.data(), N});
    }

    // From hex string — streams directly to sink
    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, std::string_view hex)
    {
        s.add_hex(hex);
    }

    // From JSON (hex string)
    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, boost::json::value const& v)
    {
        s.add_hex(v.as_string());
    }

    static boost::json::value
    decode(Slice const& data)
    {
        return boost::json::string(hex_encode(data));
    }
};

// Concrete type aliases
using Hash128Codec = FixedHashCodec<16>;
using Hash160Codec = FixedHashCodec<20>;
using Hash192Codec = FixedHashCodec<24>;
using Hash256Codec = FixedHashCodec<32>;
using UInt96Codec = FixedHashCodec<12>;
using UInt384Codec = FixedHashCodec<48>;
using UInt512Codec = FixedHashCodec<64>;

}  // namespace catl::xdata::codecs
