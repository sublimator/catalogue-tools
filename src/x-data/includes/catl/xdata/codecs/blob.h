#pragma once

#include "catl/xdata/hex.h"
#include "catl/xdata/serializer.h"
#include <boost/json.hpp>

namespace catl::xdata::codecs {

struct BlobCodec
{
    // Size from JSON hex string
    static size_t
    encoded_size(boost::json::value const& v)
    {
        return v.as_string().size() / 2;
    }

    static size_t
    encoded_size(std::string_view hex)
    {
        return hex.size() / 2;
    }

    // From raw bytes (no VL — caller handles VL wrapping via field metadata)
    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, std::span<const uint8_t> data)
    {
        s.add_raw(data);
    }

    // From hex string — streams directly
    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, std::string_view hex)
    {
        s.add_hex(hex);
    }

    // From JSON (hex string — JsonVisitor may also output ASCII for printable
    // blobs, but we always treat as hex on the way back in)
    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, boost::json::value const& v)
    {
        s.add_hex(v.as_string());
    }

    // Decode: always hex (matching xrpl-py / xrpl.js behavior).
    // Optionally populates an ascii hint field (e.g. "MemoType_ascii")
    // via the visitor layer — not here.
    static boost::json::value
    decode(Slice const& data)
    {
        return boost::json::string(hex_encode(data));
    }
};

}  // namespace catl::xdata::codecs
