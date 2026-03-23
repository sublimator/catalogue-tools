#pragma once

#include "catl/xdata/codecs/codecs.h"
#include "catl/xdata/protocol.h"
#include <boost/json.hpp>
#include <vector>

namespace catl::xdata {

/// Serialize a state tree leaf (SLE) from JSON.
/// Inverse of parse_leaf.
///
/// Input:  JSON object with serialized fields (+ "index" which is skipped)
/// Output: raw item bytes (the STObject, no prefix, no trailing key)
inline std::vector<uint8_t>
serialize_leaf(boost::json::object const& obj, Protocol const& protocol)
{
    return codecs::serialize_object(obj, protocol);
}

/// Serialize a TX tree leaf (transaction + metadata) from JSON.
/// Inverse of parse_transaction.
///
/// Input:  {"hash": "...", "tx": {...}, "meta": {...}}
/// Output: VL(tx_bytes) + VL(meta_bytes)  (no prefix, no trailing key)
///
/// Single allocation: encoded_size for exact pre-alloc, one encode pass.
inline std::vector<uint8_t>
serialize_transaction(boost::json::object const& obj, Protocol const& protocol)
{
    auto const& tx = obj.at("tx");
    auto const& meta = obj.at("meta");

    size_t tx_size = codecs::STObjectCodec::encoded_size(tx, protocol);
    size_t meta_size = codecs::STObjectCodec::encoded_size(meta, protocol);

    size_t total = vl_prefix_size(tx_size) + tx_size +
        vl_prefix_size(meta_size) + meta_size;

    std::vector<uint8_t> buf;
    buf.reserve(total);
    VectorSink vs(buf);
    Serializer<VectorSink> s(vs);

    s.add_vl_prefix(tx_size);
    codecs::STObjectCodec::encode(s, tx, protocol);

    s.add_vl_prefix(meta_size);
    codecs::STObjectCodec::encode(s, meta, protocol);

    return buf;
}

}  // namespace catl::xdata
