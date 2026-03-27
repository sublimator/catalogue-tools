#pragma once

// All codecs
#include "catl/xdata/codec-error.h"
#include "catl/xdata/codecs/account_id.h"
#include "catl/xdata/codecs/amount.h"
#include "catl/xdata/codecs/blob.h"
#include "catl/xdata/codecs/currency.h"
#include "catl/xdata/codecs/hash.h"
#include "catl/xdata/codecs/enum.h"
#include "catl/xdata/codecs/int.h"
#include "catl/xdata/codecs/issue.h"
#include "catl/xdata/codecs/number.h"
#include "catl/xdata/codecs/pathset.h"
#include "catl/xdata/codecs/starray.h"
#include "catl/xdata/codecs/stobject.h"
#include "catl/xdata/codecs/uint.h"
#include "catl/xdata/codecs/vector256.h"
#include "catl/xdata/codecs/xchain_bridge.h"

#include "catl/xdata/protocol.h"
#include "catl/xdata/types.h"

namespace catl::xdata::codecs {

// ---------------------------------------------------------------------------
// Type dispatch: FieldType → codec encoded_size / encode
// ---------------------------------------------------------------------------

// Compute encoded size for a field value (value bytes only, no header/VL)
inline size_t
field_value_encoded_size(
    FieldDef const& field,
    boost::json::value const& v,
    Protocol const& protocol)
{
    auto const& t = field.meta.type;

    if (t == FieldTypes::UInt8)
        return UInt8Codec::fixed_size;
    if (t == FieldTypes::UInt16)
        return UInt16Codec::fixed_size;
    if (t == FieldTypes::UInt32)
        return UInt32Codec::fixed_size;
    if (t == FieldTypes::UInt64)
        return UInt64Codec::fixed_size;
    if (t == FieldTypes::Int32)
        return Int32Codec::fixed_size;
    if (t == FieldTypes::Int64)
        return Int64Codec::fixed_size;
    if (t == FieldTypes::Hash128)
        return Hash128Codec::fixed_size;
    if (t == FieldTypes::Hash160)
        return Hash160Codec::fixed_size;
    if (t == FieldTypes::Hash192)
        return Hash192Codec::fixed_size;
    if (t == FieldTypes::Hash256)
        return Hash256Codec::fixed_size;
    if (t == FieldTypes::UInt96)
        return UInt96Codec::fixed_size;
    if (t == FieldTypes::UInt384)
        return UInt384Codec::fixed_size;
    if (t == FieldTypes::UInt512)
        return UInt512Codec::fixed_size;
    if (t == FieldTypes::Amount)
        return AmountCodec::encoded_size(v);
    if (t == FieldTypes::AccountID)
        return AccountIDCodec::encoded_size(v);
    if (t == FieldTypes::Currency)
        return CurrencyCodec::fixed_size;
    if (t == FieldTypes::Issue)
        return IssueCodec::encoded_size(v);
    if (t == FieldTypes::Number)
        return NumberCodec::fixed_size;
    if (t == FieldTypes::XChainBridge)
        return XChainBridgeCodec::encoded_size(v);
    if (t == FieldTypes::Vector256)
        return Vector256Codec::encoded_size(v);
    if (t == FieldTypes::PathSet)
        return PathSetCodec::encoded_size(v);
    if (t == FieldTypes::Blob)
        return BlobCodec::encoded_size(v);
    if (t == FieldTypes::STObject)
        return STObjectCodec::encoded_size(v, protocol, false);
    if (t == FieldTypes::STArray)
        return STArrayCodec::encoded_size(v, protocol);

    // Fallback: fixed-size types → treat as hex blob
    if (field.meta.type.fixed_size > 0)
        return field.meta.type.fixed_size;

    // VL-encoded unknown types → treat as hex blob
    if (field.meta.is_vl_encoded)
        return BlobCodec::encoded_size(v);

    throw EncodeError(
        CodecErrorCode::invalid_value,
        std::string(field.meta.type.name),
        "unknown type code " + std::to_string(field.meta.type.code));
}

// Decode a field value from binary to JSON.
// For most types this delegates to the codec's decode(). UInt16 enum fields
// (TransactionType, LedgerEntryType) need Protocol for name resolution.
inline boost::json::value
decode_field_value(
    FieldDef const& field,
    Slice const& data,
    Protocol const& protocol)
{
    auto const& t = field.meta.type;

    // Enum fields first — numeric on wire, string enum in JSON
    if (field.code == EnumFieldCodes::TransactionType)
        return TransactionTypeCodec::decode(data, protocol);
    if (field.code == EnumFieldCodes::LedgerEntryType)
        return LedgerEntryTypeCodec::decode(data, protocol);
    if (field.code == EnumFieldCodes::TransactionResult)
        return TransactionResultCodec::decode(data, protocol);
    if (field.code == EnumFieldCodes::PermissionValue)
        return PermissionValueCodec::decode(data, protocol);

    if (t == FieldTypes::UInt8)
        return UInt8Codec::decode(data);
    if (t == FieldTypes::UInt16)
        return UInt16Codec::decode(data);
    if (t == FieldTypes::UInt32)
        return UInt32Codec::decode(data);
    if (t == FieldTypes::UInt64)
        return UInt64Codec::decode(data);
    if (t == FieldTypes::Int32)
        return Int32Codec::decode(data);
    if (t == FieldTypes::Int64)
        return Int64Codec::decode(data);
    if (t == FieldTypes::Hash128)
        return Hash128Codec::decode(data);
    if (t == FieldTypes::Hash160)
        return Hash160Codec::decode(data);
    if (t == FieldTypes::Hash192)
        return Hash192Codec::decode(data);
    if (t == FieldTypes::Hash256)
        return Hash256Codec::decode(data);
    if (t == FieldTypes::UInt96)
        return UInt96Codec::decode(data);
    if (t == FieldTypes::UInt384)
        return UInt384Codec::decode(data);
    if (t == FieldTypes::UInt512)
        return UInt512Codec::decode(data);
    if (t == FieldTypes::Amount)
        return AmountCodec::decode(data);
    if (t == FieldTypes::AccountID)
        return AccountIDCodec::decode(data);
    if (t == FieldTypes::Currency)
        return CurrencyCodec::decode(data);
    if (t == FieldTypes::Issue)
        return IssueCodec::decode(data);
    if (t == FieldTypes::Number)
        return NumberCodec::decode(data);
    if (t == FieldTypes::XChainBridge)
        return XChainBridgeCodec::decode(data);
    if (t == FieldTypes::Vector256)
        return Vector256Codec::decode(data);
    if (t == FieldTypes::PathSet)
        return PathSetCodec::decode(data);
    if (t == FieldTypes::Blob)
        return BlobCodec::decode(data);

    // Unknown type: hex fallback
    return boost::json::string(hex_encode(data));
}

// Encode a field value (value bytes only, no header/VL)
template <ByteSink Sink>
void
encode_field_value(
    Serializer<Sink>& s,
    FieldDef const& field,
    boost::json::value const& v,
    Protocol const& protocol,
    std::string const& path)
{
    auto const& t = field.meta.type;

    // Enum fields first (integer code compare, no string matching)
    if (field.code == EnumFieldCodes::TransactionType)
    {
        TransactionTypeCodec::encode(s, v, protocol, path);
    }
    else if (field.code == EnumFieldCodes::LedgerEntryType)
    {
        LedgerEntryTypeCodec::encode(s, v, protocol, path);
    }
    else if (field.code == EnumFieldCodes::TransactionResult)
    {
        TransactionResultCodec::encode(s, v, protocol, path);
    }
    else if (field.code == EnumFieldCodes::PermissionValue)
    {
        PermissionValueCodec::encode(s, v, protocol, path);
    }
    else if (t == FieldTypes::UInt8)
    {
        UInt8Codec::encode(s, v);
    }
    else if (t == FieldTypes::UInt16)
    {
        UInt16Codec::encode(s, v);
    }
    else if (t == FieldTypes::UInt32)
    {
        UInt32Codec::encode(s, v);
    }
    else if (t == FieldTypes::UInt64)
    {
        UInt64Codec::encode(s, v);
    }
    else if (t == FieldTypes::Int32)
    {
        Int32Codec::encode(s, v);
    }
    else if (t == FieldTypes::Int64)
    {
        Int64Codec::encode(s, v);
    }
    else if (t == FieldTypes::Hash128)
    {
        Hash128Codec::encode(s, v);
    }
    else if (t == FieldTypes::Hash160)
    {
        Hash160Codec::encode(s, v);
    }
    else if (t == FieldTypes::Hash192)
    {
        Hash192Codec::encode(s, v);
    }
    else if (t == FieldTypes::Hash256)
    {
        Hash256Codec::encode(s, v);
    }
    else if (t == FieldTypes::UInt96)
    {
        UInt96Codec::encode(s, v);
    }
    else if (t == FieldTypes::UInt384)
    {
        UInt384Codec::encode(s, v);
    }
    else if (t == FieldTypes::UInt512)
    {
        UInt512Codec::encode(s, v);
    }
    else if (t == FieldTypes::Amount)
    {
        AmountCodec::encode(s, v, path);
    }
    else if (t == FieldTypes::AccountID)
    {
        AccountIDCodec::encode(s, v, path);
    }
    else if (t == FieldTypes::Blob)
    {
        BlobCodec::encode(s, v);
    }
    else if (t == FieldTypes::Currency)
    {
        CurrencyCodec::encode(s, v, path);
    }
    else if (t == FieldTypes::Issue)
    {
        IssueCodec::encode(s, v, path);
    }
    else if (t == FieldTypes::Number)
    {
        NumberCodec::encode(s, v);
    }
    else if (t == FieldTypes::XChainBridge)
    {
        XChainBridgeCodec::encode(s, v);
    }
    else if (t == FieldTypes::Vector256)
    {
        Vector256Codec::encode(s, v);
    }
    else if (t == FieldTypes::PathSet)
    {
        PathSetCodec::encode(s, v);
    }
    else if (t == FieldTypes::STObject)
    {
        STObjectCodec::encode(s, v, protocol, false, path);
    }
    else if (t == FieldTypes::STArray)
    {
        STArrayCodec::encode(s, v, protocol, path);
    }
    else if (field.meta.type.fixed_size > 0 || field.meta.is_vl_encoded)
    {
        // Fallback: unknown type with known size → treat as hex blob
        BlobCodec::encode(s, v);
    }
    else
    {
        throw EncodeError(
            CodecErrorCode::invalid_value,
            std::string(field.meta.type.name),
            "unknown type code " + std::to_string(field.meta.type.code),
            path);
    }
}

// ---------------------------------------------------------------------------
// Top-level convenience: serialize a JSON object to bytes
// ---------------------------------------------------------------------------

inline std::vector<uint8_t>
serialize_object(
    boost::json::object const& obj,
    Protocol const& protocol,
    bool only_signing = false)
{
    boost::json::value v(obj);
    size_t total = STObjectCodec::encoded_size(v, protocol);

    std::vector<uint8_t> buf;
    buf.reserve(total);
    VectorSink vs(buf);
    Serializer<VectorSink> s(vs);
    STObjectCodec::encode(s, v, protocol, only_signing);

    return buf;
}

}  // namespace catl::xdata::codecs
