#pragma once

#include "catl/xdata/codec-error.h"
#include "catl/xdata/codecs/uint.h"
#include "catl/xdata/protocol.h"
#include "catl/xdata/serializer.h"
#include <boost/json.hpp>

namespace catl::xdata::codecs {

// Well-known enum field codes (stable protocol constants)
namespace EnumFieldCodes {
inline constexpr uint32_t TransactionType = 0x00010002;   // UInt16, nth=2
inline constexpr uint32_t LedgerEntryType = 0x00010001;   // UInt16, nth=1
inline constexpr uint32_t TransactionResult = 0x00100003; // UInt8,  nth=3
inline constexpr uint32_t PermissionValue = 0x00020034;   // UInt32, nth=52
}  // namespace EnumFieldCodes

// Check if a field is one of the enum fields (integer compare, no strings)
inline bool
is_enum_field(uint32_t field_code)
{
    return field_code == EnumFieldCodes::TransactionType ||
           field_code == EnumFieldCodes::LedgerEntryType ||
           field_code == EnumFieldCodes::TransactionResult;
}

// Codec for fields that are numeric on the wire but string enums in JSON.
// TransactionType (UInt16), LedgerEntryType (UInt16), TransactionResult (UInt8).

struct TransactionTypeCodec
{
    static constexpr size_t fixed_size = 2;

    static size_t
    encoded_size(boost::json::value const&)
    {
        return 2;
    }

    template <ByteSink Sink>
    static void
    encode(
        Serializer<Sink>& s,
        boost::json::value const& v,
        Protocol const& protocol,
        std::string const& path = {})
    {
        if (v.is_string())
        {
            auto sv = std::string_view(v.as_string());
            for (auto const& [name, code] : protocol.transactionTypes())
            {
                if (name == sv)
                {
                    s.add_u16(code);
                    return;
                }
            }
            throw EncodeError(
                CodecErrorCode::unknown_enum,
                "TransactionType",
                "unknown: " + std::string(sv),
                path);
        }
        UInt16Codec::encode(s, v);
    }

    static boost::json::value
    decode(Slice const& data, Protocol const& protocol)
    {
        uint16_t raw = UInt16Codec::decode_raw(data);
        auto name = protocol.get_transaction_type_name(raw);
        if (name)
            return boost::json::string(*name);
        return static_cast<std::uint64_t>(raw);
    }
};

struct LedgerEntryTypeCodec
{
    static constexpr size_t fixed_size = 2;

    static size_t
    encoded_size(boost::json::value const&)
    {
        return 2;
    }

    template <ByteSink Sink>
    static void
    encode(
        Serializer<Sink>& s,
        boost::json::value const& v,
        Protocol const& protocol,
        std::string const& path = {})
    {
        if (v.is_string())
        {
            auto sv = std::string_view(v.as_string());
            for (auto const& [name, code] : protocol.ledgerEntryTypes())
            {
                if (name == sv)
                {
                    s.add_u16(code);
                    return;
                }
            }
            throw EncodeError(
                CodecErrorCode::unknown_enum,
                "LedgerEntryType",
                "unknown: " + std::string(sv),
                path);
        }
        UInt16Codec::encode(s, v);
    }

    static boost::json::value
    decode(Slice const& data, Protocol const& protocol)
    {
        uint16_t raw = UInt16Codec::decode_raw(data);
        auto name = protocol.get_ledger_entry_type_name(raw);
        if (name)
            return boost::json::string(*name);
        return static_cast<std::uint64_t>(raw);
    }
};

struct TransactionResultCodec
{
    static constexpr size_t fixed_size = 1;

    static size_t
    encoded_size(boost::json::value const&)
    {
        return 1;
    }

    template <ByteSink Sink>
    static void
    encode(
        Serializer<Sink>& s,
        boost::json::value const& v,
        Protocol const& protocol,
        std::string const& path = {})
    {
        if (v.is_string())
        {
            auto sv = std::string_view(v.as_string());
            for (auto const& [name, code] : protocol.transactionResults())
            {
                if (name == sv)
                {
                    s.add_u8(static_cast<uint8_t>(code));
                    return;
                }
            }
            throw EncodeError(
                CodecErrorCode::unknown_enum,
                "TransactionResult",
                "unknown: " + std::string(sv),
                path);
        }
        UInt8Codec::encode(s, v);
    }

    static boost::json::value
    decode(Slice const& data, Protocol const& protocol)
    {
        uint8_t raw = data.data()[0];
        int32_t code = static_cast<int32_t>(raw);
        for (auto const& [name, c] : protocol.transactionResults())
        {
            if (c == code)
                return boost::json::string(name);
        }
        return static_cast<std::int64_t>(code);
    }
};

struct PermissionValueCodec
{
    static constexpr size_t fixed_size = 4;

    static size_t
    encoded_size(boost::json::value const&)
    {
        return 4;
    }

    // PermissionValue is UInt32 on wire. JSON is a string enum name
    // (either a tx type like "Payment" or a granular permission like
    // "AccountDomainSet"), resolved via Protocol::permissions().
    template <ByteSink Sink>
    static void
    encode(
        Serializer<Sink>& s,
        boost::json::value const& v,
        Protocol const& protocol,
        std::string const& path = {})
    {
        if (v.is_string())
        {
            auto sv = std::string_view(v.as_string());
            auto const& perms = protocol.permissions();
            auto it = perms.find(std::string(sv));
            if (it != perms.end())
            {
                s.add_u32(it->second);
                return;
            }
            throw EncodeError(
                CodecErrorCode::unknown_enum,
                "PermissionValue",
                "unknown: " + std::string(sv),
                path);
        }
        UInt32Codec::encode(s, v);
    }

    static boost::json::value
    decode(Slice const& data, Protocol const& protocol)
    {
        uint32_t raw = (static_cast<uint32_t>(data.data()[0]) << 24) |
                       (static_cast<uint32_t>(data.data()[1]) << 16) |
                       (static_cast<uint32_t>(data.data()[2]) << 8) |
                        static_cast<uint32_t>(data.data()[3]);
        auto const& perms = protocol.permissions();
        for (auto const& [name, code] : perms)
        {
            if (code == raw)
                return boost::json::string(name);
        }
        return static_cast<std::uint64_t>(raw);
    }
};

}  // namespace catl::xdata::codecs
