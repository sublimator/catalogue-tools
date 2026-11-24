#pragma once

#include "catl/base58/base58.h"
#include "catl/xdata/codec-error.h"
#include "catl/xdata/codecs/account_id.h"
#include "catl/xdata/codecs/currency.h"
#include "catl/xdata/hex.h"
#include "catl/xdata/serializer.h"
#include "catl/xdata/types/issue.h"
#include <boost/json.hpp>

namespace catl::xdata::codecs {

// noAccount sentinel: 20 bytes, all zeros except last byte = 1
// This distinguishes MPT from IOU in the wire format.
inline constexpr uint8_t NO_ACCOUNT[20] = {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1};

struct IssueCodec
{
    // Size: native=20, IOU=40, MPT=44
    static size_t
    encoded_size(boost::json::value const& v)
    {
        if (!v.is_object())
            return 20;  // not an issue; encode() refuses it
        auto const& obj = v.as_object();
        if (obj.contains("mpt_issuance_id"))
            return 44;  // MPT: issuer(20) + noAccount(20) + seq(4)
        if (!obj.contains("issuer"))
            return 20;  // native
        return 40;      // IOU: currency(20) + issuer(20)
    }

    template <ByteSink Sink>
    static void
    encode_native(Serializer<Sink>& s)
    {
        s.add_issue_native();
    }

    /// An issuer as rippled's issueFromJson takes it: a base58 account that
    /// is neither the zero account nor noAccount.
    static void
    issuer_bytes(
        std::string_view issuer,
        uint8_t out[20],
        char const* type,
        std::string const& path = {})
    {
        auto const decoded = base58::decode_account_id(issuer);
        if (!decoded || decoded->size() != 20)
        {
            throw EncodeError(
                CodecErrorCode::invalid_encoding,
                type,
                "invalid issuer: " + std::string(issuer),
                path);
        }
        std::memcpy(out, decoded->data(), 20);
        if (is_xrp_currency(out) || is_no_account(out))
        {
            throw EncodeError(
                CodecErrorCode::invalid_value,
                type,
                "issuer must be a valid account: " + std::string(issuer),
                path);
        }
    }

    // IOU: currency + issuer
    template <ByteSink Sink>
    static void
    encode_iou(
        Serializer<Sink>& s,
        std::string_view currency,
        std::string_view issuer,
        std::string const& path = {},
        std::string_view native = CurrencyCodec::default_native)
    {
        uint8_t currency_raw[20];
        CurrencyCodec::to_bytes(currency, currency_raw, path, native);
        if (CurrencyCodec::is_zero(currency_raw))
        {
            throw EncodeError(
                CodecErrorCode::invalid_value,
                "Issue",
                "the native currency has no issuer",
                path);
        }
        uint8_t issuer_raw[20];
        issuer_bytes(issuer, issuer_raw, "Issue", path);
        s.add_raw(std::span<const uint8_t>{currency_raw, 20});
        s.add_raw(std::span<const uint8_t>{issuer_raw, 20});
    }

    // MPT: issuer(20) + noAccount(20) + sequence(4)
    template <ByteSink Sink>
    static void
    encode_mpt(
        Serializer<Sink>& s,
        std::string_view mpt_issuance_id,
        std::string const& path = {})
    {
        // MPTID is 24 bytes: sequence(4) + issuer(20)
        // On wire: issuer(20) + noAccount(20) + sequence(4)
        require_hex_length(mpt_issuance_id, 48, "Issue", path);

        // Decode the 24-byte MPTID
        uint8_t mptid[24];
        hex_decode(mpt_issuance_id, std::span<uint8_t>{mptid, 24});

        // mptid[0..3] = sequence (big-endian)
        // mptid[4..23] = issuer AccountID

        // Write issuer (20 bytes from offset 4)
        s.add_raw(std::span<const uint8_t>{mptid + 4, 20});
        // Write noAccount sentinel
        s.add_raw(std::span<const uint8_t>{NO_ACCOUNT, 20});
        // Write sequence: rippled does memcpy(first 4 MPTID bytes → uint32)
        // then add32. This is a native-endian reinterpret + big-endian write.
        uint32_t sequence;
        std::memcpy(&sequence, mptid, sizeof(sequence));
        s.add_u32(sequence);
    }

    // From JSON, as rippled's issueFromJson (src/libxrpl/protocol/Issue.cpp)
    // and assetFromJson:
    //   {"currency": <native code or "">}             native, no issuer
    //   {"currency": <other>, "issuer": <account>}    issued
    //   {"mpt_issuance_id": <48 hex>}                 MPT
    // A bare string is not an issue. A currency without an issuer is native
    // only when it is the native code: any other code there would be written
    // as the native issue and misstate what the JSON says.
    template <ByteSink Sink>
    static void
    encode(
        Serializer<Sink>& s,
        boost::json::value const& v,
        std::string const& path = {},
        std::string_view native = CurrencyCodec::default_native)
    {
        if (!v.is_object())
        {
            throw EncodeError(
                CodecErrorCode::invalid_value,
                "Issue",
                "expected an object",
                path);
        }
        auto const& obj = v.as_object();
        if (obj.contains("mpt_issuance_id"))
        {
            if (obj.contains("currency") || obj.contains("issuer"))
            {
                throw EncodeError(
                    CodecErrorCode::invalid_value,
                    "Issue",
                    "mpt_issuance_id excludes currency and issuer",
                    path);
            }
            if (!obj.at("mpt_issuance_id").is_string())
            {
                throw EncodeError(
                    CodecErrorCode::invalid_value,
                    "Issue",
                    "mpt_issuance_id must be a string",
                    path);
            }
            encode_mpt(
                s,
                std::string_view(obj.at("mpt_issuance_id").as_string()),
                path);
            return;
        }
        if (!obj.contains("currency"))
        {
            throw EncodeError(
                CodecErrorCode::missing_field,
                "Issue",
                "missing 'currency' or 'mpt_issuance_id'",
                path);
        }
        if (!obj.at("currency").is_string())
        {
            throw EncodeError(
                CodecErrorCode::invalid_value,
                "Issue",
                "currency must be a string",
                path);
        }

        uint8_t currency_raw[20];
        CurrencyCodec::to_bytes(
            std::string_view(obj.at("currency").as_string()),
            currency_raw,
            path,
            native);
        if (CurrencyCodec::is_no_currency(currency_raw) ||
            CurrencyCodec::is_reserved_code(currency_raw, native))
        {
            throw EncodeError(
                CodecErrorCode::invalid_value,
                "Issue",
                "currency must be a valid currency",
                path);
        }

        auto const* issuer = obj.if_contains("issuer");
        if (CurrencyCodec::is_zero(currency_raw))
        {
            if (issuer && !issuer->is_null())
            {
                throw EncodeError(
                    CodecErrorCode::invalid_value,
                    "Issue",
                    "the native currency has no issuer",
                    path);
            }
            encode_native(s);
            return;
        }
        if (!issuer || !issuer->is_string())
        {
            throw EncodeError(
                CodecErrorCode::missing_field,
                "Issue",
                "issuer must be a string",
                path);
        }
        uint8_t issuer_raw[20];
        issuer_bytes(
            std::string_view(issuer->as_string()), issuer_raw, "Issue", path);
        s.add_raw(std::span<const uint8_t>{currency_raw, 20});
        s.add_raw(std::span<const uint8_t>{issuer_raw, 20});
    }

    // To JSON, as rippled's Issue::setJson: always an object. The native
    // issue is {"currency": <native code>} and carries no issuer.
    static boost::json::value
    decode(
        Slice const& data,
        std::string_view native = CurrencyCodec::default_native)
    {
        if (data.size() < 20)
            return boost::json::string(hex_encode(data));

        // First 20 bytes: currency or MPT issuer
        Slice first20(data.data(), 20);

        if (is_xrp_currency(first20))
        {
            boost::json::object obj;
            obj["currency"] = boost::json::string(native);
            return obj;
        }

        if (data.size() >= 40)
        {
            // Second 20 bytes: issuer or noAccount sentinel
            Slice second20(data.data() + 20, 20);

            // Check for MPT: second slot == noAccount
            if (std::memcmp(second20.data(), NO_ACCOUNT, 20) == 0 &&
                data.size() >= 44)
            {
                // MPT: first20=issuer, second20=noAccount, next 4=sequence
                // Reconstruct MPTID: sequence(4) + issuer(20)
                // Reverse the memcpy+add32 from encode: read big-endian
                // uint32, memcpy back to bytes
                uint32_t seq_be = (static_cast<uint32_t>(data.data()[40]) << 24) |
                                  (static_cast<uint32_t>(data.data()[41]) << 16) |
                                  (static_cast<uint32_t>(data.data()[42]) << 8) |
                                   static_cast<uint32_t>(data.data()[43]);
                uint8_t mptid[24];
                std::memcpy(mptid, &seq_be, 4);               // native-endian sequence
                std::memcpy(mptid + 4, data.data(), 20);      // issuer
                boost::json::object obj;
                obj["mpt_issuance_id"] =
                    boost::json::string(hex_encode(mptid, 24));
                return obj;
            }

            // IOU: first20=currency, second20=issuer
            boost::json::object obj;
            obj["currency"] = CurrencyCodec::decode(first20, native);
            obj["issuer"] = AccountIDCodec::decode(second20);
            return obj;
        }

        // A currency that is not native and has no issuer is not an issue.
        return boost::json::string(hex_encode(data));
    }
};

}  // namespace catl::xdata::codecs
