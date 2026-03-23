#pragma once

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
    // Size: XRP=20, IOU=40, MPT=44
    static size_t
    encoded_size(boost::json::value const& v)
    {
        if (v.is_string())
            return 20;  // "XRP"
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

    // IOU: currency + issuer
    template <ByteSink Sink>
    static void
    encode_iou(
        Serializer<Sink>& s,
        std::string_view currency,
        std::string_view issuer,
        std::string const& path = {})
    {
        CurrencyCodec::encode(s, currency, path);
        AccountIDCodec::encode(s, issuer, path);
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

    // From JSON: string "XRP" → native, object with currency/issuer → IOU,
    // object with mpt_issuance_id → MPT
    template <ByteSink Sink>
    static void
    encode(
        Serializer<Sink>& s,
        boost::json::value const& v,
        std::string const& path = {})
    {
        if (v.is_string())
        {
            auto sv = std::string_view(v.as_string());
            if (sv == "XRP" || sv == "XAH" || sv.empty())
            {
                encode_native(s);
            }
            else
            {
                throw EncodeError(
                    CodecErrorCode::invalid_value,
                    "Issue",
                    "string value must be XRP or XAH, got: " +
                        std::string(sv),
                    path);
            }
        }
        else if (v.is_object())
        {
            auto const& obj = v.as_object();
            if (obj.contains("mpt_issuance_id"))
            {
                encode_mpt(
                    s,
                    std::string_view(obj.at("mpt_issuance_id").as_string()),
                    path);
            }
            else if (!obj.contains("currency"))
            {
                throw EncodeError(
                    CodecErrorCode::missing_field,
                    "Issue",
                    "missing 'currency' or 'mpt_issuance_id'",
                    path);
            }
            else if (!obj.contains("issuer"))
            {
                encode_native(s);
            }
            else
            {
                encode_iou(
                    s,
                    std::string_view(obj.at("currency").as_string()),
                    std::string_view(obj.at("issuer").as_string()),
                    path);
            }
        }
        else
        {
            throw EncodeError(
                CodecErrorCode::invalid_value,
                "Issue",
                "expected string or object",
                path);
        }
    }

    static boost::json::value
    decode(Slice const& data)
    {
        if (data.size() < 20)
            return boost::json::string(hex_encode(data));

        // First 20 bytes: currency or MPT issuer
        Slice first20(data.data(), 20);

        // Check native (all zeros)
        if (is_xrp_currency(first20))
            return CurrencyCodec::decode(first20);

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
            obj["currency"] = CurrencyCodec::decode(first20);
            obj["issuer"] = AccountIDCodec::decode(second20);
            return obj;
        }

        // Just currency, no issuer (native variant)
        return CurrencyCodec::decode(first20);
    }
};

}  // namespace catl::xdata::codecs
