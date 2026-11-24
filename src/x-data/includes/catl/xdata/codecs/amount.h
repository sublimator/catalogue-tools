#pragma once

#include "catl/base58/base58.h"
#include "catl/xdata/codec-error.h"
#include "catl/xdata/codecs/account_id.h"
#include "catl/xdata/codecs/currency.h"
#include "catl/xdata/decimal.h"
#include "catl/xdata/serializer.h"
#include "catl/xdata/types/amount.h"
#include "catl/xdata/types/iou-value.h"
#include <boost/json.hpp>

namespace catl::xdata::codecs {

// Amount JSON follows rippled's STAmount::setJson and amountFromJson
// (src/libxrpl/protocol/STAmount.cpp):
//
//   native   a string of drops
//   issued   {"value", "currency", "issuer"}; the currency is never the
//            native code, which depends on the network (see CurrencyCodec)
//   MPT      {"value", "mpt_issuance_id"}
//
// Every value is a decimal in rippled's number grammar (decimal.h).
struct AmountCodec
{
    static constexpr size_t native_size = 8;
    static constexpr size_t iou_size = 48;
    static constexpr size_t mpt_size = 33;

    static size_t
    encoded_size(boost::json::value const& v)
    {
        if (v.is_string())
            return native_size;
        auto const& obj = v.as_object();
        if (obj.contains("mpt_issuance_id"))
            return mpt_size;
        return iou_size;
    }

    // -- Native amount from integer drops --
    template <ByteSink Sink>
    static void
    encode_native(Serializer<Sink>& s, int64_t drops)
    {
        s.add_native_amount(drops);
    }

    // -- Native amount from string drops --
    template <ByteSink Sink>
    static void
    encode_native(
        Serializer<Sink>& s,
        std::string_view drops_str,
        std::string const& path = {})
    {
        auto const parts = parse_decimal(drops_str, "Amount", path);
        auto const drops = decimal_to_integer(parts, drops_str, "Amount", path);
        // rippled kMaxNativeN: no more than 10^17 drops exist.
        static constexpr int64_t max_native = 100'000'000'000'000'000LL;
        if (drops > max_native || drops < -max_native)
        {
            throw EncodeError(
                CodecErrorCode::out_of_range,
                "Amount",
                "native amount out of range: " + std::string(drops_str),
                path);
        }
        s.add_native_amount(drops);
    }

    // -- IOU amount from string parts --
    template <ByteSink Sink>
    static void
    encode_iou(
        Serializer<Sink>& s,
        std::string_view value,
        std::string_view currency,
        std::string_view issuer,
        std::string const& path = {},
        std::string_view native = CurrencyCodec::default_native)
    {
        uint64_t raw = encode_iou_value(value, path);

        uint8_t currency_raw[20];
        CurrencyCodec::to_bytes(currency, currency_raw, path, native);
        // rippled: "XRP may not be specified as an object". The zero
        // currency in an issued amount is also bytes rippled cannot read
        // back ("invalid native currency"), however the JSON spells it.
        if (CurrencyCodec::is_zero(currency_raw))
        {
            throw EncodeError(
                CodecErrorCode::invalid_value,
                "Amount",
                "an issued amount cannot name the native currency",
                path);
        }

        auto const decoded = base58::decode_account_id(issuer);
        if (!decoded || decoded->size() != 20)
        {
            throw EncodeError(
                CodecErrorCode::invalid_encoding,
                "Amount",
                "invalid issuer: " + std::string(issuer),
                path);
        }
        // The zero account as issuer is likewise unreadable by rippled
        // ("invalid native account").
        if (AccountIDCodec::is_zero_account(decoded->data(), 20))
        {
            throw EncodeError(
                CodecErrorCode::invalid_value,
                "Amount",
                "an issued amount needs an issuer other than the zero account",
                path);
        }

        s.add_u64(raw);
        s.add_raw(std::span<const uint8_t>{currency_raw, 20});
        s.add_raw(std::span<const uint8_t>{decoded->data(), 20});
    }

    // -- MPT amount from string parts --
    template <ByteSink Sink>
    static void
    encode_mpt(
        Serializer<Sink>& s,
        std::string_view value,
        std::string_view mpt_issuance_id,
        std::string const& path = {})
    {
        require_hex_length(mpt_issuance_id, 48, "Amount", path);

        auto const parts = parse_decimal(value, "Amount", path);
        auto const signed_val =
            decimal_to_integer(parts, value, "Amount", path);
        bool const is_negative = signed_val < 0;
        int64_t const int_val = is_negative ? -signed_val : signed_val;

        // Zero is always positive
        uint8_t flags = 0x20;  // cMPToken
        if (!is_negative || int_val == 0)
        {
            flags |= 0x40;  // cPositive
        }

        s.add_u8(flags);
        s.add_u64(static_cast<uint64_t>(int_val));
        s.add_hex(mpt_issuance_id);
    }

    // -- From JSON: string → native, object → IOU or MPT --
    template <ByteSink Sink>
    static void
    encode(
        Serializer<Sink>& s,
        boost::json::value const& v,
        std::string const& path = {},
        std::string_view native = CurrencyCodec::default_native)
    {
        auto const text = [&](boost::json::object const& obj,
                              char const* name) -> std::string_view {
            auto const* member = obj.if_contains(name);
            if (!member || !member->is_string())
            {
                throw EncodeError(
                    member ? CodecErrorCode::invalid_value
                           : CodecErrorCode::missing_field,
                    "Amount",
                    std::string("'") + name + "' must be a string",
                    path);
            }
            return std::string_view(member->as_string());
        };

        if (v.is_string())
        {
            encode_native(s, std::string_view(v.as_string()), path);
        }
        else if (v.is_object())
        {
            auto const& obj = v.as_object();
            if (obj.contains("mpt_issuance_id"))
            {
                // rippled validJSONAsset: an MPT names no currency or issuer.
                if (obj.contains("currency") || obj.contains("issuer"))
                {
                    throw EncodeError(
                        CodecErrorCode::invalid_value,
                        "Amount",
                        "mpt_issuance_id excludes currency and issuer",
                        path);
                }
                if (!obj.contains("value"))
                {
                    throw EncodeError(
                        CodecErrorCode::missing_field,
                        "Amount",
                        "MPT missing 'value'",
                        path);
                }
                encode_mpt(
                    s, text(obj, "value"), text(obj, "mpt_issuance_id"), path);
            }
            else
            {
                if (!obj.contains("value") || !obj.contains("currency") ||
                    !obj.contains("issuer"))
                {
                    throw EncodeError(
                        CodecErrorCode::missing_field,
                        "Amount",
                        "IOU requires 'value', 'currency', and 'issuer'",
                        path);
                }
                encode_iou(
                    s,
                    text(obj, "value"),
                    text(obj, "currency"),
                    text(obj, "issuer"),
                    path,
                    native);
            }
        }
        else
        {
            throw EncodeError(
                CodecErrorCode::invalid_value,
                "Amount",
                "expected string or object",
                path);
        }
    }

    // Decode binary amount to JSON
    static boost::json::value
    decode(
        Slice const& data,
        std::string_view native = CurrencyCodec::default_native)
    {
        if (is_native_amount(data))
        {
            return boost::json::string(parse_native_drops_string(data));
        }
        // IOU: 48 bytes
        if (data.size() == 48)
        {
            IOUValue iou = IOUValue::from_bytes(data.data());
            Slice currency_slice = get_currency_raw(data);
            std::string issuer =
                base58::encode_account_id(data.data() + 28, 20);

            boost::json::object obj;
            obj["currency"] = CurrencyCodec::decode(currency_slice, native);
            obj["value"] = boost::json::string(iou.to_string());
            obj["issuer"] = boost::json::string(issuer);
            return obj;
        }
        // MPT: 33 bytes (1 flag + 8 value + 24 mptid)
        if (data.size() == 33)
        {
            uint64_t val = 0;
            for (int i = 1; i < 9; ++i)
            {
                val = (val << 8) | data.data()[i];
            }
            // rippled STAmount::getText: zero is "0" whatever the sign bit
            // says; otherwise the sign is the cleared positive bit.
            bool const negative = (data.data()[0] & 0x40) == 0;
            std::string text = std::to_string(val);
            if (negative && val != 0)
                text.insert(text.begin(), '-');
            boost::json::object obj;
            obj["value"] = boost::json::string(text);
            obj["mpt_issuance_id"] =
                boost::json::string(hex_encode(data.data() + 9, 24));
            return obj;
        }
        // Fallback
        return boost::json::string(hex_encode(data));
    }

private:
    static constexpr uint64_t IOU_BIT = 0x8000000000000000ULL;
    static constexpr uint64_t POS_BIT = 0x4000000000000000ULL;
    static constexpr uint64_t MANTISSA_MASK = 0x003FFFFFFFFFFFFFULL;
    static constexpr uint64_t MIN_MANTISSA = 1000000000000000ULL;
    static constexpr uint64_t MAX_MANTISSA = 9999999999999999ULL;

    /// The 8-byte issued value for a decimal in rippled's number grammar.
    /// A mantissa of more than 16 digits is rounded to nearest, ties to
    /// even, as rippled's Number does.
    static uint64_t
    encode_iou_value(std::string_view value_str, std::string const& path = {})
    {
        auto const parts = parse_decimal(value_str, "Amount", path);

        uint64_t mantissa = parts.mantissa;
        int64_t exponent = parts.exponent;

        if (mantissa == 0)
        {
            return IOU_BIT;
        }

        // rippled reads the digits as an unsigned 64-bit integer and then
        // treats them as signed, so digits above 2^63 - 1 come out as a
        // negative amount there. That is not a value to reproduce.
        if (mantissa > static_cast<uint64_t>(INT64_MAX))
        {
            throw EncodeError(
                CodecErrorCode::out_of_range,
                "Amount",
                "'" + std::string(value_str) + "' has too many digits",
                path);
        }

        // Normalize to [1e15, 1e16)
        while (mantissa < MIN_MANTISSA)
        {
            mantissa *= 10;
            --exponent;
        }
        unsigned first_dropped = 0;
        bool later_dropped = false;
        while (mantissa > MAX_MANTISSA)
        {
            later_dropped = later_dropped || first_dropped != 0;
            first_dropped = static_cast<unsigned>(mantissa % 10);
            mantissa /= 10;
            ++exponent;
        }
        if (first_dropped > 5 ||
            (first_dropped == 5 && (later_dropped || (mantissa & 1) == 1)))
        {
            ++mantissa;
            if (mantissa > MAX_MANTISSA)
            {
                mantissa /= 10;
                ++exponent;
            }
        }

        // Validate exponent range (-96 to 80)
        if (exponent < -96 || exponent > 80)
        {
            throw EncodeError(
                CodecErrorCode::out_of_range,
                "Amount",
                "IOU exponent out of range: " + std::to_string(exponent),
                path);
        }

        uint64_t raw = IOU_BIT;
        if (!parts.negative)
        {
            raw |= POS_BIT;
        }
        uint64_t biased_exp = static_cast<uint64_t>(exponent + 97);
        raw |= (biased_exp << 54);
        raw |= (mantissa & MANTISSA_MASK);
        return raw;
    }
};

}  // namespace catl::xdata::codecs
