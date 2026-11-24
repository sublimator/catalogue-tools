#pragma once

#include "catl/xdata/codec-error.h"
#include "catl/xdata/hex.h"
#include "catl/xdata/serializer.h"
#include <boost/json.hpp>
#include <cstring>

namespace catl::xdata::codecs {

// A currency is 160 bits. Its text follows rippled's to_string(Currency) and
// toCurrency (src/libxrpl/protocol/UintTypes.cpp):
//
//   all zero                      the network's native code ("XRP", "XAH")
//   12 zero bytes, 3 code bytes,  the three characters, when each is in the
//   5 zero bytes                  ISO character set and they are not the
//                                 native code
//   anything else                 40 uppercase hex digits
//
// Which code is native depends on the network, so the code is a parameter:
// "XAH" is an ordinary three-character code on XRPL and "XRP" is one on Xahau.
// The dispatch in codecs.h passes Protocol::native_currency().
struct CurrencyCodec
{
    static constexpr size_t fixed_size = 20;

    static constexpr std::string_view default_native = "XRP";

    // rippled detail::kIsoCharSet: the characters a three-character code may
    // be written with.
    static constexpr std::string_view iso_charset =
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "0123456789"
        "<>(){}[]|?!@#$%^&*";

    static constexpr size_t iso_offset = 12;
    static constexpr size_t iso_length = 3;

    static bool
    is_iso_code(std::string_view code)
    {
        return code.size() == iso_length &&
            code.find_first_not_of(iso_charset) == std::string_view::npos;
    }

    static size_t
    encoded_size(boost::json::value const&)
    {
        return 20;
    }

    // From raw 20 bytes
    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, std::span<const uint8_t, 20> data)
    {
        s.add_raw(std::span<const uint8_t>{data.data(), 20});
    }

    /// The 20 bytes `currency` names, as rippled's toCurrency reads it. The
    /// empty string and the native code are the zero currency; three ISO
    /// characters are a code; 40 hex digits are the bytes themselves.
    static void
    to_bytes(
        std::string_view currency,
        uint8_t out[20],
        std::string const& path = {},
        std::string_view native = default_native)
    {
        std::memset(out, 0, 20);

        if (currency.empty() || currency == native)
            return;

        if (currency.size() == iso_length)
        {
            if (!is_iso_code(currency))
            {
                throw EncodeError(
                    CodecErrorCode::invalid_value,
                    "Currency",
                    "invalid currency: " + std::string(currency),
                    path);
            }
            std::memcpy(out + iso_offset, currency.data(), iso_length);
            return;
        }

        if (currency.size() == 40)
        {
            for (size_t i = 0; i < 20; ++i)
            {
                auto const hi = hex_nibble(currency[i * 2]);
                auto const lo = hex_nibble(currency[i * 2 + 1]);
                if (hi == 0xFF || lo == 0xFF)
                {
                    throw EncodeError(
                        CodecErrorCode::invalid_encoding,
                        "Currency",
                        "invalid currency: " + std::string(currency),
                        path);
                }
                out[i] = static_cast<uint8_t>((hi << 4) | lo);
            }
            return;
        }

        throw EncodeError(
            CodecErrorCode::invalid_value,
            "Currency",
            "invalid currency: " + std::string(currency),
            path);
    }

    static bool
    is_zero(uint8_t const bytes[20])
    {
        for (size_t i = 0; i < 20; ++i)
            if (bytes[i] != 0)
                return false;
        return true;
    }

    /// rippled's noCurrency(): the value 1, a placeholder and not a currency.
    static bool
    is_no_currency(uint8_t const bytes[20])
    {
        for (size_t i = 0; i < 19; ++i)
            if (bytes[i] != 0)
                return false;
        return bytes[19] == 1;
    }

    /// True when `currency` is the native code spelled as a three-character
    /// code, which rippled reserves (badCurrency()). rippled compares it
    /// exactly; xahaud's isBadCurrency also catches its case variants.
    static bool
    is_reserved_code(
        uint8_t const currency[20],
        std::string_view native = default_native)
    {
        for (size_t i = 0; i < 20; ++i)
        {
            if (i >= iso_offset && i < iso_offset + iso_length)
                continue;
            if (currency[i] != 0)
                return false;
        }
        bool const any_case = native != default_native;
        for (size_t i = 0; i < iso_length; ++i)
        {
            auto c = currency[iso_offset + i];
            if (any_case)
                c &= 0xDF;
            if (c != static_cast<uint8_t>(native[i]))
                return false;
        }
        return true;
    }

    // From string, for a Currency field. As rippled's currencyFromJson, the
    // field takes neither the reserved code nor noCurrency. An issued amount
    // and a path step are less strict and use to_bytes().
    template <ByteSink Sink>
    static void
    encode(
        Serializer<Sink>& s,
        std::string_view currency,
        std::string const& path = {},
        std::string_view native = default_native)
    {
        uint8_t buf[20];
        to_bytes(currency, buf, path, native);
        if (is_no_currency(buf) || is_reserved_code(buf, native))
        {
            throw EncodeError(
                CodecErrorCode::invalid_value,
                "Currency",
                "currency must be a valid currency: " + std::string(currency),
                path);
        }
        s.add_raw(std::span<const uint8_t>{buf, 20});
    }

    // From JSON string
    template <ByteSink Sink>
    static void
    encode(
        Serializer<Sink>& s,
        boost::json::value const& v,
        std::string const& path = {},
        std::string_view native = default_native)
    {
        if (!v.is_string())
        {
            throw EncodeError(
                CodecErrorCode::invalid_value,
                "Currency",
                "expected string",
                path);
        }
        encode(s, std::string_view(v.as_string()), path, native);
    }

    /// The text of a 20-byte currency, as rippled's to_string(Currency).
    static std::string
    to_text(uint8_t const data[20], std::string_view native = default_native)
    {
        if (is_zero(data))
            return std::string(native);

        // noCurrency has a text of its own and no way back: toCurrency
        // refuses "1".
        if (is_no_currency(data))
            return "1";

        bool iso_pattern = true;
        for (size_t i = 0; i < 20 && iso_pattern; ++i)
        {
            if (i < iso_offset || i >= iso_offset + iso_length)
                iso_pattern = data[i] == 0;
        }
        if (iso_pattern)
        {
            std::string_view const iso(
                reinterpret_cast<char const*>(data) + iso_offset, iso_length);
            // The native code is never written in the three-character form.
            if (iso != native && is_iso_code(iso))
                return std::string(iso);
        }

        return hex_encode(data, 20);
    }

    // Decode 20-byte currency to JSON string
    static boost::json::value
    decode(Slice const& data, std::string_view native = default_native)
    {
        if (data.size() != 20)
            return boost::json::string(hex_encode(data));
        return boost::json::string(to_text(data.data(), native));
    }
};

}  // namespace catl::xdata::codecs
