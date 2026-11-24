#pragma once

#include "catl/base58/base58.h"
#include "catl/xdata/codecs/account_id.h"
#include "catl/xdata/codecs/currency.h"
#include "catl/xdata/parser-error.h"
#include "catl/xdata/serializer.h"
#include "catl/xdata/types/pathset.h"
#include <boost/json.hpp>

namespace catl::xdata::codecs {

struct PathSetCodec
{
    // Size requires walking the JSON to count hops
    static size_t
    encoded_size(boost::json::value const& v)
    {
        size_t size = 0;
        auto const& paths = v.as_array();
        for (size_t p = 0; p < paths.size(); ++p)
        {
            if (p > 0)
                ++size;  // PATH_SEPARATOR
            for (auto const& hop : paths[p].as_array())
            {
                ++size;  // type byte
                auto const& obj = hop.as_object();
                if (obj.contains("account"))
                    size += 20;
                if (obj.contains("currency"))
                    size += 20;
                if (obj.contains("issuer"))
                    size += 20;
            }
        }
        ++size;  // END_BYTE
        return size;
    }

    /// A step's account or issuer is always 20 bytes on the wire. The zero
    /// account is 20 zero bytes here, not the empty value AccountIDCodec
    /// writes for a top-level account field.
    template <ByteSink Sink>
    static void
    encode_step_account(Serializer<Sink>& s, std::string_view base58_addr)
    {
        auto const decoded = base58::decode_account_id(base58_addr);
        if (!decoded || decoded->size() != 20)
        {
            throw EncodeError(
                CodecErrorCode::invalid_encoding,
                "PathSet",
                "invalid base58 address: " + std::string(base58_addr));
        }
        s.add_raw(std::span<const uint8_t>{decoded->data(), 20});
    }

    // From JSON: array of arrays of hop objects. A step's currency reads as
    // rippled's toCurrency does: the network's native code is the zero
    // currency and any other three-character code is itself.
    template <ByteSink Sink>
    static void
    encode(
        Serializer<Sink>& s,
        boost::json::value const& v,
        std::string_view native = CurrencyCodec::default_native)
    {
        // A path set holds at least one path, and a path at least one step.
        // rippled's JSON reader takes an empty one and writes bytes that its
        // own STPathSet reader then refuses ("empty path"), so there is no
        // serialized form of it to write.
        auto const refuse = [](char const* what) {
            throw EncodeError(CodecErrorCode::invalid_value, "PathSet", what);
        };
        auto const& paths = v.as_array();
        if (paths.empty())
            refuse("a path set holds at least one path");
        for (size_t p = 0; p < paths.size(); ++p)
        {
            if (p > 0)
            {
                s.add_path_separator();
            }
            if (paths[p].as_array().empty())
                refuse("a path holds at least one step");
            for (auto const& hop : paths[p].as_array())
            {
                auto const& obj = hop.as_object();
                // A step with none of the three would be written as the
                // byte that ends the set.
                if (!obj.contains("account") && !obj.contains("currency") &&
                    !obj.contains("issuer"))
                    refuse(
                        "a path step names an account, a currency or an "
                        "issuer");
                uint8_t type_byte = 0;
                if (obj.contains("account"))
                    type_byte |= PathSet::TYPE_ACCOUNT;
                if (obj.contains("currency"))
                    type_byte |= PathSet::TYPE_CURRENCY;
                if (obj.contains("issuer"))
                    type_byte |= PathSet::TYPE_ISSUER;

                s.add_u8(type_byte);

                if (obj.contains("account"))
                {
                    encode_step_account(
                        s, std::string_view(obj.at("account").as_string()));
                }
                if (obj.contains("currency"))
                {
                    uint8_t currency_raw[20];
                    CurrencyCodec::to_bytes(
                        std::string_view(obj.at("currency").as_string()),
                        currency_raw,
                        {},
                        native);
                    s.add_raw(std::span<const uint8_t>{currency_raw, 20});
                }
                if (obj.contains("issuer"))
                {
                    encode_step_account(
                        s, std::string_view(obj.at("issuer").as_string()));
                }
            }
        }
        s.add_pathset_end();
    }

    static boost::json::value
    decode(
        Slice const& data,
        std::string_view native = CurrencyCodec::default_native)
    {
        boost::json::array paths;
        boost::json::array current_path;

        size_t pos = 0;
        while (pos < data.size())
        {
            uint8_t type_byte = data.data()[pos++];

            // A path that ends with no step in it, whether at a separator
            // or at the end of the set: rippled's STPathSet reader refuses
            // it ("empty path"), and so does this one.
            if (type_byte == PathSet::END_BYTE ||
                type_byte == PathSet::PATH_SEPARATOR)
            {
                if (current_path.empty())
                    throw ParserError("PathSet: empty path");
                paths.push_back(std::move(current_path));
                current_path = boost::json::array();
                if (type_byte == PathSet::END_BYTE)
                    break;
                continue;
            }

            // rippled's STPathSet::getJson writes the step's type byte as
            // `type`, before the members that byte says are present.
            boost::json::object hop;
            hop["type"] = type_byte;

            if (type_byte & PathSet::TYPE_ACCOUNT)
            {
                if (pos + 20 <= data.size())
                {
                    Slice s(data.data() + pos, 20);
                    hop["account"] = AccountIDCodec::decode(s);
                    pos += 20;
                }
            }

            if (type_byte & PathSet::TYPE_CURRENCY)
            {
                if (pos + 20 <= data.size())
                {
                    Slice s(data.data() + pos, 20);
                    hop["currency"] = CurrencyCodec::decode(s, native);
                    pos += 20;
                }
            }

            if (type_byte & PathSet::TYPE_ISSUER)
            {
                if (pos + 20 <= data.size())
                {
                    Slice s(data.data() + pos, 20);
                    hop["issuer"] = AccountIDCodec::decode(s);
                    pos += 20;
                }
            }

            current_path.push_back(std::move(hop));
        }

        if (!current_path.empty())
        {
            paths.push_back(std::move(current_path));
        }

        return paths;
    }
};

}  // namespace catl::xdata::codecs
