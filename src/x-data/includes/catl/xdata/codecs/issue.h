#pragma once

#include "catl/xdata/codec-error.h"
#include "catl/xdata/codecs/account_id.h"
#include "catl/xdata/codecs/currency.h"
#include "catl/xdata/serializer.h"
#include "catl/xdata/types/issue.h"
#include <boost/json.hpp>

namespace catl::xdata::codecs {

struct IssueCodec
{
    static size_t
    encoded_size(boost::json::value const& v)
    {
        if (v.is_string())
            return 20;
        auto const& obj = v.as_object();
        if (!obj.contains("issuer"))
            return 20;  // native
        return 40;
    }

    template <ByteSink Sink>
    static void
    encode_native(Serializer<Sink>& s)
    {
        s.add_issue_native();
    }

    template <ByteSink Sink>
    static void
    encode(
        Serializer<Sink>& s,
        std::string_view currency,
        std::string_view issuer,
        std::string const& path = {})
    {
        CurrencyCodec::encode(s, currency, path);
        AccountIDCodec::encode(s, issuer, path);
    }

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
                    "string value must be XRP or XAH, got: " + std::string(sv),
                    path);
            }
        }
        else if (v.is_object())
        {
            auto const& obj = v.as_object();
            if (!obj.contains("currency"))
            {
                throw EncodeError(
                    CodecErrorCode::missing_field,
                    "Issue",
                    "missing 'currency'",
                    path);
            }
            auto currency = std::string_view(obj.at("currency").as_string());
            if (!obj.contains("issuer"))
            {
                encode_native(s);
            }
            else
            {
                auto issuer = std::string_view(obj.at("issuer").as_string());
                encode(s, currency, issuer, path);
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
        auto parsed = parse_issue(data);
        if (parsed.is_native())
        {
            return CurrencyCodec::decode(parsed.currency);
        }
        boost::json::object obj;
        obj["currency"] = CurrencyCodec::decode(parsed.currency);
        obj["issuer"] = AccountIDCodec::decode(parsed.issuer);
        return obj;
    }
};

}  // namespace catl::xdata::codecs
