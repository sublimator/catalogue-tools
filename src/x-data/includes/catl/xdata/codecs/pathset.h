#pragma once

#include "catl/xdata/codecs/account_id.h"
#include "catl/xdata/codecs/currency.h"
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

    // From JSON: array of arrays of hop objects
    template <ByteSink Sink>
    static void
    encode(Serializer<Sink>& s, boost::json::value const& v)
    {
        auto const& paths = v.as_array();
        for (size_t p = 0; p < paths.size(); ++p)
        {
            if (p > 0)
            {
                s.add_path_separator();
            }
            for (auto const& hop : paths[p].as_array())
            {
                auto const& obj = hop.as_object();
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
                    AccountIDCodec::encode(
                        s, std::string_view(obj.at("account").as_string()));
                }
                if (obj.contains("currency"))
                {
                    CurrencyCodec::encode(
                        s, std::string_view(obj.at("currency").as_string()));
                }
                if (obj.contains("issuer"))
                {
                    AccountIDCodec::encode(
                        s, std::string_view(obj.at("issuer").as_string()));
                }
            }
        }
        s.add_pathset_end();
    }

    static boost::json::value
    decode(Slice const& data)
    {
        boost::json::array paths;
        boost::json::array current_path;

        size_t pos = 0;
        while (pos < data.size())
        {
            uint8_t type_byte = data.data()[pos++];

            if (type_byte == PathSet::END_BYTE)
            {
                if (!current_path.empty())
                {
                    paths.push_back(std::move(current_path));
                }
                break;
            }

            if (type_byte == PathSet::PATH_SEPARATOR)
            {
                if (!current_path.empty())
                {
                    paths.push_back(std::move(current_path));
                    current_path = boost::json::array();
                }
                continue;
            }

            boost::json::object hop;

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
                    hop["currency"] = CurrencyCodec::decode(s);
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

            if (!hop.empty())
            {
                current_path.push_back(std::move(hop));
            }
        }

        if (!current_path.empty())
        {
            paths.push_back(std::move(current_path));
        }

        return paths;
    }
};

}  // namespace catl::xdata::codecs
