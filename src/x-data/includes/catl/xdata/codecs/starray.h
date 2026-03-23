#pragma once

#include "catl/xdata/codecs/stobject.h"
#include "catl/xdata/protocol.h"
#include "catl/xdata/serializer.h"
#include <boost/json.hpp>

namespace catl::xdata::codecs {

struct STArrayCodec
{
    static size_t
    encoded_size(boost::json::value const& v, Protocol const& protocol)
    {
        size_t size = 0;
        for (auto const& elem : v.as_array())
        {
            auto const& wrapper = elem.as_object();
            for (auto const& [key, inner] : wrapper)
            {
                auto field_opt = protocol.find_field(std::string(key));
                if (!field_opt)
                    continue;
                auto type_code = get_field_type_code(field_opt->code);
                auto field_id = get_field_id(field_opt->code);
                if (type_code < 16 && field_id < 16)
                    size += 1;
                else if (type_code < 16 || field_id < 16)
                    size += 2;
                else
                    size += 3;
                size += STObjectCodec::encoded_size(inner, protocol, false);
                size += 1;  // ObjectEndMarker
            }
        }
        return size;
    }

    template <ByteSink Sink>
    static void
    encode(
        Serializer<Sink>& s,
        boost::json::value const& v,
        Protocol const& protocol,
        std::string const& path = {})
    {
        size_t idx = 0;
        for (auto const& elem : v.as_array())
        {
            auto const& wrapper = elem.as_object();
            for (auto const& [key, inner] : wrapper)
            {
                auto field_opt = protocol.find_field(std::string(key));
                if (!field_opt)
                    continue;

                auto elem_path = path + "[" + std::to_string(idx) + "]." +
                                 std::string(key);

                s.add_field_header(*field_opt);
                STObjectCodec::encode(
                    s, inner, protocol, false, elem_path);
                s.add_object_end_marker();
            }
            ++idx;
        }
    }
};

}  // namespace catl::xdata::codecs
