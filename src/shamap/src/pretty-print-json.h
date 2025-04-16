#pragma once

#include <boost/json/serialize.hpp>
#include <boost/json/value.hpp>
#include <ostream>
#include <string>

namespace json = boost::json;

inline void
pretty_print_json(
    std::ostream& os,
    const json::value& jv,
    std::string* indent = nullptr)
{
    std::string indent_;
    if (!indent)
        indent = &indent_;

    switch (jv.kind())
    {
        case json::kind::object: {
            os << "{\n";
            indent->append(4, ' ');
            const auto& obj = jv.get_object();
            for (auto it = obj.begin(); it != obj.end(); ++it)
            {
                os << *indent << json::serialize(it->key()) << " : ";
                pretty_print_json(os, it->value(), indent);
                if (std::next(it) != obj.end())
                    os << ",";
                os << "\n";
            }
            indent->resize(indent->size() - 4);
            os << *indent << "}";
            break;
        }
        case json::kind::array: {
            os << "[\n";
            indent->append(4, ' ');
            const auto& arr = jv.get_array();
            for (auto it = arr.begin(); it != arr.end(); ++it)
            {
                os << *indent;
                pretty_print_json(os, *it, indent);
                if (std::next(it) != arr.end())
                    os << ",";
                os << "\n";
            }
            indent->resize(indent->size() - 4);
            os << *indent << "]";
            break;
        }
        case json::kind::string:
            os << json::serialize(jv.get_string());
            break;
        case json::kind::uint64:
            os << jv.get_uint64();
            break;
        case json::kind::int64:
            os << jv.get_int64();
            break;
        case json::kind::double_:
            os << jv.get_double();
            break;
        case json::kind::bool_:
            os << (jv.get_bool() ? "true" : "false");
            break;
        case json::kind::null:
            os << "null";
            break;
    }

    if (indent->empty())
        os << "\n";
}
