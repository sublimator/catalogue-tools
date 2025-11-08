#pragma once

#include <boost/json/serialize.hpp>
#include <boost/json/value.hpp>
#include <ostream>
#include <string>

namespace catl::xdata::json {

/**
 * Pretty-print a JSON value to an output stream with proper indentation.
 *
 * @param os Output stream to write to
 * @param jv JSON value to print
 * @param indent Current indentation level (internal use, leave null for
 * top-level call)
 */
inline void
pretty_print(
    std::ostream& os,
    const boost::json::value& jv,
    std::string* indent = nullptr)
{
    std::string indent_;
    if (!indent)
        indent = &indent_;

    switch (jv.kind())
    {
        case boost::json::kind::object: {
            os << "{\n";
            indent->append(4, ' ');
            const auto& obj = jv.get_object();
            for (auto it = obj.begin(); it != obj.end(); ++it)
            {
                os << *indent << boost::json::serialize(it->key()) << " : ";
                pretty_print(os, it->value(), indent);
                if (std::next(it) != obj.end())
                    os << ",";
                os << "\n";
            }
            indent->resize(indent->size() - 4);
            os << *indent << "}";
            break;
        }
        case boost::json::kind::array: {
            os << "[\n";
            indent->append(4, ' ');
            const auto& arr = jv.get_array();
            for (auto it = arr.begin(); it != arr.end(); ++it)
            {
                os << *indent;
                pretty_print(os, *it, indent);
                if (std::next(it) != arr.end())
                    os << ",";
                os << "\n";
            }
            indent->resize(indent->size() - 4);
            os << *indent << "]";
            break;
        }
        case boost::json::kind::string:
            os << boost::json::serialize(jv.get_string());
            break;
        case boost::json::kind::uint64:
            os << jv.get_uint64();
            break;
        case boost::json::kind::int64:
            os << jv.get_int64();
            break;
        case boost::json::kind::double_:
            os << jv.get_double();
            break;
        case boost::json::kind::bool_:
            os << (jv.get_bool() ? "true" : "false");
            break;
        case boost::json::kind::null:
            os << "null";
            break;
    }

    if (indent->empty())
        os << "\n";
}

}  // namespace catl::xdata::json
