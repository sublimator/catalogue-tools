#pragma once

#include <boost/json.hpp>
#include <boost/json/basic_parser_impl.hpp>
#include <boost/system/error_code.hpp>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

namespace xprv::detail {

// A syntax pass sees member tokens before the DOM's last-member-wins behavior
// can discard them. The parser supplies decoded keys, so escaped aliases count
// as the same member too. The subsequent DOM pass uses the same JSON grammar.
struct UniqueJsonMembers
{
    static constexpr size_t max_array_size = size_t(-1);
    static constexpr size_t max_object_size = size_t(-1);
    static constexpr size_t max_string_size = size_t(-1);
    static constexpr size_t max_key_size = size_t(-1);
    std::vector<std::set<std::string>> objects;
    std::string key;
    using Error = boost::system::error_code;
    using View = boost::json::string_view;

    bool
    on_object_begin(Error&)
    {
        objects.emplace_back();
        return true;
    }
    bool
    on_object_end(size_t, Error&)
    {
        objects.pop_back();
        return true;
    }
    bool
    on_key_part(View part, size_t, Error&)
    {
        key.append(part);
        return true;
    }
    bool
    on_key(View part, size_t, Error&)
    {
        key.append(part);
        if (!objects.back().insert(key).second)
            throw std::runtime_error(
                "proof JSON: duplicate member '" + key + "'");
        key.clear();
        return true;
    }
    bool
    on_document_begin(Error&)
    {
        return true;
    }
    bool
    on_document_end(Error&)
    {
        return true;
    }
    bool
    on_array_begin(Error&)
    {
        return true;
    }
    bool
    on_array_end(size_t, Error&)
    {
        return true;
    }
    bool
    on_string_part(View, size_t, Error&)
    {
        return true;
    }
    bool
    on_string(View, size_t, Error&)
    {
        return true;
    }
    bool
    on_number_part(View, Error&)
    {
        return true;
    }
    bool
    on_int64(int64_t, View, Error&)
    {
        return true;
    }
    bool
    on_uint64(uint64_t, View, Error&)
    {
        return true;
    }
    bool
    on_double(double, View, Error&)
    {
        return true;
    }
    bool
    on_bool(bool, Error&)
    {
        return true;
    }
    bool
    on_null(Error&)
    {
        return true;
    }
    bool
    on_comment_part(View, Error&)
    {
        return true;
    }
    bool
    on_comment(View, Error&)
    {
        return true;
    }
};

inline boost::json::value
parse_unique_json(std::string_view text)
{
    boost::json::basic_parser<UniqueJsonMembers> parser{boost::json::parse_options{}};
    boost::system::error_code error;
    parser.write_some(false, text.data(), text.size(), error);
    if (error)
        throw std::runtime_error("proof JSON: " + error.message());
    return boost::json::parse(text);
}

}  // namespace xprv::detail
