#pragma once

#include <algorithm>
#include <array>
#include <boost/json.hpp>
#include <stdexcept>
#include <string_view>

namespace catl::shamap {

inline bool
is_trie_hash(std::string_view value)
{
    return value.size() == 64 &&
        std::all_of(value.begin(), value.end(), [](char c) {
               return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') ||
                   (c >= 'a' && c <= 'f');
           });
}

// All JSON trie consumers use this branch grammar. In particular diagnostic
// metadata must never be mistaken for an authenticated branch, or vice versa.
inline std::array<boost::json::value const*, 16>
json_trie_branches(boost::json::object const& node, size_t depth)
{
    if (depth >= 64)
        throw std::runtime_error("json trie: depth exceeds maximum (64)");
    std::array<boost::json::value const*, 16> branches{};
    for (auto const& kv : node)
    {
        if (kv.key() == "__depth__")
        {
            if ((!kv.value().is_int64() && !kv.value().is_uint64()) ||
                kv.value().to_number<int64_t>() != static_cast<int64_t>(depth))
                throw std::runtime_error(
                    "json trie: __depth__ differs from nesting depth");
            continue;
        }
        auto key = kv.key();
        if (key.size() != 1 ||
            !((key[0] >= '0' && key[0] <= '9') ||
              (key[0] >= 'A' && key[0] <= 'F')))
            throw std::runtime_error(
                "json trie: branch keys must be uppercase 0-9A-F");
        auto index = key[0] <= '9' ? key[0] - '0' : key[0] - 'A' + 10;
        if (branches[index])
            throw std::runtime_error("json trie: duplicate branch");
        branches[index] = &kv.value();
    }
    return branches;
}

inline void
validate_json_trie(boost::json::value const& node, size_t depth = 0)
{
    if (node.is_string())
    {
        if (!is_trie_hash(node.as_string()))
            throw std::runtime_error(
                "json trie: placeholder must be a 32-byte hex hash");
    }
    else if (node.is_array())
    {
        auto const& leaf = node.as_array();
        if (leaf.size() != 2 || !leaf[0].is_string() ||
            !is_trie_hash(leaf[0].as_string()) ||
            (!leaf[1].is_object() && !leaf[1].is_string()))
            throw std::runtime_error(
                "json trie: leaf must be exactly [key, data]");
    }
    else if (node.is_object())
    {
        for (auto const* child : json_trie_branches(node.as_object(), depth))
            if (child)
                validate_json_trie(*child, depth + 1);
    }
    else
        throw std::runtime_error("json trie: unsupported node type");
}

}  // namespace catl::shamap
