#pragma once

#include <algorithm>
#include <array>
#include <boost/json.hpp>
#include <stdexcept>
#include <string>
#include <string_view>

namespace catl::shamap {

// A 32-byte hash as the JSON form spells it: 64 uppercase hex digits. Hex
// has one spelling, so lowercase is not a hash (XPRV §2).
inline bool
is_trie_hash(std::string_view value)
{
    return value.size() == 64 &&
        std::all_of(value.begin(), value.end(), [](char c) {
               return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F');
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

// Checks one node and everything beneath it, and returns the number of
// leaves there. `path` is the branch nibbles from the root to this node.
//
// A trie has one shape (XPRV §2.3). An empty branch is absent: it is never
// an all-zero hash and never an inner node with nothing in it. An inner node
// is present only because a leaf lies beneath it; a subtree with no leaf is
// its hash. A leaf sits where its key puts it. Without these rules the same
// tree has many spellings that hash to one root.
inline size_t
validate_json_trie(
    boost::json::value const& node,
    size_t depth = 0,
    std::string const& path = {})
{
    // §2.3 / §3.7: the root is an inner node. A bare leaf or hash has no
    // 4-byte branch header and is not a tree root.
    if (depth == 0 && !node.is_object())
        throw std::runtime_error("json trie: root must be an inner node");
    if (node.is_string())
    {
        auto const hash = std::string_view(node.as_string());
        if (!is_trie_hash(hash))
            throw std::runtime_error(
                "json trie: placeholder must be a 32-byte uppercase hex hash");
        if (hash.find_first_not_of('0') == std::string_view::npos)
            throw std::runtime_error(
                "json trie: placeholder is a zero hash; an empty branch is "
                "absent");
        return 0;
    }
    if (node.is_array())
    {
        auto const& leaf = node.as_array();
        if (leaf.size() != 2 || !leaf[0].is_string())
            throw std::runtime_error(
                "json trie: leaf must be exactly [key, data]");
        auto const key = std::string_view(leaf[0].as_string());
        if (!is_trie_hash(key))
            throw std::runtime_error(
                "json trie: leaf key must be a 32-byte uppercase hex hash");
        if (key.substr(0, path.size()) != path)
            throw std::runtime_error(
                "json trie: leaf key does not match its trie path");
        // XPRV v7 §2.3: JSON leaves are decoded only; a hex string is not
        // leaf data (the binary form carries exact bytes).
        if (!leaf[1].is_object())
            throw std::runtime_error(
                "json trie: leaf data must be a decoded object, not hex");
        return 1;
    }
    if (node.is_object())
    {
        static constexpr char const* kNibble = "0123456789ABCDEF";
        size_t leaves = 0;
        auto const branches = json_trie_branches(node.as_object(), depth);
        for (size_t i = 0; i < branches.size(); ++i)
            if (branches[i])
                leaves += validate_json_trie(
                    *branches[i], depth + 1, path + kNibble[i]);
        if (leaves == 0)
            throw std::runtime_error(
                "json trie: inner node contains no leaves");
        return leaves;
    }
    throw std::runtime_error("json trie: unsupported node type");
}

}  // namespace catl::shamap
