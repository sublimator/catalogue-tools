#include "xprv/proof-chain-json.h"
#include "json-proof-loader.h"
#include "xprv/hex-utils.h"
#include "xprv/network-config.h"

#include <catl/shamap/binary-trie.h>
#include <catl/shamap/json-trie.h>
#include <catl/xdata/parse_leaf.h>
#include <catl/xdata/parse_transaction.h>
#include <catl/xdata/serialize.h>

#include <algorithm>
#include <cctype>
#include <cstring>
#include <initializer_list>
#include <span>

namespace xprv {

//------------------------------------------------------------------------------
// Serialize
//------------------------------------------------------------------------------

static boost::json::object
anchor_to_json(AnchorData const& a)
{
    boost::json::object obj;
    obj["type"] = "anchor";
    obj["ledger_hash"] = upper_hex(a.ledger_hash);
    obj["ledger_index"] = a.ledger_index;

    if (!a.publisher_key_hex.empty())
    {
        // An anchor without its list carries only the publisher key, the
        // JSON counterpart of a binary proof without 0x05 (§2.1, §3.3).
        boost::json::object unl;
        unl["public_key"] = a.publisher_key_hex;
        if (!a.blob.empty())
        {
            unl["manifest"] = bytes_hex(a.publisher_manifest);
            unl["blob"] = bytes_hex(a.blob);
            unl["signature"] = bytes_hex(a.blob_signature);
        }
        obj["unl"] = unl;
    }

    boost::json::object validations;
    for (auto const& [key, val] : a.validations)
    {
        validations[key] = bytes_hex(val);
    }
    obj["validations"] = validations;

    return obj;
}

static boost::json::object
header_to_json(HeaderData const& h)
{
    boost::json::object obj;
    obj["type"] = "ledger_header";
    boost::json::object hdr;
    hdr["ledger_index"] = h.seq;
    hdr["total_coins"] = std::to_string(h.drops);
    hdr["parent_hash"] = upper_hex(h.parent_hash);
    hdr["transaction_hash"] = upper_hex(h.tx_hash);
    hdr["account_hash"] = upper_hex(h.account_hash);
    hdr["parent_close_time"] = h.parent_close_time;
    hdr["close_time"] = h.close_time;
    hdr["close_time_resolution"] = h.close_time_resolution;
    hdr["close_flags"] = h.close_flags;
    obj["header"] = hdr;
    return obj;
}

static std::string
nibble_key(int branch)
{
    static constexpr char const* kNibble = "0123456789ABCDEF";
    return std::string(1, kNibble[branch]);
}

// Binary trie leaf bytes are the hashed item, with the key stored beside
// them. The xdata parsers want that key appended and no hash prefix, which
// is the same arrangement proof building uses. index/hash/blob are parser
// conveniences, not leaf content (spec §2.3). A leaf that cannot be decoded,
// or whose decoded object does not serialize back to these exact bytes, is
// an error: convert must not invent a raw-hex JSON leaf.
static boost::json::array
decoded_leaf_json(
    std::span<const uint8_t> key,
    std::span<const uint8_t> leaf_bytes,
    bool is_tx,
    catl::xdata::Protocol const& protocol)
{
    if (key.size() != 32)
        throw std::runtime_error(
            "map proof: binary trie leaf key is not 32 bytes");
    auto const key_hex = upper_hex(Hash256(key.data()));

    std::vector<uint8_t> with_key(leaf_bytes.begin(), leaf_bytes.end());
    with_key.insert(with_key.end(), key.begin(), key.end());
    Slice full(with_key.data(), with_key.size());

    catl::xdata::JsonVisitor::Options json_opts;
    json_opts.ascii_hints = false;

    boost::json::value decoded;
    try
    {
        decoded = is_tx ? catl::xdata::parse_transaction(
                              full,
                              protocol,
                              {.includes_prefix = false, .json_opts = json_opts})
                        : catl::xdata::parse_leaf(
                              full,
                              protocol,
                              {.includes_prefix = false,
                               .json_opts = json_opts});
    }
    catch (std::exception const& e)
    {
        throw std::runtime_error(
            "map proof: binary trie leaf " + key_hex +
            " did not decode: " + e.what());
    }
    if (!decoded.is_object())
        throw std::runtime_error(
            "map proof: binary trie leaf " + key_hex +
            " did not decode: leaf is not an object");

    auto& obj = decoded.as_object();
    obj.erase("index");
    obj.erase("hash");
    obj.erase("blob");
    require_codec_members(decoded, is_tx, protocol);

    std::vector<uint8_t> again;
    try
    {
        again = is_tx ? catl::xdata::serialize_transaction(obj, protocol)
                      : catl::xdata::serialize_leaf(obj, protocol);
    }
    catch (std::exception const& e)
    {
        throw std::runtime_error(
            "map proof: binary trie leaf " + key_hex +
            " did not re-encode to its exact bytes: " + e.what());
    }
    if (again.size() != leaf_bytes.size() ||
        std::memcmp(again.data(), leaf_bytes.data(), again.size()) != 0)
        throw std::runtime_error(
            "map proof: binary trie leaf " + key_hex +
            " did not re-encode to its exact bytes");

    boost::json::array arr;
    arr.emplace_back(key_hex);
    arr.emplace_back(std::move(decoded));
    return arr;
}

static boost::json::object
binary_trie_to_json(
    std::span<const uint8_t> data,
    size_t& pos,
    int depth,
    bool is_tx,
    catl::xdata::Protocol const& protocol)
{
    // Same bound as the JSON grammar (json-trie.h) and §3.8: a SHAMap key has
    // 64 nibbles, so no inner node sits at depth 64 or deeper.
    if (depth >= 64)
        throw std::runtime_error("map proof: binary trie depth exceeds 64");
    auto const header = catl::shamap::read_u32_le(data, pos);
    boost::json::object node;
    node["__depth__"] = depth;

    for (int branch = 0; branch < 16; ++branch)
    {
        auto const type = catl::shamap::get_branch_type(header, branch);
        if (type == catl::shamap::BranchType::empty)
            continue;

        auto const branch_key = nibble_key(branch);
        if (type == catl::shamap::BranchType::leaf)
        {
            if (pos + 32 > data.size())
                throw std::runtime_error("map proof: binary trie truncated");
            auto const leaf_key = data.subspan(pos, 32);
            pos += 32;
            auto const len = catl::shamap::leb128_decode(data, pos);
            if (pos + len > data.size())
                throw std::runtime_error("map proof: binary trie truncated");
            auto const leaf_bytes = data.subspan(pos, len);
            pos += len;
            node[branch_key] =
                decoded_leaf_json(leaf_key, leaf_bytes, is_tx, protocol);
        }
        else if (type == catl::shamap::BranchType::inner)
        {
            node[branch_key] =
                binary_trie_to_json(data, pos, depth + 1, is_tx, protocol);
        }
        else
        {
            if (pos + 32 > data.size())
                throw std::runtime_error("map proof: binary trie truncated");
            Hash256 placeholder(data.data() + pos);
            pos += 32;
            node[branch_key] = upper_hex(placeholder);
        }
    }
    return node;
}

static boost::json::object
json_trie_from_binary(
    std::span<const uint8_t> data,
    bool is_tx,
    catl::xdata::Protocol const& protocol)
{
    size_t pos = 0;
    auto node = binary_trie_to_json(data, pos, 0, is_tx, protocol);
    if (pos != data.size())
        throw std::runtime_error("map proof: binary trie has trailing bytes");
    return node;
}

static boost::json::object
trie_to_json(TrieData const& t, uint32_t const* network_id)
{
    if (!t.json_unavailable.empty())
        throw JsonFormUnavailable(t.json_unavailable);
    boost::json::object obj;
    obj["type"] = "map_proof";
    obj["tree"] = (t.tree == TrieData::TreeType::state) ? "state" : "tx";
    // A chain loaded from binary has the compact trie and no JSON yet.
    // step_to_json passes a null network so this stays out of http-server.
    if (t.trie_json.is_null() && network_id != nullptr &&
        !t.trie_binary.empty())
    {
        auto const protocol =
            NetworkConfig::for_network(*network_id).load_protocol();
        bool const is_tx = t.tree == TrieData::TreeType::tx;
        obj["trie"] = json_trie_from_binary(t.trie_binary, is_tx, protocol);
    }
    else
    {
        obj["trie"] = t.trie_json;
    }
    return obj;
}

static boost::json::array
steps_to_json(ProofChain const& chain)
{
    boost::json::array arr;
    for (auto const& s : chain.steps)
    {
        std::visit(
            [&](auto const& data) {
                using T = std::decay_t<decltype(data)>;
                if constexpr (std::is_same_v<T, AnchorData>)
                {
                    arr.push_back(anchor_to_json(data));
                }
                else if constexpr (std::is_same_v<T, HeaderData>)
                {
                    arr.push_back(header_to_json(data));
                }
                else if constexpr (std::is_same_v<T, TrieData>)
                {
                    arr.push_back(trie_to_json(data, &chain.network_id));
                }
            },
            s);
    }
    return arr;
}

boost::json::object
step_to_json(ChainStep const& step)
{
    return std::visit(
        [](auto const& data) -> boost::json::object {
            using T = std::decay_t<decltype(data)>;
            if constexpr (std::is_same_v<T, AnchorData>)
                return anchor_to_json(data);
            else if constexpr (std::is_same_v<T, HeaderData>)
                return header_to_json(data);
            else
                return trie_to_json(data, nullptr);
        },
        step);
}

static void
require_stobject_members(
    boost::json::value const& value,
    catl::xdata::Protocol const& protocol,
    std::string const& path)
{
    namespace types = catl::xdata::FieldTypes;
    if (!value.is_object())
        throw std::runtime_error(
            "map proof: leaf member " + path + " must be an object");
    for (auto const& [key, member] : value.as_object())
    {
        auto const name = std::string(key);
        auto const at = path.empty() ? name : path + "." + name;
        auto const field = protocol.find_field(name);
        if (!field || !field->meta.is_serialized)
            throw std::runtime_error(
                "map proof: leaf member " + at + " is not a serialized field");
        if (field->meta.type == types::STObject)
            require_stobject_members(member, protocol, at);
        else if (field->meta.type == types::STArray)
        {
            if (!member.is_array())
                throw std::runtime_error(
                    "map proof: leaf member " + at + " must be an array");
            std::size_t index = 0;
            for (auto const& element : member.as_array())
            {
                auto const element_at = at + "[" + std::to_string(index++) + "]";
                if (!element.is_object() || element.as_object().size() != 1)
                    throw std::runtime_error(
                        "map proof: leaf member " + element_at +
                        " must wrap exactly one object");
                auto const& [wrapper_key, inner] = *element.as_object().begin();
                auto const wrapper = std::string(wrapper_key);
                auto const wrapper_field = protocol.find_field(wrapper);
                if (!wrapper_field || !wrapper_field->meta.is_serialized ||
                    !(wrapper_field->meta.type == types::STObject))
                    throw std::runtime_error(
                        "map proof: leaf member " + element_at + "." + wrapper +
                        " is not a serialized object field");
                require_stobject_members(
                    inner, protocol, element_at + "." + wrapper);
            }
        }
    }
}

void
require_codec_members(
    boost::json::value const& leaf,
    bool is_tx,
    catl::xdata::Protocol const& protocol)
{
    if (!is_tx)
        return require_stobject_members(leaf, protocol, "");
    if (!leaf.is_object() || leaf.as_object().size() != 2 ||
        !leaf.as_object().contains("tx") || !leaf.as_object().contains("meta"))
        throw std::runtime_error(
            "map proof: a tx leaf must be exactly {tx, meta}");
    require_stobject_members(leaf.at("tx"), protocol, "tx");
    require_stobject_members(leaf.at("meta"), protocol, "meta");
}

boost::json::object
to_json(ProofChain const& chain)
{
    boost::json::object obj;
    obj["format_version"] = XPRV_FORMAT_VERSION;
    obj["network_id"] = chain.network_id;
    obj["steps"] = steps_to_json(chain);
    return obj;
}

//------------------------------------------------------------------------------
// Deserialize
//------------------------------------------------------------------------------

// The JSON form is closed (§2.4): an object carries exactly its members.
static void
require_members(
    boost::json::object const& obj,
    std::initializer_list<std::string_view> names,
    std::string_view what)
{
    for (auto const& kv : obj)
    {
        if (std::find(names.begin(), names.end(), kv.key()) == names.end())
            throw std::runtime_error(
                "proof JSON: unknown member " + std::string(kv.key()) +
                " in " + std::string(what));
    }
    for (auto const name : names)
    {
        if (!obj.contains(name))
            throw std::runtime_error(
                "proof JSON: " + std::string(what) + " is missing " +
                std::string(name));
    }
}

// An integer field is an integer token in range; 7.0 is not one.
static uint64_t
integer_member(
    boost::json::object const& obj,
    std::string_view name,
    uint64_t max,
    std::string_view what)
{
    auto const& v = obj.at(name);
    if (v.is_uint64() && v.get_uint64() <= max)
        return v.get_uint64();
    if (v.is_int64() && v.get_int64() >= 0 &&
        static_cast<uint64_t>(v.get_int64()) <= max)
        return static_cast<uint64_t>(v.get_int64());
    throw std::runtime_error(
        "proof JSON: " + std::string(what) + " " + std::string(name) +
        " must be an integer no greater than " + std::to_string(max));
}

static constexpr uint64_t kUint32Max = 0xFFFFFFFFu;
static constexpr uint64_t kUint8Max = 0xFFu;

static AnchorData
anchor_from_json(boost::json::object const& obj)
{
    require_members(
        obj,
        {"type", "ledger_hash", "ledger_index", "unl", "validations"},
        "anchor");
    AnchorData a;
    a.ledger_hash =
        hash_from_hex(std::string(obj.at("ledger_hash").as_string()));
    a.ledger_index = static_cast<uint32_t>(
        integer_member(obj, "ledger_index", kUint32Max, "anchor"));

    auto const& unl = obj.at("unl").as_object();
    a.publisher_key_hex = std::string(unl.at("public_key").as_string());
    if (from_hex(a.publisher_key_hex).size() != 33)
        throw std::runtime_error(
            "proof JSON: publisher key must be exactly 33 bytes");
    // The list is bundled whole or not at all: manifest, blob and signature
    // together, or only the publisher key (§2.1).
    auto const bundled = unl.contains("manifest") + unl.contains("blob") +
        unl.contains("signature");
    if (bundled != 0 && bundled != 3)
        throw std::runtime_error(
            "proof JSON: unl must carry manifest, blob and signature together, "
            "or none of them");
    if (unl.size() != static_cast<std::size_t>(1 + bundled))
        throw std::runtime_error("proof JSON: unknown member in unl");
    if (bundled == 3)
    {
        a.publisher_manifest =
            from_hex(std::string(unl.at("manifest").as_string()));
        a.blob = from_hex(std::string(unl.at("blob").as_string()));
        a.blob_signature =
            from_hex(std::string(unl.at("signature").as_string()));
        if (a.publisher_manifest.empty() || a.blob.empty() ||
            a.blob_signature.empty())
            throw std::runtime_error(
                "proof JSON: a bundled list needs a non-empty manifest, blob "
                "and signature");
    }

    auto const& validations = obj.at("validations").as_object();
    for (auto const& kv : validations)
    {
        auto const key = std::string(kv.key());
        if (from_hex(key).size() != 33 || !kv.value().is_string() ||
            !std::all_of(key.begin(), key.end(), [](char c) {
                return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F');
            }))
            throw std::runtime_error(
                "proof JSON: malformed validation key or value");
        a.validations[key] =
            from_hex(std::string(kv.value().as_string()));
    }

    return a;
}

static HeaderData
header_from_json(boost::json::object const& obj)
{
    require_members(obj, {"type", "header"}, "ledger_header step");
    HeaderData h;
    auto const& hdr = obj.at("header").as_object();
    require_members(
        hdr,
        {"ledger_index",
         "total_coins",
         "parent_hash",
         "transaction_hash",
         "account_hash",
         "parent_close_time",
         "close_time",
         "close_time_resolution",
         "close_flags"},
        "header");
    h.seq = static_cast<uint32_t>(
        integer_member(hdr, "ledger_index", kUint32Max, "header"));
    auto const total_coins =
        std::string(hdr.at("total_coins").as_string());
    if (total_coins.empty() ||
        !std::all_of(
            total_coins.begin(), total_coins.end(), [](unsigned char c) {
                return std::isdigit(c) != 0;
            }))
        throw std::runtime_error(
            "proof JSON: total_coins must be unsigned decimal digits");
    h.drops = std::stoull(total_coins);
    h.parent_hash =
        hash_from_hex(std::string(hdr.at("parent_hash").as_string()));
    h.tx_hash = hash_from_hex(std::string(hdr.at("transaction_hash").as_string()));
    h.account_hash =
        hash_from_hex(std::string(hdr.at("account_hash").as_string()));
    h.parent_close_time = static_cast<uint32_t>(
        integer_member(hdr, "parent_close_time", kUint32Max, "header"));
    h.close_time = static_cast<uint32_t>(
        integer_member(hdr, "close_time", kUint32Max, "header"));
    h.close_time_resolution = static_cast<uint8_t>(
        integer_member(hdr, "close_time_resolution", kUint8Max, "header"));
    h.close_flags = static_cast<uint8_t>(
        integer_member(hdr, "close_flags", kUint8Max, "header"));
    return h;
}

static TrieData
trie_from_json(boost::json::object const& obj)
{
    require_members(obj, {"type", "tree", "trie"}, "map_proof step");
    TrieData t;
    auto tree_str = std::string(obj.at("tree").as_string());
    if (tree_str == "state")
        t.tree = TrieData::TreeType::state;
    else if (tree_str == "tx")
        t.tree = TrieData::TreeType::tx;
    else
        throw std::runtime_error("proof JSON: unknown map tree type");
    catl::shamap::validate_json_trie(obj.at("trie"));
    t.trie_json = obj.at("trie");
    return t;
}

static ProofChain
parse_steps_array(boost::json::array const& arr)
{
    ProofChain chain;
    for (auto const& step_val : arr)
    {
        if (!step_val.is_object())
            throw std::runtime_error("proof JSON: step must be an object");
        auto const& obj = step_val.as_object();
        if (!obj.contains("type") || !obj.at("type").is_string())
            throw std::runtime_error("proof JSON: step has no string type");

        auto type = std::string(obj.at("type").as_string());
        if (type == "anchor")
        {
            chain.steps.push_back(anchor_from_json(obj));
        }
        else if (type == "ledger_header")
        {
            chain.steps.push_back(header_from_json(obj));
        }
        else if (type == "map_proof")
        {
            chain.steps.push_back(trie_from_json(obj));
        }
        else
        {
            throw std::runtime_error(
                "proof JSON: unknown step type " + type);
        }
    }
    return chain;
}

ProofChain
from_json(boost::json::value const& json)
{
    // {"format_version": N, "network_id": N, "steps": [...]} is the only
    // form: a bare steps array carries no version to check (§2).
    if (!json.is_object())
        throw std::runtime_error(
            "proof JSON: expected an object with format_version, network_id "
            "and steps");
    auto const& obj = json.as_object();
    require_members(obj, {"format_version", "network_id", "steps"}, "proof");

    auto const v = integer_member(obj, "format_version", kUint32Max, "proof");
    if (v != XPRV_FORMAT_VERSION)
        throw std::runtime_error(
            "proof JSON: format_version " + std::to_string(v) +
            " != expected " + std::to_string(XPRV_FORMAT_VERSION));

    auto chain = parse_steps_array(obj.at("steps").as_array());
    chain.network_id = static_cast<uint32_t>(
        integer_member(obj, "network_id", kUint32Max, "proof"));
    return chain;
}

ProofChain
from_json_text(std::string_view text)
{
    return from_json(detail::parse_unique_json(text));
}

}  // namespace xprv
