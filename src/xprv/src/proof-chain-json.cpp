#include "xprv/proof-chain-json.h"
#include "xprv/hex-utils.h"
#include "xprv/network-config.h"

#include <catl/shamap/binary-trie.h>
#include <catl/xdata/parse_leaf.h>
#include <catl/xdata/parse_transaction.h>
#include <catl/xdata/serialize.h>

#include <algorithm>
#include <cctype>
#include <cstring>
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
        boost::json::object unl;
        unl["public_key"] = a.publisher_key_hex;
        unl["manifest"] = bytes_hex(a.publisher_manifest);
        unl["blob"] = bytes_hex(a.blob);
        unl["signature"] = bytes_hex(a.blob_signature);
        obj["unl"] = unl;
    }

    boost::json::object validations;
    for (auto const& [key, val] : a.validations)
    {
        validations[key] = bytes_hex(val);
    }
    obj["validations"] = validations;

    // Summary for quick inspection
    boost::json::object summary;
    summary["total_validations"] = static_cast<int>(a.validations.size());
    obj["summary"] = summary;

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

static AnchorData
anchor_from_json(boost::json::object const& obj)
{
    AnchorData a;
    a.ledger_hash =
        hash_from_hex(std::string(obj.at("ledger_hash").as_string()));
    a.ledger_index = obj.at("ledger_index").to_number<uint32_t>();

    auto const& unl = obj.at("unl").as_object();
    a.publisher_key_hex = std::string(unl.at("public_key").as_string());
    if (from_hex(a.publisher_key_hex).size() != 33)
        throw std::runtime_error(
            "proof JSON: publisher key must be exactly 33 bytes");
    a.publisher_manifest =
        from_hex(std::string(unl.at("manifest").as_string()));
    a.blob = from_hex(std::string(unl.at("blob").as_string()));
    a.blob_signature =
        from_hex(std::string(unl.at("signature").as_string()));

    auto const& validations = obj.at("validations").as_object();
    for (auto const& kv : validations)
    {
        auto const key = std::string(kv.key());
        if (from_hex(key).size() != 33 || !kv.value().is_string())
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
    HeaderData h;
    auto const& hdr = obj.at("header").as_object();
    h.seq = hdr.at("ledger_index").to_number<uint32_t>();
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
    h.parent_close_time =
        hdr.at("parent_close_time").to_number<uint32_t>();
    h.close_time = hdr.at("close_time").to_number<uint32_t>();
    auto const resolution =
        hdr.at("close_time_resolution").to_number<uint32_t>();
    auto const flags = hdr.at("close_flags").to_number<uint32_t>();
    if (resolution > 255 || flags > 255)
        throw std::runtime_error(
            "proof JSON: uint8 ledger header field out of range");
    h.close_time_resolution = static_cast<uint8_t>(resolution);
    h.close_flags = static_cast<uint8_t>(flags);
    return h;
}

static TrieData
trie_from_json(boost::json::object const& obj)
{
    TrieData t;
    auto tree_str = std::string(obj.at("tree").as_string());
    if (tree_str == "state")
        t.tree = TrieData::TreeType::state;
    else if (tree_str == "tx")
        t.tree = TrieData::TreeType::tx;
    else
        throw std::runtime_error("proof JSON: unknown map tree type");
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
    // New format: {"format_version": N, "network_id": N, "steps": [...]}
    if (json.is_object())
    {
        auto const& obj = json.as_object();

        auto const v = obj.at("format_version").to_number<uint32_t>();
        if (v != XPRV_FORMAT_VERSION)
            throw std::runtime_error(
                "proof JSON: format_version " + std::to_string(v) +
                " != expected " + std::to_string(XPRV_FORMAT_VERSION));

        auto chain = parse_steps_array(obj.at("steps").as_array());
        chain.network_id = obj.at("network_id").to_number<uint32_t>();
        return chain;
    }

    // Legacy format: bare array [...]
    if (json.is_array())
    {
        return parse_steps_array(json.as_array());
    }

    throw std::runtime_error("proof JSON: expected object or array");
}

}  // namespace xprv
