#include "xprv/proof-chain-json.h"
#include "xprv/hex-utils.h"

#include <algorithm>
#include <cctype>

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

static boost::json::object
trie_to_json(TrieData const& t)
{
    if (!t.json_unavailable.empty())
        throw JsonFormUnavailable(t.json_unavailable);
    boost::json::object obj;
    obj["type"] = "map_proof";
    obj["tree"] = (t.tree == TrieData::TreeType::state) ? "state" : "tx";
    obj["trie"] = t.trie_json;
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
                    arr.push_back(trie_to_json(data));
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
                return trie_to_json(data);
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
