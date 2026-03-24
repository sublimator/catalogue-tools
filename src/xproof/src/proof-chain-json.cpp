#include "xproof/proof-chain-json.h"
#include "xproof/hex-utils.h"

namespace xproof {

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
    hdr["seq"] = h.seq;
    hdr["drops"] = std::to_string(h.drops);
    hdr["parent_hash"] = upper_hex(h.parent_hash);
    hdr["tx_hash"] = upper_hex(h.tx_hash);
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
to_json(ProofChain const& chain)
{
    boost::json::object obj;
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
    if (obj.contains("ledger_index"))
    {
        a.ledger_index = obj.at("ledger_index").to_number<uint32_t>();
    }

    if (obj.contains("unl"))
    {
        auto const& unl = obj.at("unl").as_object();
        if (unl.contains("public_key"))
        {
            a.publisher_key_hex =
                std::string(unl.at("public_key").as_string());
        }
        if (unl.contains("manifest"))
        {
            a.publisher_manifest =
                from_hex(std::string(unl.at("manifest").as_string()));
        }
        if (unl.contains("blob"))
        {
            a.blob = from_hex(std::string(unl.at("blob").as_string()));
        }
        if (unl.contains("signature"))
        {
            a.blob_signature =
                from_hex(std::string(unl.at("signature").as_string()));
        }
    }

    if (obj.contains("validations") && obj.at("validations").is_object())
    {
        for (auto const& kv : obj.at("validations").as_object())
        {
            if (kv.value().is_string())
            {
                a.validations[std::string(kv.key())] =
                    from_hex(std::string(kv.value().as_string()));
            }
        }
    }

    return a;
}

static HeaderData
header_from_json(boost::json::object const& obj)
{
    HeaderData h;
    auto const& hdr = obj.at("header").as_object();
    h.seq = hdr.at("seq").to_number<uint32_t>();
    h.drops = std::stoull(std::string(hdr.at("drops").as_string()));
    h.parent_hash =
        hash_from_hex(std::string(hdr.at("parent_hash").as_string()));
    h.tx_hash = hash_from_hex(std::string(hdr.at("tx_hash").as_string()));
    h.account_hash =
        hash_from_hex(std::string(hdr.at("account_hash").as_string()));
    h.parent_close_time =
        hdr.at("parent_close_time").to_number<uint32_t>();
    h.close_time = hdr.at("close_time").to_number<uint32_t>();
    h.close_time_resolution =
        static_cast<uint8_t>(
            hdr.at("close_time_resolution").to_number<int>());
    h.close_flags =
        static_cast<uint8_t>(hdr.at("close_flags").to_number<int>());
    return h;
}

static TrieData
trie_from_json(boost::json::object const& obj)
{
    TrieData t;
    auto tree_str = std::string(obj.at("tree").as_string());
    t.tree = (tree_str == "state") ? TrieData::TreeType::state
                                   : TrieData::TreeType::tx;
    if (obj.contains("trie"))
    {
        t.trie_json = obj.at("trie");
    }
    return t;
}

static ProofChain
parse_steps_array(boost::json::array const& arr)
{
    ProofChain chain;
    for (auto const& step_val : arr)
    {
        if (!step_val.is_object())
            continue;
        auto const& obj = step_val.as_object();
        if (!obj.contains("type"))
            continue;

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
    }
    return chain;
}

ProofChain
from_json(boost::json::value const& json)
{
    // New format: {"network_id": N, "steps": [...]}
    if (json.is_object())
    {
        auto const& obj = json.as_object();
        auto chain = parse_steps_array(obj.at("steps").as_array());
        if (obj.contains("network_id"))
        {
            chain.network_id = obj.at("network_id").to_number<uint32_t>();
        }
        return chain;
    }

    // Legacy format: bare array [...]
    if (json.is_array())
    {
        return parse_steps_array(json.as_array());
    }

    throw std::runtime_error("proof JSON: expected object or array");
}

}  // namespace xproof
