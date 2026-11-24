#include "xprv/proof-chain-json.h"
#include "json-proof-loader.h"
#include "xprv/anchor-verifier.h"
#include "xprv/hex-utils.h"
#include "xprv/network-config.h"

#include <catl/shamap/binary-trie.h>
#include <catl/shamap/json-trie.h>
#include <catl/xdata/json-visitor.h>
#include <catl/xdata/parse_leaf.h>
#include <catl/xdata/parse_transaction.h>
#include <catl/xdata/parser-context.h>
#include <catl/xdata/parser.h>
#include <catl/xdata/protocol.h>
#include <catl/xdata/serialize.h>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstring>
#include <initializer_list>
#include <optional>
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
        decoded = is_tx
            ? catl::xdata::parse_transaction(
                  full,
                  protocol,
                  {.includes_prefix = false, .json_opts = json_opts})
            : catl::xdata::parse_leaf(
                  full,
                  protocol,
                  {.includes_prefix = false, .json_opts = json_opts});
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

[[noreturn]] static void
not_a_value(std::string const& at, std::string_view type)
{
    throw std::runtime_error(
        "map proof: leaf member " + at + " is not a serialized " +
        std::string(type) + " value");
}

// The native asset's code is the network's own: XRP on XRPL, XAH on Xahau.
// The other network's symbol is an ordinary issued currency there (§2.3,
// following rippled's to_string(Currency)).
static bool
is_native_code(boost::json::value const& v, std::string const& native)
{
    return v.is_string() && v.as_string() == native;
}

static bool
has_exactly(
    boost::json::object const& obj,
    std::initializer_list<std::string_view> names)
{
    return obj.size() == names.size() &&
        std::all_of(names.begin(), names.end(), [&](auto name) {
               return obj.contains(name);
           });
}

static void
require_issue(
    boost::json::value const& v,
    std::string const& at,
    std::string const& native)
{
    // An issue is an object, as rippled writes it: the native asset is
    // {"currency": <native code>} and nothing else. A bare string is not one.
    if (!v.is_object())
        not_a_value(at, "Issue");
    auto const& o = v.as_object();
    if (has_exactly(o, {"mpt_issuance_id"}) ||
        (has_exactly(o, {"currency"}) &&
         is_native_code(o.at("currency"), native)) ||
        (has_exactly(o, {"currency", "issuer"}) &&
         !is_native_code(o.at("currency"), native)))
        return;
    not_a_value(at, "Issue");
}

// Values whose JSON is an object or array have a grammar of their own. Their
// codecs read the members they need and skip the rest, so each variant must
// carry exactly its members (§2.3).
static void
require_value_grammar(
    catl::xdata::FieldType const& type,
    boost::json::value const& v,
    std::string const& at,
    std::string const& native)
{
    namespace types = catl::xdata::FieldTypes;
    if (type == types::Amount)
    {
        if (v.is_string())
            return;
        if (v.is_object())
        {
            auto const& o = v.as_object();
            if (has_exactly(o, {"value", "mpt_issuance_id"}) ||
                (has_exactly(o, {"value", "currency", "issuer"}) &&
                 !is_native_code(o.at("currency"), native)))
                return;
        }
        not_a_value(at, "Amount");
    }
    if (type == types::Issue)
        return require_issue(v, at, native);
    if (type == types::XChainBridge)
    {
        if (!v.is_object() ||
            !has_exactly(
                v.as_object(),
                {"LockingChainDoor",
                 "LockingChainIssue",
                 "IssuingChainDoor",
                 "IssuingChainIssue"}) ||
            !v.at("LockingChainDoor").is_string() ||
            !v.at("IssuingChainDoor").is_string())
            not_a_value(at, "XChainBridge");
        require_issue(
            v.at("LockingChainIssue"), at + ".LockingChainIssue", native);
        require_issue(
            v.at("IssuingChainIssue"), at + ".IssuingChainIssue", native);
        return;
    }
    if (type == types::PathSet)
    {
        if (!v.is_array())
            not_a_value(at, "PathSet");
        for (auto const& path : v.as_array())
        {
            if (!path.is_array())
                not_a_value(at, "PathSet");
            for (auto const& hop : path.as_array())
            {
                if (!hop.is_object() || hop.as_object().empty())
                    not_a_value(at, "PathSet");
                for (auto const& kv : hop.as_object())
                    if ((kv.key() != "account" && kv.key() != "currency" &&
                         kv.key() != "issuer") ||
                        !kv.value().is_string())
                        not_a_value(at, "PathSet");
            }
        }
        return;
    }
    if (type == types::Vector256)
    {
        if (!v.is_array() ||
            !std::all_of(
                v.as_array().begin(), v.as_array().end(), [](auto const& e) {
                    return e.is_string();
                }))
            not_a_value(at, "Vector256");
        return;
    }
    if (v.is_object() || v.is_array())
        not_a_value(at, type.name);
    // A plain field's value is a string or an integer number. A codec can
    // drop a null member or read true as 1, and the reader would then see
    // something nothing hashed.
    if (!v.is_string() && !v.is_int64() && !v.is_uint64())
        throw std::runtime_error(
            "map proof: leaf member " + at + " must be a string or a number");
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
                auto const element_at =
                    at + "[" + std::to_string(index++) + "]";
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
        else
            require_value_grammar(
                field->meta.type, member, at, protocol.native_currency());
    }
}

namespace {

// Decimal text as an exact value: its sign, its significant digits and the
// power of ten of the last digit. No floating point, so two notations of one
// number compare equal and nothing else does.
struct DecimalText
{
    bool negative = false;
    std::string digits;
    long long exponent = 0;
    bool
    operator==(DecimalText const&) const = default;
};

std::optional<DecimalText>
parse_decimal_text(std::string_view s)
{
    auto const digit = [&](size_t i) {
        return i < s.size() && s[i] >= '0' && s[i] <= '9';
    };
    DecimalText d;
    size_t i = 0;
    if (i < s.size() && s[i] == '-')
    {
        d.negative = true;
        ++i;
    }
    if (!digit(i))
        return std::nullopt;
    auto const integer_at = i;
    while (digit(i))
        d.digits += s[i++];
    // One spelling of the integer part: no leading zero (§2.3).
    if (i - integer_at > 1 && s[integer_at] == '0')
        return std::nullopt;
    if (i < s.size() && s[i] == '.')
    {
        ++i;
        if (!digit(i))
            return std::nullopt;
        while (digit(i))
        {
            d.digits += s[i++];
            --d.exponent;
        }
    }
    if (i < s.size() && (s[i] == 'e' || s[i] == 'E'))
    {
        ++i;
        bool negative = false;
        if (i < s.size() && (s[i] == '+' || s[i] == '-'))
            negative = s[i++] == '-';
        if (!digit(i))
            return std::nullopt;
        long long e = 0;
        size_t count = 0;
        while (digit(i))
        {
            if (++count > 9)
                return std::nullopt;
            e = e * 10 + (s[i++] - '0');
        }
        d.exponent += negative ? -e : e;
    }
    if (i != s.size())
        return std::nullopt;
    auto const first = d.digits.find_first_not_of('0');
    if (first == std::string::npos)
    {
        // Zero has no sign: "-0" is not a second spelling of it.
        if (d.negative)
            return std::nullopt;
        return DecimalText{false, "0", 0};
    }
    d.digits.erase(0, first);
    while (d.digits.size() > 1 && d.digits.back() == '0')
    {
        d.digits.pop_back();
        ++d.exponent;
    }
    return d;
}

// What kind of text a string is, for the one place two spellings are equal.
enum class Text { exact, amount, decimal };

// Throw unless `given` is the JSON `decoded`: the same members, the same
// array order, the same JSON types, equal numbers and equal strings. A decimal
// string (an issued amount's value, a Number) may use another notation for
// exactly the same number (§2.3).
void
require_same_json(
    boost::json::value const& given,
    boost::json::value const& decoded,
    std::string const& at,
    catl::xdata::Protocol const& protocol,
    Text text)
{
    namespace types = catl::xdata::FieldTypes;
    auto const differs = [&]() {
        throw std::runtime_error(
            "map proof: leaf is not the JSON its bytes decode to: " +
            (at.empty() ? std::string("it") : "member " + at) + " differs");
    };
    if (given.is_object())
    {
        if (!decoded.is_object())
            differs();
        auto const& g = given.as_object();
        auto const& d = decoded.as_object();
        for (auto const& [key, value] : g)
        {
            auto const name = std::string(key);
            auto const child = at.empty() ? name : at + "." + name;
            auto const* other = d.if_contains(key);
            if (!other)
                throw std::runtime_error(
                    "map proof: leaf is not the JSON its bytes decode to: "
                    "member " +
                    child + " is not in it");
            auto child_text = Text::exact;
            // Only an issued amount's value is a decimal. A token (MPT)
            // amount's value is an integer with one spelling.
            if (text == Text::amount)
                child_text = name == "value" && g.contains("currency")
                    ? Text::decimal
                    : Text::exact;
            else if (auto const field = protocol.find_field(name))
            {
                if (field->meta.type == types::Amount)
                    child_text = Text::amount;
                else if (field->meta.type == types::Number)
                    child_text = Text::decimal;
            }
            require_same_json(value, *other, child, protocol, child_text);
        }
        for (auto const& [key, value] : d)
            if (!g.contains(key))
                throw std::runtime_error(
                    "map proof: leaf is not the JSON its bytes decode to: "
                    "member " +
                    (at.empty() ? std::string(key) : at + "." + std::string(key)) +
                    " is missing");
        return;
    }
    if (given.is_array())
    {
        if (!decoded.is_array() ||
            decoded.as_array().size() != given.as_array().size())
            differs();
        for (size_t i = 0; i < given.as_array().size(); ++i)
            require_same_json(
                given.as_array()[i],
                decoded.as_array()[i],
                at + "[" + std::to_string(i) + "]",
                protocol,
                Text::exact);
        return;
    }
    if (given.is_string())
    {
        if (!decoded.is_string())
            differs();
        if (given.as_string() == decoded.as_string())
            return;
        // A drops string is a decimal too, but it has one spelling; only an
        // issued value and a Number are written two ways by real codecs.
        if (text == Text::decimal)
        {
            auto const a = parse_decimal_text(given.as_string());
            auto const b = parse_decimal_text(decoded.as_string());
            if (a && b && *a == *b)
                return;
        }
        differs();
    }
    // An integer token. A float, a boolean and null equal nothing a codec
    // decodes to.
    auto const integer = [](boost::json::value const& v)
        -> std::optional<std::pair<bool, uint64_t>> {
        if (v.is_uint64())
            return std::pair{false, v.get_uint64()};
        if (!v.is_int64())
            return std::nullopt;
        auto const i = v.get_int64();
        return i < 0 ? std::pair{true, static_cast<uint64_t>(-(i + 1))}
                     : std::pair{false, static_cast<uint64_t>(i)};
    };
    auto const a = integer(given);
    auto const b = integer(decoded);
    if (!a || !b || *a != *b)
        differs();
}

// The leaf's bytes decoded by this network's codec. The parsers want the
// key after the item; it plays no part in the decoded content.
boost::json::value
decode_leaf_bytes(
    std::vector<uint8_t> bytes,
    bool is_tx,
    catl::xdata::Protocol const& protocol)
{
    bytes.insert(bytes.end(), 32, 0);
    Slice full(bytes.data(), bytes.size());
    catl::xdata::JsonVisitor::Options json_opts;
    json_opts.ascii_hints = false;
    auto decoded = is_tx
        ? catl::xdata::parse_transaction(
              full, protocol, {.includes_prefix = false, .json_opts = json_opts})
        : catl::xdata::parse_leaf(
              full, protocol, {.includes_prefix = false, .json_opts = json_opts});
    if (!decoded.is_object())
        throw std::runtime_error("map proof: leaf did not decode to an object");
    auto& obj = decoded.as_object();
    obj.erase("index");
    obj.erase("hash");
    obj.erase("blob");
    return decoded;
}

}  // namespace

void
require_codec_members(
    boost::json::value const& leaf,
    bool is_tx,
    catl::xdata::Protocol const& protocol)
try
{
    if (!is_tx)
        require_stobject_members(leaf, protocol, "");
    else
    {
        if (!leaf.is_object() || leaf.as_object().size() != 2 ||
            !leaf.as_object().contains("tx") ||
            !leaf.as_object().contains("meta"))
            throw std::runtime_error(
                "map proof: a tx leaf must be exactly {tx, meta}");
        require_stobject_members(leaf.at("tx"), protocol, "tx");
        require_stobject_members(leaf.at("meta"), protocol, "meta");
    }

    // The rule that makes the rest hold (§2.3): a JSON leaf is valid only
    // if it is the JSON its own bytes decode to. A member the codec drops, a
    // number where a name belongs, an address or an amount in a spelling the
    // codec would not write: each serializes to bytes whose decoding is some
    // other JSON, and is refused here.
    auto const& object = leaf.as_object();
    boost::json::value decoded;
    try
    {
        decoded = decode_leaf_bytes(
            is_tx ? catl::xdata::serialize_transaction(object, protocol)
                  : catl::xdata::serialize_leaf(object, protocol),
            is_tx,
            protocol);
    }
    catch (std::exception const& e)
    {
        // A leaf the codec will not serialize has no bytes to decode.
        throw std::runtime_error(
            "map proof: leaf is not the JSON its bytes decode to: it does "
            "not serialize (" +
            std::string(e.what()) + ")");
    }
    require_same_json(leaf, decoded, "", protocol, Text::exact);
}
catch (LeafFormError const&)
{
    throw;
}
catch (std::exception const& e)
{
    // Whatever a check above says, it says about the leaf's form.
    throw LeafFormError(e.what());
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
static bool
hex_equal_ci(std::string_view a, std::string_view b)
{
    if (a.size() != b.size())
        return false;
    for (size_t i = 0; i < a.size(); ++i)
    {
        if (std::tolower(static_cast<unsigned char>(a[i])) !=
            std::tolower(static_cast<unsigned char>(b[i])))
            return false;
    }
    return true;
}

static std::string
embedded_signing_pub_key(std::vector<uint8_t> const& raw)
{
    static auto const protocol =
        catl::xdata::Protocol::load_embedded_xrpl_protocol();
    Slice slice(raw.data(), raw.size());
    catl::xdata::JsonVisitor::Options options;
    options.ascii_hints = false;
    catl::xdata::JsonVisitor visitor(protocol, options);
    catl::xdata::ParserContext ctx(slice);
    catl::xdata::parse_with_visitor(ctx, protocol, visitor);
    auto parsed = visitor.get_result();
    if (!ctx.cursor.empty() || !parsed.is_object())
        return {};
    auto const* key = parsed.as_object().if_contains("SigningPubKey");
    if (!key || !key->is_string())
        return {};
    return std::string(key->as_string());
}

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
                "proof JSON: unknown member " + std::string(kv.key()) + " in " +
                std::string(what));
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

// Hex in the JSON form has one spelling: uppercase, an even number of
// digits (§2). `size` is the exact byte length, or 0 for any length.
static std::vector<uint8_t>
proof_hex(
    boost::json::value const& value,
    std::string_view what,
    std::size_t size = 0)
{
    if (!value.is_string())
        throw std::runtime_error(
            "proof JSON: " + std::string(what) + " must be a hex string");
    auto const text = std::string_view(value.as_string());
    if (text.size() % 2 != 0 ||
        !std::all_of(text.begin(), text.end(), [](char c) {
            return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F');
        }))
        throw std::runtime_error(
            "proof JSON: " + std::string(what) + " must be uppercase hex");
    if (size != 0 && text.size() != size * 2)
        throw std::runtime_error(
            "proof JSON: " + std::string(what) + " must be exactly " +
            std::to_string(size) + " bytes");
    return from_hex(text);
}

static Hash256
proof_hash(boost::json::value const& value, std::string_view what)
{
    auto const bytes = proof_hex(value, what, 32);
    return Hash256(bytes.data());
}

static AnchorData
anchor_from_json(boost::json::object const& obj)
{
    require_members(
        obj,
        {"type", "ledger_hash", "unl", "validations"},
        "anchor");
    AnchorData a;
    a.ledger_hash = proof_hash(obj.at("ledger_hash"), "anchor ledger_hash");

    auto const& unl = obj.at("unl").as_object();
    require_public_key(
        proof_hex(unl.at("public_key"), "publisher key", 33), "publisher key");
    a.publisher_key_hex = std::string(unl.at("public_key").as_string());
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
        a.publisher_manifest = proof_hex(unl.at("manifest"), "unl manifest");
        a.blob = proof_hex(unl.at("blob"), "unl blob");
        a.blob_signature = proof_hex(unl.at("signature"), "unl signature");
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
        auto const bytes = proof_hex(kv.value(), "validation value");
        if (!AnchorVerifier::is_canonical_stvalidation(bytes))
            throw std::runtime_error(
                "proof JSON: validation is not a canonical STValidation");
        if (!hex_equal_ci(embedded_signing_pub_key(bytes), key))
            throw std::runtime_error(
                "proof JSON: validation key does not match SigningPubKey");
        a.validations[key] = bytes;
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
    // The decimal text of a uint64 as rippled writes it: digits, no sign,
    // no leading zero. One spelling per value.
    auto const& coins = hdr.at("total_coins");
    auto const total_coins =
        coins.is_string() ? std::string_view(coins.as_string()) : "";
    uint64_t drops = 0;
    auto const parsed = std::from_chars(
        total_coins.data(), total_coins.data() + total_coins.size(), drops);
    if (total_coins.empty() ||
        (total_coins.size() > 1 && total_coins.front() == '0') ||
        total_coins.front() < '0' || total_coins.front() > '9' ||
        parsed.ec != std::errc{} ||
        parsed.ptr != total_coins.data() + total_coins.size())
        throw std::runtime_error(
            "proof JSON: header total_coins must be the decimal text of a "
            "uint64 with no leading zeros");
    h.drops = drops;
    h.parent_hash = proof_hash(hdr.at("parent_hash"), "header parent_hash");
    h.tx_hash =
        proof_hash(hdr.at("transaction_hash"), "header transaction_hash");
    h.account_hash =
        proof_hash(hdr.at("account_hash"), "header account_hash");
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
            throw std::runtime_error("proof JSON: unknown step type " + type);
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
    // §8: a configured maximum before either parse pass.
    if (text.size() > XPRV_MAX_JSON_SIZE)
        throw std::runtime_error(
            "proof JSON: text exceeds " + std::to_string(XPRV_MAX_JSON_SIZE) +
            " bytes");
    return from_json(detail::parse_unique_json(text));
}

}  // namespace xprv
