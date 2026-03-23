#include "xproof/proof-chain-binary.h"
#include "xproof/hex-utils.h"

#include <catl/shamap/binary-trie.h>

#include <catl/core/base64.h>
#include <catl/xdata/json-visitor.h>
#include <catl/xdata/parser-context.h>
#include <catl/xdata/parser.h>
#include <catl/xdata/protocol.h>
#include <catl/xdata/serialize.h>

#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/filter/zlib.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/json.hpp>

#include <cstring>
#include <sstream>
#include <stdexcept>

using catl::shamap::leb128_decode;
using catl::shamap::leb128_encode;

namespace xproof {

// File header
static constexpr uint8_t MAGIC[4] = {'X', 'P', 'R', 'F'};
static constexpr uint8_t VERSION = 0x01;
static constexpr uint8_t FLAG_ZLIB = 0x01;

// TLV type bytes
static constexpr uint8_t TLV_ANCHOR = 0x01;
static constexpr uint8_t TLV_LEDGER_HEADER = 0x02;
static constexpr uint8_t TLV_MAP_PROOF_TX = 0x03;
static constexpr uint8_t TLV_MAP_PROOF_STATE = 0x04;

//------------------------------------------------------------------------------
// Helpers
//------------------------------------------------------------------------------

static void
write_bytes(std::vector<uint8_t>& out, const uint8_t* data, size_t len)
{
    out.insert(out.end(), data, data + len);
}

static void
write_vl(std::vector<uint8_t>& out, std::vector<uint8_t> const& data)
{
    leb128_encode(out, data.size());
    write_bytes(out, data.data(), data.size());
}

static void
write_u32_be(std::vector<uint8_t>& out, uint32_t v)
{
    out.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
    out.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    out.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    out.push_back(static_cast<uint8_t>(v & 0xFF));
}

static void
write_u64_be(std::vector<uint8_t>& out, uint64_t v)
{
    for (int i = 7; i >= 0; --i)
        out.push_back(static_cast<uint8_t>((v >> (i * 8)) & 0xFF));
}

static uint32_t
read_u32_be(std::span<const uint8_t> data, size_t& pos)
{
    if (pos + 4 > data.size())
        throw std::runtime_error("binary proof: unexpected end reading u32");
    uint32_t v = (static_cast<uint32_t>(data[pos]) << 24) |
        (static_cast<uint32_t>(data[pos + 1]) << 16) |
        (static_cast<uint32_t>(data[pos + 2]) << 8) |
        static_cast<uint32_t>(data[pos + 3]);
    pos += 4;
    return v;
}

static uint64_t
read_u64_be(std::span<const uint8_t> data, size_t& pos)
{
    if (pos + 8 > data.size())
        throw std::runtime_error("binary proof: unexpected end reading u64");
    uint64_t v = 0;
    for (int i = 0; i < 8; i++)
        v = (v << 8) | data[pos++];
    return v;
}

static std::vector<uint8_t>
read_vl(std::span<const uint8_t> data, size_t& pos)
{
    size_t len = leb128_decode(data, pos);
    if (pos + len > data.size())
        throw std::runtime_error(
            "binary proof: unexpected end reading VL data");
    std::vector<uint8_t> result(data.data() + pos, data.data() + pos + len);
    pos += len;
    return result;
}

static void
read_bytes(std::span<const uint8_t> data, size_t& pos, uint8_t* out, size_t len)
{
    if (pos + len > data.size())
        throw std::runtime_error("binary proof: unexpected end reading bytes");
    std::memcpy(out, data.data() + pos, len);
    pos += len;
}

static std::vector<uint8_t>
hex_decode(std::string_view hex)
{
    std::vector<uint8_t> result;
    result.reserve(hex.size() / 2);
    for (size_t i = 0; i + 1 < hex.size(); i += 2)
    {
        unsigned int byte;
        std::sscanf(hex.data() + i, "%2x", &byte);
        result.push_back(static_cast<uint8_t>(byte));
    }
    return result;
}

static std::string
hex_encode(const uint8_t* data, size_t len)
{
    std::string result;
    result.reserve(len * 2);
    for (size_t i = 0; i < len; i++)
    {
        char buf[3];
        std::snprintf(buf, sizeof(buf), "%02X", data[i]);
        result += buf;
    }
    return result;
}

//------------------------------------------------------------------------------
// Zlib compress / decompress
//------------------------------------------------------------------------------

static std::vector<uint8_t>
zlib_compress(std::vector<uint8_t> const& input, int level = 9)
{
    namespace io = boost::iostreams;
    std::vector<char> output;
    io::zlib_params params;
    params.level = level;
    io::filtering_ostream out;
    out.push(io::zlib_compressor(params));
    out.push(io::back_inserter(output));
    io::write(
        out,
        reinterpret_cast<const char*>(input.data()),
        static_cast<std::streamsize>(input.size()));
    out.reset();
    return {output.begin(), output.end()};
}

static std::vector<uint8_t>
zlib_decompress(std::span<const uint8_t> input)
{
    namespace io = boost::iostreams;
    std::vector<char> output;
    io::filtering_ostream out;
    out.push(io::zlib_decompressor());
    out.push(io::back_inserter(output));
    io::write(
        out,
        reinterpret_cast<const char*>(input.data()),
        static_cast<std::streamsize>(input.size()));
    out.reset();
    return {output.begin(), output.end()};
}

//------------------------------------------------------------------------------
// Blob JSON key ordering
//------------------------------------------------------------------------------
// The blob must reconstitute to the exact same bytes for signature
// verification. JSON key ordering varies by publisher implementation.
// We encode the permutation as a single byte:
//   bits 0-2: top-level key order (3! = 6 permutations of
//             {sequence, expiration, validators})
//   bit 3:    validator key order (2! = 2 permutations of
//             {validation_public_key, manifest})
// Total: 12 combinations in 4 bits.

static const std::string BLOB_KEYS[] = {"sequence", "expiration", "validators"};
static const std::string VAL_KEYS[] = {"validation_public_key", "manifest"};

/// Detect the permutation index for an ordered set of keys.
/// Returns the Lehmer code index (0..n!-1).
static uint8_t
detect_key_order(
    boost::json::object const& obj,
    std::string const* expected,
    int n)
{
    // Collect the actual key order
    std::vector<int> order;
    for (auto const& kv : obj)
    {
        for (int i = 0; i < n; i++)
        {
            if (kv.key() == expected[i])
            {
                order.push_back(i);
                break;
            }
        }
    }

    // Convert permutation to index (factorial number system)
    if (n == 2)
    {
        return (order.size() >= 2 && order[0] == 1) ? 1 : 0;
    }
    if (n == 3 && order.size() >= 3)
    {
        // 6 permutations of {0,1,2}
        if (order[0] == 0 && order[1] == 1)
            return 0;  // 0,1,2
        if (order[0] == 0 && order[1] == 2)
            return 1;  // 0,2,1
        if (order[0] == 1 && order[1] == 0)
            return 2;  // 1,0,2
        if (order[0] == 1 && order[1] == 2)
            return 3;  // 1,2,0
        if (order[0] == 2 && order[1] == 0)
            return 4;  // 2,0,1
        if (order[0] == 2 && order[1] == 1)
            return 5;  // 2,1,0
    }
    return 0;
}

/// Get the key permutation for a given index.
static std::vector<int>
get_key_permutation(int n, uint8_t index)
{
    if (n == 2)
    {
        return index ? std::vector<int>{1, 0} : std::vector<int>{0, 1};
    }
    // n == 3
    static const int perms[6][3] = {
        {0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}};
    if (index >= 6)
        index = 0;
    return {perms[index][0], perms[index][1], perms[index][2]};
}

//------------------------------------------------------------------------------
// TLV encode
//------------------------------------------------------------------------------

static std::vector<uint8_t>
encode_anchor(AnchorData const& a)
{
    std::vector<uint8_t> payload;

    // Fixed fields
    write_bytes(payload, a.ledger_hash.data(), 32);

    auto pubkey = hex_decode(a.publisher_key_hex);
    if (pubkey.size() < 33)
        pubkey.resize(33, 0);
    write_bytes(payload, pubkey.data(), 33);

    // Publisher manifest (raw binary STObject)
    write_vl(payload, a.publisher_manifest);

    // Blob signature
    write_vl(payload, a.blob_signature);

    // Decompose the blob JSON into raw binary parts.
    // Store a shape byte encoding the JSON key ordering permutation
    // so we can reconstitute the exact original bytes.
    {
        std::string blob_str(
            reinterpret_cast<char const*>(a.blob.data()), a.blob.size());
        auto blob_json = boost::json::parse(blob_str);
        auto const& obj = blob_json.as_object();

        // Detect key ordering
        uint8_t top_perm = detect_key_order(obj, BLOB_KEYS, 3);
        uint8_t val_perm = 0;
        auto const& validators = obj.at("validators").as_array();
        if (!validators.empty())
        {
            val_perm = detect_key_order(validators[0].as_object(), VAL_KEYS, 2);
        }
        uint8_t shape = (top_perm & 0x07) | ((val_perm & 0x01) << 3);
        payload.push_back(shape);

        write_u32_be(
            payload,
            static_cast<uint32_t>(obj.at("sequence").to_number<int64_t>()));
        write_u32_be(
            payload,
            static_cast<uint32_t>(obj.at("expiration").to_number<int64_t>()));

        leb128_encode(payload, validators.size());

        for (auto const& v : validators)
        {
            auto const& vobj = v.as_object();
            auto vpk = hex_decode(
                std::string_view(vobj.at("validation_public_key").as_string()));
            if (vpk.size() < 33)
                vpk.resize(33, 0);
            write_bytes(payload, vpk.data(), 33);

            auto m_b64 = std::string(vobj.at("manifest").as_string());
            auto m_bytes = catl::base64_decode(m_b64);
            write_vl(payload, m_bytes);
        }
    }

    // Validations: decomposed into common fields (once) + unique fields (per
    // validator). Parse each STValidation, find fields that are identical
    // across all, store those once. Per validator store only what differs.
    {
        leb128_encode(payload, a.validations.size());
        if (a.validations.empty())
        {
            return payload;
        }

        // Parse all STValidations to JSON
        auto protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();

        std::vector<std::pair<std::string, boost::json::object>> parsed;
        for (auto const& [key_hex, val_bytes] : a.validations)
        {
            Slice slice(val_bytes.data(), val_bytes.size());
            catl::xdata::JsonVisitor visitor(protocol);
            catl::xdata::ParserContext ctx(slice);
            catl::xdata::parse_with_visitor(ctx, protocol, visitor);
            auto result = visitor.get_result();
            parsed.emplace_back(key_hex, result.as_object());
        }

        // Determine common fields (identical value across all validations)
        auto const& first = parsed[0].second;
        boost::json::object common;
        boost::json::object first_unique;

        for (auto const& [key, val] : first)
        {
            // Skip hint fields (lowercase with _)
            if (!key.empty() && std::islower(key[0]))
                continue;

            bool is_common = true;
            for (size_t i = 1; i < parsed.size(); i++)
            {
                auto it = parsed[i].second.find(key);
                if (it == parsed[i].second.end() ||
                    boost::json::serialize(it->value()) !=
                        boost::json::serialize(val))
                {
                    is_common = false;
                    break;
                }
            }
            if (is_common)
            {
                common[key] = val;
            }
            else
            {
                first_unique[key] = val;
            }
        }

        // Serialize common fields once as canonical binary
        auto common_bytes =
            catl::xdata::codecs::serialize_object(common, protocol);
        write_vl(payload, common_bytes);

        // Store number of unique field names so decoder knows what to expect
        std::vector<std::string> unique_keys;
        for (auto const& [key, val] : first_unique)
        {
            unique_keys.push_back(std::string(key));
        }
        leb128_encode(payload, unique_keys.size());
        for (auto const& uk : unique_keys)
        {
            leb128_encode(payload, uk.size());
            write_bytes(
                payload,
                reinterpret_cast<uint8_t const*>(uk.data()),
                uk.size());
        }

        // Per validator: signing key + serialized unique fields
        for (auto const& [key_hex, fields] : parsed)
        {
            auto key_bytes = hex_decode(key_hex);
            if (key_bytes.size() < 33)
                key_bytes.resize(33, 0);
            write_bytes(payload, key_bytes.data(), 33);

            boost::json::object unique;
            for (auto const& uk : unique_keys)
            {
                auto it = fields.find(uk);
                if (it != fields.end())
                {
                    unique[uk] = it->value();
                }
            }
            auto unique_bytes =
                catl::xdata::codecs::serialize_object(unique, protocol);
            write_vl(payload, unique_bytes);
        }
    }

    return payload;
}

static std::vector<uint8_t>
encode_header(HeaderData const& h)
{
    std::vector<uint8_t> payload;
    payload.reserve(118);

    write_u32_be(payload, h.seq);
    write_u64_be(payload, h.drops);
    write_bytes(payload, h.parent_hash.data(), 32);
    write_bytes(payload, h.tx_hash.data(), 32);
    write_bytes(payload, h.account_hash.data(), 32);
    write_u32_be(payload, h.parent_close_time);
    write_u32_be(payload, h.close_time);
    payload.push_back(h.close_time_resolution);
    payload.push_back(h.close_flags);

    return payload;
}

static void
write_tlv(
    std::vector<uint8_t>& out,
    uint8_t type,
    std::vector<uint8_t> const& payload)
{
    out.push_back(type);
    leb128_encode(out, payload.size());
    write_bytes(out, payload.data(), payload.size());
}

/// Encode the TLV body (no file header).
static std::vector<uint8_t>
encode_tlv_body(ProofChain const& chain)
{
    std::vector<uint8_t> out;

    for (auto const& step : chain.steps)
    {
        std::visit(
            [&](auto const& data) {
                using T = std::decay_t<decltype(data)>;
                if constexpr (std::is_same_v<T, AnchorData>)
                {
                    write_tlv(out, TLV_ANCHOR, encode_anchor(data));
                }
                else if constexpr (std::is_same_v<T, HeaderData>)
                {
                    write_tlv(out, TLV_LEDGER_HEADER, encode_header(data));
                }
                else if constexpr (std::is_same_v<T, TrieData>)
                {
                    uint8_t type_byte = (data.tree == TrieData::TreeType::state)
                        ? TLV_MAP_PROOF_STATE
                        : TLV_MAP_PROOF_TX;

                    if (!data.trie_binary.empty())
                    {
                        write_tlv(out, type_byte, data.trie_binary);
                    }
                    else
                    {
                        throw std::runtime_error(
                            "to_binary: TrieData has no trie_binary — "
                            "build proof with binary trie enabled");
                    }
                }
            },
            step);
    }

    return out;
}

//------------------------------------------------------------------------------
// Public encode
//------------------------------------------------------------------------------

std::vector<uint8_t>
to_binary(ProofChain const& chain, BinaryOptions const& opts)
{
    auto body = encode_tlv_body(chain);

    std::vector<uint8_t> out;
    out.reserve(6 + body.size());

    // File header
    write_bytes(out, MAGIC, 4);
    out.push_back(VERSION);

    uint8_t flags = 0;
    if (opts.compress)
        flags |= FLAG_ZLIB;
    out.push_back(flags);

    // Body
    if (opts.compress)
    {
        auto compressed = zlib_compress(body, opts.compress_level);
        write_bytes(out, compressed.data(), compressed.size());
    }
    else
    {
        write_bytes(out, body.data(), body.size());
    }

    return out;
}

//------------------------------------------------------------------------------
// TLV decode
//------------------------------------------------------------------------------

static AnchorData
decode_anchor(std::span<const uint8_t> payload)
{
    AnchorData a;
    size_t pos = 0;

    // Fixed fields
    read_bytes(payload, pos, a.ledger_hash.data(), 32);

    uint8_t pubkey[33];
    read_bytes(payload, pos, pubkey, 33);
    a.publisher_key_hex = hex_encode(pubkey, 33);

    a.publisher_manifest = read_vl(payload, pos);
    a.blob_signature = read_vl(payload, pos);

    // Reconstitute the blob JSON using the stored shape byte for key ordering.
    {
        uint8_t shape = payload[pos++];
        uint8_t top_perm = shape & 0x07;
        uint8_t val_perm = (shape >> 3) & 0x01;

        auto top_order = get_key_permutation(3, top_perm);
        auto val_order = get_key_permutation(2, val_perm);

        uint32_t sequence = read_u32_be(payload, pos);
        uint32_t expiration = read_u32_be(payload, pos);
        size_t validator_count = leb128_decode(payload, pos);

        // Read all validator data first
        struct ValEntry
        {
            std::string pubkey_hex;
            std::string manifest_b64;
        };
        std::vector<ValEntry> vals;
        for (size_t i = 0; i < validator_count; i++)
        {
            ValEntry ve;
            uint8_t vpk[33];
            read_bytes(payload, pos, vpk, 33);
            ve.pubkey_hex = hex_encode(vpk, 33);
            auto m_bytes = read_vl(payload, pos);
            ve.manifest_b64 = catl::base64_encode(m_bytes);
            vals.push_back(std::move(ve));
        }

        // Build JSON with correct key ordering
        // Top-level values indexed: 0=sequence, 1=expiration, 2=validators
        std::string blob_json = "{";
        for (int k = 0; k < 3; k++)
        {
            if (k > 0)
                blob_json += ',';
            int key_idx = top_order[k];
            blob_json += '"';
            blob_json += BLOB_KEYS[key_idx];
            blob_json += "\":";
            if (key_idx == 0)
            {
                blob_json += std::to_string(sequence);
            }
            else if (key_idx == 1)
            {
                blob_json += std::to_string(expiration);
            }
            else
            {
                blob_json += '[';
                for (size_t i = 0; i < vals.size(); i++)
                {
                    if (i > 0)
                        blob_json += ',';
                    // Validator keys: 0=validation_public_key, 1=manifest
                    blob_json += '{';
                    for (int vk = 0; vk < 2; vk++)
                    {
                        if (vk > 0)
                            blob_json += ',';
                        int vkey_idx = val_order[vk];
                        blob_json += '"';
                        blob_json += VAL_KEYS[vkey_idx];
                        blob_json += "\":\"";
                        blob_json += (vkey_idx == 0) ? vals[i].pubkey_hex
                                                     : vals[i].manifest_b64;
                        blob_json += '"';
                    }
                    blob_json += '}';
                }
                blob_json += ']';
            }
        }
        blob_json += '}';

        a.blob.assign(blob_json.begin(), blob_json.end());
    }

    // Validations: reconstitute from common + unique fields
    {
        size_t val_count = leb128_decode(payload, pos);
        if (val_count == 0)
        {
            return a;
        }

        auto protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();

        // Read common fields (canonical binary)
        auto common_bytes = read_vl(payload, pos);

        // Parse common fields to JSON
        Slice common_slice(common_bytes.data(), common_bytes.size());
        catl::xdata::JsonVisitor common_visitor(protocol);
        catl::xdata::ParserContext common_ctx(common_slice);
        catl::xdata::parse_with_visitor(common_ctx, protocol, common_visitor);
        auto common_json = common_visitor.get_result().as_object();

        // Read unique field names
        size_t num_unique_keys = leb128_decode(payload, pos);
        std::vector<std::string> unique_keys;
        for (size_t i = 0; i < num_unique_keys; i++)
        {
            size_t name_len = leb128_decode(payload, pos);
            if (pos + name_len > payload.size())
                throw std::runtime_error(
                    "binary proof: unexpected end reading field name");
            unique_keys.emplace_back(
                reinterpret_cast<char const*>(payload.data() + pos), name_len);
            pos += name_len;
        }

        // Per validator: read signing key + unique fields, merge with common,
        // re-serialize to canonical binary
        for (size_t i = 0; i < val_count; i++)
        {
            uint8_t vkey[33];
            read_bytes(payload, pos, vkey, 33);
            auto key_hex = hex_encode(vkey, 33);

            auto unique_bytes = read_vl(payload, pos);

            // Parse unique fields
            Slice unique_slice(unique_bytes.data(), unique_bytes.size());
            catl::xdata::JsonVisitor unique_visitor(protocol);
            catl::xdata::ParserContext unique_ctx(unique_slice);
            catl::xdata::parse_with_visitor(
                unique_ctx, protocol, unique_visitor);
            auto unique_json = unique_visitor.get_result().as_object();

            // Merge: common + unique
            boost::json::object merged = common_json;
            for (auto const& [k, v] : unique_json)
            {
                // Skip hint fields
                if (!k.empty() && std::islower(k[0]))
                    continue;
                merged[k] = v;
            }

            // Re-serialize to canonical binary
            auto full_bytes =
                catl::xdata::codecs::serialize_object(merged, protocol);
            a.validations[key_hex] = std::move(full_bytes);
        }
    }

    return a;
}

static HeaderData
decode_header(std::span<const uint8_t> payload)
{
    HeaderData h;
    size_t pos = 0;

    h.seq = read_u32_be(payload, pos);
    h.drops = read_u64_be(payload, pos);
    read_bytes(payload, pos, h.parent_hash.data(), 32);
    read_bytes(payload, pos, h.tx_hash.data(), 32);
    read_bytes(payload, pos, h.account_hash.data(), 32);
    h.parent_close_time = read_u32_be(payload, pos);
    h.close_time = read_u32_be(payload, pos);

    if (pos < payload.size())
        h.close_time_resolution = payload[pos++];
    if (pos < payload.size())
        h.close_flags = payload[pos++];

    return h;
}

/// Decode TLV body (no file header).
static ProofChain
decode_tlv_body(std::span<const uint8_t> data)
{
    ProofChain chain;
    size_t pos = 0;

    while (pos < data.size())
    {
        uint8_t type = data[pos++];
        size_t length = leb128_decode(data, pos);

        if (pos + length > data.size())
            throw std::runtime_error("binary proof: TLV length exceeds data");

        std::span<const uint8_t> payload(data.data() + pos, length);
        pos += length;

        switch (type)
        {
            case TLV_ANCHOR:
                chain.steps.push_back(decode_anchor(payload));
                break;

            case TLV_LEDGER_HEADER:
                chain.steps.push_back(decode_header(payload));
                break;

            case TLV_MAP_PROOF_TX:
            case TLV_MAP_PROOF_STATE: {
                TrieData trie;
                trie.tree = (type == TLV_MAP_PROOF_STATE)
                    ? TrieData::TreeType::state
                    : TrieData::TreeType::tx;
                trie.trie_binary.assign(
                    payload.data(), payload.data() + payload.size());
                chain.steps.push_back(std::move(trie));
                break;
            }

            default:
                break;
        }
    }

    return chain;
}

//------------------------------------------------------------------------------
// Public decode
//------------------------------------------------------------------------------

ProofChain
from_binary(std::span<const uint8_t> data)
{
    if (data.size() < 6)
        throw std::runtime_error("binary proof: file too short");

    // Check magic
    if (std::memcmp(data.data(), MAGIC, 4) != 0)
        throw std::runtime_error("binary proof: bad magic (expected XPRF)");

    uint8_t version = data[4];
    if (version != VERSION)
        throw std::runtime_error(
            "binary proof: unsupported version " + std::to_string(version));

    uint8_t flags = data[5];
    auto body_span = data.subspan(6);

    if (flags & FLAG_ZLIB)
    {
        auto decompressed = zlib_decompress(body_span);
        return decode_tlv_body(decompressed);
    }
    else
    {
        return decode_tlv_body(body_span);
    }
}

}  // namespace xproof
