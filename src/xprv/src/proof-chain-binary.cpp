#include "xprv/proof-chain-binary.h"
#include "xprv/anchor-verifier.h"
#include "xprv/hex-utils.h"
#include "xprv/proof-chain-json.h"

#include <catl/shamap/binary-trie.h>
#include <catl/shamap/json-trie.h>

#include <catl/xdata/protocol.h>
#include <catl/xdata/serialize.h>

#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/filter/zlib.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/json.hpp>
#include <zlib.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cstring>
#include <limits>
#include <map>
#include <sstream>
#include <stdexcept>

using catl::shamap::leb128_decode;
using catl::shamap::leb128_encode;

namespace xprv {

// File header
static constexpr auto const& MAGIC = XPRV_MAGIC;
static constexpr uint8_t VERSION = XPRV_FORMAT_VERSION;
static constexpr uint8_t FLAG_ZLIB = 0x01;

// TLV type bytes
static constexpr uint8_t TLV_ANCHOR = 0x01;
static constexpr uint8_t TLV_LEDGER_HEADER = 0x02;
static constexpr uint8_t TLV_MAP_PROOF_TX = 0x03;
static constexpr uint8_t TLV_MAP_PROOF_STATE = 0x04;
static constexpr uint8_t TLV_ANCHOR_UNL = 0x05;  // VL blob, goes at end

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
    // Subtraction form: `pos + len` could overflow on 32-bit size_t when
    // len is attacker-controlled (leb128 allows up to uint32 max), wrapping
    // past the check and letting the copy below walk off the buffer.
    // pos <= data.size() is guaranteed (leb128_decode only advances within
    // bounds).
    if (len > data.size() - pos)
        throw std::runtime_error(
            "binary proof: unexpected end reading VL data");
    std::vector<uint8_t> result(data.data() + pos, data.data() + pos + len);
    pos += len;
    return result;
}

static void
read_bytes(std::span<const uint8_t> data, size_t& pos, uint8_t* out, size_t len)
{
    // Overflow-safe: guard pos first, then subtraction form for len.
    if (pos > data.size() || len > data.size() - pos)
        throw std::runtime_error("binary proof: unexpected end reading bytes");
    std::memcpy(out, data.data() + pos, len);
    pos += len;
}

static int
hex_nibble(char c)
{
    if (c >= '0' && c <= '9')
        return c - '0';
    if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
    if (c >= 'A' && c <= 'F')
        return c - 'A' + 10;
    return -1;
}

static std::vector<uint8_t>
hex_decode(std::string_view hex)
{
    // Bounds-safe, validated nibble decode. The previous sscanf("%2x") on a
    // non-null-terminated string_view left `byte` uninitialized and its return
    // unchecked, so malformed hex pushed garbage bytes (sec #0054). Now reject
    // a bad digit instead.
    if (hex.size() % 2 != 0)
        throw std::runtime_error("binary proof: odd number of hex digits");
    std::vector<uint8_t> result;
    result.reserve(hex.size() / 2);
    for (size_t i = 0; i + 1 < hex.size(); i += 2)
    {
        int hi = hex_nibble(hex[i]);
        int lo = hex_nibble(hex[i + 1]);
        if (hi < 0 || lo < 0)
            throw std::runtime_error("binary proof: invalid hex digit");
        result.push_back(static_cast<uint8_t>((hi << 4) | lo));
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

namespace {

struct InflateStream
{
    z_stream zs{};

    InflateStream()
    {
        if (inflateInit(&zs) != Z_OK)
            throw std::runtime_error("binary proof: zlib init failed");
    }
    ~InflateStream()
    {
        inflateEnd(&zs);
    }
    InflateStream(InflateStream const&) = delete;
    InflateStream&
    operator=(InflateStream const&) = delete;
};

}  // namespace

// The body is one RFC 1950 stream (§3.1). Without a cap a hostile body of a
// few hundred KB of zeros inflates to hundreds of MB, so output is bounded
// before each append rather than after a bomb has allocated. Input left over
// after the stream's end is refused.
static std::vector<uint8_t>
zlib_decompress(std::span<const uint8_t> input)
{
    // The caller caps the body first, so the length fits zlib's uInt.
    static_assert(XPRV_MAX_BODY_SIZE <= std::numeric_limits<uInt>::max());
    if (input.size() > XPRV_MAX_BODY_SIZE)
        throw std::runtime_error("binary proof: body exceeds size cap");

    InflateStream stream;
    auto& zs = stream.zs;
    zs.next_in = const_cast<Bytef*>(input.data());
    zs.avail_in = static_cast<uInt>(input.size());

    std::vector<uint8_t> output;
    std::array<uint8_t, 64 * 1024> chunk{};
    int rc = Z_OK;
    while (rc != Z_STREAM_END)
    {
        zs.next_out = chunk.data();
        zs.avail_out = static_cast<uInt>(chunk.size());
        rc = inflate(&zs, Z_NO_FLUSH);
        // Z_BUF_ERROR: no progress is possible, so the input ran out first.
        if (rc == Z_BUF_ERROR)
            throw std::runtime_error("binary proof: truncated zlib body");
        if (rc != Z_OK && rc != Z_STREAM_END)
            throw std::runtime_error("binary proof: invalid zlib body");
        auto const produced = chunk.size() - zs.avail_out;
        if (produced > XPRV_MAX_BODY_SIZE - output.size())
            throw std::runtime_error(
                "binary proof: decompressed size exceeds cap");
        output.insert(output.end(), chunk.begin(), chunk.begin() + produced);
    }
    if (zs.avail_in != 0)
        throw std::runtime_error("binary proof: bytes after the zlib stream");
    return output;
}

//------------------------------------------------------------------------------
// TLV encode
//------------------------------------------------------------------------------

/// Encode anchor core: ledger hash + publisher key + the validations, each
/// as its raw signed bytes (§3.4). It goes at the front, so a verifier
/// holding the list can check the signatures before the list arrives.
static std::vector<uint8_t>
encode_anchor_core(AnchorData const& a)
{
    std::vector<uint8_t> payload;

    write_bytes(payload, a.ledger_hash.data(), 32);

    auto const pubkey = hex_decode(a.publisher_key_hex);
    if (pubkey.size() != 33)
        throw std::runtime_error(
            "binary proof: publisher key must be exactly 33 bytes");
    write_bytes(payload, pubkey.data(), 33);

    // Ascending order of the embedded SigningPubKey. Direct AnchorData
    // callers never passed through the collector, so a validation that is
    // not canonical, or sits under another key, must not become a proof.
    std::map<std::vector<uint8_t>, std::vector<uint8_t> const*> by_key;
    for (auto const& [key_hex, val_bytes] : a.validations)
    {
        auto const key =
            AnchorVerifier::canonical_validation_signing_key(val_bytes);
        if (!key)
            throw std::runtime_error(
                "binary proof: validation is not a canonical STValidation");
        if (hex_decode(key_hex) != *key)
            throw std::runtime_error(
                "binary proof: validation key does not match SigningPubKey");
        if (!by_key.emplace(*key, &val_bytes).second)
            throw std::runtime_error(
                "binary proof: validations must be distinct and sorted by "
                "SigningPubKey");
    }

    leb128_encode(payload, by_key.size());
    for (auto const& entry : by_key)
        write_vl(payload, *entry.second);

    return payload;
}

/// Encode anchor UNL: publisher manifest + list signature + the exact signed
/// list bytes (§3.5). It goes at the end, needed only by a verifier that
/// does not already hold the list.
static std::vector<uint8_t>
encode_anchor_unl(AnchorData const& a)
{
    // An anchor without its list omits 0x05 entirely (spec §3.3); the
    // verifier takes the list from trust material it already holds.
    if (a.blob.empty())
    {
        if (!a.publisher_manifest.empty() || !a.blob_signature.empty())
            throw std::runtime_error(
                "binary proof: anchor UNL has a manifest or signature but "
                "no validator list blob");
        return {};
    }
    if (a.publisher_manifest.empty() || a.blob_signature.empty())
        throw std::runtime_error(
            "binary proof: a bundled list needs a non-empty manifest, list "
            "and signature");

    std::vector<uint8_t> payload;
    write_vl(payload, a.publisher_manifest);
    write_vl(payload, a.blob_signature);
    write_vl(payload, a.blob);
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
///
/// TLV ordering for progressive verification:
///   1. Anchor core (0x01) — hash + publisher key + validations
///   2. Headers + map proofs (0x02, 0x03, 0x04) — the actual proof chain
///   3. Anchor UNL (0x05) — VL blob, only needed if no cached UNL
///
/// A verifier with a cached UNL can verify after reading (1) + (2),
/// without ever touching (3). For QR codes this means the customer
/// can pull the phone away after the first few frames.
// Defined below, next to the binary trie reader it inverts.
static void
encode_trie_json_node(
    std::vector<uint8_t>& out,
    boost::json::value const& node,
    bool is_tx,
    catl::xdata::Protocol const& protocol);

static std::vector<uint8_t>
encode_tlv_body(ProofChain const& chain, catl::xdata::Protocol const* protocol)
{
    std::vector<uint8_t> out;
    std::vector<uint8_t> unl_tail;  // UNL TLV(s), appended at end

    for (auto const& step : chain.steps)
    {
        std::visit(
            [&](auto const& data) {
                using T = std::decay_t<decltype(data)>;
                if constexpr (std::is_same_v<T, AnchorData>)
                {
                    write_tlv(out, TLV_ANCHOR, encode_anchor_core(data));
                    // UNL goes at the very end
                    auto unl = encode_anchor_unl(data);
                    if (!unl.empty())
                    {
                        write_tlv(unl_tail, TLV_ANCHOR_UNL, unl);
                    }
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
                    else if (!data.trie_json.is_null() && protocol)
                    {
                        catl::shamap::validate_json_trie(data.trie_json);
                        std::vector<uint8_t> encoded;
                        encode_trie_json_node(
                            encoded,
                            data.trie_json,
                            data.tree == TrieData::TreeType::tx,
                            *protocol);
                        write_tlv(out, type_byte, encoded);
                    }
                    else
                    {
                        throw std::runtime_error(
                            data.trie_json.is_null()
                                ? "to_binary: TrieData has neither a binary "
                                  "nor a JSON trie"
                                : "to_binary: chain has JSON-only tries; pass "
                                  "BinaryOptions::protocol so leaves can be "
                                  "re-serialized to canonical bytes");
                    }
                }
            },
            step);
    }

    // Append UNL data at the very end
    out.insert(out.end(), unl_tail.begin(), unl_tail.end());

    return out;
}

//------------------------------------------------------------------------------
// Public encode
//------------------------------------------------------------------------------

std::vector<uint8_t>
to_binary(ProofChain const& chain, BinaryOptions const& opts)
{
    auto body = encode_tlv_body(chain, opts.protocol);

    std::vector<uint8_t> out;
    out.reserve(10 + body.size());

    // File header: magic(4) + version(1) + flags(1) + network_id(4 BE)
    write_bytes(out, MAGIC, 4);
    out.push_back(VERSION);

    uint8_t flags = 0;
    if (opts.compress)
        flags |= FLAG_ZLIB;
    out.push_back(flags);

    // Network ID (big-endian u32, like every other multi-byte integer)
    write_u32_be(out, chain.network_id);

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

/// Decode anchor core: ledger hash + publisher key + raw validations.
static AnchorData
decode_anchor_core(std::span<const uint8_t> payload)
{
    AnchorData a;
    size_t pos = 0;

    read_bytes(payload, pos, a.ledger_hash.data(), 32);

    uint8_t pubkey[33];
    read_bytes(payload, pos, pubkey, 33);
    a.publisher_key_hex = hex_encode(pubkey, 33);

    // Each record is a complete canonical STValidation (§2.1). Its own
    // SigningPubKey orders it; there is no second key on the wire.
    size_t const val_count = leb128_decode(payload, pos);
    std::vector<uint8_t> previous;
    for (size_t i = 0; i < val_count; i++)
    {
        auto raw = read_vl(payload, pos);
        auto key = AnchorVerifier::canonical_validation_signing_key(raw);
        if (!key)
            throw std::runtime_error(
                "binary proof: validation is not a canonical STValidation");
        if (i > 0 && !(previous < *key))
            throw std::runtime_error(
                "binary proof: validations must be distinct and sorted by "
                "SigningPubKey");
        a.validations[hex_encode(key->data(), key->size())] = std::move(raw);
        previous = std::move(*key);
    }

    if (pos != payload.size())
        throw std::runtime_error(
            "binary proof: trailing bytes in anchor core payload");

    return a;
}

/// Decode anchor UNL: manifest + list signature + the signed list bytes.
/// Merges into an existing AnchorData (from decode_anchor_core).
static void
decode_anchor_unl(std::span<const uint8_t> payload, AnchorData& a)
{
    size_t pos = 0;

    a.publisher_manifest = read_vl(payload, pos);
    a.blob_signature = read_vl(payload, pos);
    a.blob = read_vl(payload, pos);

    if (pos != payload.size())
        throw std::runtime_error(
            "binary proof: trailing bytes in anchor UNL payload");
    if (a.publisher_manifest.empty() || a.blob_signature.empty() ||
        a.blob.empty())
        throw std::runtime_error(
            "binary proof: a bundled list needs a non-empty manifest, list "
            "and signature");
}

static HeaderData
decode_header(std::span<const uint8_t> payload)
{
    if (payload.size() != 118)
        throw std::runtime_error(
            "binary proof: ledger header payload must be exactly 118 bytes");

    HeaderData h;
    size_t pos = 0;

    h.seq = read_u32_be(payload, pos);
    h.drops = read_u64_be(payload, pos);
    read_bytes(payload, pos, h.parent_hash.data(), 32);
    read_bytes(payload, pos, h.tx_hash.data(), 32);
    read_bytes(payload, pos, h.account_hash.data(), 32);
    h.parent_close_time = read_u32_be(payload, pos);
    h.close_time = read_u32_be(payload, pos);

    h.close_time_resolution = payload[pos++];
    h.close_flags = payload[pos++];

    return h;
}

/// Decode TLV body (no file header).
/// Anchor core (0x01) and anchor UNL (0x05) are separate TLV records
/// that get merged into a single AnchorData step.
static ProofChain
decode_tlv_body(std::span<const uint8_t> data)
{
    ProofChain chain;
    size_t pos = 0;

    // Track the anchor index so we can merge UNL into it later
    int anchor_step_idx = -1;
    bool seen_anchor = false;
    bool seen_anchor_unl = false;

    while (pos < data.size())
    {
        if (seen_anchor_unl)
            throw std::runtime_error(
                "binary proof: record found after anchor UNL");

        uint8_t type = data[pos++];
        size_t length = leb128_decode(data, pos);

        if (length > data.size() - pos)
            throw std::runtime_error("binary proof: TLV length exceeds data");

        std::span<const uint8_t> payload(data.data() + pos, length);
        pos += length;

        switch (type)
        {
            case TLV_ANCHOR:
                if (seen_anchor)
                    throw std::runtime_error(
                        "binary proof: duplicate anchor core");
                seen_anchor = true;
                anchor_step_idx = static_cast<int>(chain.steps.size());
                chain.steps.push_back(decode_anchor_core(payload));
                break;

            case TLV_ANCHOR_UNL:
                // Merge into the existing anchor step
                if (!seen_anchor || anchor_step_idx < 0)
                    throw std::runtime_error(
                        "binary proof: anchor UNL precedes anchor core");
                if (seen_anchor_unl)
                    throw std::runtime_error(
                        "binary proof: duplicate anchor UNL");
                seen_anchor_unl = true;
                decode_anchor_unl(
                    payload,
                    std::get<AnchorData>(chain.steps[anchor_step_idx]));
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
                throw std::runtime_error(
                    "binary proof: unknown TLV type " + std::to_string(type));
        }
    }

    // NB: the "exactly one anchor" rule (spec §1, §3.8) is enforced by the
    // verifier, not here. This function is a codec: it decodes whatever TLV
    // records are present, and unit tests legitimately round-trip partial
    // chains (a lone header, a lone trie) through it. Asserting proof structure
    // at this layer conflated framing with validity and broke exactly those
    // round-trips. The 0x05 anchor UNL is separately OPTIONAL by design — see
    // §3.3: omitting it is how a verifier with cached or on-chain-pinned trust
    // avoids shipping the ~15KB VL blob.
    return chain;
}

//------------------------------------------------------------------------------
// JSON trie -> binary trie
//------------------------------------------------------------------------------

// Inverse of the binary trie reader (§3.7). Needed because TrieData carries two
// independent representations and only the builder fills both: a chain decoded
// with from_json() has trie_json but no trie_binary, so to_binary() could not
// serialize it. That made the canonical binary encoding producible only at
// prove time, with live peers — which blocked offline test-vector generation
// and left spec §8's "both forms verify identically" untestable in one
// direction.
//
// JSON node discrimination per §2.3:
//   string (64 hex) -> pruned subtree hash   -> 2-bit code 11
//   object ("0".."F") -> inner node          -> 2-bit code 10
//   array [key, data] -> leaf                -> 2-bit code 01
//   absent branch                            -> 2-bit code 00
static void
encode_trie_json_node(
    std::vector<uint8_t>& out,
    boost::json::value const& node,
    bool is_tx,
    catl::xdata::Protocol const& protocol)
{
    if (!node.is_object())
        throw std::runtime_error(
            "to_binary: JSON trie inner node must be an object");
    auto const& obj = node.as_object();

    // 2 bits per branch, 4 branches per byte, low bits first.
    std::array<uint8_t, 4> header{0, 0, 0, 0};
    std::array<boost::json::value const*, 16> children{};

    for (int i = 0; i < 16; ++i)
    {
        static constexpr char const* kNibble = "0123456789ABCDEF";
        std::string key(1, kNibble[i]);
        // Uppercase only (spec §2.3). The previous uppercase-then-lowercase
        // fallback made this encoder non-injective: with both spellings present
        // the shadowed leaf was silently dropped, so a JSON proof that verified
        // became binary bytes that did not.
        auto it = obj.find(key);
        if (it == obj.end())
            continue;

        boost::json::value const& child = it->value();
        uint8_t code = 0;
        if (child.is_string())
            code = 0b11;
        else if (child.is_object())
            code = 0b10;
        else if (child.is_array())
            code = 0b01;
        else
            throw std::runtime_error(
                "to_binary: JSON trie branch has unsupported type");

        header[static_cast<size_t>(i) / 4] |=
            static_cast<uint8_t>(code << (2 * (i % 4)));
        children[static_cast<size_t>(i)] = &child;
    }

    out.insert(out.end(), header.begin(), header.end());

    for (int i = 0; i < 16; ++i)
    {
        if (children[static_cast<size_t>(i)] == nullptr)
            continue;
        boost::json::value const& child = *children[static_cast<size_t>(i)];

        if (child.is_string())
        {
            auto h = hash_from_hex(std::string(child.as_string()));
            out.insert(out.end(), h.data(), h.data() + 32);
        }
        else if (child.is_object())
        {
            encode_trie_json_node(out, child, is_tx, protocol);
        }
        else
        {
            auto const& arr = child.as_array();
            if (arr.size() != 2 || !arr[0].is_string())
                throw std::runtime_error(
                    "to_binary: JSON trie leaf must be [key, data]");

            auto key = hash_from_hex(std::string(arr[0].as_string()));
            out.insert(out.end(), key.data(), key.data() + 32);

            // JSON leaves are decoded only (§2.3, v7); the decoded form must
            // round-trip back through the codec.
            std::vector<uint8_t> data;
            if (arr[1].is_object())
            {
                // Encode from the DECODED content, deliberately ignoring any
                // sibling `blob`. Sourcing the bytes from `blob` would make the
                // hashed datum something other than the JSON a reader sees, so
                // the legible form and the authenticated form could diverge
                // while the proof still verified. Re-serialising is what keeps
                // "what you see is what you hash" true. A leaf whose decoded
                // form cannot round-trip has no JSON form at all; only the
                // binary form can carry it.
                require_codec_members(arr[1], is_tx, protocol);
                data = is_tx
                    ? catl::xdata::serialize_transaction(
                          arr[1].as_object(), protocol)
                    : catl::xdata::serialize_leaf(arr[1].as_object(), protocol);
            }
            else
                throw std::runtime_error(
                    "to_binary: JSON trie leaf data must be a decoded object, "
                    "not hex (§2.3)");

            leb128_encode(out, data.size());
            out.insert(out.end(), data.begin(), data.end());
        }
    }
}

//------------------------------------------------------------------------------
// Public decode
//------------------------------------------------------------------------------

ProofChain
from_binary(std::span<const uint8_t> data)
{
    if (data.size() < 10)
        throw std::runtime_error("binary proof: file too short");

    // Check magic
    if (std::memcmp(data.data(), MAGIC, 4) != 0)
        throw std::runtime_error("binary proof: bad magic (expected XPRV)");

    uint8_t version = data[4];
    if (version != VERSION)
        throw std::runtime_error(
            "binary proof: unsupported version " + std::to_string(version));

    uint8_t flags = data[5];
    if ((flags & static_cast<uint8_t>(~FLAG_ZLIB)) != 0)
        throw std::runtime_error("binary proof: reserved flag bits are set");

    // Network ID (big-endian u32)
    size_t net_pos = 6;
    uint32_t network_id = read_u32_be(data, net_pos);

    // §8: the cap holds before parsing, compressed or not.
    auto body_span = data.subspan(10);
    if (body_span.size() > XPRV_MAX_BODY_SIZE)
        throw std::runtime_error("binary proof: body exceeds size cap");

    ProofChain chain;
    if (flags & FLAG_ZLIB)
    {
        auto decompressed = zlib_decompress(body_span);
        chain = decode_tlv_body(decompressed);
    }
    else
    {
        chain = decode_tlv_body(body_span);
    }

    chain.network_id = network_id;
    return chain;
}

}  // namespace xprv
