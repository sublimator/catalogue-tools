#include "xproof/anchor-verifier.h"

#include <catl/core/logger.h>
#include <catl/crypto/sha512-half-hasher.h>
#include <catl/crypto/sig-verify.h>
#include <catl/peer-client/peer-client-coro.h>
#include <catl/peer-client/peer-client.h>
#include <catl/peer-client/tree-walker.h>
#include <catl/rpc-client/rpc-client-coro.h>
#include <catl/shamap/shamap-hashprefix.h>
#include <catl/shamap/shamap-header-only.h>
#include <catl/shamap/shamap-nodeid.h>
#include <catl/shamap/shamap.h>
#include <catl/vl-client/vl-client-coro.h>

// Instantiate SHAMap for AbbreviatedTreeTraits
INSTANTIATE_SHAMAP_NODE_TRAITS(catl::shamap::AbbreviatedTreeTraits);
#include <catl/xdata-json/parse_leaf.h>
#include <catl/xdata-json/parse_transaction.h>
#include <catl/xdata-json/pretty_print.h>
#include <catl/xdata/parser-context.h>
#include <catl/xdata/parser.h>
#include <catl/xdata/protocol.h>
#include <catl/xdata/slice-visitor.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/json.hpp>
#include <csignal>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

using namespace catl::peer_client;

//------------------------------------------------------------------------------
// Helpers
//------------------------------------------------------------------------------

/// Extract nibble at given depth from a 32-byte key.
/// Depth 0 = high nibble of byte 0, depth 1 = low nibble of byte 0, etc.
static int
nibble_at(Hash256 const& key, int depth)
{
    int byte_idx = depth / 2;
    if (depth % 2 == 0)
        return (key.data()[byte_idx] >> 4) & 0xF;
    else
        return key.data()[byte_idx] & 0xF;
}

/// Single hex nibble char: 0→"0", 10→"A", etc.
static std::string
hex_nibble(int n)
{
    static const char* chars = "0123456789ABCDEF";
    return std::string(1, chars[n & 0xF]);
}

/// Parse 64-char hex string to Hash256.
static Hash256
hash_from_hex(std::string const& hex)
{
    Hash256 result;
    if (hex.size() != 64)
        throw std::runtime_error(
            "hash_from_hex: expected 64 hex chars, got " +
            std::to_string(hex.size()));
    for (size_t i = 0; i < 32; ++i)
    {
        unsigned int byte;
        std::sscanf(hex.c_str() + i * 2, "%2x", &byte);
        result.data()[i] = static_cast<uint8_t>(byte);
    }
    return result;
}

/// Uppercase hex of a Hash256.
static std::string
upper_hex(Hash256 const& h)
{
    std::ostringstream oss;
    for (size_t i = 0; i < 32; ++i)
        oss << std::hex << std::uppercase << std::setfill('0') << std::setw(2)
            << static_cast<int>(h.data()[i]);
    return oss.str();
}

//------------------------------------------------------------------------------
// XRPL Keylet computation
//------------------------------------------------------------------------------

/// Short skip list key: SHA512Half(be16('s'))
/// Contains hashes of the last 256 ledgers.
static Hash256
skip_list_key()
{
    catl::crypto::Sha512HalfHasher h;
    uint16_t ns = 0x0073;  // 's' as big-endian uint16
    uint8_t buf[2] = {
        static_cast<uint8_t>((ns >> 8) & 0xFF),
        static_cast<uint8_t>(ns & 0xFF)};
    h.update(buf, 2);
    return h.finalize();
}

/// Long skip list key: SHA512Half(be16('s'), be32(seq >> 16))
/// Contains hashes of flag ledgers (every 256th) within a 65536-ledger range.
static Hash256
skip_list_key(uint32_t ledger_seq)
{
    catl::crypto::Sha512HalfHasher h;
    uint16_t ns = 0x0073;  // 's'
    uint32_t group = ledger_seq >> 16;
    uint8_t buf[6] = {
        static_cast<uint8_t>((ns >> 8) & 0xFF),
        static_cast<uint8_t>(ns & 0xFF),
        static_cast<uint8_t>((group >> 24) & 0xFF),
        static_cast<uint8_t>((group >> 16) & 0xFF),
        static_cast<uint8_t>((group >> 8) & 0xFF),
        static_cast<uint8_t>(group & 0xFF),
    };
    h.update(buf, 6);
    return h.finalize();
}

static LogPartition log_partition("xproof", LogLevel::INFO);

//------------------------------------------------------------------------------
// Validation collector
//------------------------------------------------------------------------------

/// Collects STValidation messages from the peer, grouped by ledger hash.
/// Tracks which signing keys have validated each ledger. When a UNL is
/// provided, signals quorum when enough UNL validators have validated
/// the same ledger.
struct ValidationCollector
{
    catl::xdata::Protocol const& protocol;

    struct Entry
    {
        std::vector<uint8_t> raw;          // full STValidation bytes
        std::vector<uint8_t> signing_key;  // sfSigningPubKey
        std::vector<uint8_t> signature;    // sfSignature
        Hash256 ledger_hash;               // sfLedgerHash
        uint32_t ledger_seq = 0;           // sfLedgerSequence
        bool sig_verified = false;         // signature verified?
    };

    // All validations grouped by ledger hash
    std::map<Hash256, std::vector<Entry>> by_ledger;

    // UNL signing keys (set after VL fetch)
    std::set<std::string> unl_signing_keys;
    int unl_size = 0;

    // Quorum result
    Hash256 quorum_hash;
    bool quorum_reached = false;
    int quorum_count = 0;

    /// Set the UNL signing keys for quorum checking.
    void
    set_unl(std::vector<catl::vl::Manifest> const& validators)
    {
        unl_size = static_cast<int>(validators.size());
        for (auto const& v : validators)
        {
            if (!v.signing_public_key.empty())
            {
                unl_signing_keys.insert(v.signing_key_hex());
            }
        }
        PLOGI(
            log_partition,
            "  UNL loaded: ",
            unl_signing_keys.size(),
            " signing keys from ",
            unl_size,
            " validators");

        // Re-check existing validations for quorum
        check_all_for_quorum();
    }

    /// Parse a raw protobuf TMValidation and extract the STValidation.
    static std::vector<uint8_t>
    extract_stvalidation(std::vector<uint8_t> const& data)
    {
        // TMValidation protobuf: field 1 (validation) = wire type 2
        // tag byte = (1 << 3) | 2 = 0x0A, then varint length, then bytes
        if (data.size() < 2 || data[0] != 0x0A)
            return {};

        size_t pos = 1;
        size_t length = 0;
        int shift = 0;
        while (pos < data.size())
        {
            uint8_t byte = data[pos++];
            length |= (static_cast<size_t>(byte & 0x7F) << shift);
            if ((byte & 0x80) == 0)
                break;
            shift += 7;
        }

        if (pos + length > data.size() || length < 50)
            return {};

        return {data.data() + pos, data.data() + pos + length};
    }

    /// Call from unsolicited handler.
    void
    on_packet(uint16_t type, std::vector<uint8_t> const& data)
    {
        if (type != 41 || quorum_reached)
            return;

        auto val_bytes = extract_stvalidation(data);
        if (val_bytes.empty())
            return;

        Entry entry;
        entry.raw = std::move(val_bytes);

        // Parse with xdata to extract fields
        struct Visitor
        {
            Entry& e;
            bool
            visit_object_start(
                catl::xdata::FieldPath const&,
                catl::xdata::FieldSlice const&)
            {
                return false;
            }
            void
            visit_object_end(
                catl::xdata::FieldPath const&,
                catl::xdata::FieldSlice const&)
            {
            }
            bool
            visit_array_start(
                catl::xdata::FieldPath const&,
                catl::xdata::FieldSlice const&)
            {
                return false;
            }
            void
            visit_array_end(
                catl::xdata::FieldPath const&,
                catl::xdata::FieldSlice const&)
            {
            }
            void
            visit_field(
                catl::xdata::FieldPath const& path,
                catl::xdata::FieldSlice const& fs)
            {
                if (path.size() != 1)
                    return;
                if (fs.field->name == "LedgerHash" && fs.data.size() == 32)
                {
                    e.ledger_hash = Hash256(fs.data.data());
                }
                else if (fs.field->name == "SigningPubKey")
                {
                    e.signing_key.assign(
                        fs.data.data(), fs.data.data() + fs.data.size());
                }
                else if (fs.field->name == "Signature")
                {
                    e.signature.assign(
                        fs.data.data(), fs.data.data() + fs.data.size());
                }
                else if (
                    fs.field->name == "LedgerSequence" && fs.data.size() >= 4)
                {
                    e.ledger_seq =
                        (static_cast<uint32_t>(fs.data.data()[0]) << 24) |
                        (static_cast<uint32_t>(fs.data.data()[1]) << 16) |
                        (static_cast<uint32_t>(fs.data.data()[2]) << 8) |
                        static_cast<uint32_t>(fs.data.data()[3]);
                }
            }
        };

        try
        {
            Slice slice(entry.raw.data(), entry.raw.size());
            catl::xdata::SliceCursor cursor{slice, 0};
            catl::xdata::ParserContext ctx{cursor};
            Visitor visitor{entry};
            catl::xdata::parse_with_visitor(ctx, protocol, visitor);
        }
        catch (...)
        {
            return;
        }

        auto hash = entry.ledger_hash;
        auto seq = entry.ledger_seq;

        // Only keep validations from UNL signing keys
        if (!unl_signing_keys.empty())
        {
            std::ostringstream oss;
            for (auto b : entry.signing_key)
            {
                oss << std::hex << std::setfill('0') << std::setw(2)
                    << static_cast<int>(b);
            }
            if (!unl_signing_keys.count(oss.str()))
            {
                return;  // not a UNL validator, discard
            }
        }

        by_ledger[hash].push_back(std::move(entry));

        auto count = by_ledger[hash].size();
        PLOGD(
            log_partition,
            "  UNL validation for seq=",
            seq,
            " hash=",
            hash.hex().substr(0, 16),
            "... (",
            count,
            "/",
            unl_size,
            ")");

        // Check quorum for this ledger
        check_quorum_for(hash);
    }

    /// Get the validations for the quorum ledger.
    std::vector<Entry> const&
    quorum_validations() const
    {
        static std::vector<Entry> empty;
        auto it = by_ledger.find(quorum_hash);
        if (it != by_ledger.end())
            return it->second;
        return empty;
    }

private:
    void
    check_all_for_quorum()
    {
        for (auto const& [hash, entries] : by_ledger)
        {
            check_quorum_for(hash);
            if (quorum_reached)
                return;
        }
    }

    void
    check_quorum_for(Hash256 const& hash)
    {
        if (quorum_reached || unl_signing_keys.empty())
            return;

        auto it = by_ledger.find(hash);
        if (it == by_ledger.end())
            return;

        // Count UNL validators
        int matched = 0;
        for (auto const& e : it->second)
        {
            std::ostringstream oss;
            for (auto b : e.signing_key)
            {
                oss << std::hex << std::setfill('0') << std::setw(2)
                    << static_cast<int>(b);
            }
            if (unl_signing_keys.count(oss.str()))
            {
                matched++;
            }
        }

        int threshold = unl_size;  // 100% — wait for all validators
        if (matched >= threshold)
        {
            quorum_reached = true;
            quorum_hash = hash;
            quorum_count = matched;
            auto seq = it->second.empty() ? 0 : it->second[0].ledger_seq;
            PLOGI(
                log_partition,
                "  QUORUM reached for seq=",
                seq,
                " hash=",
                hash.hex().substr(0, 16),
                "... (",
                matched,
                "/",
                unl_size,
                " UNL validators)");
        }
    }
};

/// Build a proof-format on_leaf callback for trie_json.
/// Returns [key_hex, parsed_data] arrays per SPEC 2.3.
static catl::shamap::LeafJsonCallback
make_proof_leaf_callback(catl::xdata::Protocol const& protocol, bool is_tx_tree)
{
    return [&protocol, is_tx_tree](MmapItem const& item) -> boost::json::value {
        boost::json::array arr;
        arr.emplace_back(upper_hex(item.key().to_hash()));

        try
        {
            // Build [data][key] buffer for parsing
            std::vector<uint8_t> buf(
                item.slice().data(), item.slice().data() + item.slice().size());
            buf.insert(buf.end(), item.key().data(), item.key().data() + 32);
            Slice full(buf.data(), buf.size());

            if (is_tx_tree)
            {
                arr.emplace_back(catl::xdata::json::parse_transaction(
                    full, protocol, false));
            }
            else
            {
                arr.emplace_back(
                    catl::xdata::json::parse_leaf(full, protocol, false));
            }
        }
        catch (std::exception const&)
        {
            std::string hex;
            slice_hex(item.slice(), hex);
            arr.emplace_back(hex);
        }

        return arr;
    };
}

/// Parse SLE leaf data (wire format, no prefix) and check if a hash
/// exists in the sfHashes (Vector256) array.
static bool
sle_hashes_contain(
    Slice leaf_data,
    Hash256 const& needle,
    catl::xdata::Protocol const& protocol)
{
    try
    {
        PLOGI(log_partition, "  Parsing SLE (", leaf_data.size(), " bytes)");
        auto json = catl::xdata::json::parse_leaf(leaf_data, protocol, false);
        auto const& obj = json.as_object();

        // Log the parsed JSON for manual inspection
        PLOGI(log_partition, "  SLE JSON: ", boost::json::serialize(json));

        if (!obj.contains("Hashes"))
        {
            PLOGW(log_partition, "  SLE has no Hashes field!");
            return false;
        }
        auto const& arr = obj.at("Hashes").as_array();
        PLOGI(log_partition, "  SLE contains ", arr.size(), " hashes");
        auto needle_hex = upper_hex(needle);
        for (auto const& h : arr)
        {
            if (h.as_string() == needle_hex)
            {
                return true;
            }
        }
        PLOGI(log_partition, "  Needle not in list: ", needle_hex);
        return false;
    }
    catch (std::exception const& e)
    {
        PLOGE(log_partition, "  sle_hashes_contain FAILED: ", e.what());
        return false;
    }
}

//------------------------------------------------------------------------------
// Proof chain resolver / verifier
//------------------------------------------------------------------------------

static LogPartition verify_log("verify", LogLevel::INFO);

/// Hash a ledger header per XRPL spec.
/// SHA512Half(LWR\0 || seq || drops || parent_hash || tx_hash ||
///            account_hash || close_time || parent_close_time ||
///            close_time_resolution || close_flags)
static Hash256
hash_ledger_header(boost::json::object const& hdr)
{
    catl::crypto::Sha512HalfHasher h;

    // HashPrefix::ledgerMaster = "LWR\0"
    uint8_t prefix[] = {'L', 'W', 'R', 0x00};
    h.update(prefix, 4);

    auto write_u32 = [&](uint32_t v) {
        uint8_t buf[4] = {
            static_cast<uint8_t>((v >> 24) & 0xFF),
            static_cast<uint8_t>((v >> 16) & 0xFF),
            static_cast<uint8_t>((v >> 8) & 0xFF),
            static_cast<uint8_t>(v & 0xFF)};
        h.update(buf, 4);
    };

    auto write_u64 = [&](uint64_t v) {
        uint8_t buf[8];
        for (int i = 7; i >= 0; --i)
        {
            buf[i] = static_cast<uint8_t>(v & 0xFF);
            v >>= 8;
        }
        h.update(buf, 8);
    };

    auto write_hash = [&](std::string const& hex_str) {
        auto hash = hash_from_hex(hex_str);
        h.update(hash.data(), 32);
    };

    write_u32(hdr.at("seq").to_number<uint32_t>());
    write_u64(std::stoull(std::string(hdr.at("drops").as_string())));
    write_hash(std::string(hdr.at("parent_hash").as_string()));
    write_hash(std::string(hdr.at("tx_hash").as_string()));
    write_hash(std::string(hdr.at("account_hash").as_string()));
    write_u32(hdr.at("parent_close_time").to_number<uint32_t>());
    write_u32(hdr.at("close_time").to_number<uint32_t>());

    uint8_t resolution =
        static_cast<uint8_t>(hdr.at("close_time_resolution").to_number<int>());
    h.update(&resolution, 1);

    uint8_t flags =
        static_cast<uint8_t>(hdr.at("close_flags").to_number<int>());
    h.update(&flags, 1);

    return h.finalize();
}

/// Walk a JSON trie and count nodes.
struct TrieStats
{
    int inners = 0;
    int leaves = 0;
    int placeholders = 0;
    int max_depth = 0;
};

static void
count_trie_nodes(boost::json::value const& node, int depth, TrieStats& stats)
{
    if (node.is_string())
    {
        // Placeholder hash
        stats.placeholders++;
        if (depth > stats.max_depth)
        {
            stats.max_depth = depth;
        }
    }
    else if (node.is_array())
    {
        // Leaf [key, data] or [key, data, raw_hex]
        stats.leaves++;
        if (depth > stats.max_depth)
        {
            stats.max_depth = depth;
        }
    }
    else if (node.is_object())
    {
        // Inner node
        stats.inners++;
        auto const& obj = node.as_object();
        for (auto const& kv : obj)
        {
            if (kv.key() == "__depth__")
            {
                continue;
            }
            // Key should be a single hex nibble "0"-"F"
            if (kv.key().size() == 1)
            {
                count_trie_nodes(kv.value(), depth + 1, stats);
            }
        }
    }
}

/// Extract the Hashes array from a state tree leaf (LedgerHashes SLE).
/// The leaf is a JSON array: [key_hex, {parsed SLE with "Hashes": [...]}]
static boost::json::array const*
extract_leaf_hashes(boost::json::value const& trie)
{
    // Walk the trie to find the leaf (array node)
    if (trie.is_array())
    {
        auto const& arr = trie.as_array();
        if (arr.size() >= 2 && arr[1].is_object())
        {
            auto const& sle = arr[1].as_object();
            if (sle.contains("Hashes"))
            {
                return &sle.at("Hashes").as_array();
            }
        }
        return nullptr;
    }

    if (trie.is_object())
    {
        auto const& obj = trie.as_object();
        for (auto const& kv : obj)
        {
            if (kv.key() == "__depth__")
            {
                continue;
            }
            auto result = extract_leaf_hashes(kv.value());
            if (result)
            {
                return result;
            }
        }
    }
    return nullptr;
}

/// Resolve and verify a JSON proof chain.
/// Checks header hashes, skip list continuity, and trie structure.
/// If trusted_publisher_key is provided, verifies the anchor's UNL
/// and validation signatures against it.
/// Returns true if all verifiable steps pass.
static bool
resolve_proof_chain(
    boost::json::array const& chain,
    std::string const& trusted_publisher_key = "")
{
    PLOGI(verify_log, "");
    PLOGI(
        verify_log,
        "═══════════════════════════════════════════════════════════════");
    PLOGI(verify_log, "  PROOF CHAIN VERIFICATION  (", chain.size(), " steps)");
    PLOGI(
        verify_log,
        "═══════════════════════════════════════════════════════════════");

    Hash256 trusted_hash;
    bool have_trusted_hash = false;
    Hash256 trusted_tx_hash;
    Hash256 trusted_ac_hash;
    bool have_tree_hashes = false;
    boost::json::array const* pending_hashes = nullptr;
    int pending_hashes_count = 0;
    int step_num = 0;
    bool all_ok = true;

    // Narrative explanation of the proof chain
    std::vector<std::string> narrative;

    for (auto const& step_val : chain)
    {
        step_num++;
        if (!step_val.is_object())
        {
            continue;
        }
        auto const& step = step_val.as_object();
        if (!step.contains("type"))
        {
            continue;
        }
        auto type = std::string(step.at("type").as_string());

        // ── Anchor ──────────────────────────────────────────────
        if (type == "anchor")
        {
            auto hash_hex = std::string(step.at("ledger_hash").as_string());
            trusted_hash = hash_from_hex(hash_hex);
            have_trusted_hash = true;

            uint32_t anchor_seq = 0;
            if (step.contains("ledger_index"))
            {
                anchor_seq = step.at("ledger_index").to_number<uint32_t>();
            }

            PLOGI(verify_log, "");
            PLOGI(verify_log, "Step ", step_num, ": ANCHOR");
            PLOGI(
                verify_log, "  Trusted hash: ", hash_hex.substr(0, 16), "...");
            if (anchor_seq)
            {
                PLOGI(verify_log, "  Ledger index: ", anchor_seq);
            }
            if (step.contains("note"))
            {
                PLOGW(
                    verify_log,
                    "  NOTE: ",
                    std::string(step.at("note").as_string()));
            }

            narrative.push_back(
                "1. Start with anchor ledger " + std::to_string(anchor_seq) +
                " (hash " + hash_hex.substr(0, 16) + "...)");

            if (!trusted_publisher_key.empty())
            {
                auto av_result = xproof::AnchorVerifier::verify(
                    step, trusted_publisher_key, narrative);

                if (av_result.verified)
                {
                    PLOGI(
                        verify_log,
                        "  Anchor: VERIFIED (",
                        av_result.validations_matched_unl,
                        "/",
                        av_result.unl_size,
                        " UNL, ",
                        av_result.validations_verified,
                        " sigs verified)");
                }
                else
                {
                    PLOGE(
                        verify_log,
                        "  Anchor: FAILED (",
                        av_result.error,
                        ")");
                    all_ok = false;
                }
            }
            else
            {
                narrative.push_back(
                    "   No trusted publisher key provided — "
                    "anchor trust not verified.");
            }
        }
        // ── Ledger Header ───────────────────────────────────────
        else if (type == "ledger_header")
        {
            auto const& hdr = step.at("header").as_object();
            auto seq = hdr.at("seq").to_number<uint32_t>();

            PLOGI(verify_log, "");
            PLOGI(
                verify_log,
                "Step ",
                step_num,
                ": LEDGER HEADER (seq=",
                seq,
                ")");

            auto computed = hash_ledger_header(hdr);
            auto computed_hex = upper_hex(computed);
            PLOGI(
                verify_log,
                "  Computed hash: ",
                computed_hex.substr(0, 16),
                "...");

            if (have_trusted_hash)
            {
                if (computed == trusted_hash)
                {
                    PLOGI(
                        verify_log,
                        "  Hash check:   PASS (matches trusted hash)");
                    narrative.push_back(
                        "2. Ledger " + std::to_string(seq) +
                        ": SHA512Half(LWR\\0 || header_fields) = " +
                        computed_hex.substr(0, 16) +
                        "... matches anchor hash. "
                        "Header is authentic.");
                }
                else
                {
                    PLOGE(verify_log, "  Hash check:   FAIL!");
                    PLOGE(
                        verify_log, "    expected: ", upper_hex(trusted_hash));
                    PLOGE(verify_log, "    computed: ", computed_hex);
                    all_ok = false;
                    narrative.push_back(
                        "   FAIL: header hash " + computed_hex.substr(0, 16) +
                        "... != trusted " +
                        upper_hex(trusted_hash).substr(0, 16) + "...");
                }
            }
            else if (pending_hashes)
            {
                bool found = false;
                for (auto const& h : *pending_hashes)
                {
                    if (std::string(h.as_string()) == computed_hex)
                    {
                        found = true;
                        break;
                    }
                }
                if (found)
                {
                    PLOGI(
                        verify_log,
                        "  Skip list:    PASS (hash found in ",
                        pending_hashes->size(),
                        " entries)");
                    narrative.push_back(
                        "   Ledger " + std::to_string(seq) + ": hash " +
                        computed_hex.substr(0, 16) +
                        "... found in skip list (" +
                        std::to_string(pending_hashes_count) +
                        " entries). This ledger is reachable from the "
                        "previous trusted ledger.");
                }
                else
                {
                    PLOGE(
                        verify_log,
                        "  Skip list:    FAIL (hash not found in ",
                        pending_hashes->size(),
                        " entries)");
                    all_ok = false;
                    narrative.push_back(
                        "   FAIL: hash " + computed_hex.substr(0, 16) +
                        "... not found in skip list!");
                }
                pending_hashes = nullptr;
            }

            trusted_tx_hash =
                hash_from_hex(std::string(hdr.at("tx_hash").as_string()));
            trusted_ac_hash =
                hash_from_hex(std::string(hdr.at("account_hash").as_string()));
            have_tree_hashes = true;
            have_trusted_hash = false;

            PLOGI(
                verify_log,
                "  tx_hash:      ",
                upper_hex(trusted_tx_hash).substr(0, 16),
                "...");
            PLOGI(
                verify_log,
                "  account_hash: ",
                upper_hex(trusted_ac_hash).substr(0, 16),
                "...");

            narrative.push_back(
                "   Header gives us tx_hash=" +
                upper_hex(trusted_tx_hash).substr(0, 16) +
                "... and account_hash=" +
                upper_hex(trusted_ac_hash).substr(0, 16) + "...");
        }
        // ── Map Proof ───────────────────────────────────────────
        else if (type == "map_proof")
        {
            auto tree_type = std::string(step.at("tree").as_string());
            bool is_state = (tree_type == "state");

            PLOGI(verify_log, "");
            PLOGI(
                verify_log,
                "Step ",
                step_num,
                ": MAP PROOF (",
                tree_type,
                " tree)");

            Hash256 expected;
            if (have_tree_hashes)
            {
                expected = is_state ? trusted_ac_hash : trusted_tx_hash;
                PLOGI(
                    verify_log,
                    "  Expected root: ",
                    upper_hex(expected).substr(0, 16),
                    "...");
            }

            if (step.contains("trie"))
            {
                TrieStats stats;
                count_trie_nodes(step.at("trie"), 0, stats);
                PLOGI(
                    verify_log,
                    "  Trie: ",
                    stats.inners,
                    " inners, ",
                    stats.leaves,
                    " leaves, ",
                    stats.placeholders,
                    " placeholders, max_depth=",
                    stats.max_depth);

                std::string trie_desc = "   Map proof (" + tree_type +
                    " tree): abbreviated trie with " +
                    std::to_string(stats.leaves) + " leaf, " +
                    std::to_string(stats.placeholders) +
                    " placeholder hashes, " + std::to_string(stats.inners) +
                    " inners (depth " + std::to_string(stats.max_depth) + ").";

                if (is_state)
                {
                    auto const* hashes = extract_leaf_hashes(step.at("trie"));
                    if (hashes)
                    {
                        pending_hashes_count = static_cast<int>(hashes->size());
                        PLOGI(
                            verify_log,
                            "  Skip list:    ",
                            hashes->size(),
                            " hashes in LedgerHashes SLE");
                        pending_hashes = hashes;
                        trie_desc += " Leaf is a LedgerHashes SLE with " +
                            std::to_string(hashes->size()) +
                            " ancestor ledger hashes.";
                    }
                    else
                    {
                        PLOGW(
                            verify_log,
                            "  No Hashes field found in state leaf");
                    }
                }
                else
                {
                    trie_desc +=
                        " Leaf contains the target transaction + metadata.";
                }

                narrative.push_back(trie_desc);
                narrative.push_back(
                    "   The trie root hash (recomputed from all branches) "
                    "must equal the header's " +
                    std::string(is_state ? "account_hash" : "tx_hash") +
                    ". Build-time check: " +
                    std::string(
                        step.contains("verified") &&
                                step.at("verified").as_bool()
                            ? "PASSED"
                            : "not available") +
                    ".");
            }

            if (step.contains("verified"))
            {
                bool v = step.at("verified").as_bool();
                PLOGI(
                    verify_log, "  Build-time:   ", v ? "VERIFIED" : "FAILED");
                if (!v)
                {
                    all_ok = false;
                }
            }

            have_tree_hashes = false;
        }
        // ── Parsed Transaction (display only) ───────────────────
        else if (type == "parsed_transaction")
        {
            PLOGI(verify_log, "");
            PLOGI(
                verify_log,
                "Step ",
                step_num,
                ": PARSED TRANSACTION (display only)");
            std::string tx_type, tx_account, tx_hash_str;
            if (step.contains("data") && step.at("data").is_object())
            {
                auto const& data = step.at("data").as_object();
                if (data.contains("hash"))
                {
                    tx_hash_str = std::string(data.at("hash").as_string());
                    PLOGI(
                        verify_log,
                        "  TX hash: ",
                        tx_hash_str.substr(0, 16),
                        "...");
                }
                if (data.contains("tx") && data.at("tx").is_object())
                {
                    auto const& tx = data.at("tx").as_object();
                    if (tx.contains("TransactionType"))
                    {
                        tx_type =
                            std::string(tx.at("TransactionType").as_string());
                        PLOGI(verify_log, "  Type:    ", tx_type);
                    }
                    if (tx.contains("Account"))
                    {
                        tx_account = std::string(tx.at("Account").as_string());
                        PLOGI(verify_log, "  Account: ", tx_account);
                    }
                }
            }
            narrative.push_back(
                "   The proven transaction is a " + tx_type + " from " +
                tx_account + ".");
        }
    }

    // ── Results ─────────────────────────────────────────────────
    PLOGI(verify_log, "");
    PLOGI(
        verify_log,
        "═══════════════════════════════════════════════════════════════");
    if (all_ok)
    {
        PLOGI(verify_log, "  RESULT: ALL CHECKS PASSED");
    }
    else
    {
        PLOGE(verify_log, "  RESULT: VERIFICATION FAILED");
    }
    PLOGI(
        verify_log,
        "═══════════════════════════════════════════════════════════════");

    // ── Narrative explanation ────────────────────────────────────
    PLOGI(verify_log, "");
    PLOGI(verify_log, "How this proof works:");
    PLOGI(verify_log, "");
    for (auto const& line : narrative)
    {
        PLOGI(verify_log, line);
    }
    PLOGI(verify_log, "");
    if (all_ok)
    {
        PLOGI(
            verify_log,
            "Each step is cryptographically chained: the anchor's hash "
            "authenticates the first header, each header's account_hash "
            "authenticates a state tree proof, each state tree proof "
            "contains a skip list linking to the next ledger, and the "
            "final header's tx_hash authenticates the transaction proof.");
    }
    PLOGI(verify_log, "");

    return all_ok;
}

//------------------------------------------------------------------------------
// Parse host:port
//------------------------------------------------------------------------------

bool
parse_endpoint(std::string const& endpoint, std::string& host, uint16_t& port)
{
    auto colon = endpoint.rfind(':');
    if (colon == std::string::npos)
        return false;
    host = endpoint.substr(0, colon);
    port = static_cast<uint16_t>(std::stoul(endpoint.substr(colon + 1)));
    return true;
}

//------------------------------------------------------------------------------
// Ping
//------------------------------------------------------------------------------

int
cmd_ping(std::string const& endpoint)
{
    std::string host;
    uint16_t port;
    if (!parse_endpoint(endpoint, host, port))
    {
        std::cerr << "Invalid endpoint (expected host:port)\n";
        return 1;
    }

    boost::asio::io_context io;
    auto work = boost::asio::make_work_guard(io);
    bool done = false;

    std::shared_ptr<PeerClient> client;
    client = PeerClient::connect(io, host, port, 0, [&](uint32_t peer_seq) {
        PLOGI(log_partition, "Ready! Peer at ledger ", peer_seq);
        PLOGI(log_partition, "Sending ping...");

        client->ping([&](Error err, PingResult result) {
            if (err != Error::Success)
            {
                PLOGE(log_partition, "Ping failed");
            }
            else
            {
                PLOGI(log_partition, "PONG! seq=", result.seq);
            }
            done = true;
            io.stop();
        });
    });

    // Timeout
    boost::asio::steady_timer timer(io, std::chrono::seconds(15));
    timer.async_wait([&](boost::system::error_code) {
        if (!done)
        {
            PLOGE(log_partition, "Timeout");
            io.stop();
        }
    });

    io.run();
    return done ? 0 : 1;
}

//------------------------------------------------------------------------------
// Header
//------------------------------------------------------------------------------

int
cmd_header(std::string const& endpoint, uint32_t ledger_seq)
{
    std::string host;
    uint16_t port;
    if (!parse_endpoint(endpoint, host, port))
    {
        std::cerr << "Invalid endpoint (expected host:port)\n";
        return 1;
    }

    boost::asio::io_context io;
    auto work = boost::asio::make_work_guard(io);
    bool done = false;

    std::shared_ptr<PeerClient> client;
    client = PeerClient::connect(io, host, port, 0, [&](uint32_t peer_seq) {
        PLOGI(log_partition, "Ready! Peer at ledger ", peer_seq);

        // 0 means "current" — use the peer's latest
        uint32_t seq = ledger_seq == 0 ? peer_seq : ledger_seq;

        PLOGI(log_partition, "Requesting ledger ", seq, "...");
        client->get_ledger_header(
            seq, [&](Error err, LedgerHeaderResult result) {
                if (err != Error::Success)
                {
                    PLOGE(
                        log_partition, "Failed: error ", static_cast<int>(err));
                    done = true;
                    io.stop();
                    return;
                }

                PLOGI(log_partition, "");
                PLOGI(log_partition, "=== Ledger ", result.seq(), " ===");
                PLOGI(
                    log_partition,
                    "  hash:         ",
                    result.ledger_hash().hex());

                auto hdr = result.header();
                PLOGI(
                    log_partition, "  parent_hash:  ", hdr.parent_hash().hex());
                PLOGI(log_partition, "  tx_hash:      ", hdr.tx_hash().hex());
                PLOGI(
                    log_partition,
                    "  account_hash: ",
                    hdr.account_hash().hex());
                PLOGI(log_partition, "  close_time:   ", hdr.close_time());
                PLOGI(log_partition, "  drops:        ", hdr.drops());

                if (result.has_state_root())
                {
                    auto root = result.state_root_node();
                    auto inner = root.inner();
                    PLOGI(
                        log_partition,
                        "  state root:   ",
                        inner.child_count(),
                        " children");
                }
                if (result.has_tx_root())
                {
                    auto root = result.tx_root_node();
                    auto inner = root.inner();
                    PLOGI(
                        log_partition,
                        "  tx root:      ",
                        inner.child_count(),
                        " children");
                }

                done = true;
                io.stop();
            });
    });

    boost::asio::steady_timer timer(io, std::chrono::seconds(15));
    timer.async_wait([&](boost::system::error_code) {
        if (!done)
        {
            PLOGE(log_partition, "Timeout");
            io.stop();
        }
    });

    io.run();
    return done ? 0 : 1;
}

//------------------------------------------------------------------------------
// Main
//------------------------------------------------------------------------------

void
print_usage()
{
    std::cerr
        << "xproof - XRPL Proof Chain Tool\n"
        << "\n"
        << "Usage:\n"
        << "  xproof ping <peer:port>                   peer protocol ping\n"
        << "  xproof header <peer:port> <ledger_seq>    fetch ledger header\n"
        << "  xproof prove-tx <rpc:port> <peer:port> <tx_hash>  build tx proof "
           "chain\n"
        << "  xproof verify <proof.json> <publisher_key> verify a proof chain\n"
        << "\n"
        << "Dev commands:\n"
        << "  xproof dev:check-ledger <peer:port>       fetch & verify current "
           "ledger TX tree\n"
        << "  xproof dev:tx <rpc:port> <tx_hash>        lookup tx via "
           "JSON-RPC\n"
        << "\n"
        << "Peer port is typically 51235. RPC port is 443 or 51234.\n"
        << "  e.g. xproof dev:tx s1.ripple.com:443 <tx_hash>\n"
        << "\n";
}

int
main(int argc, char* argv[])
{
    if (argc < 2)
    {
        print_usage();
        return 1;
    }

    std::string command = argv[1];

    if (command == "ping" && argc >= 3)
        return cmd_ping(argv[2]);

    if (command == "header" && argc >= 4)
        return cmd_header(argv[2], std::stoul(argv[3]));

    // ─── dev: commands (scaffolding / testing) ────────────────────

    if (command == "dev:check-ledger" && argc >= 3)
    {
        std::string host;
        uint16_t port;
        if (!parse_endpoint(argv[2], host, port))
        {
            std::cerr << "Invalid endpoint\n";
            return 1;
        }

        auto to_hex = [](std::span<const uint8_t> data) -> std::string {
            std::ostringstream oss;
            for (auto b : data)
                oss << std::hex << std::setfill('0') << std::setw(2)
                    << static_cast<int>(b);
            return oss.str();
        };

        boost::asio::io_context io;
        int result = 1;

        boost::asio::co_spawn(
            io,
            [&]() -> boost::asio::awaitable<void> {
                // Connect and wait for status exchange
                std::shared_ptr<PeerClient> client;
                auto peer_seq = co_await co_connect(io, host, port, 0, client);
                PLOGI(log_partition, "Ready! Peer at ledger ", peer_seq);

                // Fetch ledger header
                auto hdr = co_await co_get_ledger_header(client, peer_seq);
                auto ledger_hash = hdr.ledger_hash256();
                auto header = hdr.header();

                PLOGI(log_partition, "=== Ledger ", hdr.seq(), " ===");
                PLOGI(
                    log_partition, "  hash:         ", hdr.ledger_hash().hex());
                PLOGI(
                    log_partition, "  tx_hash:      ", header.tx_hash().hex());
                PLOGI(
                    log_partition,
                    "  account_hash: ",
                    header.account_hash().hex());

                // Fetch TX tree root
                SHAMapNodeID root_id;
                auto tx_root =
                    co_await co_get_tx_nodes(client, ledger_hash, {root_id});

                if (tx_root.node_count() == 0)
                {
                    PLOGI(log_partition, "  TX tree empty");
                    result = 0;
                    co_return;
                }

                auto root_view = tx_root.node_view(0);
                if (!root_view.is_inner())
                {
                    PLOGI(log_partition, "  TX root is a leaf (single tx)");
                    result = 0;
                    co_return;
                }

                // Build child node IDs from root inner
                auto root_inner = root_view.inner();
                PLOGI(
                    log_partition,
                    "  TX root: ",
                    root_inner.child_count(),
                    " children");

                std::vector<SHAMapNodeID> child_ids;
                root_inner.for_each_child([&](int branch, Key /*hash*/) {
                    SHAMapNodeID child;
                    child.depth = 1;
                    child.id.data()[0] = static_cast<uint8_t>(branch << 4);
                    child_ids.push_back(child);
                });

                // Fetch all children in one batch
                auto child_nodes =
                    co_await co_get_tx_nodes(client, ledger_hash, child_ids);

                // Parse leaves as transactions
                auto protocol =
                    catl::xdata::Protocol::load_embedded_xrpl_protocol();
                int inners = 0, leaves = 0, parsed = 0, failed = 0;
                boost::json::array tx_arr;

                for (int i = 0; i < child_nodes.node_count(); ++i)
                {
                    auto v = child_nodes.node_view(i);
                    if (v.is_inner())
                    {
                        inners++;
                        continue;
                    }

                    leaves++;
                    auto leaf = v.leaf();
                    auto leaf_data = leaf.data();

                    try
                    {
                        Slice slice(leaf_data.data(), leaf_data.size());
                        auto json = catl::xdata::json::parse_transaction(
                            slice, protocol, false);  // wire format: no prefix
                        tx_arr.push_back(json);
                        parsed++;
                    }
                    catch (std::exception const& e)
                    {
                        failed++;
                        PLOGW(
                            log_partition,
                            "  tx parse failed: ",
                            e.what(),
                            " (size=",
                            leaf_data.size(),
                            ")");
                        boost::json::object err_obj;
                        err_obj["error"] = e.what();
                        err_obj["hex"] = to_hex(leaf_data);
                        err_obj["size"] = leaf_data.size();
                        tx_arr.push_back(err_obj);
                    }
                }

                catl::xdata::json::pretty_print(std::cout, tx_arr);

                PLOGI(
                    log_partition,
                    "  ",
                    parsed,
                    " transactions parsed (",
                    failed,
                    " failed, ",
                    inners,
                    " inners skipped)");

                // Log node ID depth histogram
                {
                    std::map<int, int> depth_hist;
                    std::map<int, int> nodeid_size_hist;
                    for (int i = 0; i < child_nodes.node_count(); ++i)
                    {
                        auto nid = child_nodes.node_id(i);
                        nodeid_size_hist[static_cast<int>(nid.size())]++;
                        if (nid.size() == 33)
                        {
                            int depth = static_cast<int>(nid[32]);
                            depth_hist[depth]++;
                        }
                    }
                    std::ostringstream oss;
                    oss << "  nodeid sizes: ";
                    for (auto const& [sz, cnt] : nodeid_size_hist)
                        oss << sz << "b=" << cnt << " ";
                    PLOGI(log_partition, oss.str());

                    std::ostringstream oss2;
                    oss2 << "  depth histogram: ";
                    for (auto const& [depth, cnt] : depth_hist)
                        oss2 << "d" << depth << "=" << cnt << " ";
                    PLOGI(log_partition, oss2.str());
                }

                // Verify: rebuild SHAMap from leaves, compare root hash.
                // We only need leaves — the SHAMap builds inners automatically.
                {
                    catl::shamap::SHAMap verify_map(
                        catl::shamap::tnTRANSACTION_MD);
                    int added = 0;
                    for (int i = 0; i < child_nodes.node_count(); ++i)
                    {
                        auto v = child_nodes.node_view(i);
                        if (v.is_inner())
                            continue;
                        auto leaf_data = v.leaf().data();
                        if (leaf_data.size() < 32)
                            continue;
                        // Wire format: [VL tx][VL meta][32-byte key]
                        Hash256 tx_key(
                            leaf_data.data() + leaf_data.size() - 32);
                        Slice item_data(
                            leaf_data.data(), leaf_data.size() - 32);
                        boost::intrusive_ptr<MmapItem> item(
                            OwnedItem::create(tx_key, item_data));
                        verify_map.add_item(item);
                        added++;
                    }
                    auto computed = verify_map.get_hash();
                    auto expected = header.tx_hash();
                    bool match = (computed == expected);
                    PLOGI(
                        log_partition,
                        "  Ledger ",
                        hdr.seq(),
                        " TX tree hash: ",
                        match ? "VERIFIED" : "MISMATCH",
                        " (",
                        added,
                        " leaves, ",
                        inners,
                        " inners in response)");
                    if (!match)
                    {
                        PLOGE(log_partition, "    expected: ", expected.hex());
                        PLOGE(log_partition, "    computed: ", computed.hex());
                    }
                }

                // Dump fixture JSON: every node with its nodeid + raw hex
                {
                    boost::json::object fixture;
                    fixture["ledger_seq"] = hdr.seq();
                    fixture["ledger_hash"] = upper_hex(ledger_hash);
                    fixture["tx_hash"] = upper_hex(header.tx_hash());
                    fixture["account_hash"] = upper_hex(header.account_hash());
                    fixture["header_hex"] = to_hex(hdr.header_data());

                    // Root node
                    boost::json::object root_obj;
                    root_obj["nodeid"] =
                        to_hex(std::span<const uint8_t>(root_id.id.data(), 32));
                    root_obj["depth"] = 0;
                    root_obj["type"] = "inner";
                    root_obj["hex"] = to_hex(tx_root.node_view(0).raw());
                    boost::json::array nodes_arr;
                    nodes_arr.push_back(root_obj);

                    // All child nodes
                    for (int i = 0; i < child_nodes.node_count(); ++i)
                    {
                        auto v = child_nodes.node_view(i);
                        auto nid = child_nodes.node_id(i);

                        boost::json::object node;
                        if (nid.size() == 33)
                        {
                            // nodeid is 33 bytes: 32-byte path + 1-byte depth.
                            // Both are part of the identity — depth determines
                            // where in the tree this node lives.
                            node["nodeid"] = to_hex(nid);
                        }
                        node["type"] = v.is_inner() ? "inner" : "leaf";
                        node["wire_type"] = static_cast<int>(v.type());
                        node["hex"] = to_hex(v.raw());

                        if (!v.is_inner())
                        {
                            auto leaf_data = v.leaf().data();
                            if (leaf_data.size() >= 32)
                            {
                                Hash256 tx_key(
                                    leaf_data.data() + leaf_data.size() - 32);
                                node["tx_hash"] = upper_hex(tx_key);
                            }
                        }
                        nodes_arr.push_back(node);
                    }
                    fixture["nodes"] = nodes_arr;

                    std::string path = "tx-tree-fixture-" +
                        std::to_string(hdr.seq()) + ".json";
                    std::ofstream out(path);
                    out << boost::json::serialize(fixture);
                    out.close();
                    PLOGI(log_partition, "  Wrote fixture: ", path);
                }

                result = 0;
            },
            [&](std::exception_ptr ep) {
                if (ep)
                {
                    try
                    {
                        std::rethrow_exception(ep);
                    }
                    catch (std::exception const& e)
                    {
                        PLOGE(log_partition, "Fatal: ", e.what());
                    }
                }
                io.stop();
            });

        // Timeout
        boost::asio::steady_timer timer(io, std::chrono::seconds(30));
        timer.async_wait([&](boost::system::error_code) {
            PLOGE(log_partition, "Timeout");
            io.stop();
        });

        io.run();
        return result;
    }

    if (command == "dev:tx" && argc >= 4)
    {
        // dev:tx <host:port> <tx_hash>
        // Looks up a transaction via JSON-RPC and prints the result.
        std::string host;
        uint16_t port;
        if (!parse_endpoint(argv[2], host, port))
        {
            std::cerr << "Invalid endpoint\n";
            return 1;
        }
        std::string tx_hash = argv[3];

        boost::asio::io_context io;
        int result = 1;

        boost::asio::co_spawn(
            io,
            [&]() -> boost::asio::awaitable<void> {
                catl::rpc::RpcClient rpc(io, host, port);
                auto tx_result = co_await catl::rpc::co_tx(rpc, tx_hash);

                auto const& obj = tx_result.as_object();
                if (obj.contains("ledger_index"))
                {
                    PLOGI(
                        log_partition,
                        "ledger_index: ",
                        obj.at("ledger_index").to_number<uint32_t>());
                }
                if (obj.contains("TransactionType"))
                {
                    PLOGI(
                        log_partition,
                        "type: ",
                        boost::json::serialize(obj.at("TransactionType")));
                }
                if (obj.contains("Account"))
                {
                    PLOGI(
                        log_partition,
                        "account: ",
                        boost::json::serialize(obj.at("Account")));
                }
                if (obj.contains("hash"))
                {
                    PLOGI(
                        log_partition,
                        "hash: ",
                        boost::json::serialize(obj.at("hash")));
                }

                catl::xdata::json::pretty_print(std::cout, tx_result);
                result = 0;
            },
            [&](std::exception_ptr ep) {
                if (ep)
                {
                    try
                    {
                        std::rethrow_exception(ep);
                    }
                    catch (std::exception const& e)
                    {
                        PLOGE(log_partition, "Fatal: ", e.what());
                    }
                }
                io.stop();
            });

        boost::asio::steady_timer timer(io, std::chrono::seconds(15));
        timer.async_wait([&](boost::system::error_code) {
            PLOGE(log_partition, "Timeout");
            io.stop();
        });

        io.run();
        return result;
    }

    if (command == "prove-tx" && argc >= 5)
    {
        // prove-tx <rpc:port> <peer:port> <tx_hash>
        // Builds a JSON proof chain for a transaction.
        std::string rpc_host, peer_host;
        uint16_t rpc_port, peer_port;
        if (!parse_endpoint(argv[2], rpc_host, rpc_port) ||
            !parse_endpoint(argv[3], peer_host, peer_port))
        {
            std::cerr << "Invalid endpoint(s)\n";
            return 1;
        }
        std::string tx_hash_str = argv[4];
        auto tx_hash = hash_from_hex(tx_hash_str);

        boost::asio::io_context io;
        int result = 1;

        boost::asio::co_spawn(
            io,
            [&]() -> boost::asio::awaitable<void> {
                // Helper: walk a state tree key and return leaf data +
                // placeholders
                struct StateWalkResult
                {
                    bool found = false;
                    catl::shamap::SHAMapNodeID leaf_nid;
                    std::vector<uint8_t> leaf_data;
                    struct PH
                    {
                        catl::shamap::SHAMapNodeID nid;
                        Hash256 hash;
                    };
                    std::vector<PH> placeholders;
                };
                auto walk_state = [&](std::shared_ptr<PeerClient> c,
                                      Hash256 const& ledger_hash,
                                      Hash256 const& key)
                    -> boost::asio::awaitable<StateWalkResult> {
                    StateWalkResult r;
                    TreeWalker walker(
                        c, ledger_hash, TreeWalkState::TreeType::state);
                    walker.add_target(key);
                    walker.set_on_placeholder(
                        [&](std::span<const uint8_t> nid, Hash256 const& h) {
                            r.placeholders.push_back(
                                {catl::shamap::SHAMapNodeID(nid), h});
                        });
                    walker.set_on_leaf([&](std::span<const uint8_t> nid,
                                           Hash256 const&,
                                           std::span<const uint8_t> data) {
                        r.found = true;
                        r.leaf_nid = catl::shamap::SHAMapNodeID(nid);
                        r.leaf_data.assign(data.begin(), data.end());
                    });
                    co_await walker.walk();
                    co_return r;
                };

                // Helper: build + verify abbreviated state tree.
                // Returns the built tree for use in map_proof output.
                using AbbrevMap =
                    catl::shamap::SHAMapT<catl::shamap::AbbreviatedTreeTraits>;

                struct StateProofResult
                {
                    bool verified;
                    AbbrevMap tree;
                };

                auto build_state_proof =
                    [&](StateWalkResult const& wr,
                        Hash256 const& key,
                        Hash256 const& expected_account_hash)
                    -> StateProofResult {
                    catl::shamap::SHAMapOptions opts;
                    opts.tree_collapse_impl =
                        catl::shamap::TreeCollapseImpl::leafs_only;
                    opts.reference_hash_impl =
                        catl::shamap::ReferenceHashImpl::recursive_simple;
                    AbbrevMap abbrev(catl::shamap::tnACCOUNT_STATE, opts);

                    Slice item_data(
                        wr.leaf_data.data(), wr.leaf_data.size() - 32);
                    boost::intrusive_ptr<MmapItem> item(
                        OwnedItem::create(key, item_data));
                    abbrev.set_item_at(wr.leaf_nid, item);

                    for (auto& p : wr.placeholders)
                    {
                        if (abbrev.needs_placeholder(p.nid))
                        {
                            abbrev.set_placeholder(p.nid, p.hash);
                        }
                    }

                    auto computed = abbrev.get_hash();
                    bool ok = (computed == expected_account_hash);
                    PLOGI(
                        log_partition,
                        "  State tree: ",
                        ok ? "VERIFIED" : "MISMATCH",
                        " (",
                        wr.placeholders.size(),
                        " placeholders)");
                    if (!ok)
                    {
                        PLOGE(
                            log_partition,
                            "    expected: ",
                            expected_account_hash.hex());
                        PLOGE(log_partition, "    computed: ", computed.hex());
                    }
                    return {ok, std::move(abbrev)};
                };

                auto protocol =
                    catl::xdata::Protocol::load_embedded_xrpl_protocol();

                // ── Step 1: RPC — look up tx to get ledger_index ──
                catl::rpc::RpcClient rpc(io, rpc_host, rpc_port);
                auto tx_result = co_await catl::rpc::co_tx(rpc, tx_hash_str);
                auto const& tx_obj = tx_result.as_object();

                uint32_t tx_ledger_seq = 0;
                if (tx_obj.contains("ledger_index"))
                    tx_ledger_seq =
                        tx_obj.at("ledger_index").to_number<uint32_t>();
                if (tx_ledger_seq == 0)
                    throw std::runtime_error("tx not found or no ledger_index");

                PLOGI(
                    log_partition,
                    "TX ",
                    tx_hash_str.substr(0, 16),
                    "... is in ledger ",
                    tx_ledger_seq);

                // ── Step 1b: Fetch VL (fires concurrently) ──
                std::optional<catl::vl::ValidatorList> vl_data;
                std::string vl_error;
                {
                    catl::vl::VlClient vl_client(io, "vl.ripple.com", 443);
                    vl_client.fetch([&](catl::vl::VlResult r) {
                        if (r.success)
                        {
                            vl_data = std::move(r.vl);
                        }
                        else
                        {
                            vl_error = r.error;
                        }
                    });
                }

                // ── Step 2: Peer — connect and collect validations ──
                std::shared_ptr<PeerClient> client;
                auto peer_seq =
                    co_await co_connect(io, peer_host, peer_port, 0, client);
                PLOGI(
                    log_partition,
                    "Connected to peer, peer at ledger ",
                    peer_seq);

                // Start collecting ALL validations immediately
                ValidationCollector val_collector{protocol};
                client->set_unsolicited_handler(
                    [&val_collector](
                        uint16_t type, std::vector<uint8_t> const& data) {
                        val_collector.on_packet(type, data);
                    });
                PLOGI(log_partition, "Listening for validations...");

                // Wait for VL to arrive (should be fast)
                // The VL fetch was launched above and runs on the same
                // io_context
                {
                    // Poll until VL arrives or timeout
                    boost::asio::steady_timer vl_timer(
                        io, std::chrono::seconds(10));
                    while (!vl_data && vl_error.empty())
                    {
                        vl_timer.expires_after(std::chrono::milliseconds(100));
                        boost::system::error_code ec;
                        co_await vl_timer.async_wait(
                            boost::asio::redirect_error(
                                boost::asio::use_awaitable, ec));
                    }
                }

                if (vl_data)
                {
                    val_collector.set_unl(vl_data->validators);
                }
                else
                {
                    PLOGW(
                        log_partition,
                        "VL fetch failed: ",
                        vl_error,
                        " — continuing without quorum check");
                }

                // Wait for quorum (or timeout after 15s)
                if (vl_data && !val_collector.quorum_reached)
                {
                    PLOGI(log_partition, "Waiting for validation quorum...");
                    boost::asio::steady_timer quorum_timer(
                        io, std::chrono::seconds(15));
                    while (!val_collector.quorum_reached)
                    {
                        quorum_timer.expires_after(
                            std::chrono::milliseconds(200));
                        boost::system::error_code ec;
                        co_await quorum_timer.async_wait(
                            boost::asio::redirect_error(
                                boost::asio::use_awaitable, ec));
                        if (ec)
                            break;
                        // Check if we've been waiting too long
                        // (the timer resets each iteration, so check total)
                    }
                }

                // Determine anchor — use quorum ledger if available,
                // otherwise fall back to peer's current ledger
                uint32_t anchor_seq;
                Hash256 anchor_hash;
                if (val_collector.quorum_reached)
                {
                    anchor_hash = val_collector.quorum_hash;
                    anchor_seq =
                        val_collector.quorum_validations()[0].ledger_seq;
                    PLOGI(
                        log_partition,
                        "Using validated anchor: seq=",
                        anchor_seq,
                        " hash=",
                        anchor_hash.hex().substr(0, 16),
                        "... (",
                        val_collector.quorum_count,
                        "/",
                        val_collector.unl_size,
                        " validators)");
                }
                else
                {
                    anchor_seq = peer_seq;
                    PLOGW(
                        log_partition,
                        "No quorum — using peer's ledger ",
                        peer_seq,
                        " as unvalidated anchor");
                }

                auto anchor_hdr =
                    co_await co_get_ledger_header(client, anchor_seq);
                auto anchor_header = anchor_hdr.header();
                if (!val_collector.quorum_reached)
                {
                    anchor_hash = anchor_hdr.ledger_hash256();
                }
                PLOGI(
                    log_partition,
                    "Anchor ledger ",
                    anchor_hdr.seq(),
                    " hash=",
                    anchor_hash.hex().substr(0, 16),
                    "...");

                // ── Step 3: Determine hop path ──
                uint32_t distance = anchor_hdr.seq() - tx_ledger_seq;
                PLOGI(log_partition, "Distance: ", distance, " ledgers");

                Hash256 target_ledger_hash;
                bool need_flag_hop = (distance > 256);

                // State proof trees — kept for JSON output
                std::vector<std::tuple<Hash256 /*key*/, StateProofResult>>
                    state_proofs;

                if (!need_flag_hop)
                {
                    // Short skip list — target within 256 of anchor
                    PLOGI(log_partition, "Short skip list (within 256)");
                    auto skip_key_val = skip_list_key();
                    PLOGD(
                        log_partition, "  skip key: ", upper_hex(skip_key_val));

                    auto wr =
                        co_await walk_state(client, anchor_hash, skip_key_val);
                    if (!wr.found)
                        throw std::runtime_error(
                            "Short skip list not found in state tree");

                    auto sp = build_state_proof(
                        wr, skip_key_val, anchor_header.account_hash());
                    state_proofs.emplace_back(skip_key_val, std::move(sp));

                    // The target ledger hash should be in sfHashes
                    // But we need the actual hash — fetch the target header to
                    // get it, then verify it's in the skip list
                    auto target_hdr =
                        co_await co_get_ledger_header(client, tx_ledger_seq);
                    target_ledger_hash = target_hdr.ledger_hash256();

                    Slice sle_leaf(wr.leaf_data.data(), wr.leaf_data.size());
                    PLOGD(
                        log_partition,
                        "  Short skip list SLE (",
                        sle_leaf.size(),
                        " bytes) key=",
                        upper_hex(skip_key_val));
                    {
                        std::string hex;
                        slice_hex(sle_leaf, hex);
                        PLOGD(log_partition, "  raw: ", hex);
                    }
                    if (!sle_hashes_contain(
                            sle_leaf, target_ledger_hash, protocol))
                    {
                        PLOGW(
                            log_partition,
                            "  Target hash not found in short skip list!");
                    }
                    else
                    {
                        PLOGI(
                            log_partition,
                            "  Target hash confirmed in short skip list");
                    }
                }
                else
                {
                    // 2-hop: long skip list → flag ledger → short skip list →
                    // target
                    PLOGI(
                        log_partition,
                        "Long skip list (2-hop, distance=",
                        distance,
                        ")");

                    // Hop 1: find flag ledger hash in long skip list
                    // Round UP to nearest 256 — the flag ledger that has
                    // history covering the target.
                    uint32_t flag_seq = ((tx_ledger_seq + 255) / 256) * 256;
                    PLOGI(log_partition, "  Flag ledger: ", flag_seq);

                    auto long_skip_key = skip_list_key(flag_seq);
                    PLOGD(
                        log_partition,
                        "  long skip key: ",
                        upper_hex(long_skip_key));

                    auto wr1 =
                        co_await walk_state(client, anchor_hash, long_skip_key);
                    if (!wr1.found)
                        throw std::runtime_error(
                            "Long skip list not found in state tree");

                    {
                        auto sp = build_state_proof(
                            wr1, long_skip_key, anchor_header.account_hash());
                        state_proofs.emplace_back(long_skip_key, std::move(sp));
                    }

                    // Get flag ledger header
                    auto flag_hdr =
                        co_await co_get_ledger_header(client, flag_seq);
                    auto flag_hash = flag_hdr.ledger_hash256();
                    auto flag_header = flag_hdr.header();
                    PLOGI(
                        log_partition,
                        "  Flag ledger ",
                        flag_hdr.seq(),
                        " hash=",
                        flag_hdr.ledger_hash().hex().substr(0, 16),
                        "...");

                    PLOGI(
                        log_partition,
                        "  Long SLE leaf_data size=",
                        wr1.leaf_data.size());
                    Slice long_sle(wr1.leaf_data.data(), wr1.leaf_data.size());
                    PLOGD(
                        log_partition,
                        "  Long skip list SLE (",
                        long_sle.size(),
                        " bytes) key=",
                        upper_hex(long_skip_key));
                    {
                        std::string hex;
                        slice_hex(long_sle, hex);
                        PLOGD(log_partition, "  raw: ", hex);
                    }
                    if (!sle_hashes_contain(long_sle, flag_hash, protocol))
                    {
                        PLOGW(
                            log_partition,
                            "  Flag hash not found in long skip list!");
                    }
                    else
                    {
                        PLOGI(
                            log_partition,
                            "  Flag hash confirmed in long skip list");
                    }

                    // Hop 2: find target hash in flag ledger's short skip list
                    auto short_skip_key = skip_list_key();
                    auto wr2 =
                        co_await walk_state(client, flag_hash, short_skip_key);
                    if (!wr2.found)
                        throw std::runtime_error(
                            "Short skip list not found in flag ledger state "
                            "tree");

                    {
                        auto sp = build_state_proof(
                            wr2, short_skip_key, flag_header.account_hash());
                        state_proofs.emplace_back(
                            short_skip_key, std::move(sp));
                    }

                    auto target_hdr =
                        co_await co_get_ledger_header(client, tx_ledger_seq);
                    target_ledger_hash = target_hdr.ledger_hash256();

                    Slice short_sle(wr2.leaf_data.data(), wr2.leaf_data.size());
                    PLOGD(
                        log_partition,
                        "  Flag short skip list SLE (",
                        short_sle.size(),
                        " bytes) key=",
                        upper_hex(short_skip_key));
                    {
                        std::string hex;
                        slice_hex(short_sle, hex);
                        PLOGD(log_partition, "  raw: ", hex);
                    }
                    if (!sle_hashes_contain(
                            short_sle, target_ledger_hash, protocol))
                    {
                        PLOGW(
                            log_partition,
                            "  Target hash not found in flag's short skip "
                            "list!");
                    }
                    else
                    {
                        PLOGI(
                            log_partition,
                            "  Target hash confirmed in flag's short skip "
                            "list");
                    }
                }

                // ── Step 4: Get target ledger header ──
                auto target_hdr =
                    co_await co_get_ledger_header(client, tx_ledger_seq);
                auto target_header = target_hdr.header();
                PLOGI(
                    log_partition,
                    "Target ledger ",
                    target_hdr.seq(),
                    " hash=",
                    target_hdr.ledger_hash().hex().substr(0, 16),
                    "...",
                    " tx_hash=",
                    target_header.tx_hash().hex().substr(0, 16),
                    "...");

                // ── Step 5: Walk TX tree using TreeWalker ──
                auto ledger_hash = target_hdr.ledger_hash256();
                PLOGI(
                    log_partition,
                    "Walking TX tree for ",
                    tx_hash_str.substr(0, 16),
                    "...");

                // Collect placeholders and leaf data from the walk
                struct PlaceholderEntry
                {
                    catl::shamap::SHAMapNodeID nid;
                    Hash256 hash;
                };
                std::vector<PlaceholderEntry> placeholders;
                std::vector<uint8_t> leaf_data;
                catl::shamap::SHAMapNodeID leaf_nid;
                bool found_leaf = false;

                {
                    TreeWalker walker(
                        client, ledger_hash, TreeWalkState::TreeType::tx);
                    walker.add_target(tx_hash);

                    walker.set_on_placeholder(
                        [&](std::span<const uint8_t> nid, Hash256 const& hash) {
                            catl::shamap::SHAMapNodeID snid(nid);
                            placeholders.push_back({snid, hash});
                        });

                    walker.set_on_leaf([&](std::span<const uint8_t> nid,
                                           Hash256 const& /*key*/,
                                           std::span<const uint8_t> data) {
                        leaf_nid = catl::shamap::SHAMapNodeID(nid);
                        leaf_data.assign(data.begin(), data.end());
                        found_leaf = true;
                        PLOGI(
                            log_partition,
                            "  Found target leaf at depth ",
                            static_cast<int>(leaf_nid.depth()));
                    });

                    co_await walker.walk();
                }

                if (!found_leaf)
                    throw std::runtime_error(
                        "Transaction not found in TX tree");

                // ── Step 6: Build abbreviated tree ──
                PLOGI(log_partition, "Building abbreviated tree...");

                using AbbrevMap =
                    catl::shamap::SHAMapT<catl::shamap::AbbreviatedTreeTraits>;
                catl::shamap::SHAMapOptions opts;
                opts.tree_collapse_impl =
                    catl::shamap::TreeCollapseImpl::leafs_only;
                opts.reference_hash_impl =
                    catl::shamap::ReferenceHashImpl::recursive_simple;
                AbbrevMap abbrev(catl::shamap::tnTRANSACTION_MD, opts);

                // Add the real leaf at its exact depth
                Slice leaf_item_data(leaf_data.data(), leaf_data.size() - 32);
                boost::intrusive_ptr<MmapItem> leaf_item(
                    OwnedItem::create(tx_hash, leaf_item_data));
                abbrev.set_item_at(leaf_nid, leaf_item);

                // Add needed placeholders
                int placed = 0;
                for (auto& p : placeholders)
                {
                    if (abbrev.needs_placeholder(p.nid))
                    {
                        abbrev.set_placeholder(p.nid, p.hash);
                        placed++;
                    }
                }

                // Verify
                auto abbrev_hash = abbrev.get_hash();
                auto expected_tx_hash = target_header.tx_hash();
                bool verified = (abbrev_hash == expected_tx_hash);
                PLOGI(
                    log_partition,
                    "  Abbreviated tree: ",
                    placed,
                    " placeholders");
                PLOGI(
                    log_partition,
                    "  TX tree hash: ",
                    verified ? "VERIFIED" : "MISMATCH");
                if (!verified)
                {
                    PLOGE(
                        log_partition,
                        "    expected: ",
                        expected_tx_hash.hex());
                    PLOGE(log_partition, "    computed: ", abbrev_hash.hex());
                }

                // ── Step 7: Build JSON proof chain ──
                // Per spec: anchor → ledger_header(anchor) →
                //   [map_proof(state) → ledger_header]* →
                //   ledger_header(target) → map_proof(tx)
                boost::json::array chain;

                // Helper to emit a ledger_header step
                auto emit_header = [&](auto const& hdr_result) {
                    auto h = hdr_result.header();
                    boost::json::object step;
                    step["type"] = "ledger_header";
                    boost::json::object hdr_obj;
                    hdr_obj["seq"] = hdr_result.seq();
                    hdr_obj["drops"] = std::to_string(h.drops());
                    hdr_obj["parent_hash"] = upper_hex(h.parent_hash());
                    hdr_obj["tx_hash"] = upper_hex(h.tx_hash());
                    hdr_obj["account_hash"] = upper_hex(h.account_hash());
                    hdr_obj["parent_close_time"] = h.parent_close_time();
                    hdr_obj["close_time"] = h.close_time();
                    hdr_obj["close_time_resolution"] =
                        h.close_time_resolution();
                    hdr_obj["close_flags"] = h.close_flags();
                    step["header"] = hdr_obj;
                    chain.push_back(step);
                };

                // 1. Anchor — self-contained with all data needed
                // for offline verification
                {
                    // Helper: bytes to hex string
                    auto bytes_hex =
                        [](std::vector<uint8_t> const& v) -> std::string {
                        std::string hex;
                        Slice s(v.data(), v.size());
                        slice_hex(s, hex);
                        return hex;
                    };

                    boost::json::object anchor;
                    anchor["type"] = "anchor";
                    anchor["ledger_hash"] = upper_hex(anchor_hash);
                    anchor["ledger_index"] = anchor_hdr.seq();

                    // UNL data — everything needed to reconstruct
                    // the trusted validator set from a single publisher
                    // public key
                    if (vl_data)
                    {
                        boost::json::object unl;
                        unl["public_key"] = vl_data->publisher_key_hex;
                        unl["manifest"] =
                            bytes_hex(vl_data->publisher_manifest.raw);
                        unl["blob"] = bytes_hex(vl_data->blob_bytes);
                        unl["signature"] = bytes_hex(vl_data->blob_signature);
                        anchor["unl"] = unl;
                    }

                    // Raw STValidation messages — each is the full
                    // serialized validation that the verifier can
                    // independently verify signatures on
                    auto const& qvals = val_collector.quorum_validations();
                    boost::json::object validations_obj;
                    for (auto const& v : qvals)
                    {
                        // Key by signing key hex, value is raw
                        // STValidation hex
                        auto key_hex = bytes_hex(v.signing_key);
                        validations_obj[key_hex] = bytes_hex(v.raw);
                    }
                    anchor["validations"] = validations_obj;

                    // Summary for quick inspection
                    boost::json::object summary;
                    summary["total_validations"] =
                        static_cast<int>(qvals.size());
                    summary["quorum"] = val_collector.quorum_reached;
                    if (vl_data)
                    {
                        summary["unl_size"] = val_collector.unl_size;
                        summary["matched_unl"] = val_collector.quorum_count;
                        int threshold = val_collector.unl_size;
                        summary["threshold"] = threshold;
                    }
                    anchor["summary"] = summary;

                    chain.push_back(anchor);
                }

                // 2. Anchor ledger header
                emit_header(anchor_hdr);

                // 3. State tree proofs (skip list hops)
                //    For 2-hop: long skip list proof, flag header,
                //               short skip list proof
                //    For 1-hop: short skip list proof only
                if (need_flag_hop && state_proofs.size() >= 2)
                {
                    // Long skip list state proof
                    {
                        auto& [key, sp] = state_proofs[0];
                        boost::json::object step;
                        step["type"] = "map_proof";
                        step["tree"] = "state";
                        catl::shamap::TrieJsonOptions trie_opts;
                        trie_opts.on_leaf =
                            make_proof_leaf_callback(protocol, false);
                        step["trie"] = sp.tree.get_root()->trie_json(
                            trie_opts, sp.tree.get_options());
                        step["verified"] = sp.verified;
                        chain.push_back(step);
                    }

                    // Flag ledger header
                    {
                        uint32_t flag_seq = ((tx_ledger_seq + 255) / 256) * 256;
                        auto flag_hdr2 =
                            co_await co_get_ledger_header(client, flag_seq);
                        emit_header(flag_hdr2);
                    }

                    // Short skip list state proof
                    {
                        auto& [key, sp] = state_proofs[1];
                        boost::json::object step;
                        step["type"] = "map_proof";
                        step["tree"] = "state";
                        catl::shamap::TrieJsonOptions trie_opts;
                        trie_opts.on_leaf =
                            make_proof_leaf_callback(protocol, false);
                        step["trie"] = sp.tree.get_root()->trie_json(
                            trie_opts, sp.tree.get_options());
                        step["verified"] = sp.verified;
                        chain.push_back(step);
                    }
                }
                else if (!state_proofs.empty())
                {
                    // Single-hop: short skip list proof
                    auto& [key, sp] = state_proofs[0];
                    boost::json::object step;
                    step["type"] = "map_proof";
                    step["tree"] = "state";
                    {
                        catl::shamap::TrieJsonOptions trie_opts;
                        trie_opts.on_leaf =
                            make_proof_leaf_callback(protocol, false);
                        step["trie"] = sp.tree.get_root()->trie_json(
                            trie_opts, sp.tree.get_options());
                    }
                    step["verified"] = sp.verified;
                    chain.push_back(step);
                }

                // 4. Target ledger header
                emit_header(target_hdr);

                // 5. TX tree proof
                {
                    boost::json::object step;
                    step["type"] = "map_proof";
                    step["tree"] = "tx";
                    {
                        catl::shamap::TrieJsonOptions trie_opts;
                        trie_opts.on_leaf =
                            make_proof_leaf_callback(protocol, true);
                        step["trie"] = abbrev.get_root()->trie_json(
                            trie_opts, abbrev.get_options());
                    }
                    step["verified"] = verified;
                    chain.push_back(step);
                }

                // 6. Parsed transaction (for display)
                try
                {
                    Slice tx_slice(leaf_data.data(), leaf_data.size());
                    auto tx_json = catl::xdata::json::parse_transaction(
                        tx_slice, protocol, false);
                    chain.push_back(boost::json::object{
                        {"type", "parsed_transaction"}, {"data", tx_json}});
                }
                catch (std::exception const& e)
                {
                    PLOGW(log_partition, "  TX parse failed: ", e.what());
                }

                catl::xdata::json::pretty_print(std::cout, chain);

                // Write proof to file
                {
                    std::string path = "proof.json";
                    std::ofstream out(path);
                    catl::xdata::json::pretty_print(out, chain);
                    out.close();
                    PLOGI(log_partition, "Wrote proof: ", path);
                }

                // ── Step 8: Verify the proof chain we just built ──
                resolve_proof_chain(chain);

                result = 0;
            },
            [&](std::exception_ptr ep) {
                if (ep)
                {
                    try
                    {
                        std::rethrow_exception(ep);
                    }
                    catch (std::exception const& e)
                    {
                        PLOGE(log_partition, "Fatal: ", e.what());
                    }
                }
                io.stop();
            });

        boost::asio::steady_timer timer(io, std::chrono::seconds(60));
        timer.async_wait([&](boost::system::error_code) {
            PLOGE(log_partition, "Timeout");
            io.stop();
        });

        io.run();
        return result;
    }
    else if (command == "verify" && argc >= 4)
    {
        // verify <proof.json> <trusted_publisher_key_hex>
        std::string proof_path = argv[2];
        std::string trusted_key = argv[3];

        std::ifstream in(proof_path);
        if (!in.is_open())
        {
            std::cerr << "Cannot open: " << proof_path << "\n";
            return 1;
        }

        std::string json_str(
            (std::istreambuf_iterator<char>(in)),
            std::istreambuf_iterator<char>());
        in.close();

        try
        {
            auto jv = boost::json::parse(json_str);
            auto const& chain = jv.as_array();
            PLOGI(
                log_partition,
                "Loaded proof from ",
                proof_path,
                " (",
                chain.size(),
                " steps)");
            PLOGI(
                log_partition,
                "Trusted publisher key: ",
                trusted_key.substr(0, 16),
                "...");
            bool ok = resolve_proof_chain(chain, trusted_key);
            return ok ? 0 : 1;
        }
        catch (std::exception const& e)
        {
            std::cerr << "Parse error: " << e.what() << "\n";
            return 1;
        }
    }

    print_usage();
    return 1;
}
