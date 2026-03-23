#include "xproof/anchor-verifier.h"
#include "xproof/hex-utils.h"
#include "xproof/proof-chain-json.h"
#include "xproof/proof-chain.h"
#include "xproof/proof-steps.h"
#include "xproof/skip-list.h"

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
using xproof::hash_from_hex;
using xproof::upper_hex;
using xproof::skip_list_key;

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
                    full,
                    protocol,
                    {.includes_prefix = false, .include_blob = true}));
            }
            else
            {
                arr.emplace_back(catl::xdata::json::parse_leaf(
                    full,
                    protocol,
                    {.includes_prefix = false, .include_blob = true}));
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

/// Find the first leaf (array node) in a JSON trie.
static boost::json::value const*
extract_trie_leaf(boost::json::value const& trie)
{
    if (trie.is_array())
    {
        return &trie;
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
            auto result = extract_trie_leaf(kv.value());
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

    // Track chain context
    int header_count = 0;
    int state_proof_count = 0;
    uint32_t anchor_seq = 0;
    uint32_t target_seq = 0;

    // Structured proof steps for narrative rendering
    std::vector<xproof::ProofStep> proof_steps;

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

            xproof::AnchorStep anchor_step;
            anchor_step.ledger_index = anchor_seq;
            anchor_step.ledger_hash = hash_hex.substr(0, 16);

            if (!trusted_publisher_key.empty())
            {
                std::vector<std::string> av_narrative;
                auto av_result = xproof::AnchorVerifier::verify(
                    step, trusted_publisher_key, av_narrative);

                anchor_step.sub_steps = std::move(av_narrative);
                anchor_step.publisher_key = trusted_publisher_key.substr(0, 16);
                anchor_step.publisher_key_check = av_result.verified
                    ? xproof::Check::pass
                    : xproof::Check::fail;
                anchor_step.blob_signature_check = xproof::Check::pass;
                anchor_step.unl_size = av_result.unl_size;
                anchor_step.validations_total = av_result.validations_total;
                anchor_step.validations_verified =
                    av_result.validations_verified;
                anchor_step.validations_matched =
                    av_result.validations_matched_unl;
                anchor_step.quorum_check = av_result.verified
                    ? xproof::Check::pass
                    : xproof::Check::fail;

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
                        verify_log, "  Anchor: FAILED (", av_result.error, ")");
                    all_ok = false;
                }
            }

            proof_steps.push_back(
                {xproof::StepType::anchor, step_num, std::move(anchor_step)});
        }
        // ── Ledger Header ───────────────────────────────────────
        else if (type == "ledger_header")
        {
            auto const& hdr = step.at("header").as_object();
            auto seq = hdr.at("seq").to_number<uint32_t>();
            header_count++;
            target_seq = seq;

            // Determine role of this header in the chain.
            // In a 2-hop chain: anchor → state(long) → flag → state(short) →
            // target → tx In a 1-hop chain: anchor → state(short) → target → tx
            // Flag only exists when there are 2 state proofs.
            std::string role;
            if (header_count == 1)
            {
                role = "anchor";
                anchor_seq = seq;
            }
            else if (pending_hashes && state_proof_count >= 2)
            {
                // After 2nd state proof = target (2-hop)
                role = "target";
            }
            else if (pending_hashes && state_proof_count == 1)
            {
                // After 1st state proof:
                // Could be flag (if more state proofs coming) or target
                // (single-hop). We don't know yet, so check: is seq a multiple
                // of 256? Flag ledgers are always multiples of 256.
                if (seq % 256 == 0 && seq != anchor_seq)
                {
                    role = "flag";
                }
                else
                {
                    role = "target";
                }
            }
            else
            {
                role = "target";
            }

            PLOGI(verify_log, "");
            PLOGI(
                verify_log,
                "Step ",
                step_num,
                ": LEDGER HEADER (seq=",
                seq,
                ", ",
                role,
                ")");

            auto computed = hash_ledger_header(hdr);
            auto computed_hex = upper_hex(computed);
            PLOGI(
                verify_log,
                "  Computed hash: ",
                computed_hex.substr(0, 16),
                "...");

            xproof::HeaderStep hdr_step;
            hdr_step.seq = seq;
            hdr_step.computed_hash = computed_hex.substr(0, 16);
            hdr_step.role = (role == "anchor")
                ? xproof::HeaderRole::anchor
                : (role == "flag" ? xproof::HeaderRole::flag
                                  : xproof::HeaderRole::target);

            if (have_trusted_hash)
            {
                hdr_step.method = xproof::HeaderVerifyMethod::anchor_hash;
                if (computed == trusted_hash)
                {
                    PLOGI(
                        verify_log,
                        "  Hash check:   PASS (matches trusted hash)");
                    hdr_step.hash_check = xproof::Check::pass;
                }
                else
                {
                    PLOGE(verify_log, "  Hash check:   FAIL!");
                    PLOGE(
                        verify_log, "    expected: ", upper_hex(trusted_hash));
                    PLOGE(verify_log, "    computed: ", computed_hex);
                    all_ok = false;
                    hdr_step.hash_check = xproof::Check::fail;
                }
            }
            else if (pending_hashes)
            {
                hdr_step.method = xproof::HeaderVerifyMethod::skip_list;
                hdr_step.skip_type = (role == "flag")
                    ? xproof::SkipListType::flag
                    : xproof::SkipListType::recent;
                hdr_step.skip_list_size =
                    static_cast<int>(pending_hashes->size());

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
                    hdr_step.hash_check = xproof::Check::pass;
                }
                else
                {
                    PLOGE(
                        verify_log,
                        "  Skip list:    FAIL (hash not found in ",
                        pending_hashes->size(),
                        " entries)");
                    all_ok = false;
                    hdr_step.hash_check = xproof::Check::fail;
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

            hdr_step.tx_hash = upper_hex(trusted_tx_hash).substr(0, 16);
            hdr_step.account_hash = upper_hex(trusted_ac_hash).substr(0, 16);

            proof_steps.push_back(
                {xproof::StepType::ledger_header,
                 step_num,
                 std::move(hdr_step)});
        }
        // ── Map Proof ───────────────────────────────────────────
        else if (type == "map_proof")
        {
            auto tree_type = std::string(step.at("tree").as_string());
            bool is_state = (tree_type == "state");
            if (is_state)
            {
                state_proof_count++;
            }

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

            xproof::MapProofStep map_step;
            map_step.is_state = is_state;

            if (have_tree_hashes)
            {
                map_step.expected_root = upper_hex(expected).substr(0, 16);
            }

            if (step.contains("trie"))
            {
                TrieStats stats;
                count_trie_nodes(step.at("trie"), 0, stats);
                map_step.inners = stats.inners;
                map_step.leaves = stats.leaves;
                map_step.placeholders = stats.placeholders;
                map_step.max_depth = stats.max_depth;

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

                if (is_state)
                {
                    auto const* hashes = extract_leaf_hashes(step.at("trie"));
                    if (hashes)
                    {
                        pending_hashes_count = static_cast<int>(hashes->size());
                        pending_hashes = hashes;
                        map_step.hashes_count = pending_hashes_count;

                        bool is_flag_skip =
                            (state_proof_count == 1 && hashes->size() < 256);
                        map_step.skip_type = is_flag_skip
                            ? xproof::SkipListType::flag
                            : xproof::SkipListType::recent;

                        PLOGI(
                            verify_log,
                            "  Skip list:    ",
                            hashes->size(),
                            " hashes in LedgerHashes SLE");
                    }
                }
                else
                {
                    // TX tree — extract transaction details
                    auto const* leaf = extract_trie_leaf(step.at("trie"));
                    if (leaf && leaf->is_array() &&
                        leaf->as_array().size() >= 2)
                    {
                        auto const& ld = leaf->as_array()[1];
                        if (ld.is_object())
                        {
                            auto const& lobj = ld.as_object();
                            if (lobj.contains("tx") &&
                                lobj.at("tx").is_object())
                            {
                                auto const& tx = lobj.at("tx").as_object();
                                if (tx.contains("TransactionType"))
                                {
                                    map_step.tx_type = std::string(
                                        tx.at("TransactionType").as_string());
                                    PLOGI(
                                        verify_log,
                                        "  TX type:      ",
                                        map_step.tx_type);
                                }
                                if (tx.contains("Account"))
                                {
                                    map_step.tx_account = std::string(
                                        tx.at("Account").as_string());
                                    PLOGI(
                                        verify_log,
                                        "  TX account:   ",
                                        map_step.tx_account);
                                }
                            }
                        }
                    }
                }
            }

            // Reconstruct abbreviated tree from trie JSON and
            // verify root hash (placeholder structure only —
            // leaf hash verification requires a serializer)
            if (step.contains("trie") && have_tree_hashes)
            {
                try
                {
                    auto node_type = is_state ? catl::shamap::tnACCOUNT_STATE
                                              : catl::shamap::tnTRANSACTION_MD;

                    using AbbrevMap = catl::shamap::SHAMapT<
                        catl::shamap::AbbreviatedTreeTraits>;

                    auto reconstructed = AbbrevMap::from_trie_json(
                        step.at("trie"),
                        node_type,
                        [](std::string const& key_hex,
                           boost::json::value const& data)
                            -> boost::intrusive_ptr<MmapItem> {
                            auto key = hash_from_hex(key_hex);

                            // Use "blob" field if present (raw hex)
                            if (data.is_object() &&
                                data.as_object().contains("blob"))
                            {
                                auto blob_hex = std::string(
                                    data.as_object().at("blob").as_string());
                                auto bytes = std::vector<uint8_t>();
                                bytes.reserve(blob_hex.size() / 2);
                                for (size_t i = 0; i + 1 < blob_hex.size();
                                     i += 2)
                                {
                                    unsigned int b;
                                    std::sscanf(
                                        blob_hex.c_str() + i, "%2x", &b);
                                    bytes.push_back(static_cast<uint8_t>(b));
                                }
                                Slice s(bytes.data(), bytes.size());
                                return boost::intrusive_ptr<MmapItem>(
                                    OwnedItem::create(key, s));
                            }

                            // No blob — dummy leaf
                            uint8_t dummy = 0;
                            Slice empty(&dummy, 0);
                            return boost::intrusive_ptr<MmapItem>(
                                OwnedItem::create(key, empty));
                        });

                    auto computed_root = reconstructed.get_hash();
                    auto expected_root =
                        is_state ? trusted_ac_hash : trusted_tx_hash;

                    map_step.computed_root =
                        upper_hex(computed_root).substr(0, 16);

                    if (computed_root == expected_root)
                    {
                        PLOGI(
                            verify_log,
                            "  Trie root:    PASS (",
                            upper_hex(computed_root).substr(0, 16),
                            "... matches header)");
                        map_step.root_hash_check = xproof::Check::pass;
                    }
                    else
                    {
                        PLOGE(verify_log, "  Trie root:    FAIL!");
                        PLOGE(
                            verify_log,
                            "    expected: ",
                            upper_hex(expected_root));
                        PLOGE(
                            verify_log,
                            "    computed: ",
                            upper_hex(computed_root));
                        map_step.root_hash_check = xproof::Check::fail;
                        all_ok = false;
                    }
                }
                catch (std::exception const& e)
                {
                    PLOGW(
                        verify_log, "  Trie reconstruction failed: ", e.what());
                }
            }

            proof_steps.push_back(
                {is_state ? xproof::StepType::state_proof
                          : xproof::StepType::tx_proof,
                 step_num,
                 std::move(map_step)});

            have_tree_hashes = false;
        }
        // Unknown step type — skip
        else
        {
            PLOGD(verify_log, "  Unknown step type: ", type);
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
    auto narrative_lines = xproof::render_narrative(proof_steps);
    PLOGI(verify_log, "");
    for (auto const& line : narrative_lines)
    {
        PLOGI(verify_log, line);
    }
    if (all_ok)
    {
        PLOGI(verify_log, "");
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
                    walker.set_speculative_depth(8);
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
                    walker.set_speculative_depth(4);
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

                // ── Step 7: Build proof chain ──
                xproof::ProofChain proof_chain;

                // Helper: convert LedgerHeaderResult to HeaderData
                auto make_header = [](auto const& hdr_result)
                    -> xproof::HeaderData {
                    auto h = hdr_result.header();
                    return {
                        .seq = hdr_result.seq(),
                        .drops = h.drops(),
                        .parent_hash = h.parent_hash(),
                        .tx_hash = h.tx_hash(),
                        .account_hash = h.account_hash(),
                        .parent_close_time = h.parent_close_time(),
                        .close_time = h.close_time(),
                        .close_time_resolution = h.close_time_resolution(),
                        .close_flags = h.close_flags(),
                    };
                };

                // Helper: render abbreviated tree to trie JSON
                auto make_trie = [&](auto& tree, bool is_tx)
                    -> boost::json::value {
                    catl::shamap::TrieJsonOptions trie_opts;
                    trie_opts.on_leaf =
                        make_proof_leaf_callback(protocol, is_tx);
                    return tree.get_root()->trie_json(
                        trie_opts, tree.get_options());
                };

                // 1. Anchor
                {
                    xproof::AnchorData anchor;
                    anchor.ledger_hash = anchor_hash;
                    anchor.ledger_index = anchor_hdr.seq();

                    if (vl_data)
                    {
                        anchor.publisher_key_hex =
                            vl_data->publisher_key_hex;
                        anchor.publisher_manifest =
                            vl_data->publisher_manifest.raw;
                        anchor.blob = vl_data->blob_bytes;
                        anchor.blob_signature = vl_data->blob_signature;
                    }

                    auto const& qvals =
                        val_collector.quorum_validations();
                    for (auto const& v : qvals)
                    {
                        anchor.validations[xproof::bytes_hex(v.signing_key)] =
                            v.raw;
                    }

                    proof_chain.steps.push_back(std::move(anchor));
                }

                // 2. Anchor header
                proof_chain.steps.push_back(make_header(anchor_hdr));

                // 3. State tree proofs
                if (need_flag_hop && state_proofs.size() >= 2)
                {
                    // Long skip list state proof
                    {
                        auto& [key, sp] = state_proofs[0];
                        xproof::TrieData trie;
                        trie.tree = xproof::TrieData::TreeType::state;
                        trie.trie_json = make_trie(sp.tree, false);
                        proof_chain.steps.push_back(std::move(trie));
                    }

                    // Flag header
                    {
                        uint32_t flag_seq =
                            ((tx_ledger_seq + 255) / 256) * 256;
                        auto flag_hdr2 =
                            co_await co_get_ledger_header(client, flag_seq);
                        proof_chain.steps.push_back(
                            make_header(flag_hdr2));
                    }

                    // Short skip list state proof
                    {
                        auto& [key, sp] = state_proofs[1];
                        xproof::TrieData trie;
                        trie.tree = xproof::TrieData::TreeType::state;
                        trie.trie_json = make_trie(sp.tree, false);
                        proof_chain.steps.push_back(std::move(trie));
                    }
                }
                else if (!state_proofs.empty())
                {
                    auto& [key, sp] = state_proofs[0];
                    xproof::TrieData trie;
                    trie.tree = xproof::TrieData::TreeType::state;
                    trie.trie_json = make_trie(sp.tree, false);
                    proof_chain.steps.push_back(std::move(trie));
                }

                // 4. Target header
                proof_chain.steps.push_back(make_header(target_hdr));

                // 5. TX tree proof
                {
                    xproof::TrieData trie;
                    trie.tree = xproof::TrieData::TreeType::tx;
                    trie.trie_json = make_trie(abbrev, true);
                    proof_chain.steps.push_back(std::move(trie));
                }

                // Serialize to JSON
                auto chain = xproof::to_json(proof_chain);
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
                resolve_proof_chain(
                    chain, vl_data ? vl_data->publisher_key_hex : "");

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
