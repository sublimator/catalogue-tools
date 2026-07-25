#include "xprv/proof-resolver.h"
#include "xprv/anchor-verifier.h"
#include "xprv/hex-utils.h"
#include "xprv/ledger-hashes.h"
#include "xprv/proof-chain-json.h"
#include "xprv/proof-steps.h"

#include <catl/core/logger.h>
#include <catl/crypto/sha512-half-hasher.h>
#include <catl/shamap/shamap.h>
#include <catl/xdata/parse_leaf.h>
#include <catl/xdata/parse_transaction.h>
#include <catl/xdata/serialize.h>

#include <algorithm>
#include <cctype>
#include <map>
#include <optional>
#include <set>
#include <string_view>

namespace xprv {

static LogPartition verify_log("verify", LogLevel::INFO);

//------------------------------------------------------------------------------
// Helpers
//------------------------------------------------------------------------------

/// Hash a ledger header per XRPL spec.
static Hash256
hash_header(HeaderData const& hdr)
{
    catl::crypto::Sha512HalfHasher h;

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

    write_u32(hdr.seq);
    write_u64(hdr.drops);
    h.update(hdr.parent_hash.data(), 32);
    h.update(hdr.tx_hash.data(), 32);
    h.update(hdr.account_hash.data(), 32);
    write_u32(hdr.parent_close_time);
    write_u32(hdr.close_time);
    h.update(&hdr.close_time_resolution, 1);
    h.update(&hdr.close_flags, 1);

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
        stats.placeholders++;
        if (depth > stats.max_depth)
            stats.max_depth = depth;
    }
    else if (node.is_array())
    {
        stats.leaves++;
        if (depth > stats.max_depth)
            stats.max_depth = depth;
    }
    else if (node.is_object())
    {
        stats.inners++;
        auto const& obj = node.as_object();
        for (auto const& kv : obj)
        {
            if (kv.key() == "__depth__")
                continue;
            if (kv.key().size() == 1)
                count_trie_nodes(kv.value(), depth + 1, stats);
        }
    }
}

struct LedgerHashesLeaf
{
    Hash256 key;
    std::vector<std::string> hashes;
    std::optional<uint32_t> last_ledger_seq;
};

struct LedgerHashesSupport
{
    Hash256 leaf_key;
    uint32_t containing_ledger_seq = 0;
    std::optional<uint32_t> last_ledger_seq;
    int hashes_count = 0;
    std::string source;
};

struct ResolvedLedgerHashesSupport
{
    LedgerHashesListType type;
    int hashes_count = 0;
    std::string source;
};

static std::string
upper_copy(std::string s);

static bool
is_hash_hex(std::string_view value)
{
    return value.size() == 64 &&
        std::all_of(value.begin(), value.end(), [](unsigned char c) {
               return std::isxdigit(c) != 0;
           });
}

static std::optional<LedgerHashesLeaf>
parse_ledger_hashes_leaf(
    std::string const& key_hex,
    boost::json::value const& leaf_data)
{
    if (!is_hash_hex(key_hex) || !leaf_data.is_object())
        return std::nullopt;

    auto const& sle = leaf_data.as_object();
    auto const type = sle.find("LedgerEntryType");
    auto const hashes = sle.find("Hashes");
    if (type == sle.end() || !type->value().is_string() ||
        type->value().as_string() != "LedgerHashes" || hashes == sle.end() ||
        !hashes->value().is_array() || hashes->value().as_array().size() > 256)
        return std::nullopt;

    LedgerHashesLeaf result;
    result.key = hash_from_hex(key_hex);
    result.hashes.reserve(hashes->value().as_array().size());
    for (auto const& hash : hashes->value().as_array())
    {
        if (!hash.is_string())
            return std::nullopt;
        auto hash_hex = std::string(hash.as_string());
        if (!is_hash_hex(hash_hex))
            return std::nullopt;
        result.hashes.push_back(upper_copy(std::move(hash_hex)));
    }

    if (auto const last = sle.find("LastLedgerSequence"); last != sle.end())
    {
        try
        {
            result.last_ledger_seq = last->value().to_number<uint32_t>();
        }
        catch (...)
        {
            return std::nullopt;
        }
    }

    return result;
}

static std::optional<ResolvedLedgerHashesSupport>
find_ledger_hashes_support(
    std::map<std::string, std::vector<LedgerHashesSupport>> const& candidates,
    std::string const& header_hash,
    uint32_t header_seq)
{
    auto const found = candidates.find(header_hash);
    if (found == candidates.end())
        return std::nullopt;

    for (auto const& candidate : found->second)
    {
        auto const type = classify_ledger_hashes_link(
            candidate.leaf_key, candidate.containing_ledger_seq, header_seq);
        if (!type ||
            !ledger_hashes_last_sequence_consistent(
                *type,
                candidate.containing_ledger_seq,
                header_seq,
                candidate.last_ledger_seq))
            continue;

        return ResolvedLedgerHashesSupport{
            *type, candidate.hashes_count, candidate.source};
    }

    return std::nullopt;
}

/// Find the first leaf (array node) in a JSON trie.
static boost::json::value const*
extract_trie_leaf(boost::json::value const& trie)
{
    if (trie.is_array())
        return &trie;
    if (trie.is_object())
    {
        for (auto const& kv : trie.as_object())
        {
            if (kv.key() == "__depth__")
                continue;
            // Only real branches. This walk feeds the human-readable narrative
            // (tx type, account), and it previously visited EVERY member — so a
            // member the hash walk ignored could still be displayed as the
            // proof's content. What is reported must be what was authenticated.
            if (kv.key().size() != 1)
                continue;
            // Uppercase only (spec §2.3), matching the hash walk. If this walk
            // admitted a spelling the hash walk refuses, it could report content
            // that was never authenticated.
            char const c = kv.key()[0];
            bool const is_branch =
                (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F');
            if (!is_branch)
                continue;
            auto result = extract_trie_leaf(kv.value());
            if (result)
                return result;
        }
    }
    return nullptr;
}

static void
add_capability(
    std::map<std::string, std::vector<std::string>>& store,
    std::string const& key,
    std::string source)
{
    auto& sources = store[key];
    if (std::find(sources.begin(), sources.end(), source) == sources.end())
        sources.push_back(std::move(source));
}

static std::string const*
first_source(
    std::map<std::string, std::vector<std::string>> const& store,
    std::string const& key)
{
    auto const it = store.find(key);
    if (it == store.end() || it->second.empty())
        return nullptr;
    return &it->second.front();
}

static std::string
upper_copy(std::string s)
{
    for (auto& c : s)
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    return s;
}

//------------------------------------------------------------------------------
// Main resolver
//------------------------------------------------------------------------------

static bool
resolve_proof_chain_impl(
    ProofChain const& chain,
    catl::xdata::Protocol const& protocol,
    std::string const& trusted_publisher_key,
    bool pretrusted_anchor_for_testing)
{
    PLOGI(verify_log, "");
    PLOGI(
        verify_log,
        "═══════════════════════════════════════════════════════════════");

    if (trusted_publisher_key.empty() && !pretrusted_anchor_for_testing)
    {
        PLOGE(
            verify_log,
            "  Anchor: no trusted publisher key configured; refusing "
            "implicit trust");
        return false;
    }
    PLOGI(
        verify_log,
        "  PROOF CHAIN VERIFICATION  (",
        chain.steps.size(),
        " steps)");
    PLOGI(
        verify_log,
        "═══════════════════════════════════════════════════════════════");

    // Spec §1: exactly one logical anchor, and it precedes every other step.
    // Checked explicitly rather than left to fall out of the capability model.
    // Without an anchor the first header simply finds no trusted hash and fails,
    // which reports as "unsupported header" — true but misleading, and it makes
    // a structurally nonsensical proof look like a cryptographic failure.
    {
        size_t anchors = 0;
        size_t first_anchor_at = 0;
        for (size_t i = 0; i < chain.steps.size(); ++i)
        {
            if (std::holds_alternative<AnchorData>(chain.steps[i]))
            {
                if (anchors == 0)
                    first_anchor_at = i;
                ++anchors;
            }
        }
        if (anchors != 1)
        {
            PLOGE(
                verify_log,
                "  Structure: expected exactly one anchor step, found ",
                anchors);
            return false;
        }
        if (first_anchor_at != 0)
        {
            PLOGE(
                verify_log,
                "  Structure: anchor must be the first step, found at index ",
                first_anchor_at);
            return false;
        }
    }

    int step_num = 0;
    bool all_ok = true;

    std::map<std::string, std::vector<std::string>> trusted_ledger_hashes;
    std::map<std::string, std::vector<std::string>> trusted_tx_roots;
    std::map<std::string, std::vector<std::string>> trusted_state_roots;
    std::map<std::string, std::set<uint32_t>> trusted_state_root_sequences;
    std::map<std::string, std::vector<LedgerHashesSupport>>
        ledger_hashes_candidates;
    std::map<std::string, uint32_t> skip_source_last_seq;
    std::set<std::string> seen_header_hashes;
    std::set<std::pair<std::string, std::string>> seen_map_roots;
    std::optional<std::pair<uint32_t, uint32_t>> anchor_vl_window;
    bool anchor_seen = false;

    std::vector<ProofStep> proof_steps;

    for (auto const& step : chain.steps)
    {
        step_num++;

        std::visit(
            [&](auto const& data) {
                using T = std::decay_t<decltype(data)>;

                // ── Anchor ──────────────────────────────────────
                if constexpr (std::is_same_v<T, AnchorData>)
                {
                    auto ledger_hash_hex = upper_hex(data.ledger_hash);

                    bool const anchor_position_valid =
                        step_num == 1 && !anchor_seen;
                    if (step_num != 1)
                    {
                        PLOGE(verify_log, "  Anchor: must be the first step");
                        all_ok = false;
                    }
                    if (anchor_seen)
                    {
                        PLOGE(verify_log, "  Anchor: duplicate anchor step");
                        all_ok = false;
                    }
                    anchor_seen = true;

                    PLOGI(verify_log, "");
                    PLOGI(verify_log, "Step ", step_num, ": ANCHOR");
                    PLOGI(
                        verify_log,
                        "  Trusted hash: ",
                        ledger_hash_hex.substr(0, 16),
                        "...");
                    PLOGI(verify_log, "  Ledger index: ", data.ledger_index);

                    AnchorStep anchor_step;
                    anchor_step.ledger_index = data.ledger_index;
                    anchor_step.ledger_hash = ledger_hash_hex.substr(0, 16);

                    bool anchor_verified = pretrusted_anchor_for_testing;
                    if (!pretrusted_anchor_for_testing)
                    {
                        try
                        {
                            // Re-serialize anchor to JSON for AnchorVerifier
                            // (it still operates on JSON for now)
                            auto anchor_json =
                                to_json(ProofChain{.steps = {data}});
                            auto const& steps =
                                anchor_json.at("steps").as_array();
                            std::vector<std::string> av_narrative;
                            auto av_result = AnchorVerifier::verify(
                                steps[0].as_object(),
                                trusted_publisher_key,
                                protocol,
                                av_narrative);

                            anchor_step.sub_steps = std::move(av_narrative);
                            anchor_step.publisher_key =
                                trusted_publisher_key.substr(0, 16);
                            anchor_step.quorum_check =
                                av_result.verified ? Check::pass : Check::fail;
                            anchor_step.unl_size = av_result.unl_size;
                            anchor_step.validations_verified =
                                av_result.validations_verified;
                            anchor_step.validations_matched =
                                av_result.validations_matched_unl;
                            anchor_verified = av_result.verified;

                            if (anchor_verified)
                            {
                                anchor_vl_window = std::pair{
                                    av_result.vl_effective,
                                    av_result.vl_expiration};
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
                        catch (std::exception const& e)
                        {
                            PLOGE(
                                verify_log,
                                "  Anchor: FAILED (malformed anchor: ",
                                e.what(),
                                ")");
                            anchor_step.quorum_check = Check::fail;
                            all_ok = false;
                        }
                    }

                    if (anchor_verified && anchor_position_valid)
                        add_capability(
                            trusted_ledger_hashes, ledger_hash_hex, "anchor");

                    proof_steps.push_back(
                        {StepType::anchor, step_num, std::move(anchor_step)});
                }
                // ── Ledger Header ───────────────────────────────
                else if constexpr (std::is_same_v<T, HeaderData>)
                {
                    PLOGI(verify_log, "");
                    PLOGI(
                        verify_log,
                        "Step ",
                        step_num,
                        ": LEDGER HEADER (seq=",
                        data.seq,
                        ")");

                    auto computed = hash_header(data);
                    auto computed_hex = upper_hex(computed);
                    PLOGI(
                        verify_log,
                        "  Computed hash: ",
                        computed_hex.substr(0, 16),
                        "...");

                    HeaderStep hdr_step;
                    hdr_step.seq = data.seq;
                    hdr_step.computed_hash = computed_hex.substr(0, 16);
                    hdr_step.role = HeaderRole::target;

                    auto const* direct_source =
                        first_source(trusted_ledger_hashes, computed_hex);
                    auto const skip_support = direct_source
                        ? std::optional<ResolvedLedgerHashesSupport>{}
                        : find_ledger_hashes_support(
                              ledger_hashes_candidates, computed_hex, data.seq);
                    bool const header_supported = direct_source || skip_support;
                    bool anchor_vl_time_valid = true;
                    if (direct_source && *direct_source == "anchor" &&
                        anchor_vl_window)
                    {
                        anchor_vl_time_valid = validator_list_active_at(
                            anchor_vl_window->first,
                            anchor_vl_window->second,
                            data.close_time);
                    }

                    if (!header_supported || !anchor_vl_time_valid)
                    {
                        if (!anchor_vl_time_valid)
                        {
                            PLOGE(
                                verify_log,
                                "  VL validity:  FAIL (anchor close time is "
                                "outside the signed list interval)");
                        }
                        else
                        {
                            PLOGE(
                                verify_log,
                                "  Hash check:   FAIL (unsupported by prior "
                                "anchor/header/recognized LedgerHashes leaf)");
                        }
                        all_ok = false;
                        hdr_step.hash_check = Check::fail;
                    }
                    else
                    {
                        hdr_step.hash_check = Check::pass;
                        if (direct_source && *direct_source == "anchor")
                        {
                            hdr_step.method = HeaderVerifyMethod::anchor_hash;
                            hdr_step.role = HeaderRole::anchor;
                            PLOGI(verify_log, "  Hash check:   PASS");
                        }
                        else if (skip_support)
                        {
                            hdr_step.method = HeaderVerifyMethod::skip_list;
                            hdr_step.role =
                                skip_support->type == LedgerHashesListType::flag
                                ? HeaderRole::flag
                                : HeaderRole::target;
                            hdr_step.skip_list_size =
                                skip_support->hashes_count;
                            hdr_step.skip_type =
                                skip_support->type == LedgerHashesListType::flag
                                ? SkipListType::flag
                                : SkipListType::recent;
                            PLOGI(
                                verify_log,
                                "  LedgerHashes: PASS (source ",
                                skip_support->source,
                                ")");

                            auto [it_last, inserted] =
                                skip_source_last_seq.emplace(
                                    skip_support->source, data.seq);
                            if (!inserted && data.seq > it_last->second)
                            {
                                PLOGW(
                                    verify_log,
                                    "  Non-canonical: sibling header seq ",
                                    data.seq,
                                    " appears after ",
                                    it_last->second,
                                    " from ",
                                    skip_support->source);
                            }
                            it_last->second = data.seq;
                        }
                        else
                        {
                            hdr_step.method = HeaderVerifyMethod::parent_hash;
                            hdr_step.role = HeaderRole::target;
                            PLOGI(
                                verify_log,
                                "  Parent hash:  PASS (source ",
                                *direct_source,
                                ")");
                        }
                    }

                    if (!seen_header_hashes.insert(computed_hex).second)
                    {
                        PLOGW(
                            verify_log,
                            "  Non-canonical: duplicate header ",
                            computed_hex.substr(0, 16),
                            "...");
                    }

                    if (header_supported && anchor_vl_time_valid)
                    {
                        add_capability(
                            trusted_ledger_hashes,
                            upper_hex(data.parent_hash),
                            "parent hash of header seq=" +
                                std::to_string(data.seq));
                        add_capability(
                            trusted_tx_roots,
                            upper_hex(data.tx_hash),
                            "header seq=" + std::to_string(data.seq));
                        auto const state_root = upper_hex(data.account_hash);
                        add_capability(
                            trusted_state_roots,
                            state_root,
                            "header seq=" + std::to_string(data.seq));
                        trusted_state_root_sequences[state_root].insert(
                            data.seq);
                    }

                    PLOGI(
                        verify_log,
                        "  tx_hash:      ",
                        upper_hex(data.tx_hash).substr(0, 16),
                        "...");
                    PLOGI(
                        verify_log,
                        "  account_hash: ",
                        upper_hex(data.account_hash).substr(0, 16),
                        "...");

                    hdr_step.tx_hash = upper_hex(data.tx_hash).substr(0, 16);
                    hdr_step.account_hash =
                        upper_hex(data.account_hash).substr(0, 16);

                    proof_steps.push_back(
                        {StepType::ledger_header,
                         step_num,
                         std::move(hdr_step)});
                }
                // ── Map Proof ───────────────────────────────────
                else if constexpr (std::is_same_v<T, TrieData>)
                {
                    bool is_state = (data.tree == TrieData::TreeType::state);

                    PLOGI(verify_log, "");
                    PLOGI(
                        verify_log,
                        "Step ",
                        step_num,
                        ": MAP PROOF (",
                        (is_state ? "state" : "tx"),
                        " tree)");

                    MapProofStep map_step;
                    map_step.is_state = is_state;

                    // Reconstruct the abbreviated SHAMap from
                    // whichever representation is available.
                    auto node_type = is_state ? catl::shamap::tnACCOUNT_STATE
                                              : catl::shamap::tnTRANSACTION_MD;

                    using AbbrevMap = catl::shamap::SHAMapT<
                        catl::shamap::AbbreviatedTreeTraits>;

                    std::optional<AbbrevMap> reconstructed;
                    std::vector<LedgerHashesLeaf> ledger_hashes_leaves;

                    // --- JSON trie path ---
                    if (!data.trie_json.is_null())
                    {
                        TrieStats stats;
                        count_trie_nodes(data.trie_json, 0, stats);
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

                        try
                        {
                            reconstructed = AbbrevMap::from_trie_json(
                                data.trie_json,
                                node_type,
                                [&](std::string const& key_hex,
                                    boost::json::value const& leaf_data)
                                    -> boost::intrusive_ptr<MmapItem> {
                                    auto key = hash_from_hex(key_hex);
                                    if (!leaf_data.is_object())
                                    {
                                        uint8_t dummy = 0;
                                        return boost::intrusive_ptr<MmapItem>(
                                            OwnedItem::create(
                                                key, Slice(&dummy, 0)));
                                    }
                                    if (is_state)
                                    {
                                        auto candidate =
                                            parse_ledger_hashes_leaf(
                                                key_hex, leaf_data);
                                        if (candidate)
                                            ledger_hashes_leaves.push_back(
                                                std::move(*candidate));
                                    }
                                    auto bytes = is_state
                                        ? catl::xdata::serialize_leaf(
                                              leaf_data.as_object(), protocol)
                                        : catl::xdata::serialize_transaction(
                                              leaf_data.as_object(), protocol);
                                    Slice s(bytes.data(), bytes.size());
                                    return boost::intrusive_ptr<MmapItem>(
                                        OwnedItem::create(key, s));
                                });
                        }
                        catch (std::exception const& e)
                        {
                            PLOGW(
                                verify_log,
                                "  JSON trie reconstruction failed: ",
                                e.what());
                        }

                        // Extract narrative info from JSON trie
                        if (is_state)
                        {
                            if (!ledger_hashes_leaves.empty())
                            {
                                for (auto const& leaf : ledger_hashes_leaves)
                                    map_step.hashes_count +=
                                        static_cast<int>(leaf.hashes.size());
                                map_step.skip_type =
                                    ledger_hashes_leaves.front().key ==
                                        skip_list_key()
                                    ? SkipListType::recent
                                    : SkipListType::flag;
                            }
                        }
                        else
                        {
                            auto const* leaf =
                                extract_trie_leaf(data.trie_json);
                            if (leaf && leaf->is_array() &&
                                leaf->as_array().size() >= 2)
                            {
                                auto const& ld = leaf->as_array()[1];
                                if (ld.is_object() &&
                                    ld.as_object().contains("tx"))
                                {
                                    auto const& tx =
                                        ld.as_object().at("tx").as_object();
                                    if (tx.contains("TransactionType"))
                                    {
                                        map_step.tx_type =
                                            std::string(tx.at("TransactionType")
                                                            .as_string());
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
                    // --- Binary trie path ---
                    else if (!data.trie_binary.empty())
                    {
                        PLOGI(
                            verify_log,
                            "  Binary trie: ",
                            data.trie_binary.size(),
                            " bytes");

                        try
                        {
                            reconstructed = AbbrevMap::from_trie_binary(
                                data.trie_binary, node_type);

                            // Decode leaf data for narrative
                            reconstructed->walk_abbreviated(
                                [&](catl::shamap::SHAMapNodeID const&,
                                    Hash256 const&,
                                    catl::shamap::SHAMapTreeNodeT<
                                        catl::shamap::
                                            AbbreviatedTreeTraits> const*
                                        node) {
                                    if (!node || !node->is_leaf())
                                        return;
                                    auto leaf = static_cast<
                                        catl::shamap::SHAMapLeafNodeT<
                                            catl::shamap::
                                                AbbreviatedTreeTraits> const*>(
                                        node);
                                    auto item = leaf->get_item();
                                    if (!item)
                                        return;

                                    // Reconstruct full leaf bytes:
                                    // item_data + 32-byte key
                                    std::vector<uint8_t> buf(
                                        item->slice().data(),
                                        item->slice().data() +
                                            item->slice().size());
                                    buf.insert(
                                        buf.end(),
                                        item->key().data(),
                                        item->key().data() + 32);
                                    Slice full(buf.data(), buf.size());

                                    try
                                    {
                                        if (is_state)
                                        {
                                            auto parsed =
                                                catl::xdata::parse_leaf(
                                                    full, protocol, false);
                                            auto candidate =
                                                parse_ledger_hashes_leaf(
                                                    upper_hex(
                                                        item->key().to_hash()),
                                                    parsed);
                                            if (candidate)
                                                ledger_hashes_leaves.push_back(
                                                    std::move(*candidate));
                                        }
                                        else
                                        {
                                            auto parsed =
                                                catl::xdata::parse_transaction(
                                                    full,
                                                    protocol,
                                                    {.includes_prefix = false});
                                            if (parsed.is_object() &&
                                                parsed.as_object().contains(
                                                    "tx"))
                                            {
                                                auto const& tx =
                                                    parsed.as_object()
                                                        .at("tx")
                                                        .as_object();
                                                if (tx.contains(
                                                        "TransactionType"))
                                                {
                                                    map_step
                                                        .tx_type = std::string(
                                                        tx.at("TransactionType")
                                                            .as_string());
                                                    PLOGI(
                                                        verify_log,
                                                        "  TX type:      ",
                                                        map_step.tx_type);
                                                }
                                                if (tx.contains("Account"))
                                                {
                                                    map_step.tx_account =
                                                        std::string(
                                                            tx.at("Account")
                                                                .as_string());
                                                    PLOGI(
                                                        verify_log,
                                                        "  TX account:   ",
                                                        map_step.tx_account);
                                                }
                                            }
                                        }
                                    }
                                    catch (...)
                                    {
                                        // Leaf decode for narrative is
                                        // best-effort
                                    }
                                });
                        }
                        catch (std::exception const& e)
                        {
                            PLOGW(
                                verify_log,
                                "  Binary trie reconstruction failed: ",
                                e.what());
                        }
                    }

                    // --- Verify root hash (shared for both paths) ---
                    if (!reconstructed)
                    {
                        PLOGE(
                            verify_log,
                            "  Trie proof:   FAIL (could not reconstruct "
                            "abbreviated tree)");
                        map_step.root_hash_check = Check::fail;
                        all_ok = false;
                    }
                    else
                    {
                        try
                        {
                            auto computed_root = reconstructed->get_hash();
                            auto computed_root_hex = upper_hex(computed_root);
                            auto const* root_source = first_source(
                                is_state ? trusted_state_roots
                                         : trusted_tx_roots,
                                computed_root_hex);

                            map_step.computed_root =
                                computed_root_hex.substr(0, 16);

                            if (root_source)
                            {
                                PLOGI(
                                    verify_log,
                                    "  Trie root:    PASS (",
                                    computed_root_hex.substr(0, 16),
                                    "...)");
                                map_step.root_hash_check = Check::pass;
                                map_step.expected_root =
                                    computed_root_hex.substr(0, 16);

                                auto root_key = std::make_pair(
                                    std::string(is_state ? "state" : "tx"),
                                    computed_root_hex);
                                if (!seen_map_roots.insert(root_key).second)
                                {
                                    PLOGW(
                                        verify_log,
                                        "  Non-canonical: repeated ",
                                        (is_state ? "state" : "tx"),
                                        " root ",
                                        computed_root_hex.substr(0, 16),
                                        "...");
                                }

                                if (is_state)
                                {
                                    map_step.hashes_count = 0;
                                    for (auto const& leaf :
                                         ledger_hashes_leaves)
                                        map_step.hashes_count +=
                                            static_cast<int>(
                                                leaf.hashes.size());
                                    if (!ledger_hashes_leaves.empty())
                                    {
                                        map_step.skip_type =
                                            ledger_hashes_leaves.front().key ==
                                                skip_list_key()
                                            ? SkipListType::recent
                                            : SkipListType::flag;
                                        PLOGI(
                                            verify_log,
                                            "  LedgerHashes: ",
                                            map_step.hashes_count,
                                            " candidate hashes in ",
                                            ledger_hashes_leaves.size(),
                                            " structurally valid leaf/leaves");
                                    }

                                    auto const sequences =
                                        trusted_state_root_sequences.find(
                                            computed_root_hex);
                                    if (sequences ==
                                        trusted_state_root_sequences.end())
                                    {
                                        PLOGE(
                                            verify_log,
                                            "  LedgerHashes: FAIL (state root "
                                            "has no authenticated containing "
                                            "ledger sequence)");
                                        map_step.root_hash_check = Check::fail;
                                        all_ok = false;
                                    }
                                    else
                                    {
                                        for (auto const& leaf :
                                             ledger_hashes_leaves)
                                        {
                                            auto const source =
                                                "LedgerHashes leaf " +
                                                upper_hex(leaf.key).substr(
                                                    0, 16) +
                                                "... from step " +
                                                std::to_string(step_num);
                                            for (auto const containing_seq :
                                                 sequences->second)
                                            {
                                                for (auto const& hash :
                                                     leaf.hashes)
                                                {
                                                    ledger_hashes_candidates[hash]
                                                        .push_back(
                                                            LedgerHashesSupport{
                                                                leaf.key,
                                                                containing_seq,
                                                                leaf.last_ledger_seq,
                                                                static_cast<
                                                                    int>(
                                                                    leaf.hashes
                                                                        .size()),
                                                                source});
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            else
                            {
                                PLOGE(
                                    verify_log,
                                    "  Trie root:    FAIL (unsupported by "
                                    "prior header)");
                                map_step.root_hash_check = Check::fail;
                                all_ok = false;
                            }
                        }
                        catch (std::exception const& e)
                        {
                            PLOGE(
                                verify_log,
                                "  Trie hash verification failed: ",
                                e.what());
                            map_step.root_hash_check = Check::fail;
                            all_ok = false;
                        }
                    }

                    proof_steps.push_back(
                        {is_state ? StepType::state_proof : StepType::tx_proof,
                         step_num,
                         std::move(map_step)});
                }
            },
            step);
    }

    if (!anchor_seen)
    {
        PLOGE(verify_log, "  Anchor: missing required anchor step");
        all_ok = false;
    }

    // Results
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

    // Narrative
    auto narrative_lines = render_narrative(proof_steps);
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

bool
resolve_proof_chain(
    ProofChain const& chain,
    catl::xdata::Protocol const& protocol,
    std::string const& trusted_publisher_key)
{
    return resolve_proof_chain_impl(
        chain, protocol, trusted_publisher_key, false);
}

#ifdef XPRV_TESTING
bool
resolve_proof_chain_with_pretrusted_anchor_for_testing(
    ProofChain const& chain,
    catl::xdata::Protocol const& protocol)
{
    return resolve_proof_chain_impl(chain, protocol, "", true);
}
#endif

}  // namespace xprv
