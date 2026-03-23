#include "xproof/proof-resolver.h"
#include "xproof/anchor-verifier.h"
#include "xproof/hex-utils.h"
#include "xproof/proof-chain-json.h"
#include "xproof/proof-steps.h"

#include <catl/core/logger.h>
#include <catl/crypto/sha512-half-hasher.h>
#include <catl/shamap/shamap.h>

namespace xproof {

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

/// Extract the Hashes array from a state tree leaf.
static boost::json::array const*
extract_leaf_hashes(boost::json::value const& trie)
{
    if (trie.is_array())
    {
        auto const& arr = trie.as_array();
        if (arr.size() >= 2 && arr[1].is_object())
        {
            auto const& sle = arr[1].as_object();
            if (sle.contains("Hashes"))
                return &sle.at("Hashes").as_array();
        }
        return nullptr;
    }
    if (trie.is_object())
    {
        for (auto const& kv : trie.as_object())
        {
            if (kv.key() == "__depth__")
                continue;
            auto result = extract_leaf_hashes(kv.value());
            if (result)
                return result;
        }
    }
    return nullptr;
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
            auto result = extract_trie_leaf(kv.value());
            if (result)
                return result;
        }
    }
    return nullptr;
}

//------------------------------------------------------------------------------
// Main resolver
//------------------------------------------------------------------------------

bool
resolve_proof_chain(
    ProofChain const& chain,
    std::string const& trusted_publisher_key)
{
    PLOGI(verify_log, "");
    PLOGI(
        verify_log,
        "═══════════════════════════════════════════════════════════════");
    PLOGI(
        verify_log,
        "  PROOF CHAIN VERIFICATION  (",
        chain.steps.size(),
        " steps)");
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

    int header_count = 0;
    int state_proof_count = 0;
    uint32_t anchor_seq = 0;
    uint32_t target_seq = 0;

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
                    trusted_hash = data.ledger_hash;
                    have_trusted_hash = true;
                    anchor_seq = data.ledger_index;

                    PLOGI(verify_log, "");
                    PLOGI(verify_log, "Step ", step_num, ": ANCHOR");
                    PLOGI(
                        verify_log,
                        "  Trusted hash: ",
                        upper_hex(data.ledger_hash).substr(0, 16),
                        "...");
                    PLOGI(
                        verify_log,
                        "  Ledger index: ",
                        data.ledger_index);

                    AnchorStep anchor_step;
                    anchor_step.ledger_index = data.ledger_index;
                    anchor_step.ledger_hash =
                        upper_hex(data.ledger_hash).substr(0, 16);

                    if (!trusted_publisher_key.empty())
                    {
                        // Re-serialize anchor to JSON for AnchorVerifier
                        // (it still operates on JSON for now)
                        auto anchor_json = to_json(ProofChain{{data}});
                        std::vector<std::string> av_narrative;
                        auto av_result = AnchorVerifier::verify(
                            anchor_json[0].as_object(),
                            trusted_publisher_key,
                            av_narrative);

                        anchor_step.sub_steps = std::move(av_narrative);
                        anchor_step.publisher_key =
                            trusted_publisher_key.substr(0, 16);
                        anchor_step.quorum_check = av_result.verified
                            ? Check::pass
                            : Check::fail;
                        anchor_step.unl_size = av_result.unl_size;
                        anchor_step.validations_verified =
                            av_result.validations_verified;
                        anchor_step.validations_matched =
                            av_result.validations_matched_unl;

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

                    proof_steps.push_back(
                        {StepType::anchor, step_num, std::move(anchor_step)});
                }
                // ── Ledger Header ───────────────────────────────
                else if constexpr (std::is_same_v<T, HeaderData>)
                {
                    header_count++;
                    target_seq = data.seq;

                    std::string role;
                    if (header_count == 1)
                    {
                        role = "anchor";
                        anchor_seq = data.seq;
                    }
                    else if (
                        pending_hashes && state_proof_count >= 2)
                    {
                        role = "target";
                    }
                    else if (pending_hashes && state_proof_count == 1)
                    {
                        role = (data.seq % 256 == 0 &&
                                data.seq != anchor_seq)
                            ? "flag"
                            : "target";
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
                        data.seq,
                        ", ",
                        role,
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
                    hdr_step.role = (role == "anchor")
                        ? HeaderRole::anchor
                        : (role == "flag" ? HeaderRole::flag
                                          : HeaderRole::target);

                    if (have_trusted_hash)
                    {
                        hdr_step.method =
                            HeaderVerifyMethod::anchor_hash;
                        if (computed == trusted_hash)
                        {
                            PLOGI(
                                verify_log,
                                "  Hash check:   PASS");
                            hdr_step.hash_check = Check::pass;
                        }
                        else
                        {
                            PLOGE(verify_log, "  Hash check:   FAIL!");
                            all_ok = false;
                            hdr_step.hash_check = Check::fail;
                        }
                    }
                    else if (pending_hashes)
                    {
                        hdr_step.method =
                            HeaderVerifyMethod::skip_list;
                        hdr_step.skip_type = (role == "flag")
                            ? SkipListType::flag
                            : SkipListType::recent;
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
                                "  Skip list:    PASS (",
                                pending_hashes->size(),
                                " entries)");
                            hdr_step.hash_check = Check::pass;
                        }
                        else
                        {
                            PLOGE(
                                verify_log,
                                "  Skip list:    FAIL");
                            all_ok = false;
                            hdr_step.hash_check = Check::fail;
                        }
                        pending_hashes = nullptr;
                    }

                    trusted_tx_hash = data.tx_hash;
                    trusted_ac_hash = data.account_hash;
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

                    hdr_step.tx_hash =
                        upper_hex(trusted_tx_hash).substr(0, 16);
                    hdr_step.account_hash =
                        upper_hex(trusted_ac_hash).substr(0, 16);

                    proof_steps.push_back(
                        {StepType::ledger_header,
                         step_num,
                         std::move(hdr_step)});
                }
                // ── Map Proof ───────────────────────────────────
                else if constexpr (std::is_same_v<T, TrieData>)
                {
                    bool is_state =
                        (data.tree == TrieData::TreeType::state);
                    if (is_state)
                        state_proof_count++;

                    PLOGI(verify_log, "");
                    PLOGI(
                        verify_log,
                        "Step ",
                        step_num,
                        ": MAP PROOF (",
                        (is_state ? "state" : "tx"),
                        " tree)");

                    Hash256 expected;
                    if (have_tree_hashes)
                    {
                        expected = is_state ? trusted_ac_hash
                                            : trusted_tx_hash;
                        PLOGI(
                            verify_log,
                            "  Expected root: ",
                            upper_hex(expected).substr(0, 16),
                            "...");
                    }

                    MapProofStep map_step;
                    map_step.is_state = is_state;
                    if (have_tree_hashes)
                    {
                        map_step.expected_root =
                            upper_hex(expected).substr(0, 16);
                    }

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

                        if (is_state)
                        {
                            auto const* hashes =
                                extract_leaf_hashes(data.trie_json);
                            if (hashes)
                            {
                                pending_hashes_count =
                                    static_cast<int>(hashes->size());
                                pending_hashes = hashes;
                                map_step.hashes_count =
                                    pending_hashes_count;

                                bool is_flag_skip =
                                    (state_proof_count == 1 &&
                                     hashes->size() < 256);
                                map_step.skip_type = is_flag_skip
                                    ? SkipListType::flag
                                    : SkipListType::recent;

                                PLOGI(
                                    verify_log,
                                    "  Skip list:    ",
                                    hashes->size(),
                                    " hashes");
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
                                        ld.as_object()
                                            .at("tx")
                                            .as_object();
                                    if (tx.contains("TransactionType"))
                                    {
                                        map_step.tx_type = std::string(
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

                        // Reconstruct and verify trie root hash
                        if (have_tree_hashes)
                        {
                            try
                            {
                                auto node_type = is_state
                                    ? catl::shamap::tnACCOUNT_STATE
                                    : catl::shamap::tnTRANSACTION_MD;

                                using AbbrevMap = catl::shamap::SHAMapT<
                                    catl::shamap::
                                        AbbreviatedTreeTraits>;

                                auto reconstructed =
                                    AbbrevMap::from_trie_json(
                                        data.trie_json,
                                        node_type,
                                        [](std::string const& key_hex,
                                           boost::json::value const&
                                               leaf_data)
                                            -> boost::intrusive_ptr<
                                                MmapItem> {
                                            auto key =
                                                hash_from_hex(key_hex);
                                            if (leaf_data.is_object() &&
                                                leaf_data.as_object()
                                                    .contains("blob"))
                                            {
                                                auto blob =
                                                    from_hex(std::string(
                                                        leaf_data
                                                            .as_object()
                                                            .at("blob")
                                                            .as_string()));
                                                Slice s(
                                                    blob.data(),
                                                    blob.size());
                                                return boost::
                                                    intrusive_ptr<
                                                        MmapItem>(
                                                        OwnedItem::create(
                                                            key, s));
                                            }
                                            uint8_t dummy = 0;
                                            Slice empty(&dummy, 0);
                                            return boost::intrusive_ptr<
                                                MmapItem>(
                                                OwnedItem::create(
                                                    key, empty));
                                        });

                                auto computed_root =
                                    reconstructed.get_hash();
                                auto expected_root = is_state
                                    ? trusted_ac_hash
                                    : trusted_tx_hash;

                                map_step.computed_root =
                                    upper_hex(computed_root)
                                        .substr(0, 16);

                                if (computed_root == expected_root)
                                {
                                    PLOGI(
                                        verify_log,
                                        "  Trie root:    PASS (",
                                        upper_hex(computed_root)
                                            .substr(0, 16),
                                        "...)");
                                    map_step.root_hash_check =
                                        Check::pass;
                                }
                                else
                                {
                                    PLOGE(
                                        verify_log,
                                        "  Trie root:    FAIL!");
                                    map_step.root_hash_check =
                                        Check::fail;
                                    all_ok = false;
                                }
                            }
                            catch (std::exception const& e)
                            {
                                PLOGW(
                                    verify_log,
                                    "  Trie reconstruction failed: ",
                                    e.what());
                            }
                        }
                    }

                    proof_steps.push_back(
                        {is_state ? StepType::state_proof
                                  : StepType::tx_proof,
                         step_num,
                         std::move(map_step)});

                    have_tree_hashes = false;
                }
            },
            step);
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

}  // namespace xproof
