#include "xprv/proof-builder.h"
#include "xprv/hex-utils.h"
#include "xprv/network-config.h"
#include "xprv/skip-list.h"
#include "xprv/validation-collector.h"

#include <catl/core/logger.h>
#include <catl/core/request-context.h>
#include <catl/peer-client/peer-client-coro.h>
#include <catl/peer-client/peer-set.h>
#include <catl/rpc-client/rpc-client-coro.h>
#include <catl/shamap/shamap-hashprefix.h>
#include <catl/shamap/shamap-nodeid.h>
#include <catl/shamap/shamap.h>
#include <catl/xdata/parse_leaf.h>
#include <catl/xdata/parse_transaction.h>

#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <optional>
#include <unordered_map>
#include <unordered_set>

using namespace catl::peer_client;
using xprv::bytes_hex;
using xprv::hash_from_hex;
using xprv::skip_list_key;
using xprv::upper_hex;

namespace xprv {

static LogPartition log_("xprv", LogLevel::INFO);

static bool
advertises_ledger(PeerSession const* peer, uint32_t ledger_seq)
{
    if (!peer || !peer->is_ready())
        return false;
    return peer->advertised_ledger_range().contains(ledger_seq);
}

static WalkResult
require_walk_transport(WalkResult result)
{
    if (result.found() ||
        std::holds_alternative<WalkBranchAbsent>(result.terminal()))
    {
        return result;
    }
    if (auto const* unavailable =
            std::get_if<WalkNoSuitablePeer>(&result.terminal()))
    {
        throw PeerClientException(
            Error::NoSuitablePeer,
            "no eligible peer advertises ledger " +
                std::to_string(unavailable->ledger_seq) + "; " +
                describe_peer_history_coverage(unavailable->coverage));
    }
    if (auto const* exhausted =
            std::get_if<WalkTransportExhausted>(&result.terminal()))
    {
        throw PeerClientException(
            Error::Timeout,
            "node transport exhausted at depth " +
                std::to_string(exhausted->depth) + " after " +
                std::to_string(exhausted->retries) + " retries");
    }
    if (std::holds_alternative<WalkCancelled>(result.terminal()))
        throw PeerClientException(Error::Cancelled);
    if (auto const* exhausted =
            std::get_if<WalkDepthExhausted>(&result.terminal()))
    {
        throw PeerClientException(
            Error::ParseError,
            "tree walk exceeded depth " + std::to_string(exhausted->depth));
    }
    throw PeerClientException(Error::ParseError, "unknown tree-walk terminal");
}

/// Build a proof-format on_leaf callback for trie_json.
static catl::shamap::LeafJsonCallback
make_proof_leaf_callback(catl::xdata::Protocol const& protocol, bool is_tx_tree)
{
    return [&protocol, is_tx_tree](MmapItem const& item) -> boost::json::value {
        boost::json::array arr;
        arr.emplace_back(upper_hex(item.key().to_hash()));

        try
        {
            std::vector<uint8_t> buf(
                item.slice().data(), item.slice().data() + item.slice().size());
            buf.insert(buf.end(), item.key().data(), item.key().data() + 32);
            Slice full(buf.data(), buf.size());

            auto decoded = is_tx_tree
                ? catl::xdata::parse_transaction(
                      full,
                      protocol,
                      {.includes_prefix = false,
                       .json_opts = {.ascii_hints = false}})
                : catl::xdata::parse_leaf(
                      full,
                      protocol,
                      {.includes_prefix = false,
                       .json_opts = {.ascii_hints = false}});

            // A leaf carries decoded XRPL content and nothing else (spec §2.3).
            // The xdata parsers add convenience members for their other
            // callers: `index`/`hash` duplicate the trie key, `blob` duplicates
            // the very bytes being hashed. Emitting them here would mean the
            // JSON a reader sees is not exactly the JSON the verifier
            // re-serializes and hashes, so a displayed field could disagree
            // with the authenticated one while the proof still verified. They
            // are also not XRPL fields, so every verifier has to strip them
            // before encoding anyway.
            if (auto* obj = decoded.if_object())
            {
                obj->erase("index");
                obj->erase("hash");
                obj->erase("blob");
            }
            arr.emplace_back(std::move(decoded));
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

/// Parse SLE leaf data and check if a hash exists in sfHashes.
static bool
sle_hashes_contain(
    Slice leaf_data,
    Hash256 const& needle,
    catl::xdata::Protocol const& protocol)
{
    try
    {
        auto json = catl::xdata::parse_leaf(leaf_data, protocol, false);
        auto const& obj = json.as_object();
        if (!obj.contains("Hashes"))
        {
            return false;
        }
        auto const& arr = obj.at("Hashes").as_array();
        auto needle_hex = upper_hex(needle);
        for (auto const& h : arr)
        {
            if (h.as_string() == needle_hex)
            {
                return true;
            }
        }
        return false;
    }
    catch (std::exception const& e)
    {
        PLOGE(log_, "  sle_hashes_contain FAILED: ", e.what());
        return false;
    }
}
// ═══════════════════════════════════════════════════════════════════════
// build_proof(BuildServices) — uses shared PeerSet, VL, validations
// ═══════════════════════════════════════════════════════════════════════

boost::asio::awaitable<BuildResult>
build_proof(BuildServices svc, std::string const& tx_hash_str)
{
    // Propagate cancellation from prove() → HTTP session || operator
    co_await boost::asio::this_coro::reset_cancellation_state(
        boost::asio::enable_total_cancellation());

    auto const net_label = network_label(svc.network_id);
    auto tx_hash = hash_from_hex(tx_hash_str);

    using AbbrevMap =
        catl::shamap::SHAMapT<catl::shamap::AbbreviatedTreeTraits>;

    struct StateProofResult
    {
        bool verified;
        AbbrevMap tree;
    };

    // ── Shared context — lives on the heap, safe across co_await suspension ──
    // All mutable state referenced by lambdas goes here so that lambda captures
    // of `shared_ptr<ProveContext>` remain valid even if the coroutine frame is
    // destroyed by enable_total_cancellation.
    struct ProveContext
    {
        // Shared services (already ref-counted)
        std::shared_ptr<catl::peer_client::PeerSet> peers;
        std::shared_ptr<NodeCache> node_cache;
        catl::xdata::Protocol protocol;

        // Anchor state
        Hash256 anchor_hash;
        LedgerHeaderResult anchor_hdr;
        // account_hash extracted from anchor_hdr at init time — LedgerInfoView
        // is not default-constructible (raw pointer), so we store the value.
        Hash256 anchor_account_hash;

        // Cancel token — checked at cancellation boundaries
        std::shared_ptr<std::atomic<bool>> cancel_token;

        // Per-prove mutable results
        Hash256 target_ledger_hash;
        std::optional<LedgerHeaderResult> flag_hdr_result;
        std::optional<LedgerHeaderResult> target_hdr_result;
        bool need_flag_hop = false;
        uint32_t distance = 0;
        std::vector<std::tuple<Hash256, StateProofResult>> state_proofs;
    };

    auto ctx = std::make_shared<ProveContext>();
    ctx->peers = svc.peers;
    ctx->node_cache = svc.node_cache;
    ctx->protocol = svc.protocol;
    ctx->cancel_token = svc.cancel_token;

    // Convenience reference — safe between co_awaits in this frame
    if (!svc.vl)
        throw std::runtime_error("No VL snapshot provided");
    auto const& vl_data = *svc.vl;

    // walk_state uses NodeCache::walk_to — content-addressed, cross-ledger
    // sharing, concurrent dedup. Needs the tree root hash (account_hash)
    // in addition to ledger_hash.
    auto walk_state =
        [ctx](
            Hash256 const& ledger_hash,
            uint32_t ledger_seq,
            Hash256 const& state_root_hash,
            Hash256 const& key,
            PeerGenerationPin peer = {}) -> boost::asio::awaitable<WalkResult> {
        //@@authority transition=prv.T0007 status=provisional scope=statement
        // bind=walk_to@a8ce5c76166dd75c
        // owner=unresolved owner_hypothesis=ledger_fetch_coordinator
        // commit=unresolved calls=walk_to
        // label=submit_state_tree_interest_without_interest_identity
        auto result = co_await ctx->node_cache->walk_to(
            state_root_hash,
            key,
            ledger_hash,
            ledger_seq,
            2,  // liAS_NODE
            ctx->peers,
            std::move(peer),
            ctx->cancel_token);  //@@authority-target ids=prv.T0007
        co_return require_walk_transport(std::move(result));
    };

    // walk_tx same pattern but liTX_NODE
    auto walk_tx =
        [ctx](
            Hash256 const& ledger_hash,
            uint32_t ledger_seq,
            Hash256 const& tx_root_hash,
            Hash256 const& key,
            PeerGenerationPin peer = {}) -> boost::asio::awaitable<WalkResult> {
        //@@authority transition=prv.T0008 status=provisional scope=statement
        // bind=walk_to@c29ef95a7f7bf289
        // owner=unresolved owner_hypothesis=ledger_fetch_coordinator
        // commit=unresolved calls=walk_to
        // label=submit_transaction_tree_interest_without_interest_identity
        auto result = co_await ctx->node_cache->walk_to(
            tx_root_hash,
            key,
            ledger_hash,
            ledger_seq,
            1,  // liTX_NODE
            ctx->peers,
            std::move(peer),
            ctx->cancel_token);  //@@authority-target ids=prv.T0008
        co_return require_walk_transport(std::move(result));
    };

    auto build_state_proof =
        [ctx](
            WalkResult const& wr,
            Hash256 const& key,
            Hash256 const& expected_account_hash) -> StateProofResult {
        catl::shamap::SHAMapOptions opts;
        opts.tree_collapse_impl = catl::shamap::TreeCollapseImpl::leafs_only;
        opts.reference_hash_impl =
            catl::shamap::ReferenceHashImpl::recursive_simple;
        AbbrevMap abbrev(catl::shamap::tnACCOUNT_STATE, opts);

        // Convert WalkResult leaf_nid (peer_client::SHAMapNodeID) to
        // shamap::SHAMapNodeID for set_item_at
        auto const& leaf = wr.leaf();
        catl::shamap::SHAMapNodeID leaf_pos(
            Hash256(leaf.leaf_nid.id.data()), leaf.leaf_nid.depth);
        Slice item_data(leaf.leaf_data.data(), leaf.leaf_data.size() - 32);
        boost::intrusive_ptr<MmapItem> item(OwnedItem::create(key, item_data));
        abbrev.set_item_at(leaf_pos, item);

        for (auto const& p : wr.placeholders())
        {
            catl::shamap::SHAMapNodeID ph_pos(
                Hash256(p.nodeid.id.data()), p.nodeid.depth);
            if (abbrev.needs_placeholder(ph_pos))
                abbrev.set_placeholder(ph_pos, p.hash);
        }

        auto computed = abbrev.get_hash();
        bool ok = (computed == expected_account_hash);
        PLOGI(
            log_,
            "  State tree: ",
            ok ? "VERIFIED" : "MISMATCH",
            " (",
            wr.placeholders().size(),
            " placeholders)");
        catl::core::emit_status(
            std::string("state tree: ") + (ok ? "verified" : "MISMATCH") +
            " (" + std::to_string(wr.placeholders().size()) + " placeholders)");
        return {ok, std::move(abbrev)};
    };

    // ── Step 1: RPC — look up tx (or use cached ledger_seq) ──
    uint32_t tx_ledger_seq = svc.tx_ledger_seq_hint;
    if (tx_ledger_seq == 0)
    {
        auto tx_result = co_await catl::rpc::co_tx(*svc.rpc, tx_hash_str);
        auto const& tx_obj = tx_result.as_object();
        if (tx_obj.contains("ledger_index"))
            tx_ledger_seq = tx_obj.at("ledger_index").to_number<uint32_t>();
        if (tx_ledger_seq == 0)
            throw std::runtime_error(
                "tx " + tx_hash_str.substr(0, 16) +
                "... not found or no ledger_index");
        PLOGI(
            log_,
            "[",
            net_label,
            "] TX ",
            tx_hash_str.substr(0, 16),
            "... is in ledger ",
            tx_ledger_seq,
            " (RPC)");
    }
    else
    {
        PLOGI(
            log_,
            "[",
            net_label,
            "] TX ",
            tx_hash_str.substr(0, 16),
            "... is in ledger ",
            tx_ledger_seq,
            " (cached)");
    }

    // ── Step 2: Fetch anchor (lazy — knows tx_ledger_seq) ──
    if (!svc.get_anchor)
        throw std::runtime_error("No anchor provider");

    catl::core::emit_status("fetching anchor...");
    auto anchor = co_await svc.get_anchor(tx_ledger_seq);
    ctx->anchor_hdr = anchor.hdr;
    ctx->anchor_hash = anchor.hash;
    ctx->anchor_account_hash = anchor.account_hash;

    auto const& anchor_validations = anchor.validations;
    uint32_t anchor_seq = ctx->anchor_hdr.seq();
    PLOGI(
        log_,
        "[",
        net_label,
        "] Using validated anchor: seq=",
        anchor_seq,
        " (",
        anchor_validations.size(),
        " validators in proof)");

    // PeerSet chooses a suitable starting peer. NodeCache owns the bounded
    // fan-out and retry policy for the operation itself; once it gives up,
    // propagate that result instead of restarting the whole operation here.
    auto with_peer = [ctx, net_label](
                         uint32_t required_ledger,
                         std::string const& purpose,
                         auto op) -> decltype(op(PeerGenerationPin{})) {
        if (ctx->cancel_token &&
            ctx->cancel_token->load(std::memory_order_relaxed))
        {
            throw PeerClientException(Error::Cancelled);
        }

        catl::core::emit_status(
            "searching for peer advertising ledger " +
            std::to_string(required_ledger));
        // A targeted connect has a 10-second initial deadline and may receive
        // up to 15 seconds of bounded post-handshake status grace. Observe the
        // whole admitted attempt rather than timing out its requester while
        // the connect servant still owns a potentially useful peer.
        constexpr int history_peer_search_timeout_seconds = 30;
        auto peer = co_await ctx->peers->wait_for_covering_peer(
            required_ledger,
            history_peer_search_timeout_seconds,
            ctx->cancel_token);

        if (ctx->cancel_token &&
            ctx->cancel_token->load(std::memory_order_relaxed))
        {
            throw PeerClientException(Error::Cancelled);
        }
        if (!advertises_ledger(peer.get(), required_ledger))
        {
            PeerHistoryCoverage coverage;
            try
            {
                coverage = co_await ctx->peers->co_history_coverage();
            }
            catch (...)
            {
                // Diagnostics must not replace the typed selection refusal.
            }
            throw PeerClientException(
                Error::NoSuitablePeer,
                "no peer advertising ledger " +
                    std::to_string(required_ledger) + " while trying to " +
                    purpose + "; " + describe_peer_history_coverage(coverage));
        }

        PLOGI(
            log_,
            "[",
            net_label,
            "] Using peer ",
            peer->endpoint(),
            " for ",
            purpose);
        catl::core::emit_status("peer " + peer->endpoint() + " → " + purpose);
        co_return co_await op(std::move(peer));
    };

    // Don't call prioritize_ledger() — with concurrent prove() calls,
    // it would retune peer discovery away from other requests' targets.
    // wait_for_covering_peer(seq) already searches by the specific seq needed.

    // ── Helpers for streaming steps as they become available ──
    ProofChain proof_chain;

    auto emit_step = [&](ChainStep step) -> boost::asio::awaitable<void> {
        proof_chain.steps.push_back(std::move(step));
        if (svc.on_step)
            co_await svc.on_step(proof_chain.steps.back());
    };

    auto make_header = [](auto const& hdr_result) -> HeaderData {
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

    auto make_trie_data =
        [ctx](
            auto& tree, bool is_tx, TrieData::TreeType tree_type) -> TrieData {
        TrieData trie;
        trie.tree = tree_type;
        catl::shamap::TrieJsonOptions trie_opts;
        trie_opts.on_leaf = make_proof_leaf_callback(ctx->protocol, is_tx);
        trie.trie_json =
            tree.get_root()->trie_json(trie_opts, tree.get_options());
        trie.trie_binary = tree.trie_binary();
        return trie;
    };

    // ── Emit anchor + anchor header immediately ──
    {
        AnchorData anchor;
        anchor.ledger_hash = ctx->anchor_hash;
        anchor.ledger_index = ctx->anchor_hdr.seq();
        anchor.publisher_key_hex = vl_data.publisher_key_hex;
        anchor.publisher_manifest = vl_data.publisher_manifest.raw;
        anchor.blob = vl_data.blob_bytes;
        anchor.blob_signature = vl_data.blob_signature;

        for (auto const& v : anchor_validations)
            anchor.validations[v.signing_key_hex] = v.raw;

        co_await emit_step(std::move(anchor));
    }
    co_await emit_step(make_header(ctx->anchor_hdr));

    // ── Step 5: Determine hop path ──
    // Anchor must be at or ahead of the tx ledger. A stale anchor can
    // still happen if the latest quorum has not yet reached tx_ledger_seq.
    if (ctx->anchor_hdr.seq() < tx_ledger_seq)
    {
        throw std::runtime_error(
            "Anchor seq " + std::to_string(ctx->anchor_hdr.seq()) +
            " is behind tx seq " + std::to_string(tx_ledger_seq) +
            " — cannot build proof (stale anchor?)");
    }
    ctx->distance = ctx->anchor_hdr.seq() - tx_ledger_seq;
    PLOGI(log_, "[", net_label, "] Distance: ", ctx->distance, " ledgers");
    catl::core::emit_status(
        "distance: " + std::to_string(ctx->distance) + " ledgers");

    ctx->need_flag_hop = (ctx->distance > 256);

    // Prefer the anchor's long (era) skip list for ANY flag-ledger target
    // (index % 256 == 0), near or far. A flag ledger's hash sits directly in
    // its era list, so it is always one hop from the anchor. The era list is
    // capped at 256 flag hashes and only partially filled for the anchor's
    // current era, so its leaf is never larger than the always-full 256-entry
    // recent list — about half the size on average — while the Merkle path is
    // a statistical wash. So the era list is the shortest single hop even for a
    // near flag target. Only a distant *non-flag* target needs the 2-hop path
    // through an intermediate flag ledger; a near non-flag target uses the
    // recent short list.
    bool const target_is_flag = (tx_ledger_seq % 256 == 0);

    if (!ctx->need_flag_hop || target_is_flag)
    {
        bool const long_direct = target_is_flag;
        if (long_direct)
        {
            PLOGI(
                log_,
                "[",
                net_label,
                "] Long/era skip list (1-hop flag target, distance=",
                ctx->distance,
                ")");
            catl::core::emit_status("long/era skip list (1-hop flag target)");
        }
        else
        {
            PLOGI(log_, "[", net_label, "] Short skip list (within 256)");
            catl::core::emit_status("short skip list (within 256)");
        }
        auto skip_key_val =
            long_direct ? skip_list_key(tx_ledger_seq) : skip_list_key();

        auto wr = co_await with_peer(
            anchor_seq,
            "walk short skip list",
            [ctx, skip_key_val, anchor_seq, walk_state](
                PeerGenerationPin peer) -> boost::asio::awaitable<WalkResult> {
                co_return co_await walk_state(
                    ctx->anchor_hash,
                    anchor_seq,
                    ctx->anchor_account_hash,
                    skip_key_val,
                    std::move(peer));
            });
        if (!wr.found())
            throw std::runtime_error("Short skip list not found in state tree");

        auto sp = build_state_proof(wr, skip_key_val, ctx->anchor_account_hash);
        co_await emit_step(
            make_trie_data(sp.tree, false, TrieData::TreeType::state));
        ctx->state_proofs.emplace_back(skip_key_val, std::move(sp));

        ctx->target_hdr_result = co_await with_peer(
            tx_ledger_seq,
            "fetch target ledger header",
            [ctx, tx_ledger_seq](PeerGenerationPin peer)
                -> boost::asio::awaitable<LedgerHeaderResult> {
                co_return co_await ctx->node_cache->get_header(
                    tx_ledger_seq, ctx->peers, std::move(peer));
            });
        ctx->target_ledger_hash = ctx->target_hdr_result->ledger_hash256();

        auto const& wr_leaf = wr.leaf();
        Slice sle_leaf(wr_leaf.leaf_data.data(), wr_leaf.leaf_data.size());
        if (sle_hashes_contain(
                sle_leaf, ctx->target_ledger_hash, ctx->protocol))
        {
            PLOGI(log_, "  Target hash confirmed in short skip list");
        }
        else
        {
            PLOGW(log_, "  Target hash not found in short skip list!");
        }
    }
    else
    {
        PLOGI(
            log_,
            "[",
            net_label,
            "] Long skip list (2-hop, distance=",
            ctx->distance,
            ")");
        catl::core::emit_status(
            "long skip list (2-hop, distance=" + std::to_string(ctx->distance) +
            ")");

        // Distant non-flag target: hop through the flag ledger just above it.
        // Flag-ledger targets are handled one hop above via the anchor's long
        // skip list, so tx_ledger_seq is always non-flag here and
        // flag_ledger_for returns the nearest multiple of 256 above it, whose
        // short skip list contains the target.
        uint32_t flag_seq = flag_ledger_for(tx_ledger_seq);
        PLOGI(log_, "  Flag ledger: ", flag_seq);

        auto long_skip_key = skip_list_key(flag_seq);
        auto wr1 = co_await with_peer(
            anchor_seq,
            "walk long skip list",
            [ctx, long_skip_key, anchor_seq, walk_state](
                PeerGenerationPin peer) -> boost::asio::awaitable<WalkResult> {
                co_return co_await walk_state(
                    ctx->anchor_hash,
                    anchor_seq,
                    ctx->anchor_account_hash,
                    long_skip_key,
                    std::move(peer));
            });
        if (!wr1.found())
            throw std::runtime_error("Long skip list not found in state tree");

        {
            auto sp =
                build_state_proof(wr1, long_skip_key, ctx->anchor_account_hash);
            co_await emit_step(
                make_trie_data(sp.tree, false, TrieData::TreeType::state));
            ctx->state_proofs.emplace_back(long_skip_key, std::move(sp));
        }

        ctx->flag_hdr_result = co_await with_peer(
            flag_seq,
            "fetch flag ledger header",
            [ctx, flag_seq](PeerGenerationPin peer)
                -> boost::asio::awaitable<LedgerHeaderResult> {
                co_return co_await ctx->node_cache->get_header(
                    flag_seq, ctx->peers, std::move(peer));
            });
        auto flag_hash = ctx->flag_hdr_result->ledger_hash256();
        auto flag_header = ctx->flag_hdr_result->header();
        co_await emit_step(make_header(*ctx->flag_hdr_result));

        auto const& wr1_leaf = wr1.leaf();
        Slice long_sle(wr1_leaf.leaf_data.data(), wr1_leaf.leaf_data.size());
        if (sle_hashes_contain(long_sle, flag_hash, ctx->protocol))
        {
            PLOGI(log_, "  Flag hash confirmed in long skip list");
        }
        else
        {
            PLOGW(log_, "  Flag hash not found in long skip list!");
        }

        auto short_skip_key = skip_list_key();
        auto wr2 = co_await with_peer(
            flag_seq,
            "walk flag short skip list",
            [ctx, flag_hash, flag_seq, short_skip_key, walk_state](
                PeerGenerationPin peer) -> boost::asio::awaitable<WalkResult> {
                co_return co_await walk_state(
                    flag_hash,
                    flag_seq,
                    ctx->flag_hdr_result->header().account_hash(),
                    short_skip_key,
                    std::move(peer));
            });
        if (!wr2.found())
            throw std::runtime_error(
                "Short skip list not found in flag ledger state tree");

        {
            auto sp = build_state_proof(
                wr2, short_skip_key, flag_header.account_hash());
            co_await emit_step(
                make_trie_data(sp.tree, false, TrieData::TreeType::state));
            ctx->state_proofs.emplace_back(short_skip_key, std::move(sp));
        }

        ctx->target_hdr_result = co_await with_peer(
            tx_ledger_seq,
            "fetch target ledger header",
            [ctx, tx_ledger_seq](PeerGenerationPin peer)
                -> boost::asio::awaitable<LedgerHeaderResult> {
                co_return co_await ctx->node_cache->get_header(
                    tx_ledger_seq, ctx->peers, std::move(peer));
            });
        ctx->target_ledger_hash = ctx->target_hdr_result->ledger_hash256();

        auto const& wr2_leaf = wr2.leaf();
        Slice short_sle(wr2_leaf.leaf_data.data(), wr2_leaf.leaf_data.size());
        if (sle_hashes_contain(
                short_sle, ctx->target_ledger_hash, ctx->protocol))
        {
            PLOGI(log_, "  Target hash confirmed in flag's short skip list");
        }
        else
        {
            PLOGW(log_, "  Target hash not found in flag's short skip list!");
        }
    }

    // ── Step 6: Target header ──
    if (!ctx->target_hdr_result)
    {
        ctx->target_hdr_result = co_await with_peer(
            tx_ledger_seq,
            "fetch target ledger header",
            [ctx, tx_ledger_seq](PeerGenerationPin peer)
                -> boost::asio::awaitable<LedgerHeaderResult> {
                co_return co_await ctx->node_cache->get_header(
                    tx_ledger_seq, ctx->peers, std::move(peer));
            });
    }
    auto const& target_hdr = *ctx->target_hdr_result;
    auto target_header = target_hdr.header();

    // ── Step 7: Walk TX tree ──
    auto ledger_hash = target_hdr.ledger_hash256();
    PLOGI(
        log_,
        "[",
        net_label,
        "] Walking TX tree for ",
        tx_hash_str.substr(0, 16),
        "...");
    catl::core::emit_status("walking TX tree");

    auto tx_walk = co_await with_peer(
        tx_ledger_seq,
        "walk transaction tree",
        [ctx, ledger_hash, tx_ledger_seq, target_header, tx_hash, walk_tx](
            PeerGenerationPin peer) -> boost::asio::awaitable<WalkResult> {
            co_return co_await walk_tx(
                ledger_hash,
                tx_ledger_seq,
                target_header.tx_hash(),
                tx_hash,
                std::move(peer));
        });

    if (!tx_walk.found())
        throw std::runtime_error("Transaction not found in TX tree");

    // ── Step 8: Build abbreviated TX tree ──
    catl::shamap::SHAMapOptions opts;
    opts.tree_collapse_impl = catl::shamap::TreeCollapseImpl::leafs_only;
    opts.reference_hash_impl =
        catl::shamap::ReferenceHashImpl::recursive_simple;
    AbbrevMap abbrev(catl::shamap::tnTRANSACTION_MD, opts);

    auto const& tx_leaf = tx_walk.leaf();
    catl::shamap::SHAMapNodeID tx_leaf_pos(
        Hash256(tx_leaf.leaf_nid.id.data()), tx_leaf.leaf_nid.depth);
    Slice leaf_item_data(
        tx_leaf.leaf_data.data(), tx_leaf.leaf_data.size() - 32);
    boost::intrusive_ptr<MmapItem> leaf_item(
        OwnedItem::create(tx_hash, leaf_item_data));
    abbrev.set_item_at(tx_leaf_pos, leaf_item);

    int placed = 0;
    for (auto const& p : tx_walk.placeholders())
    {
        catl::shamap::SHAMapNodeID ph_pos(
            Hash256(p.nodeid.id.data()), p.nodeid.depth);
        if (abbrev.needs_placeholder(ph_pos))
        {
            abbrev.set_placeholder(ph_pos, p.hash);
            placed++;
        }
    }

    auto abbrev_hash = abbrev.get_hash();
    auto expected_tx_hash = target_header.tx_hash();
    bool verified = (abbrev_hash == expected_tx_hash);
    PLOGI(log_, "  Abbreviated tree: ", placed, " placeholders");
    PLOGI(log_, "  TX tree hash: ", verified ? "VERIFIED" : "MISMATCH");
    catl::core::emit_status(
        std::string("TX tree: ") + (verified ? "verified" : "MISMATCH") + " (" +
        std::to_string(placed) + " placeholders)");

    // ── Emit target header + TX tree ──
    co_await emit_step(make_header(target_hdr));
    co_await emit_step(make_trie_data(abbrev, true, TrieData::TreeType::tx));

    co_return BuildResult{
        std::move(proof_chain), vl_data.publisher_key_hex, tx_ledger_seq};
}

}  // namespace xprv
