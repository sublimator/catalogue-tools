#include "xproof/proof-builder.h"
#include "xproof/hex-utils.h"
#include "xproof/skip-list.h"
#include "xproof/validation-collector.h"

#include <catl/core/logger.h>
#include <catl/peer-client/peer-client-coro.h>
#include <catl/peer-client/tree-walker.h>
#include <catl/rpc-client/rpc-client-coro.h>
#include <catl/shamap/shamap-hashprefix.h>
#include <catl/shamap/shamap-nodeid.h>
#include <catl/shamap/shamap.h>
#include <catl/vl-client/vl-client-coro.h>
#include <catl/xdata-json/parse_leaf.h>
#include <catl/xdata-json/parse_transaction.h>

#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

using namespace catl::peer_client;
using xproof::hash_from_hex;
using xproof::upper_hex;
using xproof::skip_list_key;
using xproof::bytes_hex;

namespace xproof {

static LogPartition log_("xproof", LogLevel::INFO);

/// Build a proof-format on_leaf callback for trie_json.
static catl::shamap::LeafJsonCallback
make_proof_leaf_callback(
    catl::xdata::Protocol const& protocol,
    bool is_tx_tree)
{
    return [&protocol, is_tx_tree](MmapItem const& item)
               -> boost::json::value {
        boost::json::array arr;
        arr.emplace_back(upper_hex(item.key().to_hash()));

        try
        {
            std::vector<uint8_t> buf(
                item.slice().data(),
                item.slice().data() + item.slice().size());
            buf.insert(
                buf.end(), item.key().data(), item.key().data() + 32);
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

/// Parse SLE leaf data and check if a hash exists in sfHashes.
static bool
sle_hashes_contain(
    Slice leaf_data,
    Hash256 const& needle,
    catl::xdata::Protocol const& protocol)
{
    try
    {
        auto json =
            catl::xdata::json::parse_leaf(leaf_data, protocol, false);
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

boost::asio::awaitable<BuildResult>
build_proof(
    boost::asio::io_context& io,
    std::string const& rpc_host,
    uint16_t rpc_port,
    std::string const& peer_host,
    uint16_t peer_port,
    std::string const& tx_hash_str)
{
    auto tx_hash = hash_from_hex(tx_hash_str);

    // Helper types
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

    using AbbrevMap =
        catl::shamap::SHAMapT<catl::shamap::AbbreviatedTreeTraits>;

    struct StateProofResult
    {
        bool verified;
        AbbrevMap tree;
    };

    auto walk_state = [&](std::shared_ptr<PeerClient> c,
                          Hash256 const& ledger_hash,
                          Hash256 const& key)
        -> boost::asio::awaitable<StateWalkResult> {
        StateWalkResult r;
        TreeWalker walker(c, ledger_hash, TreeWalkState::TreeType::state);
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

    auto protocol =
        catl::xdata::Protocol::load_embedded_xrpl_protocol();

    auto build_state_proof =
        [&](StateWalkResult const& wr,
            Hash256 const& key,
            Hash256 const& expected_account_hash) -> StateProofResult {
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
            log_,
            "  State tree: ",
            ok ? "VERIFIED" : "MISMATCH",
            " (",
            wr.placeholders.size(),
            " placeholders)");
        if (!ok)
        {
            PLOGE(log_, "    expected: ", expected_account_hash.hex());
            PLOGE(log_, "    computed: ", computed.hex());
        }
        return {ok, std::move(abbrev)};
    };

    // ── Step 1: RPC — look up tx ──
    catl::rpc::RpcClient rpc(io, rpc_host, rpc_port);
    auto tx_result = co_await catl::rpc::co_tx(rpc, tx_hash_str);
    auto const& tx_obj = tx_result.as_object();

    uint32_t tx_ledger_seq = 0;
    if (tx_obj.contains("ledger_index"))
    {
        tx_ledger_seq =
            tx_obj.at("ledger_index").to_number<uint32_t>();
    }
    if (tx_ledger_seq == 0)
    {
        throw std::runtime_error("tx not found or no ledger_index");
    }

    PLOGI(
        log_,
        "TX ",
        tx_hash_str.substr(0, 16),
        "... is in ledger ",
        tx_ledger_seq);

    // ── Step 1b: Fetch VL ──
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
    PLOGI(log_, "Connected to peer, peer at ledger ", peer_seq);

    ValidationCollector val_collector(protocol);
    client->set_unsolicited_handler(
        [&val_collector](
            uint16_t type, std::vector<uint8_t> const& data) {
            val_collector.on_packet(type, data);
        });
    PLOGI(log_, "Listening for validations...");

    // Wait for VL
    {
        boost::asio::steady_timer vl_timer(io, std::chrono::seconds(10));
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
        PLOGW(log_, "VL fetch failed: ", vl_error);
    }

    // Wait for quorum
    if (vl_data && !val_collector.quorum_reached)
    {
        PLOGI(log_, "Waiting for validation quorum...");
        boost::asio::steady_timer quorum_timer(
            io, std::chrono::seconds(15));
        while (!val_collector.quorum_reached)
        {
            quorum_timer.expires_after(std::chrono::milliseconds(200));
            boost::system::error_code ec;
            co_await quorum_timer.async_wait(
                boost::asio::redirect_error(
                    boost::asio::use_awaitable, ec));
            if (ec)
            {
                break;
            }
        }
    }

    // Determine anchor
    uint32_t anchor_seq;
    Hash256 anchor_hash;
    if (val_collector.quorum_reached)
    {
        anchor_hash = val_collector.quorum_hash;
        anchor_seq =
            val_collector.quorum_validations()[0].ledger_seq;
        PLOGI(
            log_,
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
            log_,
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
        log_,
        "Anchor ledger ",
        anchor_hdr.seq(),
        " hash=",
        anchor_hash.hex().substr(0, 16),
        "...");

    // ── Step 3: Determine hop path ──
    uint32_t distance = anchor_hdr.seq() - tx_ledger_seq;
    PLOGI(log_, "Distance: ", distance, " ledgers");

    Hash256 target_ledger_hash;
    bool need_flag_hop = (distance > 256);

    std::vector<std::tuple<Hash256, StateProofResult>> state_proofs;

    if (!need_flag_hop)
    {
        PLOGI(log_, "Short skip list (within 256)");
        auto skip_key_val = skip_list_key();

        auto wr = co_await walk_state(client, anchor_hash, skip_key_val);
        if (!wr.found)
        {
            throw std::runtime_error(
                "Short skip list not found in state tree");
        }

        auto sp = build_state_proof(
            wr, skip_key_val, anchor_header.account_hash());
        state_proofs.emplace_back(skip_key_val, std::move(sp));

        auto target_hdr =
            co_await co_get_ledger_header(client, tx_ledger_seq);
        target_ledger_hash = target_hdr.ledger_hash256();

        Slice sle_leaf(wr.leaf_data.data(), wr.leaf_data.size());
        if (!sle_hashes_contain(sle_leaf, target_ledger_hash, protocol))
        {
            PLOGW(log_, "  Target hash not found in short skip list!");
        }
        else
        {
            PLOGI(log_, "  Target hash confirmed in short skip list");
        }
    }
    else
    {
        PLOGI(log_, "Long skip list (2-hop, distance=", distance, ")");

        uint32_t flag_seq = ((tx_ledger_seq + 255) / 256) * 256;
        PLOGI(log_, "  Flag ledger: ", flag_seq);

        auto long_skip_key = skip_list_key(flag_seq);
        auto wr1 =
            co_await walk_state(client, anchor_hash, long_skip_key);
        if (!wr1.found)
        {
            throw std::runtime_error(
                "Long skip list not found in state tree");
        }

        {
            auto sp = build_state_proof(
                wr1, long_skip_key, anchor_header.account_hash());
            state_proofs.emplace_back(long_skip_key, std::move(sp));
        }

        auto flag_hdr =
            co_await co_get_ledger_header(client, flag_seq);
        auto flag_hash = flag_hdr.ledger_hash256();
        auto flag_header = flag_hdr.header();

        Slice long_sle(wr1.leaf_data.data(), wr1.leaf_data.size());
        if (!sle_hashes_contain(long_sle, flag_hash, protocol))
        {
            PLOGW(log_, "  Flag hash not found in long skip list!");
        }
        else
        {
            PLOGI(log_, "  Flag hash confirmed in long skip list");
        }

        auto short_skip_key = skip_list_key();
        auto wr2 =
            co_await walk_state(client, flag_hash, short_skip_key);
        if (!wr2.found)
        {
            throw std::runtime_error(
                "Short skip list not found in flag ledger state tree");
        }

        {
            auto sp = build_state_proof(
                wr2, short_skip_key, flag_header.account_hash());
            state_proofs.emplace_back(short_skip_key, std::move(sp));
        }

        auto target_hdr =
            co_await co_get_ledger_header(client, tx_ledger_seq);
        target_ledger_hash = target_hdr.ledger_hash256();

        Slice short_sle(wr2.leaf_data.data(), wr2.leaf_data.size());
        if (!sle_hashes_contain(
                short_sle, target_ledger_hash, protocol))
        {
            PLOGW(log_, "  Target hash not found in flag's short skip list!");
        }
        else
        {
            PLOGI(log_, "  Target hash confirmed in flag's short skip list");
        }
    }

    // ── Step 4: Get target ledger header ──
    auto target_hdr =
        co_await co_get_ledger_header(client, tx_ledger_seq);
    auto target_header = target_hdr.header();

    // ── Step 5: Walk TX tree ──
    auto ledger_hash = target_hdr.ledger_hash256();
    PLOGI(
        log_,
        "Walking TX tree for ",
        tx_hash_str.substr(0, 16),
        "...");

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
                placeholders.push_back(
                    {catl::shamap::SHAMapNodeID(nid), hash});
            });

        walker.set_on_leaf([&](std::span<const uint8_t> nid,
                               Hash256 const&,
                               std::span<const uint8_t> data) {
            leaf_nid = catl::shamap::SHAMapNodeID(nid);
            leaf_data.assign(data.begin(), data.end());
            found_leaf = true;
            PLOGI(
                log_,
                "  Found target leaf at depth ",
                static_cast<int>(leaf_nid.depth()));
        });

        co_await walker.walk();
    }

    if (!found_leaf)
    {
        throw std::runtime_error("Transaction not found in TX tree");
    }

    // ── Step 6: Build abbreviated tree ──
    PLOGI(log_, "Building abbreviated tree...");

    catl::shamap::SHAMapOptions opts;
    opts.tree_collapse_impl =
        catl::shamap::TreeCollapseImpl::leafs_only;
    opts.reference_hash_impl =
        catl::shamap::ReferenceHashImpl::recursive_simple;
    AbbrevMap abbrev(catl::shamap::tnTRANSACTION_MD, opts);

    Slice leaf_item_data(leaf_data.data(), leaf_data.size() - 32);
    boost::intrusive_ptr<MmapItem> leaf_item(
        OwnedItem::create(tx_hash, leaf_item_data));
    abbrev.set_item_at(leaf_nid, leaf_item);

    int placed = 0;
    for (auto& p : placeholders)
    {
        if (abbrev.needs_placeholder(p.nid))
        {
            abbrev.set_placeholder(p.nid, p.hash);
            placed++;
        }
    }

    auto abbrev_hash = abbrev.get_hash();
    auto expected_tx_hash = target_header.tx_hash();
    bool verified = (abbrev_hash == expected_tx_hash);
    PLOGI(log_, "  Abbreviated tree: ", placed, " placeholders");
    PLOGI(
        log_,
        "  TX tree hash: ",
        verified ? "VERIFIED" : "MISMATCH");

    // ── Step 7: Build proof chain ──
    ProofChain proof_chain;

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

    auto make_trie = [&](auto& tree, bool is_tx) -> boost::json::value {
        catl::shamap::TrieJsonOptions trie_opts;
        trie_opts.on_leaf = make_proof_leaf_callback(protocol, is_tx);
        return tree.get_root()->trie_json(
            trie_opts, tree.get_options());
    };

    // Anchor
    {
        AnchorData anchor;
        anchor.ledger_hash = anchor_hash;
        anchor.ledger_index = anchor_hdr.seq();

        if (vl_data)
        {
            anchor.publisher_key_hex = vl_data->publisher_key_hex;
            anchor.publisher_manifest =
                vl_data->publisher_manifest.raw;
            anchor.blob = vl_data->blob_bytes;
            anchor.blob_signature = vl_data->blob_signature;
        }

        auto const& qvals = val_collector.quorum_validations();
        for (auto const& v : qvals)
        {
            anchor.validations[bytes_hex(v.signing_key)] = v.raw;
        }

        proof_chain.steps.push_back(std::move(anchor));
    }

    // Anchor header
    proof_chain.steps.push_back(make_header(anchor_hdr));

    // State proofs
    if (need_flag_hop && state_proofs.size() >= 2)
    {
        {
            auto& [key, sp] = state_proofs[0];
            TrieData trie;
            trie.tree = TrieData::TreeType::state;
            trie.trie_json = make_trie(sp.tree, false);
            proof_chain.steps.push_back(std::move(trie));
        }
        {
            uint32_t flag_seq =
                ((tx_ledger_seq + 255) / 256) * 256;
            auto flag_hdr2 =
                co_await co_get_ledger_header(client, flag_seq);
            proof_chain.steps.push_back(make_header(flag_hdr2));
        }
        {
            auto& [key, sp] = state_proofs[1];
            TrieData trie;
            trie.tree = TrieData::TreeType::state;
            trie.trie_json = make_trie(sp.tree, false);
            proof_chain.steps.push_back(std::move(trie));
        }
    }
    else if (!state_proofs.empty())
    {
        auto& [key, sp] = state_proofs[0];
        TrieData trie;
        trie.tree = TrieData::TreeType::state;
        trie.trie_json = make_trie(sp.tree, false);
        proof_chain.steps.push_back(std::move(trie));
    }

    // Target header
    proof_chain.steps.push_back(make_header(target_hdr));

    // TX tree proof
    {
        TrieData trie;
        trie.tree = TrieData::TreeType::tx;
        trie.trie_json = make_trie(abbrev, true);
        proof_chain.steps.push_back(std::move(trie));
    }

    co_return BuildResult{
        std::move(proof_chain),
        vl_data ? vl_data->publisher_key_hex : ""};
}

}  // namespace xproof
