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
#include <catl/vl-client/vl-client-coro.h>
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

            if (is_tx_tree)
            {
                arr.emplace_back(catl::xdata::parse_transaction(
                    full,
                    protocol,
                    {.includes_prefix = false,
                     .json_opts = {.ascii_hints = false}}));
            }
            else
            {
                arr.emplace_back(catl::xdata::parse_leaf(
                    full,
                    protocol,
                    {.includes_prefix = false,
                     .json_opts = {.ascii_hints = false}}));
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

// Legacy self-contained build_proof removed — all callers use
// build_proof(BuildServices) via ProofEngine.

#if 0  // DEAD CODE — kept temporarily for reference, will be deleted
boost::asio::awaitable<BuildResult>
build_proof_DEAD(
    boost::asio::io_context& io,
    std::string const& rpc_host,
    uint16_t rpc_port,
    std::string const& peer_host,
    uint16_t peer_port,
    std::string const& peer_cache_path,
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

    struct PlaceholderEntry
    {
        catl::shamap::SHAMapNodeID nid;
        Hash256 hash;
    };

    struct TxWalkResult
    {
        bool found = false;
        catl::shamap::SHAMapNodeID leaf_nid;
        std::vector<uint8_t> leaf_data;
        std::vector<PlaceholderEntry> placeholders;
    };

    auto walk_state =
        [&](std::shared_ptr<PeerClient> c,
            Hash256 const& ledger_hash,
            Hash256 const& key) -> boost::asio::awaitable<StateWalkResult> {
        StateWalkResult r;
        TreeWalker walker(c, ledger_hash, TreeWalkState::TreeType::state);
        walker.set_speculative_depth(8);
        walker.add_target(key);
        walker.set_on_placeholder(
            [&](std::span<const uint8_t> nid, Hash256 const& h) {
                r.placeholders.push_back({catl::shamap::SHAMapNodeID(nid), h});
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

    auto protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();

    auto build_state_proof =
        [&](StateWalkResult const& wr,
            Hash256 const& key,
            Hash256 const& expected_account_hash) -> StateProofResult {
        catl::shamap::SHAMapOptions opts;
        opts.tree_collapse_impl = catl::shamap::TreeCollapseImpl::leafs_only;
        opts.reference_hash_impl =
            catl::shamap::ReferenceHashImpl::recursive_simple;
        AbbrevMap abbrev(catl::shamap::tnACCOUNT_STATE, opts);

        Slice item_data(wr.leaf_data.data(), wr.leaf_data.size() - 32);
        boost::intrusive_ptr<MmapItem> item(OwnedItem::create(key, item_data));
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
        tx_ledger_seq = tx_obj.at("ledger_index").to_number<uint32_t>();
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
    //@@start fetch-vl
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
    //@@end fetch-vl

    // ── Step 2: Peer — connect and collect validations ──
    PeerSetOptions peer_options;
    peer_options.endpoint_cache_path = peer_cache_path;
    auto peers = PeerSet::create(io, peer_options);
    peers->start();
    peers->prioritize_ledger(tx_ledger_seq);

    ValidationCollector val_collector(protocol);
    peers->set_unsolicited_handler(
        [&val_collector](uint16_t type, std::vector<uint8_t> const& data) {
            val_collector.on_packet(type, data);
        });

    struct PeerRetryPolicy
    {
        int per_peer_retries = 2;
        std::chrono::seconds cooldown{30};
        int wait_timeout_secs = 15;
    };

    PeerRetryPolicy peer_retry_policy;
    std::unordered_map<std::string, int> peer_retry_counts;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point>
        peer_cooldowns;

    auto peer_covers_ledger = [](std::shared_ptr<PeerClient> const& peer,
                                 uint32_t ledger_seq) {
        if (!peer || !peer->is_ready())
            return false;
        auto const first = peer->peer_first_seq();
        auto const last = peer->peer_last_seq();
        return first != 0 && last != 0 && ledger_seq >= first &&
            ledger_seq <= last;
    };

    auto collect_excluded_peers = [&]() {
        std::unordered_set<std::string> excluded;
        auto const now = std::chrono::steady_clock::now();
        for (auto it = peer_cooldowns.begin(); it != peer_cooldowns.end();)
        {
            if (it->second <= now)
            {
                it = peer_cooldowns.erase(it);
                continue;
            }
            excluded.insert(it->first);
            ++it;
        }
        return excluded;
    };

    auto acquire_peer = [&](std::shared_ptr<PeerClient>& current,
                            std::optional<uint32_t> required_ledger,
                            std::string const& purpose)
        -> boost::asio::awaitable<std::shared_ptr<PeerClient>> {
        for (;;)
        {
            auto excluded = collect_excluded_peers();
            // Merge in per-ledger failures from peer history
            if (required_ledger)
            {
                auto ledger_excluded = ctx->excluded_for(*required_ledger);
                excluded.insert(
                    ledger_excluded.begin(), ledger_excluded.end());
            }

            if (current && current->is_ready() &&
                !excluded.count(current->endpoint()) &&
                (!required_ledger ||
                 peer_covers_ledger(current, *required_ledger)))
            {
                co_return current;
            }

            std::shared_ptr<PeerClient> found;
            if (required_ledger)
            {
                found = co_await peers->wait_for_peer(
                    *required_ledger,
                    peer_retry_policy.wait_timeout_secs,
                    excluded);
            }
            else
            {
                found = co_await peers->wait_for_any_peer(
                    peer_retry_policy.wait_timeout_secs, excluded);
            }

            if (found)
            {
                current = found;
                if (!current->endpoint().empty())
                {
                    peer_retry_counts[current->endpoint()] = 0;
                }
                PLOGI(
                    log_, "Using peer ", current->endpoint(), " for ", purpose);
                co_return current;
            }

            if (required_ledger)
            {
                PLOGI(
                    log_,
                    "Still waiting for a peer with ledger ",
                    *required_ledger,
                    " for ",
                    purpose,
                    " (",
                    excluded.size(),
                    " peers cooled down)");
            }
            else
            {
                PLOGI(
                    log_,
                    "Still waiting for a ready peer for ",
                    purpose,
                    " (",
                    excluded.size(),
                    " peers cooled down)");
            }
        }
    };

    auto is_retryable_peer_error = [](PeerClientException const& e) {
        switch (e.error)
        {
            case Error::Timeout:
            case Error::Disconnected:
            case Error::NoLedger:
            case Error::NoNode:
                return true;
            default:
                return false;
        }
    };

    // Global deadline for any single with_peer_failover operation.
    // Prevents infinite retry loops when all peers can't serve the data.
    static constexpr auto kFailoverDeadline = std::chrono::seconds(60);

    auto with_peer_failover = [&](std::shared_ptr<PeerClient>& current,
                                  std::optional<uint32_t> required_ledger,
                                  std::string const& purpose,
                                  auto op) -> decltype(op(current)) {
        auto const deadline =
            std::chrono::steady_clock::now() + kFailoverDeadline;

        for (;;)
        {
            if (std::chrono::steady_clock::now() > deadline)
            {
                throw PeerClientException(
                    Error::Timeout,
                    "with_peer_failover deadline exceeded for: " + purpose);
            }

            current = co_await acquire_peer(current, required_ledger, purpose);

            try
            {
                co_return co_await op(current);
            }
            catch (PeerClientException const& e)
            {
                if (!is_retryable_peer_error(e))
                {
                    PLOGE(
                        log_,
                        purpose,
                        " failed on ",
                        current ? current->endpoint() : "<none>",
                        ": ",
                        e.what());
                    throw;
                }

                auto const endpoint =
                    current ? current->endpoint() : std::string{};
                if (endpoint.empty())
                {
                    PLOGW(
                        log_,
                        purpose,
                        " failed on an unnamed peer: ",
                        e.what(),
                        " — trying another peer");
                    current.reset();
                    continue;
                }

                auto& failures = peer_retry_counts[endpoint];
                ++failures;

                if (failures <= peer_retry_policy.per_peer_retries)
                {
                    PLOGW(
                        log_,
                        purpose,
                        " failed on ",
                        endpoint,
                        ": ",
                        e.what(),
                        " (retry ",
                        failures,
                        "/",
                        peer_retry_policy.per_peer_retries,
                        " on same peer)");
                    continue;
                }

                failures = 0;
                peer_cooldowns[endpoint] = std::chrono::steady_clock::now() +
                    peer_retry_policy.cooldown;

                // Record this peer as failed for this ledger so it's
                // excluded from future attempts at the same ledger
                // across all retry loops in this prove.
                if (required_ledger)
                    ctx->note_peer_failure(endpoint, *required_ledger);

                PLOGW(
                    log_,
                    purpose,
                    " failed on ",
                    endpoint,
                    ": ",
                    e.what(),
                    " — cooling down peer for ",
                    peer_retry_policy.cooldown.count(),
                    "s and trying another peer");
                current.reset();
            }
        }
    };

    // Start archival discovery as soon as the target ledger is known so crawl
    // and ranked bootstrap can overlap with the first peer connect, VL fetch,
    // and validation quorum wait.
    peers->bootstrap();

    // Try to connect to the specified peer — may fail (503 etc)
    // but redirect IPs get fed into the tracker automatically.
    auto initial_client = co_await peers->try_connect(peer_host, peer_port);
    if (initial_client)
    {
        PLOGI(
            log_,
            "Connected to peer, peer at ledger ",
            initial_client->peer_ledger_seq());
    }
    else
    {
        PLOGW(
            log_,
            "Initial peer ",
            peer_host,
            ":",
            peer_port,
            " failed — continuing with background bootstrap...");
        auto found_peer = co_await peers->wait_for_any_peer();
        if (!found_peer)
        {
            throw std::runtime_error(
                "No peers available. Check network connectivity.");
        }
        initial_client = found_peer;
    }
    auto client = initial_client;
    PLOGI(log_, "Listening for validations...");

    // Wait for VL
    {
        boost::asio::steady_timer vl_timer(io, std::chrono::seconds(10));
        while (!vl_data && vl_error.empty())
        {
            vl_timer.expires_after(std::chrono::milliseconds(100));
            boost::system::error_code ec;
            co_await vl_timer.async_wait(
                boost::asio::redirect_error(boost::asio::use_awaitable, ec));
        }
    }

    if (vl_data)
    {
        val_collector.set_unl(vl_data->validators);
    }
    else
    {
        throw std::runtime_error("VL fetch failed: " + vl_error);
    }

    // Wait for quorum
    if (vl_data && !val_collector.quorum_reached)
    {
        PLOGI(log_, "Waiting for validation quorum...");
        boost::asio::steady_timer quorum_timer(io, std::chrono::seconds(15));
        while (!val_collector.quorum_reached)
        {
            quorum_timer.expires_after(std::chrono::milliseconds(200));
            boost::system::error_code ec;
            co_await quorum_timer.async_wait(
                boost::asio::redirect_error(boost::asio::use_awaitable, ec));
            if (ec)
            {
                break;
            }
        }
    }

    // Determine anchor
    auto anchor_validations = val_collector.get_quorum(kAnchorQuorumPercent);
    if (anchor_validations.empty())
    {
        throw std::runtime_error(
            "Timed out waiting for validation quorum at " +
            std::to_string(kAnchorQuorumPercent) + "%");
    }

    uint32_t anchor_seq = anchor_validations.front().ledger_seq;
    Hash256 anchor_hash = anchor_validations.front().ledger_hash;
    PLOGI(
        log_,
        "Using validated anchor: seq=",
        anchor_seq,
        " hash=",
        anchor_hash.hex().substr(0, 16),
        "... (",
        anchor_validations.size(),
        "/",
        val_collector.unl_size,
        " validators in proof)");

    auto anchor_hdr = co_await with_peer_failover(
        client,
        anchor_seq,
        "fetch anchor ledger header",
        [anchor_seq](std::shared_ptr<PeerClient> peer)
            -> boost::asio::awaitable<LedgerHeaderResult> {
            co_return co_await co_get_ledger_header(peer, anchor_seq);
        });
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

    // ── Step 2b: Find a peer with the target ledger range ──
    client = co_await acquire_peer(
        client, tx_ledger_seq, "proof fetches for target ledger");

    // ── Step 3: Determine hop path ──
    uint32_t distance = anchor_hdr.seq() - tx_ledger_seq;
    PLOGI(log_, "Distance: ", distance, " ledgers");

    Hash256 target_ledger_hash;
    std::optional<LedgerHeaderResult> flag_hdr_result;
    std::optional<LedgerHeaderResult> target_hdr_result;
    bool need_flag_hop = (distance > 256);

    std::vector<std::tuple<Hash256, StateProofResult>> state_proofs;

    if (!need_flag_hop)
    {
        PLOGI(log_, "Short skip list (within 256)");
        auto skip_key_val = skip_list_key();

        auto wr = co_await with_peer_failover(
            client,
            anchor_seq,
            "walk short skip list",
            [&](std::shared_ptr<PeerClient> peer)
                -> boost::asio::awaitable<StateWalkResult> {
                co_return co_await walk_state(peer, anchor_hash, skip_key_val);
            });
        if (!wr.found)
        {
            throw std::runtime_error("Short skip list not found in state tree");
        }

        auto sp =
            build_state_proof(wr, skip_key_val, anchor_header.account_hash());
        state_proofs.emplace_back(skip_key_val, std::move(sp));

        target_hdr_result = co_await with_peer_failover(
            client,
            tx_ledger_seq,
            "fetch target ledger header from short skip list",
            [&](std::shared_ptr<PeerClient> peer)
                -> boost::asio::awaitable<LedgerHeaderResult> {
                co_return co_await co_get_ledger_header(peer, tx_ledger_seq);
            });
        target_ledger_hash = target_hdr_result->ledger_hash256();

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

        uint32_t flag_seq = flag_ledger_for(tx_ledger_seq);
        PLOGI(log_, "  Flag ledger: ", flag_seq);

        auto long_skip_key = skip_list_key(flag_seq);
        auto wr1 = co_await with_peer_failover(
            client,
            anchor_seq,
            "walk long skip list",
            [&](std::shared_ptr<PeerClient> peer)
                -> boost::asio::awaitable<StateWalkResult> {
                co_return co_await walk_state(peer, anchor_hash, long_skip_key);
            });
        if (!wr1.found)
        {
            throw std::runtime_error("Long skip list not found in state tree");
        }

        {
            auto sp = build_state_proof(
                wr1, long_skip_key, anchor_header.account_hash());
            state_proofs.emplace_back(long_skip_key, std::move(sp));
        }

        flag_hdr_result = co_await with_peer_failover(
            client, flag_seq, "fetch flag ledger header",
            [&](std::shared_ptr<PeerClient> peer)
                -> boost::asio::awaitable<LedgerHeaderResult> {
                co_return co_await node_cache->get_header(flag_seq, peers, peer);
            });
        auto flag_hash = flag_hdr_result->ledger_hash256();
        auto flag_header = flag_hdr_result->header();

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
        auto wr2 = co_await with_peer_failover(
            client,
            flag_seq,
            "walk flag short skip list",
            [&](std::shared_ptr<PeerClient> peer)
                -> boost::asio::awaitable<StateWalkResult> {
                co_return co_await walk_state(peer, flag_hash, short_skip_key);
            });
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

        target_hdr_result = co_await with_peer_failover(
            client,
            tx_ledger_seq,
            "fetch target ledger header from flag skip list",
            [&](std::shared_ptr<PeerClient> peer)
                -> boost::asio::awaitable<LedgerHeaderResult> {
                co_return co_await co_get_ledger_header(peer, tx_ledger_seq);
            });
        target_ledger_hash = target_hdr_result->ledger_hash256();

        Slice short_sle(wr2.leaf_data.data(), wr2.leaf_data.size());
        if (!sle_hashes_contain(short_sle, target_ledger_hash, protocol))
        {
            PLOGW(log_, "  Target hash not found in flag's short skip list!");
        }
        else
        {
            PLOGI(log_, "  Target hash confirmed in flag's short skip list");
        }
    }

    // ── Step 4: Get target ledger header ──
    if (!target_hdr_result)
    {
        target_hdr_result =
            co_await node_cache->get_header(tx_ledger_seq, peers, client);
    }
    auto const& target_hdr = *target_hdr_result;
    auto target_header = target_hdr.header();

    // ── Step 5: Walk TX tree ──
    auto ledger_hash = target_hdr.ledger_hash256();
    PLOGI(log_, "Walking TX tree for ", tx_hash_str.substr(0, 16), "...");

    auto tx_walk = co_await with_peer_failover(
        client,
        tx_ledger_seq,
        "walk transaction tree",
        [&](std::shared_ptr<PeerClient> peer)
            -> boost::asio::awaitable<TxWalkResult> {
            TxWalkResult result;
            TreeWalker walker(peer, ledger_hash, TreeWalkState::TreeType::tx);
            walker.set_speculative_depth(4);
            walker.add_target(tx_hash);

            walker.set_on_placeholder(
                [&](std::span<const uint8_t> nid, Hash256 const& hash) {
                    result.placeholders.push_back(
                        {catl::shamap::SHAMapNodeID(nid), hash});
                });

            walker.set_on_leaf([&](std::span<const uint8_t> nid,
                                   Hash256 const&,
                                   std::span<const uint8_t> data) {
                result.leaf_nid = catl::shamap::SHAMapNodeID(nid);
                result.leaf_data.assign(data.begin(), data.end());
                result.found = true;
                PLOGI(
                    log_,
                    "  Found target leaf at depth ",
                    static_cast<int>(result.leaf_nid.depth()));
            });

            co_await walker.walk();
            co_return result;
        });

    if (!tx_walk.found)
    {
        throw std::runtime_error("Transaction not found in TX tree");
    }

    // ── Step 6: Build abbreviated tree ──
    PLOGI(log_, "Building abbreviated tree...");

    catl::shamap::SHAMapOptions opts;
    opts.tree_collapse_impl = catl::shamap::TreeCollapseImpl::leafs_only;
    opts.reference_hash_impl =
        catl::shamap::ReferenceHashImpl::recursive_simple;
    AbbrevMap abbrev(catl::shamap::tnTRANSACTION_MD, opts);

    Slice leaf_item_data(
        tx_walk.leaf_data.data(), tx_walk.leaf_data.size() - 32);
    boost::intrusive_ptr<MmapItem> leaf_item(
        OwnedItem::create(tx_hash, leaf_item_data));
    abbrev.set_item_at(tx_walk.leaf_nid, leaf_item);

    int placed = 0;
    for (auto& p : tx_walk.placeholders)
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
    PLOGI(log_, "  TX tree hash: ", verified ? "VERIFIED" : "MISMATCH");

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

    auto make_trie_data =
        [&](auto& tree, bool is_tx, TrieData::TreeType tree_type) -> TrieData {
        TrieData trie;
        trie.tree = tree_type;
        // JSON trie (with decoded leaf data for human readability)
        catl::shamap::TrieJsonOptions trie_opts;
        trie_opts.on_leaf = make_proof_leaf_callback(protocol, is_tx);
        trie.trie_json =
            tree.get_root()->trie_json(trie_opts, tree.get_options());
        // Binary trie (raw leaf data for compact encoding)
        trie.trie_binary = tree.trie_binary();
        return trie;
    };

    // Anchor
    {
        AnchorData anchor;
        anchor.ledger_hash = anchor_hash;
        anchor.ledger_index = anchor_hdr.seq();

        if (vl_data)
        {
            anchor.publisher_key_hex = vl_data->publisher_key_hex;
            anchor.publisher_manifest = vl_data->publisher_manifest.raw;
            anchor.blob = vl_data->blob_bytes;
            anchor.blob_signature = vl_data->blob_signature;
        }

        for (auto const& v : anchor_validations)
        {
            anchor.validations[v.signing_key_hex] = v.raw;
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
            proof_chain.steps.push_back(
                make_trie_data(sp.tree, false, TrieData::TreeType::state));
        }
        {
            uint32_t flag_seq = flag_ledger_for(tx_ledger_seq);
            if (!flag_hdr_result)
            {
                flag_hdr_result = co_await with_peer_failover(
                    client,
                    flag_seq,
                    "re-fetch flag ledger header for proof chain",
                    [&](std::shared_ptr<PeerClient> peer)
                        -> boost::asio::awaitable<LedgerHeaderResult> {
                        co_return co_await co_get_ledger_header(peer, flag_seq);
                    });
            }
            proof_chain.steps.push_back(make_header(*flag_hdr_result));
        }
        {
            auto& [key, sp] = state_proofs[1];
            proof_chain.steps.push_back(
                make_trie_data(sp.tree, false, TrieData::TreeType::state));
        }
    }
    else if (!state_proofs.empty())
    {
        auto& [key, sp] = state_proofs[0];
        proof_chain.steps.push_back(
            make_trie_data(sp.tree, false, TrieData::TreeType::state));
    }

    // Target header
    proof_chain.steps.push_back(make_header(target_hdr));

    // TX tree proof
    proof_chain.steps.push_back(
        make_trie_data(abbrev, true, TrieData::TreeType::tx));

    co_return BuildResult{
        std::move(proof_chain),
        vl_data ? vl_data->publisher_key_hex : "",
        tx_ledger_seq};
}

#endif  // DEAD CODE

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

        // Peer retry state
        struct PeerRetryPolicy
        {
            int per_peer_retries = 2;
            std::chrono::seconds cooldown{30};
            int wait_timeout_secs = 15;
        };
        PeerRetryPolicy peer_retry_policy;
        std::unordered_map<std::string, int> peer_retry_counts;
        std::unordered_map<std::string, std::chrono::steady_clock::time_point>
            peer_cooldowns;

        // Per-prove peer history — tracks what each peer has failed
        // to serve so we don't hammer the same peer for the same
        // ledger across retry loops. Keyed by endpoint (host:port).
        // Grows during the prove, never resets.
        struct PeerRecord
        {
            std::unordered_set<uint32_t> failed_ledgers;
            // Room to grow: timeout_count, last_failure_at, etc.
        };
        std::unordered_map<std::string, PeerRecord> peer_history;

        // Check if a peer has already failed for a specific ledger
        bool
        peer_failed_for(std::string const& ep, uint32_t seq) const
        {
            auto it = peer_history.find(ep);
            if (it == peer_history.end())
                return false;
            return it->second.failed_ledgers.count(seq) > 0;
        }

        // Record that a peer failed for a specific ledger
        void
        note_peer_failure(std::string const& ep, uint32_t seq)
        {
            peer_history[ep].failed_ledgers.insert(seq);
        }

        // Build an exclusion set for a specific ledger from history
        std::unordered_set<std::string>
        excluded_for(uint32_t seq) const
        {
            std::unordered_set<std::string> result;
            for (auto const& [ep, record] : peer_history)
            {
                if (record.failed_ledgers.count(seq))
                    result.insert(ep);
            }
            return result;
        }

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
        std::shared_ptr<PeerClient> client;
    };

    auto ctx = std::make_shared<ProveContext>();
    ctx->peers = svc.peers;
    ctx->node_cache = svc.node_cache;
    ctx->protocol = svc.protocol;
    ctx->cancel_token = svc.cancel_token;

    // Convenience reference — safe between co_awaits in this frame
    auto const& vl_data = svc.vl;

    // walk_state uses NodeCache::walk_to — content-addressed, cross-ledger
    // sharing, concurrent dedup. Needs the tree root hash (account_hash)
    // in addition to ledger_hash.
    auto walk_state = [ctx](Hash256 const& ledger_hash,
                            uint32_t ledger_seq,
                            Hash256 const& state_root_hash,
                            Hash256 const& key,
                            std::shared_ptr<PeerClient> peer =
                                nullptr) -> boost::asio::awaitable<WalkResult> {
        auto result = co_await ctx->node_cache->walk_to(
            state_root_hash,
            key,
            ledger_hash,
            ledger_seq,
            2,  // liAS_NODE
            ctx->peers,
            peer,
            ctx->cancel_token);
        if (!result.found)
            throw PeerClientException(Error::Timeout);
        co_return result;
    };

    // walk_tx same pattern but liTX_NODE
    auto walk_tx = [ctx](Hash256 const& ledger_hash,
                         uint32_t ledger_seq,
                         Hash256 const& tx_root_hash,
                         Hash256 const& key,
                         std::shared_ptr<PeerClient> peer =
                             nullptr) -> boost::asio::awaitable<WalkResult> {
        auto result = co_await ctx->node_cache->walk_to(
            tx_root_hash,
            key,
            ledger_hash,
            ledger_seq,
            1,  // liTX_NODE
            ctx->peers,
            peer,
            ctx->cancel_token);
        if (!result.found)
            throw PeerClientException(Error::Timeout);
        co_return result;
    };

    auto build_state_proof =
        [ctx](WalkResult const& wr,
              Hash256 const& key,
              Hash256 const& expected_account_hash) -> StateProofResult {
        catl::shamap::SHAMapOptions opts;
        opts.tree_collapse_impl = catl::shamap::TreeCollapseImpl::leafs_only;
        opts.reference_hash_impl =
            catl::shamap::ReferenceHashImpl::recursive_simple;
        AbbrevMap abbrev(catl::shamap::tnACCOUNT_STATE, opts);

        // Convert WalkResult leaf_nid (peer_client::SHAMapNodeID) to
        // shamap::SHAMapNodeID for set_item_at
        catl::shamap::SHAMapNodeID leaf_pos(
            Hash256(wr.leaf_nid.id.data()), wr.leaf_nid.depth);
        Slice item_data(wr.leaf_data.data(), wr.leaf_data.size() - 32);
        boost::intrusive_ptr<MmapItem> item(OwnedItem::create(key, item_data));
        abbrev.set_item_at(leaf_pos, item);

        for (auto& p : wr.placeholders)
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
            wr.placeholders.size(),
            " placeholders)");
        catl::core::emit_status(
            std::string("state tree: ") + (ok ? "verified" : "MISMATCH") +
            " (" + std::to_string(wr.placeholders.size()) + " placeholders)");
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
            "[", net_label, "] TX ",
            tx_hash_str.substr(0, 16),
            "... is in ledger ",
            tx_ledger_seq,
            " (RPC)");
    }
    else
    {
        PLOGI(
            log_,
            "[", net_label, "] TX ",
            tx_hash_str.substr(0, 16),
            "... is in ledger ",
            tx_ledger_seq,
            " (cached)");
    }

    // ── Step 2: Fetch anchor (lazy — knows tx_ledger_seq) ──
    if (!svc.get_anchor)
        throw std::runtime_error("No anchor provider");

    auto anchor = co_await svc.get_anchor(tx_ledger_seq);
    ctx->anchor_hdr = anchor.hdr;
    ctx->anchor_hash = anchor.hash;
    ctx->anchor_account_hash = anchor.account_hash;

    auto const& anchor_validations = anchor.validations;
    uint32_t anchor_seq = ctx->anchor_hdr.seq();
    PLOGI(
        log_,
        "[", net_label, "] Using validated anchor: seq=",
        anchor_seq,
        " (",
        anchor_validations.size(),
        " validators in proof)");

    auto collect_excluded_peers = [ctx]() {
        std::unordered_set<std::string> excluded;
        auto const now = std::chrono::steady_clock::now();
        for (auto it = ctx->peer_cooldowns.begin();
             it != ctx->peer_cooldowns.end();)
        {
            if (it->second <= now)
            {
                it = ctx->peer_cooldowns.erase(it);
                continue;
            }
            excluded.insert(it->first);
            ++it;
        }
        return excluded;
    };

    auto acquire_peer = [ctx, collect_excluded_peers, net_label](
                            std::shared_ptr<PeerClient>& current,
                            std::optional<uint32_t> required_ledger,
                            std::string const& purpose)
        -> boost::asio::awaitable<std::shared_ptr<PeerClient>> {
        auto peer_covers_ledger = [](std::shared_ptr<PeerClient> const& peer,
                                     uint32_t ledger_seq) {
            if (!peer || !peer->is_ready())
                return false;
            auto const first = peer->peer_first_seq();
            auto const last = peer->peer_last_seq();
            return first != 0 && last != 0 && ledger_seq >= first &&
                ledger_seq <= last;
        };

        for (;;)
        {
            auto excluded = collect_excluded_peers();
            // Merge in per-ledger failures from peer history
            if (required_ledger)
            {
                auto ledger_excluded = ctx->excluded_for(*required_ledger);
                excluded.insert(
                    ledger_excluded.begin(), ledger_excluded.end());
            }
            if (current && current->is_ready() &&
                !excluded.count(current->endpoint()) &&
                (!required_ledger ||
                 peer_covers_ledger(current, *required_ledger)))
            {
                co_return current;
            }

            std::shared_ptr<PeerClient> found;
            if (required_ledger)
            {
                found = co_await ctx->peers->wait_for_peer(
                    *required_ledger,
                    ctx->peer_retry_policy.wait_timeout_secs,
                    excluded);
            }
            else
            {
                found = co_await ctx->peers->wait_for_any_peer(
                    ctx->peer_retry_policy.wait_timeout_secs, excluded);
            }

            if (found)
            {
                current = found;
                if (!current->endpoint().empty())
                    ctx->peer_retry_counts[current->endpoint()] = 0;
                PLOGI(
                    log_, "[", net_label, "] Using peer ", current->endpoint(), " for ", purpose);
                catl::core::emit_status(
                    "peer " + current->endpoint() + " → " + purpose);
                co_return current;
            }

            PLOGI(
                log_,
                "[", net_label, "] Still waiting for peer for ",
                purpose,
                " (",
                excluded.size(),
                " cooled down)");
            catl::core::emit_status(
                "waiting for peer for " + purpose +
                " (" + std::to_string(excluded.size()) + " excluded)");
        }
    };

    auto is_retryable_peer_error = [](PeerClientException const& e) {
        switch (e.error)
        {
            case Error::Timeout:
            case Error::Disconnected:
            case Error::NoLedger:
            case Error::NoNode:
                return true;
            default:
                return false;
        }
    };

    auto with_peer_failover =
        [ctx, acquire_peer, is_retryable_peer_error](
            std::shared_ptr<PeerClient>& current,
            std::optional<uint32_t> required_ledger,
            std::string const& purpose,
            auto op) -> decltype(op(current)) {
        for (;;)
        {
            // Don't start new work if cancelled
            if (ctx->cancel_token &&
                ctx->cancel_token->load(std::memory_order_relaxed))
            {
                throw PeerClientException(Error::Timeout);
            }

            current = co_await acquire_peer(current, required_ledger, purpose);
            try
            {
                co_return co_await op(current);
            }
            catch (PeerClientException const& e)
            {
                // Don't retry if cancelled — the failure was from
                // walk_to bailing, not a real peer error
                if (ctx->cancel_token &&
                    ctx->cancel_token->load(std::memory_order_relaxed))
                {
                    throw;
                }

                if (!is_retryable_peer_error(e))
                    throw;

                auto const endpoint =
                    current ? current->endpoint() : std::string{};
                if (endpoint.empty())
                {
                    current.reset();
                    continue;
                }

                auto& failures = ctx->peer_retry_counts[endpoint];
                ++failures;
                if (failures <= ctx->peer_retry_policy.per_peer_retries)
                    continue;

                failures = 0;
                ctx->peer_cooldowns[endpoint] =
                    std::chrono::steady_clock::now() +
                    ctx->peer_retry_policy.cooldown;
                if (required_ledger)
                    ctx->note_peer_failure(endpoint, *required_ledger);
                current.reset();
            }
        }
    };

    // Don't call prioritize_ledger() — with concurrent prove() calls,
    // it would retune peer discovery away from other requests' targets.
    // wait_for_peer(seq) already searches by the specific seq needed.

    // ── Helpers for streaming steps as they become available ──
    ProofChain proof_chain;

    auto emit_step =
        [&](ChainStep step) -> boost::asio::awaitable<void> {
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
        [ctx](auto& tree, bool is_tx, TrieData::TreeType tree_type) -> TrieData {
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

    if (!ctx->need_flag_hop)
    {
        PLOGI(log_, "[", net_label, "] Short skip list (within 256)");
        catl::core::emit_status("short skip list (within 256)");
        auto skip_key_val = skip_list_key();

        auto wr = co_await with_peer_failover(
            ctx->client,
            anchor_seq,
            "walk short skip list",
            [ctx, skip_key_val, anchor_seq,
             walk_state](std::shared_ptr<PeerClient> peer)
                -> boost::asio::awaitable<WalkResult> {
                co_return co_await walk_state(
                    ctx->anchor_hash,
                    anchor_seq,
                    ctx->anchor_account_hash,
                    skip_key_val,
                    peer);
            });
        if (!wr.found)
            throw std::runtime_error("Short skip list not found in state tree");

        auto sp = build_state_proof(
            wr, skip_key_val, ctx->anchor_account_hash);
        co_await emit_step(
            make_trie_data(sp.tree, false, TrieData::TreeType::state));
        ctx->state_proofs.emplace_back(skip_key_val, std::move(sp));

        ctx->target_hdr_result = co_await with_peer_failover(
            ctx->client,
            tx_ledger_seq,
            "fetch target ledger header",
            [ctx, tx_ledger_seq](std::shared_ptr<PeerClient> peer)
                -> boost::asio::awaitable<LedgerHeaderResult> {
                co_return co_await ctx->node_cache->get_header(
                    tx_ledger_seq, ctx->peers, peer);
            });
        ctx->target_ledger_hash = ctx->target_hdr_result->ledger_hash256();

        Slice sle_leaf(wr.leaf_data.data(), wr.leaf_data.size());
        if (sle_hashes_contain(sle_leaf, ctx->target_ledger_hash, ctx->protocol))
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
        PLOGI(log_, "[", net_label, "] Long skip list (2-hop, distance=", ctx->distance, ")");
        catl::core::emit_status(
            "long skip list (2-hop, distance=" +
            std::to_string(ctx->distance) + ")");

        // Flag ledger: the nearest multiple of 256 AT OR ABOVE the target.
        // When the target IS a flag ledger (target % 256 == 0), the flag's
        // own short skip list doesn't contain its own hash — it only has
        // hashes of the 256 ledgers BEFORE it. So bump to the next flag.
        uint32_t flag_seq = flag_ledger_for(tx_ledger_seq);
        PLOGI(log_, "  Flag ledger: ", flag_seq);

        auto long_skip_key = skip_list_key(flag_seq);
        auto wr1 = co_await with_peer_failover(
            ctx->client,
            anchor_seq,
            "walk long skip list",
            [ctx, long_skip_key, anchor_seq,
             walk_state](std::shared_ptr<PeerClient> peer)
                -> boost::asio::awaitable<WalkResult> {
                co_return co_await walk_state(
                    ctx->anchor_hash,
                    anchor_seq,
                    ctx->anchor_account_hash,
                    long_skip_key,
                    peer);
            });
        if (!wr1.found)
            throw std::runtime_error("Long skip list not found in state tree");

        {
            auto sp = build_state_proof(
                wr1, long_skip_key, ctx->anchor_account_hash);
            co_await emit_step(
                make_trie_data(sp.tree, false, TrieData::TreeType::state));
            ctx->state_proofs.emplace_back(long_skip_key, std::move(sp));
        }

        ctx->flag_hdr_result = co_await with_peer_failover(
            ctx->client,
            flag_seq,
            "fetch flag ledger header",
            [ctx, flag_seq](std::shared_ptr<PeerClient> peer)
                -> boost::asio::awaitable<LedgerHeaderResult> {
                co_return co_await ctx->node_cache->get_header(
                    flag_seq, ctx->peers, peer);
            });
        auto flag_hash = ctx->flag_hdr_result->ledger_hash256();
        auto flag_header = ctx->flag_hdr_result->header();
        co_await emit_step(make_header(*ctx->flag_hdr_result));

        Slice long_sle(wr1.leaf_data.data(), wr1.leaf_data.size());
        if (sle_hashes_contain(long_sle, flag_hash, ctx->protocol))
        {
            PLOGI(log_, "  Flag hash confirmed in long skip list");
        }
        else
        {
            PLOGW(log_, "  Flag hash not found in long skip list!");
        }

        auto short_skip_key = skip_list_key();
        auto wr2 = co_await with_peer_failover(
            ctx->client,
            flag_seq,
            "walk flag short skip list",
            [ctx, flag_hash, flag_seq, short_skip_key,
             walk_state](std::shared_ptr<PeerClient> peer)
                -> boost::asio::awaitable<WalkResult> {
                co_return co_await walk_state(
                    flag_hash,
                    flag_seq,
                    ctx->flag_hdr_result->header().account_hash(),
                    short_skip_key,
                    peer);
            });
        if (!wr2.found)
            throw std::runtime_error(
                "Short skip list not found in flag ledger state tree");

        {
            auto sp = build_state_proof(
                wr2, short_skip_key, flag_header.account_hash());
            co_await emit_step(
                make_trie_data(sp.tree, false, TrieData::TreeType::state));
            ctx->state_proofs.emplace_back(short_skip_key, std::move(sp));
        }

        ctx->target_hdr_result = co_await with_peer_failover(
            ctx->client,
            tx_ledger_seq,
            "fetch target ledger header",
            [ctx, tx_ledger_seq](std::shared_ptr<PeerClient> peer)
                -> boost::asio::awaitable<LedgerHeaderResult> {
                co_return co_await ctx->node_cache->get_header(
                    tx_ledger_seq, ctx->peers, peer);
            });
        ctx->target_ledger_hash = ctx->target_hdr_result->ledger_hash256();

        Slice short_sle(wr2.leaf_data.data(), wr2.leaf_data.size());
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
        ctx->target_hdr_result = co_await with_peer_failover(
            ctx->client,
            tx_ledger_seq,
            "fetch target ledger header",
            [ctx, tx_ledger_seq](std::shared_ptr<PeerClient> peer)
                -> boost::asio::awaitable<LedgerHeaderResult> {
                co_return co_await ctx->node_cache->get_header(
                    tx_ledger_seq, ctx->peers, peer);
            });
    }
    auto const& target_hdr = *ctx->target_hdr_result;
    auto target_header = target_hdr.header();

    // ── Step 7: Walk TX tree ──
    auto ledger_hash = target_hdr.ledger_hash256();
    PLOGI(log_, "[", net_label, "] Walking TX tree for ", tx_hash_str.substr(0, 16), "...");
    catl::core::emit_status("walking TX tree");

    auto tx_walk = co_await with_peer_failover(
        ctx->client,
        tx_ledger_seq,
        "walk transaction tree",
        [ctx, ledger_hash, tx_ledger_seq, target_header, tx_hash,
         walk_tx](std::shared_ptr<PeerClient> peer)
            -> boost::asio::awaitable<WalkResult> {
            co_return co_await walk_tx(
                ledger_hash,
                tx_ledger_seq,
                target_header.tx_hash(),
                tx_hash,
                peer);
        });

    if (!tx_walk.found)
        throw std::runtime_error("Transaction not found in TX tree");

    // ── Step 8: Build abbreviated TX tree ──
    catl::shamap::SHAMapOptions opts;
    opts.tree_collapse_impl = catl::shamap::TreeCollapseImpl::leafs_only;
    opts.reference_hash_impl =
        catl::shamap::ReferenceHashImpl::recursive_simple;
    AbbrevMap abbrev(catl::shamap::tnTRANSACTION_MD, opts);

    catl::shamap::SHAMapNodeID tx_leaf_pos(
        Hash256(tx_walk.leaf_nid.id.data()), tx_walk.leaf_nid.depth);
    Slice leaf_item_data(
        tx_walk.leaf_data.data(), tx_walk.leaf_data.size() - 32);
    boost::intrusive_ptr<MmapItem> leaf_item(
        OwnedItem::create(tx_hash, leaf_item_data));
    abbrev.set_item_at(tx_leaf_pos, leaf_item);

    int placed = 0;
    for (auto& p : tx_walk.placeholders)
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
        std::string("TX tree: ") + (verified ? "verified" : "MISMATCH") +
        " (" + std::to_string(placed) + " placeholders)");

    // ── Emit target header + TX tree ──
    co_await emit_step(make_header(target_hdr));
    co_await emit_step(make_trie_data(abbrev, true, TrieData::TreeType::tx));

    co_return BuildResult{
        std::move(proof_chain), vl_data.publisher_key_hex, tx_ledger_seq};
}

}  // namespace xprv
