#include "xproof/anchor-verifier.h"
#include "xproof/hex-utils.h"
#include "xproof/proof-chain-json.h"
#include "xproof/proof-chain.h"
#include "xproof/proof-chain-json.h"
#include "xproof/proof-resolver.h"
#include "xproof/proof-steps.h"
#include "xproof/skip-list.h"
#include "xproof/validation-collector.h"

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
                xproof::ValidationCollector val_collector(protocol);
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
                xproof::resolve_proof_chain(
                    proof_chain,
                    vl_data ? vl_data->publisher_key_hex : "");

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
            auto proof = xproof::from_json(chain);
            bool ok = xproof::resolve_proof_chain(proof, trusted_key);
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
