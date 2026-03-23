#include "xproof/hex-utils.h"
#include "xproof/proof-builder.h"
#include "xproof/proof-chain-json.h"
#include "xproof/proof-resolver.h"

#include <catl/core/logger.h>
#include <catl/peer-client/peer-client-coro.h>
#include <catl/peer-client/peer-client.h>
#include <catl/rpc-client/rpc-client-coro.h>
#include <catl/shamap/shamap.h>

#include <catl/xdata/parse_transaction.h>
#include <catl/xdata/pretty_print.h>
#include <catl/xdata/protocol.h>

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
using xproof::upper_hex;

static LogPartition log_partition("xproof", LogLevel::INFO);

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
    auto protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();

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
                        auto json = catl::xdata::parse_transaction(
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

                catl::xdata::pretty_print(std::cout, tx_arr);

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

                catl::xdata::pretty_print(std::cout, tx_result);
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

        boost::asio::io_context io;
        int result = 1;

        boost::asio::co_spawn(
            io,
            [&]() -> boost::asio::awaitable<void> {
                auto build_result = co_await xproof::build_proof(
                    io, rpc_host, rpc_port, peer_host, peer_port, tx_hash_str);

                // Serialize to JSON
                auto chain = xproof::to_json(build_result.chain);
                catl::xdata::pretty_print(std::cout, chain);

                // Write proof to file
                {
                    std::string path = "proof.json";
                    std::ofstream out(path);
                    catl::xdata::pretty_print(out, chain);
                    out.close();
                    PLOGI(log_partition, "Wrote proof: ", path);
                }

                // Verify the proof chain we just built
                xproof::resolve_proof_chain(
                    build_result.chain,
                    protocol,
                    build_result.publisher_key_hex);

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
            bool ok = xproof::resolve_proof_chain(proof, protocol, trusted_key);
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
