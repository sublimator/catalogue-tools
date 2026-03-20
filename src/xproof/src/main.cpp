#include <catl/core/logger.h>
#include <catl/crypto/sha512-half-hasher.h>
#include <catl/peer-client/peer-client-coro.h>
#include <catl/peer-client/peer-client.h>
#include <catl/peer-client/tree-walker.h>
#include <catl/rpc-client/rpc-client-coro.h>
#include <catl/shamap/shamap-hashprefix.h>
#include <catl/shamap/shamap-header-only.h>
#include <catl/shamap/shamap-nodeid.h>
#include <catl/shamap/shamap.h>

// Instantiate SHAMap for AbbreviatedTreeTraits
INSTANTIATE_SHAMAP_NODE_TRAITS(catl::shamap::AbbreviatedTreeTraits);
#include <catl/xdata-json/parse_transaction.h>
#include <catl/xdata-json/pretty_print.h>
#include <catl/xdata/json-visitor.h>
#include <catl/xdata/parser-context.h>
#include <catl/xdata/parser.h>
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
        << "  xproof verify <proof.json>                verify a proof chain\n"
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

                // Helper: build + verify abbreviated state tree
                auto verify_state_proof =
                    [&](StateWalkResult const& wr,
                        Hash256 const& key,
                        Hash256 const& expected_account_hash) -> bool {
                    using AbbrevMap = catl::shamap::SHAMapT<
                        catl::shamap::AbbreviatedTreeTraits>;
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
                        if (abbrev.needs_placeholder(p.nid))
                            abbrev.set_placeholder(p.nid, p.hash);

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
                    return ok;
                };

                // No helper needed — Hash256::find_in(Slice) does the scan

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

                // ── Step 2: Peer — connect and get anchor ledger ──
                std::shared_ptr<PeerClient> client;
                auto peer_seq =
                    co_await co_connect(io, peer_host, peer_port, 0, client);
                PLOGI(
                    log_partition,
                    "Connected to peer, anchor ledger ",
                    peer_seq);

                auto anchor_hdr =
                    co_await co_get_ledger_header(client, peer_seq);
                auto anchor_header = anchor_hdr.header();
                auto anchor_hash = anchor_hdr.ledger_hash256();
                PLOGI(
                    log_partition,
                    "Anchor ledger ",
                    anchor_hdr.seq(),
                    " hash=",
                    anchor_hdr.ledger_hash().hex().substr(0, 16),
                    "...");

                // ── Step 3: Determine hop path ──
                uint32_t distance = anchor_hdr.seq() - tx_ledger_seq;
                PLOGI(log_partition, "Distance: ", distance, " ledgers");

                Hash256 target_ledger_hash;
                bool need_flag_hop = (distance > 256);

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

                    verify_state_proof(
                        wr, skip_key_val, anchor_header.account_hash());

                    // The target ledger hash should be in sfHashes
                    // But we need the actual hash — fetch the target header to
                    // get it, then verify it's in the skip list
                    auto target_hdr =
                        co_await co_get_ledger_header(client, tx_ledger_seq);
                    target_ledger_hash = target_hdr.ledger_hash256();

                    Slice sle_data(
                        wr.leaf_data.data(), wr.leaf_data.size() - 32);
                    if (!target_ledger_hash.find_in(sle_data) >= 0)
                        PLOGW(
                            log_partition,
                            "  Target hash not found in short skip list!");
                    else
                        PLOGI(
                            log_partition,
                            "  Target hash confirmed in short skip list");
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
                    uint32_t flag_seq =
                        tx_ledger_seq & ~0xFFu;  // round down to nearest 256
                    PLOGI(log_partition, "  Flag ledger: ", flag_seq);

                    auto long_skip_key = skip_list_key(tx_ledger_seq);
                    PLOGD(
                        log_partition,
                        "  long skip key: ",
                        upper_hex(long_skip_key));

                    auto wr1 =
                        co_await walk_state(client, anchor_hash, long_skip_key);
                    if (!wr1.found)
                        throw std::runtime_error(
                            "Long skip list not found in state tree");

                    verify_state_proof(
                        wr1, long_skip_key, anchor_header.account_hash());

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

                    Slice long_sle(
                        wr1.leaf_data.data(), wr1.leaf_data.size() - 32);
                    if (!flag_hash.find_in(long_sle) >= 0)
                        PLOGW(
                            log_partition,
                            "  Flag hash not found in long skip list!");
                    else
                        PLOGI(
                            log_partition,
                            "  Flag hash confirmed in long skip list");

                    // Hop 2: find target hash in flag ledger's short skip list
                    auto short_skip_key = skip_list_key();
                    auto wr2 =
                        co_await walk_state(client, flag_hash, short_skip_key);
                    if (!wr2.found)
                        throw std::runtime_error(
                            "Short skip list not found in flag ledger state "
                            "tree");

                    verify_state_proof(
                        wr2, short_skip_key, flag_header.account_hash());

                    auto target_hdr =
                        co_await co_get_ledger_header(client, tx_ledger_seq);
                    target_ledger_hash = target_hdr.ledger_hash256();

                    Slice short_sle(
                        wr2.leaf_data.data(), wr2.leaf_data.size() - 32);
                    if (!target_ledger_hash.find_in(short_sle) >= 0)
                        PLOGW(
                            log_partition,
                            "  Target hash not found in flag's short skip "
                            "list!");
                    else
                        PLOGI(
                            log_partition,
                            "  Target hash confirmed in flag's short skip "
                            "list");
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
                boost::json::array chain;

                // Anchor (stub — no validations yet)
                {
                    boost::json::object anchor;
                    anchor["type"] = "anchor";
                    anchor["ledger_hash"] = upper_hex(anchor_hash);
                    anchor["ledger_index"] = anchor_hdr.seq();
                    anchor["note"] = "stub - validations not yet implemented";
                    chain.push_back(anchor);
                }

                // Ledger header (target ledger)
                {
                    boost::json::object step;
                    step["type"] = "ledger_header";
                    boost::json::object hdr_obj;
                    hdr_obj["seq"] = target_hdr.seq();
                    hdr_obj["drops"] = std::to_string(target_header.drops());
                    hdr_obj["parent_hash"] =
                        upper_hex(target_header.parent_hash());
                    hdr_obj["tx_hash"] = upper_hex(target_header.tx_hash());
                    hdr_obj["account_hash"] =
                        upper_hex(target_header.account_hash());
                    hdr_obj["parent_close_time"] =
                        target_header.parent_close_time();
                    hdr_obj["close_time"] = target_header.close_time();
                    hdr_obj["close_time_resolution"] =
                        target_header.close_time_resolution();
                    hdr_obj["close_flags"] = target_header.close_flags();
                    step["header"] = hdr_obj;
                    chain.push_back(step);
                }

                // Map proof (TX tree)
                {
                    boost::json::object step;
                    step["type"] = "map_proof";
                    step["tree"] = "tx";
                    step["key"] = upper_hex(tx_hash);

                    // Build the trie JSON from walk_abbreviated
                    // We need to build the nested structure from the flat walk.
                    // For now, just output a flat list of nodes with their
                    // types.
                    boost::json::object trie;
                    // TODO: build nested trie JSON per spec section 2.3
                    step["trie"] = "TODO";
                    step["verified"] = verified;

                    chain.push_back(step);
                }

                // Parse the actual transaction for display
                auto protocol =
                    catl::xdata::Protocol::load_embedded_xrpl_protocol();
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
    else if (command == "verify" && argc >= 3)
    {
        PLOGI(log_partition, "Not yet implemented: verify");
        return 1;
    }

    print_usage();
    return 1;
}
