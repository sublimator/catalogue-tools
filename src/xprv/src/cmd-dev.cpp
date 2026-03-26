#include "commands.h"

#include "xprv/hex-utils.h"

#include <catl/core/logger.h>
#include <catl/peer-client/peer-client-coro.h>
#include <catl/peer-client/peer-client.h>
#include <catl/rpc-client/rpc-client-coro.h>
#include <catl/shamap/shamap.h>
#include <catl/xdata/parse_transaction.h>
#include <catl/xdata/pretty_print.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/json.hpp>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>

using namespace catl::peer_client;
using xprv::upper_hex;

static LogPartition log_("xprv", LogLevel::INFO);

[[maybe_unused]] static std::string
to_hex(std::span<const uint8_t> data)
{
    std::ostringstream oss;
    for (auto b : data)
        oss << std::hex << std::setfill('0') << std::setw(2)
            << static_cast<int>(b);
    return oss.str();
}

int
cmd_dev_check_ledger(
    std::string const& endpoint,
    catl::xdata::Protocol const& protocol)
{
    std::string host;
    uint16_t port;
    if (!parse_endpoint(endpoint, host, port))
    {
        std::cerr << "Invalid endpoint\n";
        return 1;
    }

    boost::asio::io_context io;
    int result = 1;

    boost::asio::co_spawn(
        io,
        [&]() -> boost::asio::awaitable<void> {
            std::shared_ptr<PeerClient> client;
            auto peer_seq = co_await co_connect(io, host, port, 0, client);
            PLOGI(log_, "Ready! Peer at ledger ", peer_seq);

            auto hdr = co_await co_get_ledger_header(client, peer_seq);
            auto ledger_hash = hdr.ledger_hash256();
            auto header = hdr.header();

            PLOGI(log_, "=== Ledger ", hdr.seq(), " ===");
            PLOGI(log_, "  hash:         ", hdr.ledger_hash().hex());
            PLOGI(log_, "  tx_hash:      ", header.tx_hash().hex());
            PLOGI(log_, "  account_hash: ", header.account_hash().hex());

            SHAMapNodeID root_id;
            auto tx_root =
                co_await co_get_tx_nodes(client, ledger_hash, {root_id});

            if (tx_root.node_count() == 0)
            {
                PLOGI(log_, "  TX tree empty");
                result = 0;
                co_return;
            }

            auto root_view = tx_root.node_view(0);
            if (!root_view.is_inner())
            {
                PLOGI(log_, "  TX root is a leaf (single tx)");
                result = 0;
                co_return;
            }

            auto root_inner = root_view.inner();
            PLOGI(log_, "  TX root: ", root_inner.child_count(), " children");

            std::vector<SHAMapNodeID> child_ids;
            root_inner.for_each_child([&](int branch, Key) {
                SHAMapNodeID child;
                child.depth = 1;
                child.id.data()[0] = static_cast<uint8_t>(branch << 4);
                child_ids.push_back(child);
            });

            auto child_nodes =
                co_await co_get_tx_nodes(client, ledger_hash, child_ids);

            int inners = 0, parsed = 0, failed = 0;
            boost::json::array tx_arr;

            for (int i = 0; i < child_nodes.node_count(); ++i)
            {
                auto v = child_nodes.node_view(i);
                if (v.is_inner())
                {
                    inners++;
                    continue;
                }

                auto leaf_data = v.leaf().data();
                try
                {
                    Slice slice(leaf_data.data(), leaf_data.size());
                    auto json =
                        catl::xdata::parse_transaction(slice, protocol, false);
                    tx_arr.push_back(json);
                    parsed++;
                }
                catch (std::exception const& e)
                {
                    failed++;
                    PLOGW(
                        log_,
                        "  tx parse failed: ",
                        e.what(),
                        " (size=",
                        leaf_data.size(),
                        ")");
                }
            }

            catl::xdata::pretty_print(std::cout, tx_arr);
            PLOGI(
                log_,
                "  ",
                parsed,
                " transactions parsed (",
                failed,
                " failed, ",
                inners,
                " inners skipped)");

            // Verify: rebuild SHAMap from leaves
            {
                catl::shamap::SHAMap verify_map(catl::shamap::tnTRANSACTION_MD);
                int added = 0;
                for (int i = 0; i < child_nodes.node_count(); ++i)
                {
                    auto v = child_nodes.node_view(i);
                    if (v.is_inner())
                        continue;
                    auto ld = v.leaf().data();
                    if (ld.size() < 32)
                        continue;
                    Hash256 tx_key(ld.data() + ld.size() - 32);
                    Slice item_data(ld.data(), ld.size() - 32);
                    boost::intrusive_ptr<MmapItem> item(
                        OwnedItem::create(tx_key, item_data));
                    verify_map.add_item(item);
                    added++;
                }
                auto computed = verify_map.get_hash();
                auto expected = header.tx_hash();
                bool match = (computed == expected);
                PLOGI(
                    log_,
                    "  TX tree hash: ",
                    match ? "VERIFIED" : "MISMATCH",
                    " (",
                    added,
                    " leaves)");
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
                    PLOGE(log_, "Fatal: ", e.what());
                }
            }
            io.stop();
        });

    boost::asio::steady_timer timer(io, std::chrono::seconds(30));
    timer.async_wait([&](boost::system::error_code) {
        PLOGE(log_, "Timeout");
        io.stop();
    });

    io.run();
    return result;
}

int
cmd_dev_tx(
    std::string const& endpoint,
    std::string const& tx_hash,
    [[maybe_unused]] catl::xdata::Protocol const& protocol)
{
    std::string host;
    uint16_t port;
    if (!parse_endpoint(endpoint, host, port))
    {
        std::cerr << "Invalid endpoint\n";
        return 1;
    }

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
                    log_,
                    "ledger_index: ",
                    obj.at("ledger_index").to_number<uint32_t>());
            }
            if (obj.contains("TransactionType"))
            {
                PLOGI(
                    log_,
                    "type: ",
                    boost::json::serialize(obj.at("TransactionType")));
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
                    PLOGE(log_, "Fatal: ", e.what());
                }
            }
            io.stop();
        });

    boost::asio::steady_timer timer(io, std::chrono::seconds(15));
    timer.async_wait([&](boost::system::error_code) {
        PLOGE(log_, "Timeout");
        io.stop();
    });

    io.run();
    return result;
}
