#include "ripple.pb.h"
#include <catl/common/ledger-info.h>
#include <catl/core/logger.h>
#include <catl/peer/peer-connection.h>
#include <catl/peer/types.h>

#include <chrono>
#include <csignal>
#include <iomanip>
#include <iostream>
#include <string>

using namespace catl::peer;

static LogPartition log_partition("xproof", LogLevel::INFO);
static std::atomic<bool> g_stop{false};

void
signal_handler(int)
{
    g_stop = true;
}

//------------------------------------------------------------------------------
// Parse host:port
//------------------------------------------------------------------------------

bool
parse_endpoint(
    std::string const& endpoint,
    std::string& host,
    uint16_t& port)
{
    auto colon = endpoint.rfind(':');
    if (colon == std::string::npos)
        return false;
    host = endpoint.substr(0, colon);
    port = static_cast<uint16_t>(std::stoul(endpoint.substr(colon + 1)));
    return true;
}

//------------------------------------------------------------------------------
// Connect, send ping, print pong
//------------------------------------------------------------------------------

int
cmd_ping(std::string const& endpoint)
{
    std::string host;
    uint16_t port;
    if (!parse_endpoint(endpoint, host, port))
    {
        std::cerr << "Invalid endpoint: " << endpoint << " (expected host:port)\n";
        return 1;
    }

    asio::io_context io_context;
    asio::ssl::context ssl_context(asio::ssl::context::tlsv12);
    ssl_context.set_options(
        asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 |
        asio::ssl::context::no_sslv3 | asio::ssl::context::single_dh_use);
    ssl_context.set_verify_mode(asio::ssl::verify_none);
    SSL_CTX_set_ecdh_auto(ssl_context.native_handle(), 1);

    peer_config config;
    config.host = host;
    config.port = port;
    config.network_id = 0;  // XRPL mainnet

    auto connection =
        std::make_shared<peer_connection>(io_context, ssl_context, config);

    auto ping_sent_at = std::chrono::steady_clock::now();
    bool got_pong = false;

    PLOGI(log_partition, "Connecting to ", host, ":", port, "...");

    connection->async_connect([&](boost::system::error_code ec) {
        if (ec)
        {
            PLOGE(log_partition, "Connection failed: ", ec.message());
            return;
        }

        PLOGI(
            log_partition,
            "Connected to ",
            connection->remote_endpoint());

        // Start reading packets
        connection->start_read(
            [&](packet_header const& header,
                std::vector<uint8_t> const& payload) {
                if (header.type ==
                    static_cast<uint16_t>(packet_type::ping))
                {
                    protocol::TMPing msg;
                    if (msg.ParseFromArray(payload.data(), payload.size()))
                    {
                        if (msg.type() == protocol::TMPing_pingType_ptPONG)
                        {
                            auto elapsed =
                                std::chrono::duration_cast<
                                    std::chrono::milliseconds>(
                                    std::chrono::steady_clock::now() -
                                    ping_sent_at);
                            PLOGI(
                                log_partition,
                                "PONG! seq=",
                                msg.seq(),
                                " rtt=",
                                elapsed.count(),
                                "ms");
                            if (msg.has_pingtime())
                            {
                                PLOGI(
                                    log_partition,
                                    "  pingTime=",
                                    msg.pingtime());
                            }
                            if (msg.has_nettime())
                            {
                                PLOGI(
                                    log_partition,
                                    "  netTime=",
                                    msg.nettime());
                            }
                            got_pong = true;
                            io_context.stop();
                        }
                        else if (
                            msg.type() == protocol::TMPing_pingType_ptPING)
                        {
                            // Peer pinged us — reply with pong
                            PLOGI(
                                log_partition,
                                "Received PING from peer, sending PONG");
                            protocol::TMPing pong;
                            pong.set_type(
                                protocol::TMPing_pingType_ptPONG);
                            if (msg.has_seq())
                                pong.set_seq(msg.seq());

                            std::vector<uint8_t> pong_data(
                                pong.ByteSizeLong());
                            pong.SerializeToArray(
                                pong_data.data(), pong_data.size());
                            connection->async_send_packet(
                                packet_type::ping,
                                pong_data,
                                [](boost::system::error_code) {});
                        }
                    }
                }
                else
                {
                    PLOGI(
                        log_partition,
                        "Received packet type=",
                        header.type,
                        " size=",
                        header.payload_size);
                }
            });

        // Send our ping
        protocol::TMPing ping;
        ping.set_type(protocol::TMPing_pingType_ptPING);
        ping.set_seq(42);

        std::vector<uint8_t> ping_data(ping.ByteSizeLong());
        ping.SerializeToArray(ping_data.data(), ping_data.size());

        ping_sent_at = std::chrono::steady_clock::now();

        connection->async_send_packet(
            packet_type::ping,
            ping_data,
            [](boost::system::error_code ec) {
                if (ec)
                {
                    PLOGE(log_partition, "Failed to send PING: ", ec.message());
                }
                else
                {
                    PLOGI(log_partition, "PING sent (seq=42)");
                }
            });
    });

    // Run with a timeout
    auto work_guard = asio::make_work_guard(io_context);
    auto timer = asio::steady_timer(io_context, std::chrono::seconds(10));
    timer.async_wait([&](boost::system::error_code) {
        if (!got_pong)
        {
            PLOGE(log_partition, "Timeout waiting for PONG");
            io_context.stop();
        }
    });

    std::signal(SIGINT, signal_handler);
    io_context.run();

    return got_pong ? 0 : 1;
}

//------------------------------------------------------------------------------
// Connect, fetch ledger header by sequence, print it
//------------------------------------------------------------------------------

int
cmd_header(std::string const& endpoint, uint32_t ledger_seq)
{
    std::string host;
    uint16_t port;
    if (!parse_endpoint(endpoint, host, port))
    {
        std::cerr << "Invalid endpoint: " << endpoint << " (expected host:port)\n";
        return 1;
    }

    asio::io_context io_context;
    asio::ssl::context ssl_context(asio::ssl::context::tlsv12);
    ssl_context.set_options(
        asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 |
        asio::ssl::context::no_sslv3 | asio::ssl::context::single_dh_use);
    ssl_context.set_verify_mode(asio::ssl::verify_none);
    SSL_CTX_set_ecdh_auto(ssl_context.native_handle(), 1);

    peer_config config;
    config.host = host;
    config.port = port;
    config.network_id = 0;  // XRPL mainnet

    auto connection =
        std::make_shared<peer_connection>(io_context, ssl_context, config);

    bool got_response = false;
    bool request_sent = false;
    uint32_t target_seq = ledger_seq;

    PLOGI(log_partition, "Connecting to ", host, ":", port, "...");

    // Lambda to send the actual ledger request
    auto send_request = [&](uint32_t seq) {
        if (request_sent)
            return;
        request_sent = true;

        protocol::TMGetLedger request;
        request.set_itype(protocol::liBASE);
        request.set_ledgerseq(seq);

        std::vector<uint8_t> req_data(request.ByteSizeLong());
        request.SerializeToArray(req_data.data(), req_data.size());

        connection->async_send_packet(
            packet_type::get_ledger,
            req_data,
            [seq](boost::system::error_code ec) {
                if (ec)
                {
                    PLOGE(
                        log_partition,
                        "Failed to send request: ",
                        ec.message());
                }
                else
                {
                    PLOGI(
                        log_partition,
                        "Sent TMGetLedger(liBASE, seq=",
                        seq,
                        ")");
                }
            });
    };

    connection->async_connect([&](boost::system::error_code ec) {
        if (ec)
        {
            PLOGE(log_partition, "Connection failed: ", ec.message());
            return;
        }

        PLOGI(log_partition, "Connected, waiting for peer status...");

        // IMPORTANT: Peers won't respond to data requests (TMGetLedger etc.)
        // until the status exchange is complete. The sequence is:
        //   1. We send TMStatusChange(nsMONITORING)
        //   2. Peer sends us TMStatusChange (with their current ledger)
        //   3. We mirror their status back
        //   4. NOW we can make data requests
        // Without this, TMGetLedger simply times out with no response.
        // This should eventually be handled automatically by PeerClient.

        // Send monitoring status
        {
            protocol::TMStatusChange status;
            status.set_newstatus(protocol::nsMONITORING);
            std::vector<uint8_t> status_data(status.ByteSizeLong());
            status.SerializeToArray(status_data.data(), status_data.size());
            connection->async_send_packet(
                packet_type::status_change,
                status_data,
                [](boost::system::error_code) {});
        }

        // Start reading
        connection->start_read(
            [&](packet_header const& header,
                std::vector<uint8_t> const& payload) {
                // Watch for status changes to learn current ledger
                if (header.type ==
                    static_cast<uint16_t>(packet_type::status_change))
                {
                    protocol::TMStatusChange status;
                    if (status.ParseFromArray(
                            payload.data(), payload.size()))
                    {
                        if (status.has_ledgerseq())
                        {
                            auto peer_seq = status.ledgerseq();
                            PLOGI(
                                log_partition,
                                "Peer at ledger ",
                                peer_seq);

                            // If user asked for 0, use current
                            if (target_seq == 0)
                                target_seq = peer_seq;

                            // Mirror status back
                            connection->async_send_packet(
                                packet_type::status_change,
                                payload,
                                [](boost::system::error_code) {});

                            // Now send our request
                            send_request(target_seq);
                        }
                    }
                }
                else if (
                    header.type ==
                    static_cast<uint16_t>(packet_type::ledger_data))
                {
                    protocol::TMLedgerData msg;
                    if (!msg.ParseFromArray(payload.data(), payload.size()))
                    {
                        PLOGE(log_partition, "Failed to parse TMLedgerData");
                        return;
                    }

                    if (msg.has_error())
                    {
                        PLOGE(
                            log_partition,
                            "Error from peer: ",
                            msg.error());
                        io_context.stop();
                        return;
                    }

                    PLOGI(
                        log_partition,
                        "Received ledger data: seq=",
                        msg.ledgerseq(),
                        " nodes=",
                        msg.nodes_size());

                    if (msg.nodes_size() > 0)
                    {
                        auto const& node = msg.nodes(0);
                        auto const& data = node.nodedata();
                        auto data_size = data.size();

                        PLOGI(
                            log_partition,
                            "Header node: ",
                            data_size,
                            " bytes");

                        if (data_size >=
                            catl::common::LedgerInfoView::
                                HEADER_SIZE_WITHOUT_HASH)
                        {
                            auto view = catl::common::LedgerInfoView(
                                reinterpret_cast<const uint8_t*>(data.data()),
                                data_size);

                            auto info = view.to_ledger_info();
                            PLOGI(log_partition, "");
                            PLOGI(
                                log_partition,
                                "=== Ledger ",
                                info.seq,
                                " ===");
                            PLOGI(
                                log_partition,
                                "  hash:         ",
                                info.hash ? info.hash->hex() : "(not in response)");
                            PLOGI(
                                log_partition,
                                "  parent_hash:  ",
                                info.parent_hash.hex());
                            PLOGI(
                                log_partition,
                                "  tx_hash:      ",
                                info.tx_hash.hex());
                            PLOGI(
                                log_partition,
                                "  account_hash: ",
                                info.account_hash.hex());
                            PLOGI(
                                log_partition,
                                "  close_time:   ",
                                info.close_time);
                            PLOGI(
                                log_partition,
                                "  drops:        ",
                                info.drops);
                            PLOGI(
                                log_partition,
                                "  close_res:    ",
                                static_cast<int>(
                                    info.close_time_resolution));
                            PLOGI(
                                log_partition,
                                "  close_flags:  ",
                                static_cast<int>(info.close_flags));
                        }
                        else
                        {
                            PLOGE(
                                log_partition,
                                "Header too small: ",
                                data_size,
                                " bytes");
                        }

                        // Also show state/tx root nodes if present
                        for (int i = 1; i < msg.nodes_size(); ++i)
                        {
                            PLOGI(
                                log_partition,
                                "  tree root node ",
                                i,
                                ": ",
                                msg.nodes(i).nodedata().size(),
                                " bytes");
                        }
                    }

                    got_response = true;
                    io_context.stop();
                }
                else if (
                    header.type ==
                    static_cast<uint16_t>(packet_type::ping))
                {
                    // Reply to peer pings
                    protocol::TMPing msg;
                    if (msg.ParseFromArray(payload.data(), payload.size()) &&
                        msg.type() == protocol::TMPing_pingType_ptPING)
                    {
                        protocol::TMPing pong;
                        pong.set_type(protocol::TMPing_pingType_ptPONG);
                        if (msg.has_seq())
                            pong.set_seq(msg.seq());
                        std::vector<uint8_t> pong_data(pong.ByteSizeLong());
                        pong.SerializeToArray(
                            pong_data.data(), pong_data.size());
                        connection->async_send_packet(
                            packet_type::ping,
                            pong_data,
                            [](boost::system::error_code) {});
                    }
                }
            });

    });

    // Timeout
    auto work_guard = asio::make_work_guard(io_context);
    auto timer = asio::steady_timer(io_context, std::chrono::seconds(15));
    timer.async_wait([&](boost::system::error_code) {
        if (!got_response)
        {
            PLOGE(log_partition, "Timeout waiting for ledger data");
            io_context.stop();
        }
    });

    std::signal(SIGINT, signal_handler);
    io_context.run();

    return got_response ? 0 : 1;
}

//------------------------------------------------------------------------------
// Main
//------------------------------------------------------------------------------

void
print_usage()
{
    std::cerr << "xproof - XRPL Proof Chain Tool\n"
              << "\n"
              << "Usage:\n"
              << "  xproof ping <host:port>\n"
              << "  xproof header <host:port> <ledger_seq>\n"
              << "  xproof prove-tx <host:port> <tx_hash>\n"
              << "  xproof verify <proof.json>\n"
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
    {
        return cmd_header(argv[2], std::stoul(argv[3]));
    }
    else if (command == "prove-tx" && argc >= 4)
    {
        PLOGI(log_partition, "Not yet implemented: prove-tx");
        return 1;
    }
    else if (command == "verify" && argc >= 3)
    {
        PLOGI(log_partition, "Not yet implemented: verify");
        return 1;
    }

    print_usage();
    return 1;
}
