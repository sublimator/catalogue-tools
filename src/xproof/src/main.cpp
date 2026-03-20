#include <catl/core/logger.h>
#include <catl/peer-client/peer-client.h>

#include <csignal>
#include <iostream>
#include <string>

using namespace catl::peer_client;

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
    client = PeerClient::connect(io, host, port, 0,
        [&](uint32_t peer_seq) {
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
    client = PeerClient::connect(io, host, port, 0,
        [&](uint32_t peer_seq) {
            PLOGI(log_partition, "Ready! Peer at ledger ", peer_seq);

            // 0 means "current" — use the peer's latest
            uint32_t seq = ledger_seq == 0 ? peer_seq : ledger_seq;

            PLOGI(log_partition, "Requesting ledger ", seq, "...");
            client->get_ledger_header(seq,
                [&](Error err, LedgerHeaderResult result) {
                    if (err != Error::Success)
                    {
                        PLOGE(
                            log_partition,
                            "Failed: error ",
                            static_cast<int>(err));
                        done = true;
                        io.stop();
                        return;
                    }

                    PLOGI(log_partition, "");
                    PLOGI(
                        log_partition,
                        "=== Ledger ",
                        result.seq(),
                        " ===");
                    PLOGI(
                        log_partition,
                        "  hash:         ",
                        result.ledger_hash().hex());

                    auto hdr = result.header();
                    PLOGI(
                        log_partition,
                        "  parent_hash:  ",
                        hdr.parent_hash().hex());
                    PLOGI(
                        log_partition,
                        "  tx_hash:      ",
                        hdr.tx_hash().hex());
                    PLOGI(
                        log_partition,
                        "  account_hash: ",
                        hdr.account_hash().hex());
                    PLOGI(
                        log_partition,
                        "  close_time:   ",
                        hdr.close_time());
                    PLOGI(
                        log_partition,
                        "  drops:        ",
                        hdr.drops());

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
        return cmd_header(argv[2], std::stoul(argv[3]));

    if (command == "prove-tx" && argc >= 4)
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
