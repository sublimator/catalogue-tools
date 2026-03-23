#include "commands.h"

#include <catl/core/logger.h>
#include <catl/peer-client/peer-client.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>

using namespace catl::peer_client;

static LogPartition log_("xproof", LogLevel::INFO);

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
        PLOGI(log_, "Ready! Peer at ledger ", peer_seq);

        uint32_t seq = ledger_seq == 0 ? peer_seq : ledger_seq;

        PLOGI(log_, "Requesting ledger ", seq, "...");
        client->get_ledger_header(
            seq, [&](Error err, LedgerHeaderResult result) {
                if (err != Error::Success)
                {
                    PLOGE(log_, "Failed: error ", static_cast<int>(err));
                    done = true;
                    io.stop();
                    return;
                }

                PLOGI(log_, "");
                PLOGI(log_, "=== Ledger ", result.seq(), " ===");
                PLOGI(log_, "  hash:         ", result.ledger_hash().hex());

                auto hdr = result.header();
                PLOGI(log_, "  parent_hash:  ", hdr.parent_hash().hex());
                PLOGI(log_, "  tx_hash:      ", hdr.tx_hash().hex());
                PLOGI(log_, "  account_hash: ", hdr.account_hash().hex());
                PLOGI(log_, "  close_time:   ", hdr.close_time());
                PLOGI(log_, "  drops:        ", hdr.drops());

                if (result.has_state_root())
                {
                    auto root = result.state_root_node();
                    auto inner = root.inner();
                    PLOGI(
                        log_,
                        "  state root:   ",
                        inner.child_count(),
                        " children");
                }
                if (result.has_tx_root())
                {
                    auto root = result.tx_root_node();
                    auto inner = root.inner();
                    PLOGI(
                        log_,
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
            PLOGE(log_, "Timeout");
            io.stop();
        }
    });

    io.run();
    return done ? 0 : 1;
}
