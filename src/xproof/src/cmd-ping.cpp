#include "commands.h"

#include <catl/core/logger.h>
#include <catl/peer-client/peer-client.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>

using namespace catl::peer_client;

static LogPartition log_("xproof", LogLevel::INFO);

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
        PLOGI(log_, "Ready! Peer at ledger ", peer_seq);
        PLOGI(log_, "Sending ping...");

        client->ping([&](Error err, PingResult result) {
            if (err != Error::Success)
            {
                PLOGE(log_, "Ping failed");
            }
            else
            {
                PLOGI(log_, "PONG! seq=", result.seq);
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
