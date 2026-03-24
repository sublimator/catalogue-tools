#include <catl/peer-client/peer-set.h>
#include <catl/peer-client/peer-client-coro.h>

#include <catl/core/logger.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>

namespace catl::peer_client {

static LogPartition log_("peer-set", LogLevel::INFO);

// ── Bootstrap peers by network ──────────────────────────────────

static const std::vector<BootstrapPeer> XRPL_BOOTSTRAP = {
    {"r.ripple.com", 51235},
    {"sahyadri.isrdc.in", 51235},
    {"hubs.xrpkuwait.com", 51235},
    {"hub.xrpl-commons.org", 51235},
};

static const std::vector<BootstrapPeer> XAHAU_BOOTSTRAP = {
    {"bacab.alloy.ee", 21337},
};

static const std::vector<BootstrapPeer> EMPTY_BOOTSTRAP = {};

std::vector<BootstrapPeer> const&
get_bootstrap_peers(uint32_t network_id)
{
    switch (network_id)
    {
        case 0:
            return XRPL_BOOTSTRAP;
        case 21337:
            return XAHAU_BOOTSTRAP;
        default:
            return EMPTY_BOOTSTRAP;
    }
}

// ── PeerSet ─────────────────────────────────────────────────────

PeerSet::PeerSet(boost::asio::io_context& io, uint32_t network_id)
    : io_(io)
    , network_id_(network_id)
    , tracker_(std::make_shared<EndpointTracker>())
{
}

boost::asio::awaitable<std::shared_ptr<PeerClient>>
PeerSet::try_connect(std::string const& host, uint16_t port)
{
    auto key = make_key(host, port);

    // Already connected?
    auto it = connections_.find(key);
    if (it != connections_.end() && it->second->is_ready())
    {
        co_return it->second;
    }

    try
    {
        std::shared_ptr<PeerClient> client;
        co_await co_connect(io_, host, port, network_id_, client);
        client->set_tracker(tracker_);
        if (unsolicited_handler_)
        {
            client->set_unsolicited_handler(unsolicited_handler_);
        }
        connections_[key] = client;
        PLOGI(
            log_,
            "Connected to ",
            key,
            " (range: ",
            client->peer_first_seq(),
            "-",
            client->peer_last_seq(),
            ")");
        co_return client;
    }
    catch (std::exception const& e)
    {
        PLOGD(log_, "Failed to connect to ", key, ": ", e.what());
        co_return nullptr;
    }
}

boost::asio::awaitable<int>
PeerSet::bootstrap()
{
    auto const& boot_peers = get_bootstrap_peers(network_id_);
    if (boot_peers.empty())
    {
        PLOGW(log_, "No bootstrap peers for network ", network_id_);
        co_return 0;
    }

    PLOGI(
        log_,
        "Bootstrapping with ",
        boot_peers.size(),
        " peers for network ",
        network_id_);

    // Connect to all bootstrap peers in parallel
    auto connected = std::make_shared<std::atomic<int>>(0);
    auto remaining = std::make_shared<std::atomic<int>>(
        static_cast<int>(boot_peers.size()));

    // Signal when all attempts complete
    auto signal = std::make_shared<boost::asio::steady_timer>(
        io_, boost::asio::steady_timer::time_point::max());

    for (auto const& bp : boot_peers)
    {
        boost::asio::co_spawn(
            io_,
            [this, bp, connected, remaining, signal]()
                -> boost::asio::awaitable<void> {
                auto client = co_await try_connect(bp.host, bp.port);
                if (client)
                {
                    connected->fetch_add(1);
                }
                if (remaining->fetch_sub(1) == 1)
                {
                    // Last one done — wake up the caller
                    signal->cancel();
                }
            },
            boost::asio::detached);
    }

    // Wait for all to complete
    boost::system::error_code ec;
    co_await signal->async_wait(
        boost::asio::redirect_error(boost::asio::use_awaitable, ec));

    int result = connected->load();
    PLOGI(
        log_,
        "Bootstrap complete: ",
        result,
        "/",
        boot_peers.size(),
        " peers connected");

    co_return result;
}

boost::asio::awaitable<std::shared_ptr<PeerClient>>
PeerSet::add(std::string const& host, uint16_t port)
{
    auto client = co_await try_connect(host, port);
    if (!client)
    {
        throw std::runtime_error(
            "PeerSet: failed to connect to " + make_key(host, port));
    }
    co_return client;
}

boost::asio::awaitable<std::shared_ptr<PeerClient>>
PeerSet::peer_for(uint32_t ledger_seq, int discovery_timeout_secs)
{
    // 1. Check existing connections
    for (auto& [key, client] : connections_)
    {
        if (!client->is_ready())
            continue;
        if (client->peer_first_seq() != 0 &&
            ledger_seq >= client->peer_first_seq() &&
            ledger_seq <= client->peer_last_seq())
        {
            PLOGD(log_, "Using existing peer ", key, " for ledger ", ledger_seq);
            co_return client;
        }
    }

    // 2. Check tracker for known peers we haven't connected to
    auto best = tracker_->best_peer_for(ledger_seq);
    if (best)
    {
        auto colon = best->rfind(':');
        if (colon != std::string::npos)
        {
            auto host = best->substr(0, colon);
            auto port = static_cast<uint16_t>(
                std::stoul(best->substr(colon + 1)));
            auto client = co_await try_connect(host, port);
            if (client && client->peer_first_seq() != 0 &&
                ledger_seq >= client->peer_first_seq() &&
                ledger_seq <= client->peer_last_seq())
            {
                co_return client;
            }
        }
    }

    // 3. Bootstrap: connect to well-known peers in parallel
    PLOGI(log_, "No existing peer has ledger ", ledger_seq, " — bootstrapping");
    co_await bootstrap();

    // Check again after bootstrap
    for (auto& [key, c] : connections_)
    {
        if (!c->is_ready())
            continue;
        if (c->peer_first_seq() != 0 &&
            ledger_seq >= c->peer_first_seq() &&
            ledger_seq <= c->peer_last_seq())
        {
            PLOGI(log_, "Found peer ", key, " for ledger ", ledger_seq);
            co_return c;
        }
    }

    throw std::runtime_error(
        "PeerSet: no peer found with ledger " + std::to_string(ledger_seq));
}

std::shared_ptr<PeerClient>
PeerSet::any_peer() const
{
    for (auto& [_, client] : connections_)
    {
        if (client->is_ready())
            return client;
    }
    return nullptr;
}

}  // namespace catl::peer_client
