#include <catl/peer-client/endpoint-tracker.h>

#include <algorithm>
#include <cctype>

namespace catl::peer_client {

void
EndpointTracker::update(std::string const& endpoint, PeerStatus status)
{
    StatusObserver observer;
    {
        std::lock_guard lock(mutex_);
        status.last_seen = std::chrono::steady_clock::now();
        peers_[endpoint] = status;
        observer = status_observer_;
    }

    if (observer)
    {
        observer(endpoint, status);
    }
}

void
EndpointTracker::remove(std::string const& endpoint)
{
    std::lock_guard lock(mutex_);
    peers_.erase(endpoint);
}

std::optional<std::string>
EndpointTracker::best_peer_for(uint32_t ledger_seq) const
{
    std::lock_guard lock(mutex_);

    std::optional<std::string> best;
    std::chrono::steady_clock::time_point best_time;

    for (auto const& [endpoint, status] : peers_)
    {
        if (status.first_seq == 0 && status.last_seq == 0)
            continue;

        if (ledger_seq >= status.first_seq && ledger_seq <= status.last_seq)
        {
            // Prefer most recently seen peer
            if (!best || status.last_seen > best_time)
            {
                best = endpoint;
                best_time = status.last_seen;
            }
        }
    }

    return best;
}

std::vector<std::string>
EndpointTracker::peers_for(uint32_t ledger_seq) const
{
    std::lock_guard lock(mutex_);

    std::vector<std::string> result;
    for (auto const& [endpoint, status] : peers_)
    {
        if (status.first_seq == 0 && status.last_seq == 0)
            continue;

        if (ledger_seq >= status.first_seq && ledger_seq <= status.last_seq)
        {
            result.push_back(endpoint);
        }
    }

    return result;
}

bool
EndpointTracker::has_peer_for(uint32_t ledger_seq) const
{
    std::lock_guard lock(mutex_);

    for (auto const& [_, status] : peers_)
    {
        if (status.first_seq == 0 && status.last_seq == 0)
            continue;

        if (ledger_seq >= status.first_seq && ledger_seq <= status.last_seq)
        {
            return true;
        }
    }

    return false;
}

std::optional<PeerStatus>
EndpointTracker::get_status(std::string const& endpoint) const
{
    std::lock_guard lock(mutex_);

    auto it = peers_.find(endpoint);
    if (it != peers_.end())
        return it->second;
    return std::nullopt;
}

size_t
EndpointTracker::size() const
{
    std::lock_guard lock(mutex_);
    return peers_.size();
}

void
EndpointTracker::add_discovered(std::string const& endpoint)
{
    DiscoveredObserver observer;
    {
        std::lock_guard lock(mutex_);
        auto& status = peers_[endpoint];
        status.last_seen = std::chrono::steady_clock::now();
        observer = discovered_observer_;
    }

    if (observer)
    {
        observer(endpoint);
    }
}

std::vector<std::string>
EndpointTracker::undiscovered() const
{
    std::lock_guard lock(mutex_);

    std::vector<std::string> result;
    for (auto const& [endpoint, status] : peers_)
    {
        // No range info yet — never got a TMStatusChange
        if (status.first_seq == 0 && status.last_seq == 0 &&
            status.current_seq == 0)
        {
            result.push_back(endpoint);
        }
    }

    return result;
}

std::vector<std::string>
EndpointTracker::all_endpoints() const
{
    std::lock_guard lock(mutex_);

    std::vector<std::string> result;
    result.reserve(peers_.size());
    for (auto const& [endpoint, _] : peers_)
    {
        result.push_back(endpoint);
    }

    return result;
}

void
EndpointTracker::clear()
{
    std::lock_guard lock(mutex_);
    peers_.clear();
}

void
EndpointTracker::set_discovered_observer(DiscoveredObserver observer)
{
    std::lock_guard lock(mutex_);
    discovered_observer_ = std::move(observer);
}

void
EndpointTracker::set_status_observer(StatusObserver observer)
{
    std::lock_guard lock(mutex_);
    status_observer_ = std::move(observer);
}

bool
EndpointTracker::parse_endpoint(
    std::string const& endpoint,
    std::string& host,
    uint16_t& port)
{
    if (endpoint.empty())
        return false;

    // IPv6 bracket notation: [::ffff:1.2.3.4]:51235
    if (endpoint[0] == '[')
    {
        auto close = endpoint.find(']');
        if (close == std::string::npos)
            return false;
        host = endpoint.substr(1, close - 1);
        // Expect ]:port
        if (close + 1 >= endpoint.size() || endpoint[close + 1] != ':')
            return false;
        try
        {
            port =
                static_cast<uint16_t>(std::stoul(endpoint.substr(close + 2)));
            return true;
        }
        catch (...)
        {
            return false;
        }
    }

    // Count colons — if more than one, it may be IPv4-mapped IPv6 with an
    // appended port (for example ::ffff:1.2.3.4:51235).
    auto colons = std::count(endpoint.begin(), endpoint.end(), ':');
    if (colons > 1)
    {
        auto colon = endpoint.rfind(':');
        if (colon == std::string::npos)
            return false;

        auto const port_part = endpoint.substr(colon + 1);
        if (port_part.empty() ||
            !std::all_of(
                port_part.begin(), port_part.end(), [](unsigned char ch) {
                    return std::isdigit(ch) != 0;
                }))
        {
            return false;
        }

        host = endpoint.substr(0, colon);
        if (host.find('.') == std::string::npos)
        {
            // Still looks like a bare IPv6 literal without bracket notation.
            return false;
        }

        try
        {
            port = static_cast<uint16_t>(std::stoul(port_part));
            return true;
        }
        catch (...)
        {
            return false;
        }
    }

    // IPv4 or hostname: host:port
    auto colon = endpoint.rfind(':');
    if (colon == std::string::npos)
        return false;
    host = endpoint.substr(0, colon);
    try
    {
        port = static_cast<uint16_t>(std::stoul(endpoint.substr(colon + 1)));
        return true;
    }
    catch (...)
    {
        return false;
    }
}

}  // namespace catl::peer_client
