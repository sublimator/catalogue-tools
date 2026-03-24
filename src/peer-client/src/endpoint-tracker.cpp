#include <catl/peer-client/endpoint-tracker.h>

#include <algorithm>

namespace catl::peer_client {

void
EndpointTracker::update(std::string const& endpoint, PeerStatus status)
{
    std::lock_guard lock(mutex_);
    status.last_seen = std::chrono::steady_clock::now();
    peers_[endpoint] = status;
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
    std::lock_guard lock(mutex_);
    // Only add if not already tracked
    if (peers_.find(endpoint) == peers_.end())
    {
        PeerStatus s;
        s.last_seen = std::chrono::steady_clock::now();
        peers_[endpoint] = s;
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

void
EndpointTracker::clear()
{
    std::lock_guard lock(mutex_);
    peers_.clear();
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
            port = static_cast<uint16_t>(
                std::stoul(endpoint.substr(close + 2)));
            return true;
        }
        catch (...)
        {
            return false;
        }
    }

    // Count colons — if more than one, it's bare IPv6 without port
    auto colons = std::count(endpoint.begin(), endpoint.end(), ':');
    if (colons > 1)
    {
        // Bare IPv6 without brackets — can't extract port
        return false;
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
