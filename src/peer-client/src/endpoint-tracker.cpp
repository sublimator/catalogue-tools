#include <catl/peer-client/endpoint-tracker.h>

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

}  // namespace catl::peer_client
