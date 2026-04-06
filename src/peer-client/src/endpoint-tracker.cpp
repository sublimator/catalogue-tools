#include <catl/peer-client/endpoint-tracker.h>

#include <boost/asio/ip/address.hpp>

#include <algorithm>
#include <cctype>

namespace catl::peer_client {

namespace {

std::string
strip_ipv4_mapped(std::string const& host)
{
    static constexpr auto prefix = std::string_view("::ffff:");
    if (host.size() >= prefix.size())
    {
        bool match = true;
        for (std::size_t i = 0; i < prefix.size(); ++i)
        {
            if (std::tolower(static_cast<unsigned char>(host[i])) != prefix[i])
            {
                match = false;
                break;
            }
        }
        if (match)
            return host.substr(prefix.size());
    }
    return host;
}

std::string
format_endpoint(std::string const& host, uint16_t port)
{
    if (host.find(':') != std::string::npos &&
        host.find('.') == std::string::npos)
    {
        return "[" + host + "]:" + std::to_string(port);
    }
    return host + ":" + std::to_string(port);
}

bool
is_non_public_ipv4(boost::asio::ip::address_v4 const& addr)
{
    if (addr.is_unspecified() || addr.is_loopback() || addr.is_multicast())
        return true;

    auto const bytes = addr.to_bytes();
    auto const a = bytes[0];
    auto const b = bytes[1];
    auto const c = bytes[2];

    if (a == 0 || a == 10 || a == 127)
        return true;
    if (a == 169 && b == 254)
        return true;
    if (a == 172 && b >= 16 && b <= 31)
        return true;
    if (a == 192 && b == 168)
        return true;
    if (a == 100 && b >= 64 && b <= 127)
        return true;
    if (a == 198 && (b == 18 || b == 19))
        return true;
    if (a == 192 && b == 0 && c == 2)
        return true;
    if (a == 198 && b == 51 && c == 100)
        return true;
    if (a == 203 && b == 0 && c == 113)
        return true;
    if (a >= 224)
        return true;

    return false;
}

bool
is_non_public_ipv6(boost::asio::ip::address_v6 const& addr)
{
    if (addr.is_unspecified() || addr.is_loopback() || addr.is_link_local() ||
        addr.is_multicast())
        return true;

    auto const bytes = addr.to_bytes();

    if ((bytes[0] & 0xfe) == 0xfc)
        return true;

    if (bytes[0] == 0x20 && bytes[1] == 0x01 && bytes[2] == 0x0d &&
        bytes[3] == 0xb8)
        return true;

    return false;
}

std::optional<std::string>
normalize_discovered_host_impl(std::string host, uint16_t port)
{
    if (port == 0)
        return std::nullopt;

    host = strip_ipv4_mapped(host);

    boost::system::error_code ec;
    auto const addr = boost::asio::ip::make_address(host, ec);
    if (!ec)
    {
        if ((addr.is_v4() && is_non_public_ipv4(addr.to_v4())) ||
            (addr.is_v6() && is_non_public_ipv6(addr.to_v6())))
        {
            return std::nullopt;
        }
    }

    return format_endpoint(host, port);
}

}  // namespace

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
EndpointTracker::add_discovered(
    std::string const& endpoint,
    DiscoverySource source)
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
        observer(endpoint, source);
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

std::string
EndpointTracker::canonical_endpoint(std::string const& endpoint)
{
    std::string host;
    uint16_t port = 0;
    if (!parse_endpoint(endpoint, host, port))
        return endpoint;

    host = strip_ipv4_mapped(host);
    return format_endpoint(host, port);
}

std::optional<std::string>
EndpointTracker::normalize_discovered_endpoint(std::string const& endpoint)
{
    std::string host;
    uint16_t port = 0;
    if (!parse_endpoint(endpoint, host, port) || port == 0)
        return std::nullopt;

    return normalize_discovered_host(host, port);
}

std::optional<std::string>
EndpointTracker::normalize_discovered_host(
    std::string const& host,
    uint16_t port)
{
    std::string parsed_host;
    uint16_t parsed_port = 0;
    if (parse_endpoint(host, parsed_host, parsed_port))
        return normalize_discovered_host_impl(parsed_host, parsed_port);

    return normalize_discovered_host_impl(host, port);
}

}  // namespace catl::peer_client
