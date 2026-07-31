#include <catl/peer-client/endpoint-tracker.h>

#include <boost/asio/ip/address.hpp>

#include <algorithm>
#include <cctype>
#include <limits>

namespace catl::peer_client {

namespace {

bool
is_ipv4_mapped(boost::asio::ip::address_v6 const& addr)
{
    auto const bytes = addr.to_bytes();
    return std::all_of(
               bytes.begin(),
               bytes.begin() + 10,
               [](auto byte) { return byte == 0; }) &&
        bytes[10] == 0xff && bytes[11] == 0xff;
}

bool
is_ipv6_literal(std::string const& host)
{
    boost::system::error_code ec;
    auto const address = boost::asio::ip::make_address(host, ec);
    return !ec && address.is_v6();
}

bool
is_valid_dns_hostname(std::string_view host)
{
    if (host.empty() || host.size() > 253)
        return false;

    std::size_t label_start = 0;
    while (label_start < host.size())
    {
        auto const label_end = host.find('.', label_start);
        auto const end =
            label_end == std::string_view::npos ? host.size() : label_end;
        auto const label_size = end - label_start;
        if (label_size == 0 || label_size > 63 || host[label_start] == '-' ||
            host[end - 1] == '-')
        {
            return false;
        }
        for (auto i = label_start; i < end; ++i)
        {
            auto const ch = static_cast<unsigned char>(host[i]);
            auto const ascii_alnum = (ch >= 'a' && ch <= 'z') ||
                (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9');
            if (!(ascii_alnum || ch == '-'))
                return false;
        }
        if (label_end == std::string_view::npos)
            return true;
        label_start = label_end + 1;
    }
    return false;
}

std::optional<std::string>
canonicalize_host(std::string host)
{
    if (host.empty())
        return std::nullopt;

    boost::system::error_code ec;
    auto const addr = boost::asio::ip::make_address(host, ec);
    if (!ec)
    {
        if (addr.is_v6() && is_ipv4_mapped(addr.to_v6()))
        {
            auto const bytes = addr.to_v6().to_bytes();
            boost::asio::ip::address_v4::bytes_type v4_bytes{
                bytes[12], bytes[13], bytes[14], bytes[15]};
            return boost::asio::ip::address_v4(v4_bytes).to_string();
        }
        return addr.to_string();
    }

    // A terminal dot is valid DNS notation but is not part of the canonical
    // endpoint identity. Removing it also coalesces absolute and relative
    // spellings of the same configured peer.
    if (host.back() == '.')
        host.pop_back();
    if (!is_valid_dns_hostname(host))
        return std::nullopt;

    std::transform(
        host.begin(), host.end(), host.begin(), [](unsigned char ch) {
            return static_cast<char>(std::tolower(ch));
        });
    return host;
}

std::string
format_endpoint(std::string const& host, uint16_t port)
{
    if (host.find(':') != std::string::npos)
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

    auto canonical = canonicalize_host(std::move(host));
    if (!canonical)
        return std::nullopt;

    boost::system::error_code ec;
    auto const addr = boost::asio::ip::make_address(*canonical, ec);
    if (!ec)
    {
        if ((addr.is_v4() && is_non_public_ipv4(addr.to_v4())) ||
            (addr.is_v6() && is_non_public_ipv6(addr.to_v6())))
        {
            return std::nullopt;
        }
    }

    return format_endpoint(*canonical, port);
}

bool
parse_port(std::string_view text, uint16_t& port)
{
    if (text.empty() ||
        !std::all_of(text.begin(), text.end(), [](unsigned char ch) {
            return std::isdigit(ch) != 0;
        }))
    {
        return false;
    }

    try
    {
        auto const value = std::stoul(std::string(text));
        if (value > std::numeric_limits<uint16_t>::max())
            return false;
        port = static_cast<uint16_t>(value);
        return true;
    }
    catch (...)
    {
        return false;
    }
}

}  // namespace

void
EndpointTracker::update(std::string const& endpoint, PeerStatus status)
{
    auto const key = normalize_endpoint_identity(endpoint);
    if (!key)
        return;

    StatusObserver observer;
    {
        std::lock_guard lock(mutex_);
        status.last_seen = std::chrono::steady_clock::now();
        peers_[*key] = status;
        observer = status_observer_;
    }

    if (observer)
    {
        observer(*key, status);
    }
}

void
EndpointTracker::remove(std::string const& endpoint)
{
    auto const key = normalize_endpoint_identity(endpoint);
    if (!key)
        return;
    std::lock_guard lock(mutex_);
    peers_.erase(*key);
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
    auto const key = normalize_endpoint_identity(endpoint);
    if (!key)
        return std::nullopt;
    std::lock_guard lock(mutex_);

    auto it = peers_.find(*key);
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
    auto const key = normalize_endpoint_identity(endpoint);
    if (!key)
        return;

    DiscoveredObserver observer;
    {
        std::lock_guard lock(mutex_);
        auto& status = peers_[*key];
        status.last_seen = std::chrono::steady_clock::now();
        observer = discovered_observer_;
    }

    if (observer)
    {
        observer(*key, source);
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
        if (host.empty() || close + 1 >= endpoint.size() ||
            endpoint[close + 1] != ':')
            return false;
        return parse_port(std::string_view(endpoint).substr(close + 2), port);
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

        return !host.empty() && parse_port(port_part, port);
    }

    // IPv4 or hostname: host:port
    auto colon = endpoint.rfind(':');
    if (colon == std::string::npos)
        return false;
    host = endpoint.substr(0, colon);
    return !host.empty() &&
        parse_port(std::string_view(endpoint).substr(colon + 1), port);
}

std::string
EndpointTracker::canonical_endpoint(std::string const& endpoint)
{
    auto normalized = normalize_endpoint_identity(endpoint);
    return normalized ? std::move(*normalized) : endpoint;
}

std::optional<std::string>
EndpointTracker::normalize_endpoint_identity(std::string const& endpoint)
{
    std::string host;
    uint16_t port = 0;
    if (!parse_endpoint(endpoint, host, port) || port == 0)
        return std::nullopt;
    if (endpoint.front() == '[' && !is_ipv6_literal(host))
        return std::nullopt;
    auto canonical = canonicalize_host(std::move(host));
    if (!canonical)
        return std::nullopt;
    return format_endpoint(*canonical, port);
}

std::optional<std::string>
EndpointTracker::normalize_endpoint_identity(
    std::string const& host,
    uint16_t port)
{
    if (host.empty() || port == 0)
        return std::nullopt;

    auto unwrapped = host;
    bool const bracketed = host.front() == '[' || host.back() == ']';
    if (bracketed)
    {
        if (host.size() < 3 || host.front() != '[' || host.back() != ']')
            return std::nullopt;
        unwrapped = host.substr(1, host.size() - 2);
    }
    if (unwrapped.empty() || unwrapped.find('[') != std::string::npos ||
        unwrapped.find(']') != std::string::npos)
        return std::nullopt;
    if (bracketed && !is_ipv6_literal(unwrapped))
        return std::nullopt;
    auto canonical = canonicalize_host(std::move(unwrapped));
    if (!canonical)
        return std::nullopt;
    return format_endpoint(*canonical, port);
}

std::optional<std::string>
EndpointTracker::normalize_discovered_endpoint(std::string const& endpoint)
{
    std::string host;
    uint16_t port = 0;
    if (!parse_endpoint(endpoint, host, port) || port == 0)
        return std::nullopt;
    if (endpoint.front() == '[' && !is_ipv6_literal(host))
        return std::nullopt;

    return normalize_discovered_host(host, port);
}

std::optional<std::string>
EndpointTracker::normalize_discovered_host(
    std::string const& host,
    uint16_t port)
{
    if (canonicalize_host(host))
        return normalize_discovered_host_impl(host, port);

    std::string parsed_host;
    uint16_t parsed_port = 0;
    if (parse_endpoint(host, parsed_host, parsed_port) && parsed_port == port)
        return normalize_discovered_host_impl(parsed_host, parsed_port);

    return std::nullopt;
}

}  // namespace catl::peer_client
