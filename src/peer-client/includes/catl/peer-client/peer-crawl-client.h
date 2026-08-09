#pragma once

#include <boost/asio/awaitable.hpp>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <vector>

namespace catl::peer_client {

inline constexpr std::size_t kMaxCrawlBodyBytes = 4 * 1024 * 1024;
inline constexpr std::size_t kMaxCrawlPeersPerBatch = 4096;
inline constexpr std::size_t kMaxCrawlIntervalsPerPeer = 256;
inline constexpr std::size_t kMaxCrawlPublicKeyChars = 128;
inline constexpr std::size_t kMaxCrawlObservations = 4096;

struct CrawlParseLimits
{
    /// Test seams may lower these limits; the parser always clamps them to the
    /// production ceilings above.
    std::size_t peers = kMaxCrawlPeersPerBatch;
    std::size_t intervals_per_peer = kMaxCrawlIntervalsPerPeer;
    std::size_t public_key_chars = kMaxCrawlPublicKeyChars;
};

struct CrawlLedgerRange
{
    uint32_t first_seq = 0;
    uint32_t last_seq = 0;
};

struct CrawlPeer
{
    std::string endpoint;
    std::optional<std::string> reported_public_key;
    /// Ordered, strict, bounded legacy projection. Kept separate until the
    /// atomic evidence cutover so canonical diagnostics cannot alter selection.
    std::vector<CrawlLedgerRange> complete_ledgers;
    /// Canonical diagnostic-only interval set derived from complete_ledgers.
    std::vector<CrawlLedgerRange> reported_intervals;
    bool truncated = false;
};

struct CrawlResponse
{
    bool used_tls = false;
    std::vector<CrawlPeer> peers;
    std::size_t reported_peer_rows = 0;
    std::size_t truncated_peer_rows = 0;
    std::size_t truncated_interval_rows = 0;
    std::size_t truncated_public_keys = 0;
    std::size_t invalid_public_keys = 0;
    std::size_t malformed_interval_tokens = 0;
};

std::vector<CrawlLedgerRange>
parse_complete_ledgers(std::string_view value);

/// Parse one already-bounded /crawl body into a canonical, bounded batch.
/// Reported keys remain untrusted metadata even when syntactically valid.
CrawlResponse
parse_peer_crawl_body(
    std::string_view body,
    CrawlParseLimits const& limits = {});

std::string
summarize_crawl_error(std::string_view error);

boost::asio::awaitable<CrawlResponse>
co_fetch_peer_crawl(std::string host, uint16_t port);

namespace detail {

struct CrawlFetchHooks
{
    std::function<void()> before_stop_check;
    std::function<void()> before_https_fallback;
};

boost::asio::awaitable<CrawlResponse>
co_fetch_peer_crawl_with_stop(
    std::string host,
    uint16_t port,
    std::stop_token stop,
    CrawlFetchHooks hooks = {});

}  // namespace detail

}  // namespace catl::peer_client
