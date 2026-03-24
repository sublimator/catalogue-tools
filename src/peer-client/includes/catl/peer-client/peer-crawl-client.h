#pragma once

#include <boost/asio/awaitable.hpp>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace catl::peer_client {

struct CrawlLedgerRange
{
    uint32_t first_seq = 0;
    uint32_t last_seq = 0;
};

struct CrawlPeer
{
    std::string endpoint;
    std::vector<CrawlLedgerRange> complete_ledgers;
};

struct CrawlResponse
{
    bool used_tls = false;
    std::vector<CrawlPeer> peers;
};

std::vector<CrawlLedgerRange>
parse_complete_ledgers(std::string_view value);

boost::asio::awaitable<CrawlResponse>
co_fetch_peer_crawl(std::string host, uint16_t port);

}  // namespace catl::peer_client
