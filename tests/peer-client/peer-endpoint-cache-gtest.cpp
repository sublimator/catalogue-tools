#include <catl/peer-client/endpoint-tracker.h>
#include <catl/peer-client/peer-endpoint-cache.h>
#include <catl/peer-client/peer-set.h>

#include <boost/asio/io_context.hpp>
#include <gtest/gtest.h>
#include <sqlite3.h>

#include <chrono>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

using namespace catl::peer_client;

namespace {

class TempCachePath
{
public:
    explicit TempCachePath(std::string stem)
    {
        auto const nonce =
            std::chrono::high_resolution_clock::now().time_since_epoch().count();
        path_ = std::filesystem::temp_directory_path() /
            (std::move(stem) + "-" + std::to_string(nonce) + ".sqlite3");
    }

    ~TempCachePath()
    {
        std::error_code ec;
        std::filesystem::remove(path_, ec);
    }

    std::string
    string() const
    {
        return path_.string();
    }

private:
    std::filesystem::path path_;
};

std::unordered_map<std::string, PeerEndpointCache::Entry>
index_entries(std::vector<PeerEndpointCache::Entry> entries)
{
    std::unordered_map<std::string, PeerEndpointCache::Entry> indexed;
    for (auto& entry : entries)
    {
        indexed.emplace(entry.endpoint, std::move(entry));
    }
    return indexed;
}

void
exec_sql(std::string const& path, std::string const& sql)
{
    sqlite3* db = nullptr;
    ASSERT_EQ(
        sqlite3_open_v2(
            path.c_str(),
            &db,
            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
            nullptr),
        SQLITE_OK);

    char* err = nullptr;
    auto const rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &err);
    std::string const message = err ? err : sqlite3_errmsg(db);
    if (err)
        sqlite3_free(err);
    sqlite3_close(db);
    ASSERT_EQ(rc, SQLITE_OK) << message;
}

void
drain_io(boost::asio::io_context& io)
{
    for (int i = 0; i < 8; ++i)
    {
        io.poll();
        io.restart();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

}  // namespace

TEST(PeerEndpointCache, ConnectFailureClearsConnectedOkAndKeepsSourceFlags)
{
    TempCachePath temp{"peer-endpoint-cache-flags"};
    auto cache = PeerEndpointCache::open(temp.string());

    cache->remember_seen_crawl(0, "peer-a.example.com:51235");
    cache->remember_seen_redirect(0, "peer-a.example.com:51235");
    cache->remember_connect_success(
        0,
        "peer-a.example.com:51235",
        PeerStatus{
            .first_seq = 100,
            .last_seq = 200,
            .current_seq = 150,
        });
    cache->remember_connect_failure(0, "peer-a.example.com:51235");

    auto entries = index_entries(cache->load_bootstrap_candidates(0, 8));
    ASSERT_TRUE(entries.count("peer-a.example.com:51235") > 0);

    auto const& entry = entries.at("peer-a.example.com:51235");
    EXPECT_FALSE(entry.connected_ok);
    EXPECT_TRUE(entry.seen_crawl);
    EXPECT_TRUE(entry.seen_redirect);
    EXPECT_FALSE(entry.seen_endpoints);
    EXPECT_EQ(entry.success_count, 1u);
    EXPECT_EQ(entry.failure_count, 1u);
}

TEST(PeerEndpointCache, RankingPenalizesLegacyStaleConnectedOkRows)
{
    TempCachePath temp{"peer-endpoint-cache-ranking"};
    auto cache = PeerEndpointCache::open(temp.string());

    auto const status = PeerStatus{
        .first_seq = 100,
        .last_seq = 200,
        .current_seq = 150,
    };

    cache->remember_connect_success(0, "legacy-stale.example.com:51235", status);
    cache->remember_connect_success(0, "healthy.example.com:51235", status);

    exec_sql(
        temp.string(),
        "UPDATE peer_endpoints "
        "SET last_success_at = 100, last_failure_at = 200, connected_ok = 1, "
        "    success_count = 1, failure_count = 1 "
        "WHERE network_id = 0 AND endpoint = 'legacy-stale.example.com:51235';");
    exec_sql(
        temp.string(),
        "UPDATE peer_endpoints "
        "SET last_success_at = 150, last_failure_at = 0, connected_ok = 1, "
        "    success_count = 1, failure_count = 0 "
        "WHERE network_id = 0 AND endpoint = 'healthy.example.com:51235';");

    auto candidates = cache->load_bootstrap_candidates(0, 8);
    ASSERT_GE(candidates.size(), 2u);
    EXPECT_EQ(candidates.front().endpoint, "healthy.example.com:51235");
}

TEST(PeerEndpointCache, PeerSetPersistenceSeparatesEndpointAndRedirectSources)
{
    TempCachePath temp{"peer-endpoint-cache-persistence"};

    boost::asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{
        .network_id = 0,
        .endpoint_cache_path = temp.string(),
        .max_in_flight_connects = 0,
        .max_in_flight_crawls = 0,
    });
    peers->start();

    peers->tracker()->add_discovered(
        "tm.example.com:51235", DiscoverySource::Endpoints);
    peers->tracker()->add_discovered(
        "redirect.example.com:51235", DiscoverySource::Redirect);
    peers->tracker()->add_discovered("unknown.example.com:51235");

    drain_io(io);

    auto cache = PeerEndpointCache::open(temp.string());
    auto entries = index_entries(cache->load_bootstrap_candidates(0, 8));

    ASSERT_TRUE(entries.count("tm.example.com:51235") > 0);
    ASSERT_TRUE(entries.count("redirect.example.com:51235") > 0);
    ASSERT_TRUE(entries.count("unknown.example.com:51235") > 0);

    EXPECT_TRUE(entries.at("tm.example.com:51235").seen_endpoints);
    EXPECT_FALSE(entries.at("tm.example.com:51235").seen_redirect);

    EXPECT_FALSE(entries.at("redirect.example.com:51235").seen_endpoints);
    EXPECT_TRUE(entries.at("redirect.example.com:51235").seen_redirect);

    EXPECT_FALSE(entries.at("unknown.example.com:51235").seen_endpoints);
    EXPECT_FALSE(entries.at("unknown.example.com:51235").seen_redirect);
}
