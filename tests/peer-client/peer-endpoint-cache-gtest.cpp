#include "peer-set-test-access.h"

#include <catl/peer-client/endpoint-tracker.h>
#include <catl/peer-client/peer-endpoint-cache.h>
#include <catl/peer-client/peer-set.h>

#include <boost/asio/io_context.hpp>
#include <gtest/gtest.h>
#include <sqlite3.h>

#include <algorithm>
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
        auto const nonce = std::chrono::high_resolution_clock::now()
                               .time_since_epoch()
                               .count();
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

    cache->remember_connect_success(
        0, "legacy-stale.example.com:51235", status);
    cache->remember_connect_success(0, "healthy.example.com:51235", status);

    exec_sql(
        temp.string(),
        "UPDATE peer_endpoints "
        "SET last_success_at = 100, last_failure_at = 200, connected_ok = 1, "
        "    success_count = 1, failure_count = 1 "
        "WHERE network_id = 0 AND endpoint = "
        "'legacy-stale.example.com:51235';");
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

TEST(
    PeerEndpointCache,
    TargetedLookupEscapesBootstrapWindowAndRemainsOnlyASearchHint)
{
    TempCachePath temp{"peer-endpoint-cache-targeted"};
    auto cache = PeerEndpointCache::open(temp.string());
    constexpr uint32_t network_id = 42;
    constexpr uint32_t wanted = 500;
    auto const recent_only = PeerStatus{
        .first_seq = 800,
        .last_seq = 900,
        .current_seq = 900,
    };
    auto const covering = PeerStatus{
        .first_seq = 1,
        .last_seq = 1000,
        .current_seq = 1000,
    };

    for (std::size_t i = 0; i < 64; ++i)
    {
        cache->remember_connect_success(
            network_id,
            "healthy-" + std::to_string(i) + ".example.com:51235",
            recent_only);
    }

    auto const direct = std::string{"45.139.107.65:21337"};
    auto const mapped = std::string{"[::ffff:45.139.107.65]:21337"};
    cache->remember_status(network_id, direct, covering);
    cache->remember_connect_failure(network_id, direct);
    cache->remember_status(network_id, mapped, covering);
    cache->remember_connect_failure(network_id, mapped);
    cache->remember_status(network_id + 1, "other-network:1", covering);

    auto bootstrap = cache->load_bootstrap_candidates(network_id, 64);
    ASSERT_EQ(bootstrap.size(), 64u);
    EXPECT_TRUE(std::ranges::none_of(bootstrap, [wanted](auto const& entry) {
        return entry.status.first_seq <= wanted &&
            wanted <= entry.status.last_seq;
    }));

    auto targeted = cache->load_covering_candidates(network_id, wanted, 64);
    ASSERT_EQ(targeted.size(), 2u);
    EXPECT_TRUE(std::ranges::all_of(targeted, [wanted](auto const& entry) {
        return entry.status.first_seq <= wanted &&
            wanted <= entry.status.last_seq;
    }));
    auto other_network =
        cache->load_covering_candidates(network_id + 1, wanted, 64);
    ASSERT_EQ(other_network.size(), 1u);
    EXPECT_EQ(other_network.front().endpoint, "other-network:1");

    boost::asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = network_id,
            .endpoint_cache_path = temp.string(),
            .cached_endpoint_limit = 64,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    peers->start();
    drain_io(io);
    ASSERT_FALSE(PeerSetTestAccess::tracker_contains(io, peers, direct));

    auto queued =
        PeerSetTestAccess::try_candidates_for(io, peers, wanted, {}, 2);
    ASSERT_EQ(queued.size(), 2u);
    EXPECT_EQ(queued.front(), direct);
    EXPECT_EQ(std::ranges::count(queued, direct), 1u)
        << "IPv4 and IPv4-mapped cache aliases are one candidate";
    EXPECT_TRUE(PeerSetTestAccess::is_targeted_connect(io, peers, direct));
    EXPECT_FALSE(PeerSetTestAccess::tracker_contains(io, peers, direct))
        << "loading a persistent hint must not counterfeit live observation";
}

TEST(PeerEndpointCache, TargetedHintCannotOverwriteNewerLiveRange)
{
    TempCachePath temp{"peer-endpoint-cache-targeted-live-wins"};
    auto cache = PeerEndpointCache::open(temp.string());
    constexpr uint32_t network_id = 42;
    constexpr uint32_t wanted = 500;

    for (std::size_t i = 0; i < 64; ++i)
    {
        cache->remember_connect_success(
            network_id,
            "healthy-" + std::to_string(i) + ".example.com:51235",
            PeerStatus{
                .first_seq = 800,
                .last_seq = 900,
                .current_seq = 900,
            });
    }
    auto const endpoint = std::string{"45.139.107.65:21337"};
    cache->remember_status(
        network_id,
        endpoint,
        PeerStatus{
            .first_seq = 1,
            .last_seq = 1000,
            .current_seq = 1000,
        });
    cache->remember_connect_failure(network_id, endpoint);

    boost::asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = network_id,
            .endpoint_cache_path = temp.string(),
            .cached_endpoint_limit = 64,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    peers->start();
    drain_io(io);
    ASSERT_FALSE(PeerSetTestAccess::tracker_contains(io, peers, endpoint));

    peers->tracker()->update(
        endpoint,
        PeerStatus{
            .first_seq = 800,
            .last_seq = 900,
            .current_seq = 900,
        });
    drain_io(io);
    auto queued =
        PeerSetTestAccess::try_candidates_for(io, peers, wanted, {}, 1);
    ASSERT_EQ(queued.size(), 1u);
    EXPECT_NE(queued.front(), endpoint);

    auto const live = peers->tracker()->get_status(endpoint);
    ASSERT_TRUE(live.has_value());
    EXPECT_EQ(live->first_seq, 800u);
    EXPECT_EQ(live->last_seq, 900u);
}

TEST(
    PeerEndpointCache,
    TargetedLookupDeduplicatesKnownUnknownAliasWithoutPublishingCachedRange)
{
    TempCachePath temp{"peer-endpoint-cache-targeted-known-unknown"};
    auto cache = PeerEndpointCache::open(temp.string());
    constexpr uint32_t network_id = 42;
    constexpr uint32_t wanted = 500;
    auto const recent_only = PeerStatus{
        .first_seq = 800,
        .last_seq = 900,
        .current_seq = 900,
    };
    auto const covering = PeerStatus{
        .first_seq = 1,
        .last_seq = 1000,
        .current_seq = 1000,
    };
    for (std::size_t i = 0; i < 64; ++i)
    {
        cache->remember_connect_success(
            network_id,
            "healthy-" + std::to_string(i) + ".example.com:51235",
            recent_only);
    }

    auto const direct = std::string{"45.139.107.65:21337"};
    auto const mapped = std::string{"[::ffff:45.139.107.65]:21337"};
    cache->remember_status(network_id, direct, covering);
    cache->remember_connect_failure(network_id, direct);
    cache->remember_status(network_id, mapped, covering);
    cache->remember_connect_failure(network_id, mapped);

    boost::asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = network_id,
            .endpoint_cache_path = temp.string(),
            .cached_endpoint_limit = 64,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    peers->start();
    drain_io(io);

    peers->tracker()->add_discovered(mapped);
    drain_io(io);
    auto const before = peers->tracker()->get_status(mapped);
    ASSERT_TRUE(before.has_value());
    EXPECT_EQ(before->first_seq, 0u);
    EXPECT_EQ(before->last_seq, 0u);

    auto queued =
        PeerSetTestAccess::try_candidates_for(io, peers, wanted, {}, 1);
    ASSERT_EQ(queued.size(), 1u);
    EXPECT_EQ(queued.front(), direct)
        << "one logical endpoint must consume only one target-attempt slot";

    auto const after = peers->tracker()->get_status(mapped);
    ASSERT_TRUE(after.has_value());
    EXPECT_EQ(after->first_seq, 0u);
    EXPECT_EQ(after->last_seq, 0u)
        << "the cached range remains a search hint, not a live observation";
}

TEST(PeerEndpointCache, PeerSetPersistenceSeparatesEndpointAndRedirectSources)
{
    TempCachePath temp{"peer-endpoint-cache-persistence"};

    boost::asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
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
