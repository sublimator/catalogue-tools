#include "peer-set-test-access.h"

#include <catl/peer-client/endpoint-tracker.h>
#include <catl/peer-client/peer-endpoint-cache.h>
#include <catl/peer-client/peer-set.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>
#include <sqlite3.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <limits>
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

TEST(PeerEndpointCache, SupportedSchemaUpgradePreservesRetryEvidence)
{
    TempCachePath temp{"peer-endpoint-cache-schema-upgrade"};
    constexpr uint32_t network_id = 42;
    auto const direct = std::string{"45.139.107.65:21337"};
    auto const mapped = std::string{"[::ffff:45.139.107.65]:21337"};
    auto const covering = PeerStatus{
        .first_seq = 1,
        .last_seq = 1000,
        .current_seq = 1000,
    };

    {
        auto cache = PeerEndpointCache::open(temp.string());
        cache->remember_status(network_id, direct, covering);
        cache->remember_connect_failure(network_id, mapped);
        EXPECT_EQ(cache->count_endpoints(network_id), 1u);
    }

    exec_sql(temp.string(), "PRAGMA user_version = 1;");

    auto cache = PeerEndpointCache::open(temp.string());
    EXPECT_EQ(cache->count_endpoints(network_id), 1u)
        << "the bounded-schema upgrade must preserve the canonical row";
    auto failures = cache->load_active_failures(network_id);
    ASSERT_EQ(failures.size(), 1u);
    EXPECT_EQ(failures.front().endpoint, direct);
    EXPECT_EQ(failures.front().failure_count, 1u);
}

TEST(PeerEndpointCache, FutureSchemaVersionIsRefusedWithoutDowngrade)
{
    TempCachePath temp{"peer-endpoint-cache-future-schema"};
    {
        auto cache = PeerEndpointCache::open(temp.string());
        cache->remember_connect_failure(42, "failure.example.com:51235");
    }
    exec_sql(temp.string(), "PRAGMA user_version = 999;");

    EXPECT_THROW(
        (void)PeerEndpointCache::open(temp.string()), std::runtime_error);

    exec_sql(temp.string(), "PRAGMA user_version = 2;");
    auto cache = PeerEndpointCache::open(temp.string());
    auto failures = cache->load_active_failures(42);
    ASSERT_EQ(failures.size(), 1u)
        << "refusing a future schema must not destructively reinterpret it";
    EXPECT_EQ(failures.front().endpoint, "failure.example.com:51235");
}

TEST(PeerEndpointCache, CanonicalIdentityCoversHostnamesAndNumericAddresses)
{
    TempCachePath temp{"peer-endpoint-cache-canonical-identity"};
    auto cache = PeerEndpointCache::open(temp.string());
    constexpr uint32_t network_id = 42;
    auto const covering = PeerStatus{
        .first_seq = 1,
        .last_seq = 1000,
        .current_seq = 1000,
    };

    cache->remember_status(network_id, "History.Example.COM:51235", covering);
    cache->remember_connect_failure(network_id, "history.example.com:51235");
    cache->remember_status(
        network_id, "[2001:4860:4860:0:0:0:0:8888]:51235", covering);
    cache->remember_connect_failure(network_id, "[2001:4860:4860::8888]:51235");
    cache->remember_status(network_id, "45.139.107.65:21337", covering);
    cache->remember_connect_failure(
        network_id, "[0:0:0:0:0:ffff:2d8b:6b41]:21337");

    EXPECT_EQ(cache->count_endpoints(network_id), 3u);
    auto failures = index_entries(cache->load_active_failures(network_id));
    EXPECT_EQ(failures.size(), 3u);
    EXPECT_TRUE(failures.contains("history.example.com:51235"));
    EXPECT_TRUE(failures.contains("[2001:4860:4860::8888]:51235"));
    EXPECT_TRUE(failures.contains("45.139.107.65:21337"));
}

TEST(PeerEndpointCache, RetentionAndQueryPopulationAreHardBounded)
{
    TempCachePath temp{"peer-endpoint-cache-hard-bound"};
    constexpr uint32_t network_id = 42;
    {
        auto cache = PeerEndpointCache::open(temp.string());
        EXPECT_EQ(cache->count_endpoints(network_id), 0u);
    }
    exec_sql(
        temp.string(),
        "WITH RECURSIVE n(value) AS ("
        "  SELECT 0 UNION ALL SELECT value + 1 FROM n WHERE value < 4096"
        ") "
        "INSERT INTO peer_endpoints(network_id, endpoint, last_seen_at) "
        "SELECT 42, printf('peer-%04d.example.com:51235', value), value "
        "FROM n;");

    auto cache = PeerEndpointCache::open(temp.string());
    EXPECT_EQ(
        cache->count_endpoints(network_id),
        PeerEndpointCache::max_endpoints_per_network);
    auto bounded = cache->load_bootstrap_candidates(
        network_id, std::numeric_limits<std::size_t>::max());
    EXPECT_EQ(bounded.size(), PeerEndpointCache::max_endpoints_per_network);

    cache->remember_status(
        network_id,
        "192.168.1.1:51235",
        PeerStatus{.first_seq = 1, .last_seq = 2, .current_seq = 2});
    cache->remember_status(
        network_id,
        "malformed-endpoint",
        PeerStatus{.first_seq = 1, .last_seq = 2, .current_seq = 2});
    EXPECT_EQ(
        cache->count_endpoints(network_id),
        PeerEndpointCache::max_endpoints_per_network)
        << "every write path must preserve the hard retention bound";
}

TEST(
    PeerEndpointCache,
    PrivateFailureIsRetainedWithoutStarvingPublicSearchCandidates)
{
    TempCachePath temp{"peer-endpoint-cache-private-evidence"};
    constexpr uint32_t network_id = 42;
    constexpr uint32_t wanted = 500;
    auto const covering = PeerStatus{
        .first_seq = 1,
        .last_seq = 1000,
        .current_seq = 1000,
    };
    auto const private_search = std::string{"127.0.0.1:51235"};
    auto const private_failure = std::string{"127.0.0.2:51235"};
    auto const public_search = std::string{"history.example.com:51235"};

    auto cache = PeerEndpointCache::open(temp.string());
    cache->remember_connect_success(network_id, private_search, covering);
    cache->remember_connect_failure(network_id, private_failure);
    cache->remember_status(network_id, public_search, covering);

    auto active_failures =
        index_entries(cache->load_active_failures(network_id));
    ASSERT_TRUE(active_failures.contains(private_failure))
        << "admitted private peers still own durable retry evidence";

    auto candidates = cache->load_covering_candidates(network_id, wanted, 1);
    ASSERT_EQ(candidates.size(), 1u);
    EXPECT_EQ(candidates.front().endpoint, public_search)
        << "a high-ranked private row cannot consume the bounded public "
           "search result";
}

TEST(PeerEndpointCache, ActiveFailureDisplacesHealthyHintAtHardBound)
{
    TempCachePath temp{"peer-endpoint-cache-failure-retention-bound"};
    constexpr uint32_t network_id = 42;
    {
        auto cache = PeerEndpointCache::open(temp.string());
    }
    exec_sql(
        temp.string(),
        "WITH RECURSIVE n(value) AS ("
        "  SELECT 0 UNION ALL SELECT value + 1 FROM n WHERE value < 4095"
        ") "
        "INSERT INTO peer_endpoints("
        "  network_id, endpoint, first_seq, last_seq, current_seq, "
        "  last_seen_at, last_success_at, success_count, connected_ok"
        ") "
        "SELECT 42, printf('healthy-%04d.example.com:51235', value), "
        "       1, 1000, 1000, value + 1, value + 1, 1, 1 "
        "FROM n;");

    auto cache = PeerEndpointCache::open(temp.string());
    ASSERT_EQ(
        cache->count_endpoints(network_id),
        PeerEndpointCache::max_endpoints_per_network);
    cache->remember_connect_failure(network_id, "127.0.0.2:51235");

    EXPECT_EQ(
        cache->count_endpoints(network_id),
        PeerEndpointCache::max_endpoints_per_network);
    auto failures = index_entries(cache->load_active_failures(network_id));
    ASSERT_TRUE(failures.contains("127.0.0.2:51235"))
        << "active failure evidence must displace a reconstructible healthy "
           "hint at capacity";
}

TEST(PeerEndpointCache, NewestFailuresWinWhenFailuresExceedHardBound)
{
    TempCachePath temp{"peer-endpoint-cache-all-failures-bound"};
    constexpr uint32_t network_id = 42;
    {
        auto cache = PeerEndpointCache::open(temp.string());
    }
    exec_sql(
        temp.string(),
        "WITH RECURSIVE n(value) AS ("
        "  SELECT 0 UNION ALL SELECT value + 1 FROM n WHERE value < 4096"
        ") "
        "INSERT INTO peer_endpoints("
        "  network_id, endpoint, last_seen_at, last_failure_at, "
        "  failure_count, connected_ok"
        ") "
        "SELECT 42, printf('failed-%04d.example.com:51235', value), "
        "       value + 1, value + 1, 1, 0 "
        "FROM n;");

    auto cache = PeerEndpointCache::open(temp.string());
    auto failures = index_entries(cache->load_active_failures(network_id));
    EXPECT_EQ(failures.size(), PeerEndpointCache::max_endpoints_per_network);
    EXPECT_FALSE(failures.contains("failed-0000.example.com:51235"));
    EXPECT_TRUE(failures.contains("failed-4096.example.com:51235"));
}

TEST(PeerEndpointCache, SameSecondLaterFailureRemainsActive)
{
    TempCachePath temp{"peer-endpoint-cache-same-second-failure"};
    constexpr uint32_t network_id = 42;
    constexpr uint32_t wanted = 500;
    auto const failed = std::string{"failed.example.com:51235"};
    auto const healthy = std::string{"healthy.example.com:51235"};
    auto const covering = PeerStatus{
        .first_seq = 1,
        .last_seq = 1000,
        .current_seq = 1000,
    };

    auto cache = PeerEndpointCache::open(temp.string());
    cache->remember_connect_success(network_id, failed, covering);
    cache->remember_connect_failure(network_id, failed);
    cache->remember_status(network_id, healthy, covering);
    exec_sql(
        temp.string(),
        "UPDATE peer_endpoints "
        "SET last_success_at = CAST(strftime('%s','now') AS INTEGER), "
        "    last_failure_at = CAST(strftime('%s','now') AS INTEGER), "
        "    success_count = 1, failure_count = 1, connected_ok = 0 "
        "WHERE network_id = 42 AND endpoint = "
        "'failed.example.com:51235';");

    auto ranked = cache->load_covering_candidates(network_id, wanted, 1);
    ASSERT_EQ(ranked.size(), 1u);
    EXPECT_EQ(ranked.front().endpoint, healthy)
        << "connected_ok breaks equal-second event ties";

    boost::asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = network_id,
            .endpoint_cache_path = temp.string(),
            .cached_endpoint_limit = 8,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto queued =
        PeerSetTestAccess::try_candidates_for(io, peers, wanted, {}, 2);
    EXPECT_EQ(std::ranges::count(queued, failed), 0u);
    EXPECT_TRUE(PeerSetTestAccess::has_failed(io, peers, failed))
        << "equal-second durable failure must reconstruct retry backoff";
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
    auto const distinct = std::string{"46.139.107.65:21337"};
    cache->remember_status(network_id, direct, covering);
    cache->remember_status(network_id, mapped, covering);
    cache->remember_status(network_id, distinct, covering);
    cache->remember_status(network_id + 1, "other-network:1", covering);

    auto bootstrap = cache->load_bootstrap_candidates(network_id, 64);
    ASSERT_EQ(bootstrap.size(), 64u);
    EXPECT_TRUE(std::ranges::none_of(bootstrap, [wanted](auto const& entry) {
        return entry.status.first_seq <= wanted &&
            wanted <= entry.status.last_seq;
    }));

    auto targeted = cache->load_covering_candidates(network_id, wanted, 64);
    ASSERT_EQ(targeted.size(), 2u);
    EXPECT_EQ(
        std::ranges::count_if(
            targeted,
            [&](auto const& entry) { return entry.endpoint == direct; }),
        1u)
        << "aliases must be deduplicated before applying the logical limit";
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
    auto const after = PeerSetTestAccess::snapshot(io, peers);
    EXPECT_EQ(after.queued_crawls, 0u)
        << "a target wait must not mint an uncensused crawl population";
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

TEST(PeerEndpointCache, TargetedLookupRetainsPersistentFailureBackoff)
{
    TempCachePath temp{"peer-endpoint-cache-targeted-backoff"};
    auto cache = PeerEndpointCache::open(temp.string());
    constexpr uint32_t network_id = 42;
    constexpr uint32_t wanted = 500;
    auto const failed = std::string{"failed-history.example.com:51235"};
    auto const healthy = std::string{"healthy-history.example.com:51235"};
    auto const covering = PeerStatus{
        .first_seq = 1,
        .last_seq = 1000,
        .current_seq = 1000,
    };
    cache->remember_status(network_id, failed, covering);
    cache->remember_connect_failure(network_id, failed);
    cache->remember_status(network_id, healthy, covering);

    auto ranked = cache->load_covering_candidates(network_id, wanted, 1);
    ASSERT_EQ(ranked.size(), 1u);
    EXPECT_EQ(ranked.front().endpoint, healthy)
        << "a newer failure than success must be a leading ranking penalty";

    boost::asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = network_id,
            .endpoint_cache_path = temp.string(),
            .cached_endpoint_limit = 1,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    peers->start();

    PeerSetTestAccess::set_status(io, peers, failed, covering);
    auto queued =
        PeerSetTestAccess::try_candidates_for(io, peers, wanted, {}, 2);
    EXPECT_EQ(std::ranges::count(queued, failed), 0u)
        << "the durable failure timestamp must reconstruct active backoff";
    EXPECT_TRUE(PeerSetTestAccess::has_failed(io, peers, failed));
    EXPECT_FALSE(PeerSetTestAccess::has_failed(io, peers, healthy))
        << "startup must restore the complete bounded active-failure "
           "population, not reinterpret every cached row as failed";

    PeerSetTestAccess::prune_discovery_state(io, peers);
    EXPECT_TRUE(PeerSetTestAccess::has_failed(io, peers, failed))
        << "cache-only backoff must not depend on live tracker membership";
}

TEST(PeerEndpointCache, ZeroCandidateLimitStillRestoresRetryEvidence)
{
    TempCachePath temp{"peer-endpoint-cache-zero-candidates-backoff"};
    constexpr uint32_t network_id = 42;
    constexpr uint32_t wanted = 500;
    auto const failed = std::string{"failed-history.example.com:51235"};
    auto const private_failed = std::string{"127.0.0.2:51235"};
    auto const covering = PeerStatus{
        .first_seq = 1,
        .last_seq = 1000,
        .current_seq = 1000,
    };
    {
        auto cache = PeerEndpointCache::open(temp.string());
        cache->remember_status(network_id, failed, covering);
        cache->remember_connect_failure(network_id, failed);
        cache->remember_connect_failure(network_id, private_failed);
    }

    boost::asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = network_id,
            .endpoint_cache_path = temp.string(),
            .cached_endpoint_limit = 0,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    peers->start();
    drain_io(io);

    PeerSetTestAccess::set_status(io, peers, failed, covering);
    auto queued =
        PeerSetTestAccess::try_candidates_for(io, peers, wanted, {}, 2);
    EXPECT_EQ(std::ranges::count(queued, failed), 0u)
        << "zero candidate import cannot erase independent retry evidence";
    EXPECT_TRUE(PeerSetTestAccess::has_failed(io, peers, failed));

    peers->start_connect("127.0.0.2", 51235);
    drain_io(io);
    EXPECT_TRUE(PeerSetTestAccess::has_failed(io, peers, private_failed));
    EXPECT_EQ(PeerSetTestAccess::snapshot(io, peers).queued_connects, 0u)
        << "a configured private peer must observe its durable backoff even "
           "when cached candidate import is disabled";
}

TEST(PeerEndpointCache, DisabledQueryNeverDegradesToHealthyEmpty)
{
    TempCachePath temp{"peer-endpoint-cache-disabled-query"};
    auto cache = PeerEndpointCache::open(temp.string());
    cache->remember_status(
        42,
        "history.example.com:51235",
        PeerStatus{
            .first_seq = 1,
            .last_seq = 1000,
            .current_seq = 1000,
        });
    exec_sql(temp.string(), "DROP TABLE peer_endpoints;");

    EXPECT_THROW(cache->load_covering_candidates(42, 500, 4), std::exception);
    auto const health = cache->health();
    EXPECT_FALSE(health.available);
    EXPECT_FALSE(health.error.empty());
    EXPECT_THROW(cache->load_covering_candidates(42, 500, 4), std::exception)
        << "the latched failure must not become a later empty success";
}

TEST(PeerEndpointCache, InFlightTargetRetainsLedgerForSnapshot)
{
    TempCachePath temp{"peer-endpoint-cache-target-in-flight"};
    auto cache = PeerEndpointCache::open(temp.string());
    constexpr uint32_t network_id = 42;
    constexpr uint32_t wanted = 500;
    auto const endpoint = std::string{"45.139.107.65:21337"};
    cache->remember_status(
        network_id,
        endpoint,
        PeerStatus{
            .first_seq = 1,
            .last_seq = 1000,
            .current_seq = 1000,
        });

    boost::asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = network_id,
            .endpoint_cache_path = temp.string(),
            .cached_endpoint_limit = 64,
            .max_in_flight_connects = 1,
            .max_in_flight_crawls = 0,
        });

    auto [queued, snapshot] =
        PeerSetTestAccess::try_candidates_and_snapshot_in_same_owner_turn(
            io, peers, wanted, 1);
    ASSERT_EQ(queued.size(), 1u);
    EXPECT_EQ(queued.front(), endpoint);
    EXPECT_EQ(snapshot.targeted_connects, 1u);

    auto const peer = std::ranges::find_if(
        snapshot.peers,
        [&](auto const& entry) { return entry.endpoint == endpoint; });
    ASSERT_NE(peer, snapshot.peers.end());
    EXPECT_TRUE(peer->in_flight);
    EXPECT_TRUE(peer->targeted_connect);
    EXPECT_EQ(peer->targeted_ledgers, (std::vector<uint32_t>{wanted}));
}

TEST(PeerEndpointCache, PublicTargetWaitLeavesNoCrawlOrAuthorizationResidue)
{
    TempCachePath temp{"peer-endpoint-cache-public-target-wait"};
    auto cache = PeerEndpointCache::open(temp.string());
    constexpr uint32_t network_id = 42;
    constexpr uint32_t wanted = 500;
    cache->remember_status(
        network_id,
        "45.139.107.65:21337",
        PeerStatus{
            .first_seq = 1,
            .last_seq = 1000,
            .current_seq = 1000,
        });

    boost::asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = network_id,
            .endpoint_cache_path = temp.string(),
            .cached_endpoint_limit = 64,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 4,
            .max_targeted_connect_attempts = 1,
        });

    auto waited = boost::asio::co_spawn(
        io, peers->wait_for_covering_peer(wanted, 0), boost::asio::use_future);
    io.run();
    EXPECT_FALSE(waited.get());

    io.restart();
    auto const snapshot = PeerSetTestAccess::snapshot(io, peers);
    EXPECT_EQ(snapshot.queued_crawls, 0u);
    EXPECT_EQ(snapshot.crawl_in_flight, 0u);
    EXPECT_EQ(snapshot.targeted_connects, 0u);
    EXPECT_TRUE(snapshot.wanted_ledgers.empty());
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
    cache->remember_status(network_id, mapped, covering);

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
