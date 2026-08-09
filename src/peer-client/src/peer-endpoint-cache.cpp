#include <catl/peer-client/peer-endpoint-cache.h>

#include <catl/core/logger.h>

#include <algorithm>
#include <exception>
#include <filesystem>
#include <mutex>
#include <optional>
#include <sqlite3.h>
#include <stdexcept>
#include <string_view>
#include <unordered_map>
#include <utility>

namespace catl::peer_client {

namespace {

LogPartition log_("peer-cache", LogLevel::INHERIT);

class SqliteError final : public std::runtime_error
{
public:
    SqliteError(int code, std::string message)
        : std::runtime_error(std::move(message)), code_(code)
    {
    }

    int
    code() const noexcept
    {
        return code_;
    }

private:
    int code_;
};

[[noreturn]] void
throw_sqlite(sqlite3* db, int code, std::string message = {})
{
    if (message.empty())
        message = db ? sqlite3_errmsg(db) : "sqlite failure";
    throw SqliteError(code, std::move(message));
}

bool
current_failure_is_transient_sqlite() noexcept
{
    try
    {
        auto failure = std::current_exception();
        if (!failure)
            return false;
        std::rethrow_exception(std::move(failure));
    }
    catch (SqliteError const& e)
    {
        auto const primary = e.code() & 0xff;
        return primary == SQLITE_BUSY || primary == SQLITE_LOCKED;
    }
    catch (...)
    {
        return false;
    }
}

struct CacheRegistry
{
    std::mutex mutex;
    std::unordered_map<std::string, std::weak_ptr<PeerEndpointCache>> caches;
};

CacheRegistry&
cache_registry()
{
    static CacheRegistry registry;
    return registry;
}

std::string
canonical_cache_path(std::string const& path)
{
    if (path == ":memory:" || path.rfind("file:", 0) == 0)
        return path;

    std::error_code ec;
    auto absolute = std::filesystem::absolute(path, ec);
    if (ec)
        absolute = std::filesystem::path(path);
    auto canonical = std::filesystem::weakly_canonical(absolute, ec);
    return (ec ? absolute.lexically_normal() : canonical).string();
}

std::optional<std::string>
persistent_cache_endpoint(std::string const& endpoint)
{
    return EndpointTracker::normalize_endpoint_identity(endpoint);
}

constexpr char kCreatePeerEndpoints[] =
    "CREATE TABLE IF NOT EXISTS peer_endpoints ("
    "  network_id INTEGER NOT NULL,"
    "  endpoint TEXT NOT NULL,"
    "  first_seq INTEGER NOT NULL DEFAULT 0,"
    "  last_seq INTEGER NOT NULL DEFAULT 0,"
    "  current_seq INTEGER NOT NULL DEFAULT 0,"
    "  last_seen_at INTEGER NOT NULL DEFAULT 0,"
    "  last_success_at INTEGER NOT NULL DEFAULT 0,"
    "  last_failure_at INTEGER NOT NULL DEFAULT 0,"
    "  success_count INTEGER NOT NULL DEFAULT 0,"
    "  failure_count INTEGER NOT NULL DEFAULT 0,"
    "  seen_crawl INTEGER NOT NULL DEFAULT 0,"
    "  seen_endpoints INTEGER NOT NULL DEFAULT 0,"
    "  seen_redirect INTEGER NOT NULL DEFAULT 0,"
    "  connected_ok INTEGER NOT NULL DEFAULT 0,"
    "  PRIMARY KEY(network_id, endpoint)"
    ");";

void
exec_sql(sqlite3* db, char const* sql)
{
    char* err = nullptr;
    auto const rc = sqlite3_exec(db, sql, nullptr, nullptr, &err);
    if (rc != SQLITE_OK)
    {
        std::string message = err ? err : sqlite3_errmsg(db);
        if (err)
        {
            sqlite3_free(err);
        }
        throw_sqlite(db, rc, std::move(message));
    }
}

class Statement
{
public:
    Statement(sqlite3* db, char const* sql)
    {
        auto const rc = sqlite3_prepare_v2(db, sql, -1, &stmt_, nullptr);
        if (rc != SQLITE_OK)
        {
            throw_sqlite(db, rc);
        }
    }

    ~Statement()
    {
        if (stmt_)
        {
            sqlite3_finalize(stmt_);
        }
    }

    sqlite3_stmt*
    get() const
    {
        return stmt_;
    }

private:
    sqlite3_stmt* stmt_ = nullptr;
};

PeerEndpointCache::Entry
read_entry(sqlite3_stmt* stmt)
{
    PeerEndpointCache::Entry entry;
    auto const* endpoint_text = sqlite3_column_text(stmt, 0);
    entry.endpoint =
        endpoint_text ? reinterpret_cast<char const*>(endpoint_text) : "";
    entry.status.first_seq = static_cast<uint32_t>(sqlite3_column_int(stmt, 1));
    entry.status.last_seq = static_cast<uint32_t>(sqlite3_column_int(stmt, 2));
    entry.status.current_seq =
        static_cast<uint32_t>(sqlite3_column_int(stmt, 3));
    entry.status.last_seen = std::chrono::steady_clock::now();
    entry.last_seen_at =
        static_cast<std::int64_t>(sqlite3_column_int64(stmt, 4));
    entry.last_success_at =
        static_cast<std::int64_t>(sqlite3_column_int64(stmt, 5));
    entry.last_failure_at =
        static_cast<std::int64_t>(sqlite3_column_int64(stmt, 6));
    entry.success_count =
        static_cast<std::uint64_t>(sqlite3_column_int64(stmt, 7));
    entry.failure_count =
        static_cast<std::uint64_t>(sqlite3_column_int64(stmt, 8));
    entry.seen_crawl = sqlite3_column_int(stmt, 9) != 0;
    entry.seen_endpoints = sqlite3_column_int(stmt, 10) != 0;
    entry.seen_redirect = sqlite3_column_int(stmt, 11) != 0;
    entry.connected_ok = sqlite3_column_int(stmt, 12) != 0;
    return entry;
}

void
require_rows_complete(sqlite3* db, int rc)
{
    if (rc != SQLITE_DONE)
        throw_sqlite(db, rc);
}

//@@authority transition=prv.T0222 status=provisional scope=function
// bind=catl::peer_client::prune_network_rows@3ee6bd2dd9577aa6
// owner=unresolved owner_hypothesis=peer_endpoint_cache
// commit=cache_writer_serialization touches=prv.S0073
// label=retain_active_failures_by_timestamp_before_reconstructible_hints
void
prune_network_rows(sqlite3* db, std::string_view table, uint32_t network_id)
{
    {
        auto const count_sql = std::string{"SELECT COUNT(*) FROM "} +
            std::string(table) + " WHERE network_id = ?";
        Statement count(db, count_sql.c_str());
        sqlite3_bind_int(count.get(), 1, static_cast<int>(network_id));
        auto const rc = sqlite3_step(count.get());
        if (rc != SQLITE_ROW)
            throw_sqlite(db, rc);
        if (sqlite3_column_int64(count.get(), 0) <=
            static_cast<sqlite3_int64>(
                PeerEndpointCache::max_endpoints_per_network))
        {
            return;
        }
    }

    auto const sql = std::string{"DELETE FROM "} + std::string(table) +
        " WHERE rowid IN ("
        "  SELECT rowid FROM " +
        std::string(table) +
        "  WHERE network_id = ? "
        "  ORDER BY "
        "    CASE WHEN failure_count > 0 AND "
        "              (last_failure_at > last_success_at OR "
        "               (last_failure_at = last_success_at AND "
        "                connected_ok = 0)) "
        "         THEN 1 ELSE 0 END DESC, "
        "    CASE WHEN failure_count > 0 AND "
        "              (last_failure_at > last_success_at OR "
        "               (last_failure_at = last_success_at AND "
        "                connected_ok = 0)) "
        "         THEN last_failure_at ELSE 0 END DESC, "
        "    CASE WHEN first_seq > 0 AND last_seq > 0 THEN 1 ELSE 0 END DESC, "
        "    connected_ok DESC, "
        "    CASE WHEN last_seq > first_seq THEN last_seq - first_seq "
        "         ELSE 0 END DESC, "
        "    last_success_at DESC, last_seen_at DESC, "
        "    (seen_crawl + seen_endpoints + seen_redirect) DESC, "
        "    endpoint ASC "
        "  LIMIT -1 OFFSET ?"
        ");";
    Statement prune(db, sql.c_str());
    sqlite3_bind_int(prune.get(), 1, static_cast<int>(network_id));
    sqlite3_bind_int64(
        prune.get(),
        2,
        static_cast<sqlite3_int64>(
            PeerEndpointCache::max_endpoints_per_network));
    auto const rc = sqlite3_step(prune.get());
    if (rc != SQLITE_DONE)
        throw_sqlite(db, rc);
}

void
prune_all_networks(sqlite3* db, std::string_view table)
{
    std::vector<uint32_t> network_ids;
    {
        auto const sql = std::string{"SELECT DISTINCT network_id FROM "} +
            std::string(table);
        Statement networks(db, sql.c_str());
        int rc = SQLITE_OK;
        while ((rc = sqlite3_step(networks.get())) == SQLITE_ROW)
        {
            network_ids.push_back(
                static_cast<uint32_t>(sqlite3_column_int(networks.get(), 0)));
        }
        require_rows_complete(db, rc);
    }

    for (auto network_id : network_ids)
        prune_network_rows(db, table, network_id);
}

//@@authority transition=prv.T0224 status=provisional scope=function
// bind=catl::peer_client::migrate_legacy_to_v3@9f7d938f183e6691
// owner=unresolved owner_hypothesis=peer_endpoint_cache
// commit=sqlite_immediate_transaction touches=prv.S0073
// label=canonicalize_legacy_endpoint_identities_and_conserve_retry_evidence
void
migrate_legacy_to_v3(sqlite3* db)
{
    exec_sql(db, "BEGIN IMMEDIATE;");
    try
    {
        exec_sql(db, "DROP TABLE IF EXISTS peer_endpoints_identity_migration;");
        exec_sql(
            db,
            "ALTER TABLE peer_endpoints RENAME TO "
            "peer_endpoints_identity_migration;");
        exec_sql(db, kCreatePeerEndpoints);

        Statement rows(
            db,
            "SELECT network_id, endpoint, first_seq, last_seq, current_seq, "
            "       last_seen_at, last_success_at, last_failure_at, "
            "       success_count, failure_count, seen_crawl, seen_endpoints, "
            "       seen_redirect, connected_ok "
            "FROM peer_endpoints_identity_migration;");
        Statement merge(
            db,
            "INSERT INTO peer_endpoints("
            "  network_id, endpoint, first_seq, last_seq, current_seq, "
            "  last_seen_at, last_success_at, last_failure_at, "
            "  success_count, failure_count, seen_crawl, seen_endpoints, "
            "  seen_redirect, connected_ok"
            ") VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(network_id, endpoint) DO UPDATE SET "
            "  first_seq = CASE "
            "    WHEN excluded.first_seq > 0 AND excluded.last_seq > 0 AND "
            "         (peer_endpoints.first_seq = 0 OR "
            "          peer_endpoints.last_seq = 0 OR "
            "          excluded.last_seen_at >= peer_endpoints.last_seen_at) "
            "    THEN excluded.first_seq ELSE peer_endpoints.first_seq END, "
            "  last_seq = CASE "
            "    WHEN excluded.first_seq > 0 AND excluded.last_seq > 0 AND "
            "         (peer_endpoints.first_seq = 0 OR "
            "          peer_endpoints.last_seq = 0 OR "
            "          excluded.last_seen_at >= peer_endpoints.last_seen_at) "
            "    THEN excluded.last_seq ELSE peer_endpoints.last_seq END, "
            "  current_seq = CASE "
            "    WHEN excluded.first_seq > 0 AND excluded.last_seq > 0 AND "
            "         (peer_endpoints.first_seq = 0 OR "
            "          peer_endpoints.last_seq = 0 OR "
            "          excluded.last_seen_at >= peer_endpoints.last_seen_at) "
            "    THEN excluded.current_seq "
            "    ELSE peer_endpoints.current_seq END, "
            "  last_seen_at = MAX("
            "      peer_endpoints.last_seen_at, excluded.last_seen_at), "
            "  last_success_at = MAX("
            "      peer_endpoints.last_success_at, excluded.last_success_at), "
            "  last_failure_at = MAX("
            "      peer_endpoints.last_failure_at, excluded.last_failure_at), "
            "  success_count = peer_endpoints.success_count + "
            "      excluded.success_count, "
            "  failure_count = peer_endpoints.failure_count + "
            "      excluded.failure_count, "
            "  seen_crawl = MAX(peer_endpoints.seen_crawl, "
            "excluded.seen_crawl), "
            "  seen_endpoints = MAX("
            "      peer_endpoints.seen_endpoints, excluded.seen_endpoints), "
            "  seen_redirect = MAX("
            "      peer_endpoints.seen_redirect, excluded.seen_redirect), "
            "  connected_ok = CASE "
            "    WHEN MAX(peer_endpoints.last_failure_at, "
            "             excluded.last_failure_at) > "
            "         MAX(peer_endpoints.last_success_at, "
            "             excluded.last_success_at) "
            "    THEN 0 "
            "    WHEN MAX(peer_endpoints.last_success_at, "
            "             excluded.last_success_at) > "
            "         MAX(peer_endpoints.last_failure_at, "
            "             excluded.last_failure_at) "
            "    THEN 1 "
            "    WHEN MAX(peer_endpoints.last_failure_at, "
            "             excluded.last_failure_at) > 0 "
            "    THEN 0 "
            "    ELSE MAX(peer_endpoints.connected_ok, "
            "             excluded.connected_ok) END;");

        int rc = SQLITE_OK;
        while ((rc = sqlite3_step(rows.get())) == SQLITE_ROW)
        {
            auto const* endpoint_text = sqlite3_column_text(rows.get(), 1);
            auto const normalized = persistent_cache_endpoint(
                endpoint_text ? reinterpret_cast<char const*>(endpoint_text)
                              : "");
            if (!normalized)
                continue;

            sqlite3_bind_value(
                merge.get(), 1, sqlite3_column_value(rows.get(), 0));
            sqlite3_bind_text(
                merge.get(), 2, normalized->c_str(), -1, SQLITE_TRANSIENT);
            for (int column = 2; column < 14; ++column)
            {
                sqlite3_bind_value(
                    merge.get(),
                    column + 1,
                    sqlite3_column_value(rows.get(), column));
            }
            auto const merge_rc = sqlite3_step(merge.get());
            if (merge_rc != SQLITE_DONE)
                throw_sqlite(db, merge_rc);
            sqlite3_reset(merge.get());
            sqlite3_clear_bindings(merge.get());
        }
        require_rows_complete(db, rc);

        exec_sql(db, "DROP TABLE peer_endpoints_identity_migration;");
        exec_sql(db, "PRAGMA user_version = 3;");
        exec_sql(db, "COMMIT;");
    }
    catch (...)
    {
        char* ignored = nullptr;
        sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, &ignored);
        if (ignored)
            sqlite3_free(ignored);
        throw;
    }
}

}  // namespace

std::shared_ptr<PeerEndpointCache>
PeerEndpointCache::open(std::string const& path)
{
    auto const canonical = canonical_cache_path(path);
    if (canonical == ":memory:")
        return std::shared_ptr<PeerEndpointCache>(
            new PeerEndpointCache(canonical));

    auto& registry = cache_registry();
    std::lock_guard lock(registry.mutex);
    if (auto found = registry.caches.find(canonical);
        found != registry.caches.end())
    {
        if (auto existing = found->second.lock())
            return existing;
        registry.caches.erase(found);
    }

    auto cache =
        std::shared_ptr<PeerEndpointCache>(new PeerEndpointCache(canonical));
    registry.caches.emplace(canonical, cache);
    return cache;
}

PeerEndpointCache::PeerEndpointCache(std::string path) : path_(std::move(path))
{
    std::filesystem::path fs_path(path_);
    if (fs_path.has_parent_path())
    {
        std::filesystem::create_directories(fs_path.parent_path());
    }

    auto const rc = sqlite3_open_v2(
        path_.c_str(),
        &db_,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
        nullptr);
    if (rc != SQLITE_OK)
    {
        std::string message =
            db_ ? sqlite3_errmsg(db_) : "sqlite3_open_v2 failed";
        if (db_)
        {
            sqlite3_close(db_);
            db_ = nullptr;
        }
        throw SqliteError(rc, std::move(message));
    }

    try
    {
        sqlite3_busy_timeout(db_, 2000);
        initialize_schema();
    }
    catch (...)
    {
        sqlite3_close(db_);
        db_ = nullptr;
        throw;
    }
}

PeerEndpointCache::~PeerEndpointCache()
{
    if (db_)
    {
        sqlite3_close(db_);
    }
}

//@@authority transition=prv.T0220 status=provisional scope=function
// bind=catl::peer_client::initialize_schema@75d686f8616c5daa
// owner=unresolved owner_hypothesis=peer_endpoint_cache
// commit=cache_constructor touches=prv.S0073
// label=install_versioned_bounded_peer_endpoint_cache_schema
void
PeerEndpointCache::initialize_schema()
{
    static constexpr int schema_version_current = 3;
    int schema_version = 0;
    {
        Statement version(db_, "PRAGMA user_version");
        auto const rc = sqlite3_step(version.get());
        if (rc != SQLITE_ROW)
            throw_sqlite(db_, rc);
        schema_version = sqlite3_column_int(version.get(), 0);
    }

    if (schema_version > schema_version_current)
    {
        throw std::runtime_error(
            "peer endpoint cache schema version " +
            std::to_string(schema_version) +
            " is newer than supported version " +
            std::to_string(schema_version_current));
    }

    if (schema_version == 0)
    {
        // The unversioned table predates the canonical endpoint identity.
        // It contains reconstructible discovery hints and has no supported
        // migration contract, so reset it once rather than guessing which
        // legacy aliases name the same peer.
        exec_sql(db_, "BEGIN IMMEDIATE;");
        try
        {
            exec_sql(db_, "DROP TABLE IF EXISTS peer_endpoints;");
            exec_sql(db_, kCreatePeerEndpoints);
            exec_sql(db_, "PRAGMA user_version = 3;");
            exec_sql(db_, "COMMIT;");
        }
        catch (...)
        {
            char* ignored = nullptr;
            sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, &ignored);
            if (ignored)
                sqlite3_free(ignored);
            throw;
        }
    }
    else
    {
        exec_sql(db_, kCreatePeerEndpoints);
        if (schema_version == 1 || schema_version == 2)
        {
            // V1 did not canonicalize DNS names or pure IPv6. Rejected V2
            // predecessors additionally admitted malformed host spellings.
            // Rewrite either version once under one transaction, coalescing
            // valid aliases, conserving their counters, and dropping invalid
            // rows before the retention bound is applied.
            migrate_legacy_to_v3(db_);
        }
    }

    prune_all_networks(db_, "peer_endpoints");
    exec_sql(
        db_,
        "CREATE INDEX IF NOT EXISTS peer_endpoints_bootstrap_idx "
        "ON peer_endpoints("
        "  network_id,"
        "  last_success_at DESC,"
        "  last_seen_at DESC,"
        "  success_count DESC,"
        "  failure_count ASC"
        ");");
    exec_sql(
        db_,
        "CREATE INDEX IF NOT EXISTS peer_endpoints_range_idx "
        "ON peer_endpoints(network_id, first_seq, last_seq);");
}

bool
PeerEndpointCache::is_disabled() const
{
    return disabled_ || db_ == nullptr;
}

void
PeerEndpointCache::throw_if_disabled() const
{
    if (!is_disabled())
        return;

    auto message = std::string{"peer endpoint cache unavailable"};
    if (!disabled_reason_.empty())
        message += ": " + disabled_reason_;
    throw std::runtime_error(std::move(message));
}

//@@authority transition=prv.T0219 status=provisional scope=function
// bind=catl::peer_client::disable@f799d8ec54c20dfb
// owner=unresolved owner_hypothesis=peer_endpoint_cache commit=cache_mutex
// touches=prv.S0075
// label=latch_peer_endpoint_cache_failure_before_future_queries
void
PeerEndpointCache::disable()
{
    disabled_ = true;
    if (!disabled_reason_.empty())
        return;

    auto failure = std::current_exception();
    if (!failure)
    {
        disabled_reason_ = "unknown cache failure";
        return;
    }

    try
    {
        std::rethrow_exception(std::move(failure));
    }
    catch (std::exception const& e)
    {
        disabled_reason_ = e.what();
    }
    catch (...)
    {
        disabled_reason_ = "unknown cache failure";
    }
}

std::int64_t
PeerEndpointCache::now_unix()
{
    using clock = std::chrono::system_clock;
    return std::chrono::duration_cast<std::chrono::seconds>(
               clock::now().time_since_epoch())
        .count();
}

std::vector<PeerEndpointCache::Entry>
PeerEndpointCache::load_bootstrap_candidates(
    uint32_t network_id,
    std::size_t limit) const
{
    std::lock_guard lock(mutex_);
    auto const bounded_limit = std::min(limit, max_endpoints_per_network);
    if (bounded_limit == 0)
        return {};
    throw_if_disabled();

    try
    {
        Statement stmt(
            db_,
            "SELECT endpoint, first_seq, last_seq, current_seq, last_seen_at, "
            "       last_success_at, last_failure_at, success_count, "
            "       failure_count, seen_crawl, seen_endpoints, "
            "       seen_redirect, connected_ok "
            "FROM peer_endpoints "
            "WHERE network_id = ? "
            "ORDER BY "
            "  CASE "
            "    WHEN failure_count > 0 AND "
            "         (last_failure_at > last_success_at OR "
            "          (last_failure_at = last_success_at AND "
            "           connected_ok = 0)) "
            "    THEN 1 ELSE 0 "
            "  END ASC, "
            "  connected_ok DESC, "
            "  CASE WHEN first_seq > 0 AND last_seq > 0 THEN 1 ELSE 0 END "
            "DESC, "
            "  CASE WHEN last_success_at > 0 THEN 1 ELSE 0 END DESC, "
            "  (seen_crawl + seen_endpoints + seen_redirect) DESC, "
            "  last_success_at DESC, "
            "  failure_count ASC, "
            "  last_seen_at DESC, "
            "  success_count DESC, "
            "  endpoint ASC "
            "LIMIT ?");

        sqlite3_bind_int(stmt.get(), 1, static_cast<int>(network_id));
        sqlite3_bind_int64(
            stmt.get(),
            2,
            static_cast<sqlite3_int64>(max_endpoints_per_network));

        std::vector<Entry> result;
        int rc = SQLITE_OK;
        while ((rc = sqlite3_step(stmt.get())) == SQLITE_ROW)
        {
            auto entry = read_entry(stmt.get());
            auto normalized =
                EndpointTracker::normalize_discovered_endpoint(entry.endpoint);
            if (normalized && result.size() < bounded_limit)
            {
                entry.endpoint = std::move(*normalized);
                result.push_back(std::move(entry));
            }
        }
        require_rows_complete(db_, rc);

        return result;
    }
    catch (...)
    {
        if (!current_failure_is_transient_sqlite())
            const_cast<PeerEndpointCache*>(this)->disable();
        throw;
    }
}

std::vector<PeerEndpointCache::Entry>
PeerEndpointCache::load_covering_candidates(
    uint32_t network_id,
    uint32_t ledger_seq,
    std::size_t limit) const
{
    std::lock_guard lock(mutex_);
    auto const bounded_limit = std::min(limit, max_endpoints_per_network);
    if (bounded_limit == 0)
        return {};
    throw_if_disabled();

    try
    {
        Statement stmt(
            db_,
            "SELECT endpoint, first_seq, last_seq, current_seq, last_seen_at, "
            "       last_success_at, last_failure_at, success_count, "
            "       failure_count, seen_crawl, seen_endpoints, "
            "       seen_redirect, connected_ok "
            "FROM peer_endpoints "
            "WHERE network_id = ? "
            "  AND first_seq > 0 AND last_seq > 0 "
            "  AND first_seq <= ? AND last_seq >= ? "
            "ORDER BY "
            "  CASE "
            "    WHEN failure_count > 0 AND "
            "         (last_failure_at > last_success_at OR "
            "          (last_failure_at = last_success_at AND "
            "           connected_ok = 0)) "
            "    THEN 1 ELSE 0 "
            "  END ASC, "
            "  connected_ok DESC, "
            "  CASE WHEN last_success_at > 0 THEN 1 ELSE 0 END DESC, "
            "  last_success_at DESC, "
            "  last_seen_at DESC, "
            "  failure_count ASC, "
            "  success_count DESC, "
            "  endpoint ASC "
            "LIMIT ?");

        sqlite3_bind_int(stmt.get(), 1, static_cast<int>(network_id));
        sqlite3_bind_int64(
            stmt.get(), 2, static_cast<sqlite3_int64>(ledger_seq));
        sqlite3_bind_int64(
            stmt.get(), 3, static_cast<sqlite3_int64>(ledger_seq));
        sqlite3_bind_int64(
            stmt.get(),
            4,
            static_cast<sqlite3_int64>(max_endpoints_per_network));

        std::vector<Entry> result;
        int rc = SQLITE_OK;
        while ((rc = sqlite3_step(stmt.get())) == SQLITE_ROW)
        {
            auto entry = read_entry(stmt.get());
            auto normalized =
                EndpointTracker::normalize_discovered_endpoint(entry.endpoint);
            if (normalized && result.size() < bounded_limit)
            {
                entry.endpoint = std::move(*normalized);
                result.push_back(std::move(entry));
            }
        }
        require_rows_complete(db_, rc);
        return result;
    }
    catch (...)
    {
        if (!current_failure_is_transient_sqlite())
            const_cast<PeerEndpointCache*>(this)->disable();
        throw;
    }
}

std::vector<PeerEndpointCache::Entry>
PeerEndpointCache::load_active_failures(uint32_t network_id) const
{
    std::lock_guard lock(mutex_);
    throw_if_disabled();

    try
    {
        Statement stmt(
            db_,
            "SELECT endpoint, first_seq, last_seq, current_seq, last_seen_at, "
            "       last_success_at, last_failure_at, success_count, "
            "       failure_count, seen_crawl, seen_endpoints, "
            "       seen_redirect, connected_ok "
            "FROM peer_endpoints "
            "WHERE network_id = ? AND failure_count > 0 AND "
            "      (last_failure_at > last_success_at OR "
            "       (last_failure_at = last_success_at AND connected_ok = 0)) "
            "ORDER BY last_failure_at DESC, endpoint ASC "
            "LIMIT ?");

        sqlite3_bind_int(stmt.get(), 1, static_cast<int>(network_id));
        sqlite3_bind_int64(
            stmt.get(),
            2,
            static_cast<sqlite3_int64>(max_endpoints_per_network));

        std::vector<Entry> result;
        int rc = SQLITE_OK;
        while ((rc = sqlite3_step(stmt.get())) == SQLITE_ROW)
            result.push_back(read_entry(stmt.get()));
        require_rows_complete(db_, rc);
        return result;
    }
    catch (...)
    {
        if (!current_failure_is_transient_sqlite())
            const_cast<PeerEndpointCache*>(this)->disable();
        throw;
    }
}

std::size_t
PeerEndpointCache::count_endpoints(uint32_t network_id) const
{
    std::lock_guard lock(mutex_);
    throw_if_disabled();

    try
    {
        Statement stmt(
            db_, "SELECT COUNT(*) FROM peer_endpoints WHERE network_id = ?");
        sqlite3_bind_int(stmt.get(), 1, static_cast<int>(network_id));

        auto const rc = sqlite3_step(stmt.get());
        if (rc != SQLITE_ROW)
        {
            throw_sqlite(db_, rc);
        }

        return static_cast<std::size_t>(sqlite3_column_int64(stmt.get(), 0));
    }
    catch (...)
    {
        if (!current_failure_is_transient_sqlite())
            const_cast<PeerEndpointCache*>(this)->disable();
        throw;
    }
}

PeerEndpointCache::Health
PeerEndpointCache::health() const
{
    std::lock_guard lock(mutex_);
    return Health{
        .available = !is_disabled(),
        .error = disabled_reason_,
    };
}

void
PeerEndpointCache::remember_discovered(
    uint32_t network_id,
    std::string const& endpoint)
{
    std::lock_guard lock(mutex_);
    if (is_disabled())
        return;

    try
    {
        auto const cache_key = persistent_cache_endpoint(endpoint);
        if (!cache_key)
            return;
        Statement stmt(
            db_,
            "INSERT INTO peer_endpoints("
            "  network_id, endpoint, last_seen_at"
            ") VALUES(?, ?, ?) "
            "ON CONFLICT(network_id, endpoint) DO UPDATE SET "
            "  last_seen_at = excluded.last_seen_at;");

        sqlite3_bind_int(stmt.get(), 1, static_cast<int>(network_id));
        sqlite3_bind_text(
            stmt.get(), 2, cache_key->c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt.get(), 3, now_unix());

        auto const rc = sqlite3_step(stmt.get());
        if (rc != SQLITE_DONE)
        {
            throw_sqlite(db_, rc);
        }
        prune_network_rows(db_, "peer_endpoints", network_id);
    }
    catch (...)
    {
        if (!current_failure_is_transient_sqlite())
            disable();
        throw;
    }
}

void
PeerEndpointCache::remember_status(
    uint32_t network_id,
    std::string const& endpoint,
    PeerStatus const& status)
{
    std::lock_guard lock(mutex_);
    if (is_disabled())
        return;

    try
    {
        auto const cache_key = persistent_cache_endpoint(endpoint);
        if (!cache_key)
            return;
        Statement stmt(
            db_,
            "INSERT INTO peer_endpoints("
            "  network_id, endpoint, first_seq, last_seq, current_seq, "
            "last_seen_at"
            ") VALUES(?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(network_id, endpoint) DO UPDATE SET "
            "  first_seq = excluded.first_seq, "
            "  last_seq = excluded.last_seq, "
            "  current_seq = excluded.current_seq, "
            "  last_seen_at = excluded.last_seen_at;");

        sqlite3_bind_int(stmt.get(), 1, static_cast<int>(network_id));
        sqlite3_bind_text(
            stmt.get(), 2, cache_key->c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(
            stmt.get(), 3, static_cast<sqlite3_int64>(status.first_seq));
        sqlite3_bind_int64(
            stmt.get(), 4, static_cast<sqlite3_int64>(status.last_seq));
        sqlite3_bind_int64(
            stmt.get(), 5, static_cast<sqlite3_int64>(status.current_seq));
        sqlite3_bind_int64(stmt.get(), 6, now_unix());

        auto const rc = sqlite3_step(stmt.get());
        if (rc != SQLITE_DONE)
        {
            throw_sqlite(db_, rc);
        }
        prune_network_rows(db_, "peer_endpoints", network_id);
    }
    catch (...)
    {
        if (!current_failure_is_transient_sqlite())
            disable();
        throw;
    }
}

void
PeerEndpointCache::remember_connect_success(
    uint32_t network_id,
    std::string const& endpoint,
    PeerStatus const& status)
{
    std::lock_guard lock(mutex_);
    if (is_disabled())
        return;

    try
    {
        auto const now = now_unix();
        auto const cache_key = persistent_cache_endpoint(endpoint);
        if (!cache_key)
            return;
        Statement stmt(
            db_,
            "INSERT INTO peer_endpoints("
            "  network_id, endpoint, first_seq, last_seq, current_seq, "
            "  last_seen_at, last_success_at, success_count, connected_ok"
            ") VALUES(?, ?, ?, ?, ?, ?, ?, 1, 1) "
            "ON CONFLICT(network_id, endpoint) DO UPDATE SET "
            "  first_seq = excluded.first_seq, "
            "  last_seq = excluded.last_seq, "
            "  current_seq = excluded.current_seq, "
            "  last_seen_at = excluded.last_seen_at, "
            "  last_success_at = excluded.last_success_at, "
            "  success_count = peer_endpoints.success_count + 1, "
            "  connected_ok = 1;");

        sqlite3_bind_int(stmt.get(), 1, static_cast<int>(network_id));
        sqlite3_bind_text(
            stmt.get(), 2, cache_key->c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(
            stmt.get(), 3, static_cast<sqlite3_int64>(status.first_seq));
        sqlite3_bind_int64(
            stmt.get(), 4, static_cast<sqlite3_int64>(status.last_seq));
        sqlite3_bind_int64(
            stmt.get(), 5, static_cast<sqlite3_int64>(status.current_seq));
        sqlite3_bind_int64(stmt.get(), 6, now);
        sqlite3_bind_int64(stmt.get(), 7, now);

        auto const rc = sqlite3_step(stmt.get());
        if (rc != SQLITE_DONE)
        {
            throw_sqlite(db_, rc);
        }
        prune_network_rows(db_, "peer_endpoints", network_id);
    }
    catch (...)
    {
        if (!current_failure_is_transient_sqlite())
            disable();
        throw;
    }
}

void
PeerEndpointCache::remember_connect_failure(
    uint32_t network_id,
    std::string const& endpoint)
{
    std::lock_guard lock(mutex_);
    if (is_disabled())
        return;

    try
    {
        auto const now = now_unix();
        auto const cache_key = persistent_cache_endpoint(endpoint);
        if (!cache_key)
            return;
        Statement stmt(
            db_,
            "INSERT INTO peer_endpoints("
            "  network_id, endpoint, last_seen_at, last_failure_at, "
            "  failure_count, connected_ok"
            ") VALUES(?, ?, ?, ?, 1, 0) "
            "ON CONFLICT(network_id, endpoint) DO UPDATE SET "
            "  last_seen_at = excluded.last_seen_at, "
            "  last_failure_at = excluded.last_failure_at, "
            "  failure_count = peer_endpoints.failure_count + 1, "
            "  connected_ok = 0;");

        sqlite3_bind_int(stmt.get(), 1, static_cast<int>(network_id));
        sqlite3_bind_text(
            stmt.get(), 2, cache_key->c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt.get(), 3, now);
        sqlite3_bind_int64(stmt.get(), 4, now);

        auto const rc = sqlite3_step(stmt.get());
        if (rc != SQLITE_DONE)
        {
            throw_sqlite(db_, rc);
        }
        prune_network_rows(db_, "peer_endpoints", network_id);
    }
    catch (...)
    {
        if (!current_failure_is_transient_sqlite())
            disable();
        throw;
    }
}

void
PeerEndpointCache::remember_seen_crawl(
    uint32_t network_id,
    std::string const& endpoint)
{
    std::lock_guard lock(mutex_);
    if (is_disabled())
        return;

    try
    {
        auto const cache_key = persistent_cache_endpoint(endpoint);
        if (!cache_key)
            return;
        Statement stmt(
            db_,
            "INSERT INTO peer_endpoints("
            "  network_id, endpoint, last_seen_at, seen_crawl"
            ") VALUES(?, ?, ?, 1) "
            "ON CONFLICT(network_id, endpoint) DO UPDATE SET "
            "  last_seen_at = excluded.last_seen_at, "
            "  seen_crawl = 1;");

        sqlite3_bind_int(stmt.get(), 1, static_cast<int>(network_id));
        sqlite3_bind_text(
            stmt.get(), 2, cache_key->c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt.get(), 3, now_unix());

        auto const rc = sqlite3_step(stmt.get());
        if (rc != SQLITE_DONE)
        {
            throw_sqlite(db_, rc);
        }
        prune_network_rows(db_, "peer_endpoints", network_id);
    }
    catch (...)
    {
        if (!current_failure_is_transient_sqlite())
            disable();
        throw;
    }
}

void
PeerEndpointCache::remember_seen_endpoints(
    uint32_t network_id,
    std::string const& endpoint)
{
    std::lock_guard lock(mutex_);
    if (is_disabled())
        return;

    try
    {
        auto const cache_key = persistent_cache_endpoint(endpoint);
        if (!cache_key)
            return;
        Statement stmt(
            db_,
            "INSERT INTO peer_endpoints("
            "  network_id, endpoint, last_seen_at, seen_endpoints"
            ") VALUES(?, ?, ?, 1) "
            "ON CONFLICT(network_id, endpoint) DO UPDATE SET "
            "  last_seen_at = excluded.last_seen_at, "
            "  seen_endpoints = 1;");

        sqlite3_bind_int(stmt.get(), 1, static_cast<int>(network_id));
        sqlite3_bind_text(
            stmt.get(), 2, cache_key->c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt.get(), 3, now_unix());

        auto const rc = sqlite3_step(stmt.get());
        if (rc != SQLITE_DONE)
        {
            throw_sqlite(db_, rc);
        }
        prune_network_rows(db_, "peer_endpoints", network_id);
    }
    catch (...)
    {
        if (!current_failure_is_transient_sqlite())
            disable();
        throw;
    }
}

void
PeerEndpointCache::remember_seen_redirect(
    uint32_t network_id,
    std::string const& endpoint)
{
    std::lock_guard lock(mutex_);
    if (is_disabled())
        return;

    try
    {
        auto const cache_key = persistent_cache_endpoint(endpoint);
        if (!cache_key)
            return;
        Statement stmt(
            db_,
            "INSERT INTO peer_endpoints("
            "  network_id, endpoint, last_seen_at, seen_redirect"
            ") VALUES(?, ?, ?, 1) "
            "ON CONFLICT(network_id, endpoint) DO UPDATE SET "
            "  last_seen_at = excluded.last_seen_at, "
            "  seen_redirect = 1;");

        sqlite3_bind_int(stmt.get(), 1, static_cast<int>(network_id));
        sqlite3_bind_text(
            stmt.get(), 2, cache_key->c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt.get(), 3, now_unix());

        auto const rc = sqlite3_step(stmt.get());
        if (rc != SQLITE_DONE)
        {
            throw_sqlite(db_, rc);
        }
        prune_network_rows(db_, "peer_endpoints", network_id);
    }
    catch (...)
    {
        if (!current_failure_is_transient_sqlite())
            disable();
        throw;
    }
}

}  // namespace catl::peer_client
