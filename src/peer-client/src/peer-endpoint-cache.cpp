#include <catl/peer-client/peer-endpoint-cache.h>

#include <catl/core/logger.h>

#include <filesystem>
#include <sqlite3.h>
#include <stdexcept>

namespace catl::peer_client {

namespace {

LogPartition log_("peer-cache", LogLevel::INHERIT);

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
        throw std::runtime_error(message);
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
            throw std::runtime_error(sqlite3_errmsg(db));
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

}  // namespace

std::shared_ptr<PeerEndpointCache>
PeerEndpointCache::open(std::string const& path)
{
    return std::shared_ptr<PeerEndpointCache>(new PeerEndpointCache(path));
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
        throw std::runtime_error(message);
    }

    sqlite3_busy_timeout(db_, 2000);
    initialize_schema();
}

PeerEndpointCache::~PeerEndpointCache()
{
    if (db_)
    {
        sqlite3_close(db_);
    }
}

void
PeerEndpointCache::initialize_schema()
{
    exec_sql(
        db_,
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
        ");");

    // Migrate existing databases — ADD COLUMN is safe if column exists
    // (SQLite returns error which we silently ignore).
    auto try_add_column = [this](char const* sql) {
        char* err = nullptr;
        sqlite3_exec(db_, sql, nullptr, nullptr, &err);
        if (err)
            sqlite3_free(err);
    };
    try_add_column(
        "ALTER TABLE peer_endpoints ADD COLUMN seen_crawl INTEGER NOT NULL "
        "DEFAULT 0");
    try_add_column(
        "ALTER TABLE peer_endpoints ADD COLUMN seen_endpoints INTEGER NOT NULL "
        "DEFAULT 0");
    try_add_column(
        "ALTER TABLE peer_endpoints ADD COLUMN seen_redirect INTEGER NOT NULL "
        "DEFAULT 0");
    try_add_column(
        "ALTER TABLE peer_endpoints ADD COLUMN connected_ok INTEGER NOT NULL "
        "DEFAULT 0");

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
}

bool
PeerEndpointCache::is_disabled() const
{
    return disabled_ || db_ == nullptr;
}

void
PeerEndpointCache::disable()
{
    disabled_ = true;
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
    if (is_disabled())
        return {};

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
            "    WHEN last_failure_at > 0 AND "
            "         (last_success_at = 0 OR last_failure_at > "
            "last_success_at) "
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
        sqlite3_bind_int64(stmt.get(), 2, static_cast<sqlite3_int64>(limit));

        std::vector<Entry> result;
        while (sqlite3_step(stmt.get()) == SQLITE_ROW)
        {
            Entry entry;
            auto const* endpoint_text = sqlite3_column_text(stmt.get(), 0);
            entry.endpoint = endpoint_text
                ? reinterpret_cast<char const*>(endpoint_text)
                : "";
            entry.status.first_seq =
                static_cast<uint32_t>(sqlite3_column_int(stmt.get(), 1));
            entry.status.last_seq =
                static_cast<uint32_t>(sqlite3_column_int(stmt.get(), 2));
            entry.status.current_seq =
                static_cast<uint32_t>(sqlite3_column_int(stmt.get(), 3));
            entry.status.last_seen = std::chrono::steady_clock::now();
            entry.last_seen_at =
                static_cast<std::int64_t>(sqlite3_column_int64(stmt.get(), 4));
            entry.last_success_at =
                static_cast<std::int64_t>(sqlite3_column_int64(stmt.get(), 5));
            entry.last_failure_at =
                static_cast<std::int64_t>(sqlite3_column_int64(stmt.get(), 6));
            entry.success_count =
                static_cast<std::uint64_t>(sqlite3_column_int64(stmt.get(), 7));
            entry.failure_count =
                static_cast<std::uint64_t>(sqlite3_column_int64(stmt.get(), 8));
            entry.seen_crawl = sqlite3_column_int(stmt.get(), 9) != 0;
            entry.seen_endpoints = sqlite3_column_int(stmt.get(), 10) != 0;
            entry.seen_redirect = sqlite3_column_int(stmt.get(), 11) != 0;
            entry.connected_ok = sqlite3_column_int(stmt.get(), 12) != 0;
            result.push_back(std::move(entry));
        }

        return result;
    }
    catch (...)
    {
        const_cast<PeerEndpointCache*>(this)->disable();
        throw;
    }
}

std::size_t
PeerEndpointCache::count_endpoints(uint32_t network_id) const
{
    std::lock_guard lock(mutex_);
    if (is_disabled())
        return 0;

    try
    {
        Statement stmt(
            db_, "SELECT COUNT(*) FROM peer_endpoints WHERE network_id = ?");
        sqlite3_bind_int(stmt.get(), 1, static_cast<int>(network_id));

        if (sqlite3_step(stmt.get()) != SQLITE_ROW)
        {
            throw std::runtime_error(sqlite3_errmsg(db_));
        }

        return static_cast<std::size_t>(sqlite3_column_int64(stmt.get(), 0));
    }
    catch (...)
    {
        const_cast<PeerEndpointCache*>(this)->disable();
        throw;
    }
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
        Statement stmt(
            db_,
            "INSERT INTO peer_endpoints("
            "  network_id, endpoint, last_seen_at"
            ") VALUES(?, ?, ?) "
            "ON CONFLICT(network_id, endpoint) DO UPDATE SET "
            "  last_seen_at = excluded.last_seen_at;");

        sqlite3_bind_int(stmt.get(), 1, static_cast<int>(network_id));
        sqlite3_bind_text(
            stmt.get(), 2, endpoint.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt.get(), 3, now_unix());

        if (sqlite3_step(stmt.get()) != SQLITE_DONE)
        {
            throw std::runtime_error(sqlite3_errmsg(db_));
        }
    }
    catch (...)
    {
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
            stmt.get(), 2, endpoint.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(
            stmt.get(), 3, static_cast<sqlite3_int64>(status.first_seq));
        sqlite3_bind_int64(
            stmt.get(), 4, static_cast<sqlite3_int64>(status.last_seq));
        sqlite3_bind_int64(
            stmt.get(), 5, static_cast<sqlite3_int64>(status.current_seq));
        sqlite3_bind_int64(stmt.get(), 6, now_unix());

        if (sqlite3_step(stmt.get()) != SQLITE_DONE)
        {
            throw std::runtime_error(sqlite3_errmsg(db_));
        }
    }
    catch (...)
    {
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
            stmt.get(), 2, endpoint.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(
            stmt.get(), 3, static_cast<sqlite3_int64>(status.first_seq));
        sqlite3_bind_int64(
            stmt.get(), 4, static_cast<sqlite3_int64>(status.last_seq));
        sqlite3_bind_int64(
            stmt.get(), 5, static_cast<sqlite3_int64>(status.current_seq));
        sqlite3_bind_int64(stmt.get(), 6, now);
        sqlite3_bind_int64(stmt.get(), 7, now);

        if (sqlite3_step(stmt.get()) != SQLITE_DONE)
        {
            throw std::runtime_error(sqlite3_errmsg(db_));
        }
    }
    catch (...)
    {
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
            stmt.get(), 2, endpoint.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt.get(), 3, now);
        sqlite3_bind_int64(stmt.get(), 4, now);

        if (sqlite3_step(stmt.get()) != SQLITE_DONE)
        {
            throw std::runtime_error(sqlite3_errmsg(db_));
        }
    }
    catch (...)
    {
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
            stmt.get(), 2, endpoint.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt.get(), 3, now_unix());

        if (sqlite3_step(stmt.get()) != SQLITE_DONE)
        {
            throw std::runtime_error(sqlite3_errmsg(db_));
        }
    }
    catch (...)
    {
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
            stmt.get(), 2, endpoint.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt.get(), 3, now_unix());

        if (sqlite3_step(stmt.get()) != SQLITE_DONE)
        {
            throw std::runtime_error(sqlite3_errmsg(db_));
        }
    }
    catch (...)
    {
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
            stmt.get(), 2, endpoint.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt.get(), 3, now_unix());

        if (sqlite3_step(stmt.get()) != SQLITE_DONE)
        {
            throw std::runtime_error(sqlite3_errmsg(db_));
        }
    }
    catch (...)
    {
        disable();
        throw;
    }
}

}  // namespace catl::peer_client
