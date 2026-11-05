#include "bootcache.h"

#include <sqlite3.h>

#include <cstdio>
#include <memory>
#include <set>
#include <sstream>
#include <stdexcept>

namespace peer_seed {

namespace {

constexpr char const* kTable = "PeerFinder_BootstrapCache";

struct CloseDatabase
{
    void
    operator()(sqlite3* db) const
    {
        sqlite3_close(db);
    }
};
using Database = std::unique_ptr<sqlite3, CloseDatabase>;

struct FinalizeStatement
{
    void
    operator()(sqlite3_stmt* statement) const
    {
        sqlite3_finalize(statement);
    }
};
using Statement = std::unique_ptr<sqlite3_stmt, FinalizeStatement>;

Database
open_database(std::filesystem::path const& path, int flags)
{
    sqlite3* raw = nullptr;
    auto const rc = sqlite3_open_v2(path.c_str(), &raw, flags, nullptr);
    Database db(raw);
    if (rc != SQLITE_OK)
    {
        throw std::runtime_error(
            "open " + path.string() + ": " +
            (raw ? sqlite3_errmsg(raw) : sqlite3_errstr(rc)));
    }
    sqlite3_busy_timeout(db.get(), 2000);
    return db;
}

Statement
prepare(sqlite3* db, std::string const& sql)
{
    sqlite3_stmt* raw = nullptr;
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &raw, nullptr) != SQLITE_OK)
        throw std::runtime_error(std::string("sqlite: ") + sqlite3_errmsg(db));
    return Statement(raw);
}

void
exec(sqlite3* db, char const* sql)
{
    char* error = nullptr;
    if (sqlite3_exec(db, sql, nullptr, nullptr, &error) != SQLITE_OK)
    {
        std::string message = error ? error : "unknown error";
        sqlite3_free(error);
        throw std::runtime_error(std::string("sqlite: ") + message);
    }
}

std::string
shell_quote(std::string const& text)
{
    std::string out = "'";
    for (char const c : text)
    {
        if (c == '\'')
            out += "'\\''";
        else
            out += c;
    }
    return out + "'";
}

}  // namespace

std::vector<BootcacheEntry>
read_bootcache(std::filesystem::path const& db_path)
{
    auto db = open_database(db_path, SQLITE_OPEN_READONLY);
    auto statement = prepare(
        db.get(), std::string("SELECT address, valence FROM ") + kTable);
    std::vector<BootcacheEntry> entries;
    while (sqlite3_step(statement.get()) == SQLITE_ROW)
    {
        auto const* text = sqlite3_column_text(statement.get(), 0);
        if (!text)
            continue;
        entries.push_back(
            {reinterpret_cast<char const*>(text),
             sqlite3_column_int64(statement.get(), 1)});
    }
    return entries;
}

std::vector<std::string>
open_by(std::filesystem::path const& db)
{
    std::string command = "lsof -t --";
    bool any = false;
    for (auto const& suffix : {"", "-wal", "-shm"})
    {
        auto const path = db.string() + suffix;
        if (std::filesystem::exists(path))
        {
            command += " " + shell_quote(path);
            any = true;
        }
    }
    if (!any)
        return {};
    command += " 2>/dev/null";
    std::unique_ptr<FILE, int (*)(FILE*)> pipe(
        popen(command.c_str(), "r"), pclose);
    if (!pipe)
        throw std::runtime_error("could not run lsof");
    std::set<std::string> pids;
    char buffer[64];
    while (fgets(buffer, sizeof buffer, pipe.get()))
    {
        std::string pid(buffer);
        while (!pid.empty() && (pid.back() == '\n' || pid.back() == ' '))
            pid.pop_back();
        if (!pid.empty())
            pids.insert(pid);
    }
    return {pids.begin(), pids.end()};
}

void
copy_database(
    std::filesystem::path const& from,
    std::filesystem::path const& to)
{
    auto source = open_database(from, SQLITE_OPEN_READONLY);
    auto target = open_database(to, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE);
    auto* backup =
        sqlite3_backup_init(target.get(), "main", source.get(), "main");
    if (!backup)
        throw std::runtime_error(
            std::string("backup: ") + sqlite3_errmsg(target.get()));
    auto const rc = sqlite3_backup_step(backup, -1);
    sqlite3_backup_finish(backup);
    if (rc != SQLITE_DONE)
        throw std::runtime_error(std::string("backup: ") + sqlite3_errstr(rc));
}

void
upsert_valences(
    std::filesystem::path const& db_path,
    std::vector<std::pair<std::string, int64_t>> const& rows)
{
    auto db = open_database(db_path, SQLITE_OPEN_READWRITE);
    exec(db.get(), "BEGIN IMMEDIATE");
    try
    {
        auto statement = prepare(
            db.get(),
            std::string("INSERT INTO ") + kTable +
                " (address, valence) VALUES (?, ?)"
                " ON CONFLICT(address) DO UPDATE SET valence = "
                "excluded.valence");
        for (auto const& [address, valence] : rows)
        {
            sqlite3_reset(statement.get());
            sqlite3_bind_text(
                statement.get(), 1, address.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int64(statement.get(), 2, valence);
            if (sqlite3_step(statement.get()) != SQLITE_DONE)
                throw std::runtime_error(
                    std::string("sqlite: ") + sqlite3_errmsg(db.get()));
        }
        exec(db.get(), "COMMIT");
    }
    catch (...)
    {
        sqlite3_exec(db.get(), "ROLLBACK", nullptr, nullptr, nullptr);
        throw;
    }
}

}  // namespace peer_seed
