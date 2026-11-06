#include "bootcache.h"

#include <sqlite3.h>

#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstdio>
#include <memory>
#include <set>
#include <sstream>
#include <stdexcept>
#include <system_error>

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
        std::error_code ec;
        if (std::filesystem::exists(path, ec))
        {
            command += " " + shell_quote(path);
            any = true;
        }
    }
    if (!any)
        return {};
    command += " 2>&1";
    auto* pipe = popen(command.c_str(), "r");
    if (!pipe)
        throw std::runtime_error("could not run lsof");
    std::set<std::string> pids;
    std::string other;
    char buffer[512];
    while (fgets(buffer, sizeof buffer, pipe))
    {
        std::string line(buffer);
        while (!line.empty() &&
               std::isspace(static_cast<unsigned char>(line.back())))
            line.pop_back();
        if (line.empty())
            continue;
        if (std::all_of(line.begin(), line.end(), [](unsigned char c) {
                return std::isdigit(c);
            }))
            pids.insert(line);
        else if (other.size() < 400)
            other += (other.empty() ? "" : "; ") + line;
    }
    auto const status = pclose(pipe);

    // lsof exits 0 when it lists a holder and 1, silently, when nothing has
    // the files open. Anything else means the check didn't happen, and a
    // write must not go ahead on a check that didn't happen.
    auto const code = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
    if (code == 0 && !pids.empty() && other.empty())
        return {pids.begin(), pids.end()};
    if (code == 1 && pids.empty() && other.empty())
        return {};
    if (code == 127)
        throw std::runtime_error(
            "lsof is not on PATH, so whether the node has the database open "
            "can't be checked");
    throw std::runtime_error(
        "lsof couldn't check whether the database is open (exit " +
        std::to_string(code) + (other.empty() ? "" : ": " + other) + ")");
}

std::filesystem::path
reserve_new_file(std::filesystem::path const& dir, std::string const& stem)
{
    std::filesystem::create_directories(dir);
    for (int n = 0; n < 1000; ++n)
    {
        auto const path =
            dir / (stem + (n == 0 ? "" : "-" + std::to_string(n)) + ".sqlite");
        int const fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
        if (fd >= 0)
        {
            ::close(fd);
            return path;
        }
        if (errno != EEXIST)
            throw std::system_error(
                errno, std::generic_category(), path.string());
    }
    throw std::runtime_error("no free backup name in " + dir.string());
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

std::string
mapped_twin(std::string const& address)
{
    if (address.empty() || address.front() == '[')
        return {};
    auto const colon = address.rfind(':');
    if (colon == std::string::npos)
        return {};
    return "[::ffff:" + address.substr(0, colon) + "]" + address.substr(colon);
}

std::size_t
upsert_valences(
    std::filesystem::path const& db_path,
    std::vector<std::pair<std::string, int64_t>> const& rows)
{
    auto db = open_database(db_path, SQLITE_OPEN_READWRITE);
    exec(db.get(), "BEGIN IMMEDIATE");
    try
    {
        auto upsert = prepare(
            db.get(),
            std::string("INSERT INTO ") + kTable +
                " (address, valence) VALUES (?, ?)"
                " ON CONFLICT(address) DO UPDATE SET valence = "
                "excluded.valence");
        auto remove = prepare(
            db.get(),
            std::string("DELETE FROM ") + kTable + " WHERE address = ?");
        std::size_t removed = 0;
        for (auto const& [address, valence] : rows)
        {
            sqlite3_reset(upsert.get());
            sqlite3_bind_text(
                upsert.get(), 1, address.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int64(upsert.get(), 2, valence);
            if (sqlite3_step(upsert.get()) != SQLITE_DONE)
                throw std::runtime_error(
                    std::string("sqlite: ") + sqlite3_errmsg(db.get()));

            // The node keys its cache by text, so [::ffff:a.b.c.d]:p is a
            // second entry for the same peer. Drop it, so the seed is the
            // only entry for that machine.
            auto const twin = mapped_twin(address);
            if (twin.empty())
                continue;
            sqlite3_reset(remove.get());
            sqlite3_bind_text(
                remove.get(), 1, twin.c_str(), -1, SQLITE_TRANSIENT);
            if (sqlite3_step(remove.get()) != SQLITE_DONE)
                throw std::runtime_error(
                    std::string("sqlite: ") + sqlite3_errmsg(db.get()));
            removed += static_cast<std::size_t>(sqlite3_changes(db.get()));
        }
        exec(db.get(), "COMMIT");
        return removed;
    }
    catch (...)
    {
        sqlite3_exec(db.get(), "ROLLBACK", nullptr, nullptr, nullptr);
        throw;
    }
}

}  // namespace peer_seed
