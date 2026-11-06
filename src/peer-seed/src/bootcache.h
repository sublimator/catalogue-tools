#pragma once

// The node's bootcache: the PeerFinder_BootstrapCache table in
// <data dir>/db/peerfinder.sqlite, which the node ranks by valence when it
// picks its first outbound dials.

#include <cstdint>
#include <filesystem>
#include <string>
#include <utility>
#include <vector>

namespace peer_seed {

struct BootcacheEntry
{
    std::string address;
    int64_t valence = 0;
};

std::vector<BootcacheEntry>
read_bootcache(std::filesystem::path const& db);

/// Pids that have the database, or its WAL or SHM file, open (from lsof).
/// Throws when lsof can't say, so a caller never mistakes a failed check for
/// a closed database.
std::vector<std::string>
open_by(std::filesystem::path const& db);

/// Create dir/stem.sqlite, or stem-1.sqlite and so on, exclusively, so a new
/// backup never replaces an earlier one.
std::filesystem::path
reserve_new_file(std::filesystem::path const& dir, std::string const& stem);

/// "[::ffff:a.b.c.d]:p" for "a.b.c.d:p"; empty for an IPv6 address.
std::string
mapped_twin(std::string const& address);

/// Copy one SQLite database over another with SQLite's online backup.
void
copy_database(
    std::filesystem::path const& from,
    std::filesystem::path const& to);

/// Insert each address, or set its valence if it is already there, and drop
/// its IPv4-mapped twin row, in one transaction. Returns the twins dropped.
std::size_t
upsert_valences(
    std::filesystem::path const& db,
    std::vector<std::pair<std::string, int64_t>> const& rows);

}  // namespace peer_seed
