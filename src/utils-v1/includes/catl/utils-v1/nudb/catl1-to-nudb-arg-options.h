#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace catl::v1::utils::nudb {

/**
 * Type-safe structure for catl1-to-nudb command line options
 */
struct Catl1ToNudbOptions
{
    /** Path to the input CATL file */
    std::optional<std::string> input_file;

    /** Path to the output NuDB database directory */
    std::optional<std::string> nudb_path;

    /** Start ledger sequence number (optional, defaults to file's min_ledger)
     */
    std::optional<uint32_t> start_ledger;

    /** End ledger sequence number (optional, defaults to file's max_ledger) */
    std::optional<uint32_t> end_ledger;

    /** Whether to force overwrite existing database without prompting */
    bool force_overwrite = false;

    /** Create database if it doesn't exist */
    bool create_database = true;

    /** NuDB key size in bytes */
    uint32_t key_size = 32;  // Default for 256-bit hashes

    /** NuDB block size */
    uint32_t block_size = 4096;

    /** NuDB load factor (0.0 - 1.0) - lower = faster, higher = more space
     * efficient */
    double load_factor = 0.5;

    /** Log level (error, warn, info, debug) */
    std::string log_level = "info";

    /** Test snapshot memory usage mode */
    bool test_snapshots = false;

    /** Number of threads for parallel hashing (must be power of 2: 1, 2, 4, 8,
     * 16) NOTE: Counterintuitively, single-threaded (1) often performs better
     * than multi-threaded due to thread coordination overhead outweighing
     * parallelization benefits. Default is 1 for optimal performance in most
     * cases.
     */
    int hasher_threads = 1;

    /** Number of threads for parallel compression (default: 2) */
    int compressor_threads = 2;

    /** Max write queue size in megabytes (default: 2048 MB = 2 GB) */
    uint32_t max_write_queue_mb = 2048;

    /** Enable verbose debug log partitions (MAP_OPS, WALK_NODES, VERSION_TRACK,
     * PIPE_VERSION) */
    bool enable_debug_partitions = false;

    /** Enable WALK_NODES logging only for a specific ledger (for debugging) */
    std::optional<uint32_t> walk_nodes_ledger;

    /** Debug key prefix (hex) to print detailed info during walk_nodes */
    std::optional<std::string> walk_nodes_debug_key;

    /** Mock NuDB mode - for performance testing. Options:
     *  - "" (empty/disabled): Use bulk writer (optimized)
     *  - "noop" or "memory": Skip all I/O operations
     *  - "disk": Write keys/values to buffered append-only file
     *  - "nudb": Use regular NuDB inserts (not bulk writer, for comparison)
     */
    std::string nudb_mock = "";

    /** Skip deduplication tracking (no dedup = faster, but duplicates written
     * to .dat and handled by rekey) */
    bool no_dedupe = false;

    /** Whether to display help information */
    bool show_help = false;

    /** Whether parsing completed successfully */
    bool valid = true;

    /** Any error message to display */
    std::optional<std::string> error_message;

    /** Pre-formatted help text */
    std::string help_text;
};

/**
 * Parse command line arguments into a structured options object
 *
 * @param argc Argument count from main
 * @param argv Argument values from main
 * @return A populated Catl1ToNudbOptions structure
 */
Catl1ToNudbOptions
parse_catl1_to_nudb_argv(int argc, char* argv[]);

}  // namespace catl::v1::utils::nudb