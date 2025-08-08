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

    /** NuDB load factor (0.0 - 1.0) */
    double load_factor = 0.5;

    /** Log level (error, warn, info, debug) */
    std::string log_level = "info";

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