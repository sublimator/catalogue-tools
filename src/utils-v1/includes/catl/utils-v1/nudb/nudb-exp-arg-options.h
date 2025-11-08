#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace catl::v1::utils::nudb {

/**
 * Type-safe structure for nudb-exp command line options
 */
struct NudbExpOptions
{
    /** Path to the NuDB database directory */
    std::optional<std::string> nudb_path;

    /** Key to lookup (in hex) */
    std::optional<std::string> key_hex;

    /** Ledger hash for tree walking (in hex) */
    std::optional<std::string> ledger_hash;

    /** State key to lookup in account tree (in hex) */
    std::optional<std::string> state_key;

    /** Transaction key to lookup in tx tree (in hex) */
    std::optional<std::string> tx_key;

    /** Output format (hex, binary, info) */
    std::string output_format = "hex";

    /** List all keys in the database */
    bool list_keys = false;

    /** Show database statistics */
    bool show_stats = false;

    /** Log level (error, warn, info, debug) */
    std::string log_level = "info";

    /** Network ID for protocol definitions (0=XRPL, 21337=Xahau) */
    uint32_t network_id = 21337;

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
 * @return A populated NudbExpOptions structure
 */
NudbExpOptions
parse_nudb_exp_argv(int argc, char* argv[]);

}  // namespace catl::v1::utils::nudb