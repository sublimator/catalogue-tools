#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace catl::v1::utils::nudb {

/**
 * Type-safe structure for nudb-to-catl1 command line options
 */
struct NudbToCatl1Options
{
    /** Path to the input NuDB database directory */
    std::optional<std::string> nudb_path;

    /** Path to the output CATL file */
    std::optional<std::string> output_file;

    /** Start ledger sequence number */
    std::optional<uint32_t> start_ledger;

    /** End ledger sequence number */
    std::optional<uint32_t> end_ledger;

    /** Network ID for the output CATL file */
    uint16_t network_id = 0;

    /** Compression level for output (0-9) */
    uint8_t compression_level = 0;

    /** Whether to force overwrite existing output file without prompting */
    bool force_overwrite = false;

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
 * @return A populated NudbToCatl1Options structure
 */
NudbToCatl1Options
parse_nudb_to_catl1_argv(int argc, char* argv[]);

}  // namespace catl::v1::utils::nudb