#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace catl::utils::slicer {

/**
 * Type-safe structure for command line options
 */
struct CommandLineOptions
{
    /** Path to the input CATL file */
    std::optional<std::string> input_file;

    /** Path to the output slice file */
    std::optional<std::string> output_file;

    /** Start ledger sequence number */
    std::optional<uint32_t> start_ledger;

    /** End ledger sequence number */
    std::optional<uint32_t> end_ledger;

    /** Path to store snapshots */
    std::optional<std::string> snapshots_path;

    /** Compression level for output (0-9) */
    uint8_t compression_level = 0;

    /** Whether to force overwrite existing output file without prompting */
    bool force_overwrite = false;

    /** Whether to create a state snapshot for the next slice */
    bool create_next_slice_state_snapshot = true;

    /** Whether to use start snapshot if available */
    bool use_start_snapshot = true;

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
 * @return A populated CommandLineOptions structure
 */
CommandLineOptions
parse_argv(int argc, char* argv[]);

}  // namespace catl::utils::slicer