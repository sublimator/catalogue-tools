#pragma once

#include "catl/core/logger.h"
#include <cstdint>
#include <optional>
#include <string>

namespace catl::utils::slicer {

/**
 * Enumeration for mapping string log levels to core LogLevel
 */
enum class SlicerLogLevel { ERROR, WARN, INFO, DEBUG };

/**
 * Type-safe structure for command line options
 */
struct CommandLineOptions
{
    /** Path to the source CATL file (v1 format) */
    std::optional<std::string> input_file;

    /** Path where the generated CATL slice file will be saved */
    std::optional<std::string> output_file;

    /** The sequence number of the first ledger to include in the slice */
    std::optional<uint32_t> start_ledger;

    /** The sequence number of the last ledger to include in the slice */
    std::optional<uint32_t> end_ledger;

    /** Directory where state snapshots are stored and looked for */
    std::optional<std::string> snapshots_path;

    /** Compression level for the output slice file and state snapshots (0-9) */
    uint8_t compression_level = 0;

    /** Log verbosity level */
    SlicerLogLevel log_level = SlicerLogLevel::INFO;

    /** Whether to force overwrite existing output file without prompting */
    bool force_overwrite = false;

    /** Whether to create a state snapshot for the next slice */
    bool create_next_slice_state_snapshot = true;

    /** Whether to use a start snapshot if available */
    bool use_start_snapshot = true;

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

/**
 * Convert string to SlicerLogLevel enum
 *
 * @param level_str String representation of log level
 * @return Corresponding SlicerLogLevel value, or SlicerLogLevel::INFO if
 * invalid
 */
SlicerLogLevel
string_to_log_level(const std::string& level_str);

/**
 * Convert SlicerLogLevel enum to string
 *
 * @param level SlicerLogLevel enum value
 * @return String representation of the log level
 */
std::string
log_level_to_string(SlicerLogLevel level);

/**
 * Convert SlicerLogLevel to core LogLevel
 *
 * @param level SlicerLogLevel enum value
 * @return Corresponding core LogLevel value
 */
LogLevel
convert_to_core_log_level(SlicerLogLevel level);

}  // namespace catl::utils::slicer