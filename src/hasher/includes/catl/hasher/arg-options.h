#pragma once

#include <optional>
#include <string>
#include <vector>

namespace catl::hasher {

/**
 * Enumeration of recognized log levels
 */
enum class LogLevel { ERROR, WARN, INFO, DEBUG };

/**
 * Type-safe structure for command line options
 */
struct CommandLineOptions
{
    /** Path to the CATL file to process */
    std::optional<std::string> input_file;

    /** Log verbosity level */
    LogLevel log_level = LogLevel::INFO;

    /** Whether to start the HTTP server */
    bool start_server = false;

    /** First ledger to capture in the snapshot range */
    std::optional<uint32_t> first_ledger;

    /** Last ledger to process in the file */
    std::optional<uint32_t> last_ledger;

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
 * Convert string to LogLevel enum
 *
 * @param level_str String representation of log level
 * @return Corresponding LogLevel value, or LogLevel::INFO if invalid
 */
LogLevel
string_to_log_level(const std::string& level_str);

/**
 * Convert LogLevel enum to string
 *
 * @param level LogLevel enum value
 * @return String representation of the log level
 */
std::string
log_level_to_string(LogLevel level);

}  // namespace catl::hasher