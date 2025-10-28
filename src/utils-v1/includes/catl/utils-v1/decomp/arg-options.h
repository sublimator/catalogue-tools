#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace catl::v1::utils::decomp {

/**
 * Type-safe structure for command line options
 */
struct CommandLineOptions
{
    /** Path to the input CATL file */
    std::optional<std::string> input_file;

    /** Path to the output CATL file */
    std::optional<std::string> output_file;

    /** Target compression level (0-9, where 0 is uncompressed) */
    std::optional<int> compression_level;

    /** Whether to force overwrite existing output file without prompting */
    bool force_overwrite = false;

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

}  // namespace catl::v1::utils::decomp