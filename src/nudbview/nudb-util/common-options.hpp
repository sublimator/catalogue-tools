#pragma once

#include <boost/program_options.hpp>
#include <optional>
#include <string>

namespace nudbutil {

/**
 * Common command-line options shared across nudb-util subcommands
 */
struct CommonOptions
{
    /** Path to the NuDB database directory */
    std::optional<std::string> nudb_path;

    /** Log level (error, warn, info, debug) */
    std::string log_level = "info";

    /** Whether to display help information */
    bool show_help = false;
};

/**
 * Add common options to a Boost program_options description
 *
 * @param desc The options description to add common options to
 */
inline void
add_common_options(boost::program_options::options_description& desc)
{
    namespace po = boost::program_options;
    desc.add_options()("help,h", "Display help message")(
        "nudb-path,n",
        po::value<std::string>()->required(),
        "Path to the NuDB database directory (required)")(
        "log-level,l",
        po::value<std::string>()->default_value("info"),
        "Log level: error, warn, info, debug");
}

/**
 * Parse common options from a variables_map
 *
 * @param vm The parsed variables map
 * @return CommonOptions structure populated with values
 */
inline CommonOptions
parse_common_options(const boost::program_options::variables_map& vm)
{
    CommonOptions options;

    if (vm.count("help"))
    {
        options.show_help = true;
    }

    if (vm.count("nudb-path"))
    {
        options.nudb_path = vm["nudb-path"].as<std::string>();
    }

    if (vm.count("log-level"))
    {
        options.log_level = vm["log-level"].as<std::string>();
    }

    return options;
}

}  // namespace nudbutil
