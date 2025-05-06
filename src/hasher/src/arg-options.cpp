#include "catl/hasher/arg-options.h"
#include "catl/core/logger.h"

#include <algorithm>
#include <boost/program_options.hpp>
#include <cctype>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

namespace po = boost::program_options;
namespace catl::hasher {

LogLevel
string_to_log_level(const std::string& level_str)
{
    // Convert to lowercase for case-insensitive comparison
    std::string level_lower = level_str;
    std::transform(
        level_lower.begin(),
        level_lower.end(),
        level_lower.begin(),
        [](unsigned char c) { return std::tolower(c); });

    if (level_lower == "error")
        return LogLevel::ERROR;
    if (level_lower == "warn" || level_lower == "warning")
        return LogLevel::WARN;
    if (level_lower == "info")
        return LogLevel::INFO;
    if (level_lower == "debug")
        return LogLevel::DEBUG;

    // Default to INFO if invalid
    return LogLevel::INFO;
}

std::string
log_level_to_string(LogLevel level)
{
    switch (level)
    {
        case LogLevel::ERROR:
            return "error";
        case LogLevel::WARN:
            return "warn";
        case LogLevel::INFO:
            return "info";
        case LogLevel::DEBUG:
            return "debug";
        default:
            return "info";
    }
}

CommandLineOptions
parse_argv(int argc, char* argv[])
{
    CommandLineOptions options;

    // Define command line options with Boost
    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "Display this help message")(
        "input-file", po::value<std::string>(), "Path to the CATL file")(
        "level,l",
        po::value<std::string>()->default_value("info"),
        "Set log verbosity (error, warn, info, debug)")(
        "serve,s", po::bool_switch(), "Start HTTP server");

    // Set up positional arguments
    po::positional_options_description pos_desc;
    pos_desc.add("input-file", 1);

    // Generate the help text
    std::ostringstream helpStream;
    helpStream << "Usage: " << (argc > 0 ? argv[0] : "catl-hasher")
               << " [options] <catalogue_file>" << std::endl
               << desc << std::endl
               << "Processes CATL files, builds SHAMaps, verifies hashes."
               << std::endl;
    options.help_text = helpStream.str();

    try
    {
        // Parse command line
        po::variables_map vm;
        po::store(
            po::command_line_parser(argc, argv)
                .options(desc)
                .positional(pos_desc)
                .run(),
            vm);
        po::notify(vm);

        // Check for help flag
        if (vm.count("help"))
        {
            options.show_help = true;
            return options;
        }

        // Check for required input file
        if (vm.count("input-file"))
        {
            options.input_file = vm["input-file"].as<std::string>();
        }
        else
        {
            options.valid = false;
            options.error_message = "No input file specified";
            return options;
        }

        // Parse log level
        if (vm.count("level"))
        {
            std::string level_str = vm["level"].as<std::string>();
            options.log_level = string_to_log_level(level_str);
        }

        // Check server flag
        if (vm.count("serve"))
        {
            options.start_server = vm["serve"].as<bool>();
        }
    }
    catch (const po::error& e)
    {
        options.valid = false;
        options.error_message = e.what();
    }
    catch (const std::exception& e)
    {
        options.valid = false;
        options.error_message = std::string("Unexpected error: ") + e.what();
    }

    return options;
}

}  // namespace catl::hasher