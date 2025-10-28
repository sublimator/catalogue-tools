#include "catl/utils-v1/nudb/nudb-exp-arg-options.h"

#include <boost/program_options.hpp>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

namespace po = boost::program_options;
namespace catl::v1::utils::nudb {

NudbExpOptions
parse_nudb_exp_argv(int argc, char* argv[])
{
    NudbExpOptions options;

    // Define command line options with Boost
    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "Display this help message")(
        "nudb-path,n",
        po::value<std::string>(),
        "Path to the NuDB database directory")(
        "key,k", po::value<std::string>(), "Key to lookup (in hex)")(
        "format,f",
        po::value<std::string>()->default_value("hex"),
        "Output format: hex, binary, info")(
        "list-keys", po::bool_switch(), "List all keys in the database")(
        "stats", po::bool_switch(), "Show database statistics")(
        "log-level,l",
        po::value<std::string>()->default_value("info"),
        "Log level (error, warn, info, debug)");

    // Generate the help text
    std::ostringstream help_stream;
    help_stream << "NuDB Explorer Tool" << std::endl
                << "------------------" << std::endl
                << "Explore and query NuDB databases" << std::endl
                << std::endl
                << "Usage: " << (argc > 0 ? argv[0] : "nudb-exp")
                << " --nudb-path <db_directory> [options]" << std::endl
                << desc << std::endl
                << "Examples:" << std::endl
                << "  Get a specific key:" << std::endl
                << "    nudb-exp -n /path/to/db -k 00000001" << std::endl
                << "  List all keys:" << std::endl
                << "    nudb-exp -n /path/to/db --list-keys" << std::endl
                << "  Show database stats:" << std::endl
                << "    nudb-exp -n /path/to/db --stats" << std::endl;
    options.help_text = help_stream.str();

    try
    {
        // Parse command line
        po::variables_map vm;
        po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);
        po::notify(vm);

        // Check for help flag
        if (vm.count("help"))
        {
            options.show_help = true;
            return options;
        }

        // Check for required nudb path
        if (vm.count("nudb-path"))
        {
            options.nudb_path = vm["nudb-path"].as<std::string>();
        }
        else
        {
            options.valid = false;
            options.error_message = "No NuDB path specified (--nudb-path)";
            return options;
        }

        // Check for key
        if (vm.count("key"))
        {
            options.key_hex = vm["key"].as<std::string>();
        }

        // Check output format
        if (vm.count("format"))
        {
            std::string format = vm["format"].as<std::string>();
            if (format != "hex" && format != "binary" && format != "info")
            {
                options.valid = false;
                options.error_message =
                    "Invalid output format. Must be: hex, binary, or info";
                return options;
            }
            options.output_format = format;
        }

        // Check list keys flag
        if (vm.count("list-keys"))
        {
            options.list_keys = vm["list-keys"].as<bool>();
        }

        // Check stats flag
        if (vm.count("stats"))
        {
            options.show_stats = vm["stats"].as<bool>();
        }

        // Get log level
        if (vm.count("log-level"))
        {
            std::string level = vm["log-level"].as<std::string>();
            // Validate log level
            if (level != "error" && level != "warn" && level != "info" &&
                level != "debug")
            {
                options.valid = false;
                options.error_message =
                    "Log level must be one of: error, warn, info, debug";
                return options;
            }
            options.log_level = level;
        }

        // Validate that at least one action is specified
        if (!options.key_hex && !options.list_keys && !options.show_stats)
        {
            options.valid = false;
            options.error_message =
                "Must specify an action: --key, --list-keys, or --stats";
            return options;
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

}  // namespace catl::v1::utils::nudb