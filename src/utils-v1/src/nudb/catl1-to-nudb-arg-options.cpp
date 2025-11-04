#include "catl/utils-v1/nudb/catl1-to-nudb-arg-options.h"

#include <boost/program_options.hpp>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

namespace po = boost::program_options;
namespace catl::v1::utils::nudb {

Catl1ToNudbOptions
parse_catl1_to_nudb_argv(int argc, char* argv[])
{
    Catl1ToNudbOptions options;

    // Define command line options with Boost
    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "Display this help message")(
        "input,i", po::value<std::string>(), "Path to the input CATL file")(
        "nudb-path,n",
        po::value<std::string>(),
        "Path to the output NuDB database directory")(
        "start-ledger,s",
        po::value<uint32_t>(),
        "Start ledger sequence number (optional, defaults to file's "
        "min_ledger)")(
        "end-ledger,e",
        po::value<uint32_t>(),
        "End ledger sequence number (optional, defaults to file's max_ledger)")(
        "force,f",
        po::bool_switch(),
        "Force overwrite of existing database without prompting")(
        "create-database",
        po::bool_switch()->default_value(true),
        "Create database if it doesn't exist")(
        "key-size",
        po::value<uint32_t>()->default_value(32),
        "NuDB key size in bytes (default: 32 for 256-bit hashes)")(
        "block-size",
        po::value<uint32_t>()->default_value(4096),
        "NuDB block size (default: 4096)")(
        "load-factor",
        po::value<double>()->default_value(0.5),
        "NuDB load factor 0.0-1.0 (default: 0.5)")(
        "log-level,l",
        po::value<std::string>()->default_value("info"),
        "Log level (error, warn, info, debug)")(
        "test-snapshots",
        po::bool_switch(),
        "Test snapshot memory usage (reads file and creates snapshots without pipeline)");

    // Generate the help text
    std::ostringstream help_stream;
    help_stream
        << "CATL to NuDB Converter Tool" << std::endl
        << "---------------------------" << std::endl
        << "Converts a CATL v1 file into a NuDB database" << std::endl
        << std::endl
        << "Usage: " << (argc > 0 ? argv[0] : "catl1-to-nudb")
        << " --input <catl_file> --nudb-path <db_directory> [options]"
        << std::endl
        << desc << std::endl
        << "This tool reads ledger data from a CATL file and stores it "
           "in a NuDB database"
        << std::endl
        << "for efficient key-value lookups. If start/end ledgers are not "
           "specified,"
        << std::endl
        << "it will process the entire file." << std::endl;
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

        // Check for required input file
        if (vm.count("input"))
        {
            options.input_file = vm["input"].as<std::string>();
        }
        else
        {
            options.valid = false;
            options.error_message = "No input file specified (--input)";
            return options;
        }

        // Check for test-snapshots mode
        if (vm.count("test-snapshots"))
        {
            options.test_snapshots = vm["test-snapshots"].as<bool>();
        }

        // Check for required nudb path (not required in test-snapshots mode)
        if (vm.count("nudb-path"))
        {
            options.nudb_path = vm["nudb-path"].as<std::string>();
        }
        else if (!options.test_snapshots)
        {
            options.valid = false;
            options.error_message = "No NuDB path specified (--nudb-path)";
            return options;
        }

        // Check for optional start ledger
        if (vm.count("start-ledger"))
        {
            options.start_ledger = vm["start-ledger"].as<uint32_t>();
        }

        // Check for optional end ledger
        if (vm.count("end-ledger"))
        {
            options.end_ledger = vm["end-ledger"].as<uint32_t>();

            // Validate end_ledger is >= start_ledger if both specified
            if (options.start_ledger &&
                *options.end_ledger < *options.start_ledger)
            {
                options.valid = false;
                options.error_message = "End ledger must be >= start ledger";
                return options;
            }
        }

        // Check force overwrite flag
        if (vm.count("force"))
        {
            options.force_overwrite = vm["force"].as<bool>();
        }

        // Check create database flag
        if (vm.count("create-database"))
        {
            options.create_database = vm["create-database"].as<bool>();
        }

        // Get NuDB parameters
        if (vm.count("key-size"))
        {
            options.key_size = vm["key-size"].as<uint32_t>();
            if (options.key_size == 0)
            {
                options.valid = false;
                options.error_message = "Key size must be greater than 0";
                return options;
            }
        }

        if (vm.count("block-size"))
        {
            options.block_size = vm["block-size"].as<uint32_t>();
            if (options.block_size == 0)
            {
                options.valid = false;
                options.error_message = "Block size must be greater than 0";
                return options;
            }
        }

        if (vm.count("load-factor"))
        {
            options.load_factor = vm["load-factor"].as<double>();
            if (options.load_factor <= 0.0 || options.load_factor > 1.0)
            {
                options.valid = false;
                options.error_message =
                    "Load factor must be between 0.0 and 1.0";
                return options;
            }
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