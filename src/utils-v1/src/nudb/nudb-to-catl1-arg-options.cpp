#include "catl/utils-v1/nudb/nudb-to-catl1-arg-options.h"

#include <boost/program_options.hpp>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

namespace po = boost::program_options;
namespace catl::v1::utils::nudb {

NudbToCatl1Options
parse_nudb_to_catl1_argv(int argc, char* argv[])
{
    NudbToCatl1Options options;

    // Define command line options with Boost
    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "Display this help message")(
        "nudb-path,n",
        po::value<std::string>(),
        "Path to the NuDB database directory")(
        "output,o", po::value<std::string>(), "Path to the output CATL file")(
        "start-ledger,s",
        po::value<uint32_t>(),
        "Start ledger sequence number")(
        "end-ledger,e", po::value<uint32_t>(), "End ledger sequence number")(
        "network-id",
        po::value<uint16_t>()->default_value(0),
        "Network ID for the output CATL file")(
        "compression-level,c",
        po::value<int>()->default_value(0),
        "Compression level for output (0-9)")(
        "force,f",
        po::bool_switch(),
        "Force overwrite of existing output file without prompting")(
        "log-level,l",
        po::value<std::string>()->default_value("info"),
        "Log level (error, warn, info, debug)");

    // Generate the help text
    std::ostringstream help_stream;
    help_stream
        << "NuDB to CATL Converter Tool" << std::endl
        << "---------------------------" << std::endl
        << "Converts a NuDB database into a CATL v1 file format" << std::endl
        << std::endl
        << "Usage: " << (argc > 0 ? argv[0] : "nudb-to-catl1")
        << " --nudb-path <db_directory> --output <output_file> "
           "--start-ledger <seq> --end-ledger <seq> [options]"
        << std::endl
        << desc << std::endl
        << "This tool reads ledger data from a NuDB database and creates "
           "a CATL file"
        << std::endl
        << "containing the specified range of ledgers." << std::endl;
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

        // Check for required output file
        if (vm.count("output"))
        {
            options.output_file = vm["output"].as<std::string>();
        }
        else
        {
            options.valid = false;
            options.error_message = "No output file specified (--output)";
            return options;
        }

        // Check for required start ledger
        if (vm.count("start-ledger"))
        {
            options.start_ledger = vm["start-ledger"].as<uint32_t>();
        }
        else
        {
            options.valid = false;
            options.error_message =
                "No start ledger specified (--start-ledger)";
            return options;
        }

        // Check for required end ledger
        if (vm.count("end-ledger"))
        {
            options.end_ledger = vm["end-ledger"].as<uint32_t>();

            // Validate end_ledger is >= start_ledger
            if (options.start_ledger &&
                *options.end_ledger < *options.start_ledger)
            {
                options.valid = false;
                options.error_message = "End ledger must be >= start ledger";
                return options;
            }
        }
        else
        {
            options.valid = false;
            options.error_message = "No end ledger specified (--end-ledger)";
            return options;
        }

        // Get network ID
        if (vm.count("network-id"))
        {
            options.network_id = vm["network-id"].as<uint16_t>();
        }

        // Get compression level
        if (vm.count("compression-level"))
        {
            int level = vm["compression-level"].as<int>();
            if (level < 0 || level > 9)
            {
                options.valid = false;
                options.error_message =
                    "Compression level must be between 0 and 9, got: " +
                    std::to_string(level);
                return options;
            }
            options.compression_level = static_cast<uint8_t>(level);
        }

        // Check force overwrite flag
        if (vm.count("force"))
        {
            options.force_overwrite = vm["force"].as<bool>();
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