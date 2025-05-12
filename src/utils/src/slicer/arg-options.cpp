#include "catl/utils/slicer/arg-options.h"

#include <boost/program_options.hpp>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

namespace po = boost::program_options;
namespace catl::utils::slicer {

CommandLineOptions
parse_argv(int argc, char* argv[])
{
    CommandLineOptions options;

    // Define command line options with Boost
    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "Display this help message")(
        "input,i",
        po::value<std::string>(),
        "Path to the source CATL file (v1 format)")(
        "output,o",
        po::value<std::string>(),
        "Path where the generated CATL slice file will be saved")(
        "start-ledger,s",
        po::value<uint32_t>(),
        "The sequence number of the first ledger to include in the slice")(
        "end-ledger,e",
        po::value<uint32_t>(),
        "The sequence number of the last ledger to include in the slice")(
        "snapshots-path",
        po::value<std::string>(),
        "Directory where state snapshots are stored and looked for")(
        "compression-level",
        po::value<uint8_t>()->default_value(0),
        "Compression level for output (0-9)")(
        "force-overwrite,f",
        po::bool_switch(),
        "Force overwrite of existing output file without prompting")(
        "no-create-next-slice-state-snapshot",
        po::bool_switch(),
        "Disable creation of state snapshot for the next slice")(
        "no-use-start-snapshot",
        po::bool_switch(),
        "Don't use existing start snapshot even if available")(
        "log-level,l",
        po::value<std::string>()->default_value("info"),
        "Log level (error, warn, info, debug)");

    // Generate the help text
    std::ostringstream help_stream;
    help_stream << "CATL Slice Tool" << std::endl
                << "-------------" << std::endl
                << "Extract a contiguous range of ledgers (a 'slice') from a "
                   "larger CATL file"
                << std::endl
                << std::endl
                << "Usage: " << (argc > 0 ? argv[0] : "catl-slice")
                << " --input <input_file> --output <output_file> "
                   "--start-ledger <seq> --end-ledger <seq> [options]"
                << std::endl
                << desc << std::endl
                << "This tool extracts a specified range of ledgers from a "
                   "CATL file, creating a new"
                << std::endl
                << "valid CATL file containing only those ledgers. It can "
                   "optionally use and create"
                << std::endl
                << "state snapshots to improve performance when creating "
                   "multiple consecutive slices."
                << std::endl;
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

        // Check optional snapshots path
        if (vm.count("snapshots-path"))
        {
            options.snapshots_path = vm["snapshots-path"].as<std::string>();
        }

        // Get compression level
        if (vm.count("compression-level"))
        {
            uint8_t level = vm["compression-level"].as<uint8_t>();
            if (level > 9)
            {
                options.valid = false;
                options.error_message =
                    "Compression level must be between 0 and 9";
                return options;
            }
            options.compression_level = level;
        }

        // Check force overwrite flag
        if (vm.count("force-overwrite"))
        {
            options.force_overwrite = vm["force-overwrite"].as<bool>();
        }

        // Check snapshot creation flag (inverted from command line)
        if (vm.count("no-create-next-slice-state-snapshot"))
        {
            options.create_next_slice_state_snapshot =
                !vm["no-create-next-slice-state-snapshot"].as<bool>();
        }

        // Check use start snapshot flag (inverted from command line)
        if (vm.count("no-use-start-snapshot"))
        {
            options.use_start_snapshot =
                !vm["no-use-start-snapshot"].as<bool>();
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

}  // namespace catl::utils::slicer