#include "catl/utils/slicer/arg-options.h"
#include <boost/program_options.hpp>
#include <filesystem>
#include <iostream>
#include <sstream>

namespace po = boost::program_options;
namespace fs = std::filesystem;

namespace catl::utils::slicer {

CommandLineOptions
parse_argv(int argc, char* argv[])
{
    CommandLineOptions options;

    // Set up the help text with basic usage
    std::ostringstream help_text;
    help_text
        << "catl-slice: High-Performance CATL File Slicing Tool\n\n"
        << "Usage: catl-slice --input <input_catl_file> \\\n"
        << "                  --output <output_slice_file> \\\n"
        << "                  --start-ledger <start_sequence_number> \\\n"
        << "                  --end-ledger <end_sequence_number> \\\n"
        << "                  [--snapshots-path "
           "<path_to_directory_for_snapshots>] \\\n"
        << "                  [--compression-level <0-9>] \\\n"
        << "                  [--force-overwrite] \\\n"
        << "                  [--no-create-next-slice-state-snapshot] \\\n"
        << "                  [--no-use-start-snapshot] \\\n"
        << "                  [--log-level <error|warn|info|debug>] \\\n"
        << "                  [--help]\n\n";

    // Define the options
    po::options_description desc("Options");
    desc.add_options()("help,h", "Display this help message")(
        "input,i",
        po::value<std::string>(),
        "Path to the source CATL file (v1 format) (required)")(
        "output,o",
        po::value<std::string>(),
        "Path where the generated CATL slice file will be saved (required)")(
        "start-ledger",
        po::value<uint32_t>(),
        "The sequence number of the first ledger to include in the slice "
        "(required)")(
        "end-ledger",
        po::value<uint32_t>(),
        "The sequence number of the last ledger to include in the slice "
        "(required)")(
        "snapshots-path",
        po::value<std::string>(),
        "Directory where state snapshots are stored and looked for")(
        "compression-level",
        po::value<uint8_t>()->default_value(0),
        "Compression level (0-9, where 0 means uncompressed)")(
        "force-overwrite",
        "If output files exist, overwrite them without prompting")(
        "create-next-slice-state-snapshot",
        po::value<bool>()->default_value(true),
        "Create a state snapshot for the next slice")(
        "no-create-next-slice-state-snapshot",
        "Disable creation of a state snapshot for the next slice")(
        "use-start-snapshot",
        po::value<bool>()->default_value(true),
        "Use a start snapshot if available")(
        "no-use-start-snapshot", "Ignore any existing start snapshots")(
        "log-level",
        po::value<std::string>()->default_value("info"),
        "Log verbosity (error, warn, info, debug)");

    // Store the formatted options help
    std::ostringstream desc_stream;
    desc_stream << desc;
    help_text << desc_stream.str();
    options.help_text = help_text.str();

    try
    {
        // Parse the command line arguments
        po::variables_map vm;
        po::store(po::parse_command_line(argc, argv, desc), vm);
        po::notify(vm);

        // Check for help flag
        if (vm.count("help"))
        {
            options.show_help = true;
            return options;
        }

        // Process required arguments
        if (!vm.count("input"))
        {
            options.valid = false;
            options.error_message = "Error: --input option is required";
            return options;
        }
        options.input_file = vm["input"].as<std::string>();

        if (!vm.count("output"))
        {
            options.valid = false;
            options.error_message = "Error: --output option is required";
            return options;
        }
        options.output_file = vm["output"].as<std::string>();

        if (!vm.count("start-ledger"))
        {
            options.valid = false;
            options.error_message = "Error: --start-ledger option is required";
            return options;
        }
        options.start_ledger = vm["start-ledger"].as<uint32_t>();

        if (!vm.count("end-ledger"))
        {
            options.valid = false;
            options.error_message = "Error: --end-ledger option is required";
            return options;
        }
        options.end_ledger = vm["end-ledger"].as<uint32_t>();

        // Validate ledger range
        if (options.start_ledger > options.end_ledger)
        {
            options.valid = false;
            options.error_message =
                "Error: start-ledger must be less than or equal to end-ledger";
            return options;
        }

        // Process optional arguments
        if (vm.count("snapshots-path"))
        {
            options.snapshots_path = vm["snapshots-path"].as<std::string>();
        }

        options.compression_level = vm["compression-level"].as<uint8_t>();
        if (options.compression_level > 9)
        {
            options.compression_level = 9;
        }

        options.force_overwrite = vm.count("force-overwrite") > 0;

        // Handle snapshot creation flag
        if (vm.count("no-create-next-slice-state-snapshot"))
        {
            options.create_next_slice_state_snapshot = false;
        }
        else if (vm.count("create-next-slice-state-snapshot"))
        {
            options.create_next_slice_state_snapshot =
                vm["create-next-slice-state-snapshot"].as<bool>();
        }

        // Handle start snapshot usage flag
        if (vm.count("no-use-start-snapshot"))
        {
            options.use_start_snapshot = false;
        }
        else if (vm.count("use-start-snapshot"))
        {
            options.use_start_snapshot = vm["use-start-snapshot"].as<bool>();
        }

        // Process log level
        if (vm.count("log-level"))
        {
            options.log_level =
                string_to_log_level(vm["log-level"].as<std::string>());
        }

        // Set default snapshots path if not provided
        if (!options.snapshots_path)
        {
            fs::path output_dir =
                fs::path(options.output_file.value()).parent_path();
            options.snapshots_path = (output_dir / "catl_snapshots").string();
        }
    }
    catch (const po::error& e)
    {
        options.valid = false;
        options.error_message =
            std::string("Error parsing command line: ") + e.what();
    }
    catch (const std::exception& e)
    {
        options.valid = false;
        options.error_message = std::string("Error: ") + e.what();
    }

    return options;
}

SlicerLogLevel
string_to_log_level(const std::string& level_str)
{
    if (level_str == "error")
        return SlicerLogLevel::ERROR;
    if (level_str == "warn")
        return SlicerLogLevel::WARN;
    if (level_str == "info")
        return SlicerLogLevel::INFO;
    if (level_str == "debug")
        return SlicerLogLevel::DEBUG;
    return SlicerLogLevel::INFO;  // Default
}

std::string
log_level_to_string(SlicerLogLevel level)
{
    switch (level)
    {
        case SlicerLogLevel::ERROR:
            return "error";
        case SlicerLogLevel::WARN:
            return "warn";
        case SlicerLogLevel::INFO:
            return "info";
        case SlicerLogLevel::DEBUG:
            return "debug";
        default:
            return "info";
    }
}

LogLevel
convert_to_core_log_level(SlicerLogLevel level)
{
    switch (level)
    {
        case SlicerLogLevel::ERROR:
            return LogLevel::ERROR;
        case SlicerLogLevel::WARN:
            return LogLevel::WARNING;
        case SlicerLogLevel::INFO:
            return LogLevel::INFO;
        case SlicerLogLevel::DEBUG:
            return LogLevel::DEBUG;
        default:
            return LogLevel::INFO;
    }
}

}  // namespace catl::utils::slicer