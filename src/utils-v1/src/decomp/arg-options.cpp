#include "catl/utils-v1/decomp/arg-options.h"

#include <boost/program_options.hpp>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

namespace po = boost::program_options;
namespace catl::utils::decomp {

CommandLineOptions
parse_argv(int argc, char* argv[])
{
    CommandLineOptions options;

    // Define command line options with Boost
    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "Display this help message")(
        "input-file", po::value<std::string>(), "Path to the input CATL file")(
        "output-file",
        po::value<std::string>(),
        "Path to the output CATL file")(
        "compression-level,c",
        po::value<int>(),
        "Target compression level (0-9, where 0 is uncompressed). "
        "If not specified, level 0 (decompression) is used")(
        "force,f",
        po::bool_switch(),
        "Force overwrite of existing output file without prompting");

    // Set up positional arguments
    po::positional_options_description pos_desc;
    pos_desc.add("input-file", 1);
    pos_desc.add("output-file", 1);

    // Generate the help text
    std::ostringstream help_stream;
    help_stream
        << "CATL Copy Tool" << std::endl
        << "--------------" << std::endl
        << "Copies a CATL file with optional compression level change"
        << std::endl
        << std::endl
        << "Usage: " << (argc > 0 ? argv[0] : "catl1-copy")
        << " [options] <input_catl_file> <output_catl_file>" << std::endl
        << desc << std::endl
        << "Examples:" << std::endl
        << "  Decompress a file (level 0):" << std::endl
        << "    catl1-copy compressed.catl uncompressed.catl" << std::endl
        << "  Compress with maximum compression (level 9):" << std::endl
        << "    catl1-copy uncompressed.catl compressed.catl -c 9" << std::endl
        << "  Recompress at different level:" << std::endl
        << "    catl1-copy level5.catl level9.catl -c 9" << std::endl
        << std::endl
        << "The tool copies the file contents without examining them, using"
        << std::endl
        << "the Reader and Writer classes to handle compression/decompression."
        << std::endl;
    options.help_text = help_stream.str();

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

        // Check for required output file
        if (vm.count("output-file"))
        {
            options.output_file = vm["output-file"].as<std::string>();
        }
        else
        {
            options.valid = false;
            options.error_message = "No output file specified";
            return options;
        }

        // Check force overwrite flag
        if (vm.count("force"))
        {
            options.force_overwrite = vm["force"].as<bool>();
        }

        // Check compression level
        if (vm.count("compression-level"))
        {
            int level = vm["compression-level"].as<int>();
            if (level < 0 || level > 9)
            {
                options.valid = false;
                options.error_message =
                    "Compression level must be between 0 and 9";
                return options;
            }
            options.compression_level = level;
        }
        else
        {
            // Default to decompression (level 0) if not specified
            options.compression_level = 0;
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

}  // namespace catl::utils::decomp