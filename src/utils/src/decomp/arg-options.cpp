#include "catl/utils/decomp/arg-options.h"

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
        "input-file",
        po::value<std::string>(),
        "Path to the compressed CATL file")(
        "output-file",
        po::value<std::string>(),
        "Path to the output uncompressed CATL file")(
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
        << "CATL Decompressor Tool" << std::endl
        << "--------------------" << std::endl
        << "Converts a compressed CATL file to an uncompressed version"
        << std::endl
        << std::endl
        << "Usage: " << (argc > 0 ? argv[0] : "catl-decomp")
        << " [options] <input_catl_file> <output_catl_file>" << std::endl
        << desc << std::endl
        << "The tool simply decompresses the contents without examining them, "
           "using"
        << std::endl
        << "the Reader and Writer classes to handle the actual data transfer."
        << std::endl
        << std::endl
        << "For a full-featured implementation, see catl-decomp-reference."
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