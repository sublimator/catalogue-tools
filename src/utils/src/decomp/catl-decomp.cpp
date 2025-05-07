#include "catl/utils/decomp/arg-options.h"
#include "catl/v1/catl-v1-reader.h"
#include "catl/v1/catl-v1-utils.h"
#include "catl/v1/catl-v1-writer.h"
#include <boost/filesystem.hpp>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

using namespace catl::v1;
using namespace catl::utils::decomp;

// Format file size in human-readable format
std::string
format_file_size(uint64_t bytes)
{
    static const char* units[] = {"B", "KB", "MB", "GB", "TB", "PB"};
    int unit_index = 0;
    auto size = static_cast<double>(bytes);

    while (size >= 1024.0 && unit_index < 5)
    {
        size /= 1024.0;
        unit_index++;
    }

    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2) << size << " "
        << units[unit_index];
    return oss.str();
}

/**
 * CATLDecompressor - A simple CATL file decompression utility
 *
 * This tool has a single focused responsibility: take a compressed CATL file
 * and create an uncompressed version that can be processed by other tools.
 *
 * The decompression process:
 * 1. Read the header information from the compressed file
 * 2. Create a new file with identical header information, but with compression
 * level set to 0
 * 3. Simply decompress the body to the new file's uncompressed body without
 * examining the contents
 * 4. Let the Reader and Writer classes handle the actual data decompression and
 * copying
 * 5. Update the output file's size and hash values during finalization
 *
 * Note: This tool doesn't need to understand the internal structure of the CATL
 * file data. It simply relies on the Reader class to handle decompression of
 * the input stream and the Writer class to create the proper uncompressed
 * output stream. Conceptually, it's equivalent to:
 * compressed_reader.stream_pipe(uncompressed_writer.body_stream())
 *
 * After decompression, use catl-validator or other tools to verify file
 * integrity.
 */
class CATLDecompressor
{
private:
    std::string input_file_path_;
    std::string output_file_path_;

public:
    CATLDecompressor(std::string in_file, std::string out_file)
        : input_file_path_(std::move(in_file))
        , output_file_path_(std::move(out_file))
    {
        if (!boost::filesystem::exists(input_file_path_))
        {
            throw CatlV1Error("Input file does not exist: " + input_file_path_);
        }

        // Check if input and output are the same
        if (boost::filesystem::equivalent(
                boost::filesystem::path(input_file_path_),
                boost::filesystem::path(output_file_path_)))
        {
            throw CatlV1Error("Input and output files must be different");
        }
    }

    bool
    decompress()
    {
        try
        {
            // Open the input file with Reader class which handles decompression
            std::cout << "Opening input file: " << input_file_path_
                      << std::endl;
            Reader reader(input_file_path_);

            boost::uintmax_t input_file_size =
                boost::filesystem::file_size(input_file_path_);
            std::cout << "Input file size: " << input_file_size << " ("
                      << format_file_size(input_file_size) << ")" << std::endl;

            // Get header and compression information
            const CatlHeader& header = reader.header();
            uint8_t compression_level = get_compression_level(header.version);

            if (compression_level == 0)
            {
                std::cerr << "File is not compressed (level 0). No need to "
                             "decompress."
                          << std::endl;
                return false;
            }

            std::cout << "File information:" << std::endl;
            std::cout << "  Ledger range: " << header.min_ledger << " - "
                      << header.max_ledger << " ("
                      << (header.max_ledger - header.min_ledger + 1)
                      << " ledgers)" << std::endl;
            std::cout << "  Compression level: "
                      << static_cast<int>(compression_level) << std::endl;
            std::cout << "  Network ID: " << header.network_id << std::endl;

            // Start decompression
            std::cout << "Starting decompression..." << std::endl;
            auto start_time = std::chrono::high_resolution_clock::now();

            // Use the Reader::decompress method
            try
            {
                if (!reader.decompress(output_file_path_))
                {
                    std::cerr << "Decompression failed" << std::endl;
                    return false;
                }
            }
            catch (const CatlV1Error& e)
            {
                std::cerr << "Decompression failed: " << e.what() << std::endl;
                return false;
            }

            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    end_time - start_time);
            double seconds = duration.count() / 1000.0;

            boost::uintmax_t output_file_size =
                boost::filesystem::file_size(output_file_path_);

            std::cout << "Decompression completed successfully:" << std::endl;
            std::cout << "  Time taken: " << std::fixed << std::setprecision(2)
                      << seconds << " seconds" << std::endl;
            std::cout << "  Output file size: " << output_file_size << " ("
                      << format_file_size(output_file_size) << ")" << std::endl;

            // Calculate expansion ratio
            if (input_file_size > 0)
            {
                const double ratio =
                    static_cast<double>(output_file_size) / input_file_size;
                std::cout << "  Expansion ratio: " << std::fixed
                          << std::setprecision(2) << ratio << "x" << std::endl;
            }

            return true;
        }
        catch (const CatlV1Error& e)
        {
            std::cerr << "Catalogue error: " << e.what() << std::endl;
            return false;
        }
        catch (const std::exception& e)
        {
            std::cerr << "Error: " << e.what() << std::endl;
            return false;
        }
    }
};

int
main(int argc, char* argv[])
{
    // Parse command line arguments
    CommandLineOptions options = parse_argv(argc, argv);

    // Display help if requested or if there was a parsing error
    if (options.show_help || !options.valid)
    {
        if (!options.valid && options.error_message)
        {
            std::cerr << "Error: " << *options.error_message << std::endl
                      << std::endl;
        }
        std::cout << options.help_text << std::endl;
        return options.valid ? 0 : 1;
    }

    try
    {
        // At this point we know input_file and output_file are present
        std::string input_file = *options.input_file;
        std::string output_file = *options.output_file;

        // Check if output file already exists
        if (boost::filesystem::exists(output_file) && !options.force_overwrite)
        {
            std::cout
                << "Warning: Output file already exists. Overwrite? (y/n): ";
            char response;
            std::cin >> response;
            if (response != 'y' && response != 'Y')
            {
                std::cout << "Operation canceled by user." << std::endl;
                return 0;
            }
        }

        std::cout << "Starting decompression: " << input_file << " -> "
                  << output_file << std::endl;

        CATLDecompressor decomp(input_file, output_file);
        if (decomp.decompress())
        {
            std::cout << "Successfully decompressed file" << std::endl;
            return 0;
        }
        else
        {
            std::cerr << "Failed to decompress the file" << std::endl;
            return 1;
        }
    }
    catch (const std::exception& e)
    {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
}
