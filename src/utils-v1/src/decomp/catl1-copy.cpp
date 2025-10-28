#include "catl/utils-v1/decomp/arg-options.h"
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
using namespace catl::v1::utils::decomp;

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
 * CATLCopier - A CATL file copy utility with compression level control
 *
 * This tool copies CATL files while optionally changing the compression level.
 * It can be used to:
 * - Decompress files (compression level 0)
 * - Compress uncompressed files (compression levels 1-9)
 * - Recompress files at different levels
 *
 * The copy process:
 * 1. Read the header information from the input file
 * 2. Create a new file with identical header information, but with the
 *    specified compression level
 * 3. Copy the body data through the appropriate compression/decompression
 * 4. Let the Reader and Writer classes handle the actual data transformation
 * 5. Update the output file's size and hash values during finalization
 *
 * Note: This tool doesn't need to understand the internal structure of the CATL
 * file data. It simply relies on the Reader class methods to handle the
 * data transformation.
 */
class CATLCopier
{
private:
    std::string input_file_path_;
    std::string output_file_path_;
    int target_compression_level_;

public:
    CATLCopier(std::string in_file, std::string out_file, int compression_level)
        : input_file_path_(std::move(in_file))
        , output_file_path_(std::move(out_file))
        , target_compression_level_(compression_level)
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

        // Validate compression level
        if (target_compression_level_ < 0 || target_compression_level_ > 9)
        {
            throw CatlV1Error(
                "Invalid compression level: " +
                std::to_string(target_compression_level_) + " (must be 0-9)");
        }
    }

    bool
    copy()
    {
        try
        {
            // Open the input file with Reader class
            std::cout << "Opening input file: " << input_file_path_
                      << std::endl;
            Reader reader(input_file_path_);

            boost::uintmax_t input_file_size =
                boost::filesystem::file_size(input_file_path_);
            std::cout << "Input file size: " << input_file_size << " ("
                      << format_file_size(input_file_size) << ")" << std::endl;

            // Get header and compression information
            const CatlHeader& header = reader.header();
            uint8_t current_compression_level =
                get_compression_level(header.version);

            std::cout << "File information:" << std::endl;
            std::cout << "  Ledger range: " << header.min_ledger << " - "
                      << header.max_ledger << " ("
                      << (header.max_ledger - header.min_ledger + 1)
                      << " ledgers)" << std::endl;
            std::cout << "  Current compression level: "
                      << static_cast<int>(current_compression_level)
                      << std::endl;
            std::cout << "  Target compression level: "
                      << target_compression_level_ << std::endl;
            std::cout << "  Network ID: " << header.network_id << std::endl;

            if (current_compression_level == target_compression_level_)
            {
                std::cerr << "File is already at compression level "
                          << target_compression_level_ << ". No need to copy."
                          << std::endl;
                return false;
            }

            // Determine operation type for messaging
            std::string operation;
            if (target_compression_level_ == 0)
            {
                operation = "decompression";
            }
            else if (current_compression_level == 0)
            {
                operation = "compression";
            }
            else
            {
                operation = "recompression";
            }

            // Start copy operation
            std::cout << "Starting " << operation << "..." << std::endl;
            auto start_time = std::chrono::high_resolution_clock::now();

            // Use the appropriate Reader method
            try
            {
                if (target_compression_level_ == 0)
                {
                    reader.decompress(output_file_path_);
                }
                else
                {
                    reader.compress(
                        output_file_path_, target_compression_level_);
                }
            }
            catch (const CatlV1Error& e)
            {
                std::cerr << "Copy failed: " << e.what() << std::endl;
                return false;
            }

            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    end_time - start_time);
            double seconds = duration.count() / 1000.0;

            boost::uintmax_t output_file_size =
                boost::filesystem::file_size(output_file_path_);

            std::cout << "Copy completed successfully:" << std::endl;
            std::cout << "  Time taken: " << std::fixed << std::setprecision(2)
                      << seconds << " seconds" << std::endl;
            std::cout << "  Output file size: " << output_file_size << " ("
                      << format_file_size(output_file_size) << ")" << std::endl;

            // Calculate size change ratio
            if (input_file_size > 0)
            {
                const double ratio =
                    static_cast<double>(output_file_size) / input_file_size;
                const double percent_change = (ratio - 1.0) * 100.0;

                if (ratio > 1.0)
                {
                    std::cout << "  Expansion ratio: " << std::fixed
                              << std::setprecision(2) << ratio << "x (+"
                              << std::setprecision(1) << percent_change << "%)"
                              << std::endl;
                }
                else
                {
                    std::cout << "  Compression ratio: " << std::fixed
                              << std::setprecision(2) << ratio << "x ("
                              << std::setprecision(1) << percent_change << "%)"
                              << std::endl;
                }
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
        // At this point we know input_file, output_file and compression_level
        // are present
        std::string input_file = *options.input_file;
        std::string output_file = *options.output_file;
        int compression_level = *options.compression_level;

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

        std::cout << "Starting copy: " << input_file << " -> " << output_file
                  << " (compression level " << compression_level << ")"
                  << std::endl;

        CATLCopier copier(input_file, output_file, compression_level);
        if (copier.copy())
        {
            std::cout << "Successfully copied file" << std::endl;
            return 0;
        }
        else
        {
            std::cerr << "Failed to copy the file" << std::endl;
            return 1;
        }
    }
    catch (const std::exception& e)
    {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
}