#include "catl/v1/catl-v1-reader.h"
#include "catl/v1/catl-v1-utils.h"
#include "catl/v1/catl-v1-writer.h"
#include <boost/filesystem.hpp>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string>

using namespace catl::v1;

// Format file size in human-readable format
std::string
format_file_size(uint64_t bytes)
{
    static const char* units[] = {"B", "KB", "MB", "GB", "TB", "PB"};
    int unit_index = 0;
    double size = static_cast<double>(bytes);

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

class CATLDecompressor
{
private:
    std::string input_file_path_;
    std::string output_file_path_;

public:
    CATLDecompressor(const std::string& inFile, const std::string& outFile)
        : input_file_path_(inFile), output_file_path_(outFile)
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

            // Create writer options for decompressed file
            WriterOptions writer_options;
            writer_options.network_id = header.network_id;
            writer_options.compression_level = 0;  // Uncompressed output

            // Create writer for output file
            std::cout << "Creating output file: " << output_file_path_
                      << std::endl;
            auto writer = Writer::for_file(output_file_path_, writer_options);

            // Write the header with the same ledger range
            try
            {
                writer->write_header(header.min_ledger, header.max_ledger);
            }
            catch (const CatlV1Error& e)
            {
                std::cerr << "Failed to write header to output file: "
                          << e.what() << std::endl;
                return false;
            }

            // Process all ledgers from input file
            std::cout << "Starting decompression process..." << std::endl;
            auto start_time = std::chrono::high_resolution_clock::now();
            uint32_t ledger_count = 0;

            // For progress reporting
            auto last_update_time = start_time;
            bool first_status = true;

            try
            {
                while (true)
                {
                    // Read ledger header (this now throws instead of returning
                    // nullopt)
                    LedgerInfo ledger_info;
                    try
                    {
                        ledger_info = reader.read_ledger_info();
                    }
                    catch (const CatlV1Error& e)
                    {
                        // End of file reached
                        break;
                    }

                    // Write ledger header to output file
                    try
                    {
                        writer->write_ledger_header(ledger_info);
                    }
                    catch (const CatlV1Error& e)
                    {
                        std::cerr
                            << "Failed to write ledger header: " << e.what()
                            << std::endl;
                        return false;
                    }

                    // TODO: Add shamap reading/writing code in a future version
                    // For now, we don't have access to the full ledger
                    // structure through the Reader interface. The focus here is
                    // to demonstrate using the Reader for decompression.

                    ledger_count++;

                    // Show progress every 2 seconds
                    auto now = std::chrono::high_resolution_clock::now();
                    auto elapsed =
                        std::chrono::duration_cast<std::chrono::seconds>(
                            now - last_update_time)
                            .count();

                    if (first_status || elapsed >= 2)
                    {
                        double total_seconds =
                            std::chrono::duration_cast<
                                std::chrono::milliseconds>(now - start_time)
                                .count() /
                            1000.0;

                        if (total_seconds > 0)
                        {
                            double ledgers_per_sec =
                                ledger_count / total_seconds;
                            std::cout << "Processed " << ledger_count
                                      << " ledgers (" << std::fixed
                                      << std::setprecision(2) << ledgers_per_sec
                                      << " ledgers/sec)" << std::endl;
                        }
                        else
                        {
                            std::cout << "Processed " << ledger_count
                                      << " ledgers" << std::endl;
                        }

                        last_update_time = now;
                        first_status = false;
                    }
                }
            }
            catch (const std::exception& e)
            {
                std::cerr << "Error during decompression: " << e.what()
                          << std::endl;
                return false;
            }

            // Finalize the output file
            try
            {
                writer->finalize();
            }
            catch (const CatlV1Error& e)
            {
                std::cerr << "Failed to finalize output file: " << e.what()
                          << std::endl;
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
            std::cout << "  Ledgers processed: " << ledger_count << std::endl;
            std::cout << "  Output file size: " << output_file_size << " ("
                      << format_file_size(output_file_size) << ")" << std::endl;

            // Calculate compression ratio
            if (input_file_size > 0)
            {
                double ratio =
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
    // Check for help flag
    if (argc > 1 &&
        (std::string(argv[1]) == "-h" || std::string(argv[1]) == "--help"))
    {
        std::cout << "CATL Decompressor Tool (Library-based Version)"
                  << std::endl;
        std::cout << "------------------------------------------" << std::endl;
        std::cout
            << "Converts a compressed CATL file to an uncompressed version"
            << std::endl;
        std::cout << "using the CatlV1 Reader and Writer classes." << std::endl;
        std::cout << std::endl;
        std::cout << "Usage: " << argv[0]
                  << " <input_catl_file> <output_catl_file>" << std::endl;
        std::cout << std::endl;
        std::cout << "The tool will:" << std::endl;
        std::cout << "  1. Read the compressed CATL file using Reader class"
                  << std::endl;
        std::cout << "  2. Create a new uncompressed file with Writer class"
                  << std::endl;
        std::cout << "  3. Process ledger headers (note: SHAMap data is not "
                     "currently processed)"
                  << std::endl;
        std::cout << std::endl;
        std::cout
            << "For a full-featured implementation, see catl-decomp-reference"
            << std::endl;
        return 0;
    }

    if (argc != 3)
    {
        std::cerr << "Usage: " << argv[0]
                  << " <input_catl_file> <output_catl_file>" << std::endl;
        std::cerr << "Run with --help for more information." << std::endl;
        return 1;
    }

    try
    {
        std::string input_file = argv[1];
        std::string output_file = argv[2];

        // Check if output file already exists
        if (boost::filesystem::exists(output_file))
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
