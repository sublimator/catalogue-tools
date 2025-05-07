#include <algorithm>
#include <array>
#include <boost/filesystem/operations.hpp>
#include <boost/system/detail/error_category.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

// For memory mapping
#include <boost/iostreams/device/mapped_file.hpp>

// For compression/decompression
#include <boost/iostreams/filter/zlib.hpp>
#include <boost/iostreams/filtering_stream.hpp>

// For crypto
#include <openssl/evp.h>

//
#include "catl/v1/catl-v1-structs.h"
#include "catl/v1/catl-v1-utils.h"

using namespace catl::v1;

inline bool
isCompressed(uint16_t versionField)
{
    return is_compressed(versionField);
}

inline uint16_t
makeCatalogueVersionField(uint8_t version, uint8_t compression_level = 0)
{
    return make_catalogue_version_field(version, compression_level);
}

// Helper function to convert binary hash to hex string
std::string
toHexString(unsigned char const* data, size_t len)
{
    static char const* hexDigits = "0123456789ABCDEF";
    std::string result;
    result.reserve(2 * len);
    for (size_t i = 0; i < len; ++i)
    {
        unsigned char c = data[i];
        result.push_back(hexDigits[c >> 4]);
        result.push_back(hexDigits[c & 15]);
    }
    return result;
}
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
    boost::iostreams::mapped_file_source mmap_file_;
    const uint8_t* data = nullptr;
    size_t file_size_ = 0;
    CatlHeader header;

public:
    CATLDecompressor(const std::string& inFile, const std::string& outFile)
        : input_file_path_(inFile), output_file_path_(outFile)
    {
        if (!boost::filesystem::exists(input_file_path_))
        {
            throw std::runtime_error(
                "Input file does not exist: " + input_file_path_);
        }

        boost::filesystem::path path(input_file_path_);
        boost::uintmax_t actual_file_size = boost::filesystem::file_size(path);
        if (actual_file_size == 0)
        {
            throw std::runtime_error(
                "Input file is empty: " + input_file_path_);
        }

        mmap_file_.open(input_file_path_);
        if (!mmap_file_.is_open())
        {
            throw std::runtime_error(
                "Failed to memory map file: " + input_file_path_);
        }

        data = reinterpret_cast<const uint8_t*>(mmap_file_.data());
        file_size_ = mmap_file_.size();

        std::cout << "Opened file: " << input_file_path_ << " (" << file_size_
                  << " bytes, " << format_file_size(file_size_) << ")"
                  << std::endl;
    }

    ~CATLDecompressor()
    {
        if (mmap_file_.is_open())
        {
            mmap_file_.close();
        }
    }

    bool
    validateHeader()
    {
        if (file_size_ < sizeof(CatlHeader))
        {
            std::cerr << "File too small to contain a valid CATL header"
                      << std::endl;
            return false;
        }

        std::memcpy(&header, data, sizeof(CatlHeader));

        if (header.magic != CATL_MAGIC)
        {
            std::cerr << "Invalid magic value: expected 0x" << std::hex
                      << CATL_MAGIC << ", got 0x" << header.magic << std::dec
                      << std::endl;
            return false;
        }

        uint8_t compression_level = get_compression_level(header.version);
        uint8_t version = get_catalogue_version(header.version);

        if (compression_level == 0)
        {
            std::cerr
                << "File is not compressed (level 0). No need to decompress."
                << std::endl;
            return false;
        }

        // Validate ledger range
        if (header.min_ledger > header.max_ledger)
        {
            std::cerr << "Invalid ledger range: min_ledger ("
                      << header.min_ledger << ") is greater than max_ledger ("
                      << header.max_ledger << ")" << std::endl;
            return false;
        }

        // Sanity check on file size
        const uint64_t MAX_REASONABLE_SIZE =
            1ULL * 1024 * 1024 * 1024 * 1024;  // 1 TB
        if (header.filesize > MAX_REASONABLE_SIZE)
        {
            std::cerr
                << "Warning: Header reports an unusually large file size: "
                << format_file_size(header.filesize) << std::endl;
            std::cerr << "This may indicate file corruption. Continue anyway? "
                         "(y/n): ";
            char response;
            std::cin >> response;
            if (response != 'y' && response != 'Y')
            {
                return false;
            }
        }

        // Display header information
        std::cout << "CATL Header Validated:" << std::endl;
        std::cout << "  Magic: 0x" << std::hex << header.magic << std::dec
                  << std::endl;
        std::cout << "  Ledger range: " << header.min_ledger << " - "
                  << header.max_ledger << " ("
                  << (header.max_ledger - header.min_ledger + 1) << " ledgers)"
                  << std::endl;
        std::cout << "  Version: " << static_cast<int>(version) << std::endl;
        std::cout << "  Compression Level: "
                  << static_cast<int>(compression_level) << std::endl;
        std::cout << "  Network ID: " << header.network_id << std::endl;
        std::cout << "  File size: " << header.filesize << " bytes ("
                  << format_file_size(header.filesize) << ")" << std::endl;

        // Output hash if present
        bool non_zero_hash = false;
        for (size_t i = 0; i < header.hash.size(); i++)
        {
            if (header.hash[i] != 0)
            {
                non_zero_hash = true;
                break;
            }
        }

        if (non_zero_hash)
        {
            std::string hash_hex =
                toHexString(header.hash.data(), header.hash.size());
            std::cout << "  Hash: " << hash_hex << std::endl;
        }
        else
        {
            std::cout << "  Hash: Not set (all zeros)" << std::endl;
        }

        if (header.filesize != file_size_)
        {
            std::cerr << "Warning: Header file size (" << header.filesize
                      << " bytes, " << format_file_size(header.filesize)
                      << ") doesn't match actual file size (" << file_size_
                      << " bytes, " << format_file_size(file_size_) << ")"
                      << std::endl;
        }

        return true;
    }

    bool
    decompress()
    {
        if (!validateHeader())
        {
            return false;
        }

        std::cout << "Creating output file: " << output_file_path_ << std::endl;
        std::ofstream out_file(output_file_path_, std::ios::binary);
        if (!out_file)
        {
            std::cerr << "Failed to create output file: " << output_file_path_
                      << std::endl;
            return false;
        }

        // Create a new header with compression level set to 0
        CatlHeader new_header = header;
        uint8_t version = get_catalogue_version(header.version);
        new_header.version =
            makeCatalogueVersionField(version, 0);  // Set compression to 0
        new_header.filesize = 0;                    // Will be updated later
        new_header.hash.fill(0);  // Clear hash, will be calculated later

        // Write the new header to the output file
        out_file.write(
            reinterpret_cast<const char*>(&new_header), sizeof(CatlHeader));
        if (!out_file)
        {
            std::cerr << "Failed to write header to output file" << std::endl;
            return false;
        }

        // Set up decompression
        uint8_t compression_level = get_compression_level(header.version);

        std::cout << "Decompressing data with compression level "
                  << static_cast<int>(compression_level) << "..." << std::endl;

        // Open the input file for reading after the header
        std::ifstream in_file(input_file_path_, std::ios::binary);
        if (!in_file)
        {
            std::cerr << "Failed to open input file for reading" << std::endl;
            return false;
        }

        // Skip the header in the input file
        in_file.seekg(sizeof(CatlHeader));

        // Set up a zlib decompression stream
        boost::iostreams::filtering_istream decomp_stream;
        boost::iostreams::zlib_params params(
            compression_level);  // Use same level as input
        params.window_bits = 15;
        params.noheader = false;
        decomp_stream.push(boost::iostreams::zlib_decompressor(params));
        decomp_stream.push(in_file);

        // Copy data from decompression stream to output file
        char buffer[64 * 1024];  // 64K buffer
        size_t total_bytes_written = sizeof(CatlHeader);

        auto start_time = std::chrono::high_resolution_clock::now();
        size_t last_report = 0;
        auto last_time_report = start_time;
        size_t bytes_processed_since_last_time = 0;

        try
        {
            while (decomp_stream)
            {
                decomp_stream.read(buffer, sizeof(buffer));
                std::streamsize bytes_read = decomp_stream.gcount();
                if (bytes_read > 0)
                {
                    // Check for corruption (this is a simple integrity check)
                    if (decomp_stream.bad())
                    {
                        throw std::runtime_error(
                            "Decompression stream reported bad state - corrupt "
                            "data detected");
                    }

                    out_file.write(buffer, bytes_read);
                    if (out_file.bad())
                    {
                        throw std::runtime_error(
                            "Error writing to output file");
                    }

                    total_bytes_written += bytes_read;
                    bytes_processed_since_last_time += bytes_read;

                    // Report progress every 10MB or 2 seconds
                    auto now = std::chrono::high_resolution_clock::now();
                    auto time_since_last_report =
                        std::chrono::duration_cast<std::chrono::seconds>(
                            now - last_time_report)
                            .count();

                    if (total_bytes_written - last_report > 10 * 1024 * 1024 ||
                        time_since_last_report >= 2)
                    {
                        double elapsed_seconds =
                            std::chrono::duration_cast<
                                std::chrono::milliseconds>(now - start_time)
                                .count() /
                            1000.0;

                        // Calculate speed
                        double mb_per_sec =
                            (total_bytes_written / (1024.0 * 1024.0)) /
                            elapsed_seconds;

                        // Estimate remaining time if we have data to make a
                        // guess
                        std::string eta_str;
                        if (total_bytes_written > 0 && header.filesize > 0 &&
                            header.filesize > total_bytes_written)
                        {
                            // Estimate output size as 3x input size
                            // (conservative decompression ratio)
                            double estimated_total_size = header.filesize * 3;
                            double remaining_bytes =
                                estimated_total_size - total_bytes_written;
                            double remaining_seconds = remaining_bytes /
                                (total_bytes_written / elapsed_seconds);

                            if (remaining_seconds < 60)
                            {
                                eta_str = std::to_string(static_cast<int>(
                                              remaining_seconds)) +
                                    " sec";
                            }
                            else if (remaining_seconds < 3600)
                            {
                                eta_str = std::to_string(static_cast<int>(
                                              remaining_seconds / 60)) +
                                    " min";
                            }
                            else
                            {
                                eta_str = std::to_string(static_cast<int>(
                                              remaining_seconds / 3600)) +
                                    " hr " +
                                    std::to_string(
                                              static_cast<int>(
                                                  (remaining_seconds / 60)) %
                                              60) +
                                    " min";
                            }
                        }
                        else
                        {
                            eta_str = "unknown";
                        }

                        std::cout << "  Progress: "
                                  << format_file_size(total_bytes_written)
                                  << " (" << std::fixed << std::setprecision(2)
                                  << mb_per_sec << " MB/s, ETA: " << eta_str
                                  << ")"
                                  << "\r" << std::flush;

                        last_report = total_bytes_written;
                        last_time_report = now;
                        bytes_processed_since_last_time = 0;
                    }
                }
            }
        }
        catch (const std::exception& e)
        {
            std::cerr << std::endl
                      << "Error during decompression: " << e.what()
                      << std::endl;

            // Close files before removing
            decomp_stream.reset();
            in_file.close();
            out_file.close();

            // Remove partial output file
            std::cout << "Removing incomplete output file: "
                      << output_file_path_ << std::endl;
            boost::filesystem::remove(output_file_path_);

            return false;
        }

        std::cout << std::endl;
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time);
        double seconds = duration.count() / 1000.0;
        double mb_per_sec = (total_bytes_written / (1024.0 * 1024.0)) / seconds;

        std::cout << "Decompression completed in " << std::fixed
                  << std::setprecision(2) << seconds << " seconds ("
                  << mb_per_sec << " MB/s)" << std::endl;
        std::cout << "Total bytes written: " << total_bytes_written << " ("
                  << format_file_size(total_bytes_written) << ")" << std::endl;

        // Close streams
        decomp_stream.reset();
        in_file.close();
        out_file.close();

        // Update the file size in the header
        std::fstream update_file(
            output_file_path_, std::ios::in | std::ios::out | std::ios::binary);
        if (!update_file)
        {
            std::cerr << "Failed to reopen output file for header update"
                      << std::endl;
            return false;
        }

        // Update the header with the final file size
        new_header.filesize = total_bytes_written;
        update_file.write(
            reinterpret_cast<const char*>(&new_header), sizeof(CatlHeader));

        if (!update_file)
        {
            std::cerr << "Failed to update header with file size" << std::endl;
            return false;
        }
        update_file.close();

        // Now compute the hash over the entire file
        std::cout << "Computing hash for output file..." << std::endl;
        std::ifstream hash_file(output_file_path_, std::ios::binary);
        if (!hash_file)
        {
            std::cerr << "Failed to open output file for hashing" << std::endl;
            return false;
        }

        // Declare hash_result outside the try block so it's visible in the
        // scope where it's used later
        std::array<unsigned char, 64> hash_result;
        unsigned int hash_len = 0;

        try
        {
            // Use the Sha512Hasher class from the catalogue-v1 library
            Sha512Hasher hasher;

            // Read and process the header with zero hash
            hash_file.read(
                reinterpret_cast<char*>(&new_header), sizeof(CatlHeader));
            std::fill(new_header.hash.begin(), new_header.hash.end(), 0);

            // Add the modified header to the hash
            hasher.update(&new_header, sizeof(CatlHeader));

            // Read and hash the rest of the file
            while (hash_file)
            {
                hash_file.read(buffer, sizeof(buffer));
                std::streamsize bytes_read = hash_file.gcount();
                if (bytes_read > 0)
                {
                    hasher.update(buffer, bytes_read);
                }
            }

            // Get the hash result
            hasher.final(hash_result.data(), &hash_len);

            if (hash_len != hash_result.size())
            {
                std::cerr << "Unexpected hash length" << std::endl;
                hash_file.close();
                return false;
            }
        }
        catch (const std::exception& e)
        {
            std::cerr << "Hash computation failed: " << e.what() << std::endl;
            hash_file.close();
            return false;
        }
        hash_file.close();

        // Update the hash in the output file header
        std::fstream update_hash_file(
            output_file_path_, std::ios::in | std::ios::out | std::ios::binary);
        if (!update_hash_file)
        {
            std::cerr << "Failed to reopen output file for hash update"
                      << std::endl;
            return false;
        }

        // Seek to the hash position in the header
        update_hash_file.seekp(offsetof(CatlHeader, hash), std::ios::beg);
        update_hash_file.write(
            reinterpret_cast<const char*>(hash_result.data()),
            hash_result.size());

        if (!update_hash_file)
        {
            std::cerr << "Failed to update header with hash" << std::endl;
            return false;
        }
        update_hash_file.close();

        std::string hash_hex =
            toHexString(hash_result.data(), hash_result.size());
        std::cout << "Hash: " << hash_hex << std::endl;

        return true;
    }
};

int
main(int argc, char* argv[])
{
    // Check for help flag
    if (argc > 1 &&
        (std::string(argv[1]) == "-h" || std::string(argv[1]) == "--help"))
    {
        std::cout << "CATL Decompressor Tool" << std::endl;
        std::cout << "----------------------" << std::endl;
        std::cout
            << "Converts a compressed CATL file to an uncompressed version."
            << std::endl
            << std::endl;
        std::cout << "Usage: " << argv[0]
                  << " <input_catl_file> <output_catl_file>" << std::endl
                  << std::endl;
        std::cout << "The tool will:" << std::endl;
        std::cout
            << "  1. Check if the input file is a valid compressed CATL file"
            << std::endl;
        std::cout << "  2. Decompress the contents" << std::endl;
        std::cout << "  3. Write a new file with compression level set to 0"
                  << std::endl;
        std::cout << "  4. Update the header with the correct file size"
                  << std::endl;
        std::cout << "  5. Calculate and update the hash" << std::endl;
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

        // Check if input and output are the same
        if (boost::filesystem::equivalent(
                boost::filesystem::path(input_file),
                boost::filesystem::path(output_file)))
        {
            std::cerr << "Error: Input and output files must be different"
                      << std::endl;
            return 1;
        }

        // Check if output file already exists
        if (boost::filesystem::exists(output_file))
        {
            std::cerr
                << "Warning: Output file already exists. Overwrite? (y/n): ";
            char response;
            std::cin >> response;
            if (response != 'y' && response != 'Y')
            {
                std::cout << "Operation canceled by user." << std::endl;
                return 0;
            }
        }

        CATLDecompressor decomp(input_file, output_file);
        if (decomp.decompress())
        {
            std::cout << "Successfully decompressed " << input_file << " to "
                      << output_file << std::endl;
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
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}