#include <array>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

// For memory mapping
#include <boost/filesystem.hpp>
#include <boost/iostreams/device/mapped_file.hpp>

// For compression/decompression
#include <boost/iostreams/filter/zlib.hpp>
#include <boost/iostreams/filtering_stream.hpp>

// For crypto
#include <openssl/evp.h>

// Magic number and parsing masks
static constexpr uint32_t CATL = 0x4C544143UL;  // "CATL" in LE
static constexpr uint16_t CATALOGUE_VERSION_MASK = 0x00FF;
static constexpr uint16_t CATALOGUE_COMPRESS_LEVEL_MASK = 0x0F00;
static constexpr uint16_t CATALOGUE_RESERVED_MASK = 0xF000;

// Helper functions for version field manipulation
inline uint8_t
getCatalogueVersion(uint16_t versionField)
{
    return versionField & CATALOGUE_VERSION_MASK;
}

inline uint8_t
getCompressionLevel(uint16_t versionField)
{
    return (versionField & CATALOGUE_COMPRESS_LEVEL_MASK) >> 8;
}

inline bool
isCompressed(uint16_t versionField)
{
    return getCompressionLevel(versionField) > 0;
}

inline uint16_t
makeCatalogueVersionField(uint8_t version, uint8_t compressionLevel = 0)
{  // 0 = no compression
    // Ensure compression level is within valid range (0-9)
    if (compressionLevel > 9)
        compressionLevel = 9;

    uint16_t result = version & CATALOGUE_VERSION_MASK;
    result |= (compressionLevel << 8);  // Store level in bits 8-11
    return result;
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

#pragma pack(push, 1)  // pack the struct tightly
struct CATLHeader
{
    uint32_t magic = CATL;
    uint32_t min_ledger;
    uint32_t max_ledger;
    uint16_t version;
    uint16_t network_id;
    uint64_t filesize = 0;  // Total size of the file including header
    std::array<uint8_t, 64> hash = {};  // SHA-512 hash, initially set to zeros
};
#pragma pack(pop)

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
    std::string inputFilePath;
    std::string outputFilePath;
    boost::iostreams::mapped_file_source mmapFile;
    const uint8_t* data = nullptr;
    size_t fileSize = 0;
    CATLHeader header;

public:
    CATLDecompressor(const std::string& inFile, const std::string& outFile)
        : inputFilePath(inFile), outputFilePath(outFile)
    {
        if (!boost::filesystem::exists(inputFilePath))
        {
            throw std::runtime_error(
                "Input file does not exist: " + inputFilePath);
        }

        boost::filesystem::path path(inputFilePath);
        boost::uintmax_t actualFileSize = boost::filesystem::file_size(path);
        if (actualFileSize == 0)
        {
            throw std::runtime_error("Input file is empty: " + inputFilePath);
        }

        mmapFile.open(inputFilePath);
        if (!mmapFile.is_open())
        {
            throw std::runtime_error(
                "Failed to memory map file: " + inputFilePath);
        }

        data = reinterpret_cast<const uint8_t*>(mmapFile.data());
        fileSize = mmapFile.size();

        std::cout << "Opened file: " << inputFilePath << " (" << fileSize
                  << " bytes, " << format_file_size(fileSize) << ")"
                  << std::endl;
    }

    ~CATLDecompressor()
    {
        if (mmapFile.is_open())
        {
            mmapFile.close();
        }
    }

    bool
    validateHeader()
    {
        if (fileSize < sizeof(CATLHeader))
        {
            std::cerr << "File too small to contain a valid CATL header"
                      << std::endl;
            return false;
        }

        std::memcpy(&header, data, sizeof(CATLHeader));

        if (header.magic != CATL)
        {
            std::cerr << "Invalid magic value: expected 0x" << std::hex << CATL
                      << ", got 0x" << header.magic << std::dec << std::endl;
            return false;
        }

        uint8_t compressionLevel = getCompressionLevel(header.version);
        uint8_t version = getCatalogueVersion(header.version);

        if (compressionLevel == 0)
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
                  << static_cast<int>(compressionLevel) << std::endl;
        std::cout << "  Network ID: " << header.network_id << std::endl;
        std::cout << "  File size: " << header.filesize << " bytes ("
                  << format_file_size(header.filesize) << ")" << std::endl;

        // Output hash if present
        bool hasNonZeroHash = false;
        for (size_t i = 0; i < header.hash.size(); i++)
        {
            if (header.hash[i] != 0)
            {
                hasNonZeroHash = true;
                break;
            }
        }

        if (hasNonZeroHash)
        {
            std::string hashHex =
                toHexString(header.hash.data(), header.hash.size());
            std::cout << "  Hash: " << hashHex << std::endl;
        }
        else
        {
            std::cout << "  Hash: Not set (all zeros)" << std::endl;
        }

        if (header.filesize != fileSize)
        {
            std::cerr << "Warning: Header file size (" << header.filesize
                      << " bytes, " << format_file_size(header.filesize)
                      << ") doesn't match actual file size (" << fileSize
                      << " bytes, " << format_file_size(fileSize) << ")"
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

        std::cout << "Creating output file: " << outputFilePath << std::endl;
        std::ofstream outFile(outputFilePath, std::ios::binary);
        if (!outFile)
        {
            std::cerr << "Failed to create output file: " << outputFilePath
                      << std::endl;
            return false;
        }

        // Create a new header with compression level set to 0
        CATLHeader newHeader = header;
        uint8_t version = getCatalogueVersion(header.version);
        newHeader.version =
            makeCatalogueVersionField(version, 0);  // Set compression to 0
        newHeader.filesize = 0;                     // Will be updated later
        newHeader.hash.fill(0);  // Clear hash, will be calculated later

        // Write the new header to the output file
        outFile.write(
            reinterpret_cast<const char*>(&newHeader), sizeof(CATLHeader));
        if (!outFile)
        {
            std::cerr << "Failed to write header to output file" << std::endl;
            return false;
        }

        // Set up decompression
        uint8_t compressionLevel = getCompressionLevel(header.version);

        std::cout << "Decompressing data with compression level "
                  << static_cast<int>(compressionLevel) << "..." << std::endl;

        // Open the input file for reading after the header
        std::ifstream inFile(inputFilePath, std::ios::binary);
        if (!inFile)
        {
            std::cerr << "Failed to open input file for reading" << std::endl;
            return false;
        }

        // Skip the header in the input file
        inFile.seekg(sizeof(CATLHeader));

        // Set up a zlib decompression stream
        boost::iostreams::filtering_istream decompStream;
        boost::iostreams::zlib_params params(
            compressionLevel);  // Use same level as input
        params.window_bits = 15;
        params.noheader = false;
        decompStream.push(boost::iostreams::zlib_decompressor(params));
        decompStream.push(inFile);

        // Copy data from decompression stream to output file
        char buffer[64 * 1024];  // 64K buffer
        size_t totalBytesWritten = sizeof(CATLHeader);

        auto startTime = std::chrono::high_resolution_clock::now();
        size_t lastReport = 0;
        auto lastTimeReport = startTime;
        size_t bytesProcessedSinceLastTime = 0;

        try
        {
            while (decompStream)
            {
                decompStream.read(buffer, sizeof(buffer));
                std::streamsize bytesRead = decompStream.gcount();
                if (bytesRead > 0)
                {
                    // Check for corruption (this is a simple integrity check)
                    if (decompStream.bad())
                    {
                        throw std::runtime_error(
                            "Decompression stream reported bad state - corrupt "
                            "data detected");
                    }

                    outFile.write(buffer, bytesRead);
                    if (outFile.bad())
                    {
                        throw std::runtime_error(
                            "Error writing to output file");
                    }

                    totalBytesWritten += bytesRead;
                    bytesProcessedSinceLastTime += bytesRead;

                    // Report progress every 10MB or 2 seconds
                    auto now = std::chrono::high_resolution_clock::now();
                    auto timeSinceLastReport =
                        std::chrono::duration_cast<std::chrono::seconds>(
                            now - lastTimeReport)
                            .count();

                    if (totalBytesWritten - lastReport > 10 * 1024 * 1024 ||
                        timeSinceLastReport >= 2)
                    {
                        double elapsedSeconds =
                            std::chrono::duration_cast<
                                std::chrono::milliseconds>(now - startTime)
                                .count() /
                            1000.0;

                        // Calculate speed
                        double mbPerSec =
                            (totalBytesWritten / (1024.0 * 1024.0)) /
                            elapsedSeconds;

                        // Estimate remaining time if we have data to make a
                        // guess
                        std::string etaStr;
                        if (totalBytesWritten > 0 && header.filesize > 0 &&
                            header.filesize > totalBytesWritten)
                        {
                            // Estimate output size as 3x input size
                            // (conservative decompression ratio)
                            double estimatedTotalSize = header.filesize * 3;
                            double remainingBytes =
                                estimatedTotalSize - totalBytesWritten;
                            double remainingSeconds = remainingBytes /
                                (totalBytesWritten / elapsedSeconds);

                            if (remainingSeconds < 60)
                            {
                                etaStr = std::to_string(static_cast<int>(
                                             remainingSeconds)) +
                                    " sec";
                            }
                            else if (remainingSeconds < 3600)
                            {
                                etaStr = std::to_string(static_cast<int>(
                                             remainingSeconds / 60)) +
                                    " min";
                            }
                            else
                            {
                                etaStr = std::to_string(static_cast<int>(
                                             remainingSeconds / 3600)) +
                                    " hr " +
                                    std::to_string(
                                             static_cast<int>(
                                                 (remainingSeconds / 60)) %
                                             60) +
                                    " min";
                            }
                        }
                        else
                        {
                            etaStr = "unknown";
                        }

                        std::cout << "  Progress: "
                                  << format_file_size(totalBytesWritten) << " ("
                                  << std::fixed << std::setprecision(2)
                                  << mbPerSec << " MB/s, ETA: " << etaStr << ")"
                                  << "\r" << std::flush;

                        lastReport = totalBytesWritten;
                        lastTimeReport = now;
                        bytesProcessedSinceLastTime = 0;
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
            decompStream.reset();
            inFile.close();
            outFile.close();

            // Remove partial output file
            std::cout << "Removing incomplete output file: " << outputFilePath
                      << std::endl;
            boost::filesystem::remove(outputFilePath);

            return false;
        }

        std::cout << std::endl;
        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            endTime - startTime);
        double seconds = duration.count() / 1000.0;
        double mbPerSec = (totalBytesWritten / (1024.0 * 1024.0)) / seconds;

        std::cout << "Decompression completed in " << std::fixed
                  << std::setprecision(2) << seconds << " seconds (" << mbPerSec
                  << " MB/s)" << std::endl;
        std::cout << "Total bytes written: " << totalBytesWritten << " ("
                  << format_file_size(totalBytesWritten) << ")" << std::endl;

        // Close streams
        decompStream.reset();
        inFile.close();
        outFile.close();

        // Update the file size in the header
        std::fstream updateFile(
            outputFilePath, std::ios::in | std::ios::out | std::ios::binary);
        if (!updateFile)
        {
            std::cerr << "Failed to reopen output file for header update"
                      << std::endl;
            return false;
        }

        // Update the header with the final file size
        newHeader.filesize = totalBytesWritten;
        updateFile.write(
            reinterpret_cast<const char*>(&newHeader), sizeof(CATLHeader));

        if (!updateFile)
        {
            std::cerr << "Failed to update header with file size" << std::endl;
            return false;
        }
        updateFile.close();

        // Now compute the hash over the entire file
        std::cout << "Computing hash for output file..." << std::endl;
        std::ifstream hashFile(outputFilePath, std::ios::binary);
        if (!hashFile)
        {
            std::cerr << "Failed to open output file for hashing" << std::endl;
            return false;
        }

        // Set up OpenSSL hash context for SHA-512
        EVP_MD_CTX* ctx = EVP_MD_CTX_new();
        if (!ctx)
        {
            std::cerr << "Failed to create EVP_MD_CTX" << std::endl;
            return false;
        }

        if (EVP_DigestInit_ex(ctx, EVP_sha512(), nullptr) != 1)
        {
            std::cerr << "Failed to initialize SHA-512 digest" << std::endl;
            EVP_MD_CTX_free(ctx);
            return false;
        }

        // Read and process the header with zero hash
        hashFile.read(reinterpret_cast<char*>(&newHeader), sizeof(CATLHeader));
        std::fill(newHeader.hash.begin(), newHeader.hash.end(), 0);

        // Add the modified header to the hash
        if (EVP_DigestUpdate(ctx, &newHeader, sizeof(CATLHeader)) != 1)
        {
            std::cerr << "Failed to update digest with header" << std::endl;
            EVP_MD_CTX_free(ctx);
            return false;
        }

        // Read and hash the rest of the file
        while (hashFile)
        {
            hashFile.read(buffer, sizeof(buffer));
            std::streamsize bytesRead = hashFile.gcount();
            if (bytesRead > 0)
            {
                if (EVP_DigestUpdate(ctx, buffer, bytesRead) != 1)
                {
                    std::cerr << "Failed to update digest with file data"
                              << std::endl;
                    EVP_MD_CTX_free(ctx);
                    return false;
                }
            }
        }

        // Get the hash result
        std::array<unsigned char, 64> hash_result;
        unsigned int hashLen = 0;
        if (EVP_DigestFinal_ex(ctx, hash_result.data(), &hashLen) != 1)
        {
            std::cerr << "Failed to finalize digest" << std::endl;
            EVP_MD_CTX_free(ctx);
            return false;
        }
        EVP_MD_CTX_free(ctx);
        hashFile.close();

        // Update the hash in the output file header
        std::fstream updateHashFile(
            outputFilePath, std::ios::in | std::ios::out | std::ios::binary);
        if (!updateHashFile)
        {
            std::cerr << "Failed to reopen output file for hash update"
                      << std::endl;
            return false;
        }

        // Seek to the hash position in the header
        updateHashFile.seekp(offsetof(CATLHeader, hash), std::ios::beg);
        updateHashFile.write(
            reinterpret_cast<const char*>(hash_result.data()),
            hash_result.size());

        if (!updateHashFile)
        {
            std::cerr << "Failed to update header with hash" << std::endl;
            return false;
        }
        updateHashFile.close();

        std::string hashHex =
            toHexString(hash_result.data(), hash_result.size());
        std::cout << "Hash: " << hashHex << std::endl;

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
        std::string inputFile = argv[1];
        std::string outputFile = argv[2];

        // Check if input and output are the same
        if (boost::filesystem::equivalent(
                boost::filesystem::path(inputFile),
                boost::filesystem::path(outputFile)))
        {
            std::cerr << "Error: Input and output files must be different"
                      << std::endl;
            return 1;
        }

        // Check if output file already exists
        if (boost::filesystem::exists(outputFile))
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

        CATLDecompressor decomp(inputFile, outputFile);
        if (decomp.decompress())
        {
            std::cout << "Successfully decompressed " << inputFile << " to "
                      << outputFile << std::endl;
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