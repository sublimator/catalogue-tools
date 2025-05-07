#include <algorithm>
#include <array>
#include <boost/core/ref.hpp>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <exception>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

// For decompression
#include <boost/iostreams/filter/zlib.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <sstream>

// Constants from the rippled code
static constexpr uint32_t CATL = 0x4C544143UL;  // "CATL" in LE
static constexpr uint16_t CATALOGUE_VERSION_MASK = 0x00FF;
static constexpr uint16_t CATALOGUE_COMPRESS_LEVEL_MASK = 0x0F00;
static constexpr uint16_t CATALOGUE_RESERVED_MASK = 0xF000;
static constexpr uint16_t BASE_CATALOGUE_VERSION = 1;

// The updated header structure with hash and filesize
#pragma pack(push, 1)  // pack the struct tightly
struct CATLHeader
{
    uint32_t magic = 0x4C544143UL;  // "CATL" in LE
    uint32_t min_ledger;
    uint32_t max_ledger;
    uint16_t version;
    uint16_t network_id;
    uint64_t filesize = 0;  // Total size of the file including header
    std::array<uint8_t, 64> hash = {};  // SHA-512 hash, initially set to zeros
};
#pragma pack(pop)

enum SHAMapNodeType : uint8_t {
    tnINNER = 1,
    tnTRANSACTION_NM = 2,  // transaction, no metadata
    tnTRANSACTION_MD = 3,  // transaction, with metadata
    tnACCOUNT_STATE = 4,
    tnREMOVE = 254,
    tnTERMINAL = 255  // special type to mark the end of a serialization stream
};

#include <openssl/sha.h>

// Function to compute SHA512 using OpenSSL
std::string
computeSHA512(const std::string& filename)
{
    FILE* file = fopen(filename.c_str(), "rb");
    if (!file)
    {
        return "";
    }

    SHA512_CTX ctx;
    SHA512_Init(&ctx);

    unsigned char buffer[8192];
    size_t bytes_read;

    while ((bytes_read = fread(buffer, 1, sizeof(buffer), file)) > 0)
    {
        SHA512_Update(&ctx, buffer, bytes_read);
    }

    unsigned char hash[SHA512_DIGEST_LENGTH];
    SHA512_Final(hash, &ctx);

    fclose(file);

    // Convert to hex string
    std::string result;
    result.reserve(SHA512_DIGEST_LENGTH * 2);
    for (size_t i = 0; i < SHA512_DIGEST_LENGTH; i++)
    {
        char hex[3];
        sprintf(hex, "%02x", hash[i]);
        result += hex;
    }

    return result;
}

// Helper functions for version field manipulation from rippled code
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

// Function to get node type description
std::string
getNodeTypeDescription(uint8_t type)
{
    switch (type)
    {
        case 1:
            return "tnINNER";
        case 2:
            return "tnTRANSACTION_NM";
        case 3:
            return "tnTRANSACTION_MD";
        case 4:
            return "tnACCOUNT_STATE";
        case 254:
            return "tnREMOVE";
        case 255:
            return "tnTERMINAL";
        default:
            return "UNKNOWN_TYPE_" + std::to_string(type);
    }
}

// Convert NetClock epoch time to human-readable string
std::string
timeToString(uint64_t net_clock_time)
{
    // NetClock uses seconds since January 1st, 2000 (946684800)
    static const time_t ripple_epoch_offset = 946684800;

    time_t unix_time = net_clock_time + ripple_epoch_offset;
    std::tm* tm = std::gmtime(&unix_time);
    if (!tm)
        return "Invalid time";

    char timeStr[30];
    std::strftime(timeStr, sizeof(timeStr), "%Y-%m-%d %H:%M:%S UTC", tm);
    return timeStr;
}

// Convert bytes to hex string
std::string
bytesToHexString(const uint8_t* data, size_t length)
{
    std::stringstream ss;
    for (size_t i = 0; i < length; ++i)
    {
        ss << std::setw(2) << std::setfill('0') << std::hex
           << static_cast<int>(data[i]);
    }
    return ss.str();
}

// Hex dump utility function - prints bytes with file offsets and annotations
void
hexDump(
    std::ostream& os,
    const std::vector<uint8_t>& data,
    size_t offset,
    const std::string& annotation = "",
    size_t bytes_per_line = 16)
{
    for (size_t i = 0; i < data.size(); i += bytes_per_line)
    {
        // Print offset
        os << std::setfill('0') << std::setw(8) << std::hex << (offset + i)
           << ": ";

        // Print hex values
        for (size_t j = 0; j < bytes_per_line; j++)
        {
            if (i + j < data.size())
            {
                os << std::setfill('0') << std::setw(2) << std::hex
                   << static_cast<int>(data[i + j]) << " ";
            }
            else
            {
                os << "   ";  // 3 spaces for missing bytes
            }

            // Add extra space after 8 bytes for readability
            if (j == 7)
            {
                os << " ";
            }
        }

        // Print ASCII representation
        os << " | ";
        for (size_t j = 0; j < bytes_per_line && (i + j) < data.size(); j++)
        {
            char c = data[i + j];
            os << (std::isprint(c) ? c : '.');
        }

        // Add annotation on first line only
        if (i == 0 && !annotation.empty())
        {
            size_t extra_spaces =
                bytes_per_line - std::min(bytes_per_line, data.size());
            os << std::string(extra_spaces, ' ');
            os << " | " << annotation;
        }

        os << std::dec << std::endl;
    }
}

class CatalogueAnalyzer
{
private:
    std::string filename_;
    std::ifstream file_;
    std::ostream& output_;
    size_t file_size_;
    bool verbose_;
    bool verify_hash_;
    uint8_t compression_level_ = 0;
    CATLHeader header_;  // Store the header for access throughout the class

    // Result tracking
    bool hash_verified_ = false;
    bool file_size_matched_ = true;
    std::vector<uint32_t> processedLedgers_;  // Track which ledgers we've seen

    // Read a block of data from the file
    std::vector<uint8_t>
    readBytes(size_t offset, size_t size)
    {
        std::vector<uint8_t> buffer(size);

        // Reading directly from file
        file_.seekg(offset, std::ios::beg);
        file_.read(reinterpret_cast<char*>(buffer.data()), size);
        size_t bytes_read = file_.gcount();
        buffer.resize(bytes_read);  // Adjust for actual bytes read

        return buffer;
    }

    // Analyze and dump header information
    size_t
    analyzeHeader(size_t offset)
    {
        output_ << "=== CATALOGUE HEADER ===\n";
        auto header_bytes = readBytes(offset, sizeof(CATLHeader));

        if (header_bytes.size() < sizeof(CATLHeader))
        {
            output_ << "ERROR: Incomplete header. File is truncated.\n";
            return offset + header_bytes.size();
        }

        // Read the header values
        std::memcpy(&header_, header_bytes.data(), sizeof(CATLHeader));

        // Dump the entire header with annotations if verbose
        if (verbose_)
        {
            hexDump(output_, header_bytes, offset, "CATL Header");
        }

        // Extract version and compression info
        uint8_t catalogue_version = getCatalogueVersion(header_.version);
        compression_level_ = getCompressionLevel(header_.version);

        // Validate header fields
        bool valid = true;
        if (header_.magic != CATL)
        {
            output_ << "WARNING: Invalid magic value, expected 0x" << std::hex
                    << CATL << std::dec << "\n";
            valid = false;
        }

        if (catalogue_version > BASE_CATALOGUE_VERSION)
        {
            output_ << "WARNING: Unexpected version. Expected "
                    << BASE_CATALOGUE_VERSION << ", got "
                    << static_cast<int>(catalogue_version) << "\n";
            valid = false;
        }

        if (header_.min_ledger > header_.max_ledger)
        {
            output_ << "WARNING: Invalid ledger range: min_ledger ("
                    << header_.min_ledger << ") > max_ledger ("
                    << header_.max_ledger << ")\n";
            valid = false;
        }

        // Convert hash to hex string
        std::string hash_hex =
            bytesToHexString(header_.hash.data(), header_.hash.size());

        // Summary
        output_ << "Header Summary:\n"
                << "  Magic: 0x" << std::hex << header_.magic << std::dec
                << (header_.magic == CATL ? " (valid)" : " (INVALID)") << "\n"
                << "  Min Ledger: " << header_.min_ledger << "\n"
                << "  Max Ledger: " << header_.max_ledger << "\n"
                << "  Version: " << static_cast<int>(catalogue_version) << "\n"
                << "  Compression Level: "
                << static_cast<int>(compression_level_) << "\n"
                << "  Network ID: " << header_.network_id << "\n"
                << "  File Size: " << header_.filesize << " bytes\n"
                << "  Hash: " << hash_hex << "\n\n";

        // Verify hash if requested
        if (verify_hash_)
        {
            verifyFileHash(header_);
        }

        return offset + sizeof(CATLHeader);
    }

    // Verify file hash
    void
    verifyFileHash(const CATLHeader& header)
    {
        output_ << "=== HASH VERIFICATION ===\n";

        // If hash is all zeros, it's not set
        bool hash_is_zero = true;
        for (auto b : header.hash)
        {
            if (b != 0)
            {
                hash_is_zero = false;
                break;
            }
        }

        if (hash_is_zero)
        {
            output_
                << "Hash verification skipped: Hash is empty (all zeros)\n\n";
            return;
        }

        // Check file size
        if (file_size_ != header.filesize)
        {
            output_ << "ERROR: File size mismatch. Header indicates "
                    << header.filesize << " bytes, but actual file size is "
                    << file_size_ << " bytes\n\n";
            file_size_matched_ = false;
            return;
        }

        output_ << "Computing SHA-512 hash for verification...\n";

        // Create a temp file with zeroed hash field
        std::string temp_file = filename_ + ".temp";
        std::ofstream out_file(temp_file, std::ios::binary);
        if (!out_file)
        {
            output_ << "ERROR: Could not create temporary file for hash "
                       "verification\n";
            return;
        }

        // Copy the original header with zeroed hash field
        CATLHeader header_copy = header;
        std::fill(header_copy.hash.begin(), header_copy.hash.end(), 0);
        out_file.write(
            reinterpret_cast<const char*>(&header_copy), sizeof(CATLHeader));

        // Copy the rest of the file
        file_.clear();
        file_.seekg(sizeof(CATLHeader), std::ios::beg);

        char buffer[64 * 1024];  // 64K buffer
        while (file_)
        {
            file_.read(buffer, sizeof(buffer));
            std::streamsize bytes_read = file_.gcount();
            if (bytes_read > 0)
            {
                out_file.write(buffer, bytes_read);
            }
            else
            {
                break;
            }
        }
        out_file.close();

        // Compute hash using sha512sum command
        std::string computed_hash_hex = computeSHA512(temp_file);
        std::string stored_hash_hex =
            bytesToHexString(header.hash.data(), header.hash.size());

        output_ << "Stored hash:   " << stored_hash_hex << "\n";
        output_ << "Computed hash: " << computed_hash_hex << "\n";

        // Compare hashes (case insensitive)
        std::string lower_computed = computed_hash_hex;
        std::string lower_stored = stored_hash_hex;
        std::transform(
            lower_computed.begin(),
            lower_computed.end(),
            lower_computed.begin(),
            ::tolower);
        std::transform(
            lower_stored.begin(),
            lower_stored.end(),
            lower_stored.begin(),
            ::tolower);

        hash_verified_ = (lower_computed == lower_stored);

        if (hash_verified_)
        {
            output_ << "VERIFICATION RESULT: Hash verification successful!\n\n";
        }
        else
        {
            output_ << "VERIFICATION RESULT: Hash verification FAILED!\n";
            output_ << "  The file may be corrupted or modified.\n\n";
        }

        // Clean up
        std::remove(temp_file.c_str());
    }

    // Process ledger info from a stream (works for any compression level)
    void
    processStreamedLedgerInfo(std::istream& stream, uint32_t sequence)
    {
        output_ << "=== LEDGER INFO ===\n";

        // We already read the sequence, so just display it
        std::vector<uint8_t> seqBytes(4);
        std::memcpy(seqBytes.data(), &sequence, 4);
        if (verbose_)
        {
            hexDump(
                output_,
                seqBytes,
                0,
                "Ledger Sequence: " + std::to_string(sequence));
        }
        else
        {
            output_ << "Ledger Sequence: " << sequence << "\n";
        }

        // Helper function to read and display data from stream
        auto read_and_dump =
            [&](size_t size, const std::string& label) -> std::vector<uint8_t> {
            std::vector<uint8_t> buffer(size);
            stream.read(reinterpret_cast<char*>(buffer.data()), size);

            if (stream.gcount() < static_cast<std::streamsize>(size))
            {
                output_ << "ERROR: Unexpected EOF reading " << label << "\n";
                buffer.resize(stream.gcount());
                return buffer;
            }

            if (!label.empty() && verbose_)
            {
                hexDump(output_, buffer, 0, label);
            }
            else if (!label.empty())
            {
                // Just print the label without hex dump
                output_ << label << "\n";
            }
            return buffer;
        };

        // Read all fields sequentially
        auto hash_bytes = read_and_dump(32, "");
        if (hash_bytes.size() < 32)
            return;
        std::string hash_hex = bytesToHexString(hash_bytes.data(), 32);
        hexDump(output_, hash_bytes, 0, "Hash: " + hash_hex);

        auto tx_hash_bytes = read_and_dump(32, "");
        if (tx_hash_bytes.size() < 32)
            return;
        std::string tx_hash_hex = bytesToHexString(tx_hash_bytes.data(), 32);
        hexDump(output_, tx_hash_bytes, 0, "Tx Hash: " + tx_hash_hex);

        auto account_hash_bytes = read_and_dump(32, "");
        if (account_hash_bytes.size() < 32)
            return;
        std::string account_hash_hex =
            bytesToHexString(account_hash_bytes.data(), 32);
        hexDump(
            output_,
            account_hash_bytes,
            0,
            "Account Hash: " + account_hash_hex);

        auto parent_hash_bytes = read_and_dump(32, "");
        if (parent_hash_bytes.size() < 32)
            return;
        std::string parent_hash_hex =
            bytesToHexString(parent_hash_bytes.data(), 32);
        hexDump(
            output_, parent_hash_bytes, 0, "Parent Hash: " + parent_hash_hex);

        // Read drops (8 bytes)
        auto drops_bytes = read_and_dump(8, "");
        if (drops_bytes.size() < 8)
            return;
        uint64_t drops = 0;
        std::memcpy(&drops, drops_bytes.data(), 8);
        hexDump(output_, drops_bytes, 0, "Drops: " + std::to_string(drops));

        // Read close_flags (4 bytes)
        auto close_flags_bytes = read_and_dump(4, "");
        if (close_flags_bytes.size() < 4)
            return;
        int32_t close_flags = 0;
        std::memcpy(&close_flags, close_flags_bytes.data(), 4);
        hexDump(
            output_,
            close_flags_bytes,
            0,
            "Close Flags: " + std::to_string(close_flags));

        // Read close_time_resolution (4 bytes)
        auto ctr_bytes = read_and_dump(4, "");
        if (ctr_bytes.size() < 4)
            return;
        uint32_t close_time_resolution = 0;
        std::memcpy(&close_time_resolution, ctr_bytes.data(), 4);
        hexDump(
            output_,
            ctr_bytes,
            0,
            "Close Time Resolution: " + std::to_string(close_time_resolution));

        // Read close_time (8 bytes)
        auto ct_bytes = read_and_dump(8, "");
        if (ct_bytes.size() < 8)
            return;
        uint64_t clock_time = 0;
        std::memcpy(&clock_time, ct_bytes.data(), 8);
        std::string close_time_str = timeToString(clock_time);
        hexDump(
            output_,
            ct_bytes,
            0,
            "Close Time: " + std::to_string(clock_time) + " (" +
                close_time_str + ")");

        // Read parent_close_time (8 bytes)
        auto pct_bytes = read_and_dump(8, "");
        if (pct_bytes.size() < 8)
            return;
        uint64_t parent_close_time = 0;
        std::memcpy(&parent_close_time, pct_bytes.data(), 8);
        std::string time_str = timeToString(parent_close_time);
        hexDump(
            output_,
            pct_bytes,
            0,
            "Parent Close Time: " + std::to_string(parent_close_time) + " (" +
                time_str + ")");

        output_ << "Ledger " << sequence << " Info - Total bytes read: "
                << (4 + 32 + 32 + 32 + 32 + 8 + 4 + 4 + 8 + 8) << "\n\n";
    }

    // Process SHAMap from a stream (works for any compression level)
    void
    analyzeStreamSHAMap(
        std::istream& stream,
        const std::string& mapType,
        uint32_t ledgerSeq,
        bool is_delta = false)
    {
        output_ << "=== " << mapType << " for Ledger " << ledgerSeq << " ===\n";
        if (is_delta)
        {
            output_
                << "Note: This is a DELTA map (changes from previous ledger)\n";
        }

        size_t node_count = 0;
        bool found_terminal = false;

        // For non-verbose mode, we'll just keep track of the counts of each
        // node type
        std::map<uint8_t, size_t> nodeTypeCounts;

        while (!stream.eof())
        {
            // Check for terminal marker
            uint8_t node_type = 0;
            stream.read(reinterpret_cast<char*>(&node_type), 1);

            if (stream.gcount() < 1 || stream.fail())
            {
                output_ << "ERROR: Unexpected EOF reading node type\n";
                return;
            }

            std::vector<uint8_t> nodeTypeBytes = {node_type};

            if (node_type == static_cast<uint8_t>(SHAMapNodeType::tnTERMINAL))
            {
                if (verbose_)
                {
                    hexDump(
                        output_,
                        nodeTypeBytes,
                        0,
                        "Terminal Marker - End of " + mapType);
                }
                output_ << "Found terminal marker. " << mapType
                        << " complete with " << node_count << " nodes.\n\n";
                found_terminal = true;
                return;
            }

            // Not a terminal marker, parse as a node
            node_count++;
            nodeTypeCounts[node_type]++;

            // In verbose mode, display detailed node info
            if (verbose_)
            {
                output_ << "--- Node " << node_count << " ---\n";

                // Node type
                hexDump(
                    output_,
                    nodeTypeBytes,
                    0,
                    "Node Type: " + getNodeTypeDescription(node_type));
            }

            // Key (32 bytes)
            std::vector<uint8_t> keyBytes(32);
            stream.read(reinterpret_cast<char*>(keyBytes.data()), 32);

            if (stream.gcount() < 32 || stream.fail())
            {
                output_ << "ERROR: Unexpected EOF reading node key\n";
                return;
            }

            std::string key_hex =
                bytesToHexString(keyBytes.data(), keyBytes.size());
            if (verbose_)
            {
                hexDump(output_, keyBytes, 0, "Key: " + key_hex);
            }

            if (node_type == SHAMapNodeType::tnREMOVE)
            {
                if (verbose_)
                {
                    output_ << "  (This is a deletion marker)\n";
                }
                continue;
            }

            // Data size (4 bytes)
            uint32_t data_size = 0;
            stream.read(reinterpret_cast<char*>(&data_size), 4);

            if (stream.gcount() < 4 || stream.fail())
            {
                output_ << "ERROR: Unexpected EOF reading data size\n";
                return;
            }

            std::vector<uint8_t> dataSizeBytes(4);
            std::memcpy(dataSizeBytes.data(), &data_size, 4);

            // Suspiciously large value check
            std::string size_note = "Data Size: " + std::to_string(data_size);
            if (data_size > 10 * 1024 * 1024)
            {
                size_note += " (SUSPICIOUS!)";
            }

            if (verbose_)
            {
                hexDump(output_, dataSizeBytes, 0, size_note);
            }

            if (data_size == 0)
            {
                if (verbose_)
                {
                    output_ << "  (This is a error = zero sized object)\n";
                }
            }
            else if (data_size > 10 * 1024 * 1024)
            {
                output_ << "WARNING: Data size is suspiciously large!\n";
                output_ << "  Possible file corruption detected.\n";
                output_ << "  Skipping to next ledger...\n";
                return;
            }
            else
            {
                // Show a preview of the data (up to 64 bytes) in verbose mode
                // only
                size_t preview_size = std::min(
                    static_cast<size_t>(data_size), static_cast<size_t>(64));
                std::vector<uint8_t> dataPreview(preview_size);

                stream.read(
                    reinterpret_cast<char*>(dataPreview.data()), preview_size);

                if (stream.gcount() <
                        static_cast<std::streamsize>(preview_size) ||
                    stream.fail())
                {
                    output_ << "ERROR: Unexpected EOF reading data preview\n";
                    return;
                }

                if (verbose_)
                {
                    hexDump(
                        output_,
                        dataPreview,
                        0,
                        "Data Preview (" + std::to_string(preview_size) +
                            " bytes of " + std::to_string(data_size) +
                            " total)");
                }

                // Skip remaining data
                if (data_size > preview_size)
                {
                    // Need to consume the remaining bytes from the stream
                    size_t remaining_bytes = data_size - preview_size;
                    std::vector<char> dummyBuffer(
                        std::min(remaining_bytes, static_cast<size_t>(4096)));

                    while (remaining_bytes > 0)
                    {
                        size_t bytes_to_read =
                            std::min(remaining_bytes, dummyBuffer.size());
                        stream.read(dummyBuffer.data(), bytes_to_read);
                        size_t bytes_read = stream.gcount();

                        if (bytes_read == 0)
                            break;  // EOF or error

                        remaining_bytes -= bytes_read;
                    }

                    if (remaining_bytes > 0)
                    {
                        output_ << "WARNING: Could not consume all remaining "
                                   "data bytes\n";
                    }
                }
            }

            if (verbose_)
            {
                output_ << "  Node " << node_count << " Complete\n";
            }
        }

        if (!found_terminal)
        {
            output_ << "WARNING: No terminal marker found for " << mapType
                    << "\n";
        }

        // Non-verbose summary - only show node type counts if not in verbose
        // mode
        if (!verbose_)
        {
            output_ << "Processed " << node_count << " nodes in " << mapType
                    << ".\n";

            // Show counts of each node type
            output_ << "Node type breakdown:\n";
            for (auto it = nodeTypeCounts.begin(); it != nodeTypeCounts.end();
                 ++it)
            {
                uint8_t type = it->first;
                size_t count = it->second;
                if (type != SHAMapNodeType::tnTERMINAL)
                {  // Don't include terminal marker in counts
                    output_ << "  " << getNodeTypeDescription(type) << ": "
                            << count << " nodes\n";
                }
            }
            output_ << "\n";
        }
    }

public:
    CatalogueAnalyzer(
        const std::string& filename,
        std::ostream& output,
        bool verbose = false,
        bool verify_hash = true)
        : filename_(filename)
        , output_(output)
        , verbose_(verbose)
        , verify_hash_(verify_hash)
    {
        file_.open(filename, std::ios::binary);
        if (!file_.is_open())
        {
            throw std::runtime_error("Failed to open file: " + filename);
        }

        // Get file size
        file_.seekg(0, std::ios::end);
        file_size_ = file_.tellg();
        file_.seekg(0, std::ios::beg);

        output_ << "Analyzing file: " << filename << "\n";
        output_ << "File size: " << file_size_ << " bytes\n\n";
    }

    ~CatalogueAnalyzer()
    {
        if (file_.is_open())
        {
            file_.close();
        }
    }

    void
    analyze()
    {
        try
        {
            size_t offset = 0;

            // Analyze header
            offset = analyzeHeader(offset);
            if (offset >= file_size_)
                return;

            // Set up a stream for reading based on compression level
            std::unique_ptr<boost::iostreams::filtering_istream> dataStream =
                std::make_unique<boost::iostreams::filtering_istream>();

            // Position file after header
            file_.clear();
            file_.seekg(sizeof(CATLHeader), std::ios::beg);

            try
            {
                if (compression_level_ > 0)
                {
                    output_ << "Processing catalogue with compression level "
                            << static_cast<int>(compression_level_) << "\n\n";

                    // Configure decompressor with options to handle potential
                    // issues
                    boost::iostreams::zlib_params params;
                    params.window_bits = 15;  // Maximum window bits
                    params.noheader = false;  // Expect zlib header

                    dataStream->push(
                        boost::iostreams::zlib_decompressor(params));
                    dataStream->push(boost::ref(file_));

                    // Test read to verify decompression is working
                    char test_byte;
                    if (!dataStream->get(test_byte))
                    {
                        output_ << "WARNING: Failed to read initial compressed "
                                   "data. The file may be corrupted or use a "
                                   "different compression format.\n";

                        // Try alternative decompression approach
                        dataStream = std::make_unique<
                            boost::iostreams::filtering_istream>();
                        file_.clear();
                        file_.seekg(sizeof(CATLHeader), std::ios::beg);

                        // Try with raw deflate (no header)
                        params.noheader = true;
                        dataStream->push(
                            boost::iostreams::zlib_decompressor(params));
                        dataStream->push(boost::ref(file_));

                        output_
                            << "Trying alternative decompression method...\n";
                    }
                    else
                    {
                        // Put back the test byte
                        dataStream->unget();
                    }
                }
                else
                {
                    output_ << "Processing catalogue with no compression "
                               "(level 0)\n\n";
                    // No decompressor needed, but still use streaming API for
                    // consistency
                    dataStream->push(boost::ref(file_));
                }
            }
            catch (const std::exception& e)
            {
                output_ << "ERROR setting up decompression: " << e.what()
                        << "\n";
                return;
            }

            // Process each ledger
            int ledger_count = 0;
            uint32_t last_ledger_seq = 0;

            while (!dataStream->eof())
            {
                try
                {
                    // Read ledger sequence first to identify
                    uint32_t ledger_seq = 0;

                    // Read the sequence once and don't attempt to seek back
                    if (!dataStream->read(
                            reinterpret_cast<char*>(&ledger_seq), 4) ||
                        dataStream->gcount() < 4)
                    {
                        // EOF or error
                        if (dataStream->bad())
                        {
                            output_ << "ERROR: Stream error occurred while "
                                       "reading ledger sequence\n";
                        }
                        else if (dataStream->eof())
                        {
                            output_ << "End of stream reached\n";
                        }
                        else
                        {
                            output_
                                << "ERROR: Failed to read ledger sequence\n";
                        }
                        break;
                    }

                    // Sanity check the ledger sequence number
                    if (ledger_seq < header_.min_ledger ||
                        ledger_seq > header_.max_ledger)
                    {
                        output_ << "WARNING: Suspicious ledger sequence "
                                << ledger_seq << " outside expected range ("
                                << header_.min_ledger << "-"
                                << header_.max_ledger << ")\n";
                        // Continue anyway, might be corrupt data or a format
                        // issue
                    }

                    output_ << "Processing Ledger " << ledger_seq << "\n";

                    // Add to our record of processed ledgers
                    processedLedgers_.push_back(ledger_seq);

                    // Process ledger info - pass the already read sequence
                    processStreamedLedgerInfo(*dataStream, ledger_seq);

                    // Analyze state map - if not the first ledger, it's a delta
                    // from previous
                    bool is_state_delta = (ledger_count > 0);
                    output_ << "Analyzing STATE MAP"
                            << (is_state_delta ? " (DELTA)" : "") << "...\n";
                    analyzeStreamSHAMap(
                        *dataStream, "STATE MAP", ledger_seq, is_state_delta);

                    // Analyze transaction map
                    output_ << "Analyzing TRANSACTION MAP...\n";
                    analyzeStreamSHAMap(
                        *dataStream, "TRANSACTION MAP", ledger_seq);

                    ledger_count++;
                    last_ledger_seq = ledger_seq;

                    if (verbose_)
                    {
                        output_ << "Ledger " << ledger_seq
                                << " processing complete.\n";
                        output_ << "-------------------------------------------"
                                   "---\n\n";
                    }

                    // If we've hit any stream errors, break the loop
                    if (dataStream->bad() || dataStream->fail())
                    {
                        output_ << "Stream error detected, stopping ledger "
                                   "processing\n";
                        break;
                    }

                    // Add a maximum ledger count check to avoid infinite loops
                    if (ledger_count >=
                        (header_.max_ledger - header_.min_ledger + 10))
                    {
                        output_ << "WARNING: Processed more ledgers than "
                                   "expected in range, stopping\n";
                        break;
                    }
                }
                catch (const std::exception& e)
                {
                    output_ << "ERROR while processing ledger: " << e.what()
                            << "\n";
                    output_ << "Attempting to continue with next ledger...\n";
                    // Try to recover by skipping ahead
                    dataStream->clear();
                }
                catch (...)
                {
                    output_ << "UNKNOWN ERROR while processing ledger\n";
                    break;
                }
            }

            if (ledger_count == 0)
            {
                output_
                    << "WARNING: No ledgers were processed. The file may use a "
                       "different compression format or be corrupted.\n";
                output_ << "Try running with --skip-hash-verification to "
                           "analyze format details.\n";
            }
            else
            {
                output_ << "Analysis complete. Processed " << ledger_count
                        << " ledgers.\n";
                output_ << "Last ledger processed: " << last_ledger_seq << "\n";

                // Add summary of expected vs actual ledger count
                int expected_ledgers =
                    header_.max_ledger - header_.min_ledger + 1;
                bool all_ledgers_found = (ledger_count == expected_ledgers);

                if (ledger_count < expected_ledgers)
                {
                    output_ << "NOTE: Expected " << expected_ledgers
                            << " ledgers based on header, but processed "
                            << ledger_count << " ledgers.\n";
                }

                // Check if ledgers are in sequence
                bool ledgers_in_sequence = true;
                std::vector<uint32_t> missingLedgers;

                if (!processedLedgers_.empty())
                {
                    std::sort(
                        processedLedgers_.begin(), processedLedgers_.end());

                    // Check first and last match expected range
                    uint32_t first_ledger = processedLedgers_.front();
                    uint32_t last_ledger = processedLedgers_.back();

                    if (first_ledger != header_.min_ledger ||
                        last_ledger != header_.max_ledger)
                    {
                        ledgers_in_sequence = false;
                        output_ << "NOTE: Ledger range in file ("
                                << first_ledger << "-" << last_ledger
                                << ") doesn't match expected range ("
                                << header_.min_ledger << "-"
                                << header_.max_ledger << ")\n";
                    }

                    // Check for gaps in sequence
                    for (size_t i = 1; i < processedLedgers_.size(); i++)
                    {
                        if (processedLedgers_[i] !=
                            processedLedgers_[i - 1] + 1)
                        {
                            ledgers_in_sequence = false;
                            // Identify missing ledgers
                            for (uint32_t missing =
                                     processedLedgers_[i - 1] + 1;
                                 missing < processedLedgers_[i];
                                 missing++)
                            {
                                missingLedgers.push_back(missing);
                            }
                        }
                    }

                    if (!missingLedgers.empty())
                    {
                        output_ << "WARNING: Found gaps in ledger sequence. "
                                   "Missing ledgers: ";
                        if (missingLedgers.size() <= 10)
                        {
                            // List all missing ledgers if there are few
                            for (size_t i = 0; i < missingLedgers.size(); i++)
                            {
                                output_ << missingLedgers[i];
                                if (i < missingLedgers.size() - 1)
                                    output_ << ", ";
                            }
                        }
                        else
                        {
                            // Just show the count if there are many
                            output_ << missingLedgers.size()
                                    << " ledgers missing";
                        }
                        output_ << "\n";
                    }
                }

                // Print final integrity summary
                output_ << "\n=== INTEGRITY SUMMARY ===\n";
                output_ << "File size check: "
                        << (file_size_matched_ ? "PASSED" : "FAILED") << "\n";
                output_ << "SHA-512 hash check: "
                        << (hash_verified_ ? "PASSED" : "FAILED") << "\n";
                output_ << "Ledger count check: "
                        << (all_ledgers_found ? "PASSED" : "FAILED") << "\n";
                output_ << "Ledger sequence check: "
                        << (ledgers_in_sequence ? "PASSED" : "FAILED") << "\n";

                if (file_size_matched_ && hash_verified_ && all_ledgers_found &&
                    ledgers_in_sequence)
                {
                    output_ << "\nOVERALL RESULT: PASSED - All integrity "
                               "checks successful\n";
                    output_ << "The catalogue file contains all expected "
                               "ledgers in sequence with a valid hash.\n";
                }
                else
                {
                    output_ << "\nOVERALL RESULT: FAILED - One or more "
                               "integrity checks failed\n";
                    if (!file_size_matched_)
                    {
                        output_ << "- The file size doesn't match the value in "
                                   "the header\n";
                    }
                    if (!hash_verified_)
                    {
                        output_ << "- The file hash doesn't match the stored "
                                   "hash\n";
                    }
                    if (!all_ledgers_found)
                    {
                        output_ << "- Not all expected ledgers were found in "
                                   "the file\n";
                    }
                    if (!ledgers_in_sequence)
                    {
                        output_ << "- The ledgers are not in proper sequence\n";
                    }
                }
            }
        }
        catch (const std::exception& e)
        {
            output_ << "ERROR during analysis: " << e.what() << "\n";
        }
    }
};

int
main(int argc, char* argv[])
{
    if (argc < 2)
    {
        std::cerr << "Usage: " << argv[0]
                  << " <catalogue_file> [output_file] "
                     "[--skip-hash-verification] [--verbose]\n";
        std::cerr << "\nThis tool analyzes CATL files from the XRP Ledger.\n";
        std::cerr << "It supports both compressed and uncompressed catalogue "
                     "files.\n";
        std::cerr << "\nOptions:\n";
        std::cerr << "  --verbose                 Show detailed hex dumps and "
                     "additional debug information\n";
        std::cerr << "  --skip-hash-verification  Skip verifying the SHA-512 "
                     "hash of the file\n";
        return 1;
    }

    std::string input_file = argv[1];
    std::ofstream output_file;
    std::ostream* output = &std::cout;
    bool verbose = false;
    bool verify_hash = true;  // Default to verifying hash

    // Check for flags
    for (int i = 2; i < argc; i++)
    {
        if (std::string(argv[i]) == "--verbose")
        {
            verbose = true;
        }
        else if (std::string(argv[i]) == "--skip-hash-verification")
        {
            verify_hash = false;
        }
        else if (i == 2)
        {
            // Assume it's the output file name
            output_file.open(argv[2]);
            if (!output_file.is_open())
            {
                std::cerr << "Failed to open output file: " << argv[2] << "\n";
                return 1;
            }
            output = &output_file;
        }
    }

    try
    {
        // Print banner
        *output << "==========================================================="
                   "========\n";
        *output << "XRPL Catalogue File Analyzer v2.0\n";
        *output
            << "Supports compressed (zlib) and uncompressed catalogue files\n";
        if (verify_hash)
        {
            *output << "SHA-512 hash verification enabled (default)\n";
        }
        else
        {
            *output << "SHA-512 hash verification disabled\n";
        }
        *output << "==========================================================="
                   "========\n\n";

        CatalogueAnalyzer analyzer(input_file, *output, verbose, verify_hash);
        analyzer.analyze();
    }
    catch (const std::exception& e)
    {
        *output << "ERROR: " << e.what() << "\n";
        return 1;
    }

    if (output_file.is_open())
    {
        output_file.close();
    }

    return 0;
}
