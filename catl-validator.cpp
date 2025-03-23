#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <iomanip>
#include <cstring>
#include <cctype>
#include <algorithm>
#include <ctime>
#include <memory>
#include <stdexcept>

// For decompression
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/zlib.hpp>
#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/copy.hpp>

// Constants from the rippled code
static constexpr uint32_t CATL = 0x4C544143UL; // "CATL" in LE
static constexpr uint16_t CATALOGUE_VERSION_MASK = 0x00FF;
static constexpr uint16_t CATALOGUE_COMPRESS_LEVEL_MASK = 0x0F00;
static constexpr uint16_t CATALOGUE_RESERVED_MASK = 0xF000;
static constexpr uint16_t BASE_CATALOGUE_VERSION = 1;

// The header structure from the rippled code
#pragma pack(push, 1)  // pack the struct tightly
struct CATLHeader {
    uint32_t magic = 0x4C544143UL;  // "CATL" in LE
    uint32_t min_ledger;
    uint32_t max_ledger;
    uint16_t version;
    uint16_t network_id;
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

// Helper functions for version field manipulation from rippled code
inline uint8_t getCatalogueVersion(uint16_t versionField)
{
    return versionField & CATALOGUE_VERSION_MASK;
}

inline uint8_t getCompressionLevel(uint16_t versionField)
{
    return (versionField & CATALOGUE_COMPRESS_LEVEL_MASK) >> 8;
}

// Function to get node type description
std::string getNodeTypeDescription(uint8_t type) {
    switch (type) {
        case 1: return "tnINNER";
        case 2: return "tnTRANSACTION_NM";
        case 3: return "tnTRANSACTION_MD";
        case 4: return "tnACCOUNT_STATE";
        case 254: return "tnREMOVE";
        case 255: return "tnTERMINAL";
        default: return "UNKNOWN_TYPE_" + std::to_string(type);
    }
}

// Convert NetClock epoch time to human-readable string
std::string timeToString(uint64_t netClockTime) {
    // NetClock uses seconds since January 1st, 2000 (946684800)
    static const time_t rippleEpochOffset = 946684800;
    
    time_t unixTime = netClockTime + rippleEpochOffset;
    std::tm* tm = std::gmtime(&unixTime);
    if (!tm) return "Invalid time";
    
    char timeStr[30];
    std::strftime(timeStr, sizeof(timeStr), "%Y-%m-%d %H:%M:%S UTC", tm);
    return timeStr;
}

// Hex dump utility function - prints bytes with file offsets and annotations
void hexDump(std::ostream& os, const std::vector<uint8_t>& data, size_t offset, 
             const std::string& annotation = "", size_t bytesPerLine = 16) {
    
    for (size_t i = 0; i < data.size(); i += bytesPerLine) {
        // Print offset
        os << std::setfill('0') << std::setw(8) << std::hex << (offset + i) << ": ";
        
        // Print hex values
        for (size_t j = 0; j < bytesPerLine; j++) {
            if (i + j < data.size()) {
                os << std::setfill('0') << std::setw(2) << std::hex << static_cast<int>(data[i + j]) << " ";
            } else {
                os << "   "; // 3 spaces for missing bytes
            }
            
            // Add extra space after 8 bytes for readability
            if (j == 7) {
                os << " ";
            }
        }
        
        // Print ASCII representation
        os << " | ";
        for (size_t j = 0; j < bytesPerLine && (i + j) < data.size(); j++) {
            char c = data[i + j];
            os << (std::isprint(c) ? c : '.');
        }
        
        // Add annotation on first line only
        if (i == 0 && !annotation.empty()) {
            size_t extraSpaces = bytesPerLine - std::min(bytesPerLine, data.size());
            os << std::string(extraSpaces, ' ');
            os << " | " << annotation;
        }
        
        os << std::dec << std::endl;
    }
}

// Convert bytes to hex string
std::string bytesToHexString(const uint8_t* data, size_t length) {
    std::stringstream ss;
    for (size_t i = 0; i < length; ++i) {
        ss << std::setw(2) << std::setfill('0') << std::hex << static_cast<int>(data[i]);
    }
    return ss.str();
}

class CatalogueAnalyzer {
private:
    std::string filename_;
    std::ifstream file_;
    std::ostream& output_;
    size_t fileSize_;
    bool verbose_;
    uint8_t compressionLevel_ = 0;
    
    // Read a block of data from the file
    std::vector<uint8_t> readBytes(size_t offset, size_t size) {
        std::vector<uint8_t> buffer(size);
        
        // Reading directly from file
        file_.seekg(offset, std::ios::beg);
        file_.read(reinterpret_cast<char*>(buffer.data()), size);
        size_t bytesRead = file_.gcount();
        buffer.resize(bytesRead); // Adjust for actual bytes read
        
        return buffer;
    }
    
    // Analyze and dump header information
    size_t analyzeHeader(size_t offset) {
        output_ << "=== CATALOGUE HEADER ===\n";
        auto headerBytes = readBytes(offset, sizeof(CATLHeader));
        
        if (headerBytes.size() < sizeof(CATLHeader)) {
            output_ << "ERROR: Incomplete header. File is truncated.\n";
            return offset + headerBytes.size();
        }
        
        // Read the header values
        CATLHeader header;
        std::memcpy(&header, headerBytes.data(), sizeof(CATLHeader));
        
        // Dump the entire header with annotations
        hexDump(output_, headerBytes, offset, "CATL Header");
        
        // Extract version and compression info
        uint8_t catalogueVersion = getCatalogueVersion(header.version);
        compressionLevel_ = getCompressionLevel(header.version);
        
        // Validate header fields
        bool valid = true;
        if (header.magic != CATL) {
            output_ << "WARNING: Invalid magic value, expected 0x" << std::hex << CATL << std::dec << "\n";
            valid = false;
        }
        
        if (catalogueVersion > BASE_CATALOGUE_VERSION) {
            output_ << "WARNING: Unexpected version. Expected " << BASE_CATALOGUE_VERSION 
                    << ", got " << static_cast<int>(catalogueVersion) << "\n";
            valid = false;
        }
        
        if (header.min_ledger > header.max_ledger) {
            output_ << "WARNING: Invalid ledger range: min_ledger (" << header.min_ledger 
                    << ") > max_ledger (" << header.max_ledger << ")\n";
            valid = false;
        }
        
        // Summary
        output_ << "Header Summary:\n"
                << "  Magic: 0x" << std::hex << header.magic << std::dec 
                << (header.magic == CATL ? " (valid)" : " (INVALID)") << "\n"
                << "  Min Ledger: " << header.min_ledger << "\n"
                << "  Max Ledger: " << header.max_ledger << "\n"
                << "  Version: " << static_cast<int>(catalogueVersion) << "\n"
                << "  Compression Level: " << static_cast<int>(compressionLevel_) << "\n"
                << "  Network ID: " << header.network_id << "\n\n";
        
        return offset + sizeof(CATLHeader);
    }
    
    // Process ledger info from a stream (works for any compression level)
    void processStreamedLedgerInfo(std::istream& stream, uint32_t sequence) {
        output_ << "=== LEDGER INFO ===\n";
        
        // We already read the sequence, so just display it
        std::vector<uint8_t> seqBytes(4);
        std::memcpy(seqBytes.data(), &sequence, 4);
        hexDump(output_, seqBytes, 0, "Ledger Sequence: " + std::to_string(sequence));
        
        // Helper function to read and display data from stream
        auto readAndDump = [&](size_t size, const std::string& label) -> std::vector<uint8_t> {
            std::vector<uint8_t> buffer(size);
            stream.read(reinterpret_cast<char*>(buffer.data()), size);
            
            if (stream.gcount() < static_cast<std::streamsize>(size)) {
                output_ << "ERROR: Unexpected EOF reading " << label << "\n";
                buffer.resize(stream.gcount());
                return buffer;
            }
            
            if (!label.empty()) {
                hexDump(output_, buffer, 0, label);
            }
            return buffer;
        };
        
        // Read all fields sequentially
        auto hashBytes = readAndDump(32, "");
        if (hashBytes.size() < 32) return;
        std::string hashHex = bytesToHexString(hashBytes.data(), 32);
        hexDump(output_, hashBytes, 0, "Hash: " + hashHex);
        
        auto txHashBytes = readAndDump(32, "");
        if (txHashBytes.size() < 32) return;
        std::string txHashHex = bytesToHexString(txHashBytes.data(), 32);
        hexDump(output_, txHashBytes, 0, "Tx Hash: " + txHashHex);
        
        auto accountHashBytes = readAndDump(32, "");
        if (accountHashBytes.size() < 32) return;
        std::string accountHashHex = bytesToHexString(accountHashBytes.data(), 32);
        hexDump(output_, accountHashBytes, 0, "Account Hash: " + accountHashHex);
        
        auto parentHashBytes = readAndDump(32, "");
        if (parentHashBytes.size() < 32) return;
        std::string parentHashHex = bytesToHexString(parentHashBytes.data(), 32);
        hexDump(output_, parentHashBytes, 0, "Parent Hash: " + parentHashHex);
        
        // Read drops (8 bytes)
        auto dropsBytes = readAndDump(8, "");
        if (dropsBytes.size() < 8) return;
        uint64_t drops = 0;
        std::memcpy(&drops, dropsBytes.data(), 8);
        hexDump(output_, dropsBytes, 0, "Drops: " + std::to_string(drops));
        
        // Read closeFlags (4 bytes)
        auto closeFlagsBytes = readAndDump(4, "");
        if (closeFlagsBytes.size() < 4) return;
        int32_t closeFlags = 0;
        std::memcpy(&closeFlags, closeFlagsBytes.data(), 4);
        hexDump(output_, closeFlagsBytes, 0, "Close Flags: " + std::to_string(closeFlags));
        
        // Read closeTimeResolution (4 bytes)
        auto ctrBytes = readAndDump(4, "");
        if (ctrBytes.size() < 4) return;
        uint32_t closeTimeResolution = 0;
        std::memcpy(&closeTimeResolution, ctrBytes.data(), 4);
        hexDump(output_, ctrBytes, 0, "Close Time Resolution: " + std::to_string(closeTimeResolution));
        
        // Read closeTime (8 bytes)
        auto ctBytes = readAndDump(8, "");
        if (ctBytes.size() < 8) return;
        uint64_t closeTime = 0;
        std::memcpy(&closeTime, ctBytes.data(), 8);
        std::string closeTimeStr = timeToString(closeTime);
        hexDump(output_, ctBytes, 0, "Close Time: " + std::to_string(closeTime) + " (" + closeTimeStr + ")");
        
        // Read parentCloseTime (8 bytes)
        auto pctBytes = readAndDump(8, "");
        if (pctBytes.size() < 8) return;
        uint64_t parentCloseTime = 0;
        std::memcpy(&parentCloseTime, pctBytes.data(), 8);
        std::string timeStr = timeToString(parentCloseTime);
        hexDump(output_, pctBytes, 0, "Parent Close Time: " + std::to_string(parentCloseTime) + " (" + timeStr + ")");
        
        output_ << "Ledger " << sequence << " Info - Total bytes read: " 
                << (4 + 32 + 32 + 32 + 32 + 8 + 4 + 4 + 8 + 8) << "\n\n";
    }
    
    // Process SHAMap from a stream (works for any compression level)
    void analyzeStreamSHAMap(std::istream& stream, const std::string& mapType, uint32_t ledgerSeq, bool isDelta = false) {
        output_ << "=== " << mapType << " for Ledger " << ledgerSeq << " ===\n";
        if (isDelta) {
            output_ << "Note: This is a DELTA map (changes from previous ledger)\n";
        }
        
        size_t nodeCount = 0;
        bool foundTerminal = false;
        
        while (!stream.eof()) {
            // Check for terminal marker
            uint8_t nodeType = 0;
            stream.read(reinterpret_cast<char*>(&nodeType), 1);
            
            if (stream.gcount() < 1 || stream.fail()) {
                output_ << "ERROR: Unexpected EOF reading node type\n";
                return;
            }
            
            std::vector<uint8_t> nodeTypeBytes = {nodeType};
            
            if (nodeType == static_cast<uint8_t>(SHAMapNodeType::tnTERMINAL)) {
                hexDump(output_, nodeTypeBytes, 0, "Terminal Marker - End of " + mapType);
                output_ << "Found terminal marker. " << mapType << " complete with " 
                        << nodeCount << " nodes.\n\n";
                foundTerminal = true;
                return;
            }
            
            // Not a terminal marker, parse as a node
            output_ << "--- Node " << (nodeCount+1) << " ---\n";
            
            // Node type
            hexDump(output_, nodeTypeBytes, 0, "Node Type: " + getNodeTypeDescription(nodeType));
            
            // Key (32 bytes)
            std::vector<uint8_t> keyBytes(32);
            stream.read(reinterpret_cast<char*>(keyBytes.data()), 32);
            
            if (stream.gcount() < 32 || stream.fail()) {
                output_ << "ERROR: Unexpected EOF reading node key\n";
                return;
            }
            
            std::string keyHex = bytesToHexString(keyBytes.data(), keyBytes.size());
            hexDump(output_, keyBytes, 0, "Key: " + keyHex);
            
            if (nodeType == SHAMapNodeType::tnREMOVE) {
                output_ << "  (This is a deletion marker)\n";
                nodeCount++;
                continue;
            }

            // Data size (4 bytes)
            uint32_t dataSize = 0;
            stream.read(reinterpret_cast<char*>(&dataSize), 4);
            
            if (stream.gcount() < 4 || stream.fail()) {
                output_ << "ERROR: Unexpected EOF reading data size\n";
                return;
            }
            
            std::vector<uint8_t> dataSizeBytes(4);
            std::memcpy(dataSizeBytes.data(), &dataSize, 4);
            
            // Suspiciously large value check
            std::string sizeNote = "Data Size: " + std::to_string(dataSize);
            if (dataSize > 10*1024*1024) {
                sizeNote += " (SUSPICIOUS!)";
            }
            
            hexDump(output_, dataSizeBytes, 0, sizeNote);
            
            if (dataSize == 0) {
                output_ << "  (This is a error = zero sized object)\n";
            } else if (dataSize > 10*1024*1024) {
                output_ << "WARNING: Data size is suspiciously large!\n";
                output_ << "  Possible file corruption detected.\n";
                output_ << "  Skipping to next ledger...\n";
                return;
            } else {
                // Show a preview of the data (up to 64 bytes)
                size_t previewSize = std::min(static_cast<size_t>(dataSize), static_cast<size_t>(64));
                std::vector<uint8_t> dataPreview(previewSize);
                
                stream.read(reinterpret_cast<char*>(dataPreview.data()), previewSize);
                
                if (stream.gcount() < static_cast<std::streamsize>(previewSize) || stream.fail()) {
                    output_ << "ERROR: Unexpected EOF reading data preview\n";
                    return;
                }
                
                hexDump(output_, dataPreview, 0, "Data Preview (" + std::to_string(previewSize) + 
                        " bytes of " + std::to_string(dataSize) + " total)");
                
                // Skip remaining data
                if (dataSize > previewSize) {
                    // Need to consume the remaining bytes from the stream
                    size_t remainingBytes = dataSize - previewSize;
                    std::vector<char> dummyBuffer(std::min(remainingBytes, static_cast<size_t>(4096)));
                    
                    while (remainingBytes > 0) {
                        size_t bytesToRead = std::min(remainingBytes, dummyBuffer.size());
                        stream.read(dummyBuffer.data(), bytesToRead);
                        size_t bytesRead = stream.gcount();
                        
                        if (bytesRead == 0) break; // EOF or error
                        
                        remainingBytes -= bytesRead;
                    }
                    
                    if (remainingBytes > 0) {
                        output_ << "WARNING: Could not consume all remaining data bytes\n";
                    }
                }
            }
            
            nodeCount++;
            
            if (verbose_) {
                output_ << "  Node " << nodeCount << " Complete\n";
            }
        }
        
        if (!foundTerminal) {
            output_ << "WARNING: No terminal marker found for " << mapType << "\n";
        }
    }
    
public:
    CatalogueAnalyzer(const std::string& filename, std::ostream& output, bool verbose = false)
        : filename_(filename), output_(output), verbose_(verbose) {
        
        file_.open(filename, std::ios::binary);
        if (!file_.is_open()) {
            throw std::runtime_error("Failed to open file: " + filename);
        }
        
        // Get file size
        file_.seekg(0, std::ios::end);
        fileSize_ = file_.tellg();
        file_.seekg(0, std::ios::beg);
        
        output_ << "Analyzing file: " << filename << "\n";
        output_ << "File size: " << fileSize_ << " bytes\n\n";
    }
    
    ~CatalogueAnalyzer() {
        if (file_.is_open()) {
            file_.close();
        }
    }
    
    void analyze() {
        try {
            size_t offset = 0;
            
            // Analyze header
            offset = analyzeHeader(offset);
            if (offset >= fileSize_) return;
            
            // Always set up a stream for reading, regardless of compression level
            std::unique_ptr<boost::iostreams::filtering_istream> dataStream = 
                std::make_unique<boost::iostreams::filtering_istream>();
            
            if (compressionLevel_ > 0) {
                output_ << "Processing catalogue with compression level " << static_cast<int>(compressionLevel_) << "\n\n";
                dataStream->push(boost::iostreams::zlib_decompressor());
            } else {
                output_ << "Processing catalogue with no compression (level 0)\n\n";
                // No decompressor needed, but still use streaming API for consistency
            }
            
            // Position file after header and connect it to the stream
            file_.seekg(sizeof(CATLHeader), std::ios::beg);
            dataStream->push(boost::ref(file_));
            
            // Process each ledger
            int ledgerCount = 0;
            uint32_t lastLedgerSeq = 0;
            
            while (!dataStream->eof()) {
                // Read ledger sequence first to identify
                uint32_t ledgerSeq = 0;
                
                // Read the sequence once and don't attempt to seek back
                if (!dataStream->read(reinterpret_cast<char*>(&ledgerSeq), 4) ||
                    dataStream->gcount() < 4) {
                    // EOF or error
                    break;
                }
                
                output_ << "Processing Ledger " << ledgerSeq << "\n";
                
                // Process ledger info - pass the already read sequence
                processStreamedLedgerInfo(*dataStream, ledgerSeq);
                
                // Analyze state map - if not the first ledger, it's a delta from previous
                bool isStateDelta = (ledgerCount > 0);
                output_ << "Analyzing STATE MAP" << (isStateDelta ? " (DELTA)" : "") << "...\n";
                analyzeStreamSHAMap(*dataStream, "STATE MAP", ledgerSeq, isStateDelta);
                
                // Analyze transaction map
                output_ << "Analyzing TRANSACTION MAP...\n";
                analyzeStreamSHAMap(*dataStream, "TRANSACTION MAP", ledgerSeq);
                
                ledgerCount++;
                lastLedgerSeq = ledgerSeq;
                
                output_ << "Ledger " << ledgerSeq << " processing complete.\n";
                output_ << "----------------------------------------------\n\n";
            }
            
            output_ << "Analysis complete. Processed " << ledgerCount << " ledgers.\n";
            output_ << "Last ledger processed: " << lastLedgerSeq << "\n";
            
        } catch (const std::exception& e) {
            output_ << "ERROR during analysis: " << e.what() << "\n";
        }
    }
};

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <catalogue_file> [output_file] [--verbose]\n";
        std::cerr << "\nThis tool analyzes CATL files from the XRP Ledger.\n";
        std::cerr << "It supports both compressed and uncompressed catalogue files.\n";
        std::cerr << "\nOptions:\n";
        std::cerr << "  --verbose     Show additional debug information\n";
        return 1;
    }
    
    std::string inputFile = argv[1];
    std::ofstream outputFile;
    std::ostream* output = &std::cout;
    bool verbose = false;
    
    // Check for verbose flag
    for (int i = 2; i < argc; i++) {
        if (std::string(argv[i]) == "--verbose") {
            verbose = true;
        }
    }
    
    // Check for output file
    if (argc > 2 && std::string(argv[2]) != "--verbose") {
        outputFile.open(argv[2]);
        if (!outputFile.is_open()) {
            std::cerr << "Failed to open output file: " << argv[2] << "\n";
            return 1;
        }
        output = &outputFile;
    }
    
    try {
        // Print banner
        *output << "===================================================================\n";
        *output << "XRPL Catalogue File Analyzer v2.0\n";
        *output << "Supports compressed (zlib) and uncompressed catalogue files\n";
        *output << "===================================================================\n\n";
        
        CatalogueAnalyzer analyzer(inputFile, *output, verbose);
        analyzer.analyze();
    } catch (const std::exception& e) {
        *output << "ERROR: " << e.what() << "\n";
        return 1;
    }
    
    if (outputFile.is_open()) {
        outputFile.close();
    }
    
    return 0;
}
