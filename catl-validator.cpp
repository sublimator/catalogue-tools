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

inline bool isCompressed(uint16_t versionField)
{
    return getCompressionLevel(versionField) > 0;
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
    bool isCompressed_ = false;
    uint8_t compressionLevel_ = 0;
    std::unique_ptr<boost::iostreams::filtering_istream> decompStream_;
    
    // Read a block of data from the appropriate input stream
    std::vector<uint8_t> readBytes(size_t offset, size_t size, bool useDecomp = false) {
        std::vector<uint8_t> buffer(size);
        
        if (useDecomp && decompStream_) {
            // Reading from decompression stream (position is not controllable)
            try {
                decompStream_->read(reinterpret_cast<char*>(buffer.data()), size);
                size_t bytesRead = decompStream_->gcount();
                buffer.resize(bytesRead); // Adjust for actual bytes read
            } catch (const std::exception& e) {
                output_ << "ERROR: Exception reading from decompression stream: " << e.what() << std::endl;
                buffer.clear();
            }
        } else {
            // Reading directly from file
            file_.seekg(offset, std::ios::beg);
            file_.read(reinterpret_cast<char*>(buffer.data()), size);
            size_t bytesRead = file_.gcount();
            buffer.resize(bytesRead); // Adjust for actual bytes read
        }
        
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
        isCompressed_ = compressionLevel_ > 0;
        
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
                << "  Compression Level: " << static_cast<int>(compressionLevel_) 
                << (isCompressed_ ? " (compressed)" : " (uncompressed)") << "\n"
                << "  Network ID: " << header.network_id << "\n\n";
        
        // If compressed, set up decompression stream
        if (isCompressed_) {
            output_ << "Setting up decompression stream (zlib level " 
                    << static_cast<int>(compressionLevel_) << ")...\n\n";
            
            try {
                decompStream_ = std::make_unique<boost::iostreams::filtering_istream>();
                decompStream_->push(boost::iostreams::zlib_decompressor());
                
                // Position file after header and connect it to the decompression stream
                file_.seekg(sizeof(CATLHeader), std::ios::beg);
                decompStream_->push(boost::ref(file_));
            } catch (const std::exception& e) {
                output_ << "ERROR: Failed to initialize decompression stream: " << e.what() << "\n";
                decompStream_.reset();
                isCompressed_ = false; // Fall back to uncompressed processing
            }
        }
        
        return offset + sizeof(CATLHeader);
    }
    
    // Analyze a ledger's information block
    size_t analyzeLedgerInfo(size_t offset) {
        if (isCompressed_ && decompStream_) {
            output_ << "=== LEDGER INFO (from decompression stream) ===\n";
        } else {
            output_ << "=== LEDGER INFO at offset 0x" << std::hex << offset << std::dec << " ===\n";
        }
        
        size_t startOffset = offset;
        bool useDecomp = isCompressed_ && decompStream_;
        
        // Read sequence number (first 4 bytes)
        auto seqBytes = readBytes(offset, 4, useDecomp);
        if (seqBytes.size() < 4) {
            output_ << "ERROR: Unexpected EOF reading ledger sequence\n";
            return fileSize_;
        }
        
        uint32_t sequence = 0;
        std::memcpy(&sequence, seqBytes.data(), 4);
        if (!useDecomp) {
            hexDump(output_, seqBytes, offset, "Ledger Sequence: " + std::to_string(sequence));
        } else {
            hexDump(output_, seqBytes, 0, "Ledger Sequence: " + std::to_string(sequence) + " (from decompression stream)");
        }
        
        offset += 4;
        
        // Read hash (32 bytes)
        auto hashBytes = readBytes(useDecomp ? 0 : offset, 32, useDecomp);
        if (hashBytes.size() < 32) {
            output_ << "ERROR: Unexpected EOF reading hash\n";
            return fileSize_;
        }
        std::string hashHex = bytesToHexString(hashBytes.data(), 32);
        if (!useDecomp) {
            hexDump(output_, hashBytes, offset, "Hash: " + hashHex);
        } else {
            hexDump(output_, hashBytes, 0, "Hash: " + hashHex + " (from decompression stream)");
        }
        offset += 32;
        
        // Read txHash (32 bytes)
        auto txHashBytes = readBytes(useDecomp ? 0 : offset, 32, useDecomp);
        if (txHashBytes.size() < 32) {
            output_ << "ERROR: Unexpected EOF reading txHash\n";
            return fileSize_;
        }
        std::string txHashHex = bytesToHexString(txHashBytes.data(), 32);
        if (!useDecomp) {
            hexDump(output_, txHashBytes, offset, "Tx Hash: " + txHashHex);
        } else {
            hexDump(output_, txHashBytes, 0, "Tx Hash: " + txHashHex + " (from decompression stream)");
        }
        offset += 32;
        
        // Read accountHash (32 bytes)
        auto accountHashBytes = readBytes(useDecomp ? 0 : offset, 32, useDecomp);
        if (accountHashBytes.size() < 32) {
            output_ << "ERROR: Unexpected EOF reading accountHash\n";
            return fileSize_;
        }
        std::string accountHashHex = bytesToHexString(accountHashBytes.data(), 32);
        if (!useDecomp) {
            hexDump(output_, accountHashBytes, offset, "Account Hash: " + accountHashHex);
        } else {
            hexDump(output_, accountHashBytes, 0, "Account Hash: " + accountHashHex + " (from decompression stream)");
        }
        offset += 32;
        
        // Read parentHash (32 bytes)
        auto parentHashBytes = readBytes(useDecomp ? 0 : offset, 32, useDecomp);
        if (parentHashBytes.size() < 32) {
            output_ << "ERROR: Unexpected EOF reading parentHash\n";
            return fileSize_;
        }
        std::string parentHashHex = bytesToHexString(parentHashBytes.data(), 32);
        if (!useDecomp) {
            hexDump(output_, parentHashBytes, offset, "Parent Hash: " + parentHashHex);
        } else {
            hexDump(output_, parentHashBytes, 0, "Parent Hash: " + parentHashHex + " (from decompression stream)");
        }
        offset += 32;
        
        // Read drops (8 bytes) - XRPAmount (uint64_t here)
        auto dropsBytes = readBytes(useDecomp ? 0 : offset, 8, useDecomp);
        if (dropsBytes.size() < 8) {
            output_ << "ERROR: Unexpected EOF reading drops\n";
            return fileSize_;
        }
        
        uint64_t drops = 0;
        std::memcpy(&drops, dropsBytes.data(), 8);
        if (!useDecomp) {
            hexDump(output_, dropsBytes, offset, "Drops: " + std::to_string(drops));
        } else {
            hexDump(output_, dropsBytes, 0, "Drops: " + std::to_string(drops) + " (from decompression stream)");
        }
        offset += 8;
        
        // Read closeFlags (4 bytes, since it's an int in LedgerInfo)
        auto closeFlagsBytes = readBytes(useDecomp ? 0 : offset, 4, useDecomp);
        if (closeFlagsBytes.size() < 4) {
            output_ << "ERROR: Unexpected EOF reading closeFlags\n";
            return fileSize_;
        }
        
        int32_t closeFlags = 0;
        std::memcpy(&closeFlags, closeFlagsBytes.data(), 4);
        if (!useDecomp) {
            hexDump(output_, closeFlagsBytes, offset, "Close Flags: " + std::to_string(closeFlags));
        } else {
            hexDump(output_, closeFlagsBytes, 0, "Close Flags: " + std::to_string(closeFlags) + " (from decompression stream)");
        }
        offset += 4;
        
        // Read closeTimeResolution (4 bytes)
        auto ctrBytes = readBytes(useDecomp ? 0 : offset, 4, useDecomp);
        if (ctrBytes.size() < 4) {
            output_ << "ERROR: Unexpected EOF reading closeTimeResolution\n";
            return fileSize_;
        }
        
        uint32_t closeTimeResolution = 0;
        std::memcpy(&closeTimeResolution, ctrBytes.data(), 4);
        if (!useDecomp) {
            hexDump(output_, ctrBytes, offset, "Close Time Resolution: " + std::to_string(closeTimeResolution));
        } else {
            hexDump(output_, ctrBytes, 0, "Close Time Resolution: " + std::to_string(closeTimeResolution) + " (from decompression stream)");
        }
        offset += 4;
        
        // Read closeTime (8 bytes) - uint64_t
        auto ctBytes = readBytes(useDecomp ? 0 : offset, 8, useDecomp);
        if (ctBytes.size() < 8) {
            output_ << "ERROR: Unexpected EOF reading closeTime\n";
            return fileSize_;
        }
        
        uint64_t closeTime = 0;
        std::memcpy(&closeTime, ctBytes.data(), 8);
        std::string closeTimeStr = timeToString(closeTime);
        if (!useDecomp) {
            hexDump(output_, ctBytes, offset, "Close Time: " + std::to_string(closeTime) + 
                  " (" + closeTimeStr + ")");
        } else {
            hexDump(output_, ctBytes, 0, "Close Time: " + std::to_string(closeTime) + 
                  " (" + closeTimeStr + ")" + " (from decompression stream)");
        }
        offset += 8;
        
        // Read parentCloseTime (8 bytes) - uint64_t
        auto pctBytes = readBytes(useDecomp ? 0 : offset, 8, useDecomp);
        if (pctBytes.size() < 8) {
            output_ << "ERROR: Unexpected EOF reading parentCloseTime\n";
            return fileSize_;
        }
        
        uint64_t parentCloseTime = 0;
        std::memcpy(&parentCloseTime, pctBytes.data(), 8);
        
        std::string timeStr = timeToString(parentCloseTime);
        if (!useDecomp) {
            hexDump(output_, pctBytes, offset, "Parent Close Time: " + std::to_string(parentCloseTime) + 
                  " (" + timeStr + ")");
        } else {
            hexDump(output_, pctBytes, 0, "Parent Close Time: " + std::to_string(parentCloseTime) + 
                  " (" + timeStr + ")" + " (from decompression stream)");
        }
        offset += 8;
        
        if (!useDecomp) {
            output_ << "Ledger " << sequence << " Info - Size: " << (offset - startOffset) << " bytes\n\n";
        } else {
            output_ << "Ledger " << sequence << " Info - Total bytes read from stream: " 
                    << (4 + 32 + 32 + 32 + 32 + 8 + 4 + 4 + 8 + 8) << "\n\n";
        }
        
        return offset;
    }
    
    // Analyze a SHAMap structure and its leaf nodes
    size_t analyzeSHAMap(size_t offset, const std::string& mapType, uint32_t ledgerSeq, bool isDelta = false) {
        if (isCompressed_ && decompStream_) {
            output_ << "=== " << mapType << " for Ledger " << ledgerSeq 
                    << " (from decompression stream) ===\n";
            if (isDelta) {
                output_ << "Note: This is a DELTA map (changes from previous ledger)\n";
            }
        } else {
            output_ << "=== " << mapType << " for Ledger " << ledgerSeq 
                    << " at offset 0x" << std::hex << offset << std::dec << " ===\n";
        }
        
        size_t nodeCount = 0;
        bool foundTerminal = false;
        bool useDecomp = isCompressed_ && decompStream_;
        
        while ((useDecomp && !decompStream_->eof()) || (!useDecomp && offset < fileSize_)) {
            // Check for terminal marker
            auto nodeTypeBytes = readBytes(useDecomp ? 0 : offset, 1, useDecomp);
            if (nodeTypeBytes.size() < 1) {
                output_ << "ERROR: Unexpected EOF reading node type\n";
                return fileSize_;
            }
            
            uint8_t nodeType = nodeTypeBytes[0];
            
            if (nodeType == static_cast<uint8_t>(SHAMapNodeType::tnTERMINAL)) {
                if (!useDecomp) {
                    hexDump(output_, nodeTypeBytes, offset, "Terminal Marker - End of " + mapType);
                } else {
                    hexDump(output_, nodeTypeBytes, 0, "Terminal Marker - End of " + mapType + " (from decompression stream)");
                }
                output_ << "Found terminal marker. " << mapType << " complete with " 
                        << nodeCount << " nodes.\n\n";
                foundTerminal = true;
                return offset + 1;
            }
            
            // Not a terminal marker, parse as a node
            output_ << "--- Node " << (nodeCount+1); 
            if (!useDecomp) {
                output_ << " at offset 0x" << std::hex << offset << std::dec;
            }
            output_ << " ---\n";
            
            // Node type
            if (!useDecomp) {
                hexDump(output_, nodeTypeBytes, offset, "Node Type: " + getNodeTypeDescription(nodeType));
            } else {
                hexDump(output_, nodeTypeBytes, 0, "Node Type: " + getNodeTypeDescription(nodeType) + " (from decompression stream)");
            }
            offset += 1;
            
            // Key (32 bytes)
            auto keyBytes = readBytes(useDecomp ? 0 : offset, 32, useDecomp);
            if (keyBytes.size() < 32) {
                output_ << "ERROR: Unexpected EOF reading node key\n";
                return fileSize_;
            }
            
            std::string keyHex = bytesToHexString(keyBytes.data(), keyBytes.size());
            
            if (!useDecomp) {
                hexDump(output_, keyBytes, offset, "Key: " + keyHex);
            } else {
                hexDump(output_, keyBytes, 0, "Key: " + keyHex + " (from decompression stream)");
            }
            offset += 32;
            
            if (nodeType == tnREMOVE)
            {
                output_ << "  (This is a deletion marker)\n";
                continue;
            }

            // Data size (4 bytes)
            auto dataSizeBytes = readBytes(useDecomp ? 0 : offset, 4, useDecomp);
            if (dataSizeBytes.size() < 4) {
                output_ << "ERROR: Unexpected EOF reading data size\n";
                return fileSize_;
            }
            
            uint32_t dataSize = 0;
            std::memcpy(&dataSize, dataSizeBytes.data(), 4);
            
            // Suspiciously large value check
            std::string sizeNote = "Data Size: " + std::to_string(dataSize);
            if (dataSize > 10*1024*1024) {
                sizeNote += " (SUSPICIOUS!)";
            }
            
            if (!useDecomp) {
                hexDump(output_, dataSizeBytes, offset, sizeNote);
            } else {
                hexDump(output_, dataSizeBytes, 0, sizeNote + " (from decompression stream)");
            }
            offset += 4;
            
            if (dataSize == 0) {
                output_ << "  (This is a error = zero sized object)\n";
            } else if (dataSize > 10*1024*1024) {
                output_ << "WARNING: Data size is suspiciously large!\n";
                output_ << "  Possible file corruption detected.\n";
                
                // Attempt to find next valid node or terminal marker
                output_ << "  Attempting to recover by scanning for next valid node...\n";
                
                if (useDecomp) {
                    output_ << "  Recovery in compressed stream not supported. Stopping analysis.\n";
                    return fileSize_;
                }
                
                size_t scanOffset = offset;
                size_t maxScan = 1024; // Limit scan distance
                bool recovered = false;
                
                for (size_t i = 0; i < maxScan && scanOffset < fileSize_; i++, scanOffset++) {
                    auto probeByte = readBytes(scanOffset, 1);
                    if (probeByte.size() < 1) break;
                    
                    if (probeByte[0] <= 3 || probeByte[0] == 255) {
                        // This could be a valid node type, check if it looks reasonable
                        output_ << "  Found possible node boundary at offset 0x" 
                                << std::hex << scanOffset << std::dec << "\n";
                        
                        // If we've found a potential node, try to read a key to see if it's valid
                        if (scanOffset + 33 <= fileSize_) {
                            auto possibleKeyBytes = readBytes(scanOffset + 1, 32);
                            bool couldBeKey = true;
                            
                                // Check if the key bytes look reasonable
                                for (auto b : possibleKeyBytes) {
                                    if (!std::isprint(b) && b != 0) {
                                        couldBeKey = false;
                                        break;
                                    }
                                }
                                
                                if (couldBeKey) {
                                    output_ << "  Found potential valid node at offset 0x" 
                                            << std::hex << scanOffset << std::dec << "\n";
                                    offset = scanOffset;
                                    recovered = true;
                                    break;
                                }
                        }
                    }
                }
                
                if (!recovered) {
                    output_ << "  Unable to recover. Stopping analysis.\n";
                    return fileSize_;
                }
                
                continue; // Skip to next iteration with the new offset
            } else {
                // Show a preview of the data (up to 64 bytes)
                size_t previewSize = std::min(static_cast<size_t>(dataSize), static_cast<size_t>(64));
                auto dataPreview = readBytes(useDecomp ? 0 : offset, previewSize, useDecomp);
                
                if (dataPreview.size() < previewSize) {
                    output_ << "ERROR: Unexpected EOF reading data preview\n";
                    return fileSize_;
                }
                
                if (!useDecomp) {
                    hexDump(output_, dataPreview, offset, "Data Preview (" + std::to_string(previewSize) + 
                            " bytes of " + std::to_string(dataSize) + " total)");
                } else {
                    hexDump(output_, dataPreview, 0, "Data Preview (" + std::to_string(previewSize) + 
                            " bytes of " + std::to_string(dataSize) + " total) (from decompression stream)");
                }
                
                // Skip remaining data
                if (useDecomp && dataSize > previewSize) {
                    // Need to consume the remaining bytes from the stream
                    size_t remainingBytes = dataSize - previewSize;
                    std::vector<char> dummyBuffer(std::min(remainingBytes, static_cast<size_t>(4096)));
                    
                    while (remainingBytes > 0) {
                        size_t bytesToRead = std::min(remainingBytes, dummyBuffer.size());
                        decompStream_->read(dummyBuffer.data(), bytesToRead);
                        size_t bytesRead = decompStream_->gcount();
                        
                        if (bytesRead == 0) break; // EOF or error
                        
                        remainingBytes -= bytesRead;
                    }
                    
                    if (remainingBytes > 0) {
                        output_ << "WARNING: Could not consume all remaining data bytes\n";
                    }
                }
                
                offset += dataSize;
            }
            
            nodeCount++;
            
            if (verbose_) {
                output_ << "  Node " << nodeCount << " Complete\n";
            }
        }
        
        if (!foundTerminal) {
            output_ << "WARNING: No terminal marker found for " << mapType << "\n";
        }
        
        return offset;
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
        
        if (decompStream_) {
            try {
                decompStream_->reset();
            } catch (...) {
                // Ignore exceptions during cleanup
            }
        }
    }
    
    void analyze() {
        try {
            size_t offset = 0;
            
            // Analyze header
            offset = analyzeHeader(offset);
            if (offset >= fileSize_) return;
            
            // Process each ledger
            int ledgerCount = 0;
            uint32_t lastLedgerSeq = 0;
            
            // If using decompression stream, we'll read sequentially
            if (isCompressed_ && decompStream_) {
                output_ << "Processing compressed catalogue using streaming decompression...\n\n";
                
                while (!decompStream_->eof()) {
                    // Read ledger sequence first to identify
                    uint32_t ledgerSeq = 0;
                    decompStream_->read(reinterpret_cast<char*>(&ledgerSeq), 4);
                    if (decompStream_->gcount() < 4) break; // EOF
                    
                    // Reset to start of sequence for LedgerInfo processing
                    decompStream_->seekg(-4, std::ios_base::cur);
                    
                    output_ << "Processing Ledger " << ledgerSeq << " (from compressed stream)\n";
                    
                    // Process ledger info (reads sequence again)
                    analyzeLedgerInfo(0); // offset is ignored for compressed streams
                    
                    // Analyze state map - if not the first ledger, it's a delta from previous
                    bool isStateDelta = (ledgerCount > 0);
                    output_ << "Analyzing STATE MAP" << (isStateDelta ? " (DELTA)" : "") << "...\n";
                    analyzeSHAMap(0, "STATE MAP", ledgerSeq, isStateDelta);
                    
                    // Analyze transaction map
                    output_ << "Analyzing TRANSACTION MAP...\n";
                    analyzeSHAMap(0, "TRANSACTION MAP", ledgerSeq);
                    
                    ledgerCount++;
                    lastLedgerSeq = ledgerSeq;
                    
                    output_ << "Ledger " << ledgerSeq << " processing complete.\n";
                    output_ << "----------------------------------------------\n\n";
                }
            } else {
                // Traditional file offset-based processing
                while (offset < fileSize_) {
                    // Read ledger info
                    uint32_t ledgerSeq = 0;
                    auto seqBytes = readBytes(offset, 4);
                    if (seqBytes.size() < 4) break;
                    std::memcpy(&ledgerSeq, seqBytes.data(), 4);
                    
                    output_ << "Processing Ledger " << ledgerSeq << "\n";
                    
                    size_t ledgerInfoStart = offset;
                    offset = analyzeLedgerInfo(offset);
                    if (offset >= fileSize_) break;
                    
                    // Analyze state map
                    output_ << "Analyzing STATE MAP...\n";
                    offset = analyzeSHAMap(offset, "STATE MAP", ledgerSeq);
                    if (offset >= fileSize_) break;
                    
                    // Analyze transaction map
                    output_ << "Analyzing TRANSACTION MAP...\n";
                    offset = analyzeSHAMap(offset, "TRANSACTION MAP", ledgerSeq);
                    if (offset >= fileSize_) break;
                    
                    ledgerCount++;
                    lastLedgerSeq = ledgerSeq;
                    
                    output_ << "Ledger " << ledgerSeq << " processing complete.\n";
                    output_ << "----------------------------------------------\n\n";
                }
            }
            
            output_ << "Analysis complete. Processed " << ledgerCount << " ledgers.\n";
            output_ << "Last ledger processed: " << lastLedgerSeq << "\n";
            
            // Check for remaining bytes (only for uncompressed)
            if (!isCompressed_) {
                size_t remainingBytes = fileSize_ - offset;
                if (remainingBytes > 0) {
                    output_ << "WARNING: " << remainingBytes << " unprocessed bytes at end of file!\n";
                    
                    // Dump some of the trailing bytes
                    size_t bytesToDump = std::min(remainingBytes, static_cast<size_t>(64));
                    auto trailingBytes = readBytes(offset, bytesToDump);
                    
                    output_ << "Trailing bytes:\n";
                    hexDump(output_, trailingBytes, offset, "Unprocessed data");
                }
            }
            
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

