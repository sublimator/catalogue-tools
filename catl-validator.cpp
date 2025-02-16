#include <iostream>
#include <fstream>
#include <vector>
#include <set>
#include <map>
#include <cstring>
#include <cstdint>
#include <string>
#include <algorithm>
#include <stdexcept>
#include <iomanip>

// Constants from the original code
static constexpr uint32_t HAS_NEXT_FLAG = 0x80000000;
static constexpr uint32_t SIZE_MASK = 0x0FFFFFFF;

// File header structure
#pragma pack(push, 1)
struct CATLHeader {
    char magic[4];
    uint32_t version;
    uint32_t min_ledger;
    uint32_t max_ledger;
    uint32_t network_id;
    uint32_t ledger_tx_offset;
};
#pragma pack(pop)

// Structure to track state entry positions
struct StatePosition {
    std::streampos filePos;
    uint32_t sequence;
    uint32_t size;
};

// Simple 256-bit hash class for tracking keys
class uint256 {
    unsigned char data_[32];

public:
    uint256() {
        std::memset(data_, 0, sizeof(data_));
    }

    const unsigned char* data() const { return data_; }
    unsigned char* data() { return data_; }

    bool operator<(const uint256& other) const {
        return std::memcmp(data_, other.data_, 32) < 0;
    }

    friend std::ostream& operator<<(std::ostream& os, const uint256& h) {
        os << std::hex;
        for (int i = 0; i < 32; ++i) {
            os << std::setw(2) << std::setfill('0') 
               << static_cast<int>(h.data_[i]);
        }
        os << std::dec;
        return os;
    }
};

class CATLValidator {
    std::string filepath_;
    std::ifstream file_;
    CATLHeader header_;
    size_t total_bytes_read_ = 0;
    std::set<uint256> unique_keys_;
    
    // Statistics
    struct Statistics {
        size_t total_keys = 0;
        size_t total_state_entries = 0;
        size_t total_ledgers = 0;
        size_t total_transactions = 0;
        size_t total_bytes = 0;
        std::map<uint32_t, size_t> states_per_ledger;
    } stats_;

    void validateHeader() {
        std::cout << "Validating header...\n";
        
        if (std::memcmp(header_.magic, "CATL", 4) != 0) {
            throw std::runtime_error("Invalid magic number in header");
        }

        if (header_.version != 1) {
            throw std::runtime_error("Unsupported version: " + 
                                   std::to_string(header_.version));
        }

        if (header_.min_ledger > header_.max_ledger) {
            throw std::runtime_error("Invalid ledger range: min > max");
        }

        if (header_.ledger_tx_offset <= sizeof(CATLHeader)) {
            throw std::runtime_error("Invalid ledger_tx_offset");
        }

        std::cout << "Header validation successful:\n"
                  << "  Version: " << header_.version << "\n"
                  << "  Network ID: " << header_.network_id << "\n"
                  << "  Ledger range: " << header_.min_ledger 
                  << " - " << header_.max_ledger << "\n"
                  << "  Ledger/TX offset: " << header_.ledger_tx_offset << "\n";
    }

    void validateStateData() {
        std::cout << "Validating state data section...\n";
        
        file_.seekg(sizeof(CATLHeader));
        if (file_.fail()) {
            throw std::runtime_error("Failed to seek to state data section");
        }

        while (file_.tellg() < header_.ledger_tx_offset) {
            uint256 key;
            if (!file_.read(reinterpret_cast<char*>(key.data()), 32)) {
                throw std::runtime_error("Failed to read state key");
            }
            total_bytes_read_ += 32;

            unique_keys_.insert(key);
            stats_.total_keys++;

            bool has_next = true;
            while (has_next) {
                uint32_t sequence, flags_and_size;
                
                if (!file_.read(reinterpret_cast<char*>(&sequence), 4) ||
                    !file_.read(reinterpret_cast<char*>(&flags_and_size), 4)) {
                    throw std::runtime_error("Failed to read state metadata");
                }
                total_bytes_read_ += 8;

                uint32_t size = flags_and_size & SIZE_MASK;
                has_next = (flags_and_size & HAS_NEXT_FLAG) != 0;

                if (sequence < header_.min_ledger || 
                    sequence > header_.max_ledger) {
                    throw std::runtime_error(
                        "State entry sequence " + std::to_string(sequence) + 
                        " outside valid range");
                }

                if (size > 0) {
                    stats_.total_state_entries++;
                    stats_.states_per_ledger[sequence]++;
                    
                    // Skip the state data
                    file_.seekg(size, std::ios::cur);
                    if (file_.fail()) {
                        throw std::runtime_error(
                            "Failed to skip state data of size " + 
                            std::to_string(size));
                    }
                    total_bytes_read_ += size;
                }
            }

            if (stats_.total_keys % 1000 == 0) {
                std::cout << "Processed " << stats_.total_keys << " keys, " 
                         << stats_.total_state_entries << " state entries\n";
            }
        }

        if (file_.tellg() != header_.ledger_tx_offset) {
            throw std::runtime_error(
                "State data section size mismatch with ledger_tx_offset");
        }

        std::cout << "State data validation completed:\n"
                  << "  Total unique keys: " << unique_keys_.size() << "\n"
                  << "  Total state entries: " << stats_.total_state_entries 
                  << "\n";
    }

    void validateLedgerAndTxData() {
        std::cout << "Validating ledger and transaction data...\n";
        
        file_.seekg(header_.ledger_tx_offset);
        
        while (!file_.eof()) {
            uint64_t next_offset;
            if (!file_.read(reinterpret_cast<char*>(&next_offset), 8)) {
                if (file_.eof()) break;
                throw std::runtime_error("Failed to read next offset");
            }
            total_bytes_read_ += 8;

            // Read ledger header (fixed size)
            constexpr size_t LEDGER_HEADER_SIZE = 
                32 * 4 + // Four 256-bit hashes
                8 +      // drops
                4 * 5;   // Five 32-bit fields
            
            std::vector<char> ledger_header(LEDGER_HEADER_SIZE);
            if (!file_.read(ledger_header.data(), LEDGER_HEADER_SIZE)) {
                throw std::runtime_error("Failed to read ledger header");
            }
            total_bytes_read_ += LEDGER_HEADER_SIZE;
            
            // Extract sequence from ledger header (first 4 bytes)
            uint32_t sequence;
            std::memcpy(&sequence, ledger_header.data(), 4);
            
            if (sequence < header_.min_ledger || 
                sequence > header_.max_ledger) {
                throw std::runtime_error(
                    "Ledger sequence " + std::to_string(sequence) + 
                    " outside valid range");
            }

            size_t tx_count = 0;
            auto current_pos = file_.tellg();
            
            // Read transactions until we reach the next ledger
            while (current_pos < next_offset && !file_.eof()) {
                // Transaction ID (32 bytes)
                uint256 tx_id;
                if (!file_.read(reinterpret_cast<char*>(tx_id.data()), 32)) {
                    throw std::runtime_error("Failed to read transaction ID");
                }
                total_bytes_read_ += 32;

                // Transaction size
                uint32_t tx_size;
                if (!file_.read(reinterpret_cast<char*>(&tx_size), 4)) {
                    throw std::runtime_error("Failed to read transaction size");
                }
                total_bytes_read_ += 4;

                // Skip transaction data
                file_.seekg(tx_size, std::ios::cur);
                if (file_.fail()) {
                    throw std::runtime_error(
                        "Failed to skip transaction data of size " + 
                        std::to_string(tx_size));
                }
                total_bytes_read_ += tx_size;

                // Metadata size
                uint32_t meta_size;
                if (!file_.read(reinterpret_cast<char*>(&meta_size), 4)) {
                    throw std::runtime_error("Failed to read metadata size");
                }
                total_bytes_read_ += 4;

                // Skip metadata if present
                if (meta_size > 0) {
                    file_.seekg(meta_size, std::ios::cur);
                    if (file_.fail()) {
                        throw std::runtime_error(
                            "Failed to skip metadata of size " + 
                            std::to_string(meta_size));
                    }
                    total_bytes_read_ += meta_size;
                }

                tx_count++;
                current_pos = file_.tellg();
            }

            stats_.total_transactions += tx_count;
            stats_.total_ledgers++;

            if (stats_.total_ledgers % 100 == 0) {
                std::cout << "Processed " << stats_.total_ledgers 
                         << " ledgers, " << stats_.total_transactions 
                         << " total transactions\n";
            }

            // Verify we're at the expected position
            if (current_pos != next_offset) {
                throw std::runtime_error(
                    "Ledger data size mismatch with next_offset");
            }
        }

        std::cout << "Ledger and transaction validation completed:\n"
                  << "  Total ledgers: " << stats_.total_ledgers << "\n"
                  << "  Total transactions: " << stats_.total_transactions 
                  << "\n";
    }

public:
    explicit CATLValidator(const std::string& filepath) 
        : filepath_(filepath) {
        file_.open(filepath, std::ios::binary);
        if (!file_) {
            throw std::runtime_error(
                "Failed to open file: " + filepath);
        }
    }

    void validate() {
        std::cout << "Starting validation of: " << filepath_ << "\n";

        // Read and validate header
        if (!file_.read(reinterpret_cast<char*>(&header_), 
                       sizeof(CATLHeader))) {
            throw std::runtime_error("Failed to read file header");
        }
        total_bytes_read_ += sizeof(CATLHeader);

        validateHeader();
        validateStateData();
        validateLedgerAndTxData();

        std::cout << "\nValidation completed successfully\n"
                  << "Summary:\n"
                  << "  Total bytes read: " << total_bytes_read_ << "\n"
                  << "  Unique keys: " << unique_keys_.size() << "\n"
                  << "  State entries: " << stats_.total_state_entries << "\n"
                  << "  Ledgers: " << stats_.total_ledgers << "\n"
                  << "  Transactions: " << stats_.total_transactions << "\n";
    }
};

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <catalogue_file>\n";
        return 1;
    }

    try {
        CATLValidator validator(argv[1]);
        validator.validate();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
}
