#include "catl/common/ledger-info.h"
#include "catl/core/log-macros.h"
#include "catl/core/types.h"
#include "catl/utils-v1/nudb/nudb-exp-arg-options.h"

#include <boost/filesystem.hpp>
#include <iomanip>
#include <iostream>
#include <lz4.h>
#include <nudb/basic_store.hpp>
#include <nudb/detail/buffer.hpp>
#include <nudb/posix_file.hpp>
#include <nudb/visit.hpp>
#include <nudb/xxhasher.hpp>
#include <stdexcept>
#include <string>
#include <vector>

using namespace catl::v1::utils::nudb;
namespace fs = boost::filesystem;

/**
 * Sparse representation of a SHAMap inner node
 * Only stores non-empty branches
 */
class InnerNode
{
private:
    std::vector<Hash256> branches_;
    uint16_t branch_mask_;

    // Get the storage index for a branch index using popcount
    size_t
    get_storage_index(size_t branch_index) const
    {
        if (branch_index >= 16 || !(branch_mask_ & (1u << (15 - branch_index))))
            return std::numeric_limits<size_t>::max();

        // Count bits set before this position
        uint16_t mask_before = branch_mask_ >> (16 - branch_index);
        return __builtin_popcount(mask_before);
    }

public:
    InnerNode(uint16_t mask) : branch_mask_(mask)
    {
        // Allocate exactly the number of branches we need
        size_t branch_count = __builtin_popcount(mask);
        branches_.reserve(branch_count);
    }

    // Factory method for compressed inner node (type 2)
    static InnerNode
    from_compressed(const uint8_t* data, size_t size)
    {
        if (size < 2)
            throw std::runtime_error("Compressed inner node too small");

        // Read the bitmask
        uint16_t mask = (static_cast<uint16_t>(data[0]) << 8) | data[1];

        InnerNode node(mask);

        const uint8_t* hash_ptr = data + 2;
        size_t remaining = size - 2;
        size_t expected_hashes = __builtin_popcount(mask);

        if (remaining != expected_hashes * 32)
            throw std::runtime_error("Size mismatch in compressed inner node");

        // Add only the present hashes
        for (size_t i = 0; i < expected_hashes; ++i)
        {
            node.branches_.emplace_back(hash_ptr);
            hash_ptr += 32;
        }

        return node;
    }

    // Factory method for full inner node (type 3)
    static InnerNode
    from_full(const uint8_t* data, size_t size)
    {
        if (size != 512)
            throw std::runtime_error("Full inner node must be 512 bytes");

        // Check which branches are non-zero to build mask
        uint16_t mask = 0;
        for (int i = 0; i < 16; ++i)
        {
            Hash256 h(data + (i * 32));
            if (h != Hash256::zero())
                mask |= (1u << (15 - i));
        }

        InnerNode node(mask);

        // Add only non-zero hashes
        for (int i = 0; i < 16; ++i)
        {
            Hash256 h(data + (i * 32));
            if (h != Hash256::zero())
                node.branches_.push_back(h);
        }

        return node;
    }

    // Get branch by index (returns zero hash if not present)
    Hash256
    get_branch(size_t index) const
    {
        if (index >= 16)
            return Hash256();

        size_t storage_idx = get_storage_index(index);
        if (storage_idx == std::numeric_limits<size_t>::max())
            return Hash256();  // Empty branch

        return branches_[storage_idx];
    }

    void
    display() const
    {
        std::cout << "\nInner node (mask: 0x" << std::hex << std::uppercase
                  << std::setw(4) << std::setfill('0') << branch_mask_
                  << std::dec << std::nouppercase << "):\n";

        std::cout << "Stored branches: " << branches_.size()
                  << " (popcount: " << __builtin_popcount(branch_mask_)
                  << ")\n\n";

        for (int i = 0; i < 16; ++i)
        {
            std::cout << std::setw(2) << i << ": ";

            Hash256 branch = get_branch(i);
            if (branch == Hash256::zero())
            {
                std::cout << "(empty)\n";
            }
            else
            {
                std::cout << branch.hex() << "\n";
            }
        }
    }
};

/**
 * NudbExplorer - Tool for exploring and querying NuDB databases
 *
 * This tool provides various ways to examine NuDB databases:
 * - Fetch and display specific keys
 * - List all keys in the database
 * - Show database statistics
 */
class NudbExplorer
{
private:
    const NudbExpOptions& options_;

    // Use xxhasher by default (what xahaud uses)
    using store_type = nudb::basic_store<nudb::xxhasher, nudb::posix_file>;

    // Convert hex string to bytes
    std::vector<uint8_t>
    hex_to_bytes(const std::string& hex)
    {
        std::vector<uint8_t> bytes;

        // Remove any 0x prefix
        std::string clean_hex = hex;
        if (clean_hex.size() >= 2 && clean_hex[0] == '0' &&
            (clean_hex[1] == 'x' || clean_hex[1] == 'X'))
        {
            clean_hex = clean_hex.substr(2);
        }

        // Ensure even number of hex digits
        if (clean_hex.size() % 2 != 0)
        {
            clean_hex = "0" + clean_hex;
        }

        for (size_t i = 0; i < clean_hex.size(); i += 2)
        {
            std::string byte_str = clean_hex.substr(i, 2);
            uint8_t byte =
                static_cast<uint8_t>(std::stoi(byte_str, nullptr, 16));
            bytes.push_back(byte);
        }

        return bytes;
    }

    // Convert bytes to hex string
    std::string
    bytes_to_hex(const uint8_t* data, size_t size)
    {
        std::stringstream ss;
        ss << std::hex << std::setfill('0');
        for (size_t i = 0; i < size; ++i)
        {
            ss << std::setw(2) << static_cast<int>(data[i]);
        }
        return ss.str();
    }

    // Read varint from buffer
    size_t
    read_varint(const uint8_t* data, size_t size, size_t& value)
    {
        if (size == 0)
            return 0;

        // For simple single-byte varints (common case)
        if (data[0] < 128)
        {
            value = data[0];
            return 1;
        }

        value = 0;
        size_t shift = 0;
        size_t i = 0;

        while (i < size && i < 10)  // max 10 bytes for 64-bit varint
        {
            uint8_t byte = data[i];
            value |= static_cast<size_t>(byte & 0x7F) << shift;
            i++;

            if ((byte & 0x80) == 0)
                return i;

            shift += 7;
        }

        return 0;  // Invalid varint
    }

    // Decompress and analyze node data
    void
    analyze_node_data(const uint8_t* compressed_data, size_t compressed_size)
    {
        // Read compression type varint
        size_t compression_type = 0;
        size_t varint_size =
            read_varint(compressed_data, compressed_size, compression_type);

        if (varint_size == 0)
        {
            std::cout << "Failed to read compression type varint\n";
            return;
        }

        std::cout << "Compression type: " << compression_type << " (";
        switch (compression_type)
        {
            case 0:
                std::cout << "uncompressed";
                break;
            case 1:
                std::cout << "lz4";
                break;
            case 2:
                std::cout << "compressed v1 inner node";
                break;
            case 3:
                std::cout << "full v1 inner node";
                break;
            default:
                std::cout << "unknown";
                break;
        }
        std::cout << ")\n";

        const uint8_t* payload = compressed_data + varint_size;
        size_t payload_size = compressed_size - varint_size;

        if (compression_type == 1)  // LZ4 compressed
        {
            // Read decompressed size varint
            size_t decompressed_size = 0;
            size_t size_varint_len =
                read_varint(payload, payload_size, decompressed_size);

            if (size_varint_len == 0)
            {
                std::cout << "Failed to read decompressed size varint\n";
                return;
            }

            std::cout << "Decompressed size: " << decompressed_size
                      << " bytes\n";
            std::cout << "Compressed payload size: "
                      << (payload_size - size_varint_len) << " bytes\n";

            // Allocate buffer for decompressed data
            std::vector<uint8_t> decompressed(decompressed_size);

            // Decompress
            int result = LZ4_decompress_safe(
                reinterpret_cast<const char*>(payload + size_varint_len),
                reinterpret_cast<char*>(decompressed.data()),
                static_cast<int>(payload_size - size_varint_len),
                static_cast<int>(decompressed_size));

            if (result < 0)
            {
                std::cout << "LZ4 decompression failed with error: " << result
                          << "\n";
                std::cout << "Expected " << decompressed_size
                          << " bytes but got error\n";

                // Show first few bytes of compressed data
                std::cout << "First 16 compressed bytes: ";
                for (size_t i = 0;
                     i < 16 && i < (payload_size - size_varint_len);
                     ++i)
                {
                    std::cout << std::hex << std::setw(2) << std::setfill('0')
                              << static_cast<int>(payload[size_varint_len + i])
                              << " ";
                }
                std::cout << std::dec << "\n";
                return;
            }

            std::cout << "Decompression successful\n\n";

            // Analyze decompressed data
            if (decompressed_size >= 9)
            {
                // Skip first 8 bytes (unused)
                uint8_t node_type = decompressed[8];
                std::cout << "Node type: " << static_cast<int>(node_type)
                          << " (";
                switch (node_type)
                {
                    case 0:
                        std::cout << "hotUNKNOWN";
                        break;
                    case 1:
                        std::cout << "hotLEDGER";
                        break;
                    case 3:
                        std::cout << "hotACCOUNT_NODE";
                        break;
                    case 4:
                        std::cout << "hotTRANSACTION_NODE";
                        break;
                    default:
                        std::cout << "unknown";
                        break;
                }
                std::cout << ")\n";

                // Create a Slice for the actual data (skip 9-byte header)
                size_t data_size = decompressed_size - 9;
                Slice data_slice(decompressed.data() + 9, data_size);

                std::cout << "Data size (after header): " << data_size
                          << " bytes\n";

                // Check if it's the right size for a LedgerInfo
                if (data_size ==
                    catl::common::LedgerInfoView::HEADER_SIZE_WITHOUT_HASH)
                {
                    std::cout << "\nData matches LedgerInfo size! Parsing as "
                                 "LedgerInfo:\n";

                    catl::common::LedgerInfoView ledger_view(
                        data_slice.data(), data_slice.size());
                    catl::common::LedgerInfo ledger_info =
                        ledger_view.to_ledger_info();

                    std::cout << ledger_info.to_string() << std::endl;
                }
                else
                {
                    std::cout << "\nNot a LedgerInfo (expected 118 bytes, got "
                              << data_size << ")\n";

                    // Show first few bytes
                    std::cout << "First 32 bytes of data: ";
                    for (size_t i = 0; i < 32 && i < data_size; ++i)
                    {
                        std::cout
                            << std::hex << std::setw(2) << std::setfill('0')
                            << static_cast<int>(data_slice.data()[i]) << " ";
                    }
                    std::cout << std::dec << "\n";

                    // Check for LWR prefix (XRPL ledger header format)
                    if (data_size >= 4 && data_slice.data()[0] == 'L' &&
                        data_slice.data()[1] == 'W' &&
                        data_slice.data()[2] == 'R')
                    {
                        std::cout << "\nFound 'LWR' prefix - likely XRPL "
                                     "ledger header format\n";
                        std::cout << "After 4-byte prefix, have "
                                  << (data_size - 4) << " bytes\n";

                        // The LedgerInfo should start right after "LWR\0"
                        // And there might be a suffix byte
                        if (data_size - 4 >= catl::common::LedgerInfoView::
                                                 HEADER_SIZE_WITHOUT_HASH)
                        {
                            std::cout
                                << "Enough data for LedgerInfo after prefix\n";
                            // Skip the 4-byte prefix
                            Slice ledger_data_slice(
                                data_slice.data() + 4,
                                catl::common::LedgerInfoView::
                                    HEADER_SIZE_WITHOUT_HASH);

                            catl::common::LedgerInfoView ledger_view(
                                ledger_data_slice.data(),
                                ledger_data_slice.size());
                            catl::common::LedgerInfo ledger_info =
                                ledger_view.to_ledger_info();

                            std::cout << "\nParsed as LedgerInfo:\n";
                            std::cout << ledger_info.to_string() << std::endl;

                            // Check if there's a suffix
                            size_t expected_size = 4 +
                                catl::common::LedgerInfoView::
                                    HEADER_SIZE_WITHOUT_HASH;
                            if (data_size > expected_size)
                            {
                                std::cout << "\nFound "
                                          << (data_size - expected_size)
                                          << " extra byte(s) at end:\n";
                                for (size_t i = expected_size; i < data_size;
                                     ++i)
                                {
                                    std::cout << "Byte " << (i - expected_size)
                                              << ": 0x" << std::hex
                                              << std::setw(2)
                                              << std::setfill('0')
                                              << static_cast<int>(
                                                     data_slice.data()[i])
                                              << std::dec << "\n";
                                }
                            }
                        }
                    }
                }
            }
        }
        else if (compression_type == 2)  // compressed v1 inner node
        {
            std::cout << "Type 2: Compressed inner node\n";
            std::cout << "Payload size: " << payload_size << " bytes\n";

            try
            {
                InnerNode node =
                    InnerNode::from_compressed(payload, payload_size);
                node.display();
            }
            catch (const std::exception& e)
            {
                std::cout << "Error parsing compressed inner node: " << e.what()
                          << "\n";
            }
        }
        else if (compression_type == 3)  // full v1 inner node
        {
            std::cout << "Type 3: Full inner node (uncompressed)\n";
            std::cout << "Payload size: " << payload_size << " bytes\n";

            if (payload_size == 512)
            {
                try
                {
                    InnerNode node =
                        InnerNode::from_full(payload, payload_size);
                    node.display();
                }
                catch (const std::exception& e)
                {
                    std::cout << "Error parsing full inner node: " << e.what()
                              << "\n";
                }
            }
        }
        else
        {
            std::cout << "Compression type " << compression_type
                      << " not implemented\n";
        }
    }

public:
    NudbExplorer(const NudbExpOptions& options) : options_(options)
    {
        // Validate NuDB path exists
        if (!fs::exists(*options_.nudb_path))
        {
            throw std::runtime_error(
                "NuDB path does not exist: " + *options_.nudb_path);
        }

        // Check if database files exist
        fs::path db_path(*options_.nudb_path);
        if (!fs::exists(db_path / "nudb.dat") ||
            !fs::exists(db_path / "nudb.key"))
        {
            throw std::runtime_error(
                "NuDB database files not found in: " + *options_.nudb_path);
        }
    }

    bool
    explore()
    {
        try
        {
            // Open the NuDB store
            store_type db;
            nudb::error_code ec;

            fs::path db_path(*options_.nudb_path);
            db.open(
                (db_path / "nudb.dat").string(),
                (db_path / "nudb.key").string(),
                (db_path / "nudb.log").string(),
                ec);

            if (ec)
            {
                LOGE("Failed to open NuDB database: ", ec.message());
                return false;
            }

            LOGI("Opened NuDB database: ", *options_.nudb_path);

            // Perform requested action
            if (options_.key_hex)
            {
                fetch_key(db, *options_.key_hex);
            }

            if (options_.list_keys)
            {
                list_all_keys(db);
            }

            if (options_.show_stats)
            {
                show_database_stats(db);
            }

            // Close the database
            nudb::error_code close_ec;
            db.close(close_ec);
            if (close_ec)
            {
                LOGW("Error closing database: ", close_ec.message());
            }

            return true;
        }
        catch (const std::exception& e)
        {
            LOGE("Error during exploration: ", e.what());
            return false;
        }
    }

private:
    void
    fetch_key(store_type& db, const std::string& key_hex)
    {
        try
        {
            std::vector<uint8_t> key_bytes = hex_to_bytes(key_hex);

            LOGI("Fetching key: ", key_hex, " (", key_bytes.size(), " bytes)");

            bool found = false;
            std::vector<uint8_t> value_data;

            // Fetch from NuDB
            nudb::error_code ec;
            db.fetch(
                key_bytes.data(),
                [&](void const* data, std::size_t size) {
                    found = true;
                    value_data.resize(size);
                    std::memcpy(value_data.data(), data, size);
                },
                ec);

            if (ec == nudb::error::key_not_found)
            {
                std::cout << "Key not found: " << key_hex << std::endl;
                return;
            }
            else if (ec)
            {
                LOGE("Error fetching key: ", ec.message());
                return;
            }

            // Output based on format
            if (options_.output_format == "hex")
            {
                std::cout << "Key: " << key_hex << std::endl;
                std::cout << "Value (" << value_data.size()
                          << " bytes):" << std::endl;
                std::cout << bytes_to_hex(value_data.data(), value_data.size())
                          << std::endl;
            }
            else if (options_.output_format == "binary")
            {
                // Write raw binary to stdout
                std::cout.write(
                    reinterpret_cast<const char*>(value_data.data()),
                    value_data.size());
            }
            else if (options_.output_format == "info")
            {
                std::cout << "Key: " << key_hex << std::endl;
                std::cout << "Value size: " << value_data.size() << " bytes"
                          << std::endl;
                std::cout << "\n";

                // Try to analyze as compressed node data
                analyze_node_data(value_data.data(), value_data.size());
            }
        }
        catch (const std::exception& e)
        {
            LOGE("Error fetching key ", key_hex, ": ", e.what());
        }
    }

    void
    list_all_keys(store_type& db)
    {
        LOGI("Listing all keys in database...");

        size_t key_count = 0;

        // Close database before visiting
        nudb::error_code close_ec;
        db.close(close_ec);
        if (close_ec)
        {
            LOGW("Error closing database before visit: ", close_ec.message());
        }

        // Visit all entries in the database using file path
        nudb::error_code ec;
        fs::path db_path(*options_.nudb_path);
        nudb::visit(
            (db_path / "nudb.dat").string(),
            [&](void const* key,
                std::size_t key_size,
                void const* /* data */,
                std::size_t data_size,
                nudb::error_code& /* ec */) {
                std::string key_hex =
                    bytes_to_hex(static_cast<const uint8_t*>(key), key_size);
                std::cout << key_hex << " (" << data_size << " bytes)"
                          << std::endl;
                key_count++;
            },
            [](std::uint64_t /* current */, std::uint64_t /* total */) {
                // Progress callback - empty for now
            },
            ec);

        if (ec)
        {
            LOGE("Error visiting database: ", ec.message());
            return;
        }

        std::cout << "\nTotal keys: " << key_count << std::endl;

        // Re-open database for other operations
        db.open(
            (db_path / "nudb.dat").string(),
            (db_path / "nudb.key").string(),
            (db_path / "nudb.log").string(),
            ec);
        if (ec)
        {
            LOGW("Error re-opening database after visit: ", ec.message());
        }
    }

    void
    show_database_stats(store_type& /* db */)
    {
        LOGI("Database statistics:");

        // Read key file header to get stats
        fs::path db_path(*options_.nudb_path);
        fs::path key_file = db_path / "nudb.key";
        fs::path dat_file = db_path / "nudb.dat";

        // File sizes
        auto key_size = fs::file_size(key_file);
        auto dat_size = fs::file_size(dat_file);

        std::cout << "Database location: " << *options_.nudb_path << std::endl;
        std::cout << "Key file size: " << key_size << " bytes" << std::endl;
        std::cout << "Data file size: " << dat_size << " bytes" << std::endl;
        std::cout << "Total size: " << (key_size + dat_size) << " bytes"
                  << std::endl;

        // TODO: Could parse key file header to get more detailed stats
        // like number of buckets, load factor, etc.
    }
};

int
main(int argc, char* argv[])
{
    // Parse command line arguments
    NudbExpOptions options = parse_nudb_exp_argv(argc, argv);

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
        // Set the log level
        if (!Logger::set_level(options.log_level))
        {
            Logger::set_level(LogLevel::INFO);
            std::cerr << "Unrecognized log level: " << options.log_level
                      << ", falling back to 'info'" << std::endl;
        }

        NudbExplorer explorer(options);
        if (explorer.explore())
        {
            return 0;
        }
        else
        {
            return 1;
        }
    }
    catch (const std::exception& e)
    {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
}