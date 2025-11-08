#include "catl/common/ledger-info.h"
#include "catl/core/log-macros.h"
#include "catl/core/types.h"
#include "catl/nodestore/backend.h"
#include "catl/nodestore/node_blob.h"
#include "catl/nodestore/tree_walker.h"
#include "catl/utils-v1/nudb/nudb-exp-arg-options.h"
#include "catl/xdata-json/parse_leaf.h"
#include "catl/xdata-json/parse_transaction.h"
#include "catl/xdata-json/pretty_print.h"
#include "catl/xdata/protocol.h"

#include <boost/filesystem.hpp>
#include <boost/json.hpp>
#include <iomanip>
#include <iostream>
#include <lz4.h>
#include <nudb/basic_store.hpp>
#include <nudb/detail/buffer.hpp>
#include <nudb/posix_file.hpp>
#include <nudb/visit.hpp>
#include <nudb/xxhasher.hpp>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

using namespace catl::v1::utils::nudb;
namespace fs = boost::filesystem;

/**
 * Load protocol definitions based on network ID
 */
catl::xdata::Protocol
load_protocol(uint32_t network_id)
{
    if (network_id == 0)  // XRPL
    {
        LOGI(
            "Using embedded XRPL protocol definitions (network ID ",
            network_id,
            ")");
        return catl::xdata::Protocol::load_embedded_xrpl_protocol();
    }
    else if (network_id == 21337)  // XAHAU
    {
        LOGI(
            "Using embedded Xahau protocol definitions (network ID ",
            network_id,
            ")");
        return catl::xdata::Protocol::load_embedded_xahau_protocol();
    }
    else
    {
        LOGW(
            "Unknown network ID ",
            network_id,
            " - using Xahau protocol definitions");
        return catl::xdata::Protocol::load_embedded_xahau_protocol();
    }
}

/**
 * NuDB Backend implementation for nodestore::Backend interface.
 * Wraps a NuDB store for tree walking operations.
 */
class NudbBackend : public catl::nodestore::Backend
{
private:
    using store_type = nudb::basic_store<nudb::xxhasher, nudb::posix_file>;
    store_type& db_;

public:
    explicit NudbBackend(store_type& db) : db_(db)
    {
    }

    std::optional<catl::nodestore::node_blob>
    get(Hash256 const& key) override
    {
        nudb::error_code ec;
        catl::nodestore::node_blob compressed;
        bool found = false;

        db_.fetch(
            key.data(),
            [&](void const* data, std::size_t size) {
                compressed.data.assign(
                    static_cast<uint8_t const*>(data),
                    static_cast<uint8_t const*>(data) + size);
                found = true;
            },
            ec);

        if (ec || !found)
        {
            return std::nullopt;  // Key not found
        }

        return compressed;
    }

    void
    store(Hash256 const& /*key*/, catl::nodestore::node_blob const& /*blob*/)
        override
    {
        // Not implemented for read-only exploration
        throw std::runtime_error("NudbBackend: store() not implemented");
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
    catl::xdata::Protocol protocol_;

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

    // Convert bytes to hex string (uppercase)
    std::string
    bytes_to_hex(const uint8_t* data, size_t size)
    {
        std::stringstream ss;
        ss << std::hex << std::uppercase << std::setfill('0');
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

    // Decompress and analyze node data using node_blob helpers
    void
    analyze_node_data(const uint8_t* compressed_data, size_t compressed_size)
    {
        if (compressed_size < 9)
        {
            std::cout << "Data too small (< 9 bytes for header)\n";
            return;
        }

        try
        {
            // Create compressed blob from NuDB data
            catl::nodestore::node_blob compressed_blob;
            compressed_blob.data.assign(
                compressed_data, compressed_data + compressed_size);

            // Get node type from compressed blob
            auto node_type = compressed_blob.get_type();
            std::cout << "Node type: " << static_cast<int>(node_type) << " ("
                      << catl::nodestore::node_type_to_string(node_type)
                      << ")\n";

            // Decompress using node_blob helper
            catl::nodestore::node_blob decompressed_blob =
                catl::nodestore::nodeobject_decompress(compressed_blob);

            // Get the decompressed payload (after 9-byte header)
            auto payload = decompressed_blob.payload();
            std::cout << "Decompressed payload size: " << payload.size()
                      << " bytes\n\n";

            // Check if this is a ledger header (hot_ledger type)
            if (node_type == catl::nodestore::node_type::hot_ledger)
            {
                // Ledger headers are 122 bytes: 4-byte LWR prefix + 118-byte
                // canonical
                if (payload.size() == 122)
                {
                    // Check for LWR prefix
                    if (payload[0] == 'L' && payload[1] == 'W' &&
                        payload[2] == 'R' && payload[3] == 0)
                    {
                        std::cout << "Found 'LWR\\0' prefix - XRPL ledger "
                                     "header format\n\n";

                        // Parse the canonical ledger info (skip 4-byte prefix)
                        Slice ledger_data_slice(
                            payload.data() + 4,
                            catl::common::LedgerInfoView::
                                HEADER_SIZE_WITHOUT_HASH);

                        catl::common::LedgerInfoView ledger_view(
                            ledger_data_slice.data(), ledger_data_slice.size());
                        catl::common::LedgerInfo ledger_info =
                            ledger_view.to_ledger_info();

                        std::cout << "Parsed LedgerInfo:\n";
                        std::cout << ledger_info.to_string() << std::endl;
                    }
                    else
                    {
                        std::cout << "Expected LWR prefix but got: " << std::hex
                                  << std::setw(2) << std::setfill('0')
                                  << static_cast<int>(payload[0]) << " "
                                  << static_cast<int>(payload[1]) << " "
                                  << static_cast<int>(payload[2]) << " "
                                  << static_cast<int>(payload[3]) << std::dec
                                  << "\n";
                    }
                }
                else
                {
                    std::cout
                        << "Unexpected ledger header size: " << payload.size()
                        << " bytes (expected 122)\n";

                    // Show first 32 bytes
                    std::cout << "First 32 bytes: ";
                    for (size_t i = 0; i < 32 && i < payload.size(); ++i)
                    {
                        std::cout << std::hex << std::setw(2)
                                  << std::setfill('0')
                                  << static_cast<int>(payload[i]) << " ";
                    }
                    std::cout << std::dec << "\n";
                }
            }
            else
            {
                // Not a ledger header - just show hex dump
                std::cout << "First 64 bytes of payload:\n";
                for (size_t i = 0; i < 64 && i < payload.size(); ++i)
                {
                    if (i > 0 && i % 32 == 0)
                        std::cout << "\n";
                    std::cout << std::hex << std::setw(2) << std::setfill('0')
                              << static_cast<int>(payload[i]) << " ";
                }
                std::cout << std::dec << "\n";
            }
        }
        catch (const std::exception& e)
        {
            std::cout << "Error decompressing node: " << e.what() << "\n";
        }
    }

public:
    NudbExplorer(const NudbExpOptions& options)
        : options_(options), protocol_(load_protocol(options.network_id))
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

            // Tree walking operations
            if (options_.ledger_hash)
            {
                if (options_.state_key)
                {
                    walk_to_state_key(
                        db, *options_.ledger_hash, *options_.state_key);
                }
                else if (options_.tx_key)
                {
                    walk_to_tx_key(db, *options_.ledger_hash, *options_.tx_key);
                }
                else if (options_.walk_tx)
                {
                    walk_all_tx(db, *options_.ledger_hash);
                }
                else if (options_.walk_state)
                {
                    walk_all_state(db, *options_.ledger_hash);
                }
                else
                {
                    std::cout << "Error: --ledger-hash requires either "
                                 "--state-key, --tx-key, --walk-tx, or "
                                 "--walk-state\n";
                }
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
    walk_to_state_key(
        store_type& db,
        const std::string& ledger_hash_hex,
        const std::string& state_key_hex)
    {
        try
        {
            std::vector<uint8_t> ledger_hash_bytes =
                hex_to_bytes(ledger_hash_hex);
            std::vector<uint8_t> state_key_bytes = hex_to_bytes(state_key_hex);

            if (ledger_hash_bytes.size() != 32 || state_key_bytes.size() != 32)
            {
                std::cout << "Error: hashes must be 32 bytes (64 hex chars)\n";
                return;
            }

            Hash256 ledger_hash(ledger_hash_bytes.data());
            Hash256 state_key(state_key_bytes.data());

            std::cout << "Walking account tree for ledger: "
                      << ledger_hash.hex() << "\n";
            std::cout << "Looking for state key: " << state_key.hex() << "\n\n";

            // Fetch ledger header
            auto ledger_blob_opt = fetch_ledger_header(db, ledger_hash);
            if (!ledger_blob_opt)
            {
                std::cout << "Ledger header not found\n";
                return;
            }

            // Parse ledger header to get account_hash
            auto payload = ledger_blob_opt->payload();
            if (payload.size() != 122)
            {
                std::cout << "Invalid ledger header size: " << payload.size()
                          << "\n";
                return;
            }

            // Use LedgerInfoView to parse the canonical format (skip 4-byte
            // "LWR\0" prefix)
            catl::common::LedgerInfoView ledger_view(
                payload.data() + 4,
                catl::common::LedgerInfoView::HEADER_SIZE_WITHOUT_HASH);
            Hash256 account_hash = ledger_view.account_hash();
            std::cout << "Account tree root hash: " << account_hash.hex()
                      << "\n\n";

            // Create backend and walker
            NudbBackend backend(db);
            catl::nodestore::TreeWalker walker(backend);

            // Walk the tree
            auto result = walker.walk(account_hash, state_key);

            std::cout << "Tree walk result:\n";
            std::cout << "  Found: " << (result.found ? "YES" : "NO") << "\n";
            std::cout << "  Depth: " << result.depth << "\n";
            std::cout << "  Path length: " << result.path.size() << "\n";

            if (result.found)
            {
                std::cout << "\nLeaf node found!\n";
                std::cout << "Node type: "
                          << static_cast<int>(result.blob.get_type()) << "\n";
                std::cout << "Payload size: " << result.blob.payload().size()
                          << " bytes\n";

                // Output JSON if requested
                if (options_.output_format == "json")
                {
                    try
                    {
                        auto payload_span = result.blob.payload();
                        Slice payload_slice(
                            payload_span.data(), payload_span.size());
                        auto json_result = catl::xdata::json::parse_leaf(
                            payload_slice, protocol_);

                        std::cout << "\nParsed JSON:\n";
                        catl::xdata::json::pretty_print(std::cout, json_result);
                    }
                    catch (const std::exception& e)
                    {
                        std::cout << "Failed to parse as JSON: " << e.what()
                                  << "\n";
                    }
                }
            }
            else
            {
                std::cout << "\nKey not found in tree\n";
            }
        }
        catch (const std::exception& e)
        {
            std::cout << "Error during tree walk: " << e.what() << "\n";
        }
    }

    void
    walk_to_tx_key(
        store_type& db,
        const std::string& ledger_hash_hex,
        const std::string& tx_key_hex)
    {
        try
        {
            std::vector<uint8_t> ledger_hash_bytes =
                hex_to_bytes(ledger_hash_hex);
            std::vector<uint8_t> tx_key_bytes = hex_to_bytes(tx_key_hex);

            if (ledger_hash_bytes.size() != 32 || tx_key_bytes.size() != 32)
            {
                std::cout << "Error: hashes must be 32 bytes (64 hex chars)\n";
                return;
            }

            Hash256 ledger_hash(ledger_hash_bytes.data());
            Hash256 tx_key(tx_key_bytes.data());

            std::cout << "Walking transaction tree for ledger: "
                      << ledger_hash.hex() << "\n";
            std::cout << "Looking for tx key: " << tx_key.hex() << "\n\n";

            // Fetch ledger header
            auto ledger_blob_opt = fetch_ledger_header(db, ledger_hash);
            if (!ledger_blob_opt)
            {
                std::cout << "Ledger header not found\n";
                return;
            }

            // Parse ledger header to get tx_hash
            auto payload = ledger_blob_opt->payload();
            if (payload.size() != 122)
            {
                std::cout << "Invalid ledger header size: " << payload.size()
                          << "\n";
                return;
            }

            // Use LedgerInfoView to parse the canonical format (skip 4-byte
            // "LWR\0" prefix)
            catl::common::LedgerInfoView ledger_view(
                payload.data() + 4,
                catl::common::LedgerInfoView::HEADER_SIZE_WITHOUT_HASH);
            Hash256 tx_hash = ledger_view.tx_hash();
            std::cout << "Transaction tree root hash: " << tx_hash.hex()
                      << "\n\n";

            // Create backend and walker
            NudbBackend backend(db);
            catl::nodestore::TreeWalker walker(backend);

            // Walk the tree
            auto result = walker.walk(tx_hash, tx_key);

            std::cout << "Tree walk result:\n";
            std::cout << "  Found: " << (result.found ? "YES" : "NO") << "\n";
            std::cout << "  Depth: " << result.depth << "\n";
            std::cout << "  Path length: " << result.path.size() << "\n";

            if (result.found)
            {
                std::cout << "\nLeaf node found!\n";
                std::cout << "Node type: "
                          << static_cast<int>(result.blob.get_type()) << "\n";
                std::cout << "Payload size: " << result.blob.payload().size()
                          << " bytes\n";

                // Output JSON if requested
                if (options_.output_format == "json")
                {
                    try
                    {
                        auto payload_span = result.blob.payload();
                        Slice payload_slice(
                            payload_span.data(), payload_span.size());
                        auto json_result = catl::xdata::json::parse_transaction(
                            payload_slice, protocol_);

                        std::cout << "\nParsed JSON:\n";
                        catl::xdata::json::pretty_print(std::cout, json_result);
                    }
                    catch (const std::exception& e)
                    {
                        std::cout << "Failed to parse as JSON: " << e.what()
                                  << "\n";
                    }
                }
            }
            else
            {
                std::cout << "\nKey not found in tree\n";
            }
        }
        catch (const std::exception& e)
        {
            std::cout << "Error during tree walk: " << e.what() << "\n";
        }
    }

    void
    walk_all_state(store_type& db, const std::string& ledger_hash_hex)
    {
        try
        {
            std::vector<uint8_t> ledger_hash_bytes =
                hex_to_bytes(ledger_hash_hex);

            if (ledger_hash_bytes.size() != 32)
            {
                std::cout
                    << "Error: ledger hash must be 32 bytes (64 hex chars)\n";
                return;
            }

            Hash256 ledger_hash(ledger_hash_bytes.data());

            std::cout << "Walking all account states for ledger: "
                      << ledger_hash.hex() << "\n\n";

            // Fetch ledger header
            auto ledger_blob_opt = fetch_ledger_header(db, ledger_hash);
            if (!ledger_blob_opt)
            {
                std::cout << "Ledger header not found\n";
                return;
            }

            // Parse ledger header to get account_hash
            auto payload = ledger_blob_opt->payload();
            if (payload.size() != 122)
            {
                std::cout << "Invalid ledger header size: " << payload.size()
                          << "\n";
                return;
            }

            // Use LedgerInfoView to parse the canonical format
            catl::common::LedgerInfoView ledger_view(
                payload.data() + 4,
                catl::common::LedgerInfoView::HEADER_SIZE_WITHOUT_HASH);
            Hash256 account_hash = ledger_view.account_hash();
            std::cout << "Account state tree root hash: " << account_hash.hex()
                      << "\n\n";

            // Create backend and walker
            NudbBackend backend(db);
            catl::nodestore::TreeWalker walker(backend);

            // Counter for account states
            size_t state_count = 0;

            // Configure walk options
            catl::nodestore::WalkOptions walk_opts;
            walk_opts.parallel = options_.parallel;
            walk_opts.num_threads = 8;

            // Walk all account states
            walker.walk_all(
                account_hash,
                [&](Hash256 const& hash,
                    catl::nodestore::node_blob const& blob) {
                    ++state_count;

                    LOGD("Account State #", state_count);
                    LOGD("  Hash: ", hash.hex());
                    LOGD("  Payload size: ", blob.payload().size(), " bytes");

                    // Parse JSON if requested (to test speed)
                    if (options_.output_format == "json")
                    {
                        try
                        {
                            auto payload_span = blob.payload();
                            Slice payload_slice(
                                payload_span.data(), payload_span.size());
                            auto json_result = catl::xdata::json::parse_leaf(
                                payload_slice, protocol_);

                            // Pretty print to string first, then log
                            std::ostringstream json_stream;
                            catl::xdata::json::pretty_print(
                                json_stream, json_result);
                            LOGD(json_stream.str());
                        }
                        catch (const std::exception& e)
                        {
                            LOGD("  Failed to parse as JSON: ", e.what());
                        }
                    }

                    LOGD("");
                },
                walk_opts);

            std::cout << "Total account states: " << state_count << "\n";
        }
        catch (const std::exception& e)
        {
            std::cout << "Error during account state walk: " << e.what()
                      << "\n";
        }
    }

    void
    walk_all_tx(store_type& db, const std::string& ledger_hash_hex)
    {
        try
        {
            std::vector<uint8_t> ledger_hash_bytes =
                hex_to_bytes(ledger_hash_hex);

            if (ledger_hash_bytes.size() != 32)
            {
                std::cout
                    << "Error: ledger hash must be 32 bytes (64 hex chars)\n";
                return;
            }

            Hash256 ledger_hash(ledger_hash_bytes.data());

            std::cout << "Walking all transactions for ledger: "
                      << ledger_hash.hex() << "\n\n";

            // Fetch ledger header
            auto ledger_blob_opt = fetch_ledger_header(db, ledger_hash);
            if (!ledger_blob_opt)
            {
                std::cout << "Ledger header not found\n";
                return;
            }

            // Parse ledger header to get tx_hash
            auto payload = ledger_blob_opt->payload();
            if (payload.size() != 122)
            {
                std::cout << "Invalid ledger header size: " << payload.size()
                          << "\n";
                return;
            }

            // Use LedgerInfoView to parse the canonical format
            catl::common::LedgerInfoView ledger_view(
                payload.data() + 4,
                catl::common::LedgerInfoView::HEADER_SIZE_WITHOUT_HASH);
            Hash256 tx_hash = ledger_view.tx_hash();
            std::cout << "Transaction tree root hash: " << tx_hash.hex()
                      << "\n\n";

            // Create backend and walker
            NudbBackend backend(db);
            catl::nodestore::TreeWalker walker(backend);

            // Counter for transactions
            size_t tx_count = 0;

            // Configure walk options
            catl::nodestore::WalkOptions walk_opts;
            walk_opts.parallel = options_.parallel;
            walk_opts.num_threads = 8;

            // Walk all transactions
            walker.walk_all(
                tx_hash,
                [&](Hash256 const& hash,
                    catl::nodestore::node_blob const& blob) {
                    ++tx_count;

                    LOGD("Transaction #", tx_count);
                    LOGD("  Hash: ", hash.hex());
                    LOGD("  Payload size: ", blob.payload().size(), " bytes");

                    // Parse JSON if requested (to test speed)
                    if (options_.output_format == "json")
                    {
                        try
                        {
                            auto payload_span = blob.payload();
                            Slice payload_slice(
                                payload_span.data(), payload_span.size());
                            auto json_result =
                                catl::xdata::json::parse_transaction(
                                    payload_slice, protocol_);

                            // Pretty print to string first, then log
                            std::ostringstream json_stream;
                            catl::xdata::json::pretty_print(
                                json_stream, json_result);
                            LOGD(json_stream.str());
                        }
                        catch (const std::exception& e)
                        {
                            LOGD("  Failed to parse as JSON: ", e.what());
                        }
                    }

                    LOGD("");
                },
                walk_opts);

            std::cout << "Total transactions: " << tx_count << "\n";
        }
        catch (const std::exception& e)
        {
            std::cout << "Error during transaction walk: " << e.what() << "\n";
        }
    }

    std::optional<catl::nodestore::node_blob>
    fetch_ledger_header(store_type& db, Hash256 const& ledger_hash)
    {
        nudb::error_code ec;
        catl::nodestore::node_blob compressed;
        bool found = false;

        db.fetch(
            ledger_hash.data(),
            [&](void const* data, std::size_t size) {
                compressed.data.assign(
                    static_cast<uint8_t const*>(data),
                    static_cast<uint8_t const*>(data) + size);
                found = true;
            },
            ec);

        if (ec || !found)
        {
            return std::nullopt;
        }

        // Decompress
        return catl::nodestore::nodeobject_decompress(compressed);
    }

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

        // Enable TREE_WALK partition for debugging tree operations
        catl::nodestore::TreeWalker::get_log_partition().set_level(
            LogLevel::DEBUG);

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