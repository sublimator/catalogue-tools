#include "catl/core/log-macros.h"
#include "catl/utils-v1/nudb/catl1-to-nudb-arg-options.h"
#include "catl/v1/catl-v1-reader.h"

#include <boost/filesystem.hpp>
#include <chrono>
#include <iostream>
#include <nudb/basic_store.hpp>
#include <nudb/create.hpp>
#include <nudb/error.hpp>
#include <nudb/posix_file.hpp>
#include <stdexcept>
#include <string>

using namespace catl::v1;
using namespace catl::v1::utils::nudb;
namespace fs = boost::filesystem;

/**
 * Catl1ToNudbConverter - Converts CATL v1 files to NuDB database format
 *
 * This tool reads ledger data from a CATL file and stores it in a NuDB
 * database for efficient key-value lookups. The database uses ledger
 * sequence numbers as keys and stores the serialized ledger data as values.
 */
class Catl1ToNudbConverter
{
private:
    const Catl1ToNudbOptions& options_;

    // NuDB uses beast::hash_append
    struct nudb_hasher
    {
        using result_type = std::size_t;

        explicit nudb_hasher(std::size_t salt = 0) : salt_(salt)
        {
        }

        template <class T>
        result_type
        operator()(T const* key, std::size_t len) const noexcept
        {
            result_type seed = salt_;
            const uint8_t* data = reinterpret_cast<const uint8_t*>(key);
            for (std::size_t i = 0; i < len; ++i)
            {
                seed ^= data[i] + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            }
            return seed;
        }

    private:
        std::size_t salt_;
    };

    using store_type = nudb::basic_store<nudb_hasher, nudb::posix_file>;

public:
    Catl1ToNudbConverter(const Catl1ToNudbOptions& options) : options_(options)
    {
        // Validate input file exists
        if (!fs::exists(*options_.input_file))
        {
            throw std::runtime_error(
                "Input file does not exist: " + *options_.input_file);
        }
    }

    bool
    convert()
    {
        try
        {
            // Open the input CATL file
            LOGI("Opening input file: ", *options_.input_file);
            Reader reader(*options_.input_file);
            const CatlHeader& header = reader.header();

            // Determine ledger range to process
            uint32_t start_ledger =
                options_.start_ledger.value_or(header.min_ledger);
            uint32_t end_ledger =
                options_.end_ledger.value_or(header.max_ledger);

            // Validate range
            if (start_ledger < header.min_ledger ||
                end_ledger > header.max_ledger)
            {
                LOGE(
                    "Requested ledger range (",
                    start_ledger,
                    "-",
                    end_ledger,
                    ") is outside file's range (",
                    header.min_ledger,
                    "-",
                    header.max_ledger,
                    ")");
                return false;
            }

            LOGI("File information:");
            LOGI(
                "  Ledger range: ",
                header.min_ledger,
                " - ",
                header.max_ledger);
            LOGI("  Network ID: ", header.network_id);
            LOGI("Processing ledgers ", start_ledger, " to ", end_ledger);

            // Create or open NuDB database
            fs::path db_path(*options_.nudb_path);
            bool db_exists = fs::exists(db_path / "nudb.dat");

            if (db_exists && !options_.force_overwrite)
            {
                std::cout
                    << "Warning: Database already exists. Overwrite? (y/n): ";
                char response;
                std::cin >> response;
                if (response != 'y' && response != 'Y')
                {
                    std::cout << "Operation canceled by user." << std::endl;
                    return 0;
                }

                // Remove existing database files
                fs::remove(db_path / "nudb.dat");
                fs::remove(db_path / "nudb.key");
                fs::remove(db_path / "nudb.log");
                db_exists = false;
            }

            if (!db_exists && options_.create_database)
            {
                LOGI("Creating new NuDB database at: ", *options_.nudb_path);

                // Create directory if it doesn't exist
                if (!fs::exists(db_path))
                {
                    fs::create_directories(db_path);
                }

                // Calculate approximate number of records
                uint64_t num_records = end_ledger - start_ledger + 1;

                // Create the database
                nudb::error_code nec;
                nudb::create<nudb_hasher, nudb::posix_file>(
                    (db_path / "nudb.dat").string(),
                    (db_path / "nudb.key").string(),
                    (db_path / "nudb.log").string(),
                    1,  // appnum
                    nudb::make_salt(),
                    options_.key_size,
                    options_.block_size,
                    options_.load_factor,
                    nec);

                if (nec)
                {
                    LOGE("Failed to create NuDB database: ", nec.message());
                    return false;
                }
            }

            // Open the NuDB store
            store_type db;
            nudb::error_code ec;

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

            // Start timing
            auto start_time = std::chrono::high_resolution_clock::now();

            size_t ledgers_processed = 0;

            // Process ledgers
            // Note: This is a simplified implementation. In practice, you would
            // need to properly parse the CATL file format and extract
            // individual ledgers with their data.
            LOGI("Processing ledgers...");

            // TODO: Implement actual CATL file parsing and ledger extraction
            // For now, this is just a skeleton showing the structure

            /*
            while (current_ledger <= end_ledger)
            {
                // Read ledger data from CATL file
                LedgerInfo ledger_info = reader.read_ledger_info();

                // Create key from ledger sequence
                std::vector<uint8_t> key(sizeof(uint32_t));
                std::memcpy(key.data(), &ledger_info.sequence,
            sizeof(uint32_t));

                // Read ledger data (this would need proper implementation)
                std::vector<uint8_t> ledger_data;
                // ... read state map and transaction map data ...

                // Insert into NuDB
                nudb::error_code nec;
                db.insert(key.data(), key.size(),
                         ledger_data.data(), ledger_data.size(), nec);

                if (nec)
                {
                    LOGE("Failed to insert ledger ", ledger_info.sequence,
                         ": ", nec.message());
                    continue;
                }

                ledgers_processed++;

                if (ledgers_processed % 1000 == 0)
                {
                    LOGI("Processed ", ledgers_processed, " ledgers...");
                }
            }
            */

            LOGW(
                "Note: This is a skeleton implementation. Actual CATL parsing "
                "needs to be implemented.");

            // Close the database
            nudb::error_code close_ec;
            db.close(close_ec);
            if (close_ec)
            {
                LOGW("Error closing database: ", close_ec.message());
            }

            // Calculate timing
            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    end_time - start_time);
            double seconds = duration.count() / 1000.0;

            LOGI("Conversion completed:");
            LOGI("  Ledgers processed: ", ledgers_processed);
            LOGI("  Time taken: ", seconds, " seconds");
            LOGI("  Database location: ", *options_.nudb_path);

            return true;
        }
        catch (const std::exception& e)
        {
            LOGE("Error during conversion: ", e.what());
            return false;
        }
    }
};

int
main(int argc, char* argv[])
{
    // Parse command line arguments
    Catl1ToNudbOptions options = parse_catl1_to_nudb_argv(argc, argv);

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

        LOGI("Starting CATL to NuDB conversion");

        Catl1ToNudbConverter converter(options);
        if (converter.convert())
        {
            LOGI("Conversion completed successfully");
            return 0;
        }
        else
        {
            LOGE("Conversion failed");
            return 1;
        }
    }
    catch (const std::exception& e)
    {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
}