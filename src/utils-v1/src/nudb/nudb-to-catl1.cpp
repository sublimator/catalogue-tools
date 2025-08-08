#include "catl/core/log-macros.h"
#include "catl/utils-v1/nudb/nudb-to-catl1-arg-options.h"
#include "catl/v1/catl-v1-writer.h"

#include <boost/filesystem.hpp>
#include <chrono>
#include <iostream>
#include <nudb/basic_store.hpp>
#include <nudb/create.hpp>
#include <nudb/error.hpp>
#include <nudb/posix_file.hpp>
#include <nudb/recover.hpp>
#include <stdexcept>
#include <string>

using namespace catl::v1;
using namespace catl::v1::utils::nudb;
namespace fs = boost::filesystem;

/**
 * NudbToCatl1Converter - Converts NuDB database to CATL v1 format
 *
 * This tool reads ledger data from a NuDB database and creates a CATL file
 * containing the specified range of ledgers. The NuDB database is expected
 * to store ledger data with keys being ledger sequence numbers and values
 * being the serialized ledger data.
 */
class NudbToCatl1Converter
{
private:
    const NudbToCatl1Options& options_;

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
    NudbToCatl1Converter(const NudbToCatl1Options& options) : options_(options)
    {
        // Validate NuDB path exists
        if (!fs::exists(*options_.nudb_path))
        {
            throw std::runtime_error(
                "NuDB path does not exist: " + *options_.nudb_path);
        }
    }

    bool
    convert()
    {
        try
        {
            LOGI("Opening NuDB database: ", *options_.nudb_path);

            // Open the NuDB store
            store_type db;
            nudb::error_code ec;

            db.open(
                *options_.nudb_path + "/nudb.dat",
                *options_.nudb_path + "/nudb.key",
                *options_.nudb_path + "/nudb.log",
                ec);

            if (ec)
            {
                LOGE("Failed to open NuDB database: ", ec.message());
                return false;
            }

            LOGI("Creating output CATL file: ", *options_.output_file);

            // Create the writer
            WriterOptions writer_options;
            writer_options.compression_level = options_.compression_level;
            writer_options.network_id = options_.network_id;

            auto writer =
                Writer::for_file(*options_.output_file, writer_options);
            writer->write_header(*options_.start_ledger, *options_.end_ledger);

            // Start timing
            auto start_time = std::chrono::high_resolution_clock::now();

            size_t ledgers_processed = 0;

            // Process ledgers in the requested range
            for (uint32_t seq = *options_.start_ledger;
                 seq <= *options_.end_ledger;
                 ++seq)
            {
                // Create key from ledger sequence
                std::vector<uint8_t> key(sizeof(uint32_t));
                std::memcpy(key.data(), &seq, sizeof(uint32_t));

                // Fetch from NuDB
                nudb::error_code nec;
                db.fetch(
                    key.data(),
                    [&](void const* data, std::size_t size) {
                        // Write the ledger data to CATL
                        writer->body_stream().write(
                            static_cast<const char*>(data), size);
                    },
                    nec);

                if (nec)
                {
                    LOGW("Ledger ", seq, " not found in NuDB database");
                    // You might want to handle missing ledgers differently
                    continue;
                }

                ledgers_processed++;

                if (ledgers_processed % 1000 == 0)
                {
                    LOGI("Processed ", ledgers_processed, " ledgers...");
                }
            }

            writer->finalize();

            // Calculate timing
            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    end_time - start_time);
            double seconds = duration.count() / 1000.0;

            LOGI("Conversion completed successfully:");
            LOGI("  Ledgers processed: ", ledgers_processed);
            LOGI("  Time taken: ", seconds, " seconds");

            if (fs::exists(*options_.output_file))
            {
                boost::uintmax_t output_file_size =
                    fs::file_size(*options_.output_file);
                LOGI("  Output file size: ", output_file_size, " bytes");
            }

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
    NudbToCatl1Options options = parse_nudb_to_catl1_argv(argc, argv);

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

        // Check if output file already exists
        if (fs::exists(*options.output_file) && !options.force_overwrite)
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

        LOGI("Starting NuDB to CATL conversion");

        NudbToCatl1Converter converter(options);
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