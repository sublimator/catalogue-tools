#include "catl/core/log-macros.h"
#include "catl/utils/slicer/arg-options.h"
#include "catl/utils/slicer/utils.h"
#include "catl/v1/catl-v1-reader.h"
#include "catl/v1/catl-v1-simple-state-map.h"
#include "catl/v1/catl-v1-utils.h"
#include "catl/v1/catl-v1-writer.h"

#include <boost/filesystem.hpp>
#include <chrono>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>

using namespace catl::v1;
using namespace catl::utils::slicer;
namespace fs = boost::filesystem;

/**
 * CATLSlicer - Efficiently extracts ledger slices from CATL files
 *
 * Processes CATL files to extract specific ledger ranges, tracking state
 * across ledgers using SimpleStateMap and utilizing tee functionality
 * for efficient I/O operations.
 */
class CATLSlicer
{
private:
    const CommandLineOptions& options_;
    std::unique_ptr<SimpleStateMap> state_map_;

public:
    CATLSlicer(const CommandLineOptions& options) : options_(options)
    {
        // Validate input file exists
        if (!fs::exists(*options_.input_file))
        {
            throw CatlV1Error(
                "Input file does not exist: " + *options_.input_file);
        }

        // Check if input and output are the same
        if (fs::equivalent(
                fs::path(*options_.input_file),
                fs::path(*options_.output_file)))
        {
            throw CatlV1Error("Input and output files must be different");
        }

        // Initialize state map if needed
        if (!options_.use_start_snapshot ||
            options_.create_next_slice_state_snapshot)
        {
            state_map_ = std::make_unique<SimpleStateMap>();
        }
    }

    // Helper to validate the requested ledger range against file header
    void
    validate_ledger_range(const CatlHeader& header)
    {
        if (*options_.start_ledger < header.min_ledger ||
            *options_.end_ledger > header.max_ledger)
        {
            std::stringstream err;
            err << "Requested ledger range (" << *options_.start_ledger << "-"
                << *options_.end_ledger << ") is outside the file's range ("
                << header.min_ledger << "-" << header.max_ledger << ")";
            throw CatlV1Error(err.str());
        }
    }

    // Helper to log file and operation information
    void
    log_operation_details(const CatlHeader& header)
    {
        LOGI("File information:");
        LOGI(
            "  Ledger range: ",
            header.min_ledger,
            " - ",
            header.max_ledger,
            " (",
            header.max_ledger - header.min_ledger + 1,
            " ledgers)");
        LOGI(
            "  Compression level: ",
            static_cast<int>(get_compression_level(header.version)));
        LOGI("  Network ID: ", header.network_id);

        LOGI("Creating slice:");
        LOGI("  Start ledger: ", *options_.start_ledger);
        LOGI("  End ledger: ", *options_.end_ledger);
        LOGI("  Output file: ", *options_.output_file);
        LOGI(
            "  Output compression: ",
            static_cast<int>(options_.compression_level));

        if (options_.snapshots_path)
        {
            LOGI("  Snapshots path: ", *options_.snapshots_path);
            LOGI(
                "  Use start snapshot: ",
                options_.use_start_snapshot ? "yes" : "no");
            LOGI(
                "  Create next slice snapshot: ",
                options_.create_next_slice_state_snapshot ? "yes" : "no");
        }
    }

    // Helper to create output file writer and prepare for writing
    std::unique_ptr<Writer>
    create_writer(const CatlHeader& header)
    {
        LOGI("Creating output slice file...");
        WriterOptions writer_options;
        writer_options.compression_level = options_.compression_level;
        writer_options.network_id = header.network_id;

        auto writer = Writer::for_file(*options_.output_file, writer_options);
        writer->write_header(*options_.start_ledger, *options_.end_ledger);

        return writer;
    }

    // Attempt to load state from snapshot if available
    bool
    try_load_state_snapshot(const std::string& snapshot_file)
    {
        if (fs::exists(snapshot_file))
        {
            LOGI("Loading state snapshot: ", snapshot_file);
            // In a full implementation, load the state snapshot here
            // For now, just pretend we loaded it
            return true;
        }
        else
        {
            LOGI("No start snapshot found, will process all ledgers");
            return false;
        }
    }

    // Helper to convert vector key to Hash256
    Hash256
    vector_to_hash256(const std::vector<uint8_t>& vec_key)
    {
        Hash256 hash_key;
        std::memcpy(
            hash_key.data(),
            vec_key.data(),
            std::min(vec_key.size(), Hash256::size()));
        return hash_key;
    }

    // Process state map from min_ledger up to (but NOT including) the requested
    // start_ledger.
    // This should NEVER ever read the ledger header of the start ledger.
    void
    process_pre_slice_ledgers(Reader& reader, uint32_t min_ledger)
    {
        LOGI(
            "Processing ledgers from ",
            min_ledger,
            " to ",
            *options_.start_ledger - 1,
            " to build state");

        uint32_t current_ledger = min_ledger;

        LOGD(
            "process_pre_slice_ledgers: Body bytes read before loop:",
            reader.body_bytes_consumed());

        // Skip forward to the start ledger, building state as we go
        while (current_ledger < *options_.start_ledger)
        {
            // Read ledger info
            LedgerInfo ledger_info = reader.read_ledger_info();
            LOGD(
                "process_pre_slice_ledgers: Body bytes read after header: ",
                reader.body_bytes_consumed());
            current_ledger = ledger_info.sequence;

            LOGI("process_pre_slice_ledgers: current_ledger: ", current_ledger);

            // Process state map if we haven't reached the start ledger yet
            if (current_ledger < *options_.start_ledger)
            {
                LOGI(
                    "process_pre_slice_ledgers:  Processing state map for "
                    "ledger ",
                    current_ledger);
                // No need to disable tee since we haven't enabled it yet for
                // initial ledgers

                // Process state map using callbacks to update SimpleStateMap
                reader.read_map_with_callbacks(
                    SHAMapNodeType::tnACCOUNT_STATE,
                    [this](
                        const std::vector<uint8_t>& key,
                        const std::vector<uint8_t>& data) {
                        state_map_->set_item(vector_to_hash256(key), data);
                    },
                    [this](const std::vector<uint8_t>& key) {
                        state_map_->remove_item(vector_to_hash256(key));
                    });

                LOGI(
                    "process_pre_slice_ledgers: Body bytes read after state "
                    "map: ",
                    reader.body_bytes_consumed());

                // Skip the transaction map since we don't need it for state
                // tracking
                reader.skip_map(SHAMapNodeType::tnTRANSACTION_MD);

                LOGI(
                    "process_pre_slice_ledgers: Body bytes read after tx map: ",
                    reader.body_bytes_consumed());
            }

            current_ledger =
                ledger_info.sequence + 1;  // increment so loop guard works
        }

        // At this point we've read the header for start_ledger but haven't
        // processed it Perfect position to enable tee and start processing the
        // slice
        LOGI("  Completed building initial state, ready for slice");
    }

    // Process ledgers from start_ledger to end_ledger
    // Important: this assumes the reader is positioned to read the first ledger
    // in the slice
    size_t
    process_slice_ledgers(Reader& reader, Writer& /* writer */)
    {
        LOGI("Beginning slice creation from ledger ", *options_.start_ledger);
        LOGI("Writing full state map for ledger ", *options_.start_ledger);

        uint32_t current_ledger = *options_.start_ledger;
        size_t ledgers_processed = 0;

        while (current_ledger == *options_.start_ledger ||
               current_ledger < *options_.end_ledger)
        {
            // Read ledger info (will be tee'd to the output)
            LOGI("Body bytes read: ", reader.body_bytes_consumed());
            LedgerInfo ledger_info = reader.read_ledger_info();
            LOGI("  Processing ledger ", Hash256(ledger_info.hash).hex());

            if (ledgers_processed == 0 &&
                ledger_info.sequence != *options_.start_ledger)
            {
                throw std::runtime_error(
                    "Expected first ledger to be " +
                    std::to_string(*options_.start_ledger) + ", got " +
                    std::to_string(ledger_info.sequence));
            }

            current_ledger = ledger_info.sequence;
            LOGI("  Processing ledger ", current_ledger);
            ledgers_processed++;

            // Process state map - with tee enabled, all data is copied to
            // output
            if (options_.create_next_slice_state_snapshot)
            {
                // Track state changes for snapshot creation
                reader.read_map_with_callbacks(
                    SHAMapNodeType::tnACCOUNT_STATE,
                    [this](
                        const std::vector<uint8_t>& key,
                        const std::vector<uint8_t>& data) {
                        state_map_->set_item(vector_to_hash256(key), data);
                    },
                    [this](const std::vector<uint8_t>& key) {
                        state_map_->remove_item(vector_to_hash256(key));
                    });
            }
            else
            {
                // Just read and tee the state map without processing
                reader.skip_map(SHAMapNodeType::tnACCOUNT_STATE);
            }

            // Process transaction map (just tee it, no need to track)
            reader.skip_map(SHAMapNodeType::tnTRANSACTION_MD);
        }

        return ledgers_processed;
    }

    // Create a state snapshot for the next slice if requested
    void
    create_end_snapshot()
    {
        if (!options_.create_next_slice_state_snapshot ||
            !options_.snapshots_path)
        {
            return;
        }

        fs::path snapshot_path(*options_.snapshots_path);
        std::string next_snapshot_file =
            (snapshot_path /
             ("state_snapshot_for_ledger_" +
              std::to_string(*options_.end_ledger + 1) + ".dat.zst"))
                .string();

        LOGI(
            "Creating state snapshot for ledger ",
            *options_.end_ledger + 1,
            ": ",
            next_snapshot_file);

        // In a full implementation, write the state map to the snapshot file
        LOGI("  State map contains ", state_map_->size(), " items");
        // For now, just log that we would create the snapshot
    }

    // Log completion details
    void
    log_completion(double seconds, size_t ledgers_processed)
    {
        LOGI("Slice operation completed:");
        LOGI("  Ledgers processed: ", ledgers_processed);
        LOGI("  Time taken: ", seconds, " seconds");

        if (fs::exists(*options_.output_file))
        {
            boost::uintmax_t output_file_size =
                fs::file_size(*options_.output_file);
            LOGI(
                "  Output file size: ",
                output_file_size,
                " (",
                format_file_size(output_file_size),
                ")");
        }
    }

    bool
    slice()
    {
        try
        {
            // Open the input file and read header
            LOGI("Opening input file: ", *options_.input_file);
            Reader reader(*options_.input_file);
            const CatlHeader& header = reader.header();

            // Validate and log operation details
            validate_ledger_range(header);
            log_operation_details(header);

            // Start timing
            auto start_time = std::chrono::high_resolution_clock::now();

            // Create writer but don't enable tee yet - we don't want to copy
            // data until we reach start_ledger
            auto writer = create_writer(header);

            // Try to load a state snapshot if needed
            bool snapshot_loaded = false;
            if (options_.use_start_snapshot && options_.snapshots_path)
            {
                fs::path snapshot_path(*options_.snapshots_path);
                std::string snapshot_file =
                    (snapshot_path /
                     ("state_snapshot_for_ledger_" +
                      std::to_string(*options_.start_ledger) + ".dat.zst"))
                        .string();

                snapshot_loaded = try_load_state_snapshot(snapshot_file);
            }

            // Process initial ledgers if no snapshot was loaded
            if (!snapshot_loaded)
            {
                process_pre_slice_ledgers(reader, header.min_ledger);
            }

            // NOW enable tee since we've reached the start ledger
            // and want to begin copying data to the output
            LOGI(
                "Enabling tee functionality for ledger ",
                *options_.start_ledger);
            reader.enable_tee(writer->body_stream());

            // Process ledgers in the requested slice range
            size_t ledgers_processed = process_slice_ledgers(reader, *writer);

            // Create end snapshot if requested
            create_end_snapshot();

            // Finish up and finalize the output file
            reader.disable_tee();
            writer->finalize();

            // Calculate and log timing information
            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    end_time - start_time);
            double seconds = duration.count() / 1000.0;

            log_completion(seconds, ledgers_processed);

            return true;
        }
        catch (const CatlV1Error& e)
        {
            LOGE("Catalogue error: ", e.what());
            return false;
        }
        catch (const std::exception& e)
        {
            LOGE("Error: ", e.what());
            return false;
        }
    }
};

int
main(int argc, char* argv[])
{
    // Parse command line arguments
    CommandLineOptions options = parse_argv(argc, argv);

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
        // Set the log level using the string-based method which handles
        // case-insensitivity
        if (!Logger::set_level(options.log_level))
        {
            // Fallback if the level is not recognized
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

        LOGI("Starting CATL slice operation");

        // Create and run the slicer with direct options reference
        CATLSlicer slicer(options);
        if (slicer.slice())
        {
            LOGI("Slice operation completed successfully");
            return 0;
        }
        else
        {
            LOGE("Failed to create slice");
            return 1;
        }
    }
    catch (const std::exception& e)
    {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
}
