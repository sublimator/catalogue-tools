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
    try_load_state_snapshot(const std::string& snapshot_file, Writer& writer)
        const
    {
        if (!fs::exists(snapshot_file))
        {
            LOGI("No start snapshot found, will process all ledgers");
            return false;
        }

        try
        {
            LOGI("Loading state snapshot: ", snapshot_file);

            // Copy the snapshot data directly to the writer's body stream
            size_t bytes_copied =
                copy_snapshot_to_stream(snapshot_file, writer.body_stream());

            LOGI("  Successfully loaded snapshot (", bytes_copied, " bytes)");

            // If we're creating a new snapshot at the end, we need to also
            // load the state map into memory
            if (options_.create_next_slice_state_snapshot &&
                !state_map_->empty())
            {
                LOGI(
                    "  Note: Also need to populate state map for end snapshot");
                // TODO: In a full implementation, we would also populate the
                // state map However, this would require a second pass through
                // the snapshot file
            }

            return true;
        }
        catch (const SnapshotError& e)
        {
            LOGE("Failed to load snapshot: ", e.what());
            return false;
        }
        catch (const std::exception& e)
        {
            LOGE("Failed to load snapshot: ", e.what());
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
    process_pre_slice_ledgers(
        Reader& reader,
        uint32_t min_ledger,
        bool using_snapshot)
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
            LOGI("Read ledger info for ledger: ", ledger_info.sequence);
            LOGD(
                "process_pre_slice_ledgers: Body bytes read after header: ",
                reader.body_bytes_consumed());
            current_ledger = ledger_info.sequence;

            LOGD("process_pre_slice_ledgers: current_ledger: ", current_ledger);

            // Process state map if we haven't reached the start ledger yet
            if (current_ledger < *options_.start_ledger)
            {
                LOGD(
                    "process_pre_slice_ledgers:  Processing state map for "
                    "ledger ",
                    current_ledger);
                // No need to disable tee since we haven't enabled it yet for
                // initial ledgers

                // Process state map using callbacks to update SimpleStateMap
                if (!using_snapshot)
                {
                    LOGI("Processing state map for ledger: ", current_ledger);
                    auto deletes = 0;
                    auto sets = 0;
                    reader.read_map_with_callbacks(
                        SHAMapNodeType::tnACCOUNT_STATE,
                        [this, &sets](
                            const std::vector<uint8_t>& key,
                            const std::vector<uint8_t>& data) {
                            state_map_->set_item(vector_to_hash256(key), data);
                            sets++;
                        },
                        [this, &deletes](const std::vector<uint8_t>& key) {
                            state_map_->remove_item(vector_to_hash256(key));
                            deletes++;
                        });
                    LOGI(
                        "Finished processing state map for ledger: ",
                        current_ledger);
                    LOGI("  Sets: ", sets);
                    LOGI("  Deletes: ", deletes);
                }
                else
                {
                    reader.skip_map(SHAMapNodeType::tnACCOUNT_STATE);
                }

                LOGD(
                    "process_pre_slice_ledgers: Body bytes read after state "
                    "map: ",
                    reader.body_bytes_consumed());

                // Skip the transaction map since we don't need it for state
                // tracking
                reader.skip_map(SHAMapNodeType::tnTRANSACTION_MD);

                LOGD(
                    "process_pre_slice_ledgers: Body bytes read after tx map: ",
                    reader.body_bytes_consumed());
            }

            LOGI(
                "Finished processing initial state for ledger ",
                current_ledger);
            current_ledger =
                ledger_info.sequence + 1;  // increment so loop guard works
        }

        // At this point we've read the header for start_ledger but haven't
        // processed it Perfect position to enable tee and start processing the
        // slice
        LOGI("  Completed building initial state, ready for slice");
    }

    MapOperations
    read_into_account_state_map(Reader& reader)
    {
        return reader.read_map_with_callbacks(
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

    // Process ledgers from start_ledger to end_ledger
    // Important: this assumes the reader is positioned to read the first ledger
    // in the slice
    size_t
    process_slice_ledgers(
        Reader& reader,
        Writer& writer,
        const std::optional<std::string>& snapshot_file)
    {
        LOGI("Beginning slice creation from ledger ", *options_.start_ledger);

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

            // Special handling for the first ledger when using a snapshot
            if (ledgers_processed == 1 &&
                *options_.start_ledger > reader.header().min_ledger)
            {
                reader.disable_tee();

                if (snapshot_file)
                {
                    LOGI("  Using snapshot for state map of first ledger");
                    // Skip the state map in the source file since we are using
                    // the snapshot.Need to temporarily disable tee to avoid
                    // copying the skipped map to the output.
                    reader.skip_map(SHAMapNodeType::tnACCOUNT_STATE);
                    try_load_state_snapshot(*snapshot_file, writer);
                }
                else
                {
                    // we need to copy the state map to the output
                    read_into_account_state_map(reader);
                    write_map_to_stream(*state_map_, writer.body_stream());
                }
                reader.enable_tee(writer.body_stream());
                // Process transaction map (just tee it)
                reader.skip_map(SHAMapNodeType::tnTRANSACTION_MD);
                continue;
            }

            // Process state map - with tee enabled, all data is copied to
            // output
            if (options_.create_next_slice_state_snapshot)
            {
                // Track state changes for snapshot creation
                auto stats = read_into_account_state_map(reader);
                LOGI(
                    "Finished processing state map for ledger: ",
                    current_ledger);
                LOGI("  Sets: ", stats.nodes_added);
                LOGI("  Deletes: ", stats.nodes_deleted);
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
    create_end_snapshot(Reader& reader)
    {
        if (!options_.create_next_slice_state_snapshot ||
            !options_.snapshots_path)
        {
            return;
        }

        const uint32_t next_ledger = *options_.end_ledger + 1;
        fs::path snapshot_path(*options_.snapshots_path);
        fs::path next_snapshot_file = snapshot_path /
            ("state_snapshot_for_ledger_" + std::to_string(next_ledger) +
             ".dat.zst");

        LOGI(
            "Creating state snapshot for ledger ",
            next_ledger,
            ": ",
            next_snapshot_file.string());

        try
        {
            // Check if the next ledger exists in the input file
            if (next_ledger > reader.header().max_ledger)
            {
                throw SnapshotError(
                    "Cannot create snapshot for ledger " +
                    std::to_string(next_ledger) +
                    " because it exceeds the max ledger in the input file (" +
                    std::to_string(reader.header().max_ledger) + ")");
            }

            // We need to read the state delta for the next ledger to create
            // the snapshot for that ledger
            LOGI("  Reading state delta for ledger ", next_ledger);

            // Fetch and apply the next ledger's state delta to our state map
            LedgerInfo next_ledger_info = reader.read_ledger_info();
            if (next_ledger_info.sequence != next_ledger)
            {
                throw SnapshotError(
                    "Expected ledger " + std::to_string(next_ledger) +
                    " but found ledger " +
                    std::to_string(next_ledger_info.sequence));
            }

            // Process state map (apply changes to our state map)
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

            // Skip the transaction map - we don't need it for the snapshot
            reader.skip_map(SHAMapNodeType::tnTRANSACTION_MD);

            LOGI("  State map now contains ", state_map_->size(), " items");

            // Use the new utility function to create the snapshot
            create_state_snapshot(
                *state_map_,
                next_snapshot_file,
                options_.compression_level,
                options_.force_overwrite);

            LOGI("  Snapshot created successfully");
        }
        catch (const SnapshotError& e)
        {
            LOGE("  Failed to create snapshot: ", e.what());
        }
        catch (const std::exception& e)
        {
            LOGE("  Failed to create snapshot: ", e.what());
        }
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

    std::optional<std::string>
    check_snapshot_path() const
    {
        std::optional<std::string> snapshot_file = std::nullopt;
        if (options_.use_start_snapshot && options_.snapshots_path)
        {
            fs::path snapshot_path(*options_.snapshots_path);
            std::string path =
                (snapshot_path /
                 ("state_snapshot_for_ledger_" +
                  std::to_string(*options_.start_ledger) + ".dat.zst"))
                    .string();

            if (boost::filesystem::exists(path))
            {
                snapshot_file = path;
            }
        }
        return snapshot_file;
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
            auto snapshot_file = check_snapshot_path();
            LOGI(
                "Snapshot file: ",
                snapshot_file ? snapshot_file.value() : "None");

            // Process initial ledgers (even if snapshot was loaded, to skip to
            // the first ledger)
            process_pre_slice_ledgers(
                reader, header.min_ledger, snapshot_file != std::nullopt);

            // NOW enable tee since we've reached the start ledger
            // and want to begin copying data to the output
            LOGI(
                "Enabling tee functionality for ledger ",
                *options_.start_ledger);
            reader.enable_tee(writer->body_stream());

            // Process ledgers in the requested slice range
            size_t ledgers_processed =
                process_slice_ledgers(reader, *writer, snapshot_file);

            // We need to disable this before creating the end snapshot which
            // will read more
            reader.disable_tee();

            // Create end snapshot if requested
            create_end_snapshot(reader);

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
