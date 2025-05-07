#include "catl/hasher/arg-options.h"  // Include command-line options
#include "catl/shamap/shamap-errors.h"
#include "catl/shamap/shamap-nodetype.h"
#include "catl/shamap/shamap-options.h"
#include "catl/v1/catl-v1-errors.h"       // Include catalogue error classes
#include "catl/v1/catl-v1-mmap-reader.h"  // Include MmapReader
#include "catl/v1/catl-v1-writer.h"       // Include Writer for slice creation
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>

// For command line parsing

// For crypto

#include "catl/core/log-macros.h"
#include "catl/core/logger.h"
#include "catl/core/types.h"
#include "catl/hasher/catalogue-consts.h"
#include "catl/hasher/http/http-handler.h"
#include "catl/hasher/http/http-server.h"
#include "catl/hasher/ledger.h"
#include "catl/hasher/utils.h"
#include "catl/shamap/shamap.h"

#include "hasher-impl.h"
// TODO: this should be in the shamap library perhaps?
#include "../../shamap/src/pretty-print-json.h"

// Command line parsing
namespace po = boost::program_options;

class CATLHasher
{
private:
    catl::v1::MmapReader reader;
    CATLHeader header;

    // Maps for tracking state
    SHAMap state_map_;
    SHAMap tx_map_;
    std::shared_ptr<LedgerStore> ledger_store_;

    // Statistics
    struct Stats
    {
        uint32_t ledgers_processed = 0;
        uint32_t state_nodes_added = 0;
        uint32_t tx_nodes_added = 0;
        uint32_t state_removals_attempted = 0;
        uint32_t state_removals_succeeded = 0;
        uint32_t successful_hash_verifications = 0;
        uint32_t failed_hash_verifications = 0;
        size_t current_offset = 0;
    } stats;

    void
    validateHeader()
    {
        // MmapReader already validates the header
        stats.current_offset = sizeof(CATLHeader);

#if STOP_AT_LEDGER
        header.max_ledger = STOP_AT_LEDGER;  // TODO: hackery
#endif

        // Log header info at INFO level
        LOGI("CATL Header Validated:");
        std::ostringstream oss_magic_info;
        oss_magic_info << "  Magic: 0x" << std::hex << header.magic << std::dec;
        LOGI(oss_magic_info.str());
        LOGI("  Ledger range: ", header.min_ledger, " - ", header.max_ledger);
        LOGI("  Version: ", (header.version & CATALOGUE_VERSION_MASK));
        LOGI("  Network ID: ", header.network_id);
        LOGI("  Header Filesize: ", header.filesize, " bytes");
    }

    // Debug helper function to print map contents as JSON
    void
    debugMapJson(const SHAMap& map, const std::string& mapTypeName)
    {
        std::ostringstream oss;
        pretty_print_json(oss, map.items_json());
        LOGI(mapTypeName, " MAP JSON: ", oss.str());
    }

    // Process a single ledger
    size_t
    processLedger(size_t offset, LedgerInfoV1& info)
    {
        stats.current_offset = offset;
        size_t initial_offset = offset;

        try
        {
            // Set reader position to the current offset
            reader.set_position(offset);

            // Read ledger info directly from the reader
            info = reader.read_structure<LedgerInfoV1>();
            offset = reader.position();
            stats.current_offset = offset;

            // Sanity check ledger sequence
            if (info.sequence < header.min_ledger ||
                info.sequence > header.max_ledger)
            {
                LOGW(
                    "Ledger sequence ",
                    info.sequence,
                    " is outside the expected range [",
                    header.min_ledger,
                    ", ",
                    header.max_ledger,
                    "] specified in the header.");
            }

            LOGI("--- Processing Ledger ", info.sequence, " ---");
            LOGI("  Ledger Hash:      ", Hash256(info.hash).hex());
            LOGI("  Parent Hash:      ", Hash256(info.parent_hash).hex());
            LOGI("  AccountState Hash:", Hash256(info.account_hash).hex());
            LOGI("  Transaction Hash: ", Hash256(info.tx_hash).hex());
            LOGI(
                "  Close Time:       ",
                utils::format_ripple_time(info.close_time));
            LOGI("  Drops:            ", info.drops);
            LOGI("  Close Flags:      ", info.close_flags);
            LOGI("  Offset at start:  ", initial_offset);

            // Process Account State Map
            bool is_first_ledger = (info.sequence == header.min_ledger);
            if (is_first_ledger)
            {
                LOGI(
                    "Initializing new State SHAMap for first ledger ",
                    info.sequence);
                state_map_ =
                    SHAMap(tnACCOUNT_STATE);  // Recreate for the first ledger
            }
            else
            {
                LOGI("Processing State Map delta for ledger ", info.sequence);
                // stateMap persists from previous ledger
            }

            // Process state map using the new read_shamap method
            LOGI("Processing State Map for ledger ", info.sequence);
            uint32_t state_nodes_processed =
                reader.read_shamap(state_map_, tnACCOUNT_STATE);
            offset = reader.position();
            stats.current_offset = offset;
            stats.state_nodes_added +=
                state_nodes_processed;  // Accumulate stats
            LOGI(
                "  State map processing finished. Nodes processed in this "
                "ledger: ",
                state_nodes_processed,
                ". New offset: ",
                offset);

            // Process Transaction Map (always created fresh for each ledger)
            LOGI("Processing Transaction Map for ledger ", info.sequence);
            tx_map_ = SHAMap(tnTRANSACTION_MD);  // Create fresh transaction map

            // Process transaction map using the new read_shamap method
            uint32_t tx_nodes_processed =
                reader.read_shamap(tx_map_, tnTRANSACTION_MD);
            offset = reader.position();
            stats.current_offset = offset;
            stats.tx_nodes_added += tx_nodes_processed;  // Accumulate stats
            LOGI(
                "  Transaction map processing finished. Nodes processed: ",
                tx_nodes_processed,
                ". Final offset for ledger: ",
                offset);

            // Verify Hashes
            LOGI("Verifying map hashes for ledger ", info.sequence);
            verifyMapHash(
                state_map_,
                Hash256(info.account_hash),
                "AccountState",
                info.sequence);
            verifyMapHash(
                tx_map_, Hash256(info.tx_hash), "Transaction", info.sequence);

            stats.ledgers_processed++;
            return offset;  // Return the final offset for this ledger
        }
        catch (const catl::v1::CatlV1Error& e)
        {
            LOGE(
                "Error processing ledger at offset ",
                initial_offset,
                ": ",
                e.what());
            return initial_offset;  // Return original offset to signal failure
        }
    }

    // Helper to verify map hashes
    void
    verifyMapHash(
        const SHAMap& map,
        const Hash256& expectedHash,
        const std::string& mapType,
        uint32_t ledgerSeq)
    {
        Hash256 computed_hash = map.get_hash();  // getHash is const
        bool hash_match = (computed_hash == expectedHash);

        if (!hash_match)
        {
            LOGW(
                "HASH MISMATCH for ",
                mapType,
                " map in ledger ",
                ledgerSeq,
                "!");
            // Log details only at DEBUG level for performance
            if (Logger::get_level() >= LogLevel::DEBUG)
            {
                LOGD("  Computed Hash: ", computed_hash.hex());
                LOGD("  Expected Hash: ", expectedHash.hex());
            }
            stats.failed_hash_verifications++;
            if ((mapType == "Transaction" && THROW_ON_TX_HASH_MISMATCH) ||
                (mapType == "AccountState" && THROW_ON_AS_HASH_MISMATCH))
            {
                std::ostringstream oss;
                oss << "Hash verification failed for " << mapType
                    << " map in ledger " << ledgerSeq
                    << ". Expected: " << expectedHash.hex()
                    << ", got: " << computed_hash.hex();
                throw catl::v1::CatlV1HashVerificationError(oss.str());
            }
        }
        else
        {
            LOGI(
                "  ",
                mapType,
                " hash verified successfully for ledger ",
                ledgerSeq);
            stats.successful_hash_verifications++;
        }
    }

private:
    // Store the command line options
    catl::hasher::CommandLineOptions options_;

public:
    // Constructor accepts options struct for flexibility
    explicit CATLHasher(
        const std::string& filename,
        const catl::hasher::CommandLineOptions& options)
        : reader(filename)  // Initialize MmapReader with filename
        , state_map_(tnACCOUNT_STATE)
        , tx_map_(tnTRANSACTION_MD)
        , ledger_store_(std::make_shared<LedgerStore>())
        , options_(options)
    {
        try
        {
            // MmapReader already validates the file and header
            LOGI("File opened with MmapReader: ", filename);
            LOGI(
                "File mapped successfully: ",
                filename,
                " (",
                reader.file_size(),
                " bytes)");

            // Copy header from MmapReader (already validated)
            header = reader.header();

            // Log ledger range if specified
            if (options_.first_ledger)
            {
                LOGI(
                    "Will start processing snapshots from ledger ",
                    *options_.first_ledger);
            }

            if (options_.last_ledger)
            {
                LOGI("Will stop processing at ledger ", *options_.last_ledger);
            }
        }
        catch (const catl::v1::CatlV1Error& e)
        {
            LOGE("Error with CATL file: ", e.what());
            throw;  // Re-throw to be caught by main
        }
        catch (const std::exception& e)
        {
            LOGE("Error during file setup '", filename, "': ", e.what());
            throw;  // Re-throw standard exceptions
        }
    }

    // Destructor (MmapReader handles unmapping)
    ~CATLHasher()
    {
        LOGD("CATLHasher destroyed, MmapReader will unmap the file.");
        // MmapReader's destructor handles closing the file
    }

    bool
    processFile()
    {
        LOGI("Starting CATL file processing...");

        try
        {
            // MmapReader already validates this, so this check shouldn't be
            // needed
            if (reader.file_size() == 0)
            {
                LOGE(
                    "No data available to process. File not mapped correctly?");
                return false;
            }

            // Validate header and log information
            validateHeader();

            // Note: MmapReader already validates file size against header value
            // This is just a double-check
            if (header.filesize != reader.file_size())
            {
                LOGW(
                    "File size mismatch: Header reports ",
                    header.filesize,
                    " bytes, actual mapped size is ",
                    reader.file_size(),
                    " bytes. Processing based on actual size.");
                // This warning shouldn't occur since MmapReader validates this
            }

            // Process ledgers starting after the header
            size_t current_file_offset = sizeof(CATLHeader);
            uint32_t expected_ledger_count =
                (header.max_ledger - header.min_ledger + 1);
            LOGI("Expecting ", expected_ledger_count, " ledgers in this file.");

            // Log if we're using a restricted ledger range
            uint32_t effective_min_ledger = header.min_ledger;
            uint32_t effective_max_ledger = header.max_ledger;

            if (options_.first_ledger)
            {
                effective_min_ledger =
                    std::max(effective_min_ledger, *options_.first_ledger);
                LOGI(
                    "Will only store snapshots from ledger ",
                    effective_min_ledger);
            }

            if (options_.last_ledger)
            {
                effective_max_ledger =
                    std::min(effective_max_ledger, *options_.last_ledger);
                LOGI("Will stop processing at ledger ", effective_max_ledger);
            }

            while (current_file_offset < reader.file_size())
            {
                LedgerInfoV1 info = {};
                size_t next_offset = processLedger(current_file_offset, info);

#if STORE_LEDGER_SNAPSHOTS
                // Determine if this ledger should be stored based on range
                // options
                // TODO: articulate why the -1 is needed here exactly
                bool in_snapshot_range =
                    info.sequence >= effective_min_ledger - 1 &&
                    info.sequence <= effective_max_ledger;
                constexpr int every = STORE_LEDGER_SNAPSHOTS_EVERY;

                if (every > 0 && info.sequence % every == 0 &&
                    in_snapshot_range)
                {
                    LOGD(
                        "Creating snapshot for ledger ",
                        info.sequence,
                        " (in requested range)");
                    // Get the ledger data pointer using the reader
                    auto ledger = std::make_shared<Ledger>(
                        reader.data_at(current_file_offset),
                        state_map_.snapshot(),
                        std::make_shared<SHAMap>(tx_map_));
                    ledger_store_->add_ledger(ledger);
                }
#endif
#if COLLAPSE_STATE_MAP
                stateMap.collapse_tree();
#endif
                // Stop processing if we've reached the end of the ledger range
                // or the max requested ledger
                if (info.sequence == header.max_ledger ||
                    (options_.last_ledger &&
                     info.sequence >= *options_.last_ledger))
                {
                    LOGI(
                        "Reached ",
                        (info.sequence == header.max_ledger
                             ? "end of file"
                             : "requested last ledger"),
                        " at sequence ",
                        info.sequence);
                    break;
                }

                if (next_offset == current_file_offset)
                {
                    // No progress was made, indicates an error in processLedger
                    LOGE(
                        "Processing stalled at offset ",
                        current_file_offset,
                        ". Error likely occurred in ledger ",
                        (stats.ledgers_processed > 0
                             ? header.min_ledger + stats.ledgers_processed
                             : header.min_ledger));
                    return false;  // Abort processing
                }
                if (next_offset < current_file_offset)
                {
                    // This should ideally not happen, defensive check
                    LOGE(
                        "Offset went backwards from ",
                        current_file_offset,
                        " to ",
                        next_offset,
                        ". Aborting.");
                    return false;
                }
                current_file_offset = next_offset;
            }

            // Check if we processed up to the expected end of the file
            if (current_file_offset != reader.file_size())
            {
                LOGW(
                    "Processing finished at offset ",
                    current_file_offset,
                    " but file size is ",
                    reader.file_size(),
                    ". Potential trailing data or incomplete processing.");
            }
            else
            {
                LOGI(
                    "Processing reached the end of the mapped file (offset ",
                    current_file_offset,
                    ").");
            }

            // Final Summary using INFO level
            LOGI("--- Processing Summary ---");
            LOGI(
                "Ledgers processed:      ",
                stats.ledgers_processed,
                " (Expected: ",
                expected_ledger_count,
                ")");
            if (stats.ledgers_processed != expected_ledger_count)
            {
                LOGW(
                    "Mismatch between processed ledgers and expected count "
                    "based on header range.");
            }
            LOGI("State map nodes added:  ", stats.state_nodes_added);
            if (stats.state_removals_attempted > 0 ||
                stats.state_removals_succeeded > 0)
            {
                LOGI(
                    "State map removals:   ",
                    stats.state_removals_succeeded,
                    " succeeded out of ",
                    stats.state_removals_attempted,
                    " attempts");
            }
            LOGI("Transaction nodes added:", stats.tx_nodes_added);
            LOGI(
                "Hash Verifications:   ",
                stats.successful_hash_verifications,
                " Succeeded, ",
                stats.failed_hash_verifications,
                " Failed");
            LOGI("--- End Summary ---");

            /// header.min_ledger to header.max_ledger
            for (uint32_t ledgerSeq = header.min_ledger;
                 ledgerSeq <= header.max_ledger;
                 ledgerSeq++)
            {
                if (const auto ledger = ledger_store_->get_ledger(ledgerSeq))
                {
                    auto valid_ledger = ledger->validate();
                    if (!valid_ledger)
                    {
                        LOGE("Ledger Info: ", ledger->header().sequence());
                        LOGE(
                            "State Map hash: ",
                            ledger->getStateMap()->get_hash().hex());
                        LOGE(
                            "Transaction Map hash: ",
                            ledger->getTxMap()->get_hash().hex());

                        throw std::runtime_error(
                            std::string("Invalid ledger: ") +
                            ledger->header().to_string());
                    }
                }
            }

            // Return true if processing completed, potentially with
            // warnings/hash failures Return false only if a fatal error
            // occurred preventing continuation. Consider hash failures fatal?
            // For this tool, maybe not, just report them.
            return true;
        }
        catch (const catl::v1::CatlV1HashVerificationError& e)
        {
            LOGE(
                "Aborting due to hash verification error at offset ~",
                stats.current_offset,
                ": ",
                e.what());
            return false;
        }
        catch (const catl::v1::CatlV1Error& e)
        {
            LOGE(
                "Aborting due to catalogue error at offset ~",
                stats.current_offset,
                ": ",
                e.what());
            return false;
        }
        catch (const SHAMapException& e)
        {
            LOGE(
                "Aborting due to SHAMap error at offset ~",
                stats.current_offset,
                ": ",
                e.what());
            return false;
        }
        catch (const std::exception& e)
        {
            LOGE(
                "Aborting due to standard error at offset ~",
                stats.current_offset,
                ": ",
                e.what());
            return false;
        }
        catch (...)
        {
            LOGE(
                "Aborting due to unknown exception at offset ~",
                stats.current_offset);
            return false;
        }
    }

    void
    startHttpServer() const
    {
        auto handler = std::make_shared<LedgerRequestHandler>(ledger_store_);
        HttpServer http_server(handler);
        http_server.run(8, true);
    }

    bool
    createSliceFile(
        const std::string& output_file,
        uint32_t first_ledger,
        uint32_t last_ledger)
    {
        LOGI("Creating slice file: ", output_file);
        LOGI("Ledger range: ", first_ledger, " - ", last_ledger);

        // Check if output file already exists
        if (boost::filesystem::exists(output_file))
        {
            LOGW("Output file already exists: ", output_file);
            LOGW("This will overwrite the existing file.");
            // In a CLI tool, we would prompt for confirmation here
            // For now, just log a warning and proceed
        }

        try
        {
            // Validate ledger range
            if (first_ledger < header.min_ledger ||
                last_ledger > header.max_ledger)
            {
                LOGE(
                    "Requested ledger range (",
                    first_ledger,
                    "-",
                    last_ledger,
                    ") is outside the available range (",
                    header.min_ledger,
                    "-",
                    header.max_ledger,
                    ")");
                return false;
            }

            // Check that we have all required ledgers in the store
            bool missing_ledgers = false;
            uint32_t missing_count = 0;

            for (uint32_t seq = first_ledger; seq <= last_ledger; seq++)
            {
                if (!ledger_store_->get_ledger(seq))
                {
                    if (!missing_ledgers)
                    {
                        LOGE("Missing ledgers in the store:");
                        missing_ledgers = true;
                    }

                    missing_count++;
                    if (missing_count <= 10)
                    {  // Only show first 10 missing ledgers
                        LOGE("  Missing ledger ", seq);
                    }
                }
            }

            if (missing_ledgers)
            {
                if (missing_count > 10)
                {
                    LOGE(
                        "  ...and ",
                        (missing_count - 10),
                        " more missing ledgers");
                }
                LOGE("Cannot create complete slice due to missing ledgers.");
                LOGW(
                    "This is likely because STORE_LEDGER_SNAPSHOTS_EVERY > 1 "
                    "in hasher-impl.h");
                LOGW(
                    "Set STORE_LEDGER_SNAPSHOTS_EVERY to 1 and reprocess the "
                    "file.");
                return false;
            }

            // Create writer options
            catl::v1::WriterOptions writer_options;
            writer_options.network_id = header.network_id;
            writer_options.compression_level = 0;  // Create uncompressed files

            // Create the writer
            auto writer =
                catl::v1::Writer::for_file(output_file, writer_options);

            // Write the header with the new ledger range
            try
            {
                writer->write_header(first_ledger, last_ledger);
            }
            catch (const catl::v1::CatlV1Error& e)
            {
                LOGE("Failed to write slice file header: ", e.what());
                return false;
            }

            // We need to build the ledgers in sequence
            uint32_t ledgers_written = 0;
            uint32_t total_to_write = last_ledger - first_ledger + 1;
            std::shared_ptr<SHAMap> previous_state_map =
                nullptr;  // Track previous state map for deltas

            for (uint32_t seq = first_ledger; seq <= last_ledger; seq++)
            {
                auto ledger = ledger_store_->get_ledger(seq);

                if (!ledger)
                {
                    LOGE(
                        "Missing ledger ",
                        seq,
                        " in store. Cannot create slice.");
                    return false;
                }

                // Convert Ledger header to LedgerInfo struct for writer
                const auto& header_view = ledger->header();
                catl::v1::LedgerInfo info = {};

                // Copy basic fields
                info.sequence = header_view.sequence();
                info.close_time = header_view.close_time();
                info.drops = header_view.drops();
                info.close_flags = header_view.close_flags();

                // Copy hash fields
                std::memcpy(
                    info.hash, header_view.hash().data(), Hash256::size());
                std::memcpy(
                    info.parent_hash,
                    header_view.parent_hash().data(),
                    Hash256::size());
                std::memcpy(
                    info.account_hash,
                    header_view.account_hash().data(),
                    Hash256::size());
                std::memcpy(
                    info.tx_hash,
                    header_view.transaction_hash().data(),
                    Hash256::size());

                if (seq == first_ledger)
                {
                    // For the first ledger, write the complete ledger with full
                    // state
                    LOGI("Writing complete state for first ledger: ", seq);
                    try
                    {
                        writer->write_ledger(
                            info, *ledger->getStateMap(), *ledger->getTxMap());
                    }
                    catch (const catl::v1::CatlV1Error& e)
                    {
                        LOGE(
                            "Failed to write first ledger ",
                            seq,
                            " to slice file: ",
                            e.what());
                        return false;
                    }

                    // Save state map for delta calculation in next ledger
                    previous_state_map = ledger->getStateMap();
                }
                else
                {
                    // For subsequent ledgers, write header and maps separately
                    // with state map delta from previous ledger
                    LOGI("Writing delta for ledger: ", seq);

                    try
                    {
                        // Write ledger header
                        writer->write_ledger_header(info);

                        // Write state map delta
                        writer->write_map_delta(
                            *previous_state_map,
                            *ledger->getStateMap(),
                            tnACCOUNT_STATE);

                        // Write full transaction map (always unique per ledger)
                        writer->write_map(
                            *(ledger->getTxMap()), tnTRANSACTION_MD);
                    }
                    catch (const catl::v1::CatlV1Error& e)
                    {
                        LOGE(
                            "Failed to write ledger ",
                            seq,
                            " to slice file: ",
                            e.what());
                        return false;
                    }

                    // Update reference to previous state map for next ledger
                    previous_state_map = ledger->getStateMap();
                }

                ledgers_written++;

                // Log progress periodically
                if (ledgers_written % 100 == 0 ||
                    ledgers_written == total_to_write)
                {
                    LOGI(
                        "Wrote ",
                        ledgers_written,
                        "/",
                        total_to_write,
                        " ledgers to slice file");
                }
            }

            // Finalize the file
            try
            {
                writer->finalize();
            }
            catch (const catl::v1::CatlV1Error& e)
            {
                LOGE("Failed to finalize slice file: ", e.what());
                return false;
            }

            LOGI(
                "Successfully created slice file with ",
                ledgers_written,
                " ledgers: ",
                output_file);
            return true;
        }
        catch (const catl::v1::CatlV1Error& e)
        {
            LOGE("Error creating slice file: ", e.what());
            return false;
        }
        catch (const std::exception& e)
        {
            LOGE("Error creating slice file: ", e.what());
            return false;
        }
    }
};

// Main function updated with type-safe argument parsing
int
main(int argc, char* argv[])
{
    // Parse command line arguments with our type-safe function
    catl::hasher::CommandLineOptions options =
        catl::hasher::parse_argv(argc, argv);

    // Handle errors or help request
    if (!options.valid || options.show_help)
    {
        if (options.error_message)
        {
            std::cerr << "Error: " << *options.error_message << std::endl;
        }

        // Display the pre-formatted help text
        std::cout << options.help_text;
        return options.show_help ? 0 : 1;
    }

    // Get input filename from the parsed options
    std::string input_file = *options.input_file;  // Safe to dereference here

    // Set log level based on parsed option
    std::string log_level_str =
        catl::hasher::log_level_to_string(options.log_level);
    if (!Logger::set_level(log_level_str))
    {
        std::cerr << "Warning: Could not set log level to '" << log_level_str
                  << "'. Using default (info)." << std::endl;
    }

    // Start timing
    auto start_time = std::chrono::high_resolution_clock::now();
    int exit_code = 1;  // Default to failure

    std::optional<CATLHasher> hasher = std::nullopt;

    try
    {
        // Create and process with CATLHasher, passing the options struct
        hasher.emplace(input_file, options);
        bool result = hasher->processFile();
        exit_code = result ? 0 : 1;  // 0 on success, 1 on failure
    }
    catch (const std::exception& e)
    {
        // Catch errors during CATLHasher construction
        LOGE("Fatal error during initialization: ", e.what());
        exit_code = 1;
    }
    catch (...)
    {
        LOGE("Caught unknown fatal error during initialization.");
        exit_code = 1;
    }

    // Calculate and display execution time
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time);
    double seconds = duration.count() / 1000.0;

    std::ostringstream time_oss;
    time_oss << "Execution completed in " << std::fixed << std::setprecision(3)
             << seconds << " seconds (" << duration.count() << " ms)";
    LOGW(time_oss.str());

    // Handle slice file creation if requested
    if (hasher && options.slice_file && options.first_ledger &&
        options.last_ledger)
    {
        LOGI("Creating slice file as requested");
        if (hasher->createSliceFile(
                *options.slice_file,
                *options.first_ledger,
                *options.last_ledger))
        {
            LOGI("Slice file creation successful");
        }
        else
        {
            LOGE("Failed to create slice file");
            exit_code = 1;  // Indicate failure
        }
    }

    // Start HTTP server if requested
    if (hasher && options.start_server)
    {
        hasher->startHttpServer();
    }

    return exit_code;
}
