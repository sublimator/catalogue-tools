#include "catl/shamap/shamap-errors.h"
#include "catl/shamap/shamap-nodetype.h"
#include "catl/shamap/shamap-options.h"
#include "catl/v1/catl-v1-errors.h"       // Include catalogue error classes
#include "catl/v1/catl-v1-mmap-reader.h"  // Include MmapReader
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
    const uint8_t* data = nullptr;
    size_t fileSize = 0;
    CATLHeader header;

    // Maps for tracking state
    SHAMap stateMap;
    SHAMap txMap;
    std::shared_ptr<LedgerStore> ledgerStore;

    // Statistics
    struct Stats
    {
        uint32_t ledgersProcessed = 0;
        uint32_t stateNodesAdded = 0;
        uint32_t txNodesAdded = 0;
        uint32_t stateRemovalsAttempted = 0;
        uint32_t stateRemovalsSucceeded = 0;
        uint32_t successfulHashVerifications = 0;
        uint32_t failedHashVerifications = 0;
        size_t currentOffset = 0;
    } stats;

    void
    validateHeader()
    {
        // MmapReader already validates the header, but we'll apply our
        // additional checks and log header information for consistency
        stats.currentOffset = sizeof(CATLHeader);

#if STOP_AT_LEDGER
        header.max_ledger = STOP_AT_LEDGER;  // TODO: hackery
#endif

        // Log header info at INFO level
        LOGI("CATL Header Validated:");
        // Use std::hex manipulator for magic value display
        std::ostringstream oss_magic_info;
        oss_magic_info << "  Magic: 0x" << std::hex << header.magic << std::dec;
        LOGI(oss_magic_info.str());
        LOGI("  Ledger range: ", header.min_ledger, " - ", header.max_ledger);
        LOGI(
            "  Version: ",
            (header.version &
             CATALOGUE_VERSION_MASK));  // Mask out compression bits
        LOGI("  Network ID: ", header.network_id);
        LOGI(
            "  Header Filesize: ",
            header.filesize,
            " bytes");  // Note: Compare with actual later
    }

    // Unified map processing function
    size_t
    processMap(
        size_t offset,
        SHAMap& map,
        uint32_t& nodesProcessedCount,
        bool debugMap,
        bool isStateMap = false)
    {
        nodesProcessedCount = 0;
        bool foundTerminal = false;
        const std::string mapTypeName = isStateMap ? "state" : "transaction";
        size_t startOffset = offset;

        LOGD(
            "Starting processing of ",
            mapTypeName,
            " map data at offset ",
            offset);

        while (offset < fileSize && !foundTerminal)
        {
            stats.currentOffset =
                offset;  // Update global offset for error reporting

            // Read node type - check bounds first
            if (offset >= fileSize)
            {
                LOGE(
                    "Unexpected EOF reading node type in ",
                    mapTypeName,
                    " map at offset ",
                    offset);
                return startOffset;  // Return original offset on error
            }
            uint8_t nodeTypeVal = data[offset++];
            auto nodeType = static_cast<SHAMapNodeType>(nodeTypeVal);

            if (nodeType == tnTERMINAL)
            {
                LOGD(
                    "Found terminal marker for ",
                    mapTypeName,
                    " map at offset ",
                    offset - 1);
                foundTerminal = true;
                break;  // Exit loop successfully
            }

            // Validate node type BEFORE reading key/data
            if (nodeType != tnINNER && nodeType != tnTRANSACTION_NM &&
                nodeType != tnTRANSACTION_MD && nodeType != tnACCOUNT_STATE &&
                nodeType != tnREMOVE)
            {
                LOGE(
                    "Invalid node type encountered: ",
                    static_cast<int>(nodeTypeVal),
                    " in ",
                    mapTypeName,
                    " map at offset ",
                    offset - 1);
                return startOffset;  // Indicate error by returning original
                // offset
            }

            // Read key (32 bytes) - check bounds
            if (offset + Key::size() > fileSize)
            {
                LOGE(
                    "Unexpected EOF reading key (",
                    Key::size(),
                    " bytes) in ",
                    mapTypeName,
                    " map. Current offset: ",
                    offset,
                    ", File size: ",
                    fileSize);
                return startOffset;
            }
            const uint8_t* keyData = data + offset;
            Key itemKey(keyData);  // Create Key object early for logging
            offset += Key::size();

            // Handle removal (only expected in state maps)
            if (nodeType == tnREMOVE)
            {
                if (isStateMap)
                {
                    LOGD_KEY("Processing tnREMOVE for key: ", itemKey);
                    stats.stateRemovalsAttempted++;
                    if (map.remove_item(itemKey))
                    {
                        stats.stateRemovalsSucceeded++;
                        nodesProcessedCount++;
                    }
                    else
                    {
                        // Log warning if removal failed (item might not have
                        // existed)
                        LOGE(
                            "Failed to remove state item (may not exist), "
                            "key: ",
                            itemKey.hex());
                        return startOffset;  // error
                    }
                }
                else
                {
                    LOGW(
                        "Found unexpected tnREMOVE node in transaction map at "
                        "offset ",
                        offset - 1 - Key::size(),
                        " for key: ",
                        itemKey.hex());
                    return startOffset;  // error
                }
                continue;  // Move to the next node
            }

            // Read data size (4 bytes) - check bounds
            if (offset + sizeof(uint32_t) > fileSize)
            {
                LOGE(
                    "Unexpected EOF reading data size (",
                    sizeof(uint32_t),
                    " bytes) in ",
                    mapTypeName,
                    " map. Current offset: ",
                    offset,
                    ", File size: ",
                    fileSize);
                return startOffset;
            }
            uint32_t dataSize = 0;
            std::memcpy(&dataSize, data + offset, sizeof(uint32_t));
            offset += sizeof(uint32_t);

            // Validate data size (check against remaining file size and sanity
            // check) Allow zero size data (e.g. some placeholder nodes)
            constexpr uint32_t MAX_REASONABLE_DATA_SIZE =
                5 * 1024 * 1024;  // 5 MiB sanity limit
            if (offset > fileSize ||
                (dataSize > 0 && offset + dataSize > fileSize) ||
                dataSize > MAX_REASONABLE_DATA_SIZE)
            {
                LOGE(
                    "Invalid data size (",
                    dataSize,
                    " bytes) or EOF reached in ",
                    mapTypeName,
                    " map. Offset: ",
                    offset,
                    ", Remaining bytes: ",
                    (fileSize > offset ? fileSize - offset : 0),
                    ", File size: ",
                    fileSize);
                LOGD_KEY("Error occurred processing node with key: ", itemKey);
                return startOffset;
            }

            // Create MmapItem (zero-copy reference)
            const uint8_t* itemDataPtr = data + offset;
            auto item = boost::intrusive_ptr(
                new MmapItem(keyData, itemDataPtr, dataSize));

            if (!isStateMap)
            {
                LOGD(
                    "Adding item to transaction map key=",
                    item->key().hex(),
                    "data=",
                    item->hex());
            }

            // Add item to the appropriate map
            if (map.set_item(item) != SetResult::FAILED)
            {
                // addItem handles logging internally now
                nodesProcessedCount++;
            }
            else
            {
                // addItem already logs errors, but we might want a higher level
                // error here
                LOGE(
                    "Failed to add item from ",
                    mapTypeName,
                    " map to SHAMap, key: ",
                    itemKey.hex(),
                    " at offset ",
                    stats.currentOffset);
                // Consider if processing should stop here. Returning
                // startOffset indicates failure.
                return startOffset;
            }

            // Advance offset past the data
            offset += dataSize;
        }  // End while loop

        if (!foundTerminal)
        {
            LOGW(
                "Processing ",
                mapTypeName,
                " map ended without finding a terminal marker (tnTERMINAL). "
                "Reached offset ",
                offset);
            // This might be okay if it's the end of the file, or an error
            // otherwise.
            if (offset < fileSize)
            {
                LOGE(
                    "Map processing stopped prematurely before EOF and without "
                    "terminal marker. Offset: ",
                    offset);
                return startOffset;  // Indicate error
            }
        }

        LOGD(
            "Finished processing ",
            mapTypeName,
            " map. Processed ",
            nodesProcessedCount,
            " nodes. Final offset: ",
            offset);

        if (debugMap)
        {
            std::ostringstream oss;
            pretty_print_json(oss, map.items_json());
            LOGI("MAP JSON: ", oss.str());
        }
        return offset;  // Return the new offset after successful processing
    }

    // Process a single ledger
    size_t
    processLedger(size_t offset, LedgerInfoV1& info)
    {
        stats.currentOffset = offset;
        size_t initialOffset = offset;

        // Check bounds for LedgerInfo
        if (offset + sizeof(LedgerInfoV1) > fileSize)
        {
            LOGE(
                "Not enough data remaining (",
                (fileSize > offset ? fileSize - offset : 0),
                " bytes) for LedgerInfo structure (",
                sizeof(LedgerInfoV1),
                " bytes) at offset ",
                offset);
            return initialOffset;  // Return original offset on error
        }

        std::memcpy(&info, data + offset, sizeof(LedgerInfoV1));
        offset += sizeof(LedgerInfoV1);
        stats.currentOffset = offset;

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
        LOGI(
            "  Ledger Hash:      ",
            Hash256(info.hash).hex());  // Using efficient macro
        LOGI("  Parent Hash:      ", Hash256(info.parent_hash).hex());
        LOGI("  AccountState Hash:", Hash256(info.account_hash).hex());
        LOGI("  Transaction Hash: ", Hash256(info.tx_hash).hex());
        LOGI(
            "  Close Time:       ", utils::format_ripple_time(info.close_time));
        LOGI("  Drops:            ", info.drops);
        LOGI("  Close Flags:      ", info.close_flags);
        LOGI("  Offset at start:  ", initialOffset);

        // Process Account State Map
        bool isFirstLedger = (info.sequence == header.min_ledger);
        if (isFirstLedger)
        {
            LOGI(
                "Initializing new State SHAMap for first ledger ",
                info.sequence);
            stateMap =
                SHAMap(tnACCOUNT_STATE);  // Recreate for the first ledger
        }
        else
        {
            LOGI("Processing State Map delta for ledger ", info.sequence);
            // stateMap persists from previous ledger
        }

        uint32_t stateNodesProcessed = 0;
        size_t stateMapEndOffset = processMap(
            offset,
            stateMap,
            stateNodesProcessed,
            false,
            true);  // true = isStateMap
        if (stateMapEndOffset == offset && stateNodesProcessed == 0 &&
            offset != fileSize)
        {
            // Check if no progress was made and not EOF
            LOGE(
                "Error processing state map data for ledger ",
                info.sequence,
                ". No progress made from offset ",
                offset);
            return initialOffset;  // Return original offset to signal failure
        }
        offset = stateMapEndOffset;
        stats.currentOffset = offset;
        stats.stateNodesAdded += stateNodesProcessed;  // Accumulate stats
        LOGI(
            "  State map processing finished. Nodes processed in this ledger: ",
            stateNodesProcessed,
            ". New offset: ",
            offset);

        // Process Transaction Map (always created fresh for each ledger)
        LOGI("Processing Transaction Map for ledger ", info.sequence);
        txMap = SHAMap(tnTRANSACTION_MD);  // Create fresh transaction map
        uint32_t txNodesProcessed = 0;
        size_t txMapEndOffset = processMap(
            offset,
            txMap,
            txNodesProcessed,
            info.sequence == DEBUG_LEDGER_TX,
            false);  // false = not isStateMap
        if (txMapEndOffset == offset && txNodesProcessed == 0 &&
            offset != fileSize)
        {
            LOGE(
                "Error processing transaction map data for ledger ",
                info.sequence,
                ". No progress made from offset ",
                offset);
            return initialOffset;  // Signal failure
        }
        offset = txMapEndOffset;
        stats.currentOffset = offset;
        stats.txNodesAdded += txNodesProcessed;  // Accumulate stats
        LOGI(
            "  Transaction map processing finished. Nodes processed: ",
            txNodesProcessed,
            ". Final offset for ledger: ",
            offset);

        // Verify Hashes
        LOGI("Verifying map hashes for ledger ", info.sequence);
        verifyMapHash(
            stateMap,
            Hash256(info.account_hash),
            "AccountState",
            info.sequence);
        verifyMapHash(
            txMap, Hash256(info.tx_hash), "Transaction", info.sequence);

        stats.ledgersProcessed++;
        // LOG_INFO("--- Completed Ledger ", info.sequence, " ---");
        return offset;  // Return the final offset for this ledger
    }

    // Helper to verify map hashes
    void
    verifyMapHash(
        const SHAMap& map,
        const Hash256& expectedHash,
        const std::string& mapType,
        uint32_t ledgerSeq)
    {
        Hash256 computedHash = map.get_hash();  // getHash is const
        bool hashMatch = (computedHash == expectedHash);

        if (!hashMatch)
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
                LOGD("  Computed Hash: ", computedHash.hex());
                LOGD("  Expected Hash: ", expectedHash.hex());
            }
            stats.failedHashVerifications++;
            if ((mapType == "Transaction" && THROW_ON_TX_HASH_MISMATCH) ||
                (mapType == "AccountState" && THROW_ON_AS_HASH_MISMATCH))
            {
                std::ostringstream oss;
                oss << "Hash verification failed for " << mapType
                    << " map in ledger " << ledgerSeq
                    << ". Expected: " << expectedHash.hex()
                    << ", got: " << computedHash.hex();
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
            stats.successfulHashVerifications++;
        }
    }

public:
    // Constructor uses initializer list and handles file opening errors
    explicit CATLHasher(const std::string& filename)
        : reader(filename)  // Initialize MmapReader with filename
        , stateMap(tnACCOUNT_STATE)
        , txMap(tnTRANSACTION_MD)
        , ledgerStore(std::make_shared<LedgerStore>())
    {
        LOGI("File opened with MmapReader: ", filename);
        try
        {
            // Get data pointer and file size from MmapReader
            data = reader.data_at(0);  // Get pointer to start of file
            fileSize = reader.file_size();

            // Copy header from MmapReader (already validated)
            header = reader.header();

            LOGI(
                "File mapped successfully: ",
                filename,
                " (",
                fileSize,
                " bytes)");
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
            if (!data || fileSize == 0)
            {
                LOGE(
                    "No data available to process. File not mapped correctly?");
                return false;
            }

            // Validate header and log information
            validateHeader();

            // Compare actual file size with header filesize
            if (header.filesize != fileSize)
            {
                LOGW(
                    "File size mismatch: Header reports ",
                    header.filesize,
                    " bytes, actual mapped size is ",
                    fileSize,
                    " bytes. Processing based on actual size.");
                // Processing will continue, but this might indicate truncation
                // or corruption.
            }

            // Process ledgers starting after the header
            size_t currentFileOffset = sizeof(CATLHeader);
            uint32_t expectedLedgerCount =
                (header.max_ledger - header.min_ledger + 1);
            LOGI("Expecting ", expectedLedgerCount, " ledgers in this file.");

            while (currentFileOffset < fileSize)
            {
                LedgerInfoV1 info = {};
                size_t nextOffset = processLedger(currentFileOffset, info);

#if STORE_LEDGER_SNAPSHOTS
                constexpr int every = STORE_LEDGER_SNAPSHOTS_EVERY;
                if (every > 0 && info.sequence % every == 0)
                {
                    auto ledger = std::make_shared<Ledger>(
                        data + currentFileOffset,
                        // TODO: configurable snapshots
                        stateMap.snapshot(),
                        std::make_shared<SHAMap>(txMap));
                    // TODO: track only some ledgers, every 1000th or something
                    // and then recompute lazily via deltas
                    ledgerStore->add_ledger(ledger);
                }
#endif
#if COLLAPSE_STATE_MAP
                stateMap.collapse_tree();
#endif
                // Stop processing if we've reached the end of the ledger range
                if (info.sequence == header.max_ledger)
                {
                    break;
                }

                if (nextOffset == currentFileOffset)
                {
                    // No progress was made, indicates an error in processLedger
                    LOGE(
                        "Processing stalled at offset ",
                        currentFileOffset,
                        ". Error likely occurred in ledger ",
                        (stats.ledgersProcessed > 0
                             ? header.min_ledger + stats.ledgersProcessed
                             : header.min_ledger));
                    return false;  // Abort processing
                }
                if (nextOffset < currentFileOffset)
                {
                    // This should ideally not happen, defensive check
                    LOGE(
                        "Offset went backwards from ",
                        currentFileOffset,
                        " to ",
                        nextOffset,
                        ". Aborting.");
                    return false;
                }
                currentFileOffset = nextOffset;
            }

            // Check if we processed up to the expected end of the file
            if (currentFileOffset != fileSize)
            {
                LOGW(
                    "Processing finished at offset ",
                    currentFileOffset,
                    " but file size is ",
                    fileSize,
                    ". Potential trailing data or incomplete processing.");
            }
            else
            {
                LOGI(
                    "Processing reached the end of the mapped file (offset ",
                    currentFileOffset,
                    ").");
            }

            // Final Summary using INFO level
            LOGI("--- Processing Summary ---");
            LOGI(
                "Ledgers processed:      ",
                stats.ledgersProcessed,
                " (Expected: ",
                expectedLedgerCount,
                ")");
            if (stats.ledgersProcessed != expectedLedgerCount)
            {
                LOGW(
                    "Mismatch between processed ledgers and expected count "
                    "based on header range.");
            }
            LOGI("State map nodes added:  ", stats.stateNodesAdded);
            if (stats.stateRemovalsAttempted > 0 ||
                stats.stateRemovalsSucceeded > 0)
            {
                LOGI(
                    "State map removals:   ",
                    stats.stateRemovalsSucceeded,
                    " succeeded out of ",
                    stats.stateRemovalsAttempted,
                    " attempts");
            }
            LOGI("Transaction nodes added:", stats.txNodesAdded);
            LOGI(
                "Hash Verifications:   ",
                stats.successfulHashVerifications,
                " Succeeded, ",
                stats.failedHashVerifications,
                " Failed");
            LOGI("--- End Summary ---");

            /// header.min_ledger to header.max_ledger
            for (uint32_t ledgerSeq = header.min_ledger;
                 ledgerSeq <= header.max_ledger;
                 ledgerSeq++)
            {
                if (const auto ledger = ledgerStore->get_ledger(ledgerSeq))
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
                stats.currentOffset,
                ": ",
                e.what());
            return false;
        }
        catch (const catl::v1::CatlV1Error& e)
        {
            LOGE(
                "Aborting due to catalogue error at offset ~",
                stats.currentOffset,
                ": ",
                e.what());
            return false;
        }
        catch (const SHAMapException& e)
        {
            LOGE(
                "Aborting due to SHAMap error at offset ~",
                stats.currentOffset,
                ": ",
                e.what());
            return false;
        }
        catch (const std::exception& e)
        {
            LOGE(
                "Aborting due to standard error at offset ~",
                stats.currentOffset,
                ": ",
                e.what());
            return false;
        }
        catch (...)
        {
            LOGE(
                "Aborting due to unknown exception at offset ~",
                stats.currentOffset);
            return false;
        }
    }

    void
    startHttpServer() const
    {
        auto handler = std::make_shared<LedgerRequestHandler>(ledgerStore);
        HttpServer httpServer(handler);
        httpServer.run(8, true);
    }
};

// Main function updated for Logger control
int
main(int argc, char* argv[])
{
    // Define command line options
    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "Display this help message")(
        "input-file", po::value<std::string>(), "Path to the CATL file")(
        "level,l",
        po::value<std::string>()->default_value("info"),
        "Set log verbosity (error, warn, info, debug)")(
        "serve,s", po::bool_switch(), "Start HTTP server");

    // Positional argument for input file
    po::positional_options_description p;
    p.add("input-file", 1);

    // Parse command line arguments
    po::variables_map vm;
    try
    {
        po::store(
            po::command_line_parser(argc, argv)
                .options(desc)
                .positional(p)
                .run(),
            vm);
        po::notify(vm);
    }
    catch (const po::error& e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        std::cerr << desc << std::endl;
        return 1;
    }

    // Display help if requested or if no input file provided
    if (vm.count("help") || !vm.count("input-file"))
    {
        std::cout << "Usage: " << argv[0] << " [options] <catalogue_file>"
                  << std::endl;
        std::cout << desc << std::endl;
        std::cout << "Processes CATL files, builds SHAMaps, verifies hashes."
                  << std::endl;
        return vm.count("help") ? 0 : 1;
    }

    // Get input filename and desired log level
    std::string inputFile = vm["input-file"].as<std::string>();
    // Parse log level
    std::string levelStr = vm["level"].as<std::string>();

    if (!Logger::set_level(levelStr))
    {
        std::cerr << "Warning: Unknown log level '" << levelStr
                  << "'. Using default (info)." << std::endl;
    }

    // Start timing
    auto startTime = std::chrono::high_resolution_clock::now();
    int exitCode = 1;  // Default to failure

    std::optional<CATLHasher> hasher = std::nullopt;

    try
    {
        // Create and process with CATLHasher
        hasher.emplace(inputFile);
        bool result = hasher->processFile();
        exitCode = result ? 0 : 1;  // 0 on success, 1 on failure
    }
    catch (const std::exception& e)
    {
        // Catch errors during CATLHasher construction
        LOGE("Fatal error during initialization: ", e.what());
        exitCode = 1;
    }
    catch (...)
    {
        LOGE("Caught unknown fatal error during initialization.");
        exitCode = 1;
    }

    // Calculate and display execution time
    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        endTime - startTime);
    double seconds = duration.count() / 1000.0;

    std::ostringstream timeOSS;
    timeOSS << "Execution completed in " << std::fixed << std::setprecision(3)
            << seconds << " seconds (" << duration.count() << " ms)";
    LOGW(timeOSS.str());

    if (hasher && vm["serve"].as<bool>())
    {
        hasher->startHttpServer();
    }

    return exitCode;
}
