#include <array>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

// For memory mapping
#include <boost/filesystem.hpp>
#include <boost/iostreams/device/mapped_file.hpp>

// For crypto
#include <openssl/evp.h>

#include "catl/core/log-macros.h"
#include "catl/core/logger.h"
#include "catl/core/types.h"
#include "catl/hasher/catalogue-consts.h"
#include "catl/hasher/http/http-handler.h"
#include "catl/hasher/http/http-server.h"
#include "catl/hasher/ledger.h"
#include "catl/hasher/utils.h"
#include "catl/shamap/shamap.h"

class CATLHasher
{
private:
    boost::iostreams::mapped_file_source mmapFile;
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

    bool
    validateHeader()
    {
        stats.currentOffset = 0;
        if (fileSize < sizeof(CATLHeader))
        {
            LOGE(
                "File too small (",
                fileSize,
                " bytes) to contain a valid CATL header (",
                sizeof(CATLHeader),
                " bytes)");
            return false;
        }

        std::memcpy(&header, data, sizeof(CATLHeader));
        stats.currentOffset = sizeof(CATLHeader);
        header.max_ledger = 100'000;  // TODO: hackery

        if (header.magic != CATL)
        {
            // Use std::hex manipulator directly in the log call
            std::ostringstream oss_magic;
            oss_magic << "Invalid magic value: expected 0x" << std::hex << CATL
                      << ", got 0x" << header.magic << std::dec;
            LOGE(oss_magic.str());
            return false;
        }

        uint8_t compressionLevel =
            (header.version & CATALOGUE_COMPRESS_LEVEL_MASK) >> 8;
        if (compressionLevel != 0)
        {
            LOGE(
                "Compressed CATL files are not supported. Compression level: ",
                static_cast<int>(compressionLevel));
            return false;
        }

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

        return true;
    }

    // Unified map processing function
    size_t
    processMap(
        size_t offset,
        SHAMap& map,
        uint32_t& nodesProcessedCount,
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
            SHAMapNodeType nodeType = static_cast<SHAMapNodeType>(nodeTypeVal);

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
        return offset;  // Return the new offset after successful processing
    }

    // Process a single ledger
    size_t
    processLedger(size_t offset, LedgerInfo& info)
    {
        stats.currentOffset = offset;
        size_t initialOffset = offset;

        // Check bounds for LedgerInfo
        if (offset + sizeof(LedgerInfo) > fileSize)
        {
            LOGE(
                "Not enough data remaining (",
                (fileSize > offset ? fileSize - offset : 0),
                " bytes) for LedgerInfo structure (",
                sizeof(LedgerInfo),
                " bytes) at offset ",
                offset);
            return initialOffset;  // Return original offset on error
        }

        std::memcpy(&info, data + offset, sizeof(LedgerInfo));
        offset += sizeof(LedgerInfo);
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
        LOGI("  Parent Hash:      ", Hash256(info.parentHash).hex());
        LOGI("  AccountState Hash:", Hash256(info.accountHash).hex());
        LOGI("  Transaction Hash: ", Hash256(info.txHash).hex());
        LOGI("  Close Time:       ", utils::format_ripple_time(info.closeTime));
        LOGI("  Drops:            ", info.drops);
        LOGI("  Close Flags:      ", info.closeFlags);
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
            offset, stateMap, stateNodesProcessed, true);  // true = isStateMap
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
            stateMap, Hash256(info.accountHash), "AccountState", info.sequence);
        verifyMapHash(
            txMap, Hash256(info.txHash), "Transaction", info.sequence);

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
            throw std::runtime_error(
                "Hash verification failed for ledger: " +
                std::to_string(ledgerSeq));
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
        : header()
        , stateMap(tnACCOUNT_STATE)
        ,  // Initialize maps here
        txMap(tnTRANSACTION_MD)
        , ledgerStore(std::make_shared<LedgerStore>())
    {
        LOGI("Attempting to open and map file: ", filename);
        try
        {
            if (!boost::filesystem::exists(filename))
            {
                throw std::runtime_error("File does not exist: " + filename);
            }

            boost::filesystem::path path(filename);
            boost::uintmax_t actualFileSize =
                boost::filesystem::file_size(path);
            if (actualFileSize == 0)
            {
                throw std::runtime_error("File is empty: " + filename);
            }

            mmapFile.open(filename);
            if (!mmapFile.is_open())
            {
                throw std::runtime_error(
                    "Boost failed to memory map file: " + filename);
            }

            data = reinterpret_cast<const uint8_t*>(mmapFile.data());
            fileSize = mmapFile.size();  // Get size from boost map

            if (fileSize != actualFileSize)
            {
                LOGW(
                    "Memory mapped size (",
                    fileSize,
                    ") differs from filesystem size (",
                    actualFileSize,
                    "). Using mapped size.");
            }

            if (!data)
            {
                // This case should ideally be caught by mmapFile.is_open()
                throw std::runtime_error(
                    "Memory mapping succeeded but data pointer is null.");
            }

            LOGI(
                "File mapped successfully: ",
                filename,
                " (",
                fileSize,
                " bytes)");
        }
        catch (const boost::filesystem::filesystem_error& e)
        {
            LOGE(
                "Boost Filesystem error opening file '",
                filename,
                "': ",
                e.what());
            throw;  // Re-throw to be caught by main
        }
        catch (const std::ios_base::failure& e)
        {
            LOGE(
                "Boost IOStreams error mapping file '",
                filename,
                "': ",
                e.what());
            throw;  // Re-throw
        }
        catch (const std::exception& e)
        {
            LOGE("Error during file setup '", filename, "': ", e.what());
            throw;  // Re-throw standard exceptions
        }
    }

    // Destructor (optional, Boost handles unmapping)
    ~CATLHasher()
    {
        LOGD("CATLHasher destroyed, Boost will unmap the file.");
        if (mmapFile.is_open())
        {
            mmapFile.close();
        }
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

            if (!validateHeader())
            {
                LOGE("CATL header validation failed. Aborting processing.");
                return false;
            }

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
                LedgerInfo info = {};
                size_t nextOffset = processLedger(currentFileOffset, info);

                constexpr int every = 1;  // 0'000;
                // TOOD: configurable
                if (info.sequence % every == 0)
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
    if (argc < 2)
    {
        // Use std::cerr directly for usage message as Logger might not be
        // configured yet
        std::cerr << "Usage: " << argv[0]
                  << " <catalogue_file> [--level <level>]" << std::endl;
        std::cerr << "  <catalogue_file>: Path to the CATL file." << std::endl;
        std::cerr << "  --level <level>: Set log verbosity (optional)."
                  << std::endl;
        std::cerr << "     Levels: error, warn, info (default), debug"
                  << std::endl;
        std::cerr << "\nProcesses CATL files, builds SHAMaps, verifies hashes."
                  << std::endl;
        return 1;
    }

    std::string inputFile = argv[1];
    LogLevel desiredLevel = LogLevel::INFO;  // Default level

    // Parse command line arguments for log level
    for (int i = 2; i < argc; ++i)
    {
        std::string arg = argv[i];
        if (arg == "--level" && i + 1 < argc)
        {
            std::string levelStr = argv[++i];
            if (levelStr == "error" || levelStr == "ERROR")
            {
                desiredLevel = LogLevel::ERROR;
            }
            else if (
                levelStr == "warn" || levelStr == "WARN" ||
                levelStr == "warning" || levelStr == "WARNING")
            {
                desiredLevel = LogLevel::WARNING;
            }
            else if (levelStr == "info" || levelStr == "INFO")
            {
                desiredLevel = LogLevel::INFO;
            }
            else if (levelStr == "debug" || levelStr == "DEBUG")
            {
                desiredLevel = LogLevel::DEBUG;
            }
            else
            {
                std::cerr << "Warning: Unknown log level '" << levelStr
                          << "'. Using default (info)." << std::endl;
            }
        }
        // Deprecated verbose flags (map to debug for backward compatibility)
        else if (arg == "--verbose" || arg == "--debug")
        {
            desiredLevel = LogLevel::DEBUG;
            std::cerr << "Warning: --verbose/--debug flags are deprecated. Use "
                         "'--level debug'."
                      << std::endl;
        }
        else
        {
            std::cerr << "Warning: Unknown argument '" << arg << "'."
                      << std::endl;
        }
    }

    // Set the logger level *before* creating CATLHasher
    Logger::setLevel(desiredLevel);

    // Start timing
    auto startTime = std::chrono::high_resolution_clock::now();
    int exitCode = 1;  // Default to failure

    std::optional<CATLHasher> hasher = std::nullopt;

    try
    {
        // Pass only filename, verbose removed
        hasher.emplace(inputFile);
        bool result = hasher->processFile();
        exitCode = result ? 0 : 1;  // 0 on success, 1 on failure
    }
    catch (const std::exception& e)
    {
        // Catch errors during CATLHasher construction (e.g., file not found)
        // Logger might already be set, so use it. If not, cerr is fallback.
        LOGE("Fatal error during initialization: ", e.what());
        exitCode = 1;
    }
    catch (...)
    {
        LOGE("Caught unknown fatal error during initialization.");
        exitCode = 1;
    }

    // Calculate and display execution time using Logger
    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        endTime - startTime);
    double seconds = duration.count() / 1000.0;

    // Use fixed/setprecision for consistent output format
    std::ostringstream timeOSS;
    timeOSS << "Execution completed in " << std::fixed << std::setprecision(3)
            << seconds << " seconds (" << duration.count() << " ms)";
    LOGW(timeOSS.str());

    if (hasher)
    {
        hasher->startHttpServer();
    }

    return exitCode;
}
