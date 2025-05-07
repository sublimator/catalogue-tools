#pragma once

#include "catl/core/types.h"
#include "catl/shamap/shamap-nodetype.h"
#include "catl/shamap/shamap.h"
#include "catl/v1/catl-v1-structs.h"
#include <functional>
#include <memory>
#include <ostream>
#include <string>

namespace catl::v1 {

/**
 * Types of write operations for callback notifications
 */
enum class WriteType {
    HEADER,         // File header writes
    LEDGER_HEADER,  // Ledger header writes
    MAP_ITEM,       // Individual map item writes
    TERMINAL        // Terminal marker writes
};

/**
 * Callback type for write notifications
 */
using WriteCallback = std::function<void(WriteType, size_t)>;

/**
 * Configuration options for CATL Writer
 */
struct WriterOptions
{
    /** Network ID to include in the file header */
    uint32_t network_id = 0;

    /** Compression level (0-9, where 0 means uncompressed) */
    uint8_t compression_level = 0;
};

/**
 * Writer for CATL v1 files
 *
 * This class creates CATL files from ledger headers and their associated
 * state and transaction maps. It supports both compressed and uncompressed
 * files.
 */
class Writer
{
public:
    /**
     * Constructor that takes ownership of streams for header and body
     *
     * @param header_stream Stream for writing/updating the header section
     * @param body_stream Stream for writing the file body (potentially
     * compressed)
     * @param options Configuration options for the writer
     */
    Writer(
        std::shared_ptr<std::ostream> header_stream,
        std::shared_ptr<std::ostream> body_stream,
        const WriterOptions& options = {});

    /**
     * Factory method for creating a file-based writer
     *
     * @param path Path to the output file
     * @param options Configuration options for the writer
     * @return Unique pointer to a Writer instance
     * @throws CatlV1Error if file cannot be opened
     */
    static std::unique_ptr<Writer>
    for_file(const std::string& path, const WriterOptions& options = {});

    /**
     * Write the file header
     *
     * @param min_ledger First ledger sequence to be included in the file
     * @param max_ledger Last ledger sequence to be included in the file
     * @throws CatlV1Error if ledger range is invalid, header already written,
     * or on IO error
     */
    void
    write_header(uint32_t min_ledger, uint32_t max_ledger);

    /**
     * Write a ledger header to the file
     *
     * @param header Ledger header information
     * @throws CatlV1Error if file header not written, already finalized, or on
     * IO error
     */
    void
    write_ledger_header(const LedgerInfo& header);

    /**
     * Write a complete SHAMap to the file
     *
     * @param map SHAMap to write
     * @param node_type Type of nodes in the map (e.g., tnACCOUNT_STATE)
     * @throws CatlV1Error if file header not written, already finalized, or on
     * IO error
     */
    void
    write_map(const SHAMap& map, SHAMapNodeType node_type);

    /**
     * Write the delta between two SHAMaps
     *
     * Computes and writes only the differences between previous and current
     * maps, including item removals and additions/modifications.
     *
     * @param previous Previous state of the map
     * @param current Current state of the map
     * @param node_type Type of nodes in the map (e.g., tnACCOUNT_STATE)
     * @throws CatlV1Error if file header not written, already finalized, or on
     * IO error
     */
    void
    write_map_delta(
        const SHAMap& previous,
        const SHAMap& current,
        SHAMapNodeType node_type);

    /**
     * Convenience method to write a complete ledger
     *
     * Writes the ledger header followed by the state and transaction maps.
     *
     * @param header Ledger header information
     * @param state_map State map for the ledger
     * @param tx_map Transaction map for the ledger
     * @throws CatlV1Error if any of the individual write operations fail
     */
    void
    write_ledger(
        const LedgerInfo& header,
        const SHAMap& state_map,
        const SHAMap& tx_map);

    /**
     * Finalize the file
     *
     * Flushes all streams and updates the header with the final file size
     * and a hash computed over the entire file. Uses the header stream to
     * determine the final file size by seeking to the end. The file cannot
     * be modified after this method is called.
     *
     * @throws CatlV1Error if file header not written, already finalized, or on
     * IO error
     */
    void
    finalize();

    /**
     * Get the number of bytes written to the body stream
     *
     * @return Total bytes written to the body stream (excluding header)
     */
    size_t
    body_bytes_written() const;

    /**
     * Set a callback to be notified of write operations
     *
     * @param callback Function to call with write type and bytes written
     */
    void
    set_write_callback(WriteCallback callback);

    /**
     * Destructor ensures proper cleanup
     */
    ~Writer();

private:
    /**
     * Write a single item to the file
     *
     * @param node_type Type of the node
     * @param key Key of the item
     * @param data Pointer to item data (null for removals)
     * @param size Size of the data
     * @throws CatlV1Error on IO error
     */
    void
    write_item(
        SHAMapNodeType node_type,
        const Key& key,
        const uint8_t* data,
        uint32_t size);

    /**
     * Write a terminal marker to end a map section
     *
     * @throws CatlV1Error on IO error
     */
    void
    write_terminal();

    /** Stream for header operations */
    std::shared_ptr<std::ostream> header_stream_;

    /** Stream for body content */
    std::shared_ptr<std::ostream> body_stream_;

    /** Header structure being built */
    CatlHeader header_;

    /** Configuration options */
    WriterOptions options_;

    /** Whether the header has been written */
    bool header_written_ = false;

    /** Whether the file has been finalized */
    bool finalized_ = false;

    /** Count of bytes written to body stream */
    size_t body_bytes_written_ = 0;

    /** Optional callback for write notifications */
    WriteCallback write_callback_;

    /**
     * Track bytes written and notify callback if registered
     *
     * @param type Type of write operation
     * @param bytes Number of bytes written
     */
    void
    track_write(WriteType type, size_t bytes);
};

}  // namespace catl::v1