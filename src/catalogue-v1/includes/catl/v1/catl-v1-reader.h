#pragma once

#include "catl/v1/catl-v1-errors.h"
#include "catl/v1/catl-v1-structs.h"
#include "catl/v1/catl-v1-types.h"
#include <boost/iostreams/filter/zlib.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <fstream>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "catl/shamap/shamap-nodetype.h"
#include "catl/shamap/shamap.h"

namespace catl::v1 {

/**
 * Reader for CATL files with support for both compressed and uncompressed
 * formats
 *
 * This class handles the reading and parsing of CATL files, with special
 * consideration for the streaming nature of the data. Important implementation
 * notes:
 *
 * 1. Memory Management: When reading into a SHAMap using read_map_to_shamap(),
 * care must be taken with memory lifetime as SHAMap items reference memory that
 * must remain valid.
 *
 * 2. Stream Limitations: When working with compressed streams (through zlib),
 * backward seeking operations are generally not supported.
 *
 * 3. Efficiency: For high-performance operations like slicing, prefer using
 * the tee functionality (enable_tee(), disable_tee()) combined with methods
 * that perform minimal processing like read_raw_data() or skip_map() rather
 * than parsing and reconstructing the data.
 *
 * 4. State Tracking: When tracking state for tools like a slicer, consider
 * using read_map_with_callbacks() with appropriate callbacks rather than
 * building a full SHAMap.
 */
class Reader
{
public:
    explicit Reader(std::string filename);
    ~Reader();

    /**
     * Decompress the file to an uncompressed version
     *
     * Creates an uncompressed copy of this file at the specified output path.
     * The method simply transfers the header (with compression level set to 0)
     * and pipes the decompressed body to the output without parsing its
     * contents.
     *
     * @param output_path Path where the uncompressed file should be written
     * @throws CatlV1Error if file cannot be decompressed
     */
    void
    decompress(const std::string& output_path);

    /**
     * Compress the file to a compressed version
     *
     * Creates a compressed copy of this file at the specified output path
     * with the given compression level. The method transfers the header
     * (with the new compression level) and pipes the body through compression
     * to the output without parsing its contents.
     *
     * @param output_path Path where the compressed file should be written
     * @param compression_level Compression level (1-9, where 9 is maximum)
     * @throws CatlV1Error if file cannot be compressed or if compression level
     *         is invalid
     */
    void
    compress(const std::string& output_path, int compression_level = 9);

private:
    /**
     * Copy the file with a different compression level
     *
     * Internal method that handles both compression and decompression by
     * copying the file with a new compression level.
     *
     * @param output_path Path where the output file should be written
     * @param new_compression_level Target compression level (0-9)
     * @param operation_name Name of operation for error messages
     * @throws CatlV1Error if the operation fails
     */
    void
    copy_with_compression(
        const std::string& output_path,
        int new_compression_level,
        const std::string& operation_name);

public:
    /**
     * Read raw data from the file without parsing
     *
     * @param buffer Buffer to receive the data
     * @param size Number of bytes to read
     * @param context Optional context for error messages
     * @return Actual number of bytes read
     * @throws CatlV1Error if EOF is reached or an I/O error occurs
     */
    size_t
    read_raw_data(
        uint8_t* buffer,
        size_t size,
        const std::string& context = "");

    /**
     * Get the total number of bytes read from the body of the file
     *
     * This method returns the count of bytes read or skipped from the file body
     * (the part after the header). This is useful for tracking progress,
     * especially when using the tee functionality to copy data while reading.
     *
     * @return Number of bytes read from the body of the file since construction
     */
    size_t
    body_bytes_consumed() const
    {
        return body_bytes_consumed_;
    };

    /**
     * Read exactly the specified number of bytes, throwing if not possible
     *
     * @param buffer Buffer to receive the data
     * @param size Number of bytes to read
     * @param context Optional context for error messages
     * @throws CatlV1Error if the exact number of bytes cannot be read
     */
    void
    read_bytes(uint8_t* buffer, size_t size, const std::string& context = "")
    {
        size_t bytes_read = read_raw_data(buffer, size, context);
        if (bytes_read != size)
        {
            throw CatlV1Error(
                "Unexpected EOF while reading " + std::to_string(size) +
                " bytes " + (context.empty() ? "bytes" : context) +
                " read only " + std::to_string(bytes_read) + " bytes");
        }
    }

    /**
     * Read a fixed-size value from the file
     *
     * @tparam T Type of value to read
     * @param value Reference to variable that will receive the value
     * @param context Optional context for error messages
     * @throws CatlV1Error if EOF is reached or an I/O error occurs
     */
    template <typename T>
    void
    read_value(T& value, const std::string& context = "")
    {
        read_bytes(reinterpret_cast<uint8_t*>(&value), sizeof(T), context);
    }

    /**
     * Read bytes into a vector at specified position
     *
     * This method reads bytes into a vector at a specific position. The
     * position must be within the current vector size (the vector must already
     * be sized appropriately).
     *
     * @param vec Vector to read into
     * @param pos Position in vector to start writing
     * @param size Number of bytes to read
     * @param context Optional context for error messages
     * @throws CatlV1Error if the exact number of bytes cannot be read
     */
    void
    read_bytes_into_vector(
        std::vector<uint8_t>& vec,
        size_t pos,
        size_t size,
        const std::string& context = "")
    {
        if (pos + size > vec.size())
        {
            throw CatlV1Error("Vector too small for read operation");
        }
        read_bytes(&vec[pos], size, context);
    }

    /**
     * Read bytes directly into a vector's existing capacity and update its size
     *
     * This method assumes the vector has already been reserved with sufficient
     * capacity but has not been resized. It writes directly to the unused
     * capacity and updates the vector's size afterward.
     *
     * @param vec Vector with sufficient capacity (but not necessarily resized)
     * @param size Number of bytes to read
     * @param context Optional context for error messages
     * @throws CatlV1Error if insufficient capacity or I/O error
     */
    void
    read_bytes_into_capacity(
        std::vector<uint8_t>& vec,
        size_t size,
        const std::string& context = "")
    {
        size_t current_size = vec.size();
        if (vec.capacity() < current_size + size)
        {
            throw CatlV1Error(
                "Vector capacity insufficient for data - required capacity: " +
                std::to_string(current_size + size) +
                ", available: " + std::to_string(vec.capacity()) +
                ". Call reserve() before reading");
        }

        // Resize to have access to the memory (will not reallocate)
        // This is safe because we've already checked capacity
        vec.resize(current_size + size);

        // Read directly into the new area
        read_bytes(&vec[current_size], size, context);
    }

    // Returns the parsed header
    const CatlHeader&
    header() const;

    int
    compression_level() const;

    int
    catalogue_version() const;

    // Reads the next LedgerHeader from the file
    // @throws CatlV1Error if EOF is reached or an error occurs
    LedgerInfo
    read_ledger_info();

    /**
     * Read a SHAMap from the file
     *
     * Process items from the current position in the file until a terminal
     * marker and add them to the provided SHAMap. This method stores item data
     * in the provided storage vector to ensure data referenced by MmapItems
     * remains valid.
     *
     * Note: This method differs from MmapReader::read_shamap() in that it
     * requires an external storage vector because it cannot rely on
     * memory-mapped file data persisting. For non-compressed files where direct
     * memory mapping is possible, consider using MmapReader instead for better
     * performance.
     *
     * IMPORTANT: The storage vector will continue to grow as more items are
     * added. For large files, this can lead to significant memory usage.
     * Consider using the on_storage_growth callback to monitor and manage
     * memory consumption if needed.
     *
     * @see MmapReader::read_shamap()
     *
     * @param map SHAMap to populate with nodes
     * @param node_type Expected type of nodes in the map
     * @param storage Vector to store item data (must outlive the SHAMap!)
     * @param allow_delta Whether to allow updates and deletes to existing keys
     * @param on_storage_growth Optional callback invoked after each item is
     * added to storage, allowing the caller to monitor or manage memory usage
     * @return MapOperations struct with counts of nodes processed
     * @throws CatlV1Error if file format is invalid or an I/O error occurs
     * @throws CatlV1DeltaError if a delta operation is attempted when
     * allow_delta is false
     *
     * @note In a future v2 format, size hints for maps should be included in
     * the file format to allow for better pre-allocation of storage vectors.
     */
    template <typename Traits = shamap::DefaultNodeTraits>
    MapOperations
    read_map_to_shamap(
        shamap::SHAMapT<Traits>& map,
        shamap::SHAMapNodeType node_type,
        std::vector<uint8_t>& storage,
        bool allow_delta = false,
        const std::function<void(size_t current_size, size_t growth)>&
            on_storage_growth = nullptr);

    /**
     * Read a node type and skip the rest of the node
     *
     * This method reads the node type and then skips over the remaining
     * data for the node (key and value data if present). It returns the
     * node type so the caller can decide how to proceed.
     *
     * @return The SHAMapNodeType that was read
     * @throws CatlV1Error if EOF or I/O error occurs
     */
    shamap::SHAMapNodeType
    read_and_skip_node();

    /**
     * Skip over an entire map section (state or transaction) without processing
     *
     * This method efficiently advances the stream position without allocating
     * memory or copying data. It simply reads the minimal information needed
     * to determine each node's size and advances past it until a terminal
     * marker.
     *
     * @param node_type Type of map to skip (tnACCOUNT_STATE or
     * tnTRANSACTION_MD/NM)
     * @throws CatlV1Error if file format is invalid or an I/O error occurs
     */
    void
    skip_map(shamap::SHAMapNodeType node_type);

    /**
     * Read the next node type and advance reader position
     *
     * @return The SHAMapNodeType that was read
     * @throws CatlV1Error if EOF or I/O error
     */
    shamap::SHAMapNodeType
    read_node_type();

    /**
     * Read a map node key into the provided vector
     *
     * @param key_out Vector to store the key data
     * @param resize_to_fit Whether to resize the vector to exactly fit the key
     * (default: true). When false, vector must have sufficient capacity
     * pre-allocated via reserve() or an exception will be thrown.
     * @throws CatlV1Error if EOF, I/O error, or insufficient vector capacity
     * when resize_to_fit is false
     */
    void
    read_node_key(std::vector<uint8_t>& key_out, bool resize_to_fit = true);

    /**
     * Read map node data into the provided vector
     *
     * @param data_out Vector to store the data
     * @param resize_to_fit Whether to resize the vector to exactly fit the data
     * (default: true). When false, vector must have sufficient capacity
     * pre-allocated via reserve() or an exception will be thrown.
     * @return The size of the data read
     * @throws CatlV1Error if EOF, I/O error, or insufficient vector capacity
     * when resize_to_fit is false
     */
    uint32_t
    read_node_data(std::vector<uint8_t>& data_out, bool resize_to_fit = true);

    /**
     * Read a complete map node (type, key, and data)
     *
     * Note: This method allocates memory for the key and data vectors. For
     * performance-critical code that needs to process many nodes without
     * allocating memory, consider using the lower-level methods that just
     * advance the stream position.
     *
     * @param type_out Output parameter for node type
     * @param key_out Vector to store the key data
     * @param data_out Vector to store the data (empty for tnREMOVE)
     * @return true if a node was read, false if reached terminal marker
     * @throws CatlV1Error if EOF or I/O error
     */
    bool
    read_map_node(
        shamap::SHAMapNodeType& type_out,
        std::vector<uint8_t>& key_out,
        std::vector<uint8_t>& data_out);

    /**
     * Enable "tee" mode to copy all read data to an output stream
     *
     * When enabled, all data read through the Reader will also be written
     * to the specified output stream. This is useful for creating slices
     * while processing the input file.
     *
     * @param output Stream to copy read data to
     */
    void
    enable_tee(std::ostream& output)
    {
        tee_stream_ = &output;
        tee_enabled_ = true;
    }

    /**
     * Disable "tee" mode
     *
     * Stops copying read data to the output stream.
     */
    void
    disable_tee()
    {
        tee_stream_ = nullptr;
        tee_enabled_ = false;
    }

    /**
     * Skip bytes while copying them to the tee stream if enabled
     *
     * @param bytes Number of bytes to skip
     * @param context Optional context for error messages
     * @return Actual number of bytes skipped
     */
    size_t
    skip_with_tee(size_t bytes, const std::string& context = "");

    /**
     * Read a map with separate callbacks for nodes and deletions
     *
     * Processes all nodes in a map section until a terminal marker is found.
     * For each regular node, the on_node callback is invoked with the node's
     * key and data. For each deletion node, the on_delete callback is invoked
     * with just the key. This allows processing the map data without building a
     * full SHAMap.
     *
     * @param type Expected node type for the map (tnACCOUNT_STATE or
     * tnTRANSACTION_*)
     * @param on_node Callback function receiving key and data vectors for
     * regular nodes
     * @param on_delete Callback function receiving key vector for deletion
     * nodes (optional)
     * @return MapOperations struct with counts of nodes processed
     * @throws CatlV1Error if file format is invalid or an I/O error occurs
     */
    MapOperations
    read_map_with_callbacks(
        shamap::SHAMapNodeType type,
        const std::function<
            void(const std::vector<uint8_t>&, const std::vector<uint8_t>&)>&
            on_node,
        const std::function<void(const std::vector<uint8_t>&)>& on_delete =
            nullptr);

    /**
     * Read a SHAMap using owned item instances
     *
     * Process items from the current position in the file until a terminal
     * marker and add them to the provided SHAMap. Unlike read_map_to_shamap(),
     * this method creates self-contained item instances that own their memory,
     * eliminating the need for an external storage vector.
     *
     * This approach simplifies memory management as each item manages its own
     * lifetime, but may result in more memory allocations compared to the
     * bulk storage approach.
     *
     * @param map SHAMap to populate with nodes
     * @param node_type Expected type of nodes in the map
     * @param allow_delta Whether to allow updates and deletes to existing keys
     * @return MapOperations struct with counts of nodes processed
     * @throws CatlV1Error if file format is invalid or an I/O error occurs
     * @throws CatlV1DeltaError if a delta operation is attempted when
     * allow_delta is false
     */
    template <typename Traits = shamap::DefaultNodeTraits>
    MapOperations
    // ReSharper disable once CppFunctionIsNotImplemented
    read_map_with_shamap_owned_items(
        shamap::SHAMapT<Traits>& map,
        shamap::SHAMapNodeType node_type,
        bool allow_delta = false);

private:
    void
    read_header();

    // Returns true if the header is valid
    bool
    valid() const;

    std::ifstream file_;
    // For compressed reading, this wraps file_ if needed
    std::unique_ptr<boost::iostreams::filtering_istream> decompressed_stream_;
    // Uniform input stream interface for all reading (non-owning)
    std::istream* input_stream_ = nullptr;

    CatlHeader header_;
    std::string filename_;
    int compression_level_{0};
    int catalogue_version_{0};
    bool valid_{false};

    // Tee functionality - for simultaneously reading and writing
    std::ostream* tee_stream_ = nullptr;
    bool tee_enabled_ = false;
    size_t body_bytes_consumed_ = 0;
};

#define INSTANTIATE_READER_SHAMAP_NODE_TRAITS(TRAITS_ARG)              \
    template catl::v1::MapOperations                                   \
    catl::v1::Reader::read_map_to_shamap<TRAITS_ARG>(                  \
        catl::shamap::SHAMapT<TRAITS_ARG> & map,                       \
        catl::shamap::SHAMapNodeType node_type,                        \
        std::vector<uint8_t> & storage,                                \
        bool allow_delta,                                              \
        const std::function<void(size_t current_size, size_t growth)>& \
            on_storage_growth);                                        \
    template catl::v1::MapOperations                                   \
    catl::v1::Reader::read_map_with_shamap_owned_items<TRAITS_ARG>(    \
        catl::shamap::SHAMapT<TRAITS_ARG> & map,                       \
        catl::shamap::SHAMapNodeType node_type,                        \
        bool allow_delta);

}  // namespace catl::v1