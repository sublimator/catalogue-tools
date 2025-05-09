#pragma once

#include "catl/v1/catl-v1-errors.h"
#include "catl/v1/catl-v1-structs.h"
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
 * 1. Memory Management: When reading into a SHAMap, care must be taken with
 * memory lifetime as SHAMap items reference memory that must remain valid.
 *
 * 2. Stream Limitations: When working with compressed streams (through zlib),
 *    backward seeking operations are generally not supported. Methods like
 * peek_node_type() may not work correctly with compressed streams.
 *
 * 3. Efficiency: For high-performance operations like slicing, prefer using
 * methods that perform direct byte copying (copy_map_to_stream) rather than
 * parsing and reconstructing the data.
 *
 * 4. State Tracking: When tracking state for tools like the slicer, consider
 * using the process_nodes callback in copy_map_to_stream rather than building a
 * full SHAMap.
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
        if (read_raw_data(buffer, size, context) != size)
        {
            throw CatlV1Error(
                "Unexpected EOF while reading " +
                (context.empty() ? "bytes" : context));
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
     * @param map SHAMap to populate with nodes
     * @param node_type Expected type of nodes in the map
     * @param storage Vector to store item data (must outlive the SHAMap!)
     * @param allow_updates Whether to allow updates to existing keys
     * @return Number of nodes processed
     * @throws CatlV1Error if file format is invalid or an I/O error occurs
     */
    uint32_t
    read_shamap(
        SHAMap& map,
        SHAMapNodeType node_type,
        std::vector<uint8_t>& storage,
        bool allow_updates = false);

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
    skip_map(SHAMapNodeType node_type);

    /**
     * Peek at the next node type without advancing the reader position
     *
     * IMPORTANT: This method may not work correctly with compressed streams as
     * they don't generally support seeking backward. Only use with uncompressed
     * streams or when you're certain the stream supports seeking.
     *
     * @return The SHAMapNodeType of the next node
     * @throws CatlV1Error if EOF or I/O error
     */
    SHAMapNodeType
    peek_node_type();

    /**
     * Read the next node type and advance reader position
     *
     * @return The SHAMapNodeType that was read
     * @throws CatlV1Error if EOF or I/O error
     */
    SHAMapNodeType
    read_node_type();

    /**
     * Skip a single node of the specified type
     *
     * This method efficiently advances the stream position without allocating
     * memory or copying data. It only reads the minimal information needed
     * to determine the node's size.
     *
     * @param node_type Expected type of the node to skip
     * @throws CatlV1Error if type doesn't match or I/O error occurs
     */
    void
    skip_node(SHAMapNodeType node_type);

    /**
     * Read a map node key into the provided vector
     *
     * @param key_out Vector to store the key data
     * @param trim Whether to resize the vector to exactly fit the key (default:
     * true) Set to false when appending to a storage vector to avoid
     * reallocations
     * @throws CatlV1Error if EOF or I/O error
     */
    void
    read_node_key(std::vector<uint8_t>& key_out, bool trim = true);

    /**
     * Read map node data into the provided vector
     *
     * @param data_out Vector to store the data
     * @param trim Whether to resize the vector to exactly fit the data
     * (default: true) Set to false when appending to a storage vector to avoid
     * reallocations
     * @return The size of the data read
     * @throws CatlV1Error if EOF or I/O error
     */
    uint32_t
    read_node_data(std::vector<uint8_t>& data_out, bool trim = true);

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
        SHAMapNodeType& type_out,
        std::vector<uint8_t>& key_out,
        std::vector<uint8_t>& data_out);

    /**
     * Copy raw map data to an output stream until terminal marker
     *
     * This method performs efficient byte-level copying of map data from the
     * input stream to the output stream without unnecessary parsing. If a
     * callback is provided, it will also parse the nodes to provide information
     * about each node being copied.
     *
     * Note: This is the most efficient way to copy map data for slicing
     * operations.
     *
     * @param output Stream to write the map data to
     * @param process_nodes Optional callback to process nodes while copying
     * @return Number of bytes copied
     * @throws CatlV1Error if file format is invalid or an I/O error
     */
    size_t
    copy_map_to_stream(
        std::ostream& output,
        std::function<void(
            SHAMapNodeType,
            const std::vector<uint8_t>&,
            const std::vector<uint8_t>&)> process_nodes = nullptr);

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
};

}  // namespace catl::v1