#pragma once

#include "catl/core/types.h"  // For Slice
#include "catl/crypto/sha512-hasher.h"
#include "catl/shamap/shamap.h"
#include "catl/v1/catl-v1-errors.h"
#include "catl/v1/catl-v1-ledger-info-view.h"
#include "catl/v1/catl-v1-structs.h"
#include "catl/v1/catl-v1-types.h"  // For MapOperations
#include <boost/iostreams/device/mapped_file.hpp>
#include <functional>
#include <memory>
#include <string>

namespace catl::v1 {

/**
 * MmapReader - Memory-mapped file reader for CATL files
 *
 * This class provides a zero-copy, memory-mapped approach to reading CATL
 * files, which is more efficient for large files than the standard stream-based
 * Reader.
 */
class MmapReader
{
public:
    /**
     * Construct a MmapReader for the specified file
     *
     * @param filename Path to the CATL file to read
     * @throws CatlV1Error if file cannot be memory-mapped
     */
    explicit MmapReader(std::string filename);

    /**
     * Destructor - automatically unmaps the file
     */
    ~MmapReader();

    /**
     * Get the parsed CATL header
     *
     * @return Reference to the header structure
     */
    const CatlHeader&
    header() const;

    /**
     * Get the compression level from the header
     *
     * @return Compression level (0-9, where 0 means uncompressed)
     */
    int
    compression_level() const;

    /**
     * Get the catalogue version from the header
     *
     * @return Version number
     */
    int
    catalogue_version() const;

    /**
     * Get a pointer to raw data at the specified offset
     *
     * @param offset Byte offset from the start of the file
     * @return Pointer to the data
     * @throws CatlV1Error if offset is invalid
     */
    const uint8_t*
    data_at(size_t offset) const;

    /**
     * Get the total size of the memory-mapped file
     *
     * @return File size in bytes
     */
    size_t
    file_size() const;

    /**
     * Get the current read position
     *
     * @return Current position in bytes from start of file
     */
    size_t
    position() const;

    /**
     * Set the current read position
     *
     * @param pos Position in bytes from start of file
     * @throws CatlV1Error if position is beyond file bounds
     */
    void
    set_position(size_t pos);

    /**
     * Check if we're at the end of the file
     *
     * @return true if no more data can be read
     */
    bool
    eof() const;
    // TODO: add eof() to Reader class too

    /**
     * Read a ledger header at the current position and advance the position
     *
     * @return LedgerHeaderView for the current position
     * @throws CatlV1Error if there's not enough data left
     */
    LedgerInfoView
    read_ledger_info();

    /**
     * Get a zero-copy view of a ledger header at the specified position
     *
     * @param position Offset where the ledger header starts
     * @return LedgerInfoV1View for the specified position
     * @throws CatlV1Error if position is invalid
     */
    LedgerInfoView
    get_ledger_info_view(size_t position) const;

    /**
     * Read a SHAMap from the current position in the file
     *
     * This method reads a series of nodes from the current position until a
     * terminal marker is encountered, adding each node to the provided SHAMap.
     * The position is advanced to the end of the map data.
     *
     * Note: This method differs from Reader::read_map_to_shamap() in that it
     * uses memory-mapped file access and doesn't require an external storage
     * vector. Items created by this method directly reference the memory-mapped
     * file data.
     *
     * Important: The MmapReader class itself only works with uncompressed
     * files. For compressed CATL files, use Reader::read_map_to_shamap()
     * instead.
     *
     * @see Reader::read_map_to_shamap()
     *
     * @param map SHAMap to populate with nodes
     * @param leaf_type Expected leaf node type (used to validate node
     * operations)
     * @return Number of nodes processed and added to the map
     * @throws CatlV1Error if invalid data or unexpected end of file is
     * encountered
     */
    uint32_t
    read_shamap(shamap::SHAMap& map, shamap::SHAMapNodeType leaf_type);

    /**
     * Read a map with callbacks using zero-copy Slice objects
     *
     * This method provides a zero-copy interface for reading map data,
     * where callbacks receive Slice objects that directly reference the
     * memory-mapped file data instead of copying into vectors.
     *
     * @param type Expected node type for the map
     * @param on_node Callback for regular nodes, receives key and data slices
     * @param on_delete Callback for deletion nodes, receives key slice
     * @return MapOperations struct with counts of nodes processed
     * @throws CatlV1Error if file format is invalid or an I/O error occurs
     */
    MapOperations
    read_map_with_callbacks(
        shamap::SHAMapNodeType type,
        const std::function<void(const Slice&, const Slice&)>& on_node,
        const std::function<void(const Slice&)>& on_delete = nullptr);

    /**
     * Verify that the file hash in the header matches the computed hash of the
     * file
     *
     * This method validates file integrity by:
     * 1. Creating a copy of the header with hash field zeroed
     * 2. Computing a SHA-512 hash of the entire file (with zeroed hash field)
     * 3. Comparing the computed hash with the stored hash
     *
     * @throws CatlV1Error if the header hash field is empty
     * @throws CatlV1HashVerificationError if the hash verification fails
     */
    void
    verify_file_hash();

    /**
     * Read arbitrary data structure at the current position and advance
     *
     * @tparam T Type of structure to read
     * @return The structure
     * @throws CatlV1Error if there's not enough data left
     */
    template <typename T>
    T
    read_structure()
    {
        if (position_ + sizeof(T) > file_size_)
        {
            throw CatlV1Error("Not enough data to read structure");
        }

        T result;
        std::memcpy(&result, data_ + position_, sizeof(T));
        position_ += sizeof(T);
        return result;
    }

private:
    /**
     * Read and validate the CATL header
     */
    void
    read_header();

    /**
     * Check if the header is valid
     *
     * @return true if the header was successfully validated
     */
    bool
    valid() const;

    /**
     * Read a uint8_t from current position and advance
     * @return The byte value
     * @throws CatlV1Error if EOF
     */
    uint8_t
    read_uint8();

    /**
     * Read a uint32_t from current position and advance
     * @param value Reference to store the value
     * @throws CatlV1Error if insufficient data
     */
    void
    read_uint32(uint32_t& value);

    /**
     * Validate that we have enough data for the given size
     * @param size Size to validate
     * @throws CatlV1Error if insufficient data
     */
    void
    validate_data_size(uint32_t size) const;

    boost::iostreams::mapped_file_source mmap_file_;
    const uint8_t* data_ = nullptr;
    size_t file_size_ = 0;
    size_t position_ = 0;

    CatlHeader header_{};
    std::string filename_;
    int compression_level_{0};
    int catalogue_version_{0};
    bool valid_{false};
};

}  // namespace catl::v1