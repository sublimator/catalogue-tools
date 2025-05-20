#include "catl/v1/catl-v1-mmap-reader.h"
#include "catl/core/types.h"
#include "catl/shamap/shamap.h"
#include "catl/v1/catl-v1-utils.h"
#include <boost/filesystem.hpp>
#include <cstring>
#include <utility>

namespace catl::v1 {

MmapReader::MmapReader(std::string filename) : filename_(std::move(filename))
{
    try
    {
        // Check if file exists
        if (!boost::filesystem::exists(filename_))
        {
            throw CatlV1Error("File does not exist: " + filename_);
        }

        // Check file size
        boost::uintmax_t file_size = boost::filesystem::file_size(filename_);
        if (file_size == 0)
        {
            throw CatlV1Error("File is empty: " + filename_);
        }

        // Open the memory-mapped file
        mmap_file_.open(filename_);
        if (!mmap_file_.is_open())
        {
            throw CatlV1Error("Failed to memory map file: " + filename_);
        }

        // Set up internal state
        data_ = reinterpret_cast<const uint8_t*>(mmap_file_.data());
        file_size_ = mmap_file_.size();

        if (!data_)
        {
            throw CatlV1Error(
                "Memory mapping succeeded but data pointer is null");
        }

        // Read the header
        read_header();

        // Set position to just after the header for subsequent reads
        position_ = sizeof(CatlHeader);

        // Check for compression - memory-mapped reader doesn't support
        // compressed files
        if (compression_level_ > 0)
        {
            throw CatlV1Error(
                "MmapReader does not support compressed CATL files. Use Reader "
                "instead.");
        }
    }
    catch (const boost::filesystem::filesystem_error& e)
    {
        throw CatlV1Error("Filesystem error: " + std::string(e.what()));
    }
    catch (const std::ios_base::failure& e)
    {
        throw CatlV1Error("I/O error: " + std::string(e.what()));
    }
    catch (const CatlV1Error&)
    {
        // Just re-throw CatlV1Error exceptions
        throw;
    }
    catch (const std::exception& e)
    {
        throw CatlV1Error("Error during file setup: " + std::string(e.what()));
    }
}

MmapReader::~MmapReader()
{
    if (mmap_file_.is_open())
    {
        mmap_file_.close();
    }
}

void
MmapReader::read_header()
{
    // Ensure the file is large enough to contain a header
    if (file_size_ < sizeof(CatlHeader))
    {
        throw CatlV1InvalidHeaderError(
            "File too small to contain a valid CATL header");
    }

    // Copy the header from the memory-mapped file
    std::memcpy(&header_, data_, sizeof(CatlHeader));

    // Validate magic number
    if (header_.magic != CATL_MAGIC)
    {
        throw CatlV1InvalidHeaderError("Invalid CATL magic value in header");
    }

    // Extract and validate version
    catalogue_version_ = get_catalogue_version(header_.version);
    if (catalogue_version_ != BASE_CATALOGUE_VERSION)
    {
        throw CatlV1UnsupportedVersionError(
            "Unsupported CATL version: " + std::to_string(catalogue_version_));
    }

    // Extract compression level
    compression_level_ = get_compression_level(header_.version);

    // Check file size
    if (header_.filesize != file_size_)
    {
        throw CatlV1FileSizeMismatchError(
            "File size does not match header value");
    }

    // Header is valid
    valid_ = true;
}

const CatlHeader&
MmapReader::header() const
{
    return header_;
}

bool
MmapReader::valid() const
{
    return valid_;
}

int
MmapReader::compression_level() const
{
    return compression_level_;
}

int
MmapReader::catalogue_version() const
{
    return catalogue_version_;
}

const uint8_t*
MmapReader::data_at(size_t offset) const
{
    if (offset >= file_size_)
    {
        throw CatlV1Error("Requested offset is beyond file bounds");
    }
    return data_ + offset;
}

size_t
MmapReader::file_size() const
{
    return file_size_;
}

size_t
MmapReader::position() const
{
    return position_;
}

void
MmapReader::set_position(size_t pos)
{
    if (pos > file_size_)
    {
        throw CatlV1Error("Attempted to set position beyond file bounds");
    }
    position_ = pos;
}

bool
MmapReader::eof() const
{
    return position_ >= file_size_;
}

LedgerInfoView
MmapReader::read_ledger_info()
{
    // Check if we have enough data left to read a ledger header
    if (position_ + sizeof(LedgerInfo) > file_size_)
    {
        throw CatlV1Error("Not enough data left to read ledger header");
    }

    // Create a view directly into the memory-mapped data
    auto view = LedgerInfoView(data_ + position_);

    // Advance the position
    position_ += sizeof(LedgerInfo);

    return view;
}

LedgerInfoView
MmapReader::get_ledger_info_view(size_t position) const
{
    // Check if the requested position is valid
    if (position + sizeof(LedgerInfo) > file_size_)
    {
        throw CatlV1Error("Invalid position for ledger header view");
    }

    // Create a view directly into the memory-mapped data at the specified
    // position
    return LedgerInfoView(data_ + position);
}

uint32_t
MmapReader::read_shamap(shamap::SHAMap& map, shamap::SHAMapNodeType leaf_type)
{
    uint32_t nodes_processed_count = 0;
    bool found_terminal = false;

    while (position_ < file_size_ && !found_terminal)
    {
        // Read node type
        uint8_t node_type_val = *(data_ + position_);
        position_++;
        auto node_type = static_cast<shamap::SHAMapNodeType>(node_type_val);

        if (node_type == shamap::tnTERMINAL)
        {
            found_terminal = true;
            break;
        }

        // Validate node type
        if (node_type != shamap::tnINNER &&
            node_type != shamap::tnTRANSACTION_NM &&
            node_type != shamap::tnTRANSACTION_MD &&
            node_type != shamap::tnACCOUNT_STATE &&
            node_type != shamap::tnREMOVE)
        {
            throw CatlV1Error(
                "Invalid node type encountered: " +
                std::to_string(static_cast<int>(node_type_val)));
        }

        // Read key (32 bytes)
        if (position_ + Key::size() > file_size_)
        {
            throw CatlV1Error("Unexpected EOF reading key");
        }
        const uint8_t* key_data = data_ + position_;
        Key item_key(key_data);
        position_ += Key::size();

        // Handle REMOVE nodes
        if (node_type == shamap::tnREMOVE)
        {
            if (leaf_type ==
                shamap::tnACCOUNT_STATE)  // Only allow removals for state maps
            {
                if (map.remove_item(item_key))
                {
                    nodes_processed_count++;
                }
                else
                {
                    throw CatlV1Error(
                        "Failed to remove state item (may not exist)");
                }
            }
            else
            {
                throw CatlV1Error(
                    "Found unexpected tnREMOVE node in non-state map");
            }
            continue;
        }

        // Read data size (4 bytes)
        if (position_ + sizeof(uint32_t) > file_size_)
        {
            throw CatlV1Error("Unexpected EOF reading data size");
        }
        uint32_t data_size = 0;
        std::memcpy(&data_size, data_ + position_, sizeof(uint32_t));
        position_ += sizeof(uint32_t);

        // Validate data size
        constexpr uint32_t MAX_REASONABLE_DATA_SIZE = 5 * 1024 * 1024;  // 5 MiB
        if (position_ > file_size_ ||
            (data_size > 0 && position_ + data_size > file_size_) ||
            data_size > MAX_REASONABLE_DATA_SIZE)
        {
            throw CatlV1Error("Invalid data size or EOF reached");
        }

        // Create MmapItem (zero-copy reference)
        const uint8_t* item_data_ptr = data_ + position_;
        auto item = boost::intrusive_ptr(
            new MmapItem(key_data, item_data_ptr, data_size));

        // Add item to the map
        if (map.set_item(item) != shamap::SetResult::FAILED)
        {
            nodes_processed_count++;
        }
        else
        {
            throw CatlV1Error("Failed to add item to SHAMap");
        }

        // Advance position past the data
        position_ += data_size;
    }

    return nodes_processed_count;
}

void
MmapReader::verify_file_hash()
{
    // If hash is all zeros, it's not set
    bool hash_is_zero = true;
    for (auto b : header_.hash)
    {
        if (b != 0)
        {
            hash_is_zero = false;
            break;
        }
    }

    if (hash_is_zero)
    {
        throw CatlV1HashVerificationError(
            "Cannot verify file hash: Header hash field is empty");
    }

    // Create a hasher
    catl::crypto::Sha512Hasher hasher;

    // First hash the header with zeroed hash field
    CatlHeader header_copy = header_;
    std::fill(header_copy.hash.begin(), header_copy.hash.end(), 0);

    // Update hasher with header
    hasher.update(
        reinterpret_cast<const uint8_t*>(&header_copy), sizeof(CatlHeader));

    // Hash the rest of the file in chunks
    const size_t buffer_size = 64 * 1024;  // 64KB buffer
    const uint8_t* current_pos = data_ + sizeof(CatlHeader);
    size_t remaining_bytes = file_size_ - sizeof(CatlHeader);

    while (remaining_bytes > 0)
    {
        size_t bytes_to_hash = std::min(remaining_bytes, buffer_size);
        hasher.update(current_pos, bytes_to_hash);

        current_pos += bytes_to_hash;
        remaining_bytes -= bytes_to_hash;
    }

    // Get the final hash
    unsigned char computed_hash[64];  // SHA-512 produces 64 bytes
    unsigned int hash_len = 0;
    hasher.final(computed_hash, &hash_len);

    // Verify hash length
    if (hash_len != header_.hash.size())
    {
        throw CatlV1HashVerificationError(
            "Hash length mismatch: expected " +
            std::to_string(header_.hash.size()) + " bytes, got " +
            std::to_string(hash_len) + " bytes");
    }

    // Compare computed hash with stored hash
    bool matches =
        (std::memcmp(computed_hash, header_.hash.data(), hash_len) == 0);

    if (!matches)
    {
        throw CatlV1HashVerificationError("File hash verification failed");
    }
}

}  // namespace catl::v1