#include "catl/v1/catl-v1-writer.h"
#include "catl/core/log-macros.h"
#include "catl/crypto/sha512-half-hasher.h"
#include "catl/v1/catl-v1-errors.h"
#include "catl/v1/catl-v1-utils.h"
#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>

// For zlib compression
#include <boost/iostreams/filter/zlib.hpp>
#include <boost/iostreams/filtering_stream.hpp>

namespace catl::v1 {

/**
 * Stream wrapper that provides zlib compression using Boost.Iostreams
 */
class CompressedOutputStream : public std::ostream
{
private:
    std::shared_ptr<boost::iostreams::filtering_ostream> filter_stream_;
    std::ostream& underlying_stream_;
    std::vector<char> buffer_;

    // Forward declaration of buffer class
    class CompressionBuffer;
    std::unique_ptr<CompressionBuffer> buffer_impl_;

    // Custom buffer for handling the data flow
    class CompressionBuffer : public std::streambuf
    {
    private:
        boost::iostreams::filtering_ostream& filter_;
        std::vector<char>& buffer_;

    public:
        CompressionBuffer(
            boost::iostreams::filtering_ostream& filter,
            std::vector<char>& buffer,
            size_t buffer_size = 8192)
            : filter_(filter), buffer_(buffer)
        {
            buffer_.resize(buffer_size);
            // Set buffer pointers
            setp(buffer_.data(), buffer_.data() + buffer_.size());
        }

        ~CompressionBuffer()
        {
            sync();
        }

    protected:
        int_type
        overflow(int_type ch) override
        {
            if (ch != traits_type::eof())
            {
                // Buffer is full, compress and write data
                *pptr() = static_cast<char>(ch);
                pbump(1);
                sync();
                return ch;
            }
            return traits_type::eof();
        }

        int
        sync() override
        {
            // Get size of data in buffer
            auto size = pptr() - pbase();
            if (size > 0)
            {
                // Write to the filtering stream which compresses to the
                // underlying stream
                filter_.write(pbase(), size);

                // Reset buffer pointers
                pbump(-static_cast<int>(size));
            }
            return 0;
        }
    };

public:
    CompressedOutputStream(std::ostream& stream, uint8_t compression_level)
        : std::ostream(nullptr)  // We'll set the buffer after initialization
        , filter_stream_(
              std::make_shared<boost::iostreams::filtering_ostream>())
        , underlying_stream_(stream)
        , buffer_(8192)  // Initialize with 8KB buffer
    {
        // Configure the zlib compression filter
        boost::iostreams::zlib_params params;
        params.level = compression_level;
        params.method = boost::iostreams::zlib::deflated;
        params.noheader = false;  // Use zlib header/footer

        // Add the compressor to the filtering stream
        filter_stream_->push(boost::iostreams::zlib_compressor(params));

        // Add the underlying output stream last in the chain
        filter_stream_->push(underlying_stream_);

        // Create and set the buffer
        buffer_impl_ =
            std::make_unique<CompressionBuffer>(*filter_stream_, buffer_);
        rdbuf(buffer_impl_.get());

        LOGI(
            "Created zlib compression stream with level ",
            static_cast<int>(compression_level));
    }

    ~CompressedOutputStream()
    {
        flush();
        // Make sure to properly finish the zlib stream
        if (filter_stream_)
        {
            filter_stream_->reset();
        }
    }

    void
    flush()
    {
        std::ostream::flush();  // Flush our buffer to the filtering_ostream
        if (filter_stream_)
        {
            filter_stream_
                ->flush();  // Flush the compression to the underlying stream
        }
        underlying_stream_.flush();  // Flush the underlying stream
    }
};

Writer::Writer(
    std::shared_ptr<std::ostream> header_stream,
    std::shared_ptr<std::ostream> body_stream,
    const WriterOptions& options,
    std::string file_path)
    : header_stream_(std::move(header_stream))
    , body_stream_(std::move(body_stream))
    , options_(options)
    , header_written_(false)
    , finalized_(false)
    , body_bytes_written_(0)
    , file_path_(std::move(file_path))
{
    // Validate options
    if (options_.compression_level > 9)
    {
        throw CatlV1Error(
            "Invalid compression level: " +
            std::to_string(options_.compression_level) +
            ". Must be between 0 and 9.");
    }

    LOGI(
        "Created CATL writer with compression level ",
        options_.compression_level);
}

std::unique_ptr<Writer>
Writer::for_file(const std::string& path, const WriterOptions& options)
{
    if (options.compression_level == 0)
    {
        // For uncompressed files, use the same file stream for both header and
        // body
        auto file_stream = std::make_shared<std::fstream>(
            path,
            std::ios::binary | std::ios::in | std::ios::out | std::ios::trunc);

        if (!file_stream->good())
        {
            throw CatlV1Error("Failed to open output file: " + path);
        }

        // Use the same stream for both header and body
        return std::make_unique<Writer>(file_stream, file_stream, options);
    }
    else
    {
        // For compressed files, we need separate handling

        // First, create and open the file
        auto file_stream = std::make_shared<std::fstream>(
            path,
            std::ios::binary | std::ios::in | std::ios::out | std::ios::trunc);

        if (!file_stream->good())
        {
            throw CatlV1Error("Failed to open output file: " + path);
        }

        // Create a compressed wrapper for the body
        auto compressed_stream = std::make_shared<CompressedOutputStream>(
            *file_stream, options.compression_level);

        // Create writer with file_stream for header and compressed_stream for
        // body
        return std::make_unique<Writer>(
            file_stream, compressed_stream, options);
    }
}

bool
Writer::writeHeader(uint32_t min_ledger, uint32_t max_ledger)
{
    if (header_written_)
    {
        throw CatlV1Error("Header already written");
    }

    // Validate ledger range
    if (min_ledger > max_ledger)
    {
        throw CatlV1Error("Invalid ledger range: min > max");
    }

    // Create header structure
    header_.magic = CATL_MAGIC;
    header_.version = BASE_CATALOGUE_VERSION;
    if (options_.compression_level > 0)
    {
        header_.version |= (options_.compression_level << 8);
    }
    header_.network_id = options_.network_id;
    header_.min_ledger = min_ledger;
    header_.max_ledger = max_ledger;
    header_.filesize = 0;  // Will update in finalize()

    // Initialize hash field to zeros (hash will be computed during finalize)
    std::memset(header_.hash.begin(), 0, sizeof(header_.hash));

    // Write header to header stream
    header_stream_->write(
        reinterpret_cast<const char*>(&header_), sizeof(header_));
    header_stream_->flush();

    // Track the header write
    track_write(WriteType::HEADER, sizeof(header_));

    LOGI(
        "Wrote CATL header: ledger range ",
        min_ledger,
        "-",
        max_ledger,
        ", network ID ",
        options_.network_id,
        ", compression level ",
        options_.compression_level);

    header_written_ = true;
    return header_stream_->good();
}

bool
Writer::writeLedgerHeader(const LedgerInfo& header)
{
    if (!header_written_)
    {
        throw CatlV1Error("Cannot write ledger before file header");
    }

    if (finalized_)
    {
        throw CatlV1Error("Cannot write ledger after finalization");
    }

    // Write ledger header to the body stream
    body_stream_->write(
        reinterpret_cast<const char*>(&header), sizeof(LedgerInfo));

    // Track the ledger header write
    track_write(WriteType::LEDGER_HEADER, sizeof(LedgerInfo));

    LOGD("Wrote ledger header for sequence ", header.sequence);

    return body_stream_->good();
}

bool
Writer::writeMap(const SHAMap& map, SHAMapNodeType node_type)
{
    if (!header_written_)
    {
        throw CatlV1Error("Cannot write map before file header");
    }

    if (finalized_)
    {
        throw CatlV1Error("Cannot write map after finalization");
    }

    // Count of items written
    uint32_t items_count = 0;

    // Use visitor pattern to write all items
    map.visit_items([&](const MmapItem& item) {
        if (writeItem(
                node_type,
                item.key(),
                item.slice().data(),
                item.slice().size()))
        {
            items_count++;
        }
        else
        {
            LOGE("Failed to write item with key: ", item.key().hex());
        }
    });

    // Write terminal marker
    if (!writeTerminal())
    {
        LOGE("Failed to write terminal marker");
        return false;
    }

    LOGI(
        "Wrote map of type ",
        static_cast<int>(node_type),
        " with ",
        items_count,
        " items");

    return body_stream_->good();
}

bool
Writer::writeMapDelta(
    const SHAMap& previous,
    const SHAMap& current,
    SHAMapNodeType node_type)
{
    if (!header_written_)
    {
        throw CatlV1Error("Cannot write map delta before file header");
    }

    if (finalized_)
    {
        throw CatlV1Error("Cannot write map delta after finalization");
    }

    // Track items and their existence in both maps
    std::map<Key, bool> previous_items;
    std::map<Key, const MmapItem*> current_items;

    // Fill the maps with items
    previous.visit_items(
        [&](const MmapItem& item) { previous_items[item.key()] = true; });

    current.visit_items(
        [&](const MmapItem& item) { current_items[item.key()] = &item; });

    // Count of changes written
    uint32_t changes = 0;

    // Process removals (items in previous but not in current)
    for (const auto& [key, _] : previous_items)
    {
        if (current_items.find(key) == current_items.end())
        {
            // This key exists in previous but not in current, so it was removed
            if (writeItem(tnREMOVE, key, nullptr, 0))
            {
                changes++;
            }
            else
            {
                LOGE("Failed to write removal for key: ", key.hex());
            }
        }
    }

    // Process additions and modifications
    for (const auto& [key, item_ptr] : current_items)
    {
        // For new items or modified items, write them to the file
        // Note: We don't have a way to detect if an item changed without
        // comparing the actual data, so we write all items that exist in
        // current
        if (writeItem(
                node_type,
                item_ptr->key(),
                item_ptr->slice().data(),
                item_ptr->slice().size()))
        {
            changes++;
        }
        else
        {
            LOGE("Failed to write item for key: ", key.hex());
        }
    }

    // Write terminal marker
    if (!writeTerminal())
    {
        LOGE("Failed to write terminal marker");
        return false;
    }

    LOGI("Wrote map delta with ", changes, " changes");

    return body_stream_->good();
}

bool
Writer::writeLedger(
    const LedgerInfo& header,
    const SHAMap& state_map,
    const SHAMap& tx_map)
{
    // Write the ledger header
    if (!writeLedgerHeader(header))
    {
        return false;
    }

    // Write the state map
    if (!writeMap(state_map, tnACCOUNT_STATE))
    {
        return false;
    }

    // Write the transaction map
    if (!writeMap(tx_map, tnTRANSACTION_MD))
    {
        return false;
    }

    LOGI("Successfully wrote complete ledger ", header.sequence);

    return true;
}

bool
Writer::finalize()
{
    if (!header_written_)
    {
        throw CatlV1Error("Cannot finalize before writing header");
    }

    if (finalized_)
    {
        throw CatlV1Error("Already finalized");
    }

    // Flush the body stream to ensure all data is written
    body_stream_->flush();

    // Flush the header stream
    header_stream_->flush();

    // Get file size by seeking to the end of the header stream
    // This works because the header stream is the direct file stream
    auto* file_stream = dynamic_cast<std::fstream*>(header_stream_.get());
    if (file_stream)
    {
        file_stream->seekp(0, std::ios::end);
        header_.filesize = static_cast<uint64_t>(file_stream->tellp());

        // Use direct file access if a path was provided
        if (!file_path_.empty())
        {
            // Ensure existing streams are flushed
            file_stream->flush();
            body_stream_->flush();

            // Reopen the file directly for hash calculation
            std::fstream direct_file(
                file_path_, std::ios::binary | std::ios::in | std::ios::out);
            if (!direct_file.good())
            {
                LOGE(
                    "Failed to reopen file for hash calculation: ", file_path_);
                return false;
            }

            // Read the header
            CatlHeader temp_header;
            direct_file.read(
                reinterpret_cast<char*>(&temp_header), sizeof(CatlHeader));

            // Zero out the hash field in memory
            std::fill(temp_header.hash.begin(), temp_header.hash.end(), 0);

            // Create hasher for reliable calculation
            catl::crypto::Sha512Hasher hasher;

            // Start by hashing the header with zeroed hash
            hasher.update(&temp_header, sizeof(temp_header));

            // Then hash the rest of the file
            direct_file.seekg(sizeof(CatlHeader));

            // Create a buffer for reading the file
            std::vector<char> buffer(1024 * 1024);  // 1MB buffer

            // Read and hash the file in chunks
            while (!direct_file.eof())
            {
                direct_file.read(
                    buffer.data(),
                    buffer
                        .size());  // TODO: why does catl-decomp use sizeof here
                std::streamsize bytes_read = direct_file.gcount();
                if (bytes_read > 0)
                {
                    hasher.update(buffer.data(), bytes_read);
                }
            }

            // Get the final hash
            unsigned int hash_len = 0;
            hasher.final(header_.hash.begin(), &hash_len);

            // Copy hash to header
            if (hash_len != header_.hash.size())
            {
                LOGE(
                    "Hash length mismatch: expected ",
                    header_.hash.size(),
                    " bytes, got ",
                    hash_len,
                    " bytes");
                return false;
            }

            // Update the header in the file
            direct_file.clear();  // Clear EOF flag
            direct_file.seekp(0);
            direct_file.write(
                reinterpret_cast<const char*>(&header_), sizeof(header_));
            direct_file.flush();
            direct_file.close();

            LOGI(
                "Finalized CATL file: size=",
                header_.filesize,
                " using direct file access: ",
                file_path_);
        }
        else
        {
            // Fall back to stream-based approach

            // Create a hasher
            catl::crypto::Sha512Hasher hasher;

            // Create a temporary header with zeroed hash field
            CatlHeader tempHeader = header_;
            std::fill(tempHeader.hash.begin(), tempHeader.hash.end(), 0);

            // Hash the header
            hasher.update(
                reinterpret_cast<const uint8_t*>(&tempHeader),
                sizeof(tempHeader));

            // Hash the rest of the file
            file_stream->clear();
            file_stream->seekg(sizeof(CatlHeader));
            std::vector<char> buffer(1024 * 1024);  // 1MB buffer

            while (!file_stream->eof())
            {
                file_stream->read(buffer.data(), buffer.size());
                std::streamsize bytes_read = file_stream->gcount();
                if (bytes_read > 0)
                {
                    hasher.update(
                        reinterpret_cast<const uint8_t*>(buffer.data()),
                        bytes_read);
                }
            }

            // Get the final hash
            unsigned int hash_len = 0;
            hasher.final(header_.hash.begin(), &hash_len);

            // Copy hash to header
            if (hash_len != header_.hash.size())
            {
                LOGE(
                    "Hash length mismatch: expected ",
                    header_.hash.size(),
                    " bytes, got ",
                    hash_len,
                    " bytes");
                return false;
            }

            // Update the header in the file
            file_stream->clear();
            file_stream->seekp(0);
            file_stream->write(
                reinterpret_cast<const char*>(&header_), sizeof(header_));
            file_stream->flush();

            LOGI("Finalized CATL file: size=", header_.filesize, " bytes");
        }
    }
    else
    {
        LOGW("Unable to determine file size - not a file stream");
    }

    finalized_ = true;
    return header_stream_->good() && body_stream_->good();
}

bool
Writer::writeItem(
    SHAMapNodeType node_type,
    const Key& key,
    const uint8_t* data,
    uint32_t size)
{
    // Calculate total bytes for this item
    size_t item_size = sizeof(uint8_t) + Key::size();
    if (node_type != tnREMOVE)
    {
        item_size += sizeof(uint32_t) + size;
    }

    // Write node type
    uint8_t type_byte = static_cast<uint8_t>(node_type);
    body_stream_->write(
        reinterpret_cast<const char*>(&type_byte), sizeof(type_byte));

    if (!body_stream_->good())
    {
        LOGE("Failed to write node type");
        return false;
    }

    // Write key
    body_stream_->write(reinterpret_cast<const char*>(key.data()), Key::size());

    if (!body_stream_->good())
    {
        LOGE("Failed to write key");
        return false;
    }

    // For node types other than tnREMOVE, write data size and data
    if (node_type != tnREMOVE)
    {
        // Write data size
        body_stream_->write(reinterpret_cast<const char*>(&size), sizeof(size));

        if (!body_stream_->good())
        {
            LOGE("Failed to write data size");
            return false;
        }

        // Write data if present
        if (size > 0 && data != nullptr)
        {
            body_stream_->write(reinterpret_cast<const char*>(data), size);

            if (!body_stream_->good())
            {
                LOGE("Failed to write item data");
                return false;
            }
        }
    }

    // Track the write if successful
    track_write(WriteType::MAP_ITEM, item_size);

    return true;
}

bool
Writer::writeTerminal()
{
    // Write the terminal node type
    uint8_t terminal = static_cast<uint8_t>(tnTERMINAL);
    body_stream_->write(
        reinterpret_cast<const char*>(&terminal), sizeof(terminal));

    if (!body_stream_->good())
    {
        LOGE("Failed to write terminal marker");
        return false;
    }

    // Track the terminal marker write
    track_write(WriteType::TERMINAL, sizeof(terminal));

    return true;
}

size_t
Writer::body_bytes_written() const
{
    return body_bytes_written_;
}

void
Writer::set_write_callback(WriteCallback callback)
{
    write_callback_ = std::move(callback);
}

void
Writer::track_write(WriteType type, size_t bytes)
{
    if (type != WriteType::HEADER)
    {
        body_bytes_written_ += bytes;
    }

    if (write_callback_)
    {
        write_callback_(type, bytes);
    }
}

Writer::~Writer()
{
    // If not finalized, try to finalize on destruction
    if (header_written_ && !finalized_)
    {
        try
        {
            LOGW(
                "Writer destroyed without explicit finalize - attempting "
                "automatic finalization");
            finalize();
        }
        catch (const std::exception& e)
        {
            LOGE("Error during automatic finalization: ", e.what());
        }
    }
}

}  // namespace catl::v1