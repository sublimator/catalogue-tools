#include "catl/v1/catl-v1-writer.h"
#include "catl/core/log-macros.h"
#include "catl/crypto/sha512-half-hasher.h"
#include "catl/shamap/shamap-diff.h"
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

Writer::Writer(
    std::shared_ptr<std::ostream> header_stream,
    std::shared_ptr<std::ostream> body_stream,
    const WriterOptions& options)
    : header_stream_(std::move(header_stream))
    , body_stream_(std::move(body_stream))
    , options_(options)
    , header_written_(false)
    , finalized_(false)
    , body_bytes_written_(0)
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
    // Create and open the file
    auto file_stream = std::make_shared<std::fstream>(
        path,
        std::ios::binary | std::ios::in | std::ios::out | std::ios::trunc);

    if (!file_stream->good())
    {
        throw CatlV1Error("Failed to open output file: " + path);
    }

    if (options.compression_level == 0)
    {
        // For uncompressed files, use the same file stream for both header and
        // body
        return std::make_unique<Writer>(file_stream, file_stream, options);
    }
    else
    {
        // For compressed files, create a filtering_ostream with zlib
        // compression
        auto filter_stream =
            std::make_shared<boost::iostreams::filtering_ostream>();

        // Configure zlib compression
        boost::iostreams::zlib_params params;
        params.level = options.compression_level;
        params.method = boost::iostreams::zlib::deflated;
        params.noheader = false;  // Use zlib header/footer

        // Add the compressor to the stream chain
        filter_stream->push(boost::iostreams::zlib_compressor(params));

        // Add the file stream as the sink
        filter_stream->push(*file_stream);

        LOGI(
            "Created zlib compression stream with level ",
            static_cast<int>(options.compression_level));

        // Create writer with file_stream for header and filter_stream for body
        return std::make_unique<Writer>(file_stream, filter_stream, options);
    }
}

void
Writer::write_header(uint32_t min_ledger, uint32_t max_ledger)
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

    // Check stream state after write
    if (!header_stream_->good())
    {
        throw CatlV1Error("Failed to write file header to stream");
    }

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
}

void
Writer::write_ledger_header(const LedgerInfo& header)
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

    // Check stream state after write
    if (!body_stream_->good())
    {
        throw CatlV1Error("Failed to write ledger header to stream");
    }

    // Track the ledger header write
    track_write(WriteType::LEDGER_HEADER, sizeof(LedgerInfo));

    LOGD("Wrote ledger header for sequence ", header.sequence);
}

void
Writer::write_map(const SHAMap& map, SHAMapNodeType node_type)
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
        try
        {
            write_item(
                node_type,
                item.key(),
                item.slice().data(),
                item.slice().size());
            items_count++;
        }
        catch (const CatlV1Error& e)
        {
            LOGE(
                "Failed to write item with key: ",
                item.key().hex(),
                ": ",
                e.what());
            throw;  // Re-throw to abort the whole operation
        }
    });

    // Write terminal marker
    write_terminal();

    // Check final stream state
    if (!body_stream_->good())
    {
        throw CatlV1Error("Stream error after writing map");
    }

    LOGI(
        "Wrote map of type ",
        static_cast<int>(node_type),
        " with ",
        items_count,
        " items");
}

void
Writer::write_map_delta(
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

    // Create shared_ptr wrappers for the maps to use with SHAMapDiff
    auto prev_ptr = std::make_shared<SHAMap>(previous);
    auto curr_ptr = std::make_shared<SHAMap>(current);

    // Create a diff object and find differences
    SHAMapDiff diff(prev_ptr, curr_ptr);
    diff.find();

    // Count of changes written
    uint32_t changes = 0;

    // Process removals
    for (const auto& key : diff.deleted())
    {
        try
        {
            write_item(tnREMOVE, key, nullptr, 0);
            changes++;
        }
        catch (const CatlV1Error& e)
        {
            LOGE(
                "Failed to write removal for key: ", key.hex(), ": ", e.what());
            throw;  // Re-throw to abort the whole operation
        }
    }

    // Process modifications
    for (const auto& key : diff.modified())
    {
        // Get the item from the current map
        auto item_ptr = current.get_item(key);
        if (item_ptr)
        {
            try
            {
                write_item(
                    node_type,
                    item_ptr->key(),
                    item_ptr->slice().data(),
                    item_ptr->slice().size());
                changes++;
            }
            catch (const CatlV1Error& e)
            {
                LOGE(
                    "Failed to write modified item for key: ",
                    key.hex(),
                    ": ",
                    e.what());
                throw;  // Re-throw to abort the whole operation
            }
        }
        else
        {
            throw CatlV1Error(
                "Modified item not found in current map: " + key.hex());
        }
    }

    // Process additions
    for (const auto& key : diff.added())
    {
        // Get the item from the current map
        auto item_ptr = current.get_item(key);
        if (item_ptr)
        {
            try
            {
                write_item(
                    node_type,
                    item_ptr->key(),
                    item_ptr->slice().data(),
                    item_ptr->slice().size());
                changes++;
            }
            catch (const CatlV1Error& e)
            {
                LOGE(
                    "Failed to write added item for key: ",
                    key.hex(),
                    ": ",
                    e.what());
                throw;  // Re-throw to abort the whole operation
            }
        }
        else
        {
            throw CatlV1Error(
                "Added item not found in current map: " + key.hex());
        }
    }

    // Write terminal marker
    write_terminal();

    // Check final stream state
    if (!body_stream_->good())
    {
        throw CatlV1Error("Stream error after writing map delta");
    }

    LOGI("Wrote map delta with ", changes, " changes");
}

void
Writer::write_ledger(
    const LedgerInfo& header,
    const SHAMap& state_map,
    const SHAMap& tx_map)
{
    // Write the ledger header
    write_ledger_header(header);

    // Write the state map
    write_map(state_map, tnACCOUNT_STATE);

    // Write the transaction map
    write_map(tx_map, tnTRANSACTION_MD);

    LOGI("Successfully wrote complete ledger ", header.sequence);
}

void
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

    // If using compression, we need to ensure all compressed data is written to
    // disk
    if (options_.compression_level > 0)
    {
        // For compressed streams, we need to reset the filtering_ostream to
        // ensure all buffered data is flushed and the zlib stream is properly
        // finalized
        if (auto* filter_stream =
                dynamic_cast<boost::iostreams::filtering_ostream*>(
                    body_stream_.get()))
        {
            try
            {
                LOGI("Finalizing compressed stream");
                filter_stream
                    ->reset();  // This properly closes all filters in the chain
            }
            catch (const boost::iostreams::zlib_error& e)
            {
                throw CatlV1Error(
                    "Error finalizing zlib stream: " + std::string(e.what()));
            }
            catch (const std::exception& e)
            {
                throw CatlV1Error(
                    "Error finalizing compression stream: " +
                    std::string(e.what()));
            }
        }
    }

    // Flush the header stream
    header_stream_->flush();

    // Get file size by seeking to the end of the header stream
    // This works because the header stream is the direct file stream
    auto* file_stream = dynamic_cast<std::fstream*>(header_stream_.get());
    if (file_stream)
    {
        file_stream->seekp(0, std::ios::end);

        if (!file_stream->good())
        {
            throw CatlV1Error(
                "Failed to seek to end of file for size calculation");
        }

        header_.filesize = static_cast<uint64_t>(file_stream->tellp());

        // Create a hasher
        catl::crypto::Sha512Hasher hasher;

        // Create a temporary header with zeroed hash field
        CatlHeader temp_header = header_;
        std::fill(temp_header.hash.begin(), temp_header.hash.end(), 0);

        // Hash the header
        hasher.update(
            reinterpret_cast<const uint8_t*>(&temp_header),
            sizeof(temp_header));

        // Hash the rest of the file
        file_stream->clear();
        file_stream->seekg(sizeof(CatlHeader));

        if (!file_stream->good())
        {
            throw CatlV1Error(
                "Failed to seek to start of file content for hashing");
        }

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

            if (file_stream->bad())
            {
                throw CatlV1Error(
                    "Error occurred while reading file content for hashing");
            }
        }

        // Get the final hash
        unsigned int hash_len = 0;
        hasher.final(header_.hash.begin(), &hash_len);

        // Copy hash to header
        if (hash_len != header_.hash.size())
        {
            throw CatlV1Error(
                "Hash length mismatch: expected " +
                std::to_string(header_.hash.size()) + " bytes, got " +
                std::to_string(hash_len) + " bytes");
        }

        // Update the header in the file
        file_stream->clear();
        file_stream->seekp(0);
        file_stream->write(
            reinterpret_cast<const char*>(&header_), sizeof(header_));
        file_stream->flush();

        if (!file_stream->good())
        {
            throw CatlV1Error(
                "Failed to update header with hash and file size");
        }

        LOGI("Finalized CATL file: size=", header_.filesize, " bytes");
    }
    else
    {
        LOGW("Unable to determine file size - not a file stream");
        if (!header_stream_->good() || !body_stream_->good())
        {
            throw CatlV1Error("Stream error occurred during finalization");
        }
    }

    finalized_ = true;
}

void
Writer::write_item(
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
    uint8_t type_byte = node_type;
    body_stream_->write(
        reinterpret_cast<const char*>(&type_byte), sizeof(type_byte));

    if (!body_stream_->good())
    {
        throw CatlV1Error("Failed to write node type");
    }

    // Write key
    body_stream_->write(reinterpret_cast<const char*>(key.data()), Key::size());

    if (!body_stream_->good())
    {
        throw CatlV1Error("Failed to write key: " + key.hex());
    }

    // For node types other than tnREMOVE, write data size and data
    if (node_type != tnREMOVE)
    {
        // Write data size
        body_stream_->write(reinterpret_cast<const char*>(&size), sizeof(size));

        if (!body_stream_->good())
        {
            throw CatlV1Error(
                "Failed to write data size for key: " + key.hex());
        }

        // Write data if present
        if (size > 0 && data != nullptr)
        {
            body_stream_->write(reinterpret_cast<const char*>(data), size);

            if (!body_stream_->good())
            {
                throw CatlV1Error(
                    "Failed to write item data for key: " + key.hex());
            }
        }
    }

    // Track the write
    track_write(WriteType::MAP_ITEM, item_size);
}

void
Writer::write_terminal()
{
    // Write the terminal node type
    uint8_t terminal = static_cast<uint8_t>(tnTERMINAL);
    body_stream_->write(
        reinterpret_cast<const char*>(&terminal), sizeof(terminal));

    if (!body_stream_->good())
    {
        throw CatlV1Error("Failed to write terminal marker");
    }

    // Track the terminal marker write
    track_write(WriteType::TERMINAL, sizeof(terminal));
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

void
Writer::write_raw_data(const uint8_t* data, size_t size)
{
    if (!header_written_ || finalized_)
    {
        throw CatlV1Error(
            "Cannot write data: file not initialized or already finalized");
    }

    // Write to the body stream
    body_stream_->write(reinterpret_cast<const char*>(data), size);

    if (!body_stream_->good())
    {
        throw CatlV1Error("Failed to write raw data to body stream");
    }

    // Track bytes written
    track_write(WriteType::MAP_ITEM, size);
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
            // We don't rethrow here as destructors should not throw
        }
    }
    else if (options_.compression_level > 0 && !finalized_)
    {
        // If we have a compression stream but weren't able to finalize
        // properly, at least try to reset the filtering_ostream to flush any
        // buffered data
        try
        {
            if (auto* filter_stream =
                    dynamic_cast<boost::iostreams::filtering_ostream*>(
                        body_stream_.get()))
            {
                LOGW(
                    "Flushing compressed stream on destruction without "
                    "finalization");
                filter_stream->flush();
                filter_stream->reset();
            }
        }
        catch (const std::exception& e)
        {
            LOGE(
                "Error flushing compression stream during destruction: ",
                e.what());
            // Destructors should not throw
        }
    }

    // Clear smart pointers to ensure proper order of destruction
    body_stream_.reset();
    header_stream_.reset();
}
}  // namespace catl::v1