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

namespace catl::v1 {

/**
 * Stream wrapper that provides zlib compression
 * This is a placeholder implementation - would need a real zlib wrapper
 * for production use.
 */
class CompressedOutputStream : public std::ostream
{
private:
    std::ostream& underlying_stream_;
    uint8_t compression_level_;

    // Custom buffer for compression
    class CompressionBuffer : public std::streambuf
    {
    private:
        std::ostream& output_;
        std::vector<char> buffer_;
        uint8_t compression_level_;

    public:
        CompressionBuffer(
            std::ostream& output,
            uint8_t compression_level,
            size_t buffer_size = 8192)
            : output_(output)
            , buffer_(buffer_size)
            , compression_level_(compression_level)
        {
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
                // In a real implementation, we'd compress the data here
                // For now, we just pass it through
                output_.write(pbase(), size);

                // Reset buffer pointers
                pbump(-static_cast<int>(size));
            }
            return 0;
        }
    };

    // Custom buffer instance
    CompressionBuffer buffer_;

public:
    CompressedOutputStream(std::ostream& stream, uint8_t compression_level)
        : std::ostream(&buffer_)
        , underlying_stream_(stream)
        , compression_level_(compression_level)
        , buffer_(stream, compression_level)
    {
    }

    void
    flush()
    {
        std::ostream::flush();
        underlying_stream_.flush();
    }
};

Writer::Writer(
    std::shared_ptr<std::ostream> header_stream,
    std::shared_ptr<std::ostream> body_stream,
    const WriterOptions& options)
    : header_stream_(std::move(header_stream))
    , body_stream_(std::move(body_stream))
    , options_(options)
    , header_written_(false)
    , finalized_(false)
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
    std::memset(header_.hash, 0, sizeof(header_.hash));

    // Write header to header stream
    header_stream_->write(
        reinterpret_cast<const char*>(&header_), sizeof(header_));
    header_stream_->flush();

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
        bool is_new = previous_items.find(key) == previous_items.end();

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

        // Calculate hash over entire file
        file_stream->seekg(0);

        // Create a buffer for reading the file
        std::vector<char> buffer(1024 * 1024);  // 1MB buffer
        catl::crypto::SHA512HalfHasher hasher;

        // Zero out the hash field before computing the hash
        file_stream->seekg(offsetof(CatlHeader, hash));
        std::vector<char> zeros(32, 0);
        file_stream->read(zeros.data(), zeros.size());

        // Reset to beginning for hash computation
        file_stream->seekg(0);

        // Read and hash the file in chunks
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
        Hash256 hash = hasher.finalize();

        // Copy hash to header
        std::memcpy(header_.hash, hash.data(), hash.size());

        // Update header with final size and hash
        file_stream->clear();  // Clear EOF flag
        file_stream->seekp(0);
        file_stream->write(
            reinterpret_cast<const char*>(&header_), sizeof(header_));
        file_stream->flush();

        LOGI(
            "Finalized CATL file: size=",
            header_.filesize,
            " bytes, hash=",
            hash.hex());
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

    return true;
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