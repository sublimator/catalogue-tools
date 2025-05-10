#include "catl/v1/catl-v1-reader.h"
#include <cstring>
#include <stdexcept>
#include <utility>
#include <vector>

#include "catl/v1/catl-v1-utils.h"
#include "catl/v1/catl-v1-writer.h"

namespace catl::v1 {

size_t
Reader::read_raw_data(uint8_t* buffer, size_t size, const std::string& context)
{
    if (!input_stream_)
    {
        throw CatlV1Error("Input stream is not available for raw reading");
    }

    input_stream_->read(reinterpret_cast<char*>(buffer), size);
    size_t bytes_read = input_stream_->gcount();
    body_bytes_read_ += bytes_read;

    // If tee is enabled, also write the data to the tee stream
    if (tee_enabled_ && tee_stream_ && bytes_read > 0)
    {
        tee_stream_->write(reinterpret_cast<const char*>(buffer), bytes_read);
    }

    return bytes_read;
}

void
Reader::decompress(const std::string& output_path)
{
    // Check if file is already uncompressed
    if (compression_level_ == 0)
    {
        throw CatlV1Error("File is not compressed (level 0)");
    }

    // Create writer with uncompressed option
    WriterOptions options;
    options.network_id = header_.network_id;
    options.compression_level = 0;

    auto writer = Writer::for_file(output_path, options);

    // Copy header information (min/max ledger)
    writer->write_header(header_.min_ledger, header_.max_ledger);

    // Set up a buffer for copying
    constexpr size_t BUFFER_SIZE = 64 * 1024;  // 64KB buffer
    std::vector<uint8_t> buffer(BUFFER_SIZE);

    // Read and copy data in chunks until EOF
    while (!input_stream_->eof())
    {
        size_t bytes_read = read_raw_data(buffer.data(), buffer.size());
        if (bytes_read > 0)
        {
            writer->write_raw_data(buffer.data(), bytes_read);
        }

        // Check for errors other than EOF
        if (input_stream_->bad())
        {
            throw CatlV1Error(
                "Error reading from input file during decompression");
        }
    }

    // Finalize the output file
    writer->finalize();
}

Reader::Reader(std::string filename) : filename_(std::move(filename))
{
    file_.open(filename_, std::ios::binary);
    if (!file_.is_open())
    {
        throw CatlV1Error("Failed to open CATL file: " + filename_);
    }
    read_header();

    if (compression_level_ > 0)
    {
        // For compressed files, open file_, seek, and wrap in
        // decompressed_stream_
        file_.close();  // Close if already open (from header read)
        file_.open(filename_, std::ios::binary);
        if (!file_.is_open())
        {
            throw CatlV1Error(
                "Failed to open CATL file for decompression: " + filename_);
        }
        if (!file_.seekg(sizeof(CatlHeader), std::ios::beg))
        {
            throw CatlV1Error(
                "Failed to seek past CATL header in: " + filename_);
        }

        // Set up decompression stream with zlib parameters
        auto temp_decompressed_stream =
            std::make_unique<boost::iostreams::filtering_istream>();
        boost::iostreams::zlib_params params;
        params.noheader = false;
        params.window_bits = 15;
        params.level = compression_level_;

        try
        {
            temp_decompressed_stream->push(
                boost::iostreams::zlib_decompressor(params));
            temp_decompressed_stream->push(file_);
        }
        catch (const std::exception& e)
        {
            throw CatlV1Error(
                std::string("Failed to set up decompression stream: ") +
                e.what());
        }

        decompressed_stream_ = std::move(temp_decompressed_stream);
        // input_stream_ points to decompressed_stream_
        input_stream_ = decompressed_stream_.get();
    }
    else
    {
        // For uncompressed files, seek to first ledger header and use file_
        // directly
        if (!file_.seekg(sizeof(CatlHeader), std::ios::beg))
        {
            throw CatlV1Error(
                "Failed to seek past CATL header in: " + filename_);
        }
        // input_stream_ points to file_
        input_stream_ = &file_;
    }
}

Reader::~Reader()
{
    if (file_.is_open())
    {
        file_.close();
    }
}

void
Reader::read_header()
{
    // --- File size calculation and pointer reset ---
    // Move the file pointer to the end to get the file size
    file_.seekg(0, std::ios::end);
    std::streamsize file_size = file_.tellg();
    // Reset the file pointer back to the beginning for reading
    file_.seekg(0, std::ios::beg);

    // Ensure the file is large enough to contain a CATLHeader
    if (file_size < static_cast<std::streamsize>(sizeof(CatlHeader)))
    {
        throw CatlV1InvalidHeaderError(
            "File too small to contain a valid CATL header.");
    }

    // Read the CATLHeader from the file
    file_.read(reinterpret_cast<char*>(&header_), sizeof(CatlHeader));
    // gcount() returns the number of bytes read by the last unformatted input
    // operation (here, read) We check that we read exactly the size of
    // CATLHeader
    if (file_.gcount() != sizeof(CatlHeader))
    {
        throw CatlV1InvalidHeaderError("Failed to read complete CATL header.");
    }

    // Validate magic
    if (header_.magic != CATL_MAGIC)
    {
        throw CatlV1InvalidHeaderError("Invalid CATL magic value in header.");
    }

    // Validate version
    uint8_t version = header_.version & CATALOGUE_VERSION_MASK;
    if (version > BASE_CATALOGUE_VERSION)
    {
        throw CatlV1InvalidHeaderError("Unsupported CATL version in header.");
    }

    // Validate file size
    if (header_.filesize != static_cast<uint64_t>(file_size))
    {
        throw CatlV1FileSizeMismatchError(
            "File size does not match header value.");
    }

    compression_level_ = get_compression_level(header_.version);
    catalogue_version_ = get_catalogue_version(header_.version);
    if (catalogue_version_ != BASE_CATALOGUE_VERSION)
        throw CatlV1UnsupportedVersionError(
            "Unsupported CATL version in header.");
    valid_ = true;
}

const CatlHeader&
Reader::header() const
{
    return header_;
}

bool
Reader::valid() const
{
    return valid_;
}

int
Reader::compression_level() const
{
    return compression_level_;
}

int
Reader::catalogue_version() const
{
    return catalogue_version_;
}

LedgerInfo
Reader::read_ledger_info()
{
    LedgerInfo ledger_header;  // NOLINT(*-pro-type-member-init)
    // Use read_bytes which calls read_raw_data internally (with tee support)
    read_bytes(
        reinterpret_cast<uint8_t*>(&ledger_header),
        sizeof(LedgerInfo),
        "ledger header");
    return ledger_header;
}

size_t
Reader::skip_with_tee(size_t bytes, const std::string& context)
{
    // If tee is not enabled, just do a normal seek
    if (!tee_enabled_ || !tee_stream_)
    {
        input_stream_->seekg(bytes, std::ios::cur);
        return bytes;
    }

    // Copy the data we're skipping using a buffer
    const size_t BUFFER_SIZE = 64 * 1024;  // 64KB buffer
    std::vector<uint8_t> buffer(std::min(BUFFER_SIZE, bytes));

    size_t remaining = bytes;
    while (remaining > 0)
    {
        size_t to_read = std::min(buffer.size(), remaining);
        size_t read = read_raw_data(buffer.data(), to_read, context);
        if (read == 0)
            break;  // EOF
        remaining -= read;
    }

    return bytes - remaining;
}

}  // namespace catl::v1