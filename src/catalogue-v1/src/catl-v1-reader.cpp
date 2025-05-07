#include "catl/v1/catl-v1-reader.h"
#include <cstring>
#include <stdexcept>
#include <utility>

#include "catl/v1/catl-v1-utils.h"

namespace catl::v1 {
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
    std::streamsize bytes_read = 0;

    if (!input_stream_)
    {
        throw CatlV1Error("Input stream is not available");
    }

    input_stream_->read(
        reinterpret_cast<char*>(&ledger_header), sizeof(LedgerInfo));
    bytes_read = input_stream_->gcount();

    if (bytes_read == 0)
    {
        throw CatlV1Error("EOF reached: no more ledger headers available");
    }
    else if (bytes_read != sizeof(LedgerInfo))
    {
        throw CatlV1Error(
            "Failed to read complete ledger header: only read " +
            std::to_string(bytes_read) + " bytes");
    }

    return ledger_header;
}
}  // namespace catl::v1
