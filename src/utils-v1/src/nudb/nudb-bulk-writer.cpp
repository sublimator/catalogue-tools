#include "catl/utils-v1/nudb/nudb-bulk-writer.h"
#include "catl/core/log-macros.h"
#include <boost/filesystem.hpp>

namespace catl::v1::utils::nudb {

NudbBulkWriter::NudbBulkWriter(
    const std::string& dat_path,
    const std::string& key_path,
    const std::string& log_path,
    uint32_t key_size)
    : dat_path_(dat_path)
    , key_path_(key_path)
    , log_path_(log_path)
    , key_size_(key_size)
{
}

NudbBulkWriter::~NudbBulkWriter()
{
    if (is_open_)
    {
        LOGW("NudbBulkWriter destroyed while still open - auto-closing");
        close();
    }
}

bool
NudbBulkWriter::open(uint32_t block_size, double load_factor)
{
    namespace fs = boost::filesystem;

    block_size_ = block_size;
    load_factor_ = load_factor;

    // Remove any existing files
    ::nudb::error_code ec;
    ::nudb::native_file::erase(dat_path_, ec);
    ::nudb::native_file::erase(key_path_, ec);
    ::nudb::native_file::erase(log_path_, ec);
    ec = {};  // Clear any erase errors (files may not exist)

    LOGI("Creating NuDB database files for bulk import...");
    LOGI("  dat: ", dat_path_);
    LOGI("  key: ", key_path_);
    LOGI("  log: ", log_path_);

    // Step 1: Create empty database with valid headers
    // We use dummy values for block_size and load_factor here,
    // as rekey() will create the real index later
    uint64_t appnum = 1;
    ::nudb::create<::nudb::xxhasher>(
        dat_path_,
        key_path_,
        log_path_,
        appnum,
        ::nudb::make_uid(),
        ::nudb::make_salt(),
        key_size_,
        4096,     // dummy block_size (rekey will use the real one)
        0.5f,     // dummy load_factor (rekey will use the real one)
        ec);

    if (ec)
    {
        LOGE("Failed to create NuDB files: ", ec.message());
        return false;
    }

    // Step 2: Open .dat file for append
    dat_file_ = std::make_unique<::nudb::native_file>();
    dat_file_->open(::nudb::file_mode::append, dat_path_, ec);

    if (ec)
    {
        LOGE("Failed to open .dat file: ", ec.message());
        dat_file_.reset();
        return false;
    }

    // Step 3: Create bulk writer
    // Start writing after the data file header
    // Use 64MB write buffer for good sequential write performance
    const size_t write_buffer_size = 64 * 1024 * 1024;  // 64MB
    bulk_writer_ = std::make_unique<bulk_writer_t>(
        *dat_file_,
        ::nudb::detail::dat_file_header::size,
        write_buffer_size);

    LOGI("Bulk writer opened with 64MB buffer");
    LOGI("  key_size: ", key_size_, " bytes");
    LOGI("  Target block_size: ", block_size_);
    LOGI("  Target load_factor: ", load_factor_);

    is_open_ = true;
    return true;
}

bool
NudbBulkWriter::insert(const Hash256& key, const uint8_t* data, size_t size)
{
    if (!is_open_)
    {
        LOGE("Cannot insert - bulk writer not open");
        return false;
    }

    // Deduplication check
    auto it = seen_keys_.find(key);
    if (it != seen_keys_.end())
    {
        duplicate_count_++;
        return false;  // Already seen this key
    }

    // Validate data size
    if (size == 0 ||
        size > ::nudb::detail::field<::nudb::detail::uint48_t>::max)
    {
        LOGE(
            "Invalid data size: ",
            size,
            " (must be 1 to ",
            ::nudb::detail::field<::nudb::detail::uint48_t>::max,
            ")");
        return false;
    }

    // Calculate total record size: [size:6][key:32][data:N]
    size_t const record_size =
        ::nudb::detail::field<::nudb::detail::uint48_t>::size +  // 6 bytes
        key_size_ +                                              // 32 bytes
        size;                                                    // N bytes

    // Prepare buffer in bulk writer
    ::nudb::error_code ec;
    ::nudb::detail::ostream os = bulk_writer_->prepare(record_size, ec);

    if (ec)
    {
        LOGE("bulk_writer prepare failed: ", ec.message());
        return false;
    }

    // Write the record: [size][key][data]
    ::nudb::detail::write<::nudb::detail::uint48_t>(os, size);
    ::nudb::detail::write(os, key.data(), key_size_);
    ::nudb::detail::write(os, data, size);

    // Track this key
    seen_keys_[key] = size;
    unique_count_++;
    total_bytes_written_ += size;

    // Log progress every 10000 inserts
    if (unique_count_ % 10000 == 0)
    {
        LOGD(
            "Bulk wrote ",
            unique_count_,
            " nodes (",
            total_bytes_written_.load() / 1024,
            " KB, ",
            duplicate_count_,
            " dups, ",
            (duplicate_count_ * 100 / (unique_count_ + duplicate_count_)),
            "%)");
    }

    return true;
}

bool
NudbBulkWriter::close(uint64_t progress_buffer_size)
{
    if (!is_open_)
    {
        return true;  // Already closed
    }

    // Mark as closed immediately to prevent destructor from calling close() again
    is_open_ = false;

    LOGI("Closing bulk writer...");
    LOGI("  Total unique items: ", unique_count_);
    LOGI("  Total duplicates: ", duplicate_count_);
    LOGI("  Total bytes: ", total_bytes_written_.load() / 1024 / 1024, " MB");

    // Step 1: Flush and close bulk writer
    ::nudb::error_code ec;
    bulk_writer_->flush(ec);

    if (ec)
    {
        LOGE("Failed to flush bulk writer: ", ec.message());
        return false;
    }

    bulk_writer_.reset();
    dat_file_->close();
    dat_file_.reset();

    LOGI("Step 1 complete: .dat file written successfully");

    // Step 2: Run rekey to build the index
    if (unique_count_ == 0)
    {
        LOGW("No items to index - skipping rekey");
        is_open_ = false;
        return true;
    }

    LOGI("Step 2: Building index with rekey...");
    LOGI("  Unique items: ", unique_count_);
    LOGI("  Block size: ", block_size_);
    LOGI("  Load factor: ", load_factor_);
    LOGI("  Buffer size: ", progress_buffer_size / 1024 / 1024, " MB");

    // Run rekey to build the .key file
    // Progress callback: called periodically during rekey
    auto progress_callback = [](std::uint64_t /*amount*/, std::uint64_t /*total*/) {
        // No-op progress callback
    };

    ::nudb::rekey<::nudb::xxhasher, ::nudb::native_file>(
        dat_path_,
        key_path_,
        log_path_,
        block_size_,
        load_factor_,
        unique_count_,
        progress_buffer_size,
        ec,
        progress_callback);

    if (ec)
    {
        LOGE("Failed to rekey: ", ec.message());
        return false;
    }

    LOGI("✅ Bulk import complete - index built successfully!");
    return true;
}

}  // namespace catl::v1::utils::nudb
