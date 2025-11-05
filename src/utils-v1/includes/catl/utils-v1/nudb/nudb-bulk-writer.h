#pragma once

#include "catl/core/logger.h"
#include "catl/core/types.h"  // For Hash256
#include <atomic>
#include <nudb/create.hpp>
#include <nudb/detail/bulkio.hpp>   // for bulk_writer
#include <nudb/detail/format.hpp>   // for dat_file_header
#include <nudb/native_file.hpp>
#include <nudb/rekey.hpp>
#include <nudb/xxhasher.hpp>
#include <string>
#include <unordered_map>

namespace catl::v1::utils::nudb {

/**
 * Optimized bulk writer for NuDB using the two-step approach:
 *
 * Step 1: Build .dat file sequentially using bulk_writer
 *   - Write all items as [size][key][data] records
 *   - Skip duplicates using in-memory tracking
 *   - No index building, just sequential writes
 *
 * Step 2: Rekey to build index
 *   - NuDB's rekey() builds the .key file from the .dat file
 *   - This is I/O bound but doesn't have the rate limiter issue
 *
 * This approach bypasses NuDB's insert rate limiter and should be
 * significantly faster for bulk imports.
 */
class NudbBulkWriter
{
public:
    /**
     * Create a new bulk writer
     * @param dat_path Path to .dat file
     * @param key_path Path to .key file
     * @param log_path Path to .log file
     * @param key_size Key size in bytes (default 32 for Hash256)
     */
    NudbBulkWriter(
        const std::string& dat_path,
        const std::string& key_path,
        const std::string& log_path,
        uint32_t key_size = 32);

    ~NudbBulkWriter();

    /**
     * Initialize the bulk writer
     * Creates empty database files with headers
     * @param block_size Block size for final database (used in rekey)
     * @param load_factor Load factor for final database (used in rekey)
     * @return true on success
     */
    bool
    open(uint32_t block_size = 4096, double load_factor = 0.5);

    /**
     * Insert a key-value pair (deduplicates automatically)
     * @param key 32-byte hash key
     * @param data Pointer to data
     * @param size Size of data
     * @return true if inserted (false if duplicate)
     */
    bool
    insert(const Hash256& key, const uint8_t* data, size_t size);

    /**
     * Close the bulk writer and build the index
     * This runs NuDB's rekey operation to build the .key file
     * @param progress_buffer_size Buffer size for rekey (default 1GB)
     * @return true on success
     */
    bool
    close(uint64_t progress_buffer_size = 1024ULL * 1024 * 1024);

    /**
     * Get total unique items written
     */
    uint64_t
    get_unique_count() const
    {
        return unique_count_;
    }

    /**
     * Get total duplicate attempts
     */
    uint64_t
    get_duplicate_count() const
    {
        return duplicate_count_;
    }

    /**
     * Get total bytes written (excluding duplicates)
     */
    uint64_t
    get_bytes_written() const
    {
        return total_bytes_written_.load();
    }

private:
    std::string dat_path_;
    std::string key_path_;
    std::string log_path_;
    uint32_t key_size_;
    uint32_t block_size_;
    double load_factor_;

    // Native file for .dat writing
    std::unique_ptr<::nudb::native_file> dat_file_;

    // Bulk writer for sequential writes
    using bulk_writer_t =
        ::nudb::detail::bulk_writer<::nudb::native_file>;
    std::unique_ptr<bulk_writer_t> bulk_writer_;

    // Track seen keys for deduplication
    struct Hash256Hasher
    {
        std::size_t
        operator()(const Hash256& key) const noexcept
        {
            ::nudb::xxhasher h(0);
            return static_cast<std::size_t>(h(key.data(), key.size()));
        }
    };
    std::unordered_map<Hash256, size_t, Hash256Hasher> seen_keys_;

    // Stats
    uint64_t unique_count_ = 0;
    uint64_t duplicate_count_ = 0;
    std::atomic<uint64_t> total_bytes_written_{0};

    bool is_open_ = false;
};

}  // namespace catl::v1::utils::nudb
