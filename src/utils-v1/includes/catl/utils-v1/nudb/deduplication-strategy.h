#pragma once

#include "catl/core/logger.h"
#include "catl/core/types.h"  // For Hash256
#include <atomic>
#include <boost/filesystem.hpp>
#include <nudb/create.hpp>
#include <nudb/native_file.hpp>
#include <nudb/posix_file.hpp>
#include <nudb/store.hpp>
#include <nudb/verify.hpp>
#include <nudb/xxhasher.hpp>
#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>

namespace catl::v1::utils::nudb {

/**
 * Strategy interface for deduplication during bulk writes
 */
class DeduplicationStrategy
{
public:
    virtual ~DeduplicationStrategy() = default;

    /**
     * Check if key is a duplicate and mark it as seen
     * @param key The 32-byte hash key
     * @param size Size of the data
     * @param node_type Node type (0=inner, 1=leaf)
     * @return true if duplicate (should skip write), false if new (should
     * write)
     */
    virtual bool
    check_and_mark(const Hash256& key, size_t size, uint8_t node_type) = 0;

    /**
     * Get total number of duplicate attempts detected
     */
    virtual uint64_t
    get_duplicate_count() const = 0;

    /**
     * Print strategy-specific statistics
     * @param unique_count Total unique keys written
     */
    virtual void
    print_stats(uint64_t unique_count) const = 0;
};

/**
 * No deduplication - all inserts succeed
 * Use this for maximum speed when duplicates are impossible or handled
 * externally
 */
class NoDeduplicationStrategy : public DeduplicationStrategy
{
public:
    bool
    check_and_mark(const Hash256&, size_t, uint8_t) override
    {
        return false;  // Never a duplicate
    }

    uint64_t
    get_duplicate_count() const override
    {
        return 0;
    }

    void print_stats(uint64_t) const override
    {
        LOGI("📊 DEDUPLICATION: DISABLED (maximum speed mode)");
    }
};

/**
 * Full Hash256 tracking (original approach)
 * Memory usage: ~40 bytes per unique key
 * For 79M keys: ~3.2GB
 */
class FullKeyDeduplicationStrategy : public DeduplicationStrategy
{
public:
    // Custom hasher for Hash256
    struct Hash256Hasher
    {
        std::size_t
        operator()(const Hash256& key) const noexcept
        {
            ::nudb::xxhasher h(0);
            return static_cast<std::size_t>(h(key.data(), key.size()));
        }
    };

    struct KeyInfo
    {
        size_t size;
        uint64_t duplicate_count;
        uint8_t node_type;
    };

    bool
    check_and_mark(const Hash256& key, size_t size, uint8_t node_type) override
    {
        auto it = seen_keys_.find(key);
        if (it != seen_keys_.end())
        {
            // Already seen - it's a duplicate
            it->second.duplicate_count++;
            duplicate_count_++;
            return true;
        }

        // New key - mark it
        seen_keys_[key] = KeyInfo{size, 0, node_type};
        return false;
    }

    uint64_t
    get_duplicate_count() const override
    {
        return duplicate_count_;
    }

    void
    print_stats(uint64_t unique_count) const override
    {
        // Count how many keys had duplicates and max dup count
        uint64_t keys_with_duplicates = 0;
        uint64_t max_dup_count = 0;
        std::unordered_map<uint8_t, uint64_t> dup_count_by_type;
        std::unordered_map<uint8_t, uint64_t> dup_attempts_by_type;

        for (const auto& [key, info] : seen_keys_)
        {
            if (info.duplicate_count > 0)
            {
                keys_with_duplicates++;
                if (info.duplicate_count > max_dup_count)
                {
                    max_dup_count = info.duplicate_count;
                }

                dup_count_by_type[info.node_type]++;
                dup_attempts_by_type[info.node_type] += info.duplicate_count;
            }
        }

        LOGI("");
        LOGI("📊 DEDUPLICATION STATS (Full Hash256 Tracking):");
        LOGI("  - Unique keys written: ", unique_count);
        LOGI(
            "  - Keys that had duplicates: ",
            keys_with_duplicates,
            " (",
            std::fixed,
            std::setprecision(2),
            (keys_with_duplicates * 100.0 / unique_count),
            "%)");
        LOGI("  - Total duplicate attempts: ", duplicate_count_);
        LOGI(
            "  - Average duplicates per unique key: ",
            std::fixed,
            std::setprecision(2),
            (static_cast<double>(duplicate_count_) / unique_count));
        LOGI("  - Max duplicates for a single key: ", max_dup_count);

        if (!dup_count_by_type.empty())
        {
            LOGI("");
            LOGI("  📋 Duplicates by node type:");
            for (const auto& [node_type, count] : dup_count_by_type)
            {
                uint64_t attempts = dup_attempts_by_type[node_type];
                const char* type_name = (node_type == 0)
                    ? "Inner"
                    : (node_type == 1) ? "Leaf" : "Unknown";
                LOGI(
                    "    - ",
                    type_name,
                    " nodes: ",
                    count,
                    " keys (",
                    attempts,
                    " duplicate attempts)");
            }
        }
    }

private:
    std::unordered_map<Hash256, KeyInfo, Hash256Hasher> seen_keys_;
    uint64_t duplicate_count_ = 0;
};

/**
 * Hybrid xxhash deduplication (new optimized approach)
 * Memory usage: ~8 bytes per unique key + ~40 bytes per collision
 * For 79M keys: ~650MB (12x smaller than full tracking!)
 *
 * Algorithm:
 * 1. Primary filter: Track only xxhash (8 bytes)
 * 2. On xxhash collision: Track full Hash256 in collision map
 * 3. Expected collisions for 79M keys: ~1000 (0.00003%)
 */
class HybridXxHashDeduplicationStrategy : public DeduplicationStrategy
{
public:
    // Identity hash - xxhash is already uniform
    struct IdentityHash
    {
        std::size_t
        operator()(uint64_t x) const noexcept
        {
            return static_cast<std::size_t>(x);
        }
    };

    // Custom hasher for Hash256
    struct Hash256Hasher
    {
        std::size_t
        operator()(const Hash256& key) const noexcept
        {
            ::nudb::xxhasher h(0);
            return static_cast<std::size_t>(h(key.data(), key.size()));
        }
    };

    struct KeyInfo
    {
        size_t size;
        uint64_t duplicate_count;
        uint8_t node_type;
    };

    bool
    check_and_mark(const Hash256& key, size_t size, uint8_t node_type) override
    {
        // Compute xxhash (8 bytes) - much smaller than Hash256 (32 bytes)
        ::nudb::xxhasher h(0);
        uint64_t xxhash_val = h(key.data(), key.size());

        // Check primary filter: Have we seen this xxhash before?
        if (seen_xxhashes_.find(xxhash_val) == seen_xxhashes_.end())
        {
            // Brand new xxhash - first occurrence
            seen_xxhashes_.insert(xxhash_val);
            // ALSO track full key so we can detect duplicates later!
            collision_tracking_[key] = KeyInfo{size, 0, node_type};
            return false;  // Not a duplicate, write it
        }

        // xxhash already seen - check if actual key is a duplicate
        auto it = collision_tracking_.find(key);
        if (it != collision_tracking_.end())
        {
            // Found the actual key - it's a real duplicate!
            it->second.duplicate_count++;
            duplicate_count_++;
            return true;  // Skip write
        }

        // xxhash collision detected (different key, same xxhash)
        xxhash_collisions_++;
        collision_tracking_[key] = KeyInfo{size, 0, node_type};
        return false;  // Not a duplicate, write it
    }

    uint64_t
    get_duplicate_count() const override
    {
        return duplicate_count_;
    }

    void
    print_stats(uint64_t unique_count) const override
    {
        // Count duplicates from collision tracking
        uint64_t keys_with_duplicates = 0;
        uint64_t max_dup_count = 0;
        std::unordered_map<uint8_t, uint64_t> dup_count_by_type;
        std::unordered_map<uint8_t, uint64_t> dup_attempts_by_type;

        for (const auto& [key, info] : collision_tracking_)
        {
            if (info.duplicate_count > 0)
            {
                keys_with_duplicates++;
                if (info.duplicate_count > max_dup_count)
                {
                    max_dup_count = info.duplicate_count;
                }

                dup_count_by_type[info.node_type]++;
                dup_attempts_by_type[info.node_type] += info.duplicate_count;
            }
        }

        LOGI("");
        LOGI("📊 DEDUPLICATION STATS (Hybrid xxHash Strategy):");
        LOGI("  - Unique keys written: ", unique_count);
        LOGI("  - xxHash collisions detected: ", xxhash_collisions_);
        LOGI(
            "  - Keys that had duplicates: ",
            keys_with_duplicates,
            " (",
            std::fixed,
            std::setprecision(2),
            (keys_with_duplicates * 100.0 / unique_count),
            "%)");
        LOGI("  - Total duplicate attempts: ", duplicate_count_);

        if (unique_count > 0)
        {
            LOGI(
                "  - Average duplicates per unique key: ",
                std::fixed,
                std::setprecision(2),
                (static_cast<double>(duplicate_count_) / unique_count));
        }

        LOGI("  - Max duplicates for a single key: ", max_dup_count);

        // Memory estimate
        size_t xxhash_mem = seen_xxhashes_.size() * 8;
        size_t collision_mem = collision_tracking_.size() * 40;  // approx
        LOGI(
            "  - Memory usage: ~",
            (xxhash_mem + collision_mem) / 1024 / 1024,
            " MB");

        if (!dup_count_by_type.empty())
        {
            LOGI("");
            LOGI("  📋 Duplicates by node type:");
            for (const auto& [node_type, count] : dup_count_by_type)
            {
                uint64_t attempts = dup_attempts_by_type[node_type];
                const char* type_name = (node_type == 0)
                    ? "Inner"
                    : (node_type == 1) ? "Leaf" : "Unknown";
                LOGI(
                    "    - ",
                    type_name,
                    " nodes: ",
                    count,
                    " keys (",
                    attempts,
                    " duplicate attempts)");
            }
        }
    }

private:
    // Primary filter: Just track xxhash (8 bytes per key)
    std::unordered_set<uint64_t, IdentityHash> seen_xxhashes_;

    // Collision tracking: Only track full keys when xxhash collision detected
    std::unordered_map<Hash256, KeyInfo, Hash256Hasher> collision_tracking_;

    uint64_t duplicate_count_ = 0;
    uint64_t xxhash_collisions_ = 0;
};

/**
 * RocksDB-backed deduplication (SSD-based, unlimited capacity)
 * Memory usage: ~100MB (RocksDB cache)
 * For any number of keys: SSD-backed, no memory limit!
 *
 * Benefits:
 * - Unlimited keys (only limited by disk space)
 * - Fast sequential writes to SSD
 * - Bloom filters for fast lookups
 * - Automatic compression
 * - Survives crashes (can resume)
 *
 * Use when:
 * - Billions of keys expected
 * - Memory is constrained
 * - Want persistent dedup state (resume imports)
 */
class RocksDBDeduplicationStrategy : public DeduplicationStrategy
{
public:
    /**
     * Create RocksDB-backed deduplication
     * @param db_path Path to temporary RocksDB database
     * @param resume If true, resume from existing database; if false, delete
     * and recreate
     */
    explicit RocksDBDeduplicationStrategy(
        const std::string& db_path,
        bool resume = false)
        : db_path_(db_path), resume_(resume)
    {
        ::rocksdb::Options options;
        options.create_if_missing = true;
        options.compression =
            ::rocksdb::kNoCompression;  // Just bools, no need to compress

        // Optimize for bulk sequential writes
        options.write_buffer_size = 64 * 1024 * 1024;  // 64MB write buffer
        options.max_write_buffer_number = 3;
        options.target_file_size_base = 64 * 1024 * 1024;

        // Use bloom filters for fast lookups
        ::rocksdb::BlockBasedTableOptions table_options;
        table_options.filter_policy.reset(::rocksdb::NewBloomFilterPolicy(10));
        table_options.block_cache =
            ::rocksdb::NewLRUCache(100 * 1024 * 1024);  // 100MB cache
        options.table_factory.reset(
            ::rocksdb::NewBlockBasedTableFactory(table_options));

        // Delete existing database if not resuming
        if (!resume_)
        {
            ::rocksdb::Status status = ::rocksdb::DestroyDB(db_path_, options);
            if (!status.ok())
            {
                LOGW("Failed to destroy existing RocksDB: ", status.ToString());
            }
        }

        // Open database
        ::rocksdb::DB* db_raw;
        ::rocksdb::Status status =
            ::rocksdb::DB::Open(options, db_path_, &db_raw);

        if (!status.ok())
        {
            LOGE("Failed to open RocksDB: ", status.ToString());
            throw std::runtime_error(
                "Failed to open RocksDB: " + status.ToString());
        }

        db_.reset(db_raw);
        LOGI("RocksDB deduplication strategy initialized at: ", db_path_);
        LOGI("  Resume mode: ", resume_ ? "YES" : "NO");
    }

    ~RocksDBDeduplicationStrategy()
    {
        if (db_)
        {
            db_->Close();
            db_.reset();
        }

        // Clean up temp database (unless user wants to keep for resume)
        if (!resume_)
        {
            ::rocksdb::Options options;
            ::rocksdb::Status status = ::rocksdb::DestroyDB(db_path_, options);
            if (!status.ok())
            {
                LOGW("Failed to clean up RocksDB: ", status.ToString());
            }
        }
    }

    bool
    check_and_mark(const Hash256& key, size_t, uint8_t) override
    {
        if (!db_)
        {
            return false;  // Database not open, treat as new
        }

        // Check if key exists (just need boolean - exists or not!)
        std::string value;
        ::rocksdb::Status status = db_->Get(
            ::rocksdb::ReadOptions(),
            ::rocksdb::Slice(
                reinterpret_cast<const char*>(key.data()), key.size()),
            &value);

        if (status.ok())
        {
            // Key exists - it's a duplicate!
            duplicate_count_++;
            return true;  // Skip write
        }

        if (status.IsNotFound())
        {
            // New key - mark it as seen (store single byte = "seen")
            static const char seen_marker = 1;
            db_->Put(
                ::rocksdb::WriteOptions(),
                ::rocksdb::Slice(
                    reinterpret_cast<const char*>(key.data()), key.size()),
                ::rocksdb::Slice(&seen_marker, 1));

            unique_keys_++;
            return false;  // Not a duplicate, write it
        }

        // Error reading - treat as new (don't lose data)
        LOGW("RocksDB Get error: ", status.ToString());
        return false;
    }

    uint64_t
    get_duplicate_count() const override
    {
        return duplicate_count_;
    }

    void
    print_stats(uint64_t unique_count) const override
    {
        LOGI("");
        LOGI("📊 DEDUPLICATION STATS (RocksDB SSD-Backed Strategy):");
        LOGI("  - Unique keys written: ", unique_count);
        LOGI("  - RocksDB tracked keys: ", unique_keys_);
        LOGI("  - Total duplicate attempts: ", duplicate_count_);

        if (unique_count > 0)
        {
            LOGI(
                "  - Average duplicates per unique key: ",
                std::fixed,
                std::setprecision(2),
                (static_cast<double>(duplicate_count_) / unique_count));
        }

        // Get RocksDB stats
        if (db_)
        {
            std::string stats;
            db_->GetProperty("rocksdb.estimate-num-keys", &stats);
            LOGI("  - RocksDB estimated keys: ", stats);

            db_->GetProperty("rocksdb.total-sst-files-size", &stats);
            uint64_t sst_size = std::stoull(stats);
            LOGI("  - RocksDB SST file size: ", sst_size / 1024 / 1024, " MB");
        }

        LOGI("  - Database path: ", db_path_);
        LOGI("  - Memory usage: ~100 MB (block cache)");
    }

private:
    std::string db_path_;
    bool resume_;
    std::unique_ptr<::rocksdb::DB> db_;
    uint64_t duplicate_count_ = 0;
    uint64_t unique_keys_ = 0;
};

/**
 * NuDB-backed deduplication (disk-backed using NuDB itself!)
 * Memory usage: ~64MB (NuDB cache)
 * For unlimited keys: SSD-backed, optimized for this exact use case
 *
 * Benefits:
 * - Unlimited keys (disk-backed)
 * - Fast lookups (NuDB's xxhash index)
 * - Already have NuDB as dependency
 * - Simple 1-byte value per key
 * - Much simpler than RocksDB!
 */
class NuDBDeduplicationStrategy : public DeduplicationStrategy
{
public:
    explicit NuDBDeduplicationStrategy(const std::string& db_path)
        : db_path_(db_path)
    {
        namespace fs = boost::filesystem;

        fs::path dir(db_path_);

        // Make absolute to avoid any relative path issues
        if (!dir.is_absolute())
        {
            dir = fs::absolute(dir);
        }

        fs::path dat = dir / "dedup.dat";
        fs::path key = dir / "dedup.key";
        fs::path log = dir / "dedup.log";

        dat_path_ = dat.string();
        key_path_ = key.string();
        log_path_ = log.string();

        // Create all parent directories (including parents of the dedup dir)
        boost::system::error_code fs_ec;
        fs::create_directories(dir, fs_ec);

        if (fs_ec && fs_ec.value() != boost::system::errc::file_exists)
        {
            LOGE("Failed to create dedup directory: ", fs_ec.message());
            throw std::runtime_error(
                "Failed to create dedup directory: " + fs_ec.message());
        }

        LOGI("Dedup paths:");
        LOGI("  dat: ", dat_path_);
        LOGI("  key: ", key_path_);
        LOGI("  log: ", log_path_);
        LOGI("  dir exists: ", fs::exists(dir));
        LOGI("  dir is writable: ", (access(dir.string().c_str(), W_OK) == 0));

        // Delete existing dedup database files using boost::filesystem
        if (fs::exists(dat))
        {
            fs::remove(dat);
            LOGI("Deleted existing ", dat_path_);
        }
        if (fs::exists(key))
        {
            fs::remove(key);
            LOGI("Deleted existing ", key_path_);
        }
        if (fs::exists(log))
        {
            fs::remove(log);
            LOGI("Deleted existing ", log_path_);
        }

        // Create new NuDB database for deduplication
        // Try 0.9 load factor (dense, less space, potentially fewer collisions)
        LOGI("Creating NuDB dedup database...");
        LOGI("  key_size: 32");
        LOGI("  block_size: 16384");
        LOGI("  load_factor: 0.9");

        ::nudb::error_code ec;
        ::nudb::create<::nudb::xxhasher>(
            dat_path_,
            key_path_,
            log_path_,
            1,  // appnum
            ::nudb::make_uid(),
            ::nudb::make_salt(),
            32,     // Hash256 key size
            16384,  // 16KB block size
            0.9f,   // 90% load factor (dense!)
            ec);

        if (ec)
        {
            LOGE("Failed to create NuDB dedup database: ", ec.message());
            LOGE("  Error code value: ", ec.value());
            LOGE("  Error category: ", ec.category().name());
            throw std::runtime_error(
                "Failed to create NuDB dedup database: " + ec.message());
        }

        LOGI("NuDB dedup database created successfully");

        // Open database (use nudb::store like rippled does)
        db_ = std::make_unique<::nudb::store>();
        db_->open(dat_path_, key_path_, log_path_, ec);

        if (ec)
        {
            LOGE("Failed to open NuDB dedup database: ", ec.message());
            throw std::runtime_error("Failed to open NuDB dedup database");
        }

        LOGI("NuDB deduplication strategy initialized at: ", db_path_);
    }

    ~NuDBDeduplicationStrategy()
    {
        if (db_)
        {
            ::nudb::error_code ec;
            db_->close(ec);
            db_.reset();

            // Keep dedup database for inspection (no cleanup)
            namespace fs = boost::filesystem;

            LOGI("Dedup database kept at: ", db_path_);

            // Log file sizes
            if (fs::exists(dat_path_) && fs::exists(key_path_))
            {
                uint64_t dat_size = fs::file_size(dat_path_);
                uint64_t key_size = fs::file_size(key_path_);
                uint64_t log_size =
                    fs::exists(log_path_) ? fs::file_size(log_path_) : 0;
                uint64_t total_size = dat_size + key_size + log_size;

                LOGI("  - dedup.dat: ", dat_size / 1024 / 1024, " MB");
                LOGI("  - dedup.key: ", key_size / 1024 / 1024, " MB");
                if (log_size > 0)
                {
                    LOGI("  - dedup.log: ", log_size / 1024 / 1024, " MB");
                }
                LOGI("  - Total: ", total_size / 1024 / 1024, " MB");
            }
            else
            {
                LOGW("Dedup database files not found!");
            }
        }
    }

    bool
    check_and_mark(const Hash256& key, size_t, uint8_t) override
    {
        if (!db_)
        {
            return false;
        }

        // Try to fetch - if found, it's a duplicate
        ::nudb::error_code ec;
        bool found = false;

        db_->fetch(
            key.data(),
            [&found](void const*, std::size_t) {
                found = true;  // Key exists!
            },
            ec);

        if (found)
        {
            // Key exists - duplicate!
            duplicate_count_++;
            return true;
        }

        // New key - insert marker (1 byte)
        // IMPORTANT: Clear error code from fetch before insert!
        ec = {};
        static const char seen_marker = 1;
        db_->insert(key.data(), &seen_marker, 1, ec);

        if (ec)
        {
            // Log first error, then stop spamming
            static std::atomic<uint64_t> insert_errors{0};
            if (insert_errors.fetch_add(1) < 10)
            {
                LOGE("Dedup insert failed: ", ec.message());
            }
        }
        else
        {
            unique_keys_++;
        }

        return false;  // Not a duplicate
    }

    uint64_t
    get_duplicate_count() const override
    {
        return duplicate_count_;
    }

    void
    print_stats(uint64_t unique_count) const override
    {
        namespace fs = boost::filesystem;

        LOGI("");
        LOGI("📊 DEDUPLICATION STATS (NuDB Disk-Backed Strategy):");
        LOGI("  - Unique keys written: ", unique_count);
        LOGI("  - NuDB tracked keys: ", unique_keys_);
        LOGI("  - Total duplicate attempts: ", duplicate_count_);

        if (unique_count > 0)
        {
            LOGI(
                "  - Average duplicates per unique key: ",
                std::fixed,
                std::setprecision(2),
                (static_cast<double>(duplicate_count_) / unique_count));
        }

        // Get file sizes
        if (fs::exists(dat_path_) && fs::exists(key_path_))
        {
            uint64_t dat_size = fs::file_size(dat_path_);
            uint64_t key_size = fs::file_size(key_path_);
            LOGI(
                "  - NuDB dedup size: ",
                (dat_size + key_size) / 1024 / 1024,
                " MB");
        }

        LOGI("  - Database path: ", db_path_);
        LOGI("  - Memory usage: ~64 MB (NuDB cache)");
    }

private:
    std::string db_path_;
    std::string dat_path_;
    std::string key_path_;
    std::string log_path_;
    std::unique_ptr<::nudb::store> db_;
    uint64_t duplicate_count_ = 0;
    uint64_t unique_keys_ = 0;
};

}  // namespace catl::v1::utils::nudb
