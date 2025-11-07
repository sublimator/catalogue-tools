#pragma once

#include "catl/core/logger.h"
#include "catl/core/types.h"  // For Hash256
#include <atomic>
#include <boost/filesystem.hpp>
#include <cuckoofilter/cuckoofilter.h>
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
#include <rocksdb/write_batch.h>
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
     * Flush any pending batched operations (for strategies that batch writes)
     */
    virtual void
    flush_batch()
    {
        // Default: no-op (for strategies that don't batch)
    }

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
 * Cuckoo+RocksDB Hybrid Deduplication (the fun one!)
 * Memory usage: Cuckoo filter (~200-300MB) + RocksDB cache (~1GB) = ~1.3GB
 * For 100M+ keys: Fast, memory-efficient, disk-backed
 *
 * Two-tier architecture:
 * 1. FAST PATH (99.9%+ of cases):
 *    - Check cuckoo filter (in-memory, ~0.1% false positive rate)
 *    - If not present: Add to both cuckoo filter AND RocksDB, write to NuDB
 * 2. SLOW PATH (duplicates + ~0.1% false positives):
 *    - Cuckoo says "maybe" → Query RocksDB to confirm
 *    - If RocksDB says "not found" → False positive, write to both
 *    - If RocksDB says "found" → True duplicate, skip write
 *
 * Benefits:
 * - 99.9%+ of inserts hit only the cuckoo filter (pure memory, instant)
 * - Only duplicates and rare false positives trigger RocksDB Get()
 * - RocksDB acts as "ground truth" for the 0.1% cuckoo false positives
 * - Unlimited keys (disk-backed via RocksDB)
 * - Optimized for write-heavy workloads (large memtables, delayed compaction)
 *
 * Use when:
 * - You want maximum speed for new key inserts
 * - You have ~1-2GB RAM available for dedup
 * - You're processing tens of millions to billions of keys
 */
class CuckooRocksStrategy : public DeduplicationStrategy
{
public:
    /**
     * Create Cuckoo+RocksDB hybrid deduplication
     * @param db_path Path to RocksDB database
     * @param resume If true, resume from existing database; if false, delete
     * and recreate
     * @param expected_items Expected number of unique items (for cuckoo filter
     * sizing)
     */
    explicit CuckooRocksStrategy(
        const std::string& db_path,
        bool resume = false,
        size_t expected_items = 100000000)  // Default: 100M items
        : db_path_(db_path), resume_(resume)
    {
        // ===== 1. Initialize Cuckoo Filter (Fast Path) =====
        // Use 12 bits per item → ~0.1% false positive rate
        // Memory: ~1.5 bytes per item → 100M items = ~150MB
        LOGI("Initializing Cuckoo filter for ", expected_items, " items...");
        cuckoo_filter_ =
            std::make_unique<cuckoofilter::CuckooFilter<uint64_t, 12>>(
                expected_items);
        LOGI(
            "  - Cuckoo filter created (12 bits/item, ~0.1% false positive "
            "rate)");

        // ===== 2. Configure RocksDB (Slow Path) =====
        ::rocksdb::Options options;
        options.create_if_missing = true;

        // Basic setup & parallelism
        options.IncreaseParallelism();

        // The "In-Memory Queue" (Memtable) - optimized for write speed
        options.write_buffer_size = 256 * 1024 * 1024;  // 256MB per memtable
        options.max_write_buffer_number = 4;  // 4 memtables = 1GB total

        // Compaction tuning - delay L0 compaction for faster writes
        options.level0_file_num_compaction_trigger =
            10;  // More files before compaction
        options.level0_slowdown_writes_trigger = 24;
        options.level0_stop_writes_trigger = 36;

        // Hash keys don't compress - skip compression to save CPU
        options.compression = ::rocksdb::kNoCompression;

        // Bloom filter for fast Get() on the slow path
        ::rocksdb::BlockBasedTableOptions table_options;
        table_options.filter_policy.reset(::rocksdb::NewBloomFilterPolicy(
            10,
            false));  // 10 bits = ~1% false positive
        table_options.whole_key_filtering =
            true;  // Optimize for full hash keys
        table_options.block_cache =
            ::rocksdb::NewLRUCache(1024 * 1024 * 1024);  // 1GB cache
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
        LOGI("Cuckoo+RocksDB deduplication strategy initialized");
        LOGI("  - Database path: ", db_path_);
        LOGI("  - Resume mode: ", resume_ ? "YES" : "NO");
        LOGI(
            "  - Memory budget: ~1.3GB (cuckoo filter + RocksDB memtables + "
            "cache)");
    }

    ~CuckooRocksStrategy()
    {
        // Flush any remaining batch before closing
        flush_batch();

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
        if (!db_ || !cuckoo_filter_)
        {
            return false;  // Not initialized, treat as new
        }

        // Compute xxhash for cuckoo filter (8 bytes, uniform distribution)
        ::nudb::xxhasher h(0);
        uint64_t xxhash_val = h(key.data(), key.size());

        // ===== FAST PATH: Check cuckoo filter first =====
        if (cuckoo_filter_->Contain(xxhash_val) != cuckoofilter::Ok)
        {
            // Not in cuckoo filter → definitely new key!

            // Add to RocksDB WriteBatch (this is the source of truth!)
            // The Put() goes into the batch (fast, in-memory)
            write_batch_.Put(
                ::rocksdb::Slice(
                    reinterpret_cast<const char*>(key.data()), key.size()),
                ::rocksdb::Slice());  // Empty value (0 bytes)

            // Add to cuckoo filter to avoid Get() on future checks
            cuckoo_filter_->Add(xxhash_val);

            unique_keys_++;
            fast_path_hits_++;
            batch_size_++;
            return false;  // Not a duplicate, write to NuDB
        }

        // ===== SLOW PATH: Cuckoo says "maybe" → Check RocksDB =====
        slow_path_hits_++;

        std::string value;
        ::rocksdb::Status status = db_->Get(
            ::rocksdb::ReadOptions(),
            ::rocksdb::Slice(
                reinterpret_cast<const char*>(key.data()), key.size()),
            &value);

        if (status.ok())
        {
            // Key exists in RocksDB → True duplicate!
            duplicate_count_++;
            true_duplicates_++;
            return true;  // Skip write
        }

        if (status.IsNotFound())
        {
            // Not in RocksDB → Cuckoo false positive!
            // Add to WriteBatch (ground truth for future checks)
            write_batch_.Put(
                ::rocksdb::Slice(
                    reinterpret_cast<const char*>(key.data()), key.size()),
                ::rocksdb::Slice());  // Empty value

            // Note: We don't add to cuckoo filter because it already
            // (falsely) contains it

            unique_keys_++;
            cuckoo_false_positives_++;
            batch_size_++;
            return false;  // Not a duplicate, write to NuDB
        }

        // Error reading - treat as new (don't lose data)
        LOGW("RocksDB Get error: ", status.ToString());
        unique_keys_++;
        return false;
    }

    void
    flush_batch() override
    {
        if (batch_size_ == 0)
        {
            return;  // Nothing to flush
        }

        if (!db_)
        {
            return;
        }

        // Commit the entire batch with one db->Write() call
        ::rocksdb::Status status =
            db_->Write(::rocksdb::WriteOptions(), &write_batch_);

        if (!status.ok())
        {
            LOGW("RocksDB WriteBatch commit failed: ", status.ToString());
        }

        // Clear the batch for next round
        write_batch_.Clear();
        batch_size_ = 0;
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
        LOGI("📊 DEDUPLICATION STATS (Cuckoo+RocksDB Hybrid Strategy):");
        LOGI("  - Unique keys written: ", unique_count);
        LOGI("  - Total duplicate attempts: ", duplicate_count_);

        if (unique_count > 0)
        {
            LOGI(
                "  - Average duplicates per unique key: ",
                std::fixed,
                std::setprecision(2),
                (static_cast<double>(duplicate_count_) / unique_count));
        }

        LOGI("");
        LOGI("  🚀 Performance Breakdown:");
        uint64_t total_checks = fast_path_hits_ + slow_path_hits_;
        if (total_checks > 0)
        {
            LOGI(
                "  - Fast path hits (cuckoo only): ",
                fast_path_hits_,
                " (",
                std::fixed,
                std::setprecision(2),
                (fast_path_hits_ * 100.0 / total_checks),
                "%)");
            LOGI(
                "  - Slow path hits (RocksDB query): ",
                slow_path_hits_,
                " (",
                std::fixed,
                std::setprecision(2),
                (slow_path_hits_ * 100.0 / total_checks),
                "%)");
        }

        if (slow_path_hits_ > 0)
        {
            LOGI(
                "  - Cuckoo false positives: ",
                cuckoo_false_positives_,
                " (",
                std::fixed,
                std::setprecision(2),
                (cuckoo_false_positives_ * 100.0 / slow_path_hits_),
                "% of slow path)");
            LOGI(
                "  - True duplicates: ",
                true_duplicates_,
                " (",
                std::fixed,
                std::setprecision(2),
                (true_duplicates_ * 100.0 / slow_path_hits_),
                "% of slow path)");
        }

        // Get RocksDB stats
        if (db_)
        {
            std::string stats;
            db_->GetProperty("rocksdb.estimate-num-keys", &stats);
            LOGI("");
            LOGI("  💾 RocksDB Stats:");
            LOGI("  - Estimated keys in DB: ", stats);

            db_->GetProperty("rocksdb.total-sst-files-size", &stats);
            if (!stats.empty())
            {
                uint64_t sst_size = std::stoull(stats);
                LOGI("  - SST file size: ", sst_size / 1024 / 1024, " MB");
            }
        }

        LOGI("");
        LOGI("  - Database path: ", db_path_);
        LOGI("  - Memory usage: ~1.3 GB (cuckoo + memtables + cache)");
    }

private:
    std::string db_path_;
    bool resume_;
    std::unique_ptr<cuckoofilter::CuckooFilter<uint64_t, 12>> cuckoo_filter_;
    std::unique_ptr<::rocksdb::DB> db_;

    // Batching for efficient writes
    ::rocksdb::WriteBatch write_batch_;
    size_t batch_size_ = 0;

    // Counters
    uint64_t duplicate_count_ = 0;
    uint64_t unique_keys_ = 0;
    uint64_t fast_path_hits_ = 0;  // Cuckoo said "not present"
    uint64_t slow_path_hits_ = 0;  // Cuckoo said "maybe", checked RocksDB
    uint64_t cuckoo_false_positives_ = 0;  // Slow path, but not in RocksDB
    uint64_t true_duplicates_ = 0;         // Slow path, confirmed in RocksDB
};

}  // namespace catl::v1::utils::nudb
