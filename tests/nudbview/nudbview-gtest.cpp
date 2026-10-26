#include "test-helpers.hpp"
#include <algorithm>
#include <limits>
#include <boost/filesystem.hpp>
#include <gtest/gtest.h>
#include <iostream>
#include <nudbview/native_file.hpp>
#include <nudbview/nudb.hpp>
#include <nudbview/view/index_format.hpp>

using namespace nudbview_test;
namespace fs = boost::filesystem;

// Test basic database creation with known keys/values
TEST(NuDBView, CreateTestDatabase)
{
    auto db = create_test_database(100, "create-test");

    ASSERT_TRUE(fs::exists(db->dat_path));
    ASSERT_TRUE(fs::exists(db->key_path));
    ASSERT_EQ(db->records.size(), 100);

    // Verify first and last record
    nudbview::error_code ec;
    EXPECT_TRUE(verify_record(
        db->dat_path.string(), db->key_path.string(), db->records[0], ec));
    EXPECT_TRUE(verify_record(
        db->dat_path.string(), db->key_path.string(), db->records[99], ec));
}

// Test key generation is deterministic
TEST(NuDBView, KeyGenerationDeterministic)
{
    auto key1 = generate_key(42);
    auto key2 = generate_key(42);
    auto key3 = generate_key(43);

    EXPECT_EQ(key1, key2);  // Same input = same key
    EXPECT_NE(key1, key3);  // Different input = different key
}

// Test database with larger dataset
TEST(NuDBView, CreateLargeDatabase)
{
    auto db = create_test_database(10000, "large-test");

    ASSERT_EQ(db->records.size(), 10000);

    // Spot check some records
    nudbview::error_code ec;
    EXPECT_TRUE(verify_record(
        db->dat_path.string(), db->key_path.string(), db->records[0], ec));
    EXPECT_TRUE(verify_record(
        db->dat_path.string(), db->key_path.string(), db->records[5000], ec));
    EXPECT_TRUE(verify_record(
        db->dat_path.string(), db->key_path.string(), db->records[9999], ec));
}

// Test index creation and verification
TEST(NuDBView, BuildAndVerifyIndex)
{
    auto db = create_test_database(1000, "index-test");

    // Build index with interval=100
    std::string index_path = (db->dir / "nudb.dat.index").string();

    nudbutil::IndexBuildOptions opts;
    opts.index_interval = 100;
    opts.show_progress = false;

    auto result =
        nudbutil::IndexBuilder::build(db->dat_path.string(), index_path, opts);

    ASSERT_TRUE(result.success) << result.error_message;
    EXPECT_EQ(result.total_records, 1000);
    EXPECT_EQ(result.entry_count, 10);  // 1000 / 100 = 10 entries

    // Verify index file exists
    EXPECT_TRUE(fs::exists(index_path));

    // Load and verify index
    nudbutil::IndexReader index;
    nudbview::error_code ec;
    ASSERT_TRUE(index.load(index_path, ec)) << ec.message();

    EXPECT_EQ(index.total_records(), 1000);
    EXPECT_EQ(index.index_interval(), 100);
    EXPECT_EQ(index.entry_count(), 10);

    // Verify each index offset points to a valid record
    // Open dat file
    boost::iostreams::mapped_file_source mmap;
    mmap.open(db->dat_path.string());
    ASSERT_TRUE(mmap.is_open());

    auto const* dat_data = reinterpret_cast<const std::uint8_t*>(mmap.data());
    nudbview::detail::dat_file_header dh;
    nudbview::detail::istream dh_is{
        dat_data, nudbview::detail::dat_file_header::size};
    nudbview::detail::read(dh_is, dh);

    // For each index entry, verify we can scan from that offset
    for (std::uint64_t record_num = 0; record_num < 1000; record_num += 100)
    {
        nudbview::noff_t closest_offset;
        std::uint64_t records_to_skip;

        index.lookup_record_start_offset(
            record_num, closest_offset, records_to_skip);

        EXPECT_EQ(records_to_skip, 0)
            << "Record " << record_num << " should be indexed exactly";

        // Scan one record from this offset to verify it's valid
        std::uint64_t scanned = 0;
        bool found = false;
        nudbutil::scan_dat_records(
            mmap,
            dh.key_size,
            [&]([[maybe_unused]] std::uint64_t rnum,
                std::uint64_t offset,
                [[maybe_unused]] std::uint64_t size) {
                if (scanned == 0)
                {
                    EXPECT_EQ(offset, closest_offset);
                    found = true;
                }
                ++scanned;
            },
            closest_offset,
            0);

        EXPECT_TRUE(found) << "Failed to scan record at offset "
                           << closest_offset;
    }
}

// Test incremental index building (extend mode)
TEST(NuDBView, IncrementalIndexing)
{
    // Phase 1: Create initial database with 500 records
    auto db = create_test_database(500, "incremental-test");
    std::string index_path = (db->dir / "nudb.dat.index").string();

    // Phase 2: Build initial index with interval=50
    nudbutil::IndexBuildOptions opts;
    opts.index_interval = 50;
    opts.show_progress = false;

    auto result =
        nudbutil::IndexBuilder::build(db->dat_path.string(), index_path, opts);

    ASSERT_TRUE(result.success) << result.error_message;
    EXPECT_EQ(result.total_records, 500);
    EXPECT_EQ(result.entry_count, 10);  // 500 / 50 = 10 entries

    // Load and verify initial index
    nudbutil::IndexReader index;
    nudbview::error_code ec;
    ASSERT_TRUE(index.load(index_path, ec)) << ec.message();

    std::cout << "\n=== Initial Index (500 records) ===" << std::endl;
    index.dump_entries(std::cout, 15);
    std::cout << "===================================\n" << std::endl;

    // Debug: scan actual records to see real offsets
    boost::iostreams::mapped_file_source debug_mmap;
    debug_mmap.open(db->dat_path.string());
    auto const* dat_data =
        reinterpret_cast<const std::uint8_t*>(debug_mmap.data());
    nudbview::detail::dat_file_header dh_debug;
    nudbview::detail::istream dh_is{
        dat_data, nudbview::detail::dat_file_header::size};
    nudbview::detail::read(dh_is, dh_debug);

    std::cout << "=== Actual Record Offsets (first 15 @ interval 50) ==="
              << std::endl;
    nudbutil::scan_dat_records(
        debug_mmap,
        dh_debug.key_size,
        [&](std::uint64_t rnum, std::uint64_t offset, std::uint64_t size) {
            if (rnum % 50 == 0 && rnum < 15 * 50)
            {
                std::cout << "  Record " << rnum << " -> offset " << offset
                          << std::endl;
            }
        });
    std::cout << "========================================================\n"
              << std::endl;

    EXPECT_EQ(index.total_records(), 500);
    EXPECT_EQ(index.entry_count(), 10);

    // Phase 3: Add 500 more records (500-999)
    debug_mmap.close();  // Close mmap before appending!
    append_to_database(*db, 500, 500);
    ASSERT_EQ(db->records.size(), 1000);

    // Verify we can read the new records from the database
    EXPECT_TRUE(verify_record(
        db->dat_path.string(), db->key_path.string(), db->records[500], ec));
    EXPECT_TRUE(verify_record(
        db->dat_path.string(), db->key_path.string(), db->records[999], ec));

    // Reopen mmap after appending
    debug_mmap.open(db->dat_path.string());
    ASSERT_TRUE(debug_mmap.is_open());

    // Phase 4: Extend the index
    auto extend_result =
        nudbutil::IndexBuilder::extend(db->dat_path.string(), index_path, opts);

    ASSERT_TRUE(extend_result.success) << extend_result.error_message;
    EXPECT_EQ(extend_result.total_records, 1000);
    EXPECT_EQ(extend_result.entry_count, 20);  // 1000 / 50 = 20 entries

    // Phase 5: Load extended index and verify
    nudbutil::IndexReader extended_index;
    ASSERT_TRUE(extended_index.load(index_path, ec)) << ec.message();

    // Debug: dump all entries
    std::cout << "\n=== Extended Index Debug ===" << std::endl;
    extended_index.dump_entries(std::cout, 25);
    std::cout << "==========================\n" << std::endl;

    EXPECT_EQ(extended_index.total_records(), 1000);
    EXPECT_EQ(extended_index.entry_count(), 20);

    // Phase 6: Verify offsets from both original and extended portions
    auto const* verify_dat_data =
        reinterpret_cast<const std::uint8_t*>(debug_mmap.data());
    nudbview::detail::dat_file_header verify_dh;
    nudbview::detail::istream verify_is{
        verify_dat_data, nudbview::detail::dat_file_header::size};
    nudbview::detail::read(verify_is, verify_dh);

    // Check all indexed entries (0, 50, 100, ..., 950)
    //
    // IMPORTANT: "record_num" is the Nth data record in physical file order,
    // NOT insertion order!
    //
    // Why records aren't in insertion order:
    // - NuDB buffers inserts in a std::map sorted by lexicographic key order
    // (memcmp)
    // - On commit, it iterates the map and writes records to .dat in sorted key
    // order
    // - So even though we insert 0→1→2→3 sequentially, SHA512(0), SHA512(1),
    // etc. have
    //   random byte values that sort differently than 0, 1, 2, 3
    // - The .dat file contains records in sorted SHA512 key order, not
    // insertion order
    //
    // Therefore we just verify each indexed offset points to a valid record
    // from our test set
    for (std::uint64_t record_num = 0; record_num < 1000; record_num += 50)
    {
        nudbview::noff_t closest_offset;
        std::uint64_t records_to_skip;

        extended_index.lookup_record_start_offset(
            record_num, closest_offset, records_to_skip);
        EXPECT_EQ(records_to_skip, 0)
            << "Record " << record_num << " should be indexed exactly";

        // Read the actual data record at this offset
        auto data_rec = read_record_at_offset(
            verify_dat_data,
            debug_mmap.size(),
            closest_offset,
            verify_dh.key_size);

        ASSERT_TRUE(data_rec.valid)
            << "Failed to read valid data record at offset " << closest_offset;

        // Verify it's a properly formatted record with correct sizes
        EXPECT_EQ(data_rec.key.size(), 64) << "Key should be 64 bytes (SHA512)";
        EXPECT_EQ(data_rec.value.size(), sizeof(std::uint32_t))
            << "Value should be 4 bytes";

        // Extract the value and verify it's in valid range [0, 999]
        std::uint32_t value;
        std::memcpy(&value, data_rec.value.data(), sizeof(value));
        EXPECT_LT(value, 1000) << "Value should be in range [0, 999]";

        // Verify the key matches the expected key for that value
        auto expected_key = generate_key(value);
        EXPECT_TRUE(std::equal(
            data_rec.key.begin(), data_rec.key.end(), expected_key.begin()))
            << "Key should match SHA512(" << value << ") at offset "
            << closest_offset;
    }
}

// TODO: Test slice creation
// TEST(NuDBView, CreateSlice)
// {
//     auto db = create_test_database(10000, "slice-test");
//
//     // Create slice for records 0-1000
//     // Verify slice files
//     // Verify records in slice
// }

// fetch_offset: the returned location must point at the record whose key
// and value bytes in the raw .dat file match what fetch returns.
TEST(NuDBView, FetchOffsetMatchesRawDatRecord)
{
    auto db = create_test_database(2000, "fetch-offset");

    nudbview::store store;
    nudbview::error_code ec;
    store.open(
        db->dat_path.string(),
        db->key_path.string(),
        db->log_path.string(),
        ec);
    ASSERT_FALSE(ec) << ec.message();

    boost::iostreams::mapped_file_source mmap;
    mmap.open(db->dat_path.string());
    ASSERT_TRUE(mmap.is_open());
    auto const* dat = reinterpret_cast<std::uint8_t const*>(mmap.data());
    auto const file_size = static_cast<std::uint64_t>(mmap.size());

    std::uint64_t min_offset = std::numeric_limits<std::uint64_t>::max();
    for (auto const& rec : db->records)
    {
        nudbview::store::record_location loc{};
        store.fetch_offset(rec.key.data(), loc, ec);
        ASSERT_FALSE(ec) << ec.message();
        EXPECT_EQ(loc.size, sizeof(rec.value));

        auto const raw = read_record_at_offset(
            dat, file_size, loc.offset, db->key_size);
        ASSERT_TRUE(raw.valid);
        ASSERT_EQ(raw.key.size(), rec.key.size());
        EXPECT_EQ(std::memcmp(raw.key.data(), rec.key.data(), rec.key.size()), 0);
        ASSERT_EQ(raw.value.size(), sizeof(rec.value));
        std::uint32_t value = 0;
        std::memcpy(&value, raw.value.data(), sizeof(value));
        EXPECT_EQ(value, rec.value);
        min_offset = std::min(min_offset, loc.offset);
    }
    // Every record lives after the data file header.
    EXPECT_GE(min_offset, nudbview::detail::dat_file_header::size);

    // An absent key is key_not_found.
    auto const absent = generate_key(1u << 30);
    nudbview::store::record_location loc{};
    store.fetch_offset(absent.data(), loc, ec);
    EXPECT_EQ(ec, nudbview::error::key_not_found);

    // A key inserted but not yet committed is key_pending; after the
    // commit that close performs it has an offset like any other.
    ec = {};
    auto const pending_key = generate_key(1u << 29);
    std::uint32_t const pending_value = 0xABCD1234u;
    store.insert(pending_key.data(), &pending_value, sizeof(pending_value), ec);
    ASSERT_FALSE(ec) << ec.message();
    store.fetch_offset(pending_key.data(), loc, ec);
    EXPECT_EQ(ec, nudbview::error::key_pending);

    ec = {};
    store.close(ec);
    ASSERT_FALSE(ec) << ec.message();
    store.open(
        db->dat_path.string(),
        db->key_path.string(),
        db->log_path.string(),
        ec);
    ASSERT_FALSE(ec) << ec.message();
    store.fetch_offset(pending_key.data(), loc, ec);
    ASSERT_FALSE(ec) << ec.message();
    EXPECT_EQ(loc.size, sizeof(pending_value));
    // The file grew; re-map and check the committed bytes.
    mmap.close();
    mmap.open(db->dat_path.string());
    ASSERT_TRUE(mmap.is_open());
    dat = reinterpret_cast<std::uint8_t const*>(mmap.data());
    auto const raw = read_record_at_offset(
        dat, static_cast<std::uint64_t>(mmap.size()), loc.offset, db->key_size);
    ASSERT_TRUE(raw.valid);
    std::uint32_t value = 0;
    std::memcpy(&value, raw.value.data(), sizeof(value));
    EXPECT_EQ(value, pending_value);
    store.close(ec);
    ASSERT_FALSE(ec) << ec.message();
}

// batch: pending inserts commit first, appended records land at the
// offsets the appender reported, a same-key duplicate is allowed, and
// everything survives a reopen.
TEST(NuDBView, BatchAppendsCommitAndSurviveReopen)
{
    auto db = create_test_database(1000, "batch");

    nudbview::store store;
    nudbview::error_code ec;
    store.open(
        db->dat_path.string(),
        db->key_path.string(),
        db->log_path.string(),
        ec);
    ASSERT_FALSE(ec) << ec.message();

    // Five inserts left pending when the batch starts.
    for (std::uint32_t i = 1000; i < 1005; ++i)
    {
        auto const k = generate_key(i);
        store.insert(k.data(), &i, sizeof(i), ec);
        ASSERT_FALSE(ec) << ec.message();
    }

    struct Appended
    {
        std::array<std::uint8_t, 64> key;
        std::uint32_t value;
        nudbview::noff_t offset;
    };
    std::vector<Appended> appended;
    nudbview::noff_t first_batch_offset = 0;
    auto const dup_key = db->records[7].key;
    std::uint32_t const dup_value = 0xDEADBEEFu;
    nudbview::noff_t dup_offset = 0;

    store.batch(
        [&](nudbview::store::appender& a) {
            first_batch_offset = a.offset();
            nudbview::noff_t last = 0;
            for (std::uint32_t i = 2000; i < 2500; ++i)
            {
                auto const k = generate_key(i);
                nudbview::error_code aec;
                auto const off = a.append(k.data(), &i, sizeof(i), aec);
                ASSERT_FALSE(aec) << aec.message();
                EXPECT_GT(off, last);
                last = off;
                appended.push_back({k, i, off});
            }
            nudbview::error_code aec;
            dup_offset = a.append(
                dup_key.data(), &dup_value, sizeof(dup_value), aec);
            ASSERT_FALSE(aec) << aec.message();
            EXPECT_EQ(a.size(), 501u);
        },
        ec);
    ASSERT_FALSE(ec) << ec.message();

    boost::iostreams::mapped_file_source mmap;
    mmap.open(db->dat_path.string());
    ASSERT_TRUE(mmap.is_open());
    auto const* dat = reinterpret_cast<std::uint8_t const*>(mmap.data());
    auto const file_size = static_cast<std::uint64_t>(mmap.size());

    // The pending inserts were committed before the batch.
    for (std::uint32_t i = 1000; i < 1005; ++i)
    {
        auto const k = generate_key(i);
        nudbview::store::record_location loc{};
        store.fetch_offset(k.data(), loc, ec);
        ASSERT_FALSE(ec) << ec.message();
        EXPECT_LT(loc.offset, first_batch_offset);
    }

    // Appended records are where the appender said, byte for byte.
    for (auto const& rec : appended)
    {
        nudbview::store::record_location loc{};
        store.fetch_offset(rec.key.data(), loc, ec);
        ASSERT_FALSE(ec) << ec.message();
        EXPECT_EQ(loc.offset, rec.offset);
        EXPECT_EQ(loc.size, sizeof(rec.value));
        auto const raw =
            read_record_at_offset(dat, file_size, rec.offset, db->key_size);
        ASSERT_TRUE(raw.valid);
        EXPECT_EQ(
            std::memcmp(raw.key.data(), rec.key.data(), rec.key.size()), 0);
        std::uint32_t value = 0;
        std::memcpy(&value, raw.value.data(), sizeof(value));
        EXPECT_EQ(value, rec.value);
    }

    // The duplicate exists as its own record; fetch returns one of the two.
    {
        auto const raw =
            read_record_at_offset(dat, file_size, dup_offset, db->key_size);
        ASSERT_TRUE(raw.valid);
        EXPECT_EQ(
            std::memcmp(raw.key.data(), dup_key.data(), dup_key.size()), 0);
        std::uint32_t value = 0;
        std::memcpy(&value, raw.value.data(), sizeof(value));
        EXPECT_EQ(value, dup_value);

        std::uint32_t fetched = 0;
        store.fetch(
            dup_key.data(),
            [&](void const* p, std::size_t n) {
                ASSERT_EQ(n, sizeof(fetched));
                std::memcpy(&fetched, p, n);
            },
            ec);
        ASSERT_FALSE(ec) << ec.message();
        EXPECT_TRUE(fetched == 7u || fetched == dup_value);
    }
    mmap.close();

    store.close(ec);
    ASSERT_FALSE(ec) << ec.message();
    EXPECT_FALSE(fs::exists(db->log_path));

    // Reopen: still there, same offsets.
    nudbview::store again;
    again.open(
        db->dat_path.string(),
        db->key_path.string(),
        db->log_path.string(),
        ec);
    ASSERT_FALSE(ec) << ec.message();
    for (auto const* rec : {&appended.front(), &appended.back()})
    {
        nudbview::store::record_location loc{};
        again.fetch_offset(rec->key.data(), loc, ec);
        ASSERT_FALSE(ec) << ec.message();
        EXPECT_EQ(loc.offset, rec->offset);
    }
    again.close(ec);
    ASSERT_FALSE(ec) << ec.message();
    EXPECT_TRUE(verify_record(
        db->dat_path.string(), db->key_path.string(), db->records[0], ec));
}

// batch: a throwing function tears the batch. Nothing it appended is
// visible after recovery, and what was committed before is intact.
TEST(NuDBView, BatchThrowIsTornAndRecovered)
{
    auto db = create_test_database(500, "batch-torn");
    auto const dat_size_before = fs::file_size(db->dat_path);
    auto const key_size_before = fs::file_size(db->key_path);

    nudbview::error_code ec;
    auto const k = generate_key(9000);
    std::uint32_t const v = 42;
    {
        nudbview::store store;
        store.open(
            db->dat_path.string(),
            db->key_path.string(),
            db->log_path.string(),
            ec);
        ASSERT_FALSE(ec) << ec.message();

        store.batch(
            [&](nudbview::store::appender& a) {
                nudbview::error_code aec;
                a.append(k.data(), &v, sizeof(v), aec);
                throw std::runtime_error("boom");
            },
            ec);
        EXPECT_EQ(ec, nudbview::error::batch_aborted);

        // Poisoned: every later call reports the same error.
        ec = {};
        store.fetch(k.data(), [](void const*, std::size_t) {}, ec);
        EXPECT_EQ(ec, nudbview::error::batch_aborted);
        ec = {};
        store.close(ec);
        EXPECT_EQ(ec, nudbview::error::batch_aborted);
    }
    // The log left behind drives recovery on the next open.
    EXPECT_TRUE(fs::exists(db->log_path));

    nudbview::store again;
    ec = {};
    again.open(
        db->dat_path.string(),
        db->key_path.string(),
        db->log_path.string(),
        ec);
    ASSERT_FALSE(ec) << ec.message();
    EXPECT_EQ(fs::file_size(db->dat_path), dat_size_before);
    EXPECT_EQ(fs::file_size(db->key_path), key_size_before);

    nudbview::store::record_location loc{};
    again.fetch_offset(k.data(), loc, ec);
    EXPECT_EQ(ec, nudbview::error::key_not_found);
    ec = {};
    again.close(ec);
    ASSERT_FALSE(ec) << ec.message();

    EXPECT_TRUE(verify_record(
        db->dat_path.string(), db->key_path.string(), db->records[0], ec));
    EXPECT_TRUE(verify_record(
        db->dat_path.string(), db->key_path.string(), db->records[499], ec));
}

// batch: a function that appends nothing leaves the store unchanged and
// the log cleared.
TEST(NuDBView, EmptyBatchIsNoOp)
{
    auto db = create_test_database(200, "batch-empty");
    auto const dat_size_before = fs::file_size(db->dat_path);
    auto const key_size_before = fs::file_size(db->key_path);

    nudbview::store store;
    nudbview::error_code ec;
    store.open(
        db->dat_path.string(),
        db->key_path.string(),
        db->log_path.string(),
        ec);
    ASSERT_FALSE(ec) << ec.message();

    bool called = false;
    store.batch(
        [&](nudbview::store::appender& a) {
            called = true;
            EXPECT_EQ(a.size(), 0u);
        },
        ec);
    ASSERT_FALSE(ec) << ec.message();
    EXPECT_TRUE(called);
    EXPECT_EQ(fs::file_size(db->dat_path), dat_size_before);
    EXPECT_EQ(fs::file_size(db->key_path), key_size_before);
    EXPECT_EQ(fs::file_size(db->log_path), 0u);

    // Still a working store: locate and fetch a record.
    nudbview::store::record_location loc{};
    store.fetch_offset(db->records[3].key.data(), loc, ec);
    ASSERT_FALSE(ec) << ec.message();
    EXPECT_EQ(loc.size, sizeof(std::uint32_t));
    store.close(ec);
    ASSERT_FALSE(ec) << ec.message();
    EXPECT_TRUE(verify_record(
        db->dat_path.string(), db->key_path.string(), db->records[3], ec));
}

namespace {

// Count the bucket entries that resolve to `key`, in the block and its
// spill chain, by parsing the key and data files directly.
struct IndexEntries
{
    std::size_t count = 0;
    std::size_t in_spill = 0;
};

IndexEntries
locate_index_entries(
    TestDatabase const& db,
    std::array<std::uint8_t, 64> const& key)
{
    using namespace nudbview::detail;
    boost::iostreams::mapped_file_source kf;
    kf.open(db.key_path.string());
    boost::iostreams::mapped_file_source df;
    df.open(db.dat_path.string());
    auto const* kdat = reinterpret_cast<std::uint8_t const*>(kf.data());
    auto const* ddat = reinterpret_cast<std::uint8_t const*>(df.data());

    key_file_header kh;
    istream is{kdat, key_file_header::size};
    read(is, static_cast<nudbview::noff_t>(kf.size()), kh);

    auto const h = nudbview::detail::hash<nudbview::xxhasher>(
        key.data(), kh.key_size, kh.salt);
    auto const n = bucket_index(h, kh.buckets, kh.modulus);
    std::vector<std::uint8_t> blk(
        kdat + (n + 1) * kh.block_size,
        kdat + (n + 2) * kh.block_size);
    bucket b{kh.block_size, blk.data()};

    IndexEntries out;
    bool spilled = false;
    for (;;)
    {
        for (auto i = b.lower_bound(h); i < b.size(); ++i)
        {
            auto const item = b[i];
            if (item.hash != h)
                break;
            if (std::memcmp(ddat + item.offset + 6, key.data(), kh.key_size) ==
                0)
            {
                ++out.count;
                if (spilled)
                    ++out.in_spill;
            }
        }
        auto const spill = b.spill();
        if (!spill)
            break;
        // A spill record is [48-bit zero][16-bit size][bucket], and spill()
        // names the bucket. Copy only its serialized bytes, which must lie
        // inside the data file, into a zeroed block: the record is not padded
        // to the block size.
        auto const size_field = field<std::uint16_t>::size;
        if (spill < size_field || spill > df.size())
            throw std::runtime_error("spill offset outside the data file");
        std::uint16_t size = 0;
        istream ss{ddat + spill - size_field, size_field};
        read<std::uint16_t>(ss, size);
        if (size > kh.block_size || size > df.size() - spill)
            throw std::runtime_error("spill record outside the data file");
        blk.assign(kh.block_size, 0);
        std::memcpy(blk.data(), ddat + spill, size);
        b = bucket{kh.block_size, blk.data()};
        spilled = true;
    }
    return out;
}

std::size_t
count_index_entries(
    TestDatabase const& db,
    std::array<std::uint8_t, 64> const& key)
{
    return locate_index_entries(db, key).count;
}

std::uint32_t
fetch_u32(nudbview::store& store, std::array<std::uint8_t, 64> const& key)
{
    std::uint32_t v = 0;
    nudbview::error_code ec;
    store.fetch(
        key.data(),
        [&](void const* p, std::size_t n) {
            if (n == sizeof(v))
                std::memcpy(&v, p, n);
        },
        ec);
    if (ec)
        throw std::runtime_error("fetch: " + ec.message());
    return v;
}

}  // namespace

// replace: the key keeps exactly one index entry, pointing at the newest
// record, across in-block entries, spill chains, repeated replaces, and a
// reopen. A tiny block size makes spills common.
TEST(NuDBView, ReplaceSwingsTheSingleIndexEntry)
{
    auto db = std::make_unique<TestDatabase>();
    db->dir = fs::temp_directory_path() /
        fs::unique_path("nudb-replace-%%%%-%%%%");
    fs::create_directories(db->dir);
    db->dat_path = db->dir / "nudb.dat";
    db->key_path = db->dir / "nudb.key";
    db->log_path = db->dir / "nudb.log";
    db->block_size = 256;  // capacity (256 - 8) / 18 = 13 entries
    for (std::uint32_t i = 0; i < 3000; ++i)
        db->records.push_back({generate_key(i), i});

    nudbview::error_code ec;
    nudbview::create<nudbview::xxhasher, nudbview::native_file>(
        db->dat_path.string(), db->key_path.string(), db->log_path.string(),
        db->appnum, db->uid, db->key_size, db->block_size, db->load_factor,
        ec);
    ASSERT_FALSE(ec) << ec.message();
    {
        nudbview::store w;
        w.open(
            db->dat_path.string(), db->key_path.string(),
            db->log_path.string(), ec);
        ASSERT_FALSE(ec) << ec.message();
        for (auto const& r : db->records)
        {
            w.insert(r.key.data(), &r.value, sizeof(r.value), ec);
            ASSERT_FALSE(ec) << ec.message();
        }
        w.close(ec);
        ASSERT_FALSE(ec) << ec.message();
    }

    // Some buckets must have spilled for this test to mean anything.
    std::size_t spilled = 0;
    for (std::uint32_t i = 0; i < 3000; i += 7)
        if (count_index_entries(*db, db->records[i].key) != 1)
            FAIL() << "record " << i << " not indexed exactly once";
    {
        boost::iostreams::mapped_file_source ddf;
        ddf.open(db->dat_path.string());
        auto const* d = reinterpret_cast<std::uint8_t const*>(ddf.data());
        for (std::uint64_t off = 92; off + 6 <= ddf.size();)
        {
            std::uint64_t sz = 0;
            for (int k = 0; k < 6; ++k)
                sz = (sz << 8) | d[off + k];
            if (sz == 0)
            {
                ++spilled;
                std::uint16_t ss = static_cast<std::uint16_t>(
                    (d[off + 6] << 8) | d[off + 7]);
                off += 8 + ss;
            }
            else
                off += 6 + 64 + sz;
        }
    }
    ASSERT_GT(spilled, 0u) << "no spill records; raise the record count";
    // Some of the keys about to be replaced must have their stale entry in
    // a spill record, or the spill-chain path is not exercised.
    std::size_t targets_in_spill = 0;
    for (std::uint32_t i = 0; i < 3000; i += 5)
        targets_in_spill += locate_index_entries(*db, db->records[i].key).in_spill;
    ASSERT_GT(targets_in_spill, 0u) << "no replace target sits in a spill";
    auto const key_file_size_before = fs::file_size(db->key_path);

    nudbview::store store;
    store.open(
        db->dat_path.string(), db->key_path.string(), db->log_path.string(),
        ec);
    ASSERT_FALSE(ec) << ec.message();

    // Replace every 5th key once, and key 10 three times in one batch.
    std::vector<std::pair<std::array<std::uint8_t, 64>, std::uint32_t>>
        expect;
    std::array<std::uint8_t, 64> const absent = generate_key(1u << 20);
    store.batch(
        [&](nudbview::store::appender& a) {
            nudbview::error_code aec;
            for (std::uint32_t i = 0; i < 3000; i += 5)
            {
                std::uint32_t const v = 100000 + i;
                a.replace(db->records[i].key.data(), &v, sizeof(v), aec);
                ASSERT_FALSE(aec) << aec.message();
                expect.push_back({db->records[i].key, v});
            }
            for (std::uint32_t v = 1; v <= 3; ++v)
            {
                std::uint32_t const vv = 777000 + v;
                a.replace(db->records[10].key.data(), &vv, sizeof(vv), aec);
                ASSERT_FALSE(aec) << aec.message();
            }
            expect[2].second = 777003;  // records[10] is the third entry
        },
        ec);
    ASSERT_FALSE(ec) << ec.message();
    // Replacing existing keys adds no entries and therefore no buckets,
    // however many times the split threshold was crossed.
    EXPECT_EQ(fs::file_size(db->key_path), key_file_size_before);

    for (auto const& [key, v] : expect)
        EXPECT_EQ(fetch_u32(store, key), v);

    // An absent key through replace is an ordinary insert, and may split.
    store.batch(
        [&](nudbview::store::appender& a) {
            nudbview::error_code aec;
            std::uint32_t const av = 4242;
            a.replace(absent.data(), &av, sizeof(av), aec);
            ASSERT_FALSE(aec) << aec.message();
        },
        ec);
    ASSERT_FALSE(ec) << ec.message();
    EXPECT_EQ(fetch_u32(store, absent), 4242u);
    // Untouched keys still resolve to their original values.
    for (std::uint32_t i = 1; i < 3000; i += 5)
        EXPECT_EQ(fetch_u32(store, db->records[i].key), i);

    // A second batch replaces the same keys again.
    store.batch(
        [&](nudbview::store::appender& a) {
            nudbview::error_code aec;
            for (auto& [key, v] : expect)
            {
                v += 1;
                a.replace(key.data(), &v, sizeof(v), aec);
                ASSERT_FALSE(aec) << aec.message();
            }
        },
        ec);
    ASSERT_FALSE(ec) << ec.message();
    for (auto const& [key, v] : expect)
        EXPECT_EQ(fetch_u32(store, key), v);
    store.close(ec);
    ASSERT_FALSE(ec) << ec.message();

    // Exactly one index entry per key, replaced or not, and the values
    // survive reopen.
    for (auto const& [key, v] : expect)
        EXPECT_EQ(count_index_entries(*db, key), 1u);
    EXPECT_EQ(count_index_entries(*db, absent), 1u);
    for (std::uint32_t i = 1; i < 3000; i += 5)
        EXPECT_EQ(count_index_entries(*db, db->records[i].key), 1u);

    nudbview::store again;
    again.open(
        db->dat_path.string(), db->key_path.string(), db->log_path.string(),
        ec);
    ASSERT_FALSE(ec) << ec.message();
    for (auto const& [key, v] : expect)
        EXPECT_EQ(fetch_u32(again, key), v);
    for (std::uint32_t i = 1; i < 3000; i += 5)
        EXPECT_EQ(fetch_u32(again, db->records[i].key), i);
    again.close(ec);
    ASSERT_FALSE(ec) << ec.message();
}

// rekey: rebuilding .key from a .dat that holds superseded records must
// reproduce the live winner: the later record, indexed exactly once.
TEST(NuDBView, RekeyReproducesThePromotedWinner)
{
    auto db = std::make_unique<TestDatabase>();
    db->dir = fs::temp_directory_path() /
        fs::unique_path("nudb-rekey-promote-%%%%-%%%%");
    fs::create_directories(db->dir);
    db->dat_path = db->dir / "nudb.dat";
    db->key_path = db->dir / "nudb.key";
    db->log_path = db->dir / "nudb.log";
    db->block_size = 256;  // spills abound
    for (std::uint32_t i = 0; i < 3000; ++i)
        db->records.push_back({generate_key(i), i});

    nudbview::error_code ec;
    nudbview::create<nudbview::xxhasher, nudbview::native_file>(
        db->dat_path.string(), db->key_path.string(), db->log_path.string(),
        db->appnum, db->uid, db->key_size, db->block_size, db->load_factor,
        ec);
    ASSERT_FALSE(ec) << ec.message();
    std::vector<std::pair<std::array<std::uint8_t, 64>, std::uint32_t>>
        expect;
    {
        nudbview::store w;
        w.open(
            db->dat_path.string(), db->key_path.string(),
            db->log_path.string(), ec);
        ASSERT_FALSE(ec) << ec.message();
        for (auto const& r : db->records)
        {
            w.insert(r.key.data(), &r.value, sizeof(r.value), ec);
            ASSERT_FALSE(ec) << ec.message();
        }
        // Two rounds of replaces so some keys have three records.
        for (std::uint32_t round = 1; round <= 2; ++round)
        {
            w.batch(
                [&](nudbview::store::appender& a) {
                    nudbview::error_code aec;
                    for (std::uint32_t i = 0; i < 3000; i += 3)
                    {
                        std::uint32_t const v = 100000 * round + i;
                        a.replace(
                            db->records[i].key.data(), &v, sizeof(v), aec);
                        ASSERT_FALSE(aec) << aec.message();
                    }
                },
                ec);
            ASSERT_FALSE(ec) << ec.message();
        }
        for (std::uint32_t i = 0; i < 3000; i += 3)
            expect.push_back({db->records[i].key, 200000 + i});
        w.close(ec);
        ASSERT_FALSE(ec) << ec.message();
    }

    // Throw the key file away and rebuild it from the data file alone.
    fs::remove(db->key_path);
    nudbview::rekey<nudbview::xxhasher, nudbview::native_file>(
        db->dat_path.string(),
        db->key_path.string(),
        db->log_path.string(),
        db->block_size,
        db->load_factor,
        3000,
        64 * 1024,
        ec,
        nudbview::no_progress{});
    ASSERT_FALSE(ec) << ec.message();

    for (auto const& [key, v] : expect)
        EXPECT_EQ(count_index_entries(*db, key), 1u);
    for (std::uint32_t i = 1; i < 3000; i += 3)
        EXPECT_EQ(count_index_entries(*db, db->records[i].key), 1u);

    nudbview::store store;
    store.open(
        db->dat_path.string(), db->key_path.string(), db->log_path.string(),
        ec);
    ASSERT_FALSE(ec) << ec.message();
    for (auto const& [key, v] : expect)
        EXPECT_EQ(fetch_u32(store, key), v);
    for (std::uint32_t i = 1; i < 3000; i += 3)
        EXPECT_EQ(fetch_u32(store, db->records[i].key), i);
    store.close(ec);
    ASSERT_FALSE(ec) << ec.message();
}

// posix_file mapped read path: initial map, growth and remap, in-place
// write visibility, truncation and short read, and the overflow guard.
TEST(NuDBView, PosixFileMappedReads)
{
    auto const dir = fs::temp_directory_path() /
        fs::unique_path("nudb-mapped-%%%%-%%%%");
    fs::create_directories(dir);
    auto const path = (dir / "f.bin").string();
    nudbview::error_code ec;
    nudbview::posix_file f;
    f.create(nudbview::file_mode::write, path, ec);
    ASSERT_FALSE(ec) << ec.message();

    std::vector<std::uint8_t> a(8192, 0xA1);
    f.write(0, a.data(), a.size(), ec);
    ASSERT_FALSE(ec);
    std::vector<std::uint8_t> out(8192);
    f.read(0, out.data(), out.size(), ec);  // creates the mapping
    ASSERT_FALSE(ec);
    EXPECT_EQ(out, a);

    // Growth past the mapping: the next read must see appended bytes.
    std::vector<std::uint8_t> b(70000, 0xB2);
    f.write(8192, b.data(), b.size(), ec);
    ASSERT_FALSE(ec);
    std::vector<std::uint8_t> tail(b.size());
    f.read(8192, tail.data(), tail.size(), ec);
    ASSERT_FALSE(ec);
    EXPECT_EQ(tail, b);

    // In-place overwrite is visible through the mapping.
    std::vector<std::uint8_t> c(4096, 0xC3);
    f.write(4096, c.data(), c.size(), ec);
    ASSERT_FALSE(ec);
    std::vector<std::uint8_t> mid(4096);
    f.read(4096, mid.data(), mid.size(), ec);
    ASSERT_FALSE(ec);
    EXPECT_EQ(mid, c);

    // Past the end: short_read, never a copy from headroom.
    std::vector<std::uint8_t> beyond(16);
    f.read(8192 + 70000 - 8, beyond.data(), beyond.size(), ec);
    EXPECT_EQ(ec, nudbview::error::short_read);
    ec = {};

    // Truncate, then a read in the removed range is a short read.
    f.trunc(4096, ec);
    ASSERT_FALSE(ec);
    std::vector<std::uint8_t> gone(4096);
    f.read(4096, gone.data(), gone.size(), ec);
    EXPECT_EQ(ec, nudbview::error::short_read);
    ec = {};
    std::vector<std::uint8_t> kept(4096);
    f.read(0, kept.data(), kept.size(), ec);
    ASSERT_FALSE(ec);
    EXPECT_EQ(kept, std::vector<std::uint8_t>(4096, 0xA1));

    // Overflow: offset + bytes wraps; must not pass the range check.
    std::vector<std::uint8_t> ov(64);
    f.read(std::numeric_limits<std::uint64_t>::max() - 8, ov.data(), ov.size(), ec);
    EXPECT_TRUE(ec);
    ec = {};
    f.close();
    fs::remove_all(dir);
}
