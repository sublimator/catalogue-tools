#pragma once

#include <array>
#include <boost/filesystem.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <catl/crypto/sha512-hasher.h>
#include <cstdint>
#include <cstring>
#include <nudbview/nudb.hpp>
#include <nudbview/view/dat_scanner.hpp>
#include <nudbview/view/index_builder.hpp>
#include <nudbview/view/index_reader.hpp>
#include <nudbview/xxhasher.hpp>
#include <span>
#include <vector>

namespace nudbview_test {

/**
 * Key-value pair for test database
 */
struct TestRecord
{
    std::array<std::uint8_t, 64> key;  // SHA512 = 64 bytes
    std::uint32_t value;
};

/**
 * Test database paths and metadata
 */
struct TestDatabase
{
    boost::filesystem::path dir;
    boost::filesystem::path dat_path;
    boost::filesystem::path key_path;
    boost::filesystem::path log_path;

    std::vector<TestRecord> records;

    std::uint64_t uid = 1;
    std::uint64_t appnum = 1;
    std::size_t key_size = 64;  // SHA512
    std::size_t block_size = 4096;
    float load_factor = 0.5f;

    ~TestDatabase()
    {
        // Clean up test files
        boost::filesystem::remove_all(dir);
    }
};

/**
 * Generate deterministic key from integer using SHA512
 */
inline std::array<std::uint8_t, 64>
generate_key(std::uint32_t n)
{
    catl::crypto::Sha512Hasher hasher;
    hasher.update(&n, sizeof(n));

    std::array<std::uint8_t, 64> key;
    unsigned int len;
    hasher.final(key.data(), &len);

    return key;
}

/**
 * Create a test NuDB database with N records
 *
 * Keys: SHA512(i) for i in [0, N)
 * Values: i as 32-bit little-endian integer
 *
 * @param record_count Number of records to create
 * @param prefix Directory prefix (will be created in /tmp)
 * @return TestDatabase with paths and all records
 */
inline std::unique_ptr<TestDatabase>
create_test_database(
    std::size_t record_count,
    std::string const& prefix = "nudb-test")
{
    auto db = std::make_unique<TestDatabase>();

    // Create unique temp directory
    db->dir = boost::filesystem::temp_directory_path() /
        boost::filesystem::unique_path(prefix + "-%%%%-%%%%-%%%%-%%%%");
    boost::filesystem::create_directories(db->dir);

    db->dat_path = db->dir / "nudb.dat";
    db->key_path = db->dir / "nudb.key";
    db->log_path = db->dir / "nudb.log";

    // Generate all test records first
    db->records.reserve(record_count);
    for (std::uint32_t i = 0; i < record_count; ++i)
    {
        TestRecord rec;
        rec.key = generate_key(i);
        rec.value = i;
        db->records.push_back(rec);
    }

    // Create NuDB database
    nudbview::error_code ec;
    nudbview::create<nudbview::xxhasher, nudbview::native_file>(
        db->dat_path.string(),
        db->key_path.string(),
        db->log_path.string(),
        db->appnum,
        db->uid,
        db->key_size,
        db->block_size,
        db->load_factor,
        ec);

    if (ec)
        throw std::runtime_error(
            "Failed to create test database: " + ec.message());

    // Open database for writing
    nudbview::basic_store<nudbview::xxhasher, nudbview::native_file> db_store;
    db_store.open(
        db->dat_path.string(),
        db->key_path.string(),
        db->log_path.string(),
        ec);

    if (ec)
        throw std::runtime_error(
            "Failed to open test database: " + ec.message());

    // Insert all records
    for (auto const& rec : db->records)
    {
        db_store.insert(rec.key.data(), &rec.value, sizeof(rec.value), ec);
        if (ec)
            throw std::runtime_error(
                "Failed to insert record: " + ec.message());
    }

    // Close and commit
    db_store.close(ec);
    if (ec)
        throw std::runtime_error(
            "Failed to close test database: " + ec.message());

    return db;
}

/**
 * Append additional records to an existing database
 *
 * @param db Database to append to
 * @param start_value Starting value for new records (e.g., 500 to add records
 * 500-999)
 * @param count Number of records to add
 */
inline void
append_to_database(
    TestDatabase& db,
    std::uint32_t start_value,
    std::size_t count)
{
    // Generate new records
    std::vector<TestRecord> new_records;
    new_records.reserve(count);
    for (std::uint32_t i = start_value; i < start_value + count; ++i)
    {
        TestRecord rec;
        rec.key = generate_key(i);
        rec.value = i;
        new_records.push_back(rec);
    }

    // Open existing database for writing
    nudbview::basic_store<nudbview::xxhasher, nudbview::native_file> db_store;
    nudbview::error_code ec;
    db_store.open(
        db.dat_path.string(), db.key_path.string(), db.log_path.string(), ec);

    if (ec)
        throw std::runtime_error(
            "Failed to open database for append: " + ec.message());

    // Insert new records
    for (auto const& rec : new_records)
    {
        db_store.insert(rec.key.data(), &rec.value, sizeof(rec.value), ec);
        if (ec)
            throw std::runtime_error(
                "Failed to insert record during append: " + ec.message());
    }

    // Close and commit
    db_store.close(ec);
    if (ec)
        throw std::runtime_error(
            "Failed to close database after append: " + ec.message());

    // Add new records to our tracking list
    db.records.insert(db.records.end(), new_records.begin(), new_records.end());
}

/**
 * Read a data record at a specific offset
 * Returns spans for key and value
 */
struct DataRecord
{
    std::span<const std::uint8_t> key;
    std::span<const std::uint8_t> value;
    bool valid;
};

inline DataRecord
read_record_at_offset(
    const std::uint8_t* dat_data,
    std::uint64_t file_size,
    std::uint64_t offset,
    std::uint16_t key_size)
{
    DataRecord rec{};
    rec.valid = false;

    // Check we can read the 6-byte size field
    if (offset + 6 > file_size)
        return rec;

    // Read size (big-endian)
    std::uint64_t size =
        (static_cast<std::uint64_t>(dat_data[offset + 0]) << 40) |
        (static_cast<std::uint64_t>(dat_data[offset + 1]) << 32) |
        (static_cast<std::uint64_t>(dat_data[offset + 2]) << 24) |
        (static_cast<std::uint64_t>(dat_data[offset + 3]) << 16) |
        (static_cast<std::uint64_t>(dat_data[offset + 4]) << 8) |
        static_cast<std::uint64_t>(dat_data[offset + 5]);

    if (size == 0)
        return rec;  // Spill record, not data record

    // Check bounds for key + value
    if (offset + 6 + key_size + size > file_size)
        return rec;

    // Point to key and value
    rec.key = std::span<const std::uint8_t>(dat_data + offset + 6, key_size);
    rec.value =
        std::span<const std::uint8_t>(dat_data + offset + 6 + key_size, size);
    rec.valid = true;

    return rec;
}

/**
 * Verify a record exists in database with correct value
 */
inline bool
verify_record(
    std::string const& dat_path,
    std::string const& key_path,
    TestRecord const& rec,
    nudbview::error_code& ec)
{
    nudbview::basic_store<nudbview::xxhasher, nudbview::native_file> db_store;
    // Generate log path from dat_path
    std::string log_path =
        dat_path.substr(0, dat_path.find_last_of('/')) + "/nudb.log";
    db_store.open(dat_path, key_path, log_path, ec);
    if (ec)
        return false;

    bool found = false;
    std::uint32_t value_out = 0;
    std::size_t size_out = 0;

    db_store.fetch(
        rec.key.data(),
        [&](void const* data, std::size_t size) {
            if (size == sizeof(std::uint32_t))
            {
                std::memcpy(&value_out, data, size);
                size_out = size;
                found = true;
            }
        },
        ec);

    db_store.close(ec);

    if (!found || ec)
        return false;

    return value_out == rec.value && size_out == sizeof(rec.value);
}

}  // namespace nudbview_test
