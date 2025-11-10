#include "common-options.hpp"

#include <catl/core/logger.h>
#include <nudbview/detail/format.hpp>

#include <boost/filesystem.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/program_options.hpp>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace fs = boost::filesystem;
namespace po = boost::program_options;
namespace io = boost::iostreams;

namespace nudbutil {

// Log partitions for different aspects of count-keys
LogPartition scan_log("SCAN", LogLevel::INFO);  // File scanning operations
LogPartition record_log(
    "RECORD",
    LogLevel::NONE);  // Individual record processing (enable with
                      // --verbose-logging)
LogPartition progress_log(
    "PROGRESS",
    LogLevel::NONE);  // Progress reporting (enable with --progress)
LogPartition stats_log("STATS", LogLevel::INFO);  // Statistics and results

// Read a 48-bit big-endian value (6 bytes)
// NuDB stores uint48 as big-endian (MSB first)
inline std::uint64_t
read_uint48(const std::uint8_t* p)
{
    return (static_cast<std::uint64_t>(p[0]) << 40) |
        (static_cast<std::uint64_t>(p[1]) << 32) |
        (static_cast<std::uint64_t>(p[2]) << 24) |
        (static_cast<std::uint64_t>(p[3]) << 16) |
        (static_cast<std::uint64_t>(p[4]) << 8) |
        static_cast<std::uint64_t>(p[5]);
}

/**
 * count-keys command: Ultra-fast key counting using mmap
 *
 * This uses mmap to memory-map the .dat file and manually walks
 * through records, reading only the size field and key, then
 * skipping past the data payload.
 */
int
run_count_keys(int argc, char* argv[])
{
    // Set up options
    po::options_description desc("count-keys options");
    add_common_options(desc);

    // Add count-keys specific options
    desc.add_options()(
        "progress,p",
        po::bool_switch(),
        "Show progress updates during counting")(
        "verbose-logging,v",
        po::bool_switch(),
        "Enable verbose record-level logging (very detailed)");

    try
    {
        // Parse command line
        po::variables_map vm;
        po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);

        // Check for help
        if (vm.count("help"))
        {
            std::cout << "nudb-util count-keys - Ultra-fast key counting\n\n"
                      << "Usage: nudb-util count-keys [options]\n\n"
                      << desc << std::endl;
            return 0;
        }

        po::notify(vm);

        // Parse common options
        CommonOptions common = parse_common_options(vm);
        bool show_progress = vm["progress"].as<bool>();
        bool verbose_logging = vm["verbose-logging"].as<bool>();

        // Set log level
        if (!Logger::set_level(common.log_level))
        {
            Logger::set_level(LogLevel::INFO);
            LOGW("Unrecognized log level: ", common.log_level, ", using INFO");
        }

        // Enable progress logging if requested
        if (show_progress)
        {
            progress_log.enable(LogLevel::INFO);
        }

        // Enable verbose record logging if requested (super detailed!)
        if (verbose_logging)
        {
            LOGI("Verbose logging enabled - RECORD partition at DEBUG level");
            record_log.enable(LogLevel::DEBUG);
        }

        if (!common.nudb_path)
        {
            LOGE("--nudb-path is required");
            return 1;
        }

        // Validate database path exists
        fs::path db_path(*common.nudb_path);
        PLOGD(scan_log, "Checking database path: ", *common.nudb_path);

        if (!fs::exists(db_path))
        {
            LOGE("Database path does not exist: ", *common.nudb_path);
            return 1;
        }

        fs::path dat_file = db_path / "nudb.dat";
        PLOGD(scan_log, "Looking for dat file: ", dat_file.string());

        if (!fs::exists(dat_file))
        {
            LOGE("Database file not found: ", dat_file);
            return 1;
        }

        std::string dat_path = dat_file.string();
        PLOGI(scan_log, "Opening database file: ", dat_path);

        // Memory-map the file using Boost
        io::mapped_file_source mmap;
        try
        {
            PLOGD(scan_log, "Attempting to memory-map file: ", dat_path);
            mmap.open(dat_path);
            PLOGI(scan_log, "Successfully memory-mapped file");
        }
        catch (const std::exception& e)
        {
            LOGE("Failed to mmap file: ", e.what());
            return 1;
        }

        if (!mmap.is_open())
        {
            LOGE("Failed to open memory-mapped file");
            return 1;
        }

        const std::uint8_t* data =
            reinterpret_cast<const std::uint8_t*>(mmap.data());
        std::uint64_t file_size = mmap.size();

        PLOGD(scan_log, "Memory-mapped ", file_size, " bytes at ", (void*)data);

        // Read dat file header (92 bytes)
        if (file_size < nudbview::detail::dat_file_header::size)
        {
            LOGE("File too small to contain header (", file_size, " bytes)");
            return 1;
        }

        PLOGD(
            scan_log,
            "Reading dat file header (",
            nudbview::detail::dat_file_header::size,
            " bytes)");

        // Parse header to get key_size
        nudbview::detail::dat_file_header header;
        nudbview::detail::istream is(
            data, nudbview::detail::dat_file_header::size);
        nudbview::detail::read(is, header);

        std::uint16_t key_size = header.key_size;

        PLOGD(
            scan_log,
            "Header parsed - version: ",
            header.version,
            ", uid: ",
            header.uid,
            ", appnum: ",
            header.appnum,
            ", key_size: ",
            key_size);

        // Verify it's a valid dat file
        std::string type(header.type, 8);
        if (type != "nudb.dat")
        {
            LOGE("Not a valid nudb.dat file (type: '", type, "')");
            return 1;
        }

        PLOGI(scan_log, "Valid nudb.dat file detected");
        PLOGI(stats_log, "Database info:");
        PLOGI(stats_log, "  Key size: ", key_size, " bytes");
        PLOGI(stats_log, "  File size: ", file_size, " bytes");

        // Start timing
        auto start_time = std::chrono::high_resolution_clock::now();

        PLOGI(
            scan_log,
            "Starting scan from offset ",
            nudbview::detail::dat_file_header::size);

        // Show first 32 bytes after header for debugging
        if (scan_log.should_log(LogLevel::DEBUG))
        {
            std::ostringstream hex_dump;
            hex_dump << "First 32 bytes after header: ";
            hex_dump << std::hex << std::setfill('0');
            std::uint64_t dump_offset = nudbview::detail::dat_file_header::size;
            for (int i = 0; i < 32 && dump_offset + i < file_size; ++i)
            {
                if (i > 0 && i % 8 == 0)
                    hex_dump << " | ";
                hex_dump << std::setw(2) << (int)data[dump_offset + i] << " ";
            }
            PLOGD(scan_log, hex_dump.str());
        }

        // Walk through data records after header
        std::uint64_t offset = nudbview::detail::dat_file_header::size;
        std::uint64_t key_count = 0;
        std::uint64_t spill_count = 0;
        std::uint64_t total_data_size = 0;
        std::uint64_t last_progress_keys = 0;

        // Progress update frequency (every 100MB)
        const std::uint64_t progress_interval = 100ULL * 1024 * 1024;
        std::uint64_t next_progress_offset = offset + progress_interval;

        PLOGD(
            scan_log,
            "Progress interval: ",
            progress_interval,
            " bytes (",
            progress_interval / (1024 * 1024),
            " MB)");

        while (offset + 6 <= file_size)
        {
            // Read size (48-bit / 6 bytes)
            std::uint64_t size = read_uint48(data + offset);

            // Log the raw bytes for debugging
            if (record_log.should_log(LogLevel::DEBUG))
            {
                std::ostringstream hex_bytes;
                hex_bytes << std::hex << std::setfill('0');
                for (int i = 0; i < 6; ++i)
                {
                    hex_bytes << std::setw(2) << (int)data[offset + i];
                    if (i < 5)
                        hex_bytes << " ";
                }
                PLOGD(
                    record_log,
                    "Offset ",
                    offset,
                    ": raw bytes = ",
                    hex_bytes.str(),
                    " -> size = ",
                    size);
            }

            offset += 6;

            if (size > 0)
            {
                // Data Record: has key + data
                PLOGD(record_log, "  Data Record detected (size=", size, ")");

                // Check we have room for key + data
                if (offset + key_size + size > file_size)
                {
                    // Truncated record at end of file
                    LOGW(
                        "Truncated record at offset ",
                        offset,
                        " (need ",
                        key_size + size,
                        " bytes, only ",
                        file_size - offset,
                        " available)");
                    break;
                }

                PLOGD(
                    record_log,
                    "  Skipping key (",
                    key_size,
                    " bytes) and data (",
                    size,
                    " bytes)");

                // Skip key (key_size bytes) - we don't need to read it
                offset += key_size;

                // Skip data payload
                offset += size;

                // Count this key
                ++key_count;
                total_data_size += size;

                PLOGD(
                    record_log,
                    "  Key #",
                    key_count,
                    " counted, total data: ",
                    total_data_size,
                    " bytes, new offset: ",
                    offset);
            }
            else
            {
                // Spill Record (size == 0): has uint16_t bucket_size + bucket
                // data No key in spill records!
                PLOGD(record_log, "  Spill Record detected");

                // Read bucket size (2 bytes)
                if (offset + 2 > file_size)
                {
                    LOGW("Truncated spill record at offset ", offset);
                    break;
                }

                std::uint16_t bucket_size =
                    static_cast<std::uint16_t>(data[offset]) |
                    (static_cast<std::uint16_t>(data[offset + 1]) << 8);
                offset += 2;

                PLOGD(
                    record_log, "  Spill bucket size: ", bucket_size, " bytes");

                // Skip bucket data
                if (offset + bucket_size > file_size)
                {
                    LOGW("Truncated spill bucket at offset ", offset);
                    break;
                }
                offset += bucket_size;

                ++spill_count;
                PLOGD(
                    record_log,
                    "  Spill #",
                    spill_count,
                    " skipped, new offset: ",
                    offset);

                // Don't count spill records as keys
            }

            // Show progress
            if (offset >= next_progress_offset)
            {
                double percent =
                    (static_cast<double>(offset) / file_size) * 100.0;
                std::uint64_t keys_since_last = key_count - last_progress_keys;
                double mb_scanned = offset / (1024.0 * 1024.0);
                double mb_total = file_size / (1024.0 * 1024.0);

                PLOGI(
                    progress_log,
                    "Progress: ",
                    static_cast<int>(mb_scanned),
                    " / ",
                    static_cast<int>(mb_total),
                    " MB (",
                    static_cast<int>(percent),
                    "%) | Keys: ",
                    key_count,
                    " (+",
                    keys_since_last,
                    ") | Spills: ",
                    spill_count);

                next_progress_offset = offset + progress_interval;
                last_progress_keys = key_count;
            }
        }

        PLOGI(
            scan_log,
            "Scan complete - stopped at offset ",
            offset,
            " / ",
            file_size);

        // Stop timing
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time);

        PLOGD(scan_log, "Timing stopped, duration: ", duration.count(), " ms");

        // mmap will be automatically closed when it goes out of scope

        // Output results
        PLOGI(stats_log, "");
        PLOGI(stats_log, "=== Scan Results ===");
        PLOGI(stats_log, "Total keys: ", key_count);
        PLOGI(stats_log, "Total spill records: ", spill_count);
        PLOGI(
            stats_log,
            "Total data size: ",
            total_data_size,
            " bytes (",
            total_data_size / (1024 * 1024),
            " MB)");
        PLOGI(
            stats_log,
            "Bytes scanned: ",
            offset,
            " / ",
            file_size,
            " (",
            (offset * 100 / file_size),
            "%)");

        if (key_count > 0)
        {
            PLOGI(
                stats_log,
                "Average data per key: ",
                (total_data_size / key_count),
                " bytes");
        }

        PLOGI(stats_log, "Scan time: ", duration.count(), " ms");

        if (duration.count() > 0)
        {
            double keys_per_sec =
                (static_cast<double>(key_count) / duration.count()) * 1000.0;
            double mb_per_sec =
                (static_cast<double>(file_size) / (1024.0 * 1024.0)) /
                (duration.count() / 1000.0);
            PLOGI(
                stats_log,
                "Scan rate: ",
                static_cast<std::uint64_t>(keys_per_sec),
                " keys/sec (",
                static_cast<int>(mb_per_sec),
                " MB/sec)");
        }

        PLOGI(stats_log, "==================");

        return 0;
    }
    catch (const po::required_option& e)
    {
        LOGE("Missing required option: ", e.what());
        std::cout << desc << std::endl;
        return 1;
    }
    catch (const std::exception& e)
    {
        LOGE("Exception during count-keys: ", e.what());
        return 1;
    }
}

}  // namespace nudbutil
