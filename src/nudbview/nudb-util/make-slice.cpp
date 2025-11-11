#include "common-options.hpp"
#include <nudbview/view/dat_scanner.hpp>
#include <nudbview/view/index_reader.hpp>

#include <catl/core/logger.h>
#include <nudbview/native_file.hpp>
#include <nudbview/view/rekey_slice.hpp>
#include <nudbview/xxhasher.hpp>

#include <boost/filesystem.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/program_options.hpp>
#include <chrono>
#include <iostream>

namespace fs = boost::filesystem;
namespace po = boost::program_options;

namespace nudbutil {

// Log partitions for make-slice
LogPartition slice_log("SLICE", LogLevel::INFO);
LogPartition slice_progress_log("PROGRESS", LogLevel::NONE);

/**
 * make-slice command: Create an optimized slice from a .dat file range
 *
 * Creates a slice key file and meta file for a contiguous range of
 * records in a .dat file. The slice can then be opened independently
 * for fast read-only access.
 */
int
run_make_slice(int argc, char* argv[])
{
    // Set up options
    po::options_description desc("make-slice options");
    add_common_options(desc);

    // Add make-slice specific options
    desc.add_options()
        // Record-based mode (requires --index)
        ("start",
         po::value<std::uint64_t>(),
         "First data record (inclusive, zero-based) - requires --index")(
            "exclusive-end",
            po::value<std::uint64_t>(),
            "Last data record (exclusive, Python range style) - requires "
            "--index")(
            "index",
            po::value<std::string>(),
            "Path to .index file (required for record-based mode)")
        // Byte-based mode
        ("start-byte",
         po::value<std::uint64_t>(),
         "First byte offset of slice (usually 92 for first slice)")(
            "end-byte",
            po::value<std::uint64_t>(),
            "Last byte offset of slice (inclusive)")
        // Common options
        ("output,o",
         po::value<std::string>()->required(),
         "Output file prefix (creates PREFIX.key and PREFIX.meta)")(
            "index-interval,i",
            po::value<std::uint64_t>()->default_value(10000),
            "Index every N records in slice meta (default: 10000)")(
            "block-size,b",
            po::value<std::size_t>()->default_value(4096),
            "Key file block size in bytes (default: 4096)")(
            "load-factor,f",
            po::value<float>()->default_value(0.5f),
            "Hash table load factor 0.0-1.0 (default: 0.5)")(
            "buffer-size",
            po::value<std::size_t>()->default_value(128 * 1024 * 1024),
            "Working memory buffer in bytes (default: 128MB)")(
            "progress,p",
            po::bool_switch(),
            "Show progress updates during slice creation");

    try
    {
        // Parse command line
        po::variables_map vm;
        po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);

        // Check for help
        if (vm.count("help"))
        {
            std::cout << "nudb-util make-slice - Create optimized slice from "
                         ".dat range\\n\\n"
                      << "Usage: nudb-util make-slice [options]\\n\\n"
                      << "Creates a slice key file and meta file for a subset "
                         "of a .dat file.\\n"
                      << "The slice provides fast read-only access without "
                         "duplicating data.\\n\\n"
                      << desc << std::endl;
            return 0;
        }

        po::notify(vm);

        // Parse options
        CommonOptions common = parse_common_options(vm);
        auto output_prefix = vm["output"].as<std::string>();
        auto index_interval = vm["index-interval"].as<std::uint64_t>();
        auto block_size = vm["block-size"].as<std::size_t>();
        auto load_factor = vm["load-factor"].as<float>();
        auto buffer_size = vm["buffer-size"].as<std::size_t>();
        bool show_progress = vm["progress"].as<bool>();

        // Determine mode (record-based vs byte-based)
        bool has_record_mode = vm.count("start") || vm.count("exclusive-end");
        bool has_byte_mode = vm.count("start-byte") || vm.count("end-byte");

        if (has_record_mode && has_byte_mode)
        {
            LOGE(
                "Cannot mix record-based (--start/--exclusive-end) and "
                "byte-based (--start-byte/--end-byte) options");
            return 1;
        }

        if (!has_record_mode && !has_byte_mode)
        {
            LOGE(
                "Must specify either record-based (--start/--exclusive-end) or "
                "byte-based (--start-byte/--end-byte) range");
            return 1;
        }

        // These will be set based on mode
        std::uint64_t start_offset;
        std::uint64_t end_offset;

        // Set log level
        if (!Logger::set_level(common.log_level))
        {
            Logger::set_level(LogLevel::INFO);
            LOGW("Unrecognized log level: ", common.log_level, ", using INFO");
        }

        // Enable progress logging if requested
        if (show_progress)
        {
            slice_progress_log.enable(LogLevel::INFO);
        }

        // Validate inputs
        if (!common.nudb_path)
        {
            LOGE("--nudb-path is required");
            return 1;
        }

        // Handle record-based mode
        if (has_record_mode)
        {
            if (!vm.count("start"))
            {
                LOGE("--start is required for record-based mode");
                return 1;
            }
            if (!vm.count("exclusive-end"))
            {
                LOGE("--exclusive-end is required for record-based mode");
                return 1;
            }
            if (!vm.count("index"))
            {
                LOGE("--index is required for record-based mode");
                return 1;
            }

            auto start_record = vm["start"].as<std::uint64_t>();
            auto exclusive_end_record = vm["exclusive-end"].as<std::uint64_t>();
            auto index_path = vm["index"].as<std::string>();

            if (exclusive_end_record <= start_record)
            {
                LOGE("--exclusive-end must be greater than --start");
                return 1;
            }

            PLOGI(slice_log, "Record-based mode:");
            PLOGI(slice_log, "  Start record: ", start_record);
            PLOGI(slice_log, "  Exclusive end record: ", exclusive_end_record);
            PLOGI(
                slice_log,
                "  Records in range: ",
                exclusive_end_record - start_record);

            // Load index file
            PLOGI(slice_log, "  Loading index: ", index_path);
            IndexReader index;
            nudbview::error_code ec;
            if (!index.load(index_path, ec))
            {
                LOGE("Failed to load index file: ", ec.message());
                return 1;
            }

            PLOGI(slice_log, "  Index total records: ", index.total_records());
            PLOGI(slice_log, "  Index interval: ", index.index_interval());

            // Validate record range
            if (start_record >= index.total_records())
            {
                LOGE(
                    "--start (",
                    start_record,
                    ") is beyond total records (",
                    index.total_records(),
                    ")");
                return 1;
            }
            if (exclusive_end_record > index.total_records())
            {
                LOGE(
                    "--exclusive-end (",
                    exclusive_end_record,
                    ") is beyond total records (",
                    index.total_records(),
                    ")");
                return 1;
            }

            // Translate start record to byte offset
            nudbview::noff_t start_byte_offset;
            std::uint64_t start_records_to_skip;
            if (!index.lookup_record_start_offset(
                    start_record, start_byte_offset, start_records_to_skip))
            {
                LOGE("Failed to lookup start record in index");
                return 1;
            }

            PLOGI(
                slice_log,
                "  Start: index offset=",
                start_byte_offset,
                ", skip=",
                start_records_to_skip,
                " records");

            // Translate exclusive-end record to byte offset
            // OPTIMIZATION: Look up the FIRST record OUTSIDE the slice
            // (exclusive_end) That gives us the START of that record, which is
            // the END of our last record!
            nudbview::noff_t end_byte_offset;
            std::uint64_t end_records_to_skip;
            if (!index.lookup_record_start_offset(
                    exclusive_end_record, end_byte_offset, end_records_to_skip))
            {
                LOGE("Failed to lookup end record in index");
                return 1;
            }

            PLOGI(
                slice_log,
                "  End: index offset=",
                end_byte_offset,
                ", skip=",
                end_records_to_skip,
                " records (to first record outside slice)");

            // OPTIMIZATION: If both start and end require zero scanning, use
            // offsets directly!
            if (start_records_to_skip == 0 && end_records_to_skip == 0)
            {
                PLOGI(
                    slice_log,
                    "  ⚡ Index hit! Using offsets directly (zero scanning "
                    "needed)");
                start_offset = start_byte_offset;
                end_offset = end_byte_offset;
            }
            else
            {
                // Need to scan - open dat file
                PLOGI(
                    slice_log,
                    "  Scanning forward to resolve exact boundaries...");
                fs::path dat_file = fs::path(*common.nudb_path) / "nudb.dat";
                if (!fs::exists(dat_file))
                {
                    LOGE("Database file not found: ", dat_file);
                    return 1;
                }

                boost::iostreams::mapped_file_source dat_mmap;
                try
                {
                    dat_mmap.open(dat_file.string());
                }
                catch (const std::exception& e)
                {
                    LOGE("Failed to mmap dat file: ", e.what());
                    return 1;
                }

                // Read dat header to verify index matches
                auto const* dat_data =
                    reinterpret_cast<const std::uint8_t*>(dat_mmap.data());
                nudbview::detail::dat_file_header dh;
                nudbview::detail::istream dh_is{
                    dat_data, nudbview::detail::dat_file_header::size};
                nudbview::detail::read(dh_is, dh);

                if (!index.verify_match(dh, ec))
                {
                    LOGE("Index file does not match dat file: ", ec.message());
                    return 1;
                }

                // Scan forward from start_byte_offset to find actual start
                if (start_records_to_skip > 0)
                {
                    PLOGI(
                        slice_log,
                        "  Scanning ",
                        start_records_to_skip,
                        " records from start...");
                    std::uint64_t scanned = 0;
                    bool found_start = false;
                    scan_dat_records(
                        dat_mmap,
                        dh.key_size,
                        [&]([[maybe_unused]] std::uint64_t record_num,
                            std::uint64_t offset,
                            [[maybe_unused]] std::uint64_t size) {
                            if (scanned == start_records_to_skip)
                            {
                                start_offset = offset;
                                found_start = true;
                            }
                            ++scanned;
                        },
                        start_byte_offset,
                        0);

                    if (!found_start)
                    {
                        LOGE("Failed to find start record by scanning");
                        return 1;
                    }
                }
                else
                {
                    start_offset = start_byte_offset;
                }

                // Scan forward from end_byte_offset to find where record
                // exclusive_end_record STARTS
                if (end_records_to_skip > 0)
                {
                    PLOGI(
                        slice_log,
                        "  Scanning ",
                        end_records_to_skip,
                        " records from end...");
                    std::uint64_t scanned = 0;
                    bool found_end = false;
                    scan_dat_records(
                        dat_mmap,
                        dh.key_size,
                        [&]([[maybe_unused]] std::uint64_t record_num,
                            std::uint64_t offset,
                            [[maybe_unused]] std::uint64_t size) {
                            if (scanned == end_records_to_skip)
                            {
                                end_offset = offset;
                                found_end = true;
                            }
                            ++scanned;
                        },
                        end_byte_offset,
                        0);

                    if (!found_end)
                    {
                        LOGE("Failed to find end boundary by scanning");
                        return 1;
                    }
                }
                else
                {
                    end_offset = end_byte_offset;
                }
            }

            PLOGI(
                slice_log,
                "  Resolved byte range: ",
                start_offset,
                " - ",
                end_offset);
            PLOGI(
                slice_log,
                "  Size: ",
                (end_offset - start_offset) / (1024 * 1024),
                " MB");
        }
        else
        {
            // Byte-based mode
            if (!vm.count("start-byte"))
            {
                LOGE("--start-byte is required for byte-based mode");
                return 1;
            }
            if (!vm.count("end-byte"))
            {
                LOGE("--end-byte is required for byte-based mode");
                return 1;
            }

            start_offset = vm["start-byte"].as<std::uint64_t>();
            end_offset = vm["end-byte"].as<std::uint64_t>();

            if (end_offset <= start_offset)
            {
                LOGE("--end-byte must be greater than --start-byte");
                return 1;
            }

            // TODO: Validate that start_offset and end_offset are at record
            // boundaries Could scan forward from nearby index entries (if index
            // provided) or validate by reading record size fields. For now,
            // user must ensure they're providing correct boundaries (e.g., from
            // previous index lookups).

            PLOGI(slice_log, "Byte-based mode:");
            PLOGI(slice_log, "  Start offset: ", start_offset);
            PLOGI(slice_log, "  End offset: ", end_offset);
            PLOGI(
                slice_log,
                "  ⚠️  WARNING: Byte offsets must be at record "
                "boundaries!");
        }

        if (load_factor <= 0.0f || load_factor >= 1.0f)
        {
            LOGE("load-factor must be between 0 and 1 (recommended: 0.5)");
            return 1;
        }

        if (index_interval < 1)
        {
            LOGE("index-interval must be at least 1");
            return 1;
        }

        // Build paths
        fs::path dat_file = fs::path(*common.nudb_path) / "nudb.dat";
        fs::path key_file = output_prefix + ".key";
        fs::path meta_file = output_prefix + ".meta";

        PLOGI(slice_log, "Creating slice from ", dat_file);
        PLOGI(
            slice_log,
            "  Range: ",
            start_offset,
            " - ",
            end_offset,
            " (",
            (end_offset - start_offset) / (1024 * 1024),
            " MB)");
        PLOGI(slice_log, "  Output: ", key_file, " + ", meta_file);
        PLOGI(slice_log, "  Block size: ", block_size, " bytes");
        PLOGI(slice_log, "  Load factor: ", load_factor);
        PLOGI(slice_log, "  Index interval: ", index_interval, " records");
        PLOGI(slice_log, "  Buffer size: ", buffer_size / (1024 * 1024), " MB");

        // Check if dat file exists
        if (!fs::exists(dat_file))
        {
            LOGE("Database file not found: ", dat_file);
            return 1;
        }

        // Check if output files already exist
        if (fs::exists(key_file))
        {
            LOGE("Output key file already exists: ", key_file);
            return 1;
        }
        if (fs::exists(meta_file))
        {
            LOGE("Output meta file already exists: ", meta_file);
            return 1;
        }

        // Progress callback
        std::uint64_t last_progress = 0;
        auto progress_callback = [&](std::uint64_t amount,
                                     std::uint64_t total) {
            if (!show_progress)
                return;

            // Report every 5% or so
            std::uint64_t percent = (amount * 100) / total;
            std::uint64_t last_percent = (last_progress * 100) / total;

            if (percent >= last_percent + 5 || amount == total)
            {
                PLOGI(
                    slice_progress_log,
                    "Progress: ",
                    percent,
                    "% (",
                    amount / (1024 * 1024),
                    " / ",
                    total / (1024 * 1024),
                    " MB)");
                last_progress = amount;
            }
        };

        // Start timing
        auto start_time = std::chrono::high_resolution_clock::now();

        PLOGI(slice_log, "Starting slice creation...");

        // Create the slice!
        nudbview::error_code ec;
        nudbview::view::rekey_slice<nudbview::xxhasher, nudbview::native_file>(
            dat_file.string(),
            start_offset,
            end_offset,
            key_file.string(),
            meta_file.string(),
            block_size,
            load_factor,
            index_interval,
            buffer_size,
            ec,
            progress_callback);

        if (ec)
        {
            LOGE("Failed to create slice: ", ec.message());
            return 1;
        }

        // Stop timing
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time);

        PLOGI(slice_log, "Slice created successfully!");
        PLOGI(slice_log, "Time: ", duration.count() / 1000.0, " seconds");
        PLOGI(slice_log, "Files:");
        PLOGI(
            slice_log,
            "  ",
            key_file,
            " (",
            fs::file_size(key_file) / 1024,
            " KB)");
        PLOGI(
            slice_log,
            "  ",
            meta_file,
            " (",
            fs::file_size(meta_file) / 1024,
            " KB)");

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
        LOGE("Exception during make-slice: ", e.what());
        return 1;
    }
}

}  // namespace nudbutil
