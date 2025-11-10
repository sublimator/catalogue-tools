#include "common-options.hpp"
#include <nudbview/view/dat_scanner.hpp>

#include <catl/core/logger.h>
#include <nudbview/native_file.hpp>
#include <nudbview/view/index_format.hpp>

#include <boost/filesystem.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/program_options.hpp>
#include <chrono>
#include <iostream>
#include <vector>

namespace fs = boost::filesystem;
namespace po = boost::program_options;

namespace nudbutil {

// Log partitions for index-dat
LogPartition index_log("INDEX", LogLevel::INFO);
LogPartition index_progress_log("PROGRESS", LogLevel::NONE);

/**
 * index-dat command: Build a global index for a .dat file
 *
 * Scans the entire .dat file once and creates an index file
 * that maps data record numbers to byte offsets for fast lookup.
 */
int
run_index_dat(int argc, char* argv[])
{
    // Set up options
    po::options_description desc("index-dat options");
    add_common_options(desc);

    // Add index-dat specific options
    desc.add_options()(
        "output,o",
        po::value<std::string>()->required(),
        "Output index file path")(
        "index-interval,i",
        po::value<std::uint64_t>()->default_value(10000),
        "Index every N records (default: 10000)")(
        "progress,p",
        po::bool_switch(),
        "Show progress updates during indexing")(
        "extend",
        po::bool_switch(),
        "Extend existing index file (append new entries)");

    try
    {
        // Parse command line
        po::variables_map vm;
        po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);

        // Check for help
        if (vm.count("help"))
        {
            std::cout
                << "nudb-util index-dat - Build global index for .dat file\n\n"
                << "Usage: nudb-util index-dat [options]\n\n"
                << "Creates an index file for fast record number → byte offset "
                   "lookup.\n"
                << "This enables creating slices by data record number instead "
                   "of byte offset.\n\n"
                << desc << std::endl;
            return 0;
        }

        po::notify(vm);

        // Parse options
        CommonOptions common = parse_common_options(vm);
        auto output_path = vm["output"].as<std::string>();
        auto index_interval = vm["index-interval"].as<std::uint64_t>();
        bool show_progress = vm["progress"].as<bool>();
        bool extend_mode = vm["extend"].as<bool>();

        // Set log level
        if (!Logger::set_level(common.log_level))
        {
            Logger::set_level(LogLevel::INFO);
            LOGW("Unrecognized log level: ", common.log_level, ", using INFO");
        }

        // Enable progress logging if requested
        if (show_progress)
        {
            index_progress_log.enable(LogLevel::INFO);
        }

        // Validate inputs
        if (!common.nudb_path)
        {
            LOGE("--nudb-path is required");
            return 1;
        }

        if (index_interval < 1)
        {
            LOGE("index-interval must be at least 1");
            return 1;
        }

        // Build paths
        fs::path dat_file = fs::path(*common.nudb_path) / "nudb.dat";
        fs::path index_file = output_path;

        PLOGI(index_log, "Building index for ", dat_file);
        PLOGI(index_log, "  Output: ", index_file);
        PLOGI(index_log, "  Index interval: ", index_interval, " records");

        // Check if dat file exists
        if (!fs::exists(dat_file))
        {
            LOGE("Database file not found: ", dat_file);
            return 1;
        }

        // Check if output file already exists
        if (fs::exists(index_file) && !extend_mode)
        {
            LOGE("Output index file already exists: ", index_file);
            LOGE("Use --extend to append to existing index");
            return 1;
        }

        if (!fs::exists(index_file) && extend_mode)
        {
            LOGE(
                "--extend specified but index file does not exist: ",
                index_file);
            return 1;
        }

        // Memory-map the dat file
        boost::iostreams::mapped_file_source mmap;
        try
        {
            mmap.open(dat_file.string());
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

        auto const* data = reinterpret_cast<const std::uint8_t*>(mmap.data());
        std::uint64_t file_size = mmap.size();

        PLOGI(index_log, "  File size: ", file_size / (1024 * 1024), " MB");

        // Read dat file header
        if (file_size < nudbview::detail::dat_file_header::size)
        {
            LOGE("File too small to contain header");
            return 1;
        }

        nudbview::detail::dat_file_header dh;
        nudbview::detail::istream is{
            data, nudbview::detail::dat_file_header::size};
        nudbview::detail::read(is, dh);

        nudbview::error_code ec;
        nudbview::detail::verify(dh, ec);
        if (ec)
        {
            LOGE("Invalid dat file header: ", ec.message());
            return 1;
        }

        PLOGI(index_log, "  Key size: ", dh.key_size, " bytes");

        // Variables for extend mode
        std::uint64_t start_offset = nudbview::detail::dat_file_header::size;
        std::uint64_t start_record_num = 0;
        std::uint64_t existing_total_records = 0;

        // Collect index entries
        std::vector<nudbview::noff_t> offsets;
        offsets.reserve(100000);  // Pre-allocate for ~1 billion records

        // If extending, read existing index file
        if (extend_mode)
        {
            PLOGI(index_log, "Reading existing index file for extension...");

            // Memory-map existing index file
            boost::iostreams::mapped_file_source index_mmap;
            try
            {
                index_mmap.open(index_file.string());
            }
            catch (const std::exception& e)
            {
                LOGE("Failed to mmap existing index file: ", e.what());
                return 1;
            }

            if (!index_mmap.is_open())
            {
                LOGE("Failed to open existing index file");
                return 1;
            }

            auto const* index_data =
                reinterpret_cast<const std::uint8_t*>(index_mmap.data());
            std::uint64_t index_file_size = index_mmap.size();

            // Read existing header
            if (index_file_size < nudbview::view::index_file_header::size)
            {
                LOGE("Existing index file too small");
                return 1;
            }

            nudbview::view::index_file_header existing_ifh;
            nudbview::detail::istream ifh_is{
                index_data, nudbview::view::index_file_header::size};
            nudbview::view::read(ifh_is, existing_ifh);

            // Verify existing header
            nudbview::view::verify(dh, existing_ifh, ec);
            if (ec)
            {
                LOGE(
                    "Existing index file header invalid or mismatched: ",
                    ec.message());
                return 1;
            }

            // Verify interval matches
            if (existing_ifh.index_interval != index_interval)
            {
                LOGE(
                    "Index interval mismatch: existing=",
                    existing_ifh.index_interval,
                    ", requested=",
                    index_interval);
                return 1;
            }

            existing_total_records = existing_ifh.total_records;

            PLOGI(index_log, "  Existing records: ", existing_total_records);
            PLOGI(index_log, "  Existing entries: ", existing_ifh.entry_count);

            // Read all existing offset entries
            offsets.reserve(existing_ifh.entry_count + 100000);
            std::uint64_t offset_array_offset =
                nudbview::view::index_file_header::size;

            for (std::uint64_t i = 0; i < existing_ifh.entry_count; ++i)
            {
                if (offset_array_offset + 8 > index_file_size)
                {
                    LOGE("Index file truncated at entry ", i);
                    return 1;
                }

                nudbview::detail::istream offset_is{
                    index_data + offset_array_offset, 8};
                nudbview::noff_t offset;
                nudbview::view::read_offset(offset_is, offset);
                offsets.push_back(offset);
                offset_array_offset += 8;
            }

            // Calculate starting point for new scan
            if (!offsets.empty())
            {
                start_offset = offsets.back();
                start_record_num = (offsets.size() - 1) * index_interval;

                PLOGI(index_log, "  Resuming from offset: ", start_offset);
                PLOGI(index_log, "  Resuming from record: ", start_record_num);
            }

            index_mmap.close();
        }

        // Start timing
        auto start_time = std::chrono::high_resolution_clock::now();

        std::uint64_t last_progress_offset = 0;
        const std::uint64_t progress_interval = 100ULL * 1024 * 1024;  // 100MB

        PLOGI(
            index_log,
            extend_mode ? "Scanning new records..." : "Scanning records...");

        std::uint64_t total_records = scan_dat_records(
            mmap,
            dh.key_size,
            [&](std::uint64_t record_num,
                std::uint64_t offset,
                std::uint64_t size) {
                // Record index entry if this is an interval boundary
                if (record_num % index_interval == 0)
                {
                    offsets.push_back(offset);
                }

                // Show progress
                if (show_progress &&
                    offset >= last_progress_offset + progress_interval)
                {
                    double percent =
                        (static_cast<double>(offset) / file_size) * 100.0;
                    PLOGI(
                        index_progress_log,
                        "Progress: ",
                        static_cast<int>(percent),
                        "% (",
                        offset / (1024 * 1024),
                        " / ",
                        file_size / (1024 * 1024),
                        " MB) | Records: ",
                        record_num);
                    last_progress_offset = offset;
                }
            },
            start_offset,
            start_record_num);

        // Stop timing
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time);

        PLOGI(index_log, "Scan complete!");
        if (extend_mode)
        {
            std::uint64_t new_records = total_records - existing_total_records;
            PLOGI(index_log, "  New records found: ", new_records);
            PLOGI(index_log, "  Total records now: ", total_records);
        }
        else
        {
            PLOGI(index_log, "  Total records: ", total_records);
        }
        PLOGI(index_log, "  Index entries: ", offsets.size());
        PLOGI(
            index_log, "  Scan time: ", duration.count() / 1000.0, " seconds");

        // Write index file
        PLOGI(
            index_log,
            extend_mode ? "Updating index file..." : "Writing index file...");

        // In extend mode, delete old file first (we'll rewrite with updated
        // header)
        if (extend_mode && fs::exists(index_file))
        {
            try
            {
                fs::remove(index_file);
            }
            catch (const std::exception& e)
            {
                LOGE("Failed to remove old index file: ", e.what());
                return 1;
            }
        }

        nudbview::native_file f;
        f.create(nudbview::file_mode::write, index_file.string(), ec);
        if (ec)
        {
            LOGE("Failed to create index file: ", ec.message());
            return 1;
        }

        // Prepare header (will write LAST as commit point)
        nudbview::view::index_file_header ifh;
        ifh.version = nudbview::view::index_file_version;
        ifh.uid = dh.uid;
        ifh.appnum = dh.appnum;
        ifh.key_size = dh.key_size;
        ifh.total_records = total_records;
        ifh.index_interval = index_interval;
        ifh.entry_count = offsets.size();

        // Write offset array in batches (much faster!)
        // Start AFTER header space - we'll write header at the end
        constexpr std::size_t batch_size = 8192;  // 8KB batches
        std::vector<std::uint8_t> batch_buf(batch_size * 8);

        PLOGI(
            index_log,
            "  Writing ",
            offsets.size(),
            " offsets in batches of ",
            batch_size);

        nudbview::noff_t file_offset = nudbview::view::index_file_header::size;
        std::size_t batch_count = 0;
        std::size_t batch_offset = 0;
        std::size_t total_written = 0;

        for (auto offset : offsets)
        {
            // Write offset to batch buffer
            nudbview::detail::ostream os{batch_buf.data() + batch_offset, 8};
            nudbview::view::write_offset(os, offset);
            batch_offset += 8;
            ++batch_count;

            // Flush batch when full
            if (batch_count >= batch_size)
            {
                PLOGD(
                    index_log,
                    "  Flushing batch: ",
                    batch_count,
                    " offsets (",
                    batch_offset,
                    " bytes)");
                f.write(file_offset, batch_buf.data(), batch_offset, ec);
                if (ec)
                {
                    LOGE("Failed to write offset batch: ", ec.message());
                    return 1;
                }
                file_offset += batch_offset;
                total_written += batch_count;
                PLOGD(
                    index_log,
                    "  Wrote batch successfully, total: ",
                    total_written,
                    " / ",
                    offsets.size());
                batch_count = 0;
                batch_offset = 0;
            }
        }

        // Flush remaining offsets
        if (batch_count > 0)
        {
            f.write(file_offset, batch_buf.data(), batch_offset, ec);
            if (ec)
            {
                LOGE("Failed to write final offset batch: ", ec.message());
                return 1;
            }
        }

        // NOW write header as final commit point
        // If we crashed above, file has no valid header yet
        PLOGI(index_log, "  Writing header (commit point)...");
        nudbview::view::write(f, ifh, ec);
        if (ec)
        {
            LOGE("Failed to write index header: ", ec.message());
            return 1;
        }

        f.sync(ec);
        if (ec)
        {
            LOGE("Failed to sync index file: ", ec.message());
            return 1;
        }

        auto index_size = fs::file_size(index_file);

        PLOGI(index_log, "Index file created successfully!");
        PLOGI(index_log, "  Size: ", index_size / 1024, " KB");
        PLOGI(index_log, "  Entries: ", offsets.size());

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
        LOGE("Exception during index-dat: ", e.what());
        return 1;
    }
}

}  // namespace nudbutil
