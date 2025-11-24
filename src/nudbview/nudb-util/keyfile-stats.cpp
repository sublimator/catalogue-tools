#include "common-options.hpp"

#include <catl/core/logger.h>
#include <nudbview/detail/bucket.hpp>
#include <nudbview/detail/format.hpp>
#include <nudbview/keyfile-stats-dashboard.h>
#include <nudbview/native_file.hpp>

#include <boost/filesystem.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/program_options.hpp>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <vector>

namespace fs = boost::filesystem;
namespace po = boost::program_options;

namespace nudbutil {

// Log partitions
LogPartition keyfile_log("KEYFILE", LogLevel::INFO);
LogPartition keyfile_progress_log("PROGRESS", LogLevel::NONE);

/**
 * keyfile-stats command: Analyze key file and generate statistics
 *
 * Walks through all buckets in a key file and produces:
 * - Total entries across all buckets
 * - Histogram of entries per bucket
 * - Hash collision statistics
 * - Spill record detection
 * - Capacity utilization metrics
 */
int
run_keyfile_stats(int argc, char* argv[])
{
    // Set up options
    po::options_description desc("keyfile-stats options");
    add_common_options(desc);

    // Add keyfile-stats specific options
    desc.add_options()(
        "progress,p",
        po::bool_switch(),
        "Show progress updates during analysis")(
        "histogram,H",
        po::bool_switch(),
        "Show detailed entry count histogram")(
        "collision-details,c",
        po::bool_switch(),
        "Show detailed collision information for each bucket")(
        "key-file,k",
        po::value<std::string>(),
        "Path to .key file (default: nudb.key in nudb-path)")(
        "dashboard,d",
        po::bool_switch(),
        "Show live FTXUI dashboard during analysis")(
        "json,j",
        po::value<std::string>(),
        "Output results as JSON to specified file");

    try
    {
        // Parse command line
        po::variables_map vm;
        po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);

        // Check for help
        if (vm.count("help"))
        {
            std::cout
                << "nudb-util keyfile-stats - Analyze key file statistics\n\n"
                << "Usage: nudb-util keyfile-stats [options]\n\n"
                << "Analyzes key file buckets and produces comprehensive "
                   "statistics:\n"
                << "- Entry count distribution\n"
                << "- Hash collision detection\n"
                << "- Spill record tracking\n"
                << "- Capacity utilization\n\n"
                << desc << std::endl;
            return 0;
        }

        po::notify(vm);

        // Parse options
        CommonOptions common = parse_common_options(vm);
        bool show_progress = vm["progress"].as<bool>();
        bool show_histogram = vm["histogram"].as<bool>();
        bool show_collision_details = vm["collision-details"].as<bool>();
        bool enable_dashboard = vm["dashboard"].as<bool>();
        std::string json_output_path;
        if (vm.count("json"))
        {
            json_output_path = vm["json"].as<std::string>();
        }

        // Set log level
        if (!Logger::set_level(common.log_level))
        {
            Logger::set_level(LogLevel::INFO);
            LOGW("Unrecognized log level: ", common.log_level, ", using INFO");
        }

        // Enable progress logging if requested (but not with dashboard)
        if (show_progress && !enable_dashboard)
        {
            keyfile_progress_log.enable(LogLevel::INFO);
        }

        // Setup dashboard if enabled
        std::shared_ptr<nudbview::KeyfileStatsDashboard> dashboard;
        std::ofstream log_file;

        if (enable_dashboard)
        {
            std::cout << "\n🎨 Starting dashboard..." << std::endl;
            std::cout << "   Redirecting logs to file" << std::endl;
            std::cout << "   Press 'q' in dashboard to quit\n" << std::endl;

            // Give user a moment to see the message
            std::this_thread::sleep_for(std::chrono::seconds(1));

            // Clear screen for dashboard
            std::cout << "\033[2J\033[H" << std::flush;

            // Redirect logs to file
            fs::path log_path =
                fs::path(*common.nudb_path) / "keyfile-stats.log";
            log_file.open(log_path.string(), std::ios::out | std::ios::trunc);
            if (log_file.is_open())
            {
                Logger::set_output_stream(&log_file);
                Logger::set_error_stream(&log_file);
            }

            dashboard = std::make_shared<nudbview::KeyfileStatsDashboard>();
            dashboard->start();
        }

        if (!common.nudb_path)
        {
            LOGE("--nudb-path is required");
            return 1;
        }

        // Build key file path
        fs::path key_file;
        if (vm.count("key-file"))
        {
            key_file = vm["key-file"].as<std::string>();
        }
        else
        {
            key_file = fs::path(*common.nudb_path) / "nudb.key";
        }

        PLOGI(keyfile_log, "Analyzing key file: ", key_file);

        // Check if key file exists
        if (!fs::exists(key_file))
        {
            LOGE("Key file not found: ", key_file);
            return 1;
        }

        std::uint64_t file_size = fs::file_size(key_file);
        PLOGI(keyfile_log, "  File size: ", file_size / (1024 * 1024), " MB");

        // Memory-map the key file
        boost::iostreams::mapped_file_source mmap;
        try
        {
            mmap.open(key_file.string());
        }
        catch (const std::exception& e)
        {
            LOGE("Failed to mmap key file: ", e.what());
            return 1;
        }

        if (!mmap.is_open())
        {
            LOGE("Failed to open memory-mapped key file");
            return 1;
        }

        auto const* data = reinterpret_cast<const std::uint8_t*>(mmap.data());

        // Read key file header
        if (file_size < nudbview::detail::key_file_header::size)
        {
            LOGE("File too small to contain header");
            return 1;
        }

        nudbview::detail::key_file_header kh;
        nudbview::detail::istream is{data, file_size};
        nudbview::detail::read(is, file_size, kh);

        // Display header info
        PLOGI(keyfile_log, "");
        PLOGI(keyfile_log, "=== Key File Header ===");
        PLOGI(keyfile_log, "  Version: ", kh.version);
        PLOGI(keyfile_log, "  UID: ", kh.uid);
        PLOGI(keyfile_log, "  Appnum: ", kh.appnum);
        PLOGI(keyfile_log, "  Key size: ", kh.key_size, " bytes");
        PLOGI(keyfile_log, "  Block size: ", kh.block_size, " bytes");
        PLOGI(
            keyfile_log,
            "  Load factor: ",
            kh.load_factor / 65536.0f,
            " (",
            kh.load_factor,
            "/65536)");
        PLOGI(keyfile_log, "  Buckets: ", kh.buckets);
        PLOGI(keyfile_log, "  Modulus: ", kh.modulus);
        PLOGI(
            keyfile_log,
            "  Capacity per bucket: ",
            kh.capacity,
            " entries max");

        // Initialize dashboard with file info
        if (dashboard)
        {
            nudbview::KeyfileStatsDashboard::Stats stats;
            stats.key_file_path = key_file.string();
            stats.file_size_mb = file_size / (1024 * 1024);
            stats.block_size = kh.block_size;
            stats.load_factor = kh.load_factor / 65536.0f;
            stats.total_buckets = kh.buckets;
            stats.capacity_per_bucket = kh.capacity;
            dashboard->update_stats(stats);
        }

        // Start timing
        auto start_time = std::chrono::high_resolution_clock::now();

        PLOGI(keyfile_log, "");
        PLOGI(keyfile_log, "=== Scanning Buckets ===");

        // Statistics tracking
        std::uint64_t total_entries = 0;
        std::uint64_t total_collisions = 0;
        std::uint64_t buckets_with_spills = 0;
        std::uint64_t empty_buckets = 0;
        std::uint64_t max_entries_in_bucket = 0;
        std::uint64_t full_buckets = 0;  // At capacity

        // Histogram: entry_count -> bucket_count
        std::map<std::size_t, std::uint64_t> entry_count_histogram;

        // Collision histogram: collision_count -> bucket_count
        std::map<std::size_t, std::uint64_t> collision_count_histogram;

        // Collision tracking per bucket (for detailed output)
        struct BucketCollisionInfo
        {
            nudbview::nbuck_t bucket_index;
            std::size_t entry_count;
            std::size_t collision_count;
            bool has_spill;
        };
        std::vector<BucketCollisionInfo> buckets_with_collisions;

        // Progress tracking
        std::uint64_t progress_interval =
            std::max<std::uint64_t>(1, kh.buckets / 100);

        // Walk through all buckets
        nudbview::detail::buffer buf;
        buf.reserve(kh.block_size);

        for (nudbview::nbuck_t n = 0; n < kh.buckets; ++n)
        {
            // Calculate bucket offset: (n+1) * block_size
            // (bucket 0 is at offset 1*block_size, header is at offset 0)
            nudbview::noff_t bucket_offset =
                static_cast<nudbview::noff_t>(n + 1) * kh.block_size;

            if (bucket_offset + kh.block_size > file_size)
            {
                LOGE("Bucket ", n, " offset exceeds file size");
                break;
            }

            // Copy bucket data to buffer
            std::memcpy(buf.get(), data + bucket_offset, kh.block_size);

            // Parse bucket
            nudbview::detail::bucket b{kh.block_size, buf.get()};

            std::size_t entry_count = b.size();
            total_entries += entry_count;

            // Update histogram
            ++entry_count_histogram[entry_count];

            // Track empty buckets
            if (entry_count == 0)
            {
                ++empty_buckets;
            }

            // Track max entries
            if (entry_count > max_entries_in_bucket)
            {
                max_entries_in_bucket = entry_count;
            }

            // Check if bucket is full
            if (entry_count >= kh.capacity)
            {
                ++full_buckets;
            }

            // Check for spill
            bool has_spill = (b.spill() != 0);
            if (has_spill)
            {
                ++buckets_with_spills;
            }

            // Detect hash collisions within this bucket
            // Entries are sorted by hash, so consecutive entries with same hash
            // = collision
            std::size_t collision_count = 0;
            if (entry_count > 1)
            {
                nudbview::detail::nhash_t prev_hash = b[0].hash;
                for (std::size_t i = 1; i < entry_count; ++i)
                {
                    auto current_hash = b[i].hash;
                    if (current_hash == prev_hash)
                    {
                        // Collision detected!
                        ++collision_count;
                        ++total_collisions;
                    }
                    prev_hash = current_hash;
                }
            }

            // Update collision count histogram
            ++collision_count_histogram[collision_count];

            // Track buckets with collisions for detailed output
            if (collision_count > 0)
            {
                buckets_with_collisions.push_back(BucketCollisionInfo{
                    n, entry_count, collision_count, has_spill});
            }

            // Progress reporting and dashboard updates
            if ((show_progress || dashboard) && (n % progress_interval == 0))
            {
                double percent = (static_cast<double>(n) / kh.buckets) * 100.0;

                if (show_progress)
                {
                    PLOGI(
                        keyfile_progress_log,
                        "Progress: ",
                        static_cast<int>(percent),
                        "% (",
                        n,
                        " / ",
                        kh.buckets,
                        " buckets)");
                }

                // Update dashboard with current stats
                if (dashboard)
                {
                    auto current_time =
                        std::chrono::high_resolution_clock::now();
                    auto elapsed =
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            current_time - start_time);
                    double elapsed_sec = elapsed.count() / 1000.0;
                    double rate = elapsed_sec > 0 ? n / elapsed_sec : 0.0;

                    nudbview::KeyfileStatsDashboard::Stats stats;
                    stats.buckets_scanned = n;
                    stats.total_buckets = kh.buckets;
                    stats.empty_buckets = empty_buckets;
                    stats.full_buckets = full_buckets;
                    stats.buckets_with_spills = buckets_with_spills;
                    stats.total_entries = total_entries;
                    stats.max_entries_in_bucket = max_entries_in_bucket;
                    stats.total_collisions = total_collisions;
                    stats.buckets_with_collisions =
                        buckets_with_collisions.size();
                    stats.capacity_per_bucket = kh.capacity;
                    stats.entry_count_histogram = entry_count_histogram;
                    stats.collision_count_histogram = collision_count_histogram;
                    stats.elapsed_sec = elapsed_sec;
                    stats.buckets_per_sec = rate;
                    stats.key_file_path = key_file.string();
                    stats.file_size_mb = file_size / (1024 * 1024);
                    stats.block_size = kh.block_size;
                    stats.load_factor = kh.load_factor / 65536.0f;
                    dashboard->update_stats(stats);
                }
            }
        }

        // Stop timing
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time);

        // Calculate statistics
        double avg_entries_per_bucket =
            static_cast<double>(total_entries) / kh.buckets;
        double utilization = avg_entries_per_bucket / kh.capacity;
        double empty_bucket_pct =
            (static_cast<double>(empty_buckets) / kh.buckets) * 100.0;

        // Final dashboard update (complete)
        if (dashboard)
        {
            double elapsed_sec = duration.count() / 1000.0;
            double rate = elapsed_sec > 0 ? kh.buckets / elapsed_sec : 0.0;

            nudbview::KeyfileStatsDashboard::Stats stats;
            stats.buckets_scanned = kh.buckets;
            stats.total_buckets = kh.buckets;
            stats.empty_buckets = empty_buckets;
            stats.full_buckets = full_buckets;
            stats.buckets_with_spills = buckets_with_spills;
            stats.total_entries = total_entries;
            stats.max_entries_in_bucket = max_entries_in_bucket;
            stats.total_collisions = total_collisions;
            stats.buckets_with_collisions = buckets_with_collisions.size();
            stats.capacity_per_bucket = kh.capacity;
            stats.entry_count_histogram = entry_count_histogram;
            stats.collision_count_histogram = collision_count_histogram;
            stats.elapsed_sec = elapsed_sec;
            stats.buckets_per_sec = rate;
            stats.key_file_path = key_file.string();
            stats.file_size_mb = file_size / (1024 * 1024);
            stats.block_size = kh.block_size;
            stats.load_factor = kh.load_factor / 65536.0f;
            dashboard->update_stats(stats);

            // Give user a moment to see final stats
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }

        // Output results
        PLOGI(keyfile_log, "");
        PLOGI(keyfile_log, "=== Statistics ===");
        PLOGI(
            keyfile_log, "Scan time: ", duration.count() / 1000.0, " seconds");
        PLOGI(keyfile_log, "");
        PLOGI(keyfile_log, "Buckets:");
        PLOGI(keyfile_log, "  Total: ", kh.buckets);
        PLOGI(
            keyfile_log,
            "  Empty: ",
            empty_buckets,
            " (",
            static_cast<int>(empty_bucket_pct),
            "%)");
        PLOGI(
            keyfile_log,
            "  Full (at capacity): ",
            full_buckets,
            " (",
            (full_buckets * 100 / kh.buckets),
            "%)");
        PLOGI(keyfile_log, "  With spills: ", buckets_with_spills);
        PLOGI(keyfile_log, "");
        PLOGI(keyfile_log, "Entries:");
        PLOGI(keyfile_log, "  Total: ", total_entries);
        PLOGI(
            keyfile_log,
            "  Average per bucket: ",
            std::fixed,
            std::setprecision(2),
            avg_entries_per_bucket);
        PLOGI(keyfile_log, "  Max in any bucket: ", max_entries_in_bucket);
        PLOGI(
            keyfile_log,
            "  Capacity utilization: ",
            std::fixed,
            std::setprecision(1),
            utilization * 100.0,
            "%");
        PLOGI(keyfile_log, "");
        PLOGI(keyfile_log, "Hash Collisions:");
        PLOGI(keyfile_log, "  Total collisions: ", total_collisions);
        PLOGI(
            keyfile_log,
            "  Buckets with collisions: ",
            buckets_with_collisions.size());
        if (total_entries > 0)
        {
            PLOGI(
                keyfile_log,
                "  Collision rate: ",
                std::fixed,
                std::setprecision(4),
                (static_cast<double>(total_collisions) / total_entries) * 100.0,
                "%");
        }

        // Show entry count histogram
        if (show_histogram)
        {
            PLOGI(keyfile_log, "");
            PLOGI(keyfile_log, "=== Entry Count Histogram ===");
            PLOGI(keyfile_log, "Entries | Buckets | Percentage");
            PLOGI(keyfile_log, "--------|---------|------------");

            for (auto const& [entry_count, bucket_count] :
                 entry_count_histogram)
            {
                double pct =
                    (static_cast<double>(bucket_count) / kh.buckets) * 100.0;
                std::cout << std::setw(7) << entry_count << " | "
                          << std::setw(7) << bucket_count << " | " << std::fixed
                          << std::setprecision(2) << std::setw(6) << pct
                          << "%\n";
            }
        }

        // Show collision details
        if (show_collision_details && !buckets_with_collisions.empty())
        {
            PLOGI(keyfile_log, "");
            PLOGI(keyfile_log, "=== Buckets with Hash Collisions ===");
            PLOGI(
                keyfile_log,
                "Showing top ",
                std::min<std::size_t>(20, buckets_with_collisions.size()),
                " buckets:");
            PLOGI(keyfile_log, "");
            PLOGI(keyfile_log, "Bucket | Entries | Collisions | Has Spill");
            PLOGI(keyfile_log, "-------|---------|------------|----------");

            // Sort by collision count (descending)
            std::sort(
                buckets_with_collisions.begin(),
                buckets_with_collisions.end(),
                [](auto const& a, auto const& b) {
                    return a.collision_count > b.collision_count;
                });

            // Show top 20
            std::size_t to_show =
                std::min<std::size_t>(20, buckets_with_collisions.size());
            for (std::size_t i = 0; i < to_show; ++i)
            {
                auto const& info = buckets_with_collisions[i];
                std::cout << std::setw(6) << info.bucket_index << " | "
                          << std::setw(7) << info.entry_count << " | "
                          << std::setw(10) << info.collision_count << " | "
                          << (info.has_spill ? "YES" : "NO") << "\n";
            }

            if (buckets_with_collisions.size() > 20)
            {
                PLOGI(
                    keyfile_log,
                    "... and ",
                    buckets_with_collisions.size() - 20,
                    " more buckets with collisions");
            }
        }

        PLOGI(keyfile_log, "");
        PLOGI(keyfile_log, "=== Analysis Complete ===");

        // JSON output
        if (!json_output_path.empty())
        {
            PLOGI(keyfile_log, "");
            PLOGI(keyfile_log, "Writing JSON output to: ", json_output_path);

            std::ofstream json_file(json_output_path);
            if (!json_file.is_open())
            {
                LOGE("Failed to open JSON output file: ", json_output_path);
                return 1;
            }

            json_file << "{\n";
            json_file << "  \"key_file\": \"" << key_file.string() << "\",\n";
            json_file << "  \"file_size_mb\": " << (file_size / (1024 * 1024))
                      << ",\n";
            json_file << "  \"header\": {\n";
            json_file << "    \"version\": " << kh.version << ",\n";
            json_file << "    \"uid\": " << kh.uid << ",\n";
            json_file << "    \"appnum\": " << kh.appnum << ",\n";
            json_file << "    \"key_size\": " << kh.key_size << ",\n";
            json_file << "    \"block_size\": " << kh.block_size << ",\n";
            json_file << "    \"load_factor\": " << (kh.load_factor / 65536.0f)
                      << ",\n";
            json_file << "    \"buckets\": " << kh.buckets << ",\n";
            json_file << "    \"modulus\": " << kh.modulus << ",\n";
            json_file << "    \"capacity\": " << kh.capacity << "\n";
            json_file << "  },\n";
            json_file << "  \"statistics\": {\n";
            json_file << "    \"scan_time_sec\": "
                      << (duration.count() / 1000.0) << ",\n";
            json_file << "    \"buckets\": {\n";
            json_file << "      \"total\": " << kh.buckets << ",\n";
            json_file << "      \"empty\": " << empty_buckets << ",\n";
            json_file << "      \"empty_pct\": " << empty_bucket_pct << ",\n";
            json_file << "      \"full\": " << full_buckets << ",\n";
            json_file << "      \"with_spills\": " << buckets_with_spills
                      << "\n";
            json_file << "    },\n";
            json_file << "    \"entries\": {\n";
            json_file << "      \"total\": " << total_entries << ",\n";
            json_file << "      \"avg_per_bucket\": " << avg_entries_per_bucket
                      << ",\n";
            json_file << "      \"max_in_bucket\": " << max_entries_in_bucket
                      << ",\n";
            json_file << "      \"capacity_utilization\": " << utilization
                      << "\n";
            json_file << "    },\n";
            json_file << "    \"collisions\": {\n";
            json_file << "      \"total\": " << total_collisions << ",\n";
            json_file << "      \"buckets_with_collisions\": "
                      << buckets_with_collisions.size();
            if (total_entries > 0)
            {
                json_file << ",\n      \"collision_rate_pct\": "
                          << ((static_cast<double>(total_collisions) /
                               total_entries) *
                              100.0);
            }
            json_file << "\n    },\n";
            json_file << "    \"entry_count_histogram\": {\n";
            bool first = true;
            for (auto const& [entry_count, bucket_count] :
                 entry_count_histogram)
            {
                if (!first)
                    json_file << ",\n";
                first = false;
                json_file << "      \"" << entry_count
                          << "\": " << bucket_count;
            }
            json_file << "\n    },\n";
            json_file << "    \"collision_count_histogram\": {\n";
            first = true;
            for (auto const& [collision_count, bucket_count] :
                 collision_count_histogram)
            {
                if (!first)
                    json_file << ",\n";
                first = false;
                json_file << "      \"" << collision_count
                          << "\": " << bucket_count;
            }
            json_file << "\n    }\n";
            json_file << "  }\n";
            json_file << "}\n";

            json_file.close();
            PLOGI(keyfile_log, "JSON output written successfully");
        }

        // Cleanup dashboard
        if (dashboard)
        {
            PLOGI(keyfile_log, "Stopping dashboard...");
            dashboard->stop();

            // Restore logger streams
            Logger::set_output_stream(&std::cout);
            Logger::set_error_stream(&std::cerr);

            if (log_file.is_open())
            {
                log_file.close();
            }

            // Show final message on console
            std::cout << "\n✓ Analysis complete!" << std::endl;
            std::cout
                << "  Logs written to: "
                << (fs::path(*common.nudb_path) / "keyfile-stats.log").string()
                << std::endl;
        }

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
        LOGE("Exception during keyfile-stats: ", e.what());
        return 1;
    }
}

}  // namespace nudbutil
