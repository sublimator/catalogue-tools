#include "common-options.hpp"
#include <catl/core/logger.h>
#include <nudbview/xxhasher.hpp>
#include <nudbview/detail/format.hpp>
#include <nudbview/detail/bucket.hpp>
#include <boost/program_options.hpp>
#include <map>
#include <vector>
#include <iostream>
#include <fstream>
#include <cstring>

namespace po = boost::program_options;
using namespace catl;

namespace nudbutil {

// Log partitions
LogPartition collisions_log("COLLISIONS", LogLevel::INFO);

/**
 * Generate deterministic key from integer seed
 * Simple approach: just spread the seed across the key bytes
 * (We only care about xxhash distribution, not crypto security)
 */
std::array<std::uint8_t, 32>
generate_key(std::uint32_t seed)
{
    std::array<std::uint8_t, 32> key;

    // Fill key with deterministic pattern based on seed
    // This ensures different seeds → different keys → different xxhashes
    for (std::size_t i = 0; i < 32; ++i)
    {
        key[i] = static_cast<std::uint8_t>((seed >> (i % 4 * 8)) ^ (i * 37));
    }

    return key;
}

/**
 * Find hash bucket collisions in a given seed range
 */
int
run_find_collisions(int argc, char* argv[])
{
    try
    {
        // Parse command-line options
        po::options_description desc("find-collisions options");

        std::uint32_t start_seed = 0;
        std::uint32_t end_seed = 10000;
        std::uint64_t salt = 1;
        std::size_t bucket_count = 100;
        std::size_t key_size = 32;
        std::size_t min_collisions = 17;  // Default: bucket capacity (16) + 1
        std::string output_file;

        desc.add_options()
            ("start-seed", po::value(&start_seed)->default_value(0),
             "Starting seed value")
            ("end-seed", po::value(&end_seed)->default_value(10000),
             "Ending seed value (inclusive)")
            ("salt", po::value(&salt)->default_value(1),
             "Hash salt value")
            ("bucket-count", po::value(&bucket_count)->default_value(100),
             "Number of hash buckets")
            ("key-size", po::value(&key_size)->default_value(32),
             "Key size in bytes")
            ("min-collisions", po::value(&min_collisions)->default_value(17),
             "Minimum keys in bucket to report (default 17 = forces spill)")
            ("output,o", po::value(&output_file),
             "Output JSON file path (optional)")
            ("help,h", "Show help message");

        po::variables_map vm;
        po::store(po::parse_command_line(argc, argv, desc), vm);
        po::notify(vm);

        if (vm.count("help"))
        {
            std::cout << "Usage: nudb-util find-collisions [options]\n\n";
            std::cout << "Find hash bucket collisions for testing spill records.\n\n";
            std::cout << desc << "\n";
            std::cout << "Example:\n";
            std::cout << "  nudb-util find-collisions --start-seed 0 --end-seed 100000 --bucket-count 100\n";
            return 0;
        }

        PLOGI(collisions_log, "\n=== Finding Hash Bucket Collisions ===");
        PLOGI(collisions_log, "Seed range: [", start_seed, ", ", end_seed, "]");
        PLOGI(collisions_log, "Bucket count: ", bucket_count);
        PLOGI(collisions_log, "Key size: ", key_size);
        PLOGI(collisions_log, "Min collisions to report: ", min_collisions);

        // Calculate modulus (next power of 2)
        auto modulus = nudbview::detail::ceil_pow2(bucket_count);

        PLOGI(collisions_log, "Modulus (power of 2): ", modulus);

        // Track which seeds map to which buckets
        std::map<nudbview::nbuck_t, std::vector<std::uint32_t>> bucket_to_seeds;

        // Scan the seed range
        std::uint64_t total_seeds = end_seed - start_seed + 1;
        std::uint64_t progress_interval = std::max<std::uint64_t>(1, total_seeds / 100);

        PLOGI(collisions_log, "Scanning ", total_seeds, " seeds...");

        for (std::uint32_t seed = start_seed; seed <= end_seed; ++seed)
        {
            // Generate key from seed
            auto key = generate_key(seed);

            // Hash the key
            nudbview::xxhasher hasher(salt);
            auto const h = hasher(key.data(), key_size);

            // Find bucket
            auto const bucket = nudbview::detail::bucket_index(h, bucket_count, modulus);

            // Track collision
            bucket_to_seeds[bucket].push_back(seed);

            // Progress reporting
            if ((seed - start_seed) % progress_interval == 0)
            {
                double pct = 100.0 * (seed - start_seed) / total_seeds;
                PLOGI(collisions_log, "Progress: ", pct, "% (", (seed - start_seed), "/", total_seeds, ")");
            }
        }

        PLOGI(collisions_log, "Scan complete!");
        PLOGI(collisions_log, "");

        // Report collision statistics
        std::size_t empty_buckets = 0;
        std::size_t total_collisions = 0;
        std::size_t max_collision_size = 0;

        for (nudbview::nbuck_t bucket = 0; bucket < bucket_count; ++bucket)
        {
            auto it = bucket_to_seeds.find(bucket);
            if (it == bucket_to_seeds.end())
            {
                ++empty_buckets;
            }
            else
            {
                std::size_t size = it->second.size();
                max_collision_size = std::max(max_collision_size, size);
                if (size > 1)
                {
                    ++total_collisions;
                }
            }
        }

        PLOGI(collisions_log, "=== Statistics ===");
        PLOGI(collisions_log, "Total buckets: ", bucket_count);
        PLOGI(collisions_log, "Empty buckets: ", empty_buckets);
        PLOGI(collisions_log, "Buckets with collisions: ", total_collisions);
        PLOGI(collisions_log, "Max keys in one bucket: ", max_collision_size);
        PLOGI(collisions_log, "");

        // Report buckets with >= min_collisions keys
        std::vector<std::pair<nudbview::nbuck_t, std::size_t>> high_collision_buckets;

        for (auto const& [bucket, seeds] : bucket_to_seeds)
        {
            if (seeds.size() >= min_collisions)
            {
                high_collision_buckets.emplace_back(bucket, seeds.size());
            }
        }

        if (high_collision_buckets.empty())
        {
            PLOGI(collisions_log, "No buckets with >= ", min_collisions, " keys found.");
            PLOGI(collisions_log, "Try increasing --end-seed or decreasing --bucket-count");
            return 0;
        }

        // Sort by collision count (descending)
        std::sort(high_collision_buckets.begin(), high_collision_buckets.end(),
                  [](auto const& a, auto const& b) { return a.second > b.second; });

        PLOGI(collisions_log, "=== Buckets with >= ", min_collisions, " keys (spill candidates) ===");
        PLOGI(collisions_log, "Found ", high_collision_buckets.size(), " buckets:");
        PLOGI(collisions_log, "");

        for (auto const& [bucket, count] : high_collision_buckets)
        {
            auto const& seeds = bucket_to_seeds[bucket];

            PLOGI(collisions_log, "Bucket ", bucket, ": ", count, " keys");

            // Print first 10 and last 10 seeds
            std::cout << "  Seeds: [";
            std::size_t to_show = std::min<std::size_t>(10, seeds.size());
            for (std::size_t i = 0; i < to_show; ++i)
            {
                if (i > 0) std::cout << ", ";
                std::cout << seeds[i];
            }

            if (seeds.size() > 20)
            {
                std::cout << ", ... (" << (seeds.size() - 20) << " more) ..., ";
                for (std::size_t i = seeds.size() - 10; i < seeds.size(); ++i)
                {
                    if (i > seeds.size() - 10) std::cout << ", ";
                    std::cout << seeds[i];
                }
            }
            else if (seeds.size() > 10)
            {
                for (std::size_t i = 10; i < seeds.size(); ++i)
                {
                    std::cout << ", " << seeds[i];
                }
            }

            std::cout << "]\n";
        }

        PLOGI(collisions_log, "");
        PLOGI(collisions_log, "TIP: Use these seeds to create a test database that will have spill records!");

        // Write JSON output if requested
        if (!output_file.empty())
        {
            std::ofstream ofs(output_file);
            if (!ofs)
            {
                PLOGE(collisions_log, "Failed to open output file: ", output_file);
                return 1;
            }

            ofs << "{\n";
            ofs << "  \"parameters\": {\n";
            ofs << "    \"start_seed\": " << start_seed << ",\n";
            ofs << "    \"end_seed\": " << end_seed << ",\n";
            ofs << "    \"salt\": " << salt << ",\n";
            ofs << "    \"bucket_count\": " << bucket_count << ",\n";
            ofs << "    \"key_size\": " << key_size << ",\n";
            ofs << "    \"min_collisions\": " << min_collisions << ",\n";
            ofs << "    \"modulus\": " << modulus << "\n";
            ofs << "  },\n";
            ofs << "  \"statistics\": {\n";
            ofs << "    \"total_seeds\": " << total_seeds << ",\n";
            ofs << "    \"empty_buckets\": " << empty_buckets << ",\n";
            ofs << "    \"buckets_with_collisions\": " << total_collisions << ",\n";
            ofs << "    \"max_keys_in_bucket\": " << max_collision_size << "\n";
            ofs << "  },\n";
            ofs << "  \"collision_buckets\": [\n";

            bool first_bucket = true;
            for (auto const& [bucket, count] : high_collision_buckets)
            {
                auto const& seeds = bucket_to_seeds[bucket];

                if (!first_bucket)
                    ofs << ",\n";
                first_bucket = false;

                ofs << "    {\n";
                ofs << "      \"bucket\": " << bucket << ",\n";
                ofs << "      \"count\": " << count << ",\n";
                ofs << "      \"seeds\": [";

                for (std::size_t i = 0; i < seeds.size(); ++i)
                {
                    if (i > 0) ofs << ", ";
                    ofs << seeds[i];
                }

                ofs << "]\n";
                ofs << "    }";
            }

            ofs << "\n  ]\n";
            ofs << "}\n";

            ofs.close();
            PLOGI(collisions_log, "Results written to: ", output_file);
        }

        return 0;
    }
    catch (std::exception const& e)
    {
        PLOGE(collisions_log, "Error: ", e.what());
        return 1;
    }
}

}  // namespace nudbutil
