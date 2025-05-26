/**
 * catl1-to-zstd-dict.cpp
 *
 * Tool to create a zstd dictionary from catl file leaves.
 * Reads leaves from both account state and transaction maps across
 * multiple ledgers to build a comprehensive dictionary.
 */

#define ZDICT_STATIC_LINKING_ONLY

#include <boost/program_options.hpp>
#include <fstream>
#include <iostream>
#include <vector>
#include <set>
#include <map>
#include <iomanip>
#include <zstd.h>
#include <zdict.h>
#include <chrono>
#include <sstream>

#include "catl/core/log-macros.h"
#include "catl/v1/catl-v1-reader.h"


namespace po = boost::program_options;
using namespace catl::v1;

int COMPRESSION_LEVEL = 3;

/**
 * Process a catl file and create a zstd dictionary from its leaves
 */
int
main(int argc, char* argv[])
{
    po::options_description desc("Create zstd dictionary from catl file");
    desc.add_options()("help,h", "Show help message")(
        "input-catl-file,i",
        po::value<std::string>()->required(),
        "Input catl file path")(
        "output-dict-file,o",
        po::value<std::string>()->required(),
        "Output dictionary file path")(
        "dict-size,s",
        po::value<size_t>()->default_value(5 * 1024 * 1024),
        "Dictionary size in bytes (default: 5MB)")(
        "max-samples,m",
        po::value<size_t>()->default_value(1000000),
        "Maximum number of samples to use (default: 1M)")(
        "max-sample-size",
        po::value<size_t>()->default_value(10000),
        "Maximum sample size to include (default: 10KB)")(
        "max-ledgers,l",
        po::value<size_t>()->default_value(0),
        "Maximum number of ledgers to process (0 = all)")(
        "min-sample-size",
        po::value<size_t>()->default_value(50),
        "Minimum sample size to include (default: 50 bytes)")(
        "sample-txns",
        po::bool_switch()->default_value(true),
        "Also sample from transaction maps")(
        "test-bulk",
        po::bool_switch()->default_value(false),
        "Test bulk compression: concatenate all samples and compress as one block")(
        "test-keys",
        po::bool_switch()->default_value(false),
        "Test key compression: concatenate all keys and compress as one block")(
        "test-custom-key-dict",
        po::bool_switch()->default_value(false),
        "Test custom key dictionary: build dictionary from most frequent keys")(
        "verbose,v", "Enable verbose logging");

    po::variables_map vm;
    try
    {
        po::store(po::parse_command_line(argc, argv, desc), vm);

        if (vm.count("help"))
        {
            std::cout << desc << std::endl;
            return 0;
        }

        po::notify(vm);
    }
    catch (const po::error& e)
    {
        std::cerr << "Error: " << e.what() << "\n\n";
        std::cerr << desc << std::endl;
        return 1;
    }

    // Set up logging
    if (vm.count("verbose"))
    {
        Logger::set_level(LogLevel::INFO);
    }
    else
    {
        Logger::set_level(LogLevel::WARNING);
    }

    const std::string input_file = vm["input-catl-file"].as<std::string>();
    const std::string output_file = vm["output-dict-file"].as<std::string>();
    const size_t dict_size = vm["dict-size"].as<size_t>();
    const size_t max_samples = vm["max-samples"].as<size_t>();
    const size_t max_sample_size = vm["max-sample-size"].as<size_t>();
    const size_t max_ledgers = vm["max-ledgers"].as<size_t>();
    const size_t min_sample_size = vm["min-sample-size"].as<size_t>();
    const bool sample_txns = vm["sample-txns"].as<bool>();
    const bool test_bulk = vm["test-bulk"].as<bool>();
    const bool test_keys = vm["test-keys"].as<bool>();
    const bool test_custom_key_dict = vm["test-custom-key-dict"].as<bool>();

    try
    {
        LOGI("Reading catl file: ", input_file);
        Reader reader(input_file);
        auto header = reader.header();

        // Collect leaf samples
        std::vector<std::vector<uint8_t>> samples;
        std::vector<size_t> sample_sizes;
        samples.reserve(max_samples);
        sample_sizes.reserve(max_samples);

        // Collect keys separately for key compression test
        std::vector<std::vector<uint8_t>> keys;
        std::vector<size_t> key_sizes;
        std::map<std::vector<uint8_t>, size_t> key_counts;  // Use map instead of unordered_map
        if (test_keys) {
            keys.reserve(max_samples);
            key_sizes.reserve(max_samples);
        }

        // Statistics
        size_t total_leaves = 0;
        size_t total_txns = 0;
        size_t skipped_small = 0;
        size_t total_bytes = 0;
        size_t ledgers_processed = 0;
        std::set<size_t> unique_sizes;

        auto start_time = std::chrono::steady_clock::now();

        // Determine number of ledgers to process
        size_t ledgers_to_process = header.max_ledger - header.min_ledger + 1;
        if (max_ledgers > 0 && max_ledgers < ledgers_to_process)
        {
            ledgers_to_process = max_ledgers;
        }

        LOGI("Processing ", ledgers_to_process, " ledgers from ",
             header.min_ledger, " to ",
             header.min_ledger + ledgers_to_process - 1);

        // Lambda to collect samples from a map
        auto collect_samples = [&](const std::vector<uint8_t>& key,
                                 const std::vector<uint8_t>& data) {
            // Skip very small leaves
            if (data.size() < min_sample_size)
            {
                skipped_small++;
                return;
            }

            // Skip very large leaves (they can skew training)
            if (data.size() > max_sample_size)
            {
                return;
            }

            // Stop if we have enough samples
            if (samples.size() >= max_samples)
            {
                return;
            }

            // Add this leaf as a sample
            samples.push_back(data);
            sample_sizes.push_back(data.size());
            total_bytes += data.size();
            unique_sizes.insert(data.size());

            // Also collect keys if testing key compression or custom dictionary
            if (test_keys || test_custom_key_dict) {
                keys.push_back(key);
                key_sizes.push_back(key.size());
                key_counts[key]++;
            }
        };

        // Process each ledger
        for (size_t i = 0; i < ledgers_to_process && samples.size() < max_samples; ++i)
        {
            uint32_t ledger_seq = header.min_ledger + i;

            if (i % 100 == 0)  // Progress update every 100 ledgers
            {
                LOGI("Processing ledger ", ledger_seq, " (", i + 1, "/",
                     ledgers_to_process, ") - samples collected: ", samples.size());
            }

            // Read ledger info
            reader.read_ledger_info();

            // Read account state map and collect leaves
            auto state_result = reader.read_map_with_callbacks(
                catl::shamap::tnACCOUNT_STATE,
                collect_samples);

            total_leaves += state_result.nodes_added + state_result.nodes_updated;

            // Read transaction map if requested
            if (sample_txns && samples.size() < max_samples)
            {
                auto txn_result = reader.read_map_with_callbacks(
                    catl::shamap::tnTRANSACTION_MD,
                    collect_samples);

                total_txns += txn_result.nodes_added + txn_result.nodes_updated;
            }
            else if (!sample_txns)
            {
                reader.skip_map(catl::shamap::tnTRANSACTION_MD);
            }

            ledgers_processed++;
        }

        auto collection_time = std::chrono::steady_clock::now();
        auto collection_duration = std::chrono::duration_cast<std::chrono::seconds>(
            collection_time - start_time).count();

        LOGI("\n=== Collection Statistics ===");
        LOGI("Processed ", ledgers_processed, " ledgers in ", collection_duration, " seconds");
        LOGI("Collected ", samples.size(), " samples");
        LOGI("  From ", total_leaves, " state leaves");
        if (sample_txns)
        {
            LOGI("  From ", total_txns, " transaction leaves");
        }
        LOGI("Skipped ", skipped_small, " leaves smaller than ", min_sample_size, " bytes");
        LOGI("Total sample data: ", std::fixed, std::setprecision(1),
             total_bytes / (1024.0 * 1024.0), " MB");
        LOGI("Unique sample sizes: ", unique_sizes.size());
        LOGI("Average sample size: ", std::fixed, std::setprecision(1),
             samples.empty() ? 0 : total_bytes / (double)samples.size(), " bytes");

        if (samples.empty())
        {
            LOGE("No suitable samples found!");
            return 1;
        }

        // Concatenate all samples into a single buffer for ZDICT/testing (only if needed)
        std::vector<char> samples_buffer;
        if (test_bulk || (!test_keys && !test_custom_key_dict)) {
            samples_buffer.reserve(total_bytes);

            for (const auto& sample : samples)
            {
                samples_buffer.insert(
                    samples_buffer.end(),
                    sample.begin(),
                    sample.end());
            }
        }

        // TEST BULK COMPRESSION MODE
        if (test_bulk)
        {
            LOGI("\n=== BULK COMPRESSION TEST ===");
            LOGI("Testing compression of all samples concatenated as one block...");

            auto bulk_start = std::chrono::steady_clock::now();

            // Compress the entire concatenated buffer as one block
            size_t comp_bound = ZSTD_compressBound(samples_buffer.size());
            std::vector<char> compressed(comp_bound);

            size_t compressed_size = ZSTD_compress(
                compressed.data(),
                comp_bound,
                samples_buffer.data(),
                samples_buffer.size(),
                COMPRESSION_LEVEL);

            auto bulk_end = std::chrono::steady_clock::now();
            auto bulk_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                bulk_end - bulk_start).count();

            if (!ZSTD_isError(compressed_size))
            {
                double compression_ratio = static_cast<double>(samples_buffer.size()) / compressed_size;

                LOGI("BULK COMPRESSION RESULTS:");
                LOGI("  Original size: ", samples_buffer.size() / (1024.0 * 1024.0), " MB");
                LOGI("  Compressed size: ", compressed_size / (1024.0 * 1024.0), " MB");
                LOGI("  Compression ratio: ", std::fixed, std::setprecision(2), compression_ratio, "x");
                LOGI("  Space saved: ", std::fixed, std::setprecision(1),
                     100.0 * (1.0 - (double)compressed_size / samples_buffer.size()), "%");
                LOGI("  Compression time: ", bulk_duration, " ms");

                LOGI("\n🔥 THIS IS WHAT WHOLE-FILE COMPRESSION ACHIEVES! 🔥");
                LOGI("This is the compression ratio we need to beat with per-leaf + dictionary!");
            }
            else
            {
                LOGE("Bulk compression failed: ", ZSTD_getErrorName(compressed_size));
            }

            LOGI("\nSkipping dictionary training in test-bulk mode.");
            return 0;
        }

        // TEST KEY COMPRESSION MODE
        if (test_keys)
        {
            LOGI("\n=== KEY COMPRESSION TEST ===");
            LOGI("Testing compression of all keys concatenated as one block...");

            // Concatenate all keys into one buffer
            std::vector<char> keys_buffer;
            size_t total_key_bytes = 0;

            for (const auto& key : keys) {
                total_key_bytes += key.size();
            }
            keys_buffer.reserve(total_key_bytes);

            for (const auto& key : keys) {
                keys_buffer.insert(keys_buffer.end(), key.begin(), key.end());
            }

            auto key_start = std::chrono::steady_clock::now();

            // Compress the entire concatenated key buffer as one block
            size_t comp_bound = ZSTD_compressBound(keys_buffer.size());
            std::vector<char> compressed(comp_bound);

            size_t compressed_size = ZSTD_compress(
                compressed.data(),
                comp_bound,
                keys_buffer.data(),
                keys_buffer.size(),
                COMPRESSION_LEVEL);

            auto key_end = std::chrono::steady_clock::now();
            auto key_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                key_end - key_start).count();

            if (!ZSTD_isError(compressed_size))
            {
                double compression_ratio = static_cast<double>(keys_buffer.size()) / compressed_size;

                LOGI("KEY COMPRESSION RESULTS:");
                LOGI("  Keys collected: ", keys.size());
                LOGI("  Original key size: ", keys_buffer.size() / (1024.0 * 1024.0), " MB");
                LOGI("  Compressed key size: ", compressed_size / (1024.0 * 1024.0), " MB");
                LOGI("  Key compression ratio: ", std::fixed, std::setprecision(2), compression_ratio, "x");
                LOGI("  Key space saved: ", std::fixed, std::setprecision(1),
                     100.0 * (1.0 - (double)compressed_size / keys_buffer.size()), "%");
                LOGI("  Key compression time: ", key_duration, " ms");
                LOGI("  Average key size: ", std::fixed, std::setprecision(1),
                     keys.empty() ? 0 : total_key_bytes / (double)keys.size(), " bytes");

                // Analyze key repetition patterns
                LOGI("\n=== KEY REPETITION ANALYSIS ===");
                LOGI("  Unique keys: ", key_counts.size());
                LOGI("  Total keys: ", keys.size());
                LOGI("  Avg repetitions per unique key: ", std::fixed, std::setprecision(2),
                     key_counts.empty() ? 0 : (double)keys.size() / key_counts.size());

                // Calculate theoretical maximum compression
                size_t unique_keys = key_counts.size();
                size_t total_keys = keys.size();
                size_t raw_key_bytes = total_keys * 32;  // All keys are 32 bytes
                size_t unique_key_bytes = unique_keys * 32;  // Storage for unique keys

                // Calculate different encoding schemes
                size_t bytes_per_reference = 1;  // Start with 1 byte
                if (unique_keys > 255) bytes_per_reference = 2;
                if (unique_keys > 65535) bytes_per_reference = 4;

                size_t reference_bytes = total_keys * bytes_per_reference;
                size_t total_with_dict = unique_key_bytes + reference_bytes;

                double theoretical_compression = (double)raw_key_bytes / total_with_dict;

                LOGI("\n=== THEORETICAL KEY COMPRESSION ===");
                LOGI("  Raw key storage: ", raw_key_bytes / (1024.0 * 1024.0), " MB");
                LOGI("  Unique key storage: ", unique_key_bytes / (1024.0 * 1024.0), " MB");
                LOGI("  Reference storage (", bytes_per_reference, " bytes each): ",
                     reference_bytes / (1024.0 * 1024.0), " MB");
                LOGI("  Total with dictionary: ", total_with_dict / (1024.0 * 1024.0), " MB");
                LOGI("  Theoretical compression ratio: ", std::fixed, std::setprecision(2),
                     theoretical_compression, "x");
                LOGI("  Theoretical space saved: ", std::fixed, std::setprecision(1),
                     100.0 * (1.0 - (double)total_with_dict / raw_key_bytes), "%");

                // Show breakdown of compression sources
                double dict_overhead_ratio = (double)unique_key_bytes / raw_key_bytes;
                double reference_ratio = (double)reference_bytes / raw_key_bytes;

                LOGI("\n=== COMPRESSION BREAKDOWN ===");
                LOGI("  Dictionary overhead: ", std::fixed, std::setprecision(1),
                     dict_overhead_ratio * 100, "% of original");
                LOGI("  Reference storage: ", std::fixed, std::setprecision(1),
                     reference_ratio * 100, "% of original");
                LOGI("  Net savings: ", std::fixed, std::setprecision(1),
                     (1.0 - dict_overhead_ratio - reference_ratio) * 100, "% of original");

                // Compare to actual ZSTD compression
                LOGI("\n=== VS ACTUAL ZSTD COMPRESSION ===");
                LOGI("  ZSTD achieved: ", std::fixed, std::setprecision(2), compression_ratio, "x");
                LOGI("  Theoretical max: ", std::fixed, std::setprecision(2), theoretical_compression, "x");
                LOGI("  ZSTD efficiency: ", std::fixed, std::setprecision(1),
                     100.0 * compression_ratio / theoretical_compression, "% of theoretical max");

                if (theoretical_compression > compression_ratio) {
                    LOGI("  🚀 ZSTD is leaving ", std::fixed, std::setprecision(2),
                         theoretical_compression - compression_ratio, "x compression on the table!");
                } else {
                    LOGI("  🔥 ZSTD is doing better than naive dictionary approach!");
                }

                // Sort keys by frequency
                std::vector<std::pair<std::vector<uint8_t>, size_t>> sorted_keys;
                for (const auto& [key, count] : key_counts) {
                    sorted_keys.emplace_back(key, count);
                }
                std::sort(sorted_keys.begin(), sorted_keys.end(),
                         [](const auto& a, const auto& b) { return a.second > b.second; });

                // Print top 100 most frequent keys
                LOGI("\n=== TOP 100 MOST REPEATED KEYS ===");
                size_t print_count = std::min(size_t(100), sorted_keys.size());
                for (size_t i = 0; i < print_count; ++i) {
                    const auto& [key, count] = sorted_keys[i];

                    // Convert key to hex string
                    std::stringstream hex_stream;
                    hex_stream << std::hex << std::uppercase << std::setfill('0');
                    for (uint8_t byte : key) {
                        hex_stream << std::setw(2) << static_cast<unsigned>(byte);
                    }

                    LOGI("  #", std::setw(3), i + 1, ": Count=", std::setw(8), count,
                         " Key=", hex_stream.str());
                }

                LOGI("\n🔑 THIS IS HOW MUCH THE KEYS CONTRIBUTE TO COMPRESSION! 🔑");
                LOGI("Keys contain repeated SHAMap paths and prefixes!");
            }
            else
            {
                LOGE("Key compression failed: ", ZSTD_getErrorName(compressed_size));
            }

            LOGI("\nSkipping dictionary training in test-keys mode.");
            return 0;
        }

        // TEST CUSTOM KEY DICTIONARY MODE
        if (test_custom_key_dict)
        {
            LOGI("\n=== CUSTOM KEY DICTIONARY TEST ===");
            LOGI("Building custom dictionary from most frequent keys...");
            LOGI("NOTE: Testing compression on 32KB blocks (1000 keys each), not individual keys");

            // Sort keys by frequency (we should already have this from key test)
            std::vector<std::pair<std::vector<uint8_t>, size_t>> sorted_keys;
            for (const auto& [key, count] : key_counts) {
                sorted_keys.emplace_back(key, count);
            }
            std::sort(sorted_keys.begin(), sorted_keys.end(),
                     [](const auto& a, const auto& b) { return a.second > b.second; });

            // Build custom dictionary from top keys
            const size_t max_dict_entries = 10000;  // Top 10K keys
            const size_t max_dict_size = 1024 * 1024;  // 1MB max
            std::vector<uint8_t> custom_dict;
            custom_dict.reserve(max_dict_size);

            size_t keys_added = 0;
            size_t total_frequency = 0;

            for (const auto& [key, freq] : sorted_keys) {
                if (keys_added >= max_dict_entries ||
                    custom_dict.size() + key.size() > max_dict_size) {
                    break;
                }
                custom_dict.insert(custom_dict.end(), key.begin(), key.end());
                keys_added++;
                total_frequency += freq;
            }

            LOGI("Custom dictionary built:");
            LOGI("  Keys in dictionary: ", keys_added);
            LOGI("  Dictionary size: ", custom_dict.size() / (1024.0 * 1024.0), " MB");
            LOGI("  Total frequency covered: ", total_frequency, "/", keys.size(),
                 " (", 100.0 * total_frequency / keys.size(), "%)");

            // Prepare training samples (concatenated buffer for ZDICT analysis)
            size_t training_count = std::min(size_t(10000), keys.size());
            std::vector<size_t> training_sizes;
            std::vector<uint8_t> training_buffer;

            training_sizes.reserve(training_count);
            size_t total_training_size = training_count * 32;  // All keys are 32 bytes
            training_buffer.reserve(total_training_size);

            // Concatenate training samples into single buffer
            for (size_t i = 0; i < training_count; i++) {
                training_buffer.insert(training_buffer.end(), keys[i].begin(), keys[i].end());
                training_sizes.push_back(keys[i].size());
            }

            // Create proper ZSTD dictionary using ZDICT_finalizeDictionary
            std::vector<uint8_t> final_dict(custom_dict.size() + 1024 * 1024);  // Extra space for headers

            ZDICT_params_t dict_params = {};  // Zero-initialize for defaults
            dict_params.compressionLevel = COMPRESSION_LEVEL;
            dict_params.notificationLevel = 1;  // Show errors
            dict_params.dictID = 0;  // Auto-generate ID

            size_t actual_dict_size = ZDICT_finalizeDictionary(
                final_dict.data(),
                final_dict.size(),
                custom_dict.data(),      // Our custom content (frequent keys)
                custom_dict.size(),
                training_buffer.data(),  // Concatenated training samples
                training_sizes.data(),   // Array of sample sizes
                training_count,
                dict_params);

            if (ZDICT_isError(actual_dict_size)) {
                LOGE("Failed to create proper ZSTD dictionary: ", ZDICT_getErrorName(actual_dict_size));
                return 1;
            }

            final_dict.resize(actual_dict_size);

            LOGI("Proper ZSTD dictionary created:");
            LOGI("  Raw content size: ", custom_dict.size() / (1024.0 * 1024.0), " MB");
            LOGI("  Final dictionary size: ", actual_dict_size / (1024.0 * 1024.0), " MB");

            // Create ZSTD custom dictionary from properly formatted dict
            ZSTD_CDict* custom_cdict = ZSTD_createCDict(
                final_dict.data(), actual_dict_size, COMPRESSION_LEVEL);

            if (!custom_cdict) {
                LOGE("Failed to create custom ZSTD dictionary");
                return 1;
            }

            // Test compression on blocks of keys (not individual keys!)
            ZSTD_CCtx* cctx = ZSTD_createCCtx();
            const size_t keys_per_block = 1000;  // 32KB blocks
            const size_t bytes_per_block = keys_per_block * 32;
            size_t max_blocks = std::min(size_t(100), keys.size() / keys_per_block);

            size_t total_original = 0;
            size_t total_no_dict = 0;
            size_t total_custom_dict = 0;
            size_t custom_wins = 0;

            auto test_start = std::chrono::steady_clock::now();

            for (size_t block = 0; block < max_blocks; block++) {
                // Create a block of 1000 keys (32KB)
                std::vector<uint8_t> key_block;
                key_block.reserve(bytes_per_block);

                for (size_t i = 0; i < keys_per_block; i++) {
                    size_t key_idx = block * keys_per_block + i;
                    if (key_idx >= keys.size()) break;

                    const auto& key = keys[key_idx];
                    key_block.insert(key_block.end(), key.begin(), key.end());
                }

                if (key_block.empty()) break;

                size_t comp_bound = ZSTD_compressBound(key_block.size());
                std::vector<char> compressed(comp_bound);

                // Compress without dictionary
                size_t comp_size_no_dict = ZSTD_compress(
                    compressed.data(), comp_bound,
                    key_block.data(), key_block.size(), COMPRESSION_LEVEL);

                // Compress with custom dictionary
                size_t comp_size_custom_dict = ZSTD_compress_usingCDict(
                    cctx, compressed.data(), comp_bound,
                    key_block.data(), key_block.size(), custom_cdict);

                if (!ZSTD_isError(comp_size_no_dict) && !ZSTD_isError(comp_size_custom_dict)) {
                    total_original += key_block.size();
                    total_no_dict += comp_size_no_dict;
                    total_custom_dict += comp_size_custom_dict;

                    if (comp_size_custom_dict < comp_size_no_dict) {
                        custom_wins++;
                    }
                }
            }

            auto test_end = std::chrono::steady_clock::now();
            auto test_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                test_end - test_start).count();

            // Calculate compression ratios
            double no_dict_ratio = (double)total_original / total_no_dict;
            double custom_dict_ratio = (double)total_original / total_custom_dict;

            LOGI("\n=== CUSTOM DICTIONARY RESULTS ===");
            LOGI("Tested on ", max_blocks, " key blocks (", keys_per_block, " keys each, ",
                 bytes_per_block / 1024, "KB blocks):");
            LOGI("  Original size: ", total_original / (1024.0 * 1024.0), " MB");
            LOGI("  No dictionary compression: ", total_no_dict / (1024.0 * 1024.0),
                 " MB (ratio: ", std::fixed, std::setprecision(2), no_dict_ratio, "x)");
            LOGI("  Custom dictionary compression: ", total_custom_dict / (1024.0 * 1024.0),
                 " MB (ratio: ", std::fixed, std::setprecision(2), custom_dict_ratio, "x)");
            LOGI("  Custom dictionary wins: ", custom_wins, "/", max_blocks,
                 " (", 100.0 * custom_wins / max_blocks, "%)");
            LOGI("  Improvement vs no-dict: ", std::fixed, std::setprecision(2),
                 custom_dict_ratio / no_dict_ratio, "x better");
            LOGI("  Test duration: ", test_duration, " ms");

            // Compare to theoretical maximum (for blocks, not individual keys)
            size_t total_keys_tested = max_blocks * keys_per_block;
            std::set<std::vector<uint8_t>> unique_keys_in_test;
            for (size_t i = 0; i < total_keys_tested && i < keys.size(); i++) {
                unique_keys_in_test.insert(keys[i]);
            }
            size_t unique_keys_tested = unique_keys_in_test.size();

            // Theoretical: unique keys storage + references for all keys in test
            size_t theoretical_dict_storage = unique_keys_tested * 32;
            size_t theoretical_ref_storage = total_keys_tested * 4;  // 4 bytes per reference
            size_t theoretical_total = theoretical_dict_storage + theoretical_ref_storage;
            double theoretical_ratio = (double)total_original / theoretical_total;

            LOGI("\n=== VS THEORETICAL OPTIMUM ===");
            LOGI("  Theoretical optimum: ", std::fixed, std::setprecision(2), theoretical_ratio, "x");
            LOGI("  Custom dictionary achieved: ", std::fixed, std::setprecision(2), custom_dict_ratio, "x");
            LOGI("  Efficiency: ", std::fixed, std::setprecision(1),
                 100.0 * custom_dict_ratio / theoretical_ratio, "% of theoretical maximum");

            if (custom_dict_ratio > no_dict_ratio) {
                LOGI("\n🚀 CUSTOM DICTIONARY BEATS NO-DICT BY ",
                     std::fixed, std::setprecision(2),
                     custom_dict_ratio / no_dict_ratio, "x! 🚀");
            }

            if (custom_dict_ratio > 1.18) {  // Beat ZDICT's key compression
                LOGI("🎯 CUSTOM DICTIONARY BEATS ZDICT (1.18x) BY ",
                     std::fixed, std::setprecision(2),
                     custom_dict_ratio / 1.18, "x! 🎯");
            }

            ZSTD_freeCCtx(cctx);
            ZSTD_freeCDict(custom_cdict);

            LOGI("\nSkipping standard dictionary training in custom-key-dict mode.");
            return 0;
        }

        // Check if we have enough training data
        double ratio = static_cast<double>(total_bytes) / dict_size;
        if (ratio < 10.0)
        {
            LOGW("\n=== WARNING ===");
            LOGW("Training data size (", total_bytes / (1024.0 * 1024.0),
                 " MB) is only ", ratio, "x the dictionary size!");
            LOGW("Recommended: at least 10x (ideally 100x) the dictionary size");
            LOGW("For a ", dict_size / (1024.0 * 1024.0), " MB dictionary:");
            LOGW("  Minimum: ", dict_size * 10 / (1024.0 * 1024.0), " MB of training data");
            LOGW("  Ideal: ", dict_size * 100 / (1024.0 * 1024.0), " MB of training data");
            LOGW("\nSuggestions:");
            LOGW("  1. Increase --max-samples");
            LOGW("  2. Decrease --dict-size");
            LOGW("  3. Process more ledgers");
            LOGW("\nProceeding anyway...\n");
        }

        // Train dictionary
        LOGI("\n=== Dictionary Training ===");
        LOGI("Training dictionary with size ", dict_size / (1024.0 * 1024.0), " MB...");
        std::vector<char> dict_buffer(dict_size);

        auto train_start = std::chrono::steady_clock::now();

        ZDICT_cover_params_t cover_params = {};
        cover_params.k = 2048;                    // Segment size
        cover_params.d = 8;                       // Dmer size
        cover_params.steps = 4;                   // Optimization steps
        cover_params.zParams.compressionLevel = COMPRESSION_LEVEL;

        size_t actual_dict_size = ZDICT_trainFromBuffer_cover(
            dict_buffer.data(),
            dict_size,
            samples_buffer.data(),
            sample_sizes.data(),
            static_cast<unsigned>(samples.size()), cover_params);

        auto train_end = std::chrono::steady_clock::now();
        auto train_duration = std::chrono::duration_cast<std::chrono::seconds>(
            train_end - train_start).count();

        if (ZDICT_isError(actual_dict_size))
        {
            LOGE(
                "Dictionary training failed: ",
                ZDICT_getErrorName(actual_dict_size));
            return 1;
        }

        dict_buffer.resize(actual_dict_size);
        LOGI("Dictionary trained successfully in ", train_duration, " seconds!");
        LOGI("Actual dictionary size: ", actual_dict_size / (1024.0 * 1024.0), " MB");

        // Save dictionary to file
        std::ofstream out_file(output_file, std::ios::binary);
        if (!out_file)
        {
            LOGE("Failed to open output file: ", output_file);
            return 1;
        }

        out_file.write(dict_buffer.data(), dict_buffer.size());
        out_file.close();

        LOGI("Dictionary saved to: ", output_file);

        // Test compression with dictionary to show effectiveness
        if (vm.count("verbose") && !samples.empty())
        {
            // Test on first few samples
            ZSTD_CDict* cdict = ZSTD_createCDict(
                dict_buffer.data(), dict_buffer.size(), COMPRESSION_LEVEL);

            if (cdict)
            {
                ZSTD_CCtx* cctx = ZSTD_createCCtx();
                size_t test_count = std::min(size_t(10000), samples.size());
                size_t total_orig = 0;
                size_t total_comp_with_dict = 0;
                size_t total_comp_without_dict = 0;
                size_t dict_wins = 0;
                double max_improvement = 0;
                double total_improvement = 0;

                for (size_t i = 0; i < test_count; i++)
                {
                    const auto& sample = samples[i];
                    size_t comp_bound = ZSTD_compressBound(sample.size());
                    std::vector<char> compressed(comp_bound);

                    // Compress with dictionary
                    size_t comp_size_dict = ZSTD_compress_usingCDict(
                        cctx,
                        compressed.data(),
                        comp_bound,
                        sample.data(),
                        sample.size(),
                        cdict);

                    // Compress without dictionary
                    size_t comp_size_no_dict = ZSTD_compress(
                        compressed.data(),
                        comp_bound,
                        sample.data(),
                        sample.size(),
                        COMPRESSION_LEVEL);

                    if (!ZSTD_isError(comp_size_dict) && !ZSTD_isError(comp_size_no_dict))
                    {
                        total_orig += sample.size();
                        total_comp_with_dict += comp_size_dict;
                        total_comp_without_dict += comp_size_no_dict;

                        if (comp_size_dict < comp_size_no_dict)
                        {
                            dict_wins++;
                            double improvement = 100.0 * (1.0 - (double)comp_size_dict / comp_size_no_dict);
                            max_improvement = std::max(max_improvement, improvement);
                            total_improvement += improvement;
                        }
                    }
                }

                LOGI("\n=== Compression Test Results ===");
                LOGI("Tested on ", test_count, " samples:");
                LOGI("  Original size: ", total_orig / (1024.0 * 1024.0), " MB");
                LOGI("  Compressed (no dict): ", total_comp_without_dict / (1024.0 * 1024.0),
                     " MB (ratio: ",
                     static_cast<double>(total_orig) / total_comp_without_dict, "x)");
                LOGI("  Compressed (with dict): ", total_comp_with_dict / (1024.0 * 1024.0),
                     " MB (ratio: ",
                     static_cast<double>(total_orig) / total_comp_with_dict, "x)");
                LOGI("\nDictionary effectiveness:");
                LOGI("  Dictionary helped in ", dict_wins, "/", test_count,
                     " samples (", 100.0 * dict_wins / test_count, "%)");
                LOGI("  Average improvement when dict helps: ",
                     dict_wins > 0 ? total_improvement / dict_wins : 0, "%");
                LOGI("  Maximum improvement: ", max_improvement, "%");
                LOGI("  Overall size reduction vs no-dict: ",
                     100.0 * (1.0 - (double)total_comp_with_dict / total_comp_without_dict), "%");

                ZSTD_freeCCtx(cctx);
                ZSTD_freeCDict(cdict);
            }
        }

        auto total_time = std::chrono::steady_clock::now();
        auto total_duration = std::chrono::duration_cast<std::chrono::seconds>(
            total_time - start_time).count();

        LOGI("\nTotal execution time: ", total_duration, " seconds");

        return 0;
    }
    catch (const std::exception& e)
    {
        LOGE("Fatal error: ", e.what());
        return 1;
    }
}