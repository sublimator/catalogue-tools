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
#include <iomanip>
#include <zstd.h>
#include <zdict.h>
#include <chrono>

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
        "Dictionary size in bytes (default: 256KB)")(
        "max-samples,m",
        po::value<size_t>()->default_value(1000000),
        "Maximum number of samples to use (default: 500k)")(
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
        auto collect_samples = [&]([[maybe_unused]] const std::vector<uint8_t>& key,
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

        // Concatenate all samples into a single buffer for ZDICT
        std::vector<char> samples_buffer;
        samples_buffer.reserve(total_bytes);
        
        for (const auto& sample : samples)
        {
            samples_buffer.insert(
                samples_buffer.end(),
                sample.begin(),
                sample.end());
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
