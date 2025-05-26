/**
 * zstd-experiments.cpp
 *
 * Simple experiment to test custom ZSTD dictionary creation and compression.
 * Creates 1000 random samples, builds dictionary, then tests compression
 * on concatenated samples to prove dictionary reference concept.
 */

#define ZDICT_STATIC_LINKING_ONLY

#include <iostream>
#include <vector>
#include <random>
#include <iomanip>
#include <zstd.h>
#include <zdict.h>

int COMPRESSION_LEVEL = 3;

int main()
{
    std::cout << "ZSTD Custom Dictionary Experiment\n";
    std::cout << "=================================\n\n";
    
    // Random number generator
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> size_dist(32, 128);
    std::uniform_int_distribution<> byte_dist(0, 255);
    
    // Step 1: Create 1000 random samples
    std::cout << "Step 1: Creating 1000 random samples (32-128 bytes each)...\n";
    
    std::vector<std::vector<uint8_t>> samples;
    std::vector<size_t> sample_sizes;
    std::vector<uint8_t> training_buffer;
    
    samples.reserve(1000);
    sample_sizes.reserve(1000);
    
    size_t total_training_size = 0;
    for (int i = 0; i < 1000; i++)
    {
        size_t sample_size = size_dist(gen);
        std::vector<uint8_t> sample;
        sample.reserve(sample_size);
        
        for (size_t j = 0; j < sample_size; j++)
        {
            sample.push_back(static_cast<uint8_t>(byte_dist(gen)));
        }
        
        samples.push_back(sample);
        sample_sizes.push_back(sample_size);
        total_training_size += sample_size;
        
        // Add to training buffer
        training_buffer.insert(training_buffer.end(), sample.begin(), sample.end());
    }
    
    std::cout << "  Created " << samples.size() << " samples\n";
    std::cout << "  Total training data: " << total_training_size << " bytes\n";
    std::cout << "  Average sample size: " << (total_training_size / samples.size()) << " bytes\n\n";
    
    // Step 2: Build dictionary
    std::cout << "Step 2: Building ZSTD dictionary...\n";
    
    const size_t dict_size = 64 * 1024;  // 64KB dictionary
    std::vector<uint8_t> dict_buffer(dict_size + 1024 * 1024);  // Extra space for headers
    
    ZDICT_params_t dict_params = {};
    dict_params.compressionLevel = COMPRESSION_LEVEL;
    dict_params.notificationLevel = 1;
    dict_params.dictID = 0;
    
    size_t actual_dict_size = ZDICT_finalizeDictionary(
        dict_buffer.data(),
        dict_buffer.size(),
        training_buffer.data(),  // Use samples as dictionary content
        training_buffer.size(),
        training_buffer.data(),  // Also use as training samples
        sample_sizes.data(),
        samples.size(),
        dict_params);
        
    if (ZDICT_isError(actual_dict_size))
    {
        std::cerr << "Dictionary creation failed: " << ZDICT_getErrorName(actual_dict_size) << std::endl;
        return 1;
    }
    
    dict_buffer.resize(actual_dict_size);
    std::cout << "  Dictionary created: " << actual_dict_size << " bytes\n\n";
    
    // Step 3: Create test data (15 random samples concatenated)
    std::cout << "Step 3: Creating test data (15 random samples concatenated)...\n";
    
    std::uniform_int_distribution<> sample_dist(0, samples.size() - 1);
    std::vector<uint8_t> test_data;
    
    for (int i = 0; i < 15; i++)
    {
        int sample_idx = sample_dist(gen);
        const auto& sample = samples[sample_idx];
        test_data.insert(test_data.end(), sample.begin(), sample.end());
        std::cout << "  Sample " << (i+1) << ": " << sample.size() << " bytes (index " << sample_idx << ")\n";
    }
    
    std::cout << "  Total test data: " << test_data.size() << " bytes\n\n";
    
    // Step 4: Test compression with and without dictionary
    std::cout << "Step 4: Testing compression...\n";
    
    size_t comp_bound = ZSTD_compressBound(test_data.size());
    std::vector<char> compressed(comp_bound);
    
    // Compress without dictionary
    size_t comp_size_no_dict = ZSTD_compress(
        compressed.data(),
        comp_bound,
        test_data.data(),
        test_data.size(),
        COMPRESSION_LEVEL);
        
    if (ZSTD_isError(comp_size_no_dict))
    {
        std::cerr << "Compression failed: " << ZSTD_getErrorName(comp_size_no_dict) << std::endl;
        return 1;
    }
    
    // Compress with dictionary
    ZSTD_CDict* cdict = ZSTD_createCDict(dict_buffer.data(), actual_dict_size, COMPRESSION_LEVEL);
    if (!cdict)
    {
        std::cerr << "Failed to create compression dictionary\n";
        return 1;
    }
    
    ZSTD_CCtx* cctx = ZSTD_createCCtx();
    size_t comp_size_with_dict = ZSTD_compress_usingCDict(
        cctx,
        compressed.data(),
        comp_bound,
        test_data.data(),
        test_data.size(),
        cdict);
        
    if (ZSTD_isError(comp_size_with_dict))
    {
        std::cerr << "Dictionary compression failed: " << ZSTD_getErrorName(comp_size_with_dict) << std::endl;
        ZSTD_freeCCtx(cctx);
        ZSTD_freeCDict(cdict);
        return 1;
    }
    
    // Step 5: Show results
    std::cout << "\nRESULTS:\n";
    std::cout << "========\n";
    std::cout << "Original size:           " << test_data.size() << " bytes\n";
    std::cout << "Compressed (no dict):    " << comp_size_no_dict << " bytes\n";
    std::cout << "Compressed (with dict):  " << comp_size_with_dict << " bytes\n";
    std::cout << "Dictionary size:         " << actual_dict_size << " bytes\n\n";
    
    double ratio_no_dict = static_cast<double>(test_data.size()) / comp_size_no_dict;
    double ratio_with_dict = static_cast<double>(test_data.size()) / comp_size_with_dict;
    int overhead_no_dict = static_cast<int>(comp_size_no_dict) - static_cast<int>(test_data.size());
    int overhead_with_dict = static_cast<int>(comp_size_with_dict) - static_cast<int>(test_data.size());
    
    std::cout << "Compression ratio (no dict):   " << std::fixed << std::setprecision(2) << ratio_no_dict << "x\n";
    std::cout << "Compression ratio (with dict): " << std::fixed << std::setprecision(2) << ratio_with_dict << "x\n";
    std::cout << "Overhead (no dict):            " << std::showpos << overhead_no_dict << " bytes\n";
    std::cout << "Overhead (with dict):          " << std::showpos << overhead_with_dict << " bytes\n\n";
    
    if (comp_size_with_dict < comp_size_no_dict)
    {
        int savings = comp_size_no_dict - comp_size_with_dict;
        double improvement = static_cast<double>(savings) / comp_size_no_dict * 100.0;
        std::cout << "✅ Dictionary WINS by " << savings << " bytes (" << std::setprecision(1) << improvement << "% improvement)\n";
    }
    else
    {
        std::cout << "❌ Dictionary doesn't help (random data has no patterns)\n";
    }
    
    // Analysis
    std::cout << "\nANALYSIS:\n";
    std::cout << "=========\n";
    std::cout << "Expected: 9 bytes ZSTD overhead + 1-4 bytes per dictionary reference\n";
    std::cout << "Actual overhead with dict: " << overhead_with_dict << " bytes\n";
    
    if (overhead_with_dict <= 20)  // 9 + 3*4 = reasonable
    {
        std::cout << "✅ Overhead is reasonable for dictionary compression\n";
    }
    else
    {
        std::cout << "⚠️  Higher overhead than expected - random data may not reference dictionary well\n";
    }
    
    // Cleanup
    ZSTD_freeCCtx(cctx);
    ZSTD_freeCDict(cdict);
    
    return 0;
}
