/**
 * zstd-experiments.cpp
 *
 * Large-scale experiment to test custom ZSTD dictionary creation and compression.
 * Creates up to 1M random samples, builds dictionary, then tests compression
 * on concatenated samples to prove dictionary reference concept at XRP ledger scale.
 */

#include <iostream>
#include <vector>
#include <random>
#include <iomanip>
#include <chrono>
#include <zstd.h>
#include "catl/experiments/zstd-custom-dict.h"

using namespace catl::experiments;

// =============================================================================
// EXPERIMENT CONFIGURATION
// =============================================================================

// Sample generation
const int COMPRESSION_LEVEL = 3;
const size_t NUM_TRAINING_SAMPLES = 10'000;  // 1M samples (XRP ledger scale)
const size_t MIN_SAMPLE_SIZE = 32;             // Minimum sample size in bytes
const size_t MAX_SAMPLE_SIZE = 128;            // Maximum sample size in bytes

// Dictionary training
const size_t DICTIONARY_SIZE = 700 * 1024 * 1024;    // 256KB dictionary (more reasonable)

// Test data generation
const size_t NUM_TEST_SAMPLES = 50;           // Samples to concatenate for testing

// Progress reporting
const size_t PROGRESS_INTERVAL = 100000;      // Report progress every 100K samples

int main()
{
    std::cout << "ZSTD Large-Scale Dictionary Experiment\n";
    std::cout << "======================================\n";
    std::cout << "Training samples: " << NUM_TRAINING_SAMPLES << "\n";
    std::cout << "Sample size range: " << MIN_SAMPLE_SIZE << "-" << MAX_SAMPLE_SIZE << " bytes\n";
    std::cout << "Dictionary size: " << DICTIONARY_SIZE / 1024 << "KB\n";
    std::cout << "Test samples: " << NUM_TEST_SAMPLES << "\n\n";
    
    // Random number generator
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> size_dist(MIN_SAMPLE_SIZE, MAX_SAMPLE_SIZE);
    std::uniform_int_distribution<> byte_dist(0, 255);
    
    // Step 1: Create large number of random samples and train dictionary
    std::cout << "Step 1: Creating " << NUM_TRAINING_SAMPLES << " random samples (" 
              << MIN_SAMPLE_SIZE << "-" << MAX_SAMPLE_SIZE << " bytes each)...\n";
    
    std::vector<std::vector<uint8_t>> samples;
    ZstdDictTrainer trainer;
    
    samples.reserve(NUM_TRAINING_SAMPLES);
    trainer.reserve(NUM_TRAINING_SAMPLES * ((MIN_SAMPLE_SIZE + MAX_SAMPLE_SIZE) / 2));  // Rough estimate
    
    auto start_time = std::chrono::steady_clock::now();
    
    for (size_t i = 0; i < NUM_TRAINING_SAMPLES; i++)
    {
        if (i > 0 && i % PROGRESS_INTERVAL == 0)
        {
            auto current_time = std::chrono::steady_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(current_time - start_time).count();
            double progress = (double)i / NUM_TRAINING_SAMPLES * 100.0;
            std::cout << "  Progress: " << std::fixed << std::setprecision(1) << progress 
                      << "% (" << i << "/" << NUM_TRAINING_SAMPLES << ") - " 
                      << elapsed << "s elapsed\n";
        }
        
        size_t sample_size = size_dist(gen);
        std::vector<uint8_t> sample;
        sample.reserve(sample_size);
        
        for (size_t j = 0; j < sample_size; j++)
        {
            sample.push_back(static_cast<uint8_t>(byte_dist(gen)));
        }
        
        samples.push_back(sample);
        trainer.add_sample(sample);  // Add to trainer
    }
    
    auto sample_gen_time = std::chrono::steady_clock::now();
    auto sample_duration = std::chrono::duration_cast<std::chrono::seconds>(sample_gen_time - start_time).count();
    
    // Get training stats
    auto stats = trainer.get_stats();
    std::cout << "\n  ✅ Sample generation complete in " << sample_duration << " seconds\n";
    std::cout << "  Created " << stats.samples_count << " samples\n";
    std::cout << "  Total training data: " << std::fixed << std::setprecision(1) 
              << stats.total_samples_size / (1024.0 * 1024.0) << " MB\n";
    std::cout << "  Average sample size: " << std::fixed << std::setprecision(1) 
              << stats.avg_sample_size << " bytes\n\n";
    
    // Step 2: Build dictionary
    std::cout << "Step 2: Building ZSTD dictionary (" << DICTIONARY_SIZE / 1024 << "KB)...\n";
    auto dict_start_time = std::chrono::steady_clock::now();
    
    try {
        ZstdDict dict = trainer.train(TrainMode::FINALIZED, DICTIONARY_SIZE, COMPRESSION_LEVEL);
        
        auto dict_end_time = std::chrono::steady_clock::now();
        auto dict_duration = std::chrono::duration_cast<std::chrono::seconds>(dict_end_time - dict_start_time).count();
        
        std::cout << "  ✅ Dictionary created in " << dict_duration << " seconds\n";
        std::cout << "  Dictionary size: " << std::fixed << std::setprecision(1) 
                  << dict.size() / 1024.0 << " KB\n";
        std::cout << "  Compression level: " << dict.compression_level() << "\n";
        
        // Calculate training data ratio
        double data_to_dict_ratio = (double)stats.total_samples_size / dict.size();
        std::cout << "  Training data ratio: " << std::fixed << std::setprecision(1) 
                  << data_to_dict_ratio << "x dictionary size";
        if (data_to_dict_ratio < 10.0) {
            std::cout << " ⚠️  (recommend 10x+)";
        } else if (data_to_dict_ratio > 100.0) {
            std::cout << " ✅ (excellent)";
        } else {
            std::cout << " ✅ (good)";
        }
        std::cout << "\n\n";
    
        // Step 3: Create test data (concatenate random samples)
        std::cout << "Step 3: Creating test data (" << NUM_TEST_SAMPLES << " random samples concatenated)...\n";
        
        std::uniform_int_distribution<> sample_dist(0, samples.size() - 1);
        std::vector<uint8_t> test_data;
        
        size_t estimated_test_size = NUM_TEST_SAMPLES * ((MIN_SAMPLE_SIZE + MAX_SAMPLE_SIZE) / 2);
        test_data.reserve(estimated_test_size);
        
        for (size_t i = 0; i < NUM_TEST_SAMPLES; i++)
        {
            size_t sample_idx = sample_dist(gen);
            const auto& sample = samples[sample_idx];
            test_data.insert(test_data.end(), sample.begin(), sample.end());
            if (i < 10 || (i + 1) % 10 == 0)  // Show first 10, then every 10th
            {
                std::cout << "  Sample " << (i+1) << ": " << sample.size() 
                          << " bytes (index " << sample_idx << ")\n";
            }
        }
        
        std::cout << "  Total test data: " << std::fixed << std::setprecision(1) 
                  << test_data.size() / 1024.0 << " KB (" << test_data.size() << " bytes)\n\n";
    
        // Step 4: Test compression with and without dictionary
        std::cout << "Step 4: Testing compression...\n";
        
        // Compress with dictionary using clean API
        std::vector<uint8_t> compressed_with_dict;
        std::vector<uint8_t> compressed_no_dict;
        
        try {
            compressed_with_dict = dict.compress(test_data);
        } catch (const std::exception& e) {
            std::cerr << "Dictionary compression failed: " << e.what() << std::endl;
            return 1;
        }
    
        // Compress without dictionary (using raw ZSTD for comparison)
        size_t comp_bound = ZSTD_compressBound(test_data.size());
        std::vector<char> temp_compressed(comp_bound);
        
        size_t comp_size_no_dict = ZSTD_compress(
            temp_compressed.data(),
            comp_bound,
            test_data.data(),
            test_data.size(),
            COMPRESSION_LEVEL);
            
        if (ZSTD_isError(comp_size_no_dict))
        {
            std::cerr << "Standard compression failed: " << ZSTD_getErrorName(comp_size_no_dict) << std::endl;
            return 1;
        }
        
        compressed_no_dict.assign(temp_compressed.begin(), temp_compressed.begin() + comp_size_no_dict);
    
        // Step 5: Show results
        std::cout << "\nRESULTS:\n";
        std::cout << "========\n";
        std::cout << "Original size:           " << std::fixed << std::setprecision(1) 
                  << test_data.size() / 1024.0 << " KB (" << test_data.size() << " bytes)\n";
        std::cout << "Compressed (no dict):    " << std::fixed << std::setprecision(1) 
                  << compressed_no_dict.size() / 1024.0 << " KB (" << compressed_no_dict.size() << " bytes)\n";
        std::cout << "Compressed (with dict):  " << std::fixed << std::setprecision(1) 
                  << compressed_with_dict.size() / 1024.0 << " KB (" << compressed_with_dict.size() << " bytes)\n";
        std::cout << "Dictionary size:         " << std::fixed << std::setprecision(1) 
                  << dict.size() / 1024.0 << " KB (" << dict.size() << " bytes)\n\n";
        
        double ratio_no_dict = static_cast<double>(test_data.size()) / compressed_no_dict.size();
        double ratio_with_dict = static_cast<double>(test_data.size()) / compressed_with_dict.size();
        int overhead_no_dict = static_cast<int>(compressed_no_dict.size()) - static_cast<int>(test_data.size());
        int overhead_with_dict = static_cast<int>(compressed_with_dict.size()) - static_cast<int>(test_data.size());
        
        std::cout << "Compression ratio (no dict):   " << std::fixed << std::setprecision(2) << ratio_no_dict << "x\n";
        std::cout << "Compression ratio (with dict): " << std::fixed << std::setprecision(2) << ratio_with_dict << "x\n";
        std::cout << "Overhead (no dict):            " << std::showpos << overhead_no_dict << " bytes\n";
        std::cout << "Overhead (with dict):          " << std::showpos << overhead_with_dict << " bytes\n\n";
        
        if (compressed_with_dict.size() < compressed_no_dict.size())
        {
            int savings = compressed_no_dict.size() - compressed_with_dict.size();
            double improvement = static_cast<double>(savings) / compressed_no_dict.size() * 100.0;
            std::cout << "✅ Dictionary WINS by " << savings << " bytes (" << std::setprecision(1) << improvement << "% improvement)\n";
        }
        else
        {
            std::cout << "❌ Dictionary doesn't help (random data has no patterns)\n";
        }
        
        // Performance metrics
        auto total_time = std::chrono::steady_clock::now();
        auto total_duration = std::chrono::duration_cast<std::chrono::seconds>(total_time - start_time).count();
        std::cout << "\nPERFORMANCE:\n";
        std::cout << "============\n";
        std::cout << "Total experiment time: " << total_duration << " seconds\n";
        std::cout << "Samples per second: " << std::fixed << std::setprecision(0) 
                  << NUM_TRAINING_SAMPLES / (double)sample_duration << "\n";
        std::cout << "Training data rate: " << std::fixed << std::setprecision(1) 
                  << (stats.total_samples_size / (1024.0 * 1024.0)) / sample_duration << " MB/s\n";
        
        // Test decompression to verify correctness
        std::cout << "\nVERIFICATION:\n";
        std::cout << "=============\n";
        try {
            std::vector<uint8_t> decompressed = dict.decompress(compressed_with_dict);
            if (decompressed == test_data) {
                std::cout << "✅ Dictionary decompression successful - data matches perfectly\n";
            } else {
                std::cout << "❌ Dictionary decompression failed - data mismatch\n";
            }
        } catch (const std::exception& e) {
            std::cout << "❌ Dictionary decompression failed: " << e.what() << "\n";
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
    
        // Optional: Save dictionary for reuse
        std::cout << "\nSaving dictionary to 'large-scale-dict.zstd' for reuse...\n";
        try {
            dict.save("large-scale-dict.zstd");
            std::cout << "✅ Dictionary saved successfully (" 
                      << std::fixed << std::setprecision(1) 
                      << dict.size() / 1024.0 << " KB)\n";
        } catch (const std::exception& e) {
            std::cout << "⚠️  Failed to save dictionary: " << e.what() << "\n";
        }
        
        // Final summary
        std::cout << "\n" << std::string(60, '=') << "\n";
        std::cout << "EXPERIMENT COMPLETE\n";
        std::cout << "Training samples processed: " << NUM_TRAINING_SAMPLES << "\n";
        std::cout << "Dictionary size achieved: " << dict.size() / 1024.0 << " KB\n";
        std::cout << "Best compression ratio: " << std::fixed << std::setprecision(2) << ratio_with_dict << "x\n";
        std::cout << "Total experiment time: " << total_duration << " seconds\n";
        std::cout << std::string(60, '=') << "\n";
        
    } catch (const std::exception& e) {
        std::cerr << "Dictionary creation failed: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
