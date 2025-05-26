/**
 * zstd-custom-dict.h
 * 
 * Clean separation: ZstdDict (immutable compress/decompress) + ZstdDictTrainer (builds dicts)
 */

#pragma once

#define ZDICT_STATIC_LINKING_ONLY

#include <vector>
#include <string>
#include <stdexcept>
#include <cstdint>
#include <fstream>
#include <zstd.h>
#include <zdict.h>

namespace catl::experiments {

/**
 * Immutable ZSTD dictionary for compression/decompression
 */
class ZstdDict {
public:
    // Factory methods
    static ZstdDict from_file(const std::string& filename) {
        std::ifstream file(filename, std::ios::binary);
        if (!file) {
            throw std::runtime_error("Cannot open dictionary file: " + filename);
        }
        
        // Read compression level (first 4 bytes)
        int compression_level;
        file.read(reinterpret_cast<char*>(&compression_level), sizeof(compression_level));
        if (!file) {
            throw std::runtime_error("Invalid dictionary file format");
        }
        
        // Read dictionary data
        std::vector<uint8_t> dict_data;
        file.seekg(0, std::ios::end);
        size_t file_size = file.tellg();
        file.seekg(sizeof(compression_level), std::ios::beg);
        
        size_t dict_size = file_size - sizeof(compression_level);
        dict_data.resize(dict_size);
        file.read(reinterpret_cast<char*>(dict_data.data()), dict_size);
        
        if (!file) {
            throw std::runtime_error("Failed to read dictionary data");
        }
        
        return ZstdDict(std::move(dict_data), compression_level);
    }
    
    static ZstdDict from_data(const std::vector<uint8_t>& dict_data, int compression_level = 3) {
        return ZstdDict(dict_data, compression_level);
    }
    
    // Core operations
    std::vector<uint8_t> compress(const std::vector<uint8_t>& data) const {
        if (data.empty()) {
            return {};
        }
        
        size_t bound = ZSTD_compressBound(data.size());
        std::vector<uint8_t> compressed(bound);
        
        size_t compressed_size = ZSTD_compress_usingCDict(
            cctx_,
            compressed.data(),
            bound,
            data.data(),
            data.size(),
            cdict_);
            
        if (ZSTD_isError(compressed_size)) {
            throw std::runtime_error("Compression failed: " + 
                                   std::string(ZSTD_getErrorName(compressed_size)));
        }
        
        compressed.resize(compressed_size);
        return compressed;
    }
    
    std::vector<uint8_t> decompress(const std::vector<uint8_t>& compressed) const {
        if (compressed.empty()) {
            return {};
        }
        
        // Get decompressed size
        unsigned long long decompressed_size = ZSTD_getFrameContentSize(compressed.data(), compressed.size());
        if (decompressed_size == ZSTD_CONTENTSIZE_ERROR) {
            throw std::runtime_error("Invalid compressed data");
        }
        if (decompressed_size == ZSTD_CONTENTSIZE_UNKNOWN) {
            throw std::runtime_error("Cannot determine decompressed size");
        }
        
        std::vector<uint8_t> decompressed(decompressed_size);
        
        size_t result = ZSTD_decompress_usingDDict(
            dctx_,
            decompressed.data(),
            decompressed.size(),
            compressed.data(),
            compressed.size(),
            ddict_);
            
        if (ZSTD_isError(result)) {
            throw std::runtime_error("Decompression failed: " + 
                                   std::string(ZSTD_getErrorName(result)));
        }
        
        return decompressed;
    }
    
    // Persistence
    void save(const std::string& filename) const {
        std::ofstream file(filename, std::ios::binary);
        if (!file) {
            throw std::runtime_error("Cannot create dictionary file: " + filename);
        }
        
        // Write compression level first
        file.write(reinterpret_cast<const char*>(&compression_level_), sizeof(compression_level_));
        
        // Write dictionary data
        file.write(reinterpret_cast<const char*>(dict_data_.data()), dict_data_.size());
        
        if (!file) {
            throw std::runtime_error("Failed to write dictionary file");
        }
    }
    
    // Info
    size_t size() const { 
        return dict_data_.size(); 
    }
    
    int compression_level() const { 
        return compression_level_; 
    }
    
    // RAII, move-only
    ~ZstdDict() {
        cleanup();
    }
    
    ZstdDict(const ZstdDict&) = delete;
    ZstdDict& operator=(const ZstdDict&) = delete;
    
    ZstdDict(ZstdDict&& other) noexcept 
        : dict_data_(std::move(other.dict_data_))
        , cdict_(other.cdict_)
        , ddict_(other.ddict_)
        , cctx_(other.cctx_)
        , dctx_(other.dctx_)
        , compression_level_(other.compression_level_)
    {
        other.cdict_ = nullptr;
        other.ddict_ = nullptr;
        other.cctx_ = nullptr;
        other.dctx_ = nullptr;
    }
    
    ZstdDict& operator=(ZstdDict&& other) noexcept {
        if (this != &other) {
            cleanup();
            dict_data_ = std::move(other.dict_data_);
            cdict_ = other.cdict_;
            ddict_ = other.ddict_;
            cctx_ = other.cctx_;
            dctx_ = other.dctx_;
            compression_level_ = other.compression_level_;
            
            other.cdict_ = nullptr;
            other.ddict_ = nullptr;
            other.cctx_ = nullptr;
            other.dctx_ = nullptr;
        }
        return *this;
    }

private:
    ZstdDict(std::vector<uint8_t> dict_data, int compression_level)
        : dict_data_(std::move(dict_data))
        , compression_level_(compression_level)
    {
        // Create compression dictionary
        cdict_ = ZSTD_createCDict(dict_data_.data(), dict_data_.size(), compression_level);
        if (!cdict_) {
            throw std::runtime_error("Failed to create compression dictionary");
        }
        
        // Create decompression dictionary
        ddict_ = ZSTD_createDDict(dict_data_.data(), dict_data_.size());
        if (!ddict_) {
            ZSTD_freeCDict(cdict_);
            throw std::runtime_error("Failed to create decompression dictionary");
        }
        
        // Create contexts
        cctx_ = ZSTD_createCCtx();
        if (!cctx_) {
            ZSTD_freeCDict(cdict_);
            ZSTD_freeDDict(ddict_);
            throw std::runtime_error("Failed to create compression context");
        }
        
        dctx_ = ZSTD_createDCtx();
        if (!dctx_) {
            ZSTD_freeCDict(cdict_);
            ZSTD_freeDDict(ddict_);
            ZSTD_freeCCtx(cctx_);
            throw std::runtime_error("Failed to create decompression context");
        }
    }
    
    void cleanup() {
        if (dctx_) { ZSTD_freeDCtx(dctx_); dctx_ = nullptr; }
        if (cctx_) { ZSTD_freeCCtx(cctx_); cctx_ = nullptr; }
        if (ddict_) { ZSTD_freeDDict(ddict_); ddict_ = nullptr; }
        if (cdict_) { ZSTD_freeCDict(cdict_); cdict_ = nullptr; }
    }
    
    std::vector<uint8_t> dict_data_;
    ZSTD_CDict* cdict_ = nullptr;
    ZSTD_DDict* ddict_ = nullptr;
    ZSTD_CCtx* cctx_ = nullptr;
    ZSTD_DCtx* dctx_ = nullptr;
    int compression_level_;
};

/**
 * Training modes for dictionary creation
 */
enum class TrainMode {
    UNTRAINED,    // Just use concatenated samples as dictionary (no training)
    FINALIZED     // Use ZDICT_finalizeDictionary for optimal dictionary
};

/**
 * Builder for training ZSTD dictionaries
 */
class ZstdDictTrainer {
public:
    ZstdDictTrainer() = default;
    
    // Sample collection
    void reserve(size_t bytes) {
        samples_buffer_.reserve(bytes);
    }
    
    void add_sample(const std::vector<uint8_t>& sample) {
        add_sample(sample.data(), sample.size());
    }
    
    void add_sample(const uint8_t* data, size_t size) {
        if (size == 0) return;
        
        samples_buffer_.insert(samples_buffer_.end(), data, data + size);
        sample_sizes_.push_back(size);
    }
    
    // Training with default mode
    ZstdDict train(size_t dict_size = 64 * 1024, int compression_level = 3) {
        return train(TrainMode::FINALIZED, dict_size, compression_level);
    }
    
    // Training with explicit mode
    ZstdDict train(TrainMode mode, size_t dict_size = 64 * 1024, int compression_level = 3) {
        if (sample_sizes_.empty()) {
            throw std::runtime_error("No samples added - cannot train dictionary");
        }
        
        switch (mode) {
            case TrainMode::UNTRAINED: {
                // Just use concatenated samples as dictionary (no training)
                // Limit to requested dict_size if samples exceed it
                size_t actual_size = std::min(samples_buffer_.size(), dict_size);
                std::vector<uint8_t> dict_buffer(samples_buffer_.begin(), 
                                                  samples_buffer_.begin() + actual_size);
                return ZstdDict::from_data(dict_buffer, compression_level);
            }
            
            case TrainMode::FINALIZED: {
                // Check training data ratio (warn if less than 10x)
                double ratio = static_cast<double>(samples_buffer_.size()) / dict_size;
                if (ratio < 10.0) {
                    // Could warn here, but just proceed
                }
                
                // Allocate dictionary buffer
                size_t buffer_size = std::max(dict_size * 2, dict_size + 64 * 1024);
                std::vector<uint8_t> dict_buffer(buffer_size);
                
                // Train dictionary using ZDICT_finalizeDictionary (put samples directly in dict)
                ZDICT_params_t params = {};
                params.compressionLevel = compression_level;
                params.notificationLevel = 1;
                params.dictID = 0;

                size_t actual_size = ZDICT_finalizeDictionary(
                    dict_buffer.data(),
                    buffer_size,
                    samples_buffer_.data(),      // Use samples AS dictionary content
                    samples_buffer_.size(),
                    samples_buffer_.data(),      // Also use samples for training analysis
                    sample_sizes_.data(),
                    static_cast<unsigned>(sample_sizes_.size()),
                    params);
                    
                if (ZDICT_isError(actual_size)) {
                    throw std::runtime_error("Dictionary training failed with " + 
                                           std::to_string(sample_sizes_.size()) + " samples (" +
                                           std::to_string(samples_buffer_.size()) + " bytes): " +
                                           ZDICT_getErrorName(actual_size));
                }
                
                // Resize to actual size
                dict_buffer.resize(actual_size);
                
                return ZstdDict::from_data(dict_buffer, compression_level);
            }
            
            default:
                throw std::runtime_error("Unknown training mode");
        }
    }
    
    // Stats
    struct Stats {
        size_t samples_count;
        size_t total_samples_size;
        double avg_sample_size;
    };
    
    Stats get_stats() const {
        return Stats{
            .samples_count = sample_sizes_.size(),
            .total_samples_size = samples_buffer_.size(),
            .avg_sample_size = sample_sizes_.empty() ? 0.0 : 
                              static_cast<double>(samples_buffer_.size()) / sample_sizes_.size()
        };
    }
    
    // Clear samples (optional memory management)
    void clear() {
        samples_buffer_.clear();
        samples_buffer_.shrink_to_fit();
        sample_sizes_.clear();
        sample_sizes_.shrink_to_fit();
    }

private:
    std::vector<uint8_t> samples_buffer_;
    std::vector<size_t> sample_sizes_;
};

} // namespace catl::experiments
