/**
 * zstd-experiment.cpp
 *
 * Simple experiment to test ZSTD compression on progressively longer
 * sequences of bytes: [0], [0,1], [0,1,2], ..., [0,1,2,...,24]
 *
 * Shows compression ratios and overhead for small sequences.
 */

#include <iostream>
#include <vector>
#include <iomanip>
#include <zstd.h>

int COMPRESSION_LEVEL = 3;

int main()
{
    std::cout << "ZSTD Compression Experiment - Progressive Byte Sequences\n";
    std::cout << "========================================================\n\n";

    std::cout << std::left << std::setw(8) << "Seq Len"
              << std::setw(12) << "Original"
              << std::setw(12) << "Compressed"
              << std::setw(10) << "Ratio"
              << std::setw(10) << "Overhead"
              << std::setw(12) << "Efficiency"
              << "Sequence\n";
    std::cout << std::string(80, '-') << "\n";

    size_t total_original = 0;
    size_t total_compressed = 0;
    size_t sequences_with_overhead = 0;
    double max_overhead = 0;

    // Test sequences from [0] to [0,1,2,...,24] (25 total sequences)
    for (int seq_len = 1; seq_len <= 25; seq_len++)
    {
        // Create sequence [0, 1, 2, ..., seq_len-1]
        std::vector<uint8_t> sequence;
        sequence.reserve(seq_len);
        for (int i = 0; i < seq_len; i++)
        {
            sequence.push_back(static_cast<uint8_t>(i));
        }

        // Get compression bound and allocate buffer
        size_t comp_bound = ZSTD_compressBound(sequence.size());
        std::vector<char> compressed(comp_bound);

        // Compress the sequence
        size_t compressed_size = ZSTD_compress(
            compressed.data(),
            comp_bound,
            sequence.data(),
            sequence.size(),
            COMPRESSION_LEVEL);

        if (ZSTD_isError(compressed_size))
        {
            std::cerr << "Compression failed for sequence length " << seq_len
                      << ": " << ZSTD_getErrorName(compressed_size) << std::endl;
            continue;
        }

        // Calculate metrics
        double compression_ratio = static_cast<double>(sequence.size()) / compressed_size;
        int overhead_bytes = static_cast<int>(compressed_size) - static_cast<int>(sequence.size());
        double overhead_percent = 100.0 * overhead_bytes / sequence.size();
        double efficiency = sequence.size() > compressed_size ?
                           100.0 * (sequence.size() - compressed_size) / sequence.size() :
                           0.0;

        // Track statistics
        total_original += sequence.size();
        total_compressed += compressed_size;
        if (overhead_bytes > 0)
        {
            sequences_with_overhead++;
            max_overhead = std::max(max_overhead, overhead_percent);
        }

        // Print results
        std::cout << std::left << std::setw(8) << seq_len
                  << std::setw(12) << (std::to_string(sequence.size()) + " bytes")
                  << std::setw(12) << (std::to_string(compressed_size) + " bytes")
                  << std::setw(10) << (std::to_string(compression_ratio).substr(0, 4) + "x")
                  << std::setw(10);

        if (overhead_bytes > 0)
        {
            std::cout << ("+" + std::to_string(overhead_bytes) + "B");
        }
        else if (overhead_bytes == 0)
        {
            std::cout << "0B";
        }
        else
        {
            std::cout << (std::to_string(overhead_bytes) + "B");
        }

        std::cout << std::setw(12);
        if (efficiency > 0)
        {
            std::cout << (std::to_string(efficiency).substr(0, 4) + "%");
        }
        else
        {
            std::cout << "0%";
        }

        // Show first few bytes of sequence
        std::cout << "[";
        for (size_t i = 0; i < std::min(static_cast<size_t>(5), sequence.size()); i++)
        {
            if (i > 0) std::cout << ",";
            std::cout << static_cast<int>(sequence[i]);
        }
        if (sequence.size() > 5)
        {
            std::cout << "...";
        }
        std::cout << "]";

        std::cout << std::endl;
    }

    // Summary statistics
    std::cout << "\n" << std::string(80, '=') << "\n";
    std::cout << "SUMMARY STATISTICS\n";
    std::cout << std::string(80, '=') << "\n";

    double overall_ratio = static_cast<double>(total_original) / total_compressed;

    std::cout << "Total original size:     " << total_original << " bytes\n";
    std::cout << "Total compressed size:   " << total_compressed << " bytes\n";
    std::cout << "Overall compression:     " << std::fixed << std::setprecision(2)
              << overall_ratio << "x\n";
    std::cout << "Overall space saved:     " << std::fixed << std::setprecision(1)
              << 100.0 * (1.0 - (double)total_compressed / total_original) << "%\n";
    std::cout << "Sequences with overhead: " << sequences_with_overhead << "/25 ("
              << std::fixed << std::setprecision(1)
              << 100.0 * sequences_with_overhead / 25 << "%)\n";
    std::cout << "Maximum overhead:        " << std::fixed << std::setprecision(1)
              << max_overhead << "%\n";

    // Analysis
    std::cout << "\n" << std::string(80, '=') << "\n";
    std::cout << "ANALYSIS\n";
    std::cout << std::string(80, '=') << "\n";

    if (sequences_with_overhead > 15)
    {
        std::cout << "🔴 HIGH OVERHEAD: Most small sequences have compression overhead!\n";
        std::cout << "    ZSTD headers/metadata dominate for tiny inputs.\n";
    }
    else if (sequences_with_overhead > 5)
    {
        std::cout << "🟡 MODERATE OVERHEAD: Some sequences expand when compressed.\n";
        std::cout << "    This is normal for very small, random-looking data.\n";
    }
    else
    {
        std::cout << "🟢 LOW OVERHEAD: Most sequences compress efficiently.\n";
    }

    std::cout << "\nKey insights:\n";
    std::cout << "• ZSTD has ~" << ZSTD_compressBound(1) - 1 << " byte minimum overhead (headers)\n";
    std::cout << "• Progressive sequences [0,1,2,3...] are highly compressible\n";
    std::cout << "• Compression becomes more effective as input size increases\n";
    std::cout << "• Small inputs (< ~50 bytes) often have overhead due to headers\n";

    if (overall_ratio > 1.0)
    {
        std::cout << "• Overall: " << std::fixed << std::setprecision(1)
                  << (overall_ratio - 1.0) * 100 << "% space savings achieved!\n";
    }

    return 0;
}