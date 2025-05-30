/**
 * catl1-to-catl2.cpp
 *
 * Converts CATL v1 files to the new CATL v2 format with:
 * - Canonical LedgerInfo format (compatible with rippled/xahaud)
 * - Tree size headers for fast skipping
 * - Structural sharing for incremental updates
 * - Ledger index at EOF for random access
 * - MMAP-friendly layout
 *
 * Key Features:
 * 1. Compact binary representation of inner nodes (6 bytes per inner)
 * 2. Depth-first serialization with structural sharing
 * 3. Zero-copy MMAP reading with tree size headers
 * 4. Efficient ledger index for O(log n) random access
 */

#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>

#include "catl/core/log-macros.h"
#include "catl/core/types.h"
#include "catl/v1/catl-v1-reader.h"
#include "catl/v1/catl-v1-utils.h"
#include "catl/v2/catl-v2-structs.h"
#include "catl/v2/catl-v2-writer.h"
#include "catl/v2/shamap-custom-traits.h"

using namespace catl;
using namespace catl::v2;
namespace po = boost::program_options;
namespace fs = boost::filesystem;

//----------------------------------------------------------
// Main Processing Logic
//----------------------------------------------------------

/**
 * Process multiple ledgers, demonstrating serialization concepts
 */
void
process_all_ledgers(const std::string& filename)
{
    v1::Reader reader(filename);
    auto header = reader.header();

    LOGI(
        "Processing ledgers from ",
        header.min_ledger,
        " to ",
        header.max_ledger);

    // Initialize state and tx maps with CoW support
    SHAMapS state_map(catl::shamap::tnACCOUNT_STATE);
    SHAMapS tx_map(catl::shamap::tnTRANSACTION_MD);
    state_map.snapshot();  // Enable CoW
    tx_map.snapshot();     // Enable CoW

    // Create a writer for actual binary output
    CatlV2Writer writer("test-serialized.bin");

    // Process subset for experimentation
    auto n_ledgers =
        header.min_ledger + 10'000;  // Just do 10 ledgers for testing
    auto max_ledger =
        std::min(static_cast<uint32_t>(n_ledgers), header.max_ledger);

    for (uint32_t ledger_seq = header.min_ledger; ledger_seq <= max_ledger;
         ledger_seq++)
    {
        LOGI("Processing ledger: ", ledger_seq);

        // Read ledger header
        auto v1_ledger_info = reader.read_ledger_info();
        auto canonical_info = to_canonical_ledger_info(v1_ledger_info);

        // Read state map using owned items for proper CoW behavior
        state_map.snapshot();  // Create snapshot point
        reader.read_map_with_shamap_owned_items(
            state_map, catl::shamap::tnACCOUNT_STATE, true);

        // Read transaction map
        tx_map.snapshot();  // Create snapshot point
        reader.read_map_with_shamap_owned_items(
            tx_map, catl::shamap::tnTRANSACTION_MD, true);

        // Write the complete ledger to disk
        auto stats_before = writer.stats();
        if (writer.write_ledger(canonical_info, state_map, tx_map))
        {
            auto stats_after = writer.stats();
            auto delta_inners = stats_after.inner_nodes_written -
                stats_before.inner_nodes_written;
            auto delta_leaves = stats_after.leaf_nodes_written -
                stats_before.leaf_nodes_written;

            LOGI(
                "Ledger ",
                ledger_seq,
                " - Wrote ",
                delta_inners,
                " new inners, ",
                delta_leaves,
                " new leaves (cumulative: ",
                stats_after.inner_nodes_written,
                "/",
                stats_after.leaf_nodes_written,
                ")");
        }
        else
        {
            LOGE("Failed to write ledger ", ledger_seq);
        }
    }

    // Finalize the file
    if (!writer.finalize())
    {
        LOGE("Failed to finalize file");
    }

    // Print final statistics
    auto final_stats = writer.stats();
    LOGI("\nFinal serialization statistics:");
    LOGI("  Total inner nodes written: ", final_stats.inner_nodes_written);
    LOGI("  Total leaf nodes written: ", final_stats.leaf_nodes_written);
    LOGI("  Total bytes written: ", final_stats.total_bytes_written);

    if (final_stats.compressed_leaves > 0)
    {
        double compression_ratio =
            static_cast<double>(final_stats.uncompressed_size) /
            static_cast<double>(final_stats.compressed_size);
        LOGI("\nCompression statistics:");
        LOGI("  Compressed leaves: ", final_stats.compressed_leaves);
        LOGI("  Uncompressed size: ", final_stats.uncompressed_size, " bytes");
        LOGI("  Compressed size: ", final_stats.compressed_size, " bytes");
        LOGI(
            "  Compression ratio: ",
            std::fixed,
            std::setprecision(2),
            compression_ratio,
            "x");
        LOGI(
            "  Space saved: ",
            final_stats.uncompressed_size - final_stats.compressed_size,
            " bytes (",
            std::fixed,
            std::setprecision(1),
            (1.0 - 1.0 / compression_ratio) * 100,
            "%)");
    }
}

/**
 * Main entry point
 */
int
main([[maybe_unused]] int argc, [[maybe_unused]] char* argv[])
{
    Logger::set_level(LogLevel::INFO);
    LOGI("Starting serialized inner node experiment");

    // TODO: Accept filename as command line argument
    process_all_ledgers(
        "/Users/nicholasdudfield/projects/xahau-history/"
        "cat.2000000-2010000.compression-0.catl");

    return 0;
}