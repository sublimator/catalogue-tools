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

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>

#include "catl/core/log-macros.h"
#include "catl/core/types.h"
#include "catl/v1/catl-v1-reader.h"
#include "catl/v1/catl-v1-utils.h"
#include "catl/v2/catl-v2-reader.h"
#include "catl/v2/catl-v2-structs.h"
#include "catl/v2/catl-v2-writer.h"
#include "catl/v2/shamap-custom-traits.h"
#include "catl/xdata/debug-visitor.h"
#include "catl/xdata/parser.h"
#include "catl/xdata/protocol.h"

using namespace catl;
using namespace catl::v2;
namespace po = boost::program_options;
namespace fs = boost::filesystem;

//----------------------------------------------------------
// Main Processing Logic
//----------------------------------------------------------

/**
 * Parse hex string to binary key
 */
std::optional<std::array<uint8_t, 32>>
parse_hex_key(const std::string& hex)
{
    if (hex.length() != 64)
    {
        return std::nullopt;
    }

    std::array<uint8_t, 32> key;
    for (size_t i = 0; i < 32; ++i)
    {
        std::string byte_str = hex.substr(i * 2, 2);
        try
        {
            key[i] = static_cast<uint8_t>(std::stoi(byte_str, nullptr, 16));
        }
        catch (...)
        {
            return std::nullopt;
        }
    }
    return key;
}

/**
 * Look up a key and display it
 */
void
lookup_key(
    CatlV2Reader& reader,
    const xdata::Protocol& protocol,
    const std::string& key_hex,
    uint32_t ledger_seq)
{
    auto key_bytes = parse_hex_key(key_hex);
    if (!key_bytes)
    {
        LOGE("Invalid key format. Expected 64 hex characters.");
        return;
    }

    // Seek to the requested ledger
    if (!reader.seek_to_ledger(ledger_seq))
    {
        LOGE("Ledger ", ledger_seq, " not found in file");
        return;
    }

    // Read the ledger header
    auto ledger_info = reader.read_ledger_info();
    LOGI("Found ledger ", ledger_info.seq);

    // Look up the key
    Key key(key_bytes->data());
    auto data_slice = reader.lookup_key_in_state(key);

    if (!data_slice.has_value())
    {
        LOGE("Key not found: ", key.hex());
        return;
    }

    LOGI("Key found! Data size: ", data_slice->size(), " bytes");

    // Parse and display using xdata
    try
    {
        // Create a counting visitor to display the data
        xdata::CountingVisitor visitor;
        xdata::ParserContext ctx(data_slice.value());

        // Parse the SLE
        xdata::parse_with_visitor(ctx, protocol, visitor);

        // Output the actual parsed data
        LOGI("\nParsed data:");
        std::cout << visitor.get_output();

        LOGI("\nParsed data statistics:");
        LOGI("  Fields: ", visitor.get_field_count());
        LOGI("  Objects: ", visitor.get_object_count());
        LOGI("  Arrays: ", visitor.get_array_count());
        LOGI("  Total output size: ", visitor.get_byte_count(), " bytes");
    }
    catch (const std::exception& e)
    {
        LOGE("Failed to parse data: ", e.what());
        // Fall back to hex display
        std::string hex;
        hex.reserve(data_slice->size() * 2);
        for (size_t i = 0; i < data_slice->size(); ++i)
        {
            char buf[3];
            snprintf(buf, sizeof(buf), "%02X", data_slice->data()[i]);
            hex += buf;
        }
        LOGI("Raw data (hex): ", hex);
    }
}

/**
 * Verify the written CATL v2 file by testing random access
 */
bool
verify_catl2_file(
    const std::string& filename,
    uint32_t min_seq,
    uint32_t max_seq)
{
    LOGI("Verifying CATL v2 file: ", filename);

    try
    {
        CatlV2Reader reader(filename);
        auto header = reader.header();

        LOGI("File contains ", header.ledger_count, " ledgers");
        LOGI(
            "Range: ", header.first_ledger_seq, " to ", header.last_ledger_seq);

        // Create vector of all ledger sequences
        std::vector<uint32_t> sequences;
        for (uint32_t seq = min_seq; seq <= max_seq; ++seq)
        {
            sequences.push_back(seq);
        }

        // Shuffle for random access pattern
        std::random_device rd;
        std::mt19937 gen(rd());
        std::shuffle(sequences.begin(), sequences.end(), gen);

        // Test subset (up to 100 random ledgers)
        size_t test_count = std::min(sequences.size(), size_t(100));
        LOGI("Testing ", test_count, " random ledger accesses");

        size_t success_count = 0;
        auto start_time = std::chrono::high_resolution_clock::now();

        for (size_t i = 0; i < test_count; ++i)
        {
            uint32_t target_seq = sequences[i];

            if (reader.seek_to_ledger(target_seq))
            {
                auto info = reader.read_ledger_info();
                if (info.seq == target_seq)
                {
                    success_count++;
                }
                else
                {
                    LOGE(
                        "Sequence mismatch: expected ",
                        target_seq,
                        " but got ",
                        info.seq);
                }
            }
            else
            {
                LOGE("Failed to seek to ledger ", target_seq);
            }
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time);

        LOGI(
            "Verification complete: ",
            success_count,
            "/",
            test_count,
            " successful (",
            duration.count(),
            "ms)");

        return success_count == test_count;
    }
    catch (const std::exception& e)
    {
        LOGE("Verification failed: ", e.what());
        return false;
    }
}

/**
 * Process multiple ledgers, converting from v1 to v2 format
 */
void
process_all_ledgers(
    const std::string& input_file,
    const std::string& output_file,
    uint32_t max_ledgers,
    bool verify)
{
    v1::Reader reader(input_file);
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
    CatlV2Writer writer(output_file);

    // Process subset if requested
    auto max_ledger = (max_ledgers > 0)
        ? std::min(header.min_ledger + max_ledgers - 1, header.max_ledger)
        : header.max_ledger;

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

            auto delta_inner_bytes = stats_after.inner_bytes_written -
                stats_before.inner_bytes_written;
            auto delta_leaf_bytes = stats_after.leaf_bytes_written -
                stats_before.leaf_bytes_written;

            LOGI(
                "Ledger ",
                ledger_seq,
                " - Wrote ",
                delta_inners,
                " new inners (",
                delta_inner_bytes,
                " bytes), ",
                delta_leaves,
                " new leaves (",
                delta_leaf_bytes,
                " bytes) (cumulative: ",
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
        return;
    }

    // Print final statistics
    auto final_stats = writer.stats();
    LOGI("\nFinal serialization statistics:");
    LOGI("  Total inner nodes written: ", final_stats.inner_nodes_written);
    LOGI("  Total leaf nodes written: ", final_stats.leaf_nodes_written);
    LOGI("  Total bytes written: ", final_stats.total_bytes_written);
    LOGI("\nBytes breakdown:");
    LOGI(
        "  Inner nodes: ",
        final_stats.inner_bytes_written,
        " bytes (",
        std::fixed,
        std::setprecision(1),
        (100.0 * final_stats.inner_bytes_written /
         final_stats.total_bytes_written),
        "%)");
    LOGI(
        "  Leaf nodes: ",
        final_stats.leaf_bytes_written,
        " bytes (",
        std::fixed,
        std::setprecision(1),
        (100.0 * final_stats.leaf_bytes_written /
         final_stats.total_bytes_written),
        "%)");
    LOGI(
        "  Other (headers, index, etc): ",
        final_stats.total_bytes_written - final_stats.inner_bytes_written -
            final_stats.leaf_bytes_written,
        " bytes");

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

    // Verify the file if requested
    if (verify)
    {
        LOGI("\nVerifying written file...");
        bool verify_success =
            verify_catl2_file(output_file, header.min_ledger, max_ledger);
        if (!verify_success)
        {
            LOGE("Verification failed!");
        }
    }
}

/**
 * Main entry point
 */
int
main(int argc, char* argv[])
{
    try
    {
        // Define command line options
        po::options_description desc("CATL v1 to v2 converter");
        desc.add_options()("help,h", "Show this help message")(
            "input,i", po::value<std::string>(), "Input CATL v1 file")(
            "output,o", po::value<std::string>(), "Output CATL v2 file")(
            "max-ledgers,m",
            po::value<uint32_t>()->default_value(0),
            "Maximum number of ledgers to process (0 = all)")(
            "verify-and-test",
            po::value<bool>()->default_value(true),
            "Verify the output file with random access tests")(
            "no-verify-and-test", "Disable output file verification")(
            "log-level,l",
            po::value<std::string>()->default_value("info"),
            "Log level (debug, info, warn, error)")(
            "get-key",
            po::value<std::string>(),
            "Look up a key (hex) in the CATL v2 file")(
            "get-ledger",
            po::value<uint32_t>(),
            "Ledger sequence to use for key lookup")(
            "protocol-definitions",
            po::value<std::string>()->default_value(
                std::string(PROJECT_ROOT) +
                "tests/x-data/fixture/xahau_definitions.json"),
            "Path to protocol definitions JSON file");

        po::variables_map vm;
        po::store(po::parse_command_line(argc, argv, desc), vm);

        // Show help if requested
        if (vm.count("help"))
        {
            std::cout << desc << std::endl;
            return 0;
        }

        po::notify(vm);

        // TODO: use log level from command line
        // TODO: use Logger::set_level(const std::string& levelStr) and add to
        // the po::options_description
        Logger::set_level(LogLevel::INFO);

        // Check if we're in key lookup mode
        if (vm.count("get-key"))
        {
            // Key lookup mode - requires input file and ledger
            if (!vm.count("input"))
            {
                std::cerr << "Error: --input is required for key lookup"
                          << std::endl;
                return 1;
            }
            if (!vm.count("get-ledger"))
            {
                std::cerr << "Error: --get-ledger is required for key lookup"
                          << std::endl;
                return 1;
            }

            std::string input_file = vm["input"].as<std::string>();
            std::string key_hex = vm["get-key"].as<std::string>();
            uint32_t ledger_seq = vm["get-ledger"].as<uint32_t>();
            std::string protocol_path =
                vm["protocol-definitions"].as<std::string>();

            // Load protocol
            xdata::Protocol protocol =
                xdata::Protocol::load_from_file(protocol_path);

            // Open reader
            CatlV2Reader reader(input_file);

            // Perform lookup
            lookup_key(reader, protocol, key_hex, ledger_seq);

            return 0;
        }

        // Handle --no-verify-and-test flag
        bool verify = vm["verify-and-test"].as<bool>();
        if (vm.count("no-verify-and-test"))
        {
            verify = false;
        }

        // // Set log level
        // std::string log_level = vm["log-level"].as<std::string>();
        // if (log_level == "debug")
        //     Logger::set_level(LogLevel::DEBUG);
        // else if (log_level == "info")
        //     Logger::set_level(LogLevel::INFO);
        // else if (log_level == "warn")
        //     Logger::set_level(LogLevel::WARNING);
        // else if (log_level == "error")
        //     Logger::set_level(LogLevel::ERROR);
        // else
        // {
        //     std::cerr << "Invalid log level: " << log_level << std::endl;
        //     return 1;
        // }

        // Conversion mode - require input and output
        if (!vm.count("input") || !vm.count("output"))
        {
            std::cerr << "Error: Both --input and --output are required for "
                         "conversion"
                      << std::endl;
            std::cerr << "Use --help for usage information" << std::endl;
            return 1;
        }

        // Get parameters
        std::string input_file = vm["input"].as<std::string>();
        std::string output_file = vm["output"].as<std::string>();
        uint32_t max_ledgers = vm["max-ledgers"].as<uint32_t>();

        // Check input file exists
        if (!fs::exists(input_file))
        {
            std::cerr << "Input file does not exist: " << input_file
                      << std::endl;
            return 1;
        }

        // Check output directory exists
        fs::path output_path(output_file);
        fs::path output_dir = output_path.parent_path();
        if (!output_dir.empty() && !fs::exists(output_dir))
        {
            std::cerr << "Output directory does not exist: " << output_dir
                      << std::endl;
            return 1;
        }

        LOGI("Converting CATL v1 to v2");
        LOGI("Input: ", input_file);
        LOGI("Output: ", output_file);
        if (max_ledgers > 0)
        {
            LOGI("Max ledgers: ", max_ledgers);
        }
        LOGI("Verify: ", verify ? "enabled" : "disabled");

        // Process the file
        process_all_ledgers(input_file, output_file, max_ledgers, verify);

        return 0;
    }
    catch (const po::error& e)
    {
        std::cerr << "Command line error: " << e.what() << std::endl;
        std::cerr << "Use --help for usage information" << std::endl;
        return 1;
    }
    catch (const std::exception& e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}