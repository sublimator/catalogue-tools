#include "catl/v1/catl-v1-mmap-reader.h"
#include "catl/xdata/parser.h"
#include "catl/xdata/protocol.h"
#include "catl/xdata/stats-visitor.h"
#include <boost/program_options.hpp>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>

namespace po = boost::program_options;
using namespace catl;
using namespace catl::xdata;

void
process_map_type(
    v1::MmapReader& reader,
    const Protocol& protocol,
    StatsVisitor& stats,
    shamap::SHAMapNodeType map_type,
    const std::string& type_name,
    size_t& total_count,
    size_t& total_bytes,
    size_t& success_count,
    size_t& error_count)
{
    reader.read_map_with_callbacks(
        map_type,
        [&](const Slice& key, const Slice& data) {
            total_count++;
            total_bytes += data.size();

            // Track key usage
            stats.track_key_use(key);

            ParserContext ctx(data);
            try
            {
                // Special handling for transaction with metadata
                if (map_type == shamap::tnTRANSACTION_MD)
                {
                    // First: Parse VL-encoded transaction
                    size_t tx_vl_length = read_vl_length(ctx.cursor);
                    Slice tx_data = ctx.cursor.read_slice(tx_vl_length);
                    ParserContext tx_ctx(tx_data);
                    parse_with_visitor(tx_ctx, protocol, stats);

                    // Second: Parse VL-encoded metadata
                    size_t meta_vl_length = read_vl_length(ctx.cursor);
                    Slice meta_data = ctx.cursor.read_slice(meta_vl_length);
                    ParserContext meta_ctx(meta_data);
                    parse_with_visitor(meta_ctx, protocol, stats);
                }
                else
                {
                    // Account state and other types are single objects
                    parse_with_visitor(ctx, protocol, stats);
                }
                success_count++;
            }
            catch (const std::exception& e)
            {
                error_count++;
                // Optionally log errors
            }
        },
        [&](const Slice& key) {
            // Track deletion
            stats.track_key_use(key, true);
        });
}

int
main(int argc, char* argv[])
{
    try
    {
        po::options_description desc("Allowed options");
        desc.add_options()("help,h", "produce help message")(
            "input,i", po::value<std::string>()->required(), "input CATL file")(
            "definitions,d",
            po::value<std::string>()->required(),
            "definitions JSON file")(
            "output,o",
            po::value<std::string>()->default_value("stats.json"),
            "output JSON file")(
            "max-ledgers,m",
            po::value<size_t>()->default_value(0),
            "maximum ledgers to process (0 = all)")(
            "top-accounts,a",
            po::value<size_t>()->default_value(100),
            "top N accounts to track")(
            "top-currencies,c",
            po::value<size_t>()->default_value(50),
            "top N currencies to track")(
            "top-amounts,n",
            po::value<size_t>()->default_value(100),
            "top N amounts to track")("pretty,p", "pretty print JSON output");

        po::variables_map vm;
        po::store(po::parse_command_line(argc, argv, desc), vm);

        if (vm.count("help"))
        {
            std::cout << desc << "\n";
            return 0;
        }

        po::notify(vm);

        // Load protocol definitions
        std::string definitions_path = vm["definitions"].as<std::string>();
        auto protocol = Protocol::load_from_file(definitions_path);

        // Configure stats visitor
        StatsVisitor::Config stats_config;
        stats_config.top_n_accounts = vm["top-accounts"].as<size_t>();
        stats_config.top_n_currencies = vm["top-currencies"].as<size_t>();
        stats_config.top_n_amounts = vm["top-amounts"].as<size_t>();

        StatsVisitor stats(protocol, stats_config);

        // Open CATL file
        std::string input_path = vm["input"].as<std::string>();
        v1::MmapReader reader(input_path);
        auto header = reader.header();
        size_t max_ledgers = vm["max-ledgers"].as<size_t>();

        // Tracking variables
        size_t account_success = 0, account_error = 0, account_total = 0;
        size_t tx_success = 0, tx_error = 0, tx_total = 0;
        size_t total_bytes = 0;
        size_t ledger_count = 0;
        uint32_t first_ledger = 0;
        uint32_t last_ledger = 0;
        bool first_ledger_seen = false;

        auto start_time = std::chrono::steady_clock::now();

        // Process ledgers
        while (!reader.eof())
        {
            auto info = reader.read_ledger_info();
            auto current_ledger = info.sequence();
            ledger_count++;

            // Track first and last ledger
            if (!first_ledger_seen)
            {
                first_ledger = current_ledger;
                first_ledger_seen = true;
            }
            last_ledger = current_ledger;

            // Progress reporting
            if (current_ledger % 1000 == 0)
            {
                auto current_time = std::chrono::steady_clock::now();
                auto elapsed =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        current_time - start_time)
                        .count();
                double bytes_per_second =
                    elapsed > 0 ? (total_bytes * 1000.0) / elapsed : 0;

                std::cerr << "Processing ledger " << current_ledger << " | "
                          << total_bytes << " bytes processed"
                          << " | " << std::fixed << std::setprecision(2)
                          << (bytes_per_second / 1024.0 / 1024.0) << " MB/s\n";
            }

            // Process account states
            process_map_type(
                reader,
                protocol,
                stats,
                shamap::tnACCOUNT_STATE,
                "Account State",
                account_total,
                total_bytes,
                account_success,
                account_error);

            // Process transaction metadata
            process_map_type(
                reader,
                protocol,
                stats,
                shamap::tnTRANSACTION_MD,
                "Transaction Metadata",
                tx_total,
                total_bytes,
                tx_success,
                tx_error);

            // Check if we've processed enough ledgers
            if (max_ledgers > 0 && ledger_count >= max_ledgers)
            {
                break;
            }

            if (info.sequence() >= header.max_ledger)
            {
                break;
            }
        }

        auto end_time = std::chrono::steady_clock::now();
        auto total_elapsed =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                end_time - start_time)
                .count();

        // Set ledger range in stats
        stats.set_ledger_range(first_ledger, last_ledger);

        // Summary
        std::cout << "\n=== Processing Summary ===\n";
        std::cout << "Ledgers processed: " << ledger_count << "\n";
        std::cout << "Account States: " << account_success << " successful, "
                  << account_error << " errors\n";
        std::cout << "Transaction Metadata: " << tx_success << " successful, "
                  << tx_error << " errors\n";
        std::cout << "Total items: " << (account_total + tx_total) << "\n";
        std::cout << "Total bytes: " << total_bytes << "\n";
        std::cout << "Time elapsed: " << (total_elapsed / 1000.0)
                  << " seconds\n";
        std::cout << "Average throughput: " << std::fixed
                  << std::setprecision(2)
                  << ((total_bytes / 1024.0 / 1024.0) /
                      (total_elapsed / 1000.0))
                  << " MB/s\n";

        // Write statistics to JSON file
        std::string output_path = vm["output"].as<std::string>();
        std::ofstream output_file(output_path);
        if (!output_file.is_open())
        {
            std::cerr << "Failed to open output file: " << output_path << "\n";
            return 1;
        }

        bool pretty = vm.count("pretty") > 0;
        output_file << stats.to_json(pretty);
        output_file.close();

        std::cout << "\nStatistics saved to: " << output_path << "\n";

        return 0;
    }
    catch (const std::exception& e)
    {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
}