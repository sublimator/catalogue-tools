#include "catl/test-utils/test-utils.h"
#include "catl/xdata/debug-visitor.h"
#include "catl/xdata/parser-context.h"
#include "catl/xdata/parser.h"
#include "catl/xdata/protocol.h"
#include "catl/xdata/slice-visitor.h"
#include <algorithm>
#include <boost/json.hpp>
#include <chrono>
#include <fstream>
#include <gtest/gtest.h>
#include <iomanip>
#include <iostream>
#include <set>
#include <sstream>

#include "catl/v1/catl-v1-reader.h"

using namespace catl;
using namespace catl::xdata;

// Helper: Convert bytes to hex string
std::string
bytes_to_hex(const unsigned char* data, size_t len)
{
    std::ostringstream oss;
    for (size_t i = 0; i < len; ++i)
        oss << std::hex << std::setw(2) << std::setfill('0')
            << static_cast<int>(data[i]);
    return oss.str();
}

// Load test vectors from JSON file
std::vector<std::pair<std::string, std::string>>
load_vectors(const std::string& path)
{
    std::ifstream f(TestDataPath::get_path(path));
    std::vector<std::pair<std::string, std::string>> vectors;
    return vectors;
}

TEST(XData, LoadXahauDefinitions)
{
    // Load the Xahau definitions JSON file
    std::string definitions_path =
        TestDataPath::get_path("x-data/fixture/xahau_definitions.json");

    // Try to load and parse the protocol from the definitions
    auto protocol = Protocol::load_from_file(definitions_path);

    // Basic validation that we loaded something
    auto tx_type_opt = protocol.find_field("TransactionType");
    auto account_opt = protocol.find_field("Account");
    auto amount_opt = protocol.find_field("Amount");

    EXPECT_TRUE(tx_type_opt.has_value());
    EXPECT_TRUE(account_opt.has_value());
    EXPECT_TRUE(amount_opt.has_value());

    // Check some specific field metadata
    const auto& tx_type = tx_type_opt.value();
    EXPECT_EQ(tx_type.name, "TransactionType");
    EXPECT_EQ(tx_type.meta.type.name, "UInt16");

    const auto& account = account_opt.value();
    EXPECT_EQ(account.name, "Account");
    EXPECT_EQ(account.meta.type.name, "AccountID");

    // Verify we can look up by field code
    uint32_t tx_type_code =
        make_field_code(tx_type.meta.type.code, tx_type.meta.nth);

    const FieldDef* field_by_code = protocol.get_field_by_code(tx_type_code);
    EXPECT_NE(field_by_code, nullptr);
    EXPECT_EQ(field_by_code->name, tx_type.name);

    // Test that we have loaded the type mappings
    EXPECT_FALSE(protocol.types().empty());
    EXPECT_FALSE(protocol.transactionTypes().empty());

    // Verify protocol has many fields loaded
    EXPECT_GT(protocol.fields().size(), 100);  // XRPL has many fields
}

TEST(XData, ParseCatlFile)
{
    std::string definitions =
        TestDataPath::get_path("x-data/fixture/xahau_definitions.json");

    auto catl_file = TestDataPath::get_path(
        "catalogue-v1/fixture/cat.2000000-2010000.compression-0.catl");

    auto protocol = Protocol::load_from_file(definitions);

    v1::Reader reader(
        "/Users/nicholasdudfield/projects/catalogue-tools/"
        "test-slice-4500000-5000000.catl");
    auto header = reader.header();
    auto end = header.max_ledger;

    EXPECT_EQ(reader.compression_level(), 0);

    size_t success_count = 0;
    size_t error_count = 0;
    size_t total_count = 0;
    size_t total_bytes_processed = 0;
    const size_t debug_n_items = 5;  // Debug first N items
    std::set<std::string> field_names_seen;

    auto start_time = std::chrono::steady_clock::now();

    // Process ledgers in range
    while (true)
    {
        auto info = reader.read_ledger_info();
        auto current_ledger = info.sequence;

        // Only process if within range
        if (current_ledger % 1000 == 0)
        {
            auto current_time = std::chrono::steady_clock::now();
            auto elapsed =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    current_time - start_time)
                    .count();
            double bytes_per_second =
                elapsed > 0 ? (total_bytes_processed * 1000.0) / elapsed : 0;

            std::cerr << "Processing ledger " << std::dec << current_ledger
                      << " | " << total_bytes_processed << " bytes processed"
                      << " | " << std::fixed << std::setprecision(2)
                      << (bytes_per_second / 1024.0 / 1024.0) << " MB/s\n";
        }

        reader.read_map_with_callbacks(
            shamap::tnACCOUNT_STATE, [&](const auto& key, const auto& data) {
                auto slice = Slice(data.data(), data.size());
                total_count++;
                total_bytes_processed += data.size();

                // Debug output for first N items
                if (total_count <= debug_n_items)
                {
                    std::cerr << "\n=== Account State #" << total_count
                              << " ===\n";
                    std::cerr << "Ledger: " << current_ledger << "\n";
                    std::cerr << "Data size: " << std::dec << data.size()
                              << " bytes\n";
                    std::cerr << std::uppercase;
                    std::cerr << "Key: ";
                    for (size_t i = 0; i < key.size(); ++i)
                    {
                        std::cerr << std::hex << std::setw(2)
                                  << std::setfill('0')
                                  << static_cast<int>(
                                         static_cast<unsigned char>(key[i]));
                    }
                    std::cerr << "\n";
                    std::cerr << "Data: ";
                    for (size_t i = 0; i < data.size(); ++i)
                    {
                        std::cerr << std::hex << std::setw(2)
                                  << std::setfill('0')
                                  << static_cast<int>(
                                         static_cast<unsigned char>(data[i]));
                    }
                    std::cerr << "\n";
                    std::cerr << std::dec << "Parsing with debug visitor:\n";

                    ParserContext debug_ctx(slice);
                    DebugTreeVisitor debug_visitor(std::cerr);
                    try
                    {
                        parse_with_visitor(debug_ctx, protocol, debug_visitor);
                        std::cerr << "Parse successful!\n";
                    }
                    catch (const std::exception& e)
                    {
                        std::cerr << "Parse failed: " << e.what() << "\n";
                    }
                    std::cerr << std::dec << "=================\n";
                }

                // Regular parsing for statistics
                ParserContext ctx(slice);
                try
                {
                    // Collect field names from this account state object
                    SimpleSliceEmitter visitor([&](const FieldSlice& fs) {
                        // field_names_seen.insert(fs.get_field().name);
                    });

                    parse_with_visitor(ctx, protocol, visitor);
                    success_count++;
                }
                catch (const std::exception& e)
                {
                    error_count++;
                }
            });

        // TODO: should parse these too
        reader.skip_map(shamap::tnTRANSACTION_MD);

        if (info.sequence >= end)
        {
            break;
        }
    }

    // Basic assertions - we should be able to parse at least some account
    // states
    EXPECT_GT(success_count, 0)
        << "Should successfully parse at least one account state";
    EXPECT_LT(error_count, success_count)
        << "Should have more successes than errors";

    // Check that we saw some expected fields
    auto has_field = [&](const std::string& name) {
        return field_names_seen.count(name) > 0;
    };

    EXPECT_TRUE(has_field("Account"))
        << "Should see Account field in account states";
    EXPECT_TRUE(has_field("Balance"))
        << "Should see Balance field in account states";

    auto end_time = std::chrono::steady_clock::now();
    auto total_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                             end_time - start_time)
                             .count();
    double final_bytes_per_second = total_elapsed > 0
        ? (total_bytes_processed * 1000.0) / total_elapsed
        : 0;

    std::cout << std::dec << "Parse results: " << success_count
              << " successful, " << error_count << " errors\n";
    std::cout << "Total bytes processed: " << total_bytes_processed
              << " bytes\n";
    std::cout << "Total time: " << (total_elapsed / 1000.0) << " seconds\n";
    std::cout << "Average throughput: " << std::fixed << std::setprecision(2)
              << (final_bytes_per_second / 1024.0 / 1024.0) << " MB/s\n";
    std::cout << "Unique fields seen: " << field_names_seen.size() << "\n";
}

TEST(XData, TestVectors)
{
    EXPECT_TRUE(true);
}
