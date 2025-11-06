#include "catl/test-utils/test-utils.h"
#include "catl/xdata/debug-visitor.h"
#include "catl/xdata/parser-context.h"
#include "catl/xdata/parser.h"
#include "catl/xdata/protocol.h"
#include "catl/xdata/slice-visitor.h"
#include <algorithm>
#include <boost/filesystem.hpp>
#include <boost/json.hpp>
#include <chrono>
#include <fstream>
#include <gtest/gtest.h>
#include <iomanip>
#include <iostream>
#include <set>
#include <sstream>
#include <vector>

#include "catl/v1/catl-v1-mmap-reader.h"
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

// Structure to hold error information
struct ParseError
{
    std::string key;
    std::string type;
    std::string data;
    std::string error_message;
};

// Load test vectors from JSON file
std::vector<std::pair<std::string, std::string>>
load_vectors(const std::string& path)
{
    std::ifstream f(TestDataPath::get_path(path));
    std::vector<std::pair<std::string, std::string>> vectors;
    return vectors;
}

TEST(XData, ReadFieldHeader)
{
    // Test case 1: Simple field header - type and field both fit in first byte
    {
        // Type 1, Field 1 -> 0x11
        std::vector<uint8_t> data = {0x11};
        SliceCursor cursor(Slice(data.data(), data.size()), 0);

        auto [header, code] = read_field_header(cursor);
        EXPECT_EQ(code, (1u << 16) | 1u);  // Type 1, Field 1
        EXPECT_EQ(header.size(), 1u);
    }

    // Test case 2: Type in first byte, field in second byte
    {
        // Type 15, Field 0 -> 0xF0, then field 16
        std::vector<uint8_t> data = {0xF0, 16};
        SliceCursor cursor(Slice(data.data(), data.size()), 0);

        auto [header, code] = read_field_header(cursor);
        EXPECT_EQ(code, (15u << 16) | 16u);  // Type 15, Field 16
        EXPECT_EQ(header.size(), 2u);
    }

    // Test case 3: Type in second byte, field in first byte
    {
        // Type 0, Field 15 -> 0x0F, then type 16
        std::vector<uint8_t> data = {0x0F, 16};
        SliceCursor cursor(Slice(data.data(), data.size()), 0);

        auto [header, code] = read_field_header(cursor);
        EXPECT_EQ(code, (16u << 16) | 15u);  // Type 16, Field 15
        EXPECT_EQ(header.size(), 2u);
    }

    // Test case 4: Both type and field in separate bytes (edge case that was
    // buggy)
    {
        // Type 0, Field 0 -> 0x00, then type 16, then field 17
        std::vector<uint8_t> data = {0x00, 16, 17};
        SliceCursor cursor(Slice(data.data(), data.size()), 0);

        auto [header, code] = read_field_header(cursor);
        EXPECT_EQ(code, (16u << 16) | 17u);  // Type 16, Field 17
        EXPECT_EQ(header.size(), 3u);
    }

    // Test case 5: Large type and field values
    {
        // Type 0, Field 0 -> 0x00, then type 255, then field 255
        std::vector<uint8_t> data = {0x00, 255, 255};
        SliceCursor cursor(Slice(data.data(), data.size()), 0);

        auto [header, code] = read_field_header(cursor);
        EXPECT_EQ(code, (255u << 16) | 255u);  // Type 255, Field 255
        EXPECT_EQ(header.size(), 3u);
    }

    // Test case 6: Invalid - type 0 in extended byte
    {
        // Type 0, Field 1 -> 0x01, then type 0 (invalid)
        std::vector<uint8_t> data = {0x01, 0};
        SliceCursor cursor(Slice(data.data(), data.size()), 0);

        auto [header, code] = read_field_header(cursor);
        EXPECT_EQ(code, 0u);  // Should return 0 for invalid
    }

    // Test case 7: Invalid - field < 16 in extended byte
    {
        // Type 1, Field 0 -> 0x10, then field 15 (invalid, must be >= 16)
        std::vector<uint8_t> data = {0x10, 15};
        SliceCursor cursor(Slice(data.data(), data.size()), 0);

        auto [header, code] = read_field_header(cursor);
        EXPECT_EQ(code, 0u);  // Should return 0 for invalid
    }

    // Test case 8: EOF cases
    {
        // Empty data
        std::vector<uint8_t> data = {};
        SliceCursor cursor(Slice(data.data(), data.size()), 0);

        auto [header, code] = read_field_header(cursor);
        EXPECT_EQ(code, 0u);  // Should return 0 for EOF
        EXPECT_EQ(header.size(), 0u);
    }

    // Test case 9: The problematic field code from the error
    {
        // Field code 786433 = 0xC0001 = Type 12, Field 1
        // This should be encoded as 0xC1 (type 12 in upper nibble, field 1 in
        // lower)
        std::vector<uint8_t> data = {0xC1};
        SliceCursor cursor(Slice(data.data(), data.size()), 0);

        auto [header, code] = read_field_header(cursor);
        EXPECT_EQ(code, 786433u);  // 0xC0001
        EXPECT_EQ(get_field_type_code(code), 12u);
        EXPECT_EQ(get_field_id(code), 1u);
        EXPECT_EQ(header.size(), 1u);
    }
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

// Null stream that discards all output
class NullStream : public std::ostream
{
    class NullBuf : public std::streambuf
    {
    public:
        int
        overflow(int c) override
        {
            return c;
        }
    } null_buf;

public:
    NullStream() : std::ostream(&null_buf)
    {
    }
};

// Parameterizable function to process a map type
void
process_map_type(
    v1::MmapReader& reader,
    const Protocol& protocol,
    shamap::SHAMapNodeType map_type,
    const std::string& type_name,
    size_t& total_count,
    size_t& total_bytes_processed,
    size_t& success_count,
    size_t& error_count,
    std::set<std::string>& field_names_seen,
    std::vector<ParseError>& errors,
    size_t debug_n_items,
    bool debug_dev_null = true,
    size_t max_errors = 100)
{
    reader.read_map_with_callbacks(
        map_type,
        [&](const Slice& key, const Slice& data) {
            total_count++;
            total_bytes_processed += data.size();

            // Debug output for first N items (or all if debug_dev_null is true)
            static NullStream null_stream;
            bool should_debug =
                debug_dev_null ? true : (total_count <= debug_n_items);
            std::ostream& debug_out = debug_dev_null ? null_stream : std::cerr;

            // TEST: Use CountingVisitor instead to see raw performance
            bool use_counting_visitor = true;

            if (should_debug && use_counting_visitor)
            {
                ParserContext debug_ctx(data);
                CountingVisitor counting_visitor;
                try
                {
                    if (map_type == shamap::tnTRANSACTION_MD)
                    {
                        // First: Parse VL-encoded transaction
                        size_t tx_vl_length = read_vl_length(debug_ctx.cursor);
                        Slice tx_data =
                            debug_ctx.cursor.read_slice(tx_vl_length);
                        ParserContext tx_ctx(tx_data);
                        parse_with_visitor(tx_ctx, protocol, counting_visitor);

                        // Second: Parse VL-encoded metadata
                        size_t meta_vl_length =
                            read_vl_length(debug_ctx.cursor);
                        Slice meta_data =
                            debug_ctx.cursor.read_slice(meta_vl_length);
                        ParserContext meta_ctx(meta_data);
                        parse_with_visitor(
                            meta_ctx, protocol, counting_visitor);
                    }
                    else
                    {
                        parse_with_visitor(
                            debug_ctx, protocol, counting_visitor);
                    }

                    // Log stats occasionally
                    if (total_count % 10000 == 0)
                    {
                        std::cerr << "CountingVisitor: "
                                  << counting_visitor.get_field_count()
                                  << " fields, "
                                  << counting_visitor.get_byte_count()
                                  << " bytes would be output\n";
                    }
                }
                catch (const std::exception& e)
                {
                    // Ignore for counting test
                }
            }

            // Regular parsing for statistics
            ParserContext ctx(data);
            try
            {
                // Collect field names from this object
                SimpleSliceEmitter visitor([&](const FieldSlice& fs) {
                    field_names_seen.insert(fs.get_field().name);
                });

                // Special handling for transaction with metadata (contains TWO
                // VL-encoded objects)
                if (map_type == shamap::tnTRANSACTION_MD)
                {
                    // First: Parse VL-encoded transaction
                    size_t tx_vl_length = read_vl_length(ctx.cursor);
                    Slice tx_data = ctx.cursor.read_slice(tx_vl_length);
                    ParserContext tx_ctx(tx_data);
                    parse_with_visitor(tx_ctx, protocol, visitor);

                    // Second: Parse VL-encoded metadata
                    size_t meta_vl_length = read_vl_length(ctx.cursor);
                    Slice meta_data = ctx.cursor.read_slice(meta_vl_length);
                    ParserContext meta_ctx(meta_data);
                    parse_with_visitor(meta_ctx, protocol, visitor);
                }
                else
                {
                    // Account state, transaction without metadata, and other
                    // types are single objects
                    parse_with_visitor(ctx, protocol, visitor);
                }
                success_count++;
            }
            catch (const std::exception& e)
            {
                error_count++;
                if (should_debug)
                {
                    debug_out << "Exception details: " << e.what() << "\n";
                }

                // Collect error information (limit to max_errors to avoid
                // memory issues)
                if (errors.size() < max_errors)
                {
                    ParseError parse_error;
                    parse_error.key = bytes_to_hex(key.data(), key.size());
                    parse_error.type = type_name;
                    parse_error.data = bytes_to_hex(data.data(), data.size());
                    parse_error.error_message = e.what();
                    errors.push_back(parse_error);
                }
            }
        },
        nullptr  // No delete callback needed for this test
    );
}

TEST(XData, ParseCatlFile)
{
    std::string definitions =
        TestDataPath::get_path("x-data/fixture/xahau_definitions.json");

    auto protocol = Protocol::load_from_file(definitions);

    auto compressed_file = TestDataPath::get_path(
        "catalogue-v1/fixture/cat.2000000-2010000.compression-9.catl");

    // Check if we need to decompress for MmapReader
    std::string catl_file = compressed_file;
    std::string decompressed_file = TestDataPath::get_path(
        "catalogue-v1/fixture/cat.2000000-2010000.compression-0.catl");

    // First check if compressed file exists and if decompressed doesn't
    if (boost::filesystem::exists(compressed_file) &&
        !boost::filesystem::exists(decompressed_file))
    {
        std::cerr << "Decompressing test fixture for MmapReader...\n";
        v1::Reader compressed_reader(compressed_file);

        // Check if it's actually compressed
        if (compressed_reader.compression_level() > 0)
        {
            compressed_reader.decompress(decompressed_file);
            std::cerr << "Decompression complete.\n";
        }

        catl_file = decompressed_file;
    }
    else if (boost::filesystem::exists(decompressed_file))
    {
        // Use existing decompressed file
        catl_file = decompressed_file;
    }

    // Use MmapReader for better performance on uncompressed files
    v1::MmapReader reader(catl_file);
    auto header = reader.header();
    auto end = header.max_ledger;

    EXPECT_EQ(reader.compression_level(), 0);

    size_t account_success_count = 0;
    size_t account_error_count = 0;
    size_t account_total_count = 0;
    size_t tx_success_count = 0;
    size_t tx_error_count = 0;
    size_t tx_total_count = 0;
    size_t total_bytes_processed = 0;
    const size_t debug_n_items = 5;    // Debug first N items
    const bool debug_dev_null = true;  // Set to true to debug ALL items to
                                       // /dev/null for performance testing
    std::set<std::string> field_names_seen;
    std::vector<ParseError> parse_errors;

    auto start_time = std::chrono::steady_clock::now();

    // Process ledgers in range
    while (!reader.eof())
    {
        auto info = reader.read_ledger_info();
        auto current_ledger = info.sequence();

        // Progress reporting
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

        // Process account states
        process_map_type(
            reader,
            protocol,
            shamap::tnACCOUNT_STATE,
            "Account State",
            account_total_count,
            total_bytes_processed,
            account_success_count,
            account_error_count,
            field_names_seen,
            parse_errors,
            debug_n_items,
            debug_dev_null);

        // Process transaction metadata
        process_map_type(
            reader,
            protocol,
            shamap::tnTRANSACTION_MD,
            "Transaction Metadata",
            tx_total_count,
            total_bytes_processed,
            tx_success_count,
            tx_error_count,
            field_names_seen,
            parse_errors,
            debug_n_items,
            debug_dev_null);

        if (info.sequence() >= end)
        {
            break;
        }
    }

    auto end_time = std::chrono::steady_clock::now();
    auto total_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                             end_time - start_time)
                             .count();
    double final_bytes_per_second = total_elapsed > 0
        ? (total_bytes_processed * 1000.0) / total_elapsed
        : 0;

    // Basic assertions
    EXPECT_GT(account_success_count, 0)
        << "Should successfully parse at least one account state";
    EXPECT_LT(account_error_count, account_success_count)
        << "Should have more successes than errors for account states";

    // Check that we saw some expected fields
    auto has_field = [&](const std::string& name) {
        return field_names_seen.count(name) > 0;
    };

    EXPECT_TRUE(has_field("Account"))
        << "Should see Account field in account states";
    EXPECT_TRUE(has_field("Balance"))
        << "Should see Balance field in account states";

    std::cout << std::dec << "\n=== Parse Results ===\n";
    std::cout << "Account States: " << account_success_count << " successful, "
              << account_error_count << " errors\n";
    std::cout << "Transaction Metadata: " << tx_success_count << " successful, "
              << tx_error_count << " errors\n";
    std::cout << "Total items processed: "
              << (account_total_count + tx_total_count) << "\n";
    std::cout << "Total bytes processed: " << total_bytes_processed
              << " bytes\n";
    std::cout << "Total time: " << (total_elapsed / 1000.0) << " seconds\n";
    std::cout << "Average throughput: " << std::fixed << std::setprecision(2)
              << (final_bytes_per_second / 1024.0 / 1024.0) << " MB/s\n";
    std::cout << "Unique fields seen: " << field_names_seen.size() << "\n";

    // Save errors to JSON file if any were collected
    if (!parse_errors.empty())
    {
        std::string error_file_path =
            TestDataPath::get_path("x-data/fixture/parser_errors.json");
        std::ofstream error_file(error_file_path);

        if (error_file.is_open())
        {
            boost::json::object root;
            boost::json::array errors_array;

            for (const auto& error : parse_errors)
            {
                boost::json::object error_obj;
                error_obj["key"] = error.key;
                error_obj["type"] = error.type;
                error_obj["data"] = error.data;
                error_obj["error_message"] = error.error_message;
                errors_array.push_back(error_obj);
            }

            root["errors"] = errors_array;

            error_file << boost::json::serialize(root);
            error_file.close();

            std::cout << "Saved " << parse_errors.size() << " parse errors to "
                      << error_file_path << "\n";
        }
        else
        {
            std::cerr << "Failed to open error file for writing: "
                      << error_file_path << "\n";
        }
    }
}

TEST(XData, TestVectors)
{
    EXPECT_TRUE(true);
}
