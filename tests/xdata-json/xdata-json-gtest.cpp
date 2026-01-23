#include <boost/json.hpp>
#include <catl/test-utils/test-utils.h>
#include <catl/xdata-json/parse_transaction.h>
#include <catl/xdata/protocol.h>
#include <gtest/gtest.h>
#include <iostream>
#include <vector>

using namespace catl;
using namespace catl::xdata;

// Helper: Convert hex string to bytes
std::vector<uint8_t>
hex_to_bytes(const std::string& hex)
{
    std::vector<uint8_t> bytes;
    for (size_t i = 0; i < hex.size(); i += 2)
    {
        unsigned int byte;
        sscanf(hex.c_str() + i, "%2x", &byte);
        bytes.push_back(static_cast<uint8_t>(byte));
    }
    return bytes;
}

TEST(XDataJson, ParseShuffleTransaction)
{
    // Raw Shuffle transaction bytes from network
    std::string hex =
        "1200582400000000260000004852E8C1A018D5B9DC41C279689B4A4DE2356D37E8986D"
        "D7"
        "4A0611DB627B4C72B0C9505E5E2B38F8E601CDC96890872EE91411FA3DDB0E252F7A4F"
        "8B"
        "DA527BCF22C573CD684000000000000000730081140000000000000000000000000000"
        "000000000000";

    auto bytes = hex_to_bytes(hex);
    Slice data(bytes.data(), bytes.size());

    // Load protocol from fixture (has Shuffle/Entropy types)
    std::string defs_path =
        TestDataPath::get_path("xdata-json/fixture/server-definitions.json");
    auto protocol = Protocol::load_from_file(defs_path);

    // Parse and print JSON
    auto json = xdata::json::parse_txset_transaction(data, protocol);
    std::string json_str = boost::json::serialize(json);

    std::cout << "=== Parsed JSON ===\n" << json_str << "\n\n";

    // Pretty print by iterating fields
    if (json.is_object())
    {
        std::cout << "=== Fields ===\n";
        for (auto const& kv : json.as_object())
        {
            std::cout << "  " << kv.key() << ": "
                      << boost::json::serialize(kv.value()) << "\n";
        }
    }

    // Basic assertions
    ASSERT_TRUE(json.is_object());
    auto& obj = json.as_object();

    // Should have TransactionType
    ASSERT_TRUE(obj.contains("TransactionType"));
    EXPECT_EQ(obj["TransactionType"].as_string(), "Shuffle");

    // Check what fields exist
    std::cout << "\n=== All field names ===\n";
    for (auto const& kv : obj)
    {
        std::cout << "  - " << kv.key() << "\n";
    }
}
