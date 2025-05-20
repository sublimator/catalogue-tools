#pragma once

#include "catl/shamap/shamap.h"
#include "catl/test-utils/test-utils.h"

// Class to manage test items and their memory
class TestItems
{
private:
    // Store vectors for memory management
    std::vector<std::vector<uint8_t>> buffers;

public:
    // Create an item from hex strings
    boost::intrusive_ptr<MmapItem>
    make(
        const std::string& hex_string,
        std::optional<std::string> hex_data = std::nullopt);

    // Helper to clear buffers if needed
    void
    clear();
};

class ShaMapFixture : public ::testing::Test
{
protected:
    ShaMapFixture();

    void
    SetUp() override;

    // Virtual methods for customization
    virtual catl::shamap::SHAMapNodeType
    get_node_type();
    virtual std::optional<catl::shamap::SHAMapOptions>
    get_map_options();
    virtual std::string
    get_fixture_directory();

    std::string
    get_fixture_path(const std::string& filename) const;

    catl::shamap::SetResult
    add_item_from_hex(
        const std::string& hex_string,
        std::optional<std::string> hex_data = std::nullopt);
    bool
    remove_item_from_hex(const std::string& hex_string);

    // Member variables
    catl::shamap::SHAMap map;
    TestItems items;
    std::string fixture_dir;
};

class TransactionFixture : public ShaMapFixture
{
protected:
    catl::shamap::SHAMapNodeType
    get_node_type() override
    {
        return catl::shamap::tnTRANSACTION_MD;
    }
};

class AccountStateFixture : public ShaMapFixture
{
protected:
    catl::shamap::SHAMapNodeType
    get_node_type() override
    {
        return catl::shamap::tnACCOUNT_STATE;
    }
};