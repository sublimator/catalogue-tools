#pragma once

#include "catl/shamap/shamap.h"
#include "catl/test-utils/test-mmap-items.h"
#include "catl/test-utils/test-utils.h"
#include "catl/core/logger.h"

class ShaMapFixture : public ::testing::Test
{
protected:
    ShaMapFixture();

    void
    SetUp() override;

    // Enable debug logging for tests that need it
    void
    enable_debug_logging() {
        Logger::set_level(LogLevel::DEBUG);
    }

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
    TestMmapItems items;
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