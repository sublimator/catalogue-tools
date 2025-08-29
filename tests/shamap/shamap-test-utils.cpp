#include "./shamap-test-utils.h"

using namespace catl::shamap;

ShaMapFixture::ShaMapFixture()
{
}

void
ShaMapFixture::SetUp()
{
    // Get the fixture directory
    fixture_dir = get_fixture_directory();

    // Create map with proper node type and options
    if (auto options = get_map_options())
    {
        map = SHAMap(get_node_type(), *options);
    }
    else
    {
        map = SHAMap(get_node_type());
    }

    // Verify empty map hash
    EXPECT_EQ(
        map.get_hash().hex(),
        "0000000000000000000000000000000000000000000000000000000000000000");
}

SHAMapNodeType
ShaMapFixture::get_node_type()
{
    return tnACCOUNT_STATE;
}

std::optional<SHAMapOptions>
ShaMapFixture::get_map_options()
{
    return std::nullopt;
}

std::string
ShaMapFixture::get_fixture_directory()
{
    return "shamap/fixture";
}

std::string
ShaMapFixture::get_fixture_path(const std::string& filename) const
{
    return TestDataPath::get_path(fixture_dir + "/" + filename);
}

SetResult
ShaMapFixture::add_item_from_hex(
    const std::string& hex_string,
    std::optional<std::string> hex_data)
{
    auto item = items.make(hex_string, std::move(hex_data));
    return map.set_item(item);
}

bool
ShaMapFixture::remove_item_from_hex(const std::string& hex_string)
{
    auto item = items.make(hex_string);
    return map.remove_item(item->key());
}
