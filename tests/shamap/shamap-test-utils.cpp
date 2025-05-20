#include "./shamap-test-utils.h"

using namespace catl::shamap;

// Implementation of the TestItems methods
boost::intrusive_ptr<MmapItem>
TestItems::make(
    const std::string& hex_string,
    std::optional<std::string> hex_data)
{
    if (hex_string.length() < 64)
    {
        throw std::invalid_argument(
            "Hex string must be at least 64 characters");
    }

    // Convert key hex to bytes and store in buffers
    auto key_bytes = hex_to_vector(hex_string.substr(0, 64));
    buffers.push_back(key_bytes);
    const uint8_t* key_ptr = buffers.back().data();

    // Handle data
    const uint8_t* data_ptr;
    size_t data_size;

    if (hex_data.has_value() && !hex_data.value().empty())
    {
        // Use separate buffer for data
        auto data_bytes = hex_to_vector(hex_data.value());
        buffers.push_back(data_bytes);
        data_ptr = buffers.back().data();
        data_size = buffers.back().size();
    }
    else
    {
        // Reuse key as data
        data_ptr = key_ptr;
        data_size = key_bytes.size();
    }

    // Create and return the MmapItem
    return boost::intrusive_ptr<MmapItem>(
        new MmapItem(key_ptr, data_ptr, data_size));
}

void
TestItems::clear()
{
    buffers.clear();
}

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
