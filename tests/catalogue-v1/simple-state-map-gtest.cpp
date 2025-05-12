#include "catl/core/types.h"
#include "catl/v1/catl-v1-simple-state-map.h"
#include <gtest/gtest.h>
#include <vector>

using namespace catl;
using namespace catl::v1;

class SimpleStateMapTest : public ::testing::Test
{
protected:
    SimpleStateMap map;
};

TEST_F(SimpleStateMapTest, EmptyMap)
{
    EXPECT_TRUE(map.empty());
    EXPECT_EQ(map.size(), 0);
}

TEST_F(SimpleStateMapTest, AddAndRetrieveItem)
{
    // Create a key
    std::array<uint8_t, 32> keyData{};
    keyData[0] = 0x01;
    keyData[1] = 0x02;
    Hash256 key(keyData);

    // Create some data
    std::vector<uint8_t> data = {0x10, 0x20, 0x30, 0x40, 0x50};

    // Add to map
    bool added = map.set_item(key, data);
    EXPECT_TRUE(added);
    EXPECT_EQ(map.size(), 1);
    EXPECT_FALSE(map.empty());

    // Check if it exists
    EXPECT_TRUE(map.contains(key));

    // Retrieve the data
    const auto& retrieved = map.get_item(key);
    EXPECT_EQ(retrieved.size(), data.size());
    EXPECT_EQ(retrieved, data);
}

TEST_F(SimpleStateMapTest, UpdateItem)
{
    // Create a key
    std::array<uint8_t, 32> keyData{};
    keyData[0] = 0x01;
    Hash256 key(keyData);

    // Add initial data
    std::vector<uint8_t> data1 = {0x01, 0x02, 0x03};
    bool added = map.set_item(key, data1);
    EXPECT_TRUE(added);

    // Update with new data
    std::vector<uint8_t> data2 = {0x04, 0x05, 0x06, 0x07};
    bool updated = map.set_item(key, data2);
    EXPECT_FALSE(updated);  // Not a new item, but an update

    // Verify the update
    const auto& retrieved = map.get_item(key);
    EXPECT_EQ(retrieved, data2);
}

TEST_F(SimpleStateMapTest, RemoveItem)
{
    // Create a key
    std::array<uint8_t, 32> keyData{};
    keyData[0] = 0x01;
    Hash256 key(keyData);

    // Add initial data
    std::vector<uint8_t> data = {0x01, 0x02, 0x03};
    map.set_item(key, data);

    // Remove it
    bool removed = map.remove_item(key);
    EXPECT_TRUE(removed);
    EXPECT_TRUE(map.empty());
    EXPECT_EQ(map.size(), 0);
    EXPECT_FALSE(map.contains(key));

    // Try to remove again
    bool removed2 = map.remove_item(key);
    EXPECT_FALSE(removed2);  // Already removed
}

TEST_F(SimpleStateMapTest, VisitItems)
{
    // Add several items
    for (int i = 0; i < 5; i++)
    {
        std::array<uint8_t, 32> keyData{};
        keyData[0] = static_cast<uint8_t>(i);
        Hash256 key(keyData);

        std::vector<uint8_t> data;
        data.push_back(static_cast<uint8_t>(i * 10));
        map.set_item(key, data);
    }

    EXPECT_EQ(map.size(), 5);

    // Visit all items
    int count = 0;
    map.visit_items([&](const Hash256& key, const std::vector<uint8_t>& data) {
        EXPECT_EQ(key.data()[0], count);
        EXPECT_EQ(data[0], count * 10);
        count++;
    });

    EXPECT_EQ(count, 5);
}

TEST_F(SimpleStateMapTest, KeyNotFound)
{
    // Create a key not in the map
    std::array<uint8_t, 32> keyData{};
    keyData[0] = 0xFF;
    Hash256 key(keyData);

    // Expect exception when trying to retrieve
    EXPECT_THROW(map.get_item(key), std::out_of_range);
}

TEST_F(SimpleStateMapTest, ClearMap)
{
    // Add some items
    for (int i = 0; i < 3; i++)
    {
        std::array<uint8_t, 32> keyData{};
        keyData[0] = static_cast<uint8_t>(i);
        Hash256 key(keyData);

        std::vector<uint8_t> data{static_cast<uint8_t>(i)};
        map.set_item(key, data);
    }

    EXPECT_EQ(map.size(), 3);

    // Clear the map
    map.clear();
    EXPECT_TRUE(map.empty());
    EXPECT_EQ(map.size(), 0);
}

TEST_F(SimpleStateMapTest, WriteToStream)
{
    // Add several items to the map
    for (int i = 0; i < 5; i++)
    {
        std::array<uint8_t, 32> keyData{};
        keyData[0] = static_cast<uint8_t>(i);
        Hash256 key(keyData);

        std::vector<uint8_t> data;
        data.push_back(static_cast<uint8_t>(i * 10));
        map.set_item(key, data);
    }

    EXPECT_EQ(map.size(), 5);

    // Write to a memory stream
    std::stringstream stream;
    size_t bytes_written = catl::v1::write_map_to_stream(map, stream);

    // Expected size calculation:
    // 5 items × (1 byte for node type + 32 bytes for key + 4 bytes for length +
    // 1 byte for data)
    // + 1 byte for terminal marker
    // = 5 × 38 + 1 = 191 bytes
    EXPECT_EQ(bytes_written, 191);
    EXPECT_EQ(stream.str().size(), 191);
}