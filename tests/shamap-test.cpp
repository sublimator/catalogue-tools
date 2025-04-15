#include <gtest/gtest.h>
#include "catl/shamap/shamap.h"

TEST(ShaMapTest, Addition) {
    auto map = SHAMap();
    EXPECT_EQ(map.get_hash().hex(), "0000000000000000000000000000000000000000000000000000000000000000");
    // make a shamap item from a 32 byte array, and use that for the key and mmap item
    std::array<uint8_t, 32> keyData{};
    std::array<uint8_t, 32> dataData{};
    for (int i = 0; i < 32; i++) {
        keyData[i] = 0;
        dataData[i] = 0;
    }
    Key key((keyData.data()));
    auto item = boost::intrusive_ptr(new MmapItem(keyData.data(), dataData.data(), 32));
    map.add_item(item);
    EXPECT_EQ(map.get_hash().hex(), "B992A0C0480B32A2F32308EA2D64E85586A3DAF663F7B383806B5C4CEA84D8BF");
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}