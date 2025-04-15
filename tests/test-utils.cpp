#include "test-utils.h"

std::string TestDataPath::getPath(const std::string &relativePath) {
    // Get the directory of the current source file
    std::string sourceDir = CURRENT_SOURCE_DIR;

    // Combine with the relative path
    boost::filesystem::path fullPath = boost::filesystem::path(sourceDir) / relativePath;

    return fullPath.string();
}

std::pair<std::unique_ptr<uint8_t[]>, boost::intrusive_ptr<MmapItem>> getItemFromHex(const std::string &hexString) {
    if (hexString.length() < 64) {
        throw std::invalid_argument("Hex string must be at least 64 characters");
    }

    // Allocate memory that will persist after the function returns
    auto keyData = std::make_unique<uint8_t[]>(32);

    // Parse hex string into bytes for the key
    for (int i = 0; i < 32; i++) {
        std::string byteStr = hexString.substr(i * 2, 2);
        keyData[i] = static_cast<uint8_t>(std::stoi(byteStr, nullptr, 16));
    }

    // Create the MmapItem using the allocated memory
    auto item = boost::intrusive_ptr<MmapItem>(new MmapItem(keyData.get(), keyData.get(), 32));

    // Return both the data buffer and the item
    return std::make_pair(std::move(keyData), item);
}

boost::json::value loadJsonFromFile(const std::string &filePath) {
    std::ifstream file(filePath);
    if (!file.is_open()) {
        throw std::runtime_error("Could not open file: " + filePath);
    }

    std::stringstream buffer;
    buffer << file.rdbuf();
    std::string jsonStr = buffer.str();

    boost::system::error_code ec;
    boost::json::value json = boost::json::parse(jsonStr, ec);
    if (ec) {
        throw std::runtime_error("Failed to parse JSON: " + ec.message());
    }

    return json;
}

ShaMapFixture::ShaMapFixture() : map(tnACCOUNT_STATE) {
}

void ShaMapFixture::SetUp() {
    // Test data location relative to this source file
    fixtureDir = "fixture";

    // Verify empty map hash
    EXPECT_EQ(map.get_hash().hex(), "0000000000000000000000000000000000000000000000000000000000000000");
}

std::string ShaMapFixture::getFixturePath(const std::string &filename) {
    return TestDataPath::getPath(fixtureDir + "/" + filename);
}

void ShaMapFixture::addItemFromHex(const std::string &hexString) {
    auto [data, item] = getItemFromHex(hexString);
    map.add_item(item);
    buffers.push_back(std::move(data)); // Keep buffer alive
}

void ShaMapFixture::removeItemFromHex(const std::string &hexString) {
    auto [data, item] = getItemFromHex(hexString);
    map.remove_item(item->key());
    buffers.push_back(std::move(data)); // Keep buffer alive
}