#include "test-utils.h"

std::string TestDataPath::getPath(const std::string &relativePath) {
    // Get the directory of the current source file
    std::string sourceDir = CURRENT_SOURCE_DIR;

    // Combine with the relative path
    boost::filesystem::path fullPath = boost::filesystem::path(sourceDir) / relativePath;

    return fullPath.string();
}

// TODO: make this an object and encapsulate the buffers and cleanup in the destructor
std::pair<std::vector<std::shared_ptr<uint8_t[]> >, boost::intrusive_ptr<MmapItem> >
getItemFromHex(const std::string &hexString, std::optional<std::string> hexData) {
    if (hexString.length() < 64) {
        throw std::invalid_argument("Hex string must be at least 64 characters");
    }

    std::vector<std::shared_ptr<uint8_t[]> > buffers;

    // Allocate memory for the key and add to buffers
    auto key_buffer = std::make_shared<uint8_t[]>(32);
    buffers.push_back(key_buffer);

    // Parse hex string into bytes for the key
    for (int i = 0; i < 32; i++) {
        std::string byteStr = hexString.substr(i * 2, 2);
        key_buffer[i] = static_cast<uint8_t>(std::stoi(byteStr, nullptr, 16));
    }

    // Pointer to data and its size
    uint8_t *data_ptr;
    size_t data_size;

    // Handle data if provided, otherwise use key as data
    if (hexData.has_value() && !hexData.value().empty()) {
        const std::string &dataStr = hexData.value();
        data_size = dataStr.length() / 2; // Two hex chars = 1 byte

        // Allocate memory for data and add to buffers
        auto data_buffer = std::make_shared<uint8_t[]>(data_size);
        buffers.push_back(data_buffer);

        // Parse hex string into bytes for the data
        for (size_t i = 0; i < data_size; i++) {
            std::string byteStr = dataStr.substr(i * 2, 2);
            data_buffer[i] = static_cast<uint8_t>(std::stoi(byteStr, nullptr, 16));
        }

        data_ptr = data_buffer.get();
    } else {
        // If no data provided, use key as data
        data_ptr = key_buffer.get();
        data_size = 32;
    }

    // Create the MmapItem using the key and data
    auto item = boost::intrusive_ptr<MmapItem>(new MmapItem(key_buffer.get(), data_ptr, data_size));

    // Return both the vector of buffers and the item
    return std::make_pair(std::move(buffers), item);
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

template<SHAMapNodeType NodeType>
ShaMapFixture<NodeType>::ShaMapFixture() : map(NodeType) {
}

template<SHAMapNodeType NodeType>
void ShaMapFixture<NodeType>::SetUp() {
    // Test data location relative to this source file
    fixtureDir = "fixture";

    // Verify empty map hash
    EXPECT_EQ(map.get_hash().hex(), "0000000000000000000000000000000000000000000000000000000000000000");
}

template<SHAMapNodeType NodeType>
std::string ShaMapFixture<NodeType>::getFixturePath(const std::string &filename) const {
    return TestDataPath::getPath(fixtureDir + "/" + filename);
}

template<SHAMapNodeType NodeType>
SetResult ShaMapFixture<NodeType>::addItemFromHex(const std::string &hexString, std::optional<std::string> hexData) {
    auto [data, item] = getItemFromHex(hexString, std::move(hexData));
    std::ranges::copy(data, std::back_inserter(buffers));
    return map.set_item(item);
}

template<SHAMapNodeType NodeType>
bool ShaMapFixture<NodeType>::removeItemFromHex(const std::string &hexString) {
    auto [data, item] = getItemFromHex(hexString);
    std::ranges::copy(data, std::back_inserter(buffers));
    return map.remove_item(item->key());
}


template class ShaMapFixture<tnACCOUNT_STATE>;
template class ShaMapFixture<tnTRANSACTION_MD>;
