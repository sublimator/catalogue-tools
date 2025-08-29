// test-utils.cpp
#include "catl/test-utils/test-utils.h"
#include "catl/core/types.h"
#include "catl/test-utils/test-mmap-items.h"
#include <boost/filesystem/path.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/value.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/system/detail/error_code.hpp>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <gtest/gtest.h>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

std::string
TestDataPath::get_path(const std::string& relative_path)
{
    // Use PROJECT_SOURCE_DIR macro set by CMake for project root
    boost::filesystem::path full_path =
        boost::filesystem::path(PROJECT_ROOT) / "tests" / relative_path;
    return full_path.string();
}

boost::json::value
load_json_from_file(const std::string& file_path)
{
    std::ifstream file(file_path);
    if (!file.is_open())
    {
        throw std::runtime_error("Could not open file: " + file_path);
    }

    std::stringstream buffer;
    buffer << file.rdbuf();
    std::string json_str = buffer.str();

    boost::system::error_code ec;
    boost::json::value json = boost::json::parse(json_str, ec);
    if (ec)
    {
        throw std::runtime_error("Failed to parse JSON: " + ec.message());
    }

    return json;
}

// Convert hex string to byte vector
std::vector<uint8_t>
hex_to_vector(const std::string& hex_string)
{
    if (hex_string.empty())
    {
        return {};
    }

    if (hex_string.length() % 2 != 0)
    {
        throw std::invalid_argument("Hex string must have even length");
    }

    std::vector<uint8_t> result;
    result.reserve(hex_string.length() / 2);

    for (size_t i = 0; i < hex_string.length(); i += 2)
    {
        std::string byte_str = hex_string.substr(i, 2);
        uint8_t byte = static_cast<uint8_t>(std::stoi(byte_str, nullptr, 16));
        result.push_back(byte);
    }

    return result;
}

boost::intrusive_ptr<MmapItem>
TestMmapItems::make(
    const std::string& hex_string,
    const std::optional<std::string>& hex_data)
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
    auto item =
        boost::intrusive_ptr(new MmapItem(key_ptr, data_ptr, data_size));
    test_items.push_back(item);
    return item;
}

void
TestMmapItems::clear()
{
    buffers.clear();
}
