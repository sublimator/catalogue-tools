// test-utils.cpp
#include "catl/test-utils/test-utils.h"
#include "catl/core/types.h"
#include "catl/shamap/shamap-nodetype.h"
#include "catl/shamap/shamap-options.h"
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
