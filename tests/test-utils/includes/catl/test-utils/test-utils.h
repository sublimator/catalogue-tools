#pragma once

#include <boost/filesystem.hpp>
#include <boost/json.hpp>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

// Define this macro to get the directory of the current source file
#define CURRENT_SOURCE_DIR \
    std::string(__FILE__).substr(0, std::string(__FILE__).find_last_of("/\\"))

// Helper class to manage file paths relative to the source file
class TestDataPath
{
public:
    static std::string
    get_path(const std::string& relative_path);
};

// Convert hex string to byte vector
std::vector<uint8_t>
hex_to_vector(const std::string& hex_string);

// JSON loading helper
boost::json::value
load_json_from_file(const std::string& file_path);
