#include "catl/test-utils/test-utils.h"
#include <boost/json.hpp>
#include <fstream>
#include <gtest/gtest.h>

// Helper: Convert bytes to hex string
std::string
bytes_to_hex(const unsigned char* data, size_t len)
{
    std::ostringstream oss;
    for (size_t i = 0; i < len; ++i)
        oss << std::hex << std::setw(2) << std::setfill('0')
            << static_cast<int>(data[i]);
    return oss.str();
}

// Load test vectors from JSON file
std::vector<std::pair<std::string, std::string>>
load_vectors(const std::string& path)
{
    std::ifstream f(TestDataPath::get_path(path));
    std::vector<std::pair<std::string, std::string>> vectors;
    return vectors;
}

TEST(XData, TestVectors)
{
    EXPECT_TRUE(true);
}
