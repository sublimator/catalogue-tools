#include "catl/crypto/sha512-hasher.h"
#include "catl/test-utils/test-utils.h"
#include <boost/json.hpp>
#include <fstream>
#include <gtest/gtest.h>
#include <iomanip>
#include <openssl/evp.h>
#include <sstream>

// Helper: Convert bytes to hex string
std::string
bytes_to_hex(const unsigned char* data, size_t len)
{
    std::ostringstream oss;
    for (size_t i = 0; i < len; ++i)
        oss << std::hex << std::setw(2) << std::setfill('0') << (int)data[i];
    return oss.str();
}

// Load test vectors from JSON file
std::vector<std::pair<std::string, std::string>>
load_vectors(const std::string& path)
{
    std::ifstream f(TestDataPath::get_path(path));
    std::stringstream buffer;
    buffer << f.rdbuf();
    boost::json::value jv = boost::json::parse(buffer.str());
    const auto& arr = jv.as_array();
    std::vector<std::pair<std::string, std::string>> vectors;
    for (const auto& item : arr)
    {
        const auto& obj = item.as_object();
        vectors.emplace_back(
            obj.at("input").as_string().c_str(),
            obj.at("output").as_string().c_str());
    }
    return vectors;
}

TEST(Sha512Hasher, TestVectors)
{
    auto vectors = load_vectors("crypto/fixture/sha512-test-vectors.json");
    for (const auto& [input, expected_hex] : vectors)
    {
        catl::crypto::Sha512Hasher hasher;
        hasher.update(input.data(), input.size());
        unsigned char out[EVP_MAX_MD_SIZE];
        unsigned int out_len = 0;
        hasher.final(out, &out_len);
        std::string actual_hex = bytes_to_hex(out, out_len);
        // Remove spaces from expected_hex (in case of formatting)
        std::string expected = expected_hex;
        expected.erase(
            remove_if(expected.begin(), expected.end(), ::isspace),
            expected.end());
        EXPECT_EQ(actual_hex, expected) << "Input: " << input;
    }
}