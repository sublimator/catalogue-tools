#include "catl/v1/catl-v1-utils.h"
#include <cstring>
#include <gtest/gtest.h>
#include <iomanip>
#include <sstream>
#include <string>
#include <vector>

using catl::v1::Sha512Hasher;

// Helper to convert hash bytes to hex string
std::string
to_hex(const unsigned char* data, size_t len)
{
    std::ostringstream oss;
    for (size_t i = 0; i < len; ++i)
        oss << std::hex << std::setw(2) << std::setfill('0') << (int)data[i];
    return oss.str();
}

// Known SHA-512 test vectors
struct Sha512TestVector
{
    std::string input;
    std::string expected_hex;
};

const Sha512TestVector kVectors[] = {
    // Empty string
    {"",
     "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce"
     "47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e"},
    // "abc"
    {"abc",
     "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a"
     "2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"},
    // "The quick brown fox jumps over the lazy dog"
    {"The quick brown fox jumps over the lazy dog",
     "07e547d9586f6a73f73fbac0435ed76951218fb7d0c8d788a309d785436bbb64"
     "2e93a252a954f23912547d1e8a3b5ed6e1bfd7097821233fa0538f3db854fee6"}};

TEST(Sha512HasherTest, HashKnownVectors)
{
    for (const auto& vec : kVectors)
    {
        Sha512Hasher hasher;
        ASSERT_TRUE(hasher.update(vec.input.data(), vec.input.size()));
        unsigned char out[64];
        unsigned int out_len = 0;
        ASSERT_TRUE(hasher.final(out, &out_len));
        ASSERT_EQ(out_len, 64u);
        EXPECT_EQ(to_hex(out, 64), vec.expected_hex);
    }
}

TEST(Sha512HasherTest, MultipleUpdateCalls)
{
    Sha512Hasher hasher;
    std::string part1 = "The quick brown ";
    std::string part2 = "fox jumps over ";
    std::string part3 = "the lazy dog";
    ASSERT_TRUE(hasher.update(part1.data(), part1.size()));
    ASSERT_TRUE(hasher.update(part2.data(), part2.size()));
    ASSERT_TRUE(hasher.update(part3.data(), part3.size()));
    unsigned char out[64];
    unsigned int out_len = 0;
    ASSERT_TRUE(hasher.final(out, &out_len));
    EXPECT_EQ(
        to_hex(out, 64),
        "07e547d9586f6a73f73fbac0435ed76951218fb7d0c8d788a309d785436bbb64"
        "2e93a252a954f23912547d1e8a3b5ed6e1bfd7097821233fa0538f3db854fee6");
}

TEST(Sha512HasherTest, FinalCalledTwiceFails)
{
    Sha512Hasher hasher;
    ASSERT_TRUE(hasher.update("abc", 3));
    unsigned char out[64];
    unsigned int out_len = 0;
    ASSERT_TRUE(hasher.final(out, &out_len));
    // Second call should fail because the hasher is already finalized
    EXPECT_THROW(hasher.final(out, &out_len), std::runtime_error);
}

TEST(Sha512HasherTest, UpdateNullZeroLength)
{
    Sha512Hasher hasher;
    // Should be valid to hash zero-length input
    ASSERT_TRUE(hasher.update(nullptr, 0));
    unsigned char out[64];
    unsigned int out_len = 0;
    ASSERT_TRUE(hasher.final(out, &out_len));
    EXPECT_EQ(
        to_hex(out, 64),
        "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce"
        "47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e");
    EXPECT_EQ(out_len, 64u);
}