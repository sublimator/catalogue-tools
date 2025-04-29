#include "catl/crypto/sha512-half-hasher.h"
#include "catl/test-utils/test-utils.h"
#include <gtest/gtest.h>
#include <string>
#include <vector>

using namespace catl;

// Test vectors with input and expected first 256 bits of SHA-512 hash
struct TestVector
{
    std::string input;
    std::string expected_hash256_hex;
};

TEST(Sha512HalfHasher, BasicFunctionality)
{
    // Test vectors: input string and first 256 bits of SHA-512 hash as hex
    std::vector<TestVector> test_vectors = {
        // Empty string
        {"",
         "CF83E1357EEFB8BDF1542850D66D8007D620E4050B5715DC83F4A921D36CE9CE"},
        // "abc"
        {"abc",
         "DDAF35A193617ABACC417349AE20413112E6FA4E89A97EA20A9EEEE64B55D39A"},
        // "abcdefghijklmnopqrstuvwxyz"
        {"abcdefghijklmnopqrstuvwxyz",
         "4DBFF86CC2CA1BAE1E16468A05CB9881C97F1753BCE3619034898FAA1AABE429"}};

    for (const auto& vector : test_vectors)
    {
        crypto::Sha512HalfHasher hasher;
        hasher.update(vector.input.data(), vector.input.size());

        Hash256 result = hasher.finalize();
        std::string result_hex = result.hex();

        EXPECT_EQ(result_hex, vector.expected_hash256_hex)
            << "Failed for input: " << vector.input;
    }
}

TEST(Sha512HalfHasher, MultipleUpdates)
{
    // Test that multiple updates produce the same result as a single update
    std::string input = "The quick brown fox jumps over the lazy dog";

    // Single update
    crypto::Sha512HalfHasher single_hasher;
    single_hasher.update(input.data(), input.size());
    Hash256 single_result = single_hasher.finalize();

    // Multiple updates
    crypto::Sha512HalfHasher multi_hasher;
    size_t mid = input.size() / 2;
    multi_hasher.update(input.data(), mid);
    multi_hasher.update(input.data() + mid, input.size() - mid);
    Hash256 multi_result = multi_hasher.finalize();

    // Results should be identical
    EXPECT_EQ(single_result.hex(), multi_result.hex());
}

TEST(Sha512HalfHasher, ErrorHandling)
{
    // Test handling of error conditions
    crypto::Sha512HalfHasher hasher;

    // Finalize without updates should work
    Hash256 empty_result = hasher.finalize();
    EXPECT_EQ(
        empty_result.hex(),
        "CF83E1357EEFB8BDF1542850D66D8007D620E4050B5715DC83F4A921D36CE9CE");

    // Update after finalize should fail
    EXPECT_THROW(hasher.update("abc", 3), std::runtime_error);
}
