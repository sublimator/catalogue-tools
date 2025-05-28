#include "catl/base58/base58.h"
#include <gtest/gtest.h>
#include <vector>

TEST(Base58, EncodeMasterSeed)
{
    // Test vector from XRPL Java implementation
    const std::string expected_master_seed = "snoPBrXtMeMyMHUVTgbuqAfg1SUTb";
    const std::vector<uint8_t> master_seed_bytes = {
        0xde,
        0xdc,
        0xe9,
        0xce,
        0x67,
        0xb4,
        0x51,
        0xd8,
        0x52,
        0xfd,
        0x4e,
        0x84,
        0x6f,
        0xcd,
        0xe3,
        0x1c};

    // Encode as seed
    std::string encoded = catl::base58::encode_seed_k256(master_seed_bytes);
    EXPECT_EQ(encoded, expected_master_seed);
}

TEST(Base58, DecodeMasterSeed)
{
    const std::string master_seed = "snoPBrXtMeMyMHUVTgbuqAfg1SUTb";
    const std::vector<uint8_t> expected_bytes = {
        0xde,
        0xdc,
        0xe9,
        0xce,
        0x67,
        0xb4,
        0x51,
        0xd8,
        0x52,
        0xfd,
        0x4e,
        0x84,
        0x6f,
        0xcd,
        0xe3,
        0x1c};

    // Decode the seed
    auto decoded = catl::base58::decode_seed(master_seed);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->version_name, "seed_k256");
    EXPECT_EQ(decoded->payload, expected_bytes);
}

TEST(Base58, BasicEncodeDecode)
{
    // Test basic encoding/decoding without checksum
    const std::vector<uint8_t> test_data = {0x01, 0x02, 0x03, 0x04, 0x05};

    std::string encoded = catl::base58::xrpl_codec.encode(test_data);
    auto decoded = catl::base58::xrpl_codec.decode(encoded);

    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, test_data);
}

TEST(Base58, ChecksummedEncodeDecode)
{
    // Test checksummed encoding/decoding
    const std::vector<uint8_t> test_data = {0xAB, 0xCD, 0xEF, 0x12, 0x34};

    std::string encoded = catl::base58::xrpl_codec.encode_checked(test_data);
    auto decoded = catl::base58::xrpl_codec.decode_checked(encoded);

    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, test_data);
}

TEST(Base58, InvalidChecksum)
{
    const std::vector<uint8_t> test_data = {0xAB, 0xCD, 0xEF};
    std::string encoded = catl::base58::xrpl_codec.encode_checked(test_data);

    // Corrupt the checksum by changing last character
    encoded.back() = (encoded.back() == 'r') ? 's' : 'r';

    auto decoded = catl::base58::xrpl_codec.decode_checked(encoded);
    EXPECT_FALSE(decoded.has_value());
}

TEST(Base58, AccountIDEncodeDecode)
{
    // 20 bytes for account ID
    const std::vector<uint8_t> account_bytes(20, 0x42);

    std::string encoded = catl::base58::encode_account_id(account_bytes);
    EXPECT_TRUE(encoded.starts_with('r'));  // XRPL accounts start with 'r'

    auto decoded = catl::base58::decode_account_id(encoded);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, account_bytes);
}

TEST(Base58, NodePublicEncodeDecode)
{
    // 33 bytes for node public key
    const std::vector<uint8_t> node_pub_bytes(33, 0x33);

    std::string encoded = catl::base58::encode_node_public(node_pub_bytes);
    EXPECT_TRUE(encoded.starts_with('n'));  // Node public keys start with 'n'

    auto decoded = catl::base58::decode_node_public(encoded);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, node_pub_bytes);
}

TEST(Base58, LeadingZeros)
{
    // Test preservation of leading zeros
    const std::vector<uint8_t> test_data = {0x00, 0x00, 0x00, 0xAB, 0xCD};

    std::string encoded = catl::base58::xrpl_codec.encode(test_data);
    auto decoded = catl::base58::xrpl_codec.decode(encoded);

    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, test_data);
    EXPECT_EQ(decoded->size(), test_data.size());
}

TEST(Base58, EmptyData)
{
    const std::vector<uint8_t> empty_data;

    std::string encoded = catl::base58::xrpl_codec.encode(empty_data);
    EXPECT_EQ(encoded, "");

    auto decoded = catl::base58::xrpl_codec.decode("");
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->size(), 0);
}

TEST(Base58, InvalidCharacters)
{
    // XRPL alphabet doesn't contain '0', 'O', 'I', 'l'
    auto decoded1 = catl::base58::xrpl_codec.decode("0invalid");
    EXPECT_FALSE(decoded1.has_value());

    auto decoded2 = catl::base58::xrpl_codec.decode("Oinvalid");
    EXPECT_FALSE(decoded2.has_value());

    auto decoded3 = catl::base58::xrpl_codec.decode("Iinvalid");
    EXPECT_FALSE(decoded3.has_value());

    auto decoded4 = catl::base58::xrpl_codec.decode("linvalid");
    EXPECT_FALSE(decoded4.has_value());
}

TEST(Base58, WrongVersionLength)
{
    // Try to encode 10 bytes as account ID (expects 20)
    const std::vector<uint8_t> wrong_size(10, 0x55);

    EXPECT_THROW(
        catl::base58::encode_account_id(wrong_size), std::invalid_argument);
}

TEST(Base58, ED25519Seed)
{
    // Test ED25519 seed encoding/decoding
    const std::vector<uint8_t> ed_seed_bytes(16, 0xED);

    // Can't use encode_seed_k256 for ED25519, need to use the codec directly
    std::string encoded = catl::base58::xrpl_codec.encode_versioned(
        ed_seed_bytes, catl::base58::SEED_ED25519);

    auto decoded = catl::base58::decode_seed(encoded);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->version_name, "seed_ed25519");
    EXPECT_EQ(decoded->payload, ed_seed_bytes);
}