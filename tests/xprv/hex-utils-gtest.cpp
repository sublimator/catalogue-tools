#include "xprv/hex-utils.h"

#include <gtest/gtest.h>

TEST(HexUtils, RejectsMalformedHashHex)
{
    EXPECT_THROW(xprv::hash_from_hex(std::string(63, '0')), std::runtime_error);
    EXPECT_THROW(xprv::hash_from_hex(std::string(64, 'Z')), std::runtime_error);
}

TEST(HexUtils, RejectsMalformedByteHex)
{
    EXPECT_THROW(xprv::from_hex("0"), std::runtime_error);
    EXPECT_THROW(xprv::from_hex("GG"), std::runtime_error);
}
