#include "catl/core/base64.h"

namespace catl {

// clang-format off
static const uint8_t DECODE_TABLE[256] = {
    64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,
    64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,
    64,64,64,64,64,64,64,64,64,64,64,62,64,64,64,63,
    52,53,54,55,56,57,58,59,60,61,64,64,64,64,64,64,
    64, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14,
    15,16,17,18,19,20,21,22,23,24,25,64,64,64,64,64,
    64,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,
    41,42,43,44,45,46,47,48,49,50,51,64,64,64,64,64,
    64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,
    64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,
    64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,
    64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,
    64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,
    64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,
    64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,
    64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,64
};

static constexpr char ENCODE_TABLE[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
// clang-format on

std::vector<uint8_t>
base64_decode(std::string_view input)
{
    std::vector<uint8_t> result;
    result.reserve(input.size() * 3 / 4);

    uint32_t accum = 0;
    int bits = 0;
    for (char c : input)
    {
        uint8_t val = DECODE_TABLE[static_cast<uint8_t>(c)];
        if (val >= 64)
            continue;
        accum = (accum << 6) | val;
        bits += 6;
        if (bits >= 8)
        {
            bits -= 8;
            result.push_back(static_cast<uint8_t>((accum >> bits) & 0xFF));
        }
    }
    return result;
}

std::string
base64_encode(std::span<const uint8_t> input)
{
    std::string result;
    result.reserve(((input.size() + 2) / 3) * 4);

    size_t i = 0;
    while (i + 2 < input.size())
    {
        uint32_t n = (static_cast<uint32_t>(input[i]) << 16) |
            (static_cast<uint32_t>(input[i + 1]) << 8) |
            static_cast<uint32_t>(input[i + 2]);
        result += ENCODE_TABLE[(n >> 18) & 0x3F];
        result += ENCODE_TABLE[(n >> 12) & 0x3F];
        result += ENCODE_TABLE[(n >> 6) & 0x3F];
        result += ENCODE_TABLE[n & 0x3F];
        i += 3;
    }

    if (i + 1 == input.size())
    {
        uint32_t n = static_cast<uint32_t>(input[i]) << 16;
        result += ENCODE_TABLE[(n >> 18) & 0x3F];
        result += ENCODE_TABLE[(n >> 12) & 0x3F];
        result += '=';
        result += '=';
    }
    else if (i + 2 == input.size())
    {
        uint32_t n = (static_cast<uint32_t>(input[i]) << 16) |
            (static_cast<uint32_t>(input[i + 1]) << 8);
        result += ENCODE_TABLE[(n >> 18) & 0x3F];
        result += ENCODE_TABLE[(n >> 12) & 0x3F];
        result += ENCODE_TABLE[(n >> 6) & 0x3F];
        result += '=';
    }

    return result;
}

}  // namespace catl
