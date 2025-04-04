#include "hasher/core-types.h"

void
slice_hex(const Slice sl, std::string& result)
{
    static constexpr char hexChars[] = "0123456789ABCDEF";  // upper case
    result.reserve(sl.size() * 2);
    const uint8_t* bytes = sl.data();
    for (size_t i = 0; i < sl.size(); ++i)
    {
        uint8_t byte = bytes[i];
        result.push_back(hexChars[(byte >> 4) & 0xF]);
        result.push_back(hexChars[byte & 0xF]);
    }
}

std::string
Hash256::hex() const
{
    std::string result;
    slice_hex({data(), size()}, result);
    return result;
}

std::string
MmapItem::hex() const
{
    const auto sl = slice();
    std::string result;
    slice_hex(sl, result);
    return result;
}