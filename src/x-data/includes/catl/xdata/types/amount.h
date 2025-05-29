#pragma once

#include "catl/core/types.h"  // for Slice
#include <cstddef>
#include <cstdint>
#include <cstring>  // for memcpy

namespace catl::xdata {

// XRP/XAH currency is represented as 20 zero bytes
static constexpr uint8_t NATIVE_CURRENCY[20] = {0};

// Get size for Amount type by peeking at first byte
inline size_t
get_amount_size(uint8_t first_byte)
{
    bool is_iou = (first_byte & 0x80) != 0;
    return is_iou ? 48 : 8;  // IOU: 8 + 20 + 20, NATIVE(XRP/XAH): just 8
}

// Check if an amount is native (XRP/XAH) or IOU
inline bool
is_native_amount(const Slice& amount)
{
    return amount.size() == 8 && (amount.data()[0] & 0x80) == 0;
}

// Get issuer for IOU amounts (returns empty slice for native (XRP/XAH))
// IOU format: [8 bytes amount][20 bytes currency][20 bytes issuer]
inline Slice
get_issuer(const Slice& amount)
{
    // Check if it's an IOU (first bit set)
    if (amount.size() < 48 || (amount.data()[0] & 0x80) == 0)
    {
        throw std::runtime_error("Amount is not an IOU");
    }

    // Issuer is the last 20 bytes
    return {amount.data() + 28, 20};
}

// Get currency code for amounts (XRP/XAH or standard 3-char format)
// out_code must have at least 3 bytes available
// Returns true if it's a standard currency (XRP/XAH or 3-char code)
// Returns false for non-standard currencies
inline bool
get_currency_code(
    const Slice& amount,
    char* out_code,
    const char* native_code = "XRP")
{
    // Check if we have valid amount data
    if (amount.size() < 8)
    {
        return false;  // Invalid amount
    }

    // Check if it's native XRP/XAH
    if ((amount.data()[0] & 0x80) == 0)
    {
        std::memcpy(out_code, native_code, 3);
        return true;
    }

    // It's an IOU - check if we have enough data
    if (amount.size() < 48)
    {
        return false;  // Invalid IOU
    }

    // Currency is 20 bytes starting at offset 8
    const uint8_t* currency = amount.data() + 8;

    // Check if it's a standard currency (first 12 bytes are 0, last 5 are 0)
    // Standard format: 12 zeros, 3 ASCII chars, 5 zeros
    for (size_t i = 0; i < 12; ++i)
    {
        if (currency[i] != 0)
            return false;
    }
    for (size_t i = 15; i < 20; ++i)
    {
        if (currency[i] != 0)
            return false;
    }

    // Copy the 3-character code (bytes 12-14)
    std::memcpy(out_code, currency + 12, 3);

    // Verify they're printable ASCII (optional safety check)
    for (int i = 0; i < 3; ++i)
    {
        if (out_code[i] < 32 || out_code[i] > 126)
        {
            return false;
        }
    }

    return true;
}

// Get the raw 20-byte currency field (for all currency types)
// Returns NATIVE_CURRENCY (20 zeros) for XRP/XAH amounts
inline Slice
get_currency_raw(const Slice& amount)
{
    // Check if it's an IOU
    if (amount.size() < 48 || (amount.data()[0] & 0x80) == 0)
    {
        // Return Native (XRP/XAH) currency (20 zeros)
        return {NATIVE_CURRENCY, 20};
    }

    // Currency is 20 bytes starting at offset 8
    return {amount.data() + 8, 20};
}

}  // namespace catl::xdata
