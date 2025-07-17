#pragma once

#include "catl/core/types.h"  // for Slice
#include "catl/xdata/slice-cursor.h"
#include <cstddef>
#include <cstdint>
#include <cstring>  // for memcmp

namespace catl::xdata {

// XRP/Native currency is represented as 20 zero bytes
static constexpr uint8_t XRP_CURRENCY[20] = {0};

// Check if a 20-byte currency is XRP (all zeros)
inline bool
is_xrp_currency(const uint8_t* currency_data)
{
    return std::memcmp(currency_data, XRP_CURRENCY, 20) == 0;
}

// Check if a currency slice is XRP
inline bool
is_xrp_currency(const Slice& currency)
{
    return currency.size() == 20 && is_xrp_currency(currency.data());
}

// Get the size of an Issue field by peeking at the currency
// Returns 20 for XRP (just currency), 40 for non-XRP (currency + issuer)
inline size_t
get_issue_size(SliceCursor& cursor)
{
    // Ensure we have at least 20 bytes for currency
    if (cursor.remaining_size() < 20)
    {
        throw SliceCursorError("Not enough data for Issue currency field");
    }

    // Peek at the currency field without advancing
    const uint8_t* currency_data = cursor.data.data() + cursor.pos;

    // Check if it's XRP (all zeros)
    if (is_xrp_currency(currency_data))
    {
        return 20;  // Just currency, no issuer
    }
    else
    {
        // Ensure we have 40 bytes total for currency + issuer
        if (cursor.remaining_size() < 40)
        {
            throw SliceCursorError("Not enough data for Issue with issuer");
        }
        return 40;  // Currency + issuer
    }
}

// Parse an Issue and return currency and issuer slices
// For XRP: currency = 20 zeros, issuer = empty slice
// For non-XRP: currency = 20 bytes, issuer = 20 bytes
struct ParsedIssue
{
    Slice currency;
    Slice issuer;

    bool
    is_native() const
    {
        return issuer.empty();
    }
};

inline ParsedIssue
parse_issue(const Slice& issue_data)
{
    if (issue_data.size() < 20)
    {
        throw std::runtime_error("Issue data too small");
    }

    Slice currency{issue_data.data(), 20};

    if (is_xrp_currency(currency))
    {
        // XRP - no issuer
        return {currency, Slice{}};
    }
    else
    {
        // Non-XRP - must have issuer
        if (issue_data.size() < 40)
        {
            throw std::runtime_error("Non-XRP Issue missing issuer");
        }
        Slice issuer{issue_data.data() + 20, 20};
        return {currency, issuer};
    }
}

// Get currency code for Issue (XRP or standard 3-char format)
// out_code must have at least 3 bytes available
// Returns true if it's a standard currency (XRP or 3-char code)
// Returns false for non-standard currencies
inline bool
get_issue_currency_code(
    const Slice& issue_data,
    char* out_code,
    const char* native_code = "XRP")
{
    if (issue_data.size() < 20)
    {
        return false;  // Invalid issue
    }

    const uint8_t* currency = issue_data.data();

    // Check if it's XRP (all zeros)
    if (is_xrp_currency(currency))
    {
        std::memcpy(out_code, native_code, 3);
        return true;
    }

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

    // Verify they're printable ASCII
    for (int i = 0; i < 3; ++i)
    {
        if (out_code[i] < 32 || out_code[i] > 126)
        {
            return false;
        }
    }

    return true;
}

}  // namespace catl::xdata