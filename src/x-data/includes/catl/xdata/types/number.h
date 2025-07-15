#pragma once

#include "catl/core/types.h"  // for Slice
#include "catl/xdata/slice-cursor.h"
#include <cstddef>
#include <cstdint>
#include <string>
#include <sstream>
#include <iomanip>

namespace catl::xdata {

/**
 * STNumber representation for XRPL.
 * 
 * STNumber format (12 bytes total):
 * - Mantissa: 64 bits (8 bytes) - stored first
 * - Exponent: 32 bits (4 bytes) - stored after mantissa
 * 
 * This matches the XRPL serialization where mantissa comes before exponent.
 */
class STNumber
{
private:
    uint64_t mantissa_;
    int32_t exponent_;

public:
    // Constructors
    STNumber() : mantissa_(0), exponent_(0) {}
    STNumber(uint64_t mantissa, int32_t exponent) 
        : mantissa_(mantissa), exponent_(exponent) {}

    // Static factory method from byte array (big-endian)
    static STNumber
    from_bytes(const uint8_t* data)
    {
        // Read mantissa (8 bytes, big-endian)
        uint64_t mantissa = 0;
        for (int i = 0; i < 8; ++i)
        {
            mantissa = (mantissa << 8) | data[i];
        }

        // Read exponent (4 bytes, big-endian)
        uint32_t exp_unsigned = 0;
        for (int i = 0; i < 4; ++i)
        {
            exp_unsigned = (exp_unsigned << 8) | data[8 + i];
        }
        
        // Interpret as signed 32-bit
        int32_t exponent = static_cast<int32_t>(exp_unsigned);

        return STNumber(mantissa, exponent);
    }

    // Getters
    uint64_t mantissa() const { return mantissa_; }
    int32_t exponent() const { return exponent_; }

    /**
     * Convert to human-readable string.
     * Format: mantissa * 10^exponent
     */
    std::string
    to_string() const
    {
        if (mantissa_ == 0)
        {
            return "0";
        }

        std::stringstream ss;
        
        // For simple cases where exponent is 0
        if (exponent_ == 0)
        {
            ss << mantissa_;
            return ss.str();
        }
        
        // For positive exponents, multiply by 10^exponent
        if (exponent_ > 0)
        {
            ss << mantissa_;
            for (int i = 0; i < exponent_; ++i)
            {
                ss << "0";
            }
            return ss.str();
        }
        
        // For negative exponents, we need to add decimal point
        std::string mantissa_str = std::to_string(mantissa_);
        int mantissa_len = mantissa_str.length();
        int decimal_pos = mantissa_len + exponent_;
        
        if (decimal_pos <= 0)
        {
            // Need leading zeros after decimal point
            ss << "0.";
            for (int i = 0; i < -decimal_pos; ++i)
            {
                ss << "0";
            }
            ss << mantissa_str;
        }
        else if (decimal_pos < mantissa_len)
        {
            // Insert decimal point within the number
            ss << mantissa_str.substr(0, decimal_pos);
            ss << ".";
            ss << mantissa_str.substr(decimal_pos);
        }
        else
        {
            // This shouldn't happen with negative exponent, but handle it
            ss << mantissa_str;
        }
        
        return ss.str();
    }

    /**
     * Convert to JSON-compatible representation.
     * Returns object with mantissa and exponent fields.
     */
    std::string
    to_json_string() const
    {
        std::stringstream ss;
        ss << "{\"mantissa\":\"" << mantissa_ << "\",\"exponent\":" << exponent_ << "}";
        return ss.str();
    }
};

// Get size for STNumber type (always 12 bytes)
inline constexpr size_t
get_number_size()
{
    return 12;  // 8 bytes mantissa + 4 bytes exponent
}

// Helper to get STNumber size from cursor (for consistency with other types)
inline size_t
get_number_size(SliceCursor& cursor)
{
    (void)cursor;  // Unused, STNumber is always 12 bytes
    return 12;
}

/**
 * Parse STNumber from data.
 *
 * @param data Slice containing exactly 12 bytes
 * @return Parsed STNumber
 * @throws std::runtime_error if data size is not 12 bytes
 */
inline STNumber
parse_number(const Slice& data)
{
    if (data.size() != 12)
    {
        throw std::runtime_error(
            "Invalid STNumber size: expected 12 bytes, got " +
            std::to_string(data.size()));
    }

    return STNumber::from_bytes(data.data());
}

/**
 * Get human-readable STNumber string from data.
 * Convenience function that combines parsing and formatting.
 *
 * @param data Slice containing exactly 12 bytes
 * @return Human-readable string
 * @throws std::runtime_error if data size is not 12 bytes
 */
inline std::string
get_number_string(const Slice& data)
{
    STNumber number = parse_number(data);
    return number.to_string();
}

}  // namespace catl::xdata