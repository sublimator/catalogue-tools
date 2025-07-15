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
    int64_t mantissa_;
    int32_t exponent_;

public:
    // Constructors
    STNumber() : mantissa_(0), exponent_(0) {}
    STNumber(int64_t mantissa, int32_t exponent) 
        : mantissa_(mantissa), exponent_(exponent) {}

    // Static factory method from byte array (big-endian)
    static STNumber
    from_bytes(const uint8_t* data)
    {
        // Read mantissa (8 bytes, big-endian, signed)
        uint64_t mantissa_unsigned = 0;
        for (int i = 0; i < 8; ++i)
        {
            mantissa_unsigned = (mantissa_unsigned << 8) | data[i];
        }
        
        // Interpret as signed 64-bit
        int64_t mantissa = static_cast<int64_t>(mantissa_unsigned);

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
    int64_t mantissa() const { return mantissa_; }
    int32_t exponent() const { return exponent_; }

    /**
     * Convert to human-readable string.
     * Format: mantissa * 10^exponent
     * 
     * Based on XRPL's Number::to_string implementation:
     * - Use scientific notation for very large/small exponents
     * - Otherwise format as decimal with proper decimal point placement
     */
    std::string
    to_string() const
    {
        if (mantissa_ == 0)
        {
            return "0";
        }

        // Use scientific notation for exponents that are too small or too large
        // (matching XRPL's logic: exponent < -25 or > -5)
        if (exponent_ != 0 && (exponent_ < -25 || exponent_ > -5))
        {
            std::string ret = std::to_string(mantissa_);
            ret.append(1, 'e');
            ret.append(std::to_string(exponent_));
            return ret;
        }

        // Handle sign
        bool negative = mantissa_ < 0;
        int64_t abs_mantissa = negative ? -mantissa_ : mantissa_;
        
        // For zero exponent, just return the mantissa
        if (exponent_ == 0)
        {
            return std::to_string(mantissa_);
        }
        
        // Convert mantissa to string
        std::string mantissa_str = std::to_string(abs_mantissa);
        
        // Calculate decimal point position
        int decimal_pos = static_cast<int>(mantissa_str.length()) + exponent_;
        
        std::string result;
        
        if (decimal_pos <= 0)
        {
            // Need leading zeros after decimal point
            result = "0.";
            for (int i = 0; i < -decimal_pos; ++i)
            {
                result += "0";
            }
            result += mantissa_str;
        }
        else if (decimal_pos < static_cast<int>(mantissa_str.length()))
        {
            // Insert decimal point within the number
            result = mantissa_str.substr(0, decimal_pos);
            result += ".";
            result += mantissa_str.substr(decimal_pos);
        }
        else
        {
            // Add trailing zeros (positive exponent)
            result = mantissa_str;
            for (int i = 0; i < exponent_; ++i)
            {
                result += "0";
            }
        }
        
        return negative ? "-" + result : result;
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