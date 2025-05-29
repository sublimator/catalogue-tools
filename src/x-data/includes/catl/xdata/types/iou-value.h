#pragma once

#include "catl/core/types.h"  // For Slice
#include <cmath>
#include <cstdint>
#include <sstream>
#include <stdexcept>
#include <string>

namespace catl::xdata {

/**
 * Exception thrown when parsing invalid IOU amount data
 */
class IOUParseError : public std::runtime_error
{
public:
    explicit IOUParseError(const std::string& msg)
        : std::runtime_error("IOUParseError: " + msg)
    {
    }
};

/**
 * IOU amount representation using 8 bytes.
 *
 * IOU format (8 bytes, big-endian):
 * - Bit 63: 1 (indicates IOU, not native)
 * - Bit 62: sign (1 = positive, 0 = negative)
 * - Bits 61-54: exponent (8 bits, biased by 97)
 * - Bits 53-0: mantissa (54 bits)
 */
class IOUValue
{
private:
    uint64_t raw;

    // Bit masks and shifts for portable access
    static constexpr uint64_t IOU_BIT_MASK = 0x8000000000000000ULL;   // Bit 63
    static constexpr uint64_t SIGN_BIT_MASK = 0x4000000000000000ULL;  // Bit 62
    static constexpr uint64_t EXPONENT_MASK =
        0x3FC0000000000000ULL;  // Bits 61-54
    static constexpr uint64_t MANTISSA_MASK =
        0x003FFFFFFFFFFFFFULL;  // Bits 53-0
    static constexpr int EXPONENT_SHIFT = 54;
    static constexpr int EXPONENT_BIAS = 97;

public:
    // Constructors
    IOUValue() : raw(IOU_BIT_MASK)
    {
    }  // Set IOU bit
    explicit IOUValue(uint64_t raw_value) : raw(raw_value)
    {
    }

    // Static factory method from byte array (big-endian)
    static IOUValue
    from_bytes(const uint8_t* data)
    {
        uint64_t value = 0;
        for (int i = 0; i < 8; ++i)
        {
            value = (value << 8) | data[i];
        }
        return IOUValue(value);
    }

    // Getters using explicit bit manipulation
    bool
    is_valid_iou() const
    {
        return (raw & IOU_BIT_MASK) != 0;
    }

    bool
    is_positive() const
    {
        return (raw & SIGN_BIT_MASK) != 0;
    }

    bool
    is_zero() const
    {
        return (raw & MANTISSA_MASK) == 0;
    }

    uint64_t
    mantissa_bits() const
    {
        return raw & MANTISSA_MASK;
    }

    int64_t
    mantissa() const
    {
        if (is_zero())
            return 0;
        uint64_t m = mantissa_bits();
        return is_positive() ? static_cast<int64_t>(m)
                             : -static_cast<int64_t>(m);
    }

    int
    exponent() const
    {
        uint64_t exp_bits = (raw & EXPONENT_MASK) >> EXPONENT_SHIFT;
        return static_cast<int>(exp_bits) - EXPONENT_BIAS;
    }

    uint64_t
    raw_value() const
    {
        return raw;
    }

    /**
     * Convert to human-readable decimal string.
     * Similar to Java's value.toPlainString()
     */
    std::string
    to_string() const
    {
        if (!is_valid_iou())
        {
            throw IOUParseError("Not a valid IOU (bit 63 not set)");
        }

        if (is_zero())
        {
            return "0";
        }

        // Convert mantissa to string
        std::stringstream ss;
        ss << mantissa_bits();
        std::string mantissa_str = ss.str();

        // Apply exponent
        int effective_scale = -exponent();

        if (effective_scale == 0)
        {
            // No decimal point needed
            return (is_positive() ? "" : "-") + mantissa_str;
        }
        else if (effective_scale > 0)
        {
            // Need to add decimal point
            int mantissa_len = mantissa_str.length();

            if (effective_scale >= mantissa_len)
            {
                // Need to add leading zeros after decimal
                std::string result = "0.";
                for (int i = 0; i < effective_scale - mantissa_len; ++i)
                {
                    result += "0";
                }
                result += mantissa_str;
                return (is_positive() ? "" : "-") + result;
            }
            else
            {
                // Insert decimal point within the number
                std::string result =
                    mantissa_str.substr(0, mantissa_len - effective_scale);
                result += ".";
                result += mantissa_str.substr(mantissa_len - effective_scale);
                return (is_positive() ? "" : "-") + result;
            }
        }
        else
        {
            // Negative scale - add trailing zeros
            std::string result = mantissa_str;
            for (int i = 0; i < -effective_scale; ++i)
            {
                result += "0";
            }
            return (is_positive() ? "" : "-") + result;
        }
    }
};

// Ensure the class is exactly 8 bytes
static_assert(sizeof(IOUValue) == 8, "IOUValue must be exactly 8 bytes");

/**
 * Parse IOU value from Amount field data.
 *
 * @param amount_data Slice containing the full 48-byte IOU amount
 * @return Parsed IOU value
 * @throws IOUParseError if data is invalid (wrong size or not an IOU)
 */
inline IOUValue
parse_iou_value(const Slice& amount_data)
{
    // Check size - IOU amounts must be exactly 48 bytes
    if (amount_data.size() != 48)
    {
        throw IOUParseError(
            "Invalid IOU amount size: expected 48 bytes, got " +
            std::to_string(amount_data.size()));
    }

    // Create IOUValue from first 8 bytes
    IOUValue value = IOUValue::from_bytes(amount_data.data());

    // Validate it's actually an IOU
    if (!value.is_valid_iou())
    {
        throw IOUParseError(
            "Not an IOU amount: bit 63 is not set (raw value: 0x" +
            std::to_string(value.raw_value()) + ")");
    }

    return value;
}

/**
 * Get human-readable IOU value string from Amount field data.
 * Convenience function that combines parsing and formatting.
 *
 * @param amount_data Slice containing the full 48-byte IOU amount
 * @return Human-readable decimal string
 * @throws IOUParseError if data is invalid (wrong size or not an IOU)
 */
inline std::string
get_iou_value_string(const Slice& amount_data)
{
    IOUValue value = parse_iou_value(amount_data);
    return value.to_string();
}

}  // namespace catl::xdata