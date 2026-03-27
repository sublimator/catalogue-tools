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
    /// Convert to string matching rippled's STAmount::getText().
    ///
    /// Uses scientific notation when offset < -25 or offset > -5,
    /// otherwise decimal with trailing zeros stripped. This is an
    /// exact port of rippled's logic.
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

        std::string const raw_value = std::to_string(mantissa_bits());
        int const offset = exponent();
        std::string ret;

        if (!is_positive())
            ret += '-';

        // rippled: scientific when offset != 0 and (offset < -25 or offset > -5)
        bool const scientific =
            (offset != 0) && ((offset < -25) || (offset > -5));

        if (scientific)
        {
            ret += raw_value;
            ret += 'e';
            ret += std::to_string(offset);
            return ret;
        }

        // Decimal form with padding and crop — same as rippled.
        // Pad raw_value with zeros on both sides, place decimal point
        // at position (offset + 43), then crop leading/trailing zeros.
        static constexpr size_t pad_prefix = 27;
        static constexpr size_t pad_suffix = 23;

        std::string val;
        val.reserve(raw_value.size() + pad_prefix + pad_suffix);
        val.append(pad_prefix, '0');
        val.append(raw_value);
        val.append(pad_suffix, '0');

        size_t const split = static_cast<size_t>(offset + 43);

        // Pre-decimal: crop leading zeros
        auto pre_from = val.begin();
        auto pre_to = val.begin() + split;

        if (std::distance(pre_from, pre_to) > static_cast<ptrdiff_t>(pad_prefix))
            pre_from += pad_prefix;

        pre_from = std::find_if(
            pre_from, pre_to, [](char c) { return c != '0'; });

        // Post-decimal: crop trailing zeros
        auto post_from = val.begin() + split;
        auto post_to = val.end();

        if (std::distance(post_from, post_to) > static_cast<ptrdiff_t>(pad_suffix))
            post_to -= pad_suffix;

        post_to = std::find_if(
                      std::make_reverse_iterator(post_to),
                      std::make_reverse_iterator(post_from),
                      [](char c) { return c != '0'; })
                      .base();

        // Assemble
        if (pre_from == pre_to)
            ret += '0';
        else
            ret.append(pre_from, pre_to);

        if (post_to != post_from)
        {
            ret += '.';
            ret.append(post_from, post_to);
        }

        return ret;
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