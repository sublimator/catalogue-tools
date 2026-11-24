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
     * The text rippled writes for this value: to_string(Number) in
     * src/libxrpl/basics/Number.cpp, applied to the Number that rippled's
     * STNumber builds from the wire mantissa and exponent.
     *
     * rippled first normalizes the pair into its mantissa range, and the
     * text depends on the normalized exponent, not the wire one. The range
     * used here is rippled's default and the one in force wherever Number
     * fields exist on a ledger: [10^18, 10^19 - 1].
     *
     * - Zero, and a value too small for the range, is "0".
     * - A normalized exponent of 0, or one from -28 to -8, gives plain
     *   decimal text with no trailing zeros.
     * - Any other exponent gives the mantissa with its trailing zeros
     *   moved into the exponent, then "e" and the exponent.
     */
    std::string
    to_string() const
    {
        static constexpr uint64_t min_mantissa = 1'000'000'000'000'000'000ULL;
        static constexpr uint64_t max_signed = static_cast<uint64_t>(INT64_MAX);
        static constexpr int64_t min_exponent = -32768;
        static constexpr int64_t max_exponent = 32768;
        static constexpr int64_t range_log = 18;

        bool const negative = mantissa_ < 0;
        // Negate as unsigned: -INT64_MIN does not fit int64.
        uint64_t mantissa = negative
            ? 0 - static_cast<uint64_t>(mantissa_)
            : static_cast<uint64_t>(mantissa_);
        int64_t exponent = exponent_;

        // Number::normalize, for a mantissa that fits 64 bits.
        if (mantissa == 0)
            return "0";
        while (mantissa < min_mantissa && exponent > min_exponent)
        {
            mantissa *= 10;
            --exponent;
        }
        if (mantissa < min_mantissa || exponent < min_exponent)
            return "0";
        // 2^63, from the wire mantissa INT64_MIN, is the one value here that
        // is neither within the signed range nor a multiple of ten. rippled
        // rounds it to 2^63 - 1.
        if (mantissa == max_signed + 1)
            mantissa = max_signed;

        // Scientific notation for exponents that are too small or too large.
        if (exponent != 0 &&
            (exponent < -(range_log + 10) || exponent > -(range_log - 10)))
        {
            while (mantissa % 10 == 0 && exponent < max_exponent)
            {
                mantissa /= 10;
                ++exponent;
            }
            std::string ret = negative ? "-" : "";
            ret += std::to_string(mantissa);
            if (exponent != 0)
            {
                ret += 'e';
                ret += std::to_string(exponent);
            }
            return ret;
        }

        // Plain decimal: the 19 digits with the point `exponent` places in
        // from the right, without leading or trailing zeros.
        std::string const digits = std::to_string(mantissa);
        auto const point = static_cast<int64_t>(digits.size()) + exponent;

        std::string whole;
        std::string fraction;
        if (point <= 0)
        {
            whole = "0";
            fraction.assign(static_cast<size_t>(-point), '0');
            fraction += digits;
        }
        else
        {
            whole = digits.substr(0, static_cast<size_t>(point));
            fraction = digits.substr(static_cast<size_t>(point));
        }
        while (!fraction.empty() && fraction.back() == '0')
            fraction.pop_back();

        std::string ret = negative ? "-" : "";
        ret += whole;
        if (!fraction.empty())
        {
            ret += '.';
            ret += fraction;
        }
        return ret;
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
