#pragma once

#include "catl/core/types.h"  // For Slice, Hash256
#include "catl/xdata/fields.h"
#include "catl/xdata/protocol.h"
#include "catl/xdata/slice-cursor.h"
#include "catl/xdata/slice-visitor.h"
#include "catl/xdata/types.h"
#include "catl/xdata/types/amount.h"
#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>  // for std::memcpy
#include <iomanip>
#include <map>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace catl::xdata {

// Simple hasher for fixed-size byte arrays
template <size_t N>
struct ArrayHasher
{
    size_t
    operator()(const std::array<uint8_t, N>& arr) const
    {
        size_t hash = 0;
        for (size_t i = 0; i < N; ++i)
        {
            hash = hash * 31 + arr[i];
        }
        return hash;
    }
};

/**
 * StatsVisitor collects comprehensive statistics about XRPL data patterns
 * to identify compression opportunities.
 *
 * Key compression insights we're looking for:
 * 1. Frequent accounts/currencies can use dictionary encoding
 * 2. Common amounts (like 0, round numbers) can be specially encoded
 * 3. Fields that appear together can be grouped for better locality
 * 4. Rarely used fields might benefit from different encoding
 * 5. Size distributions help choose optimal variable-length encodings
 * 6. Object type patterns reveal structural redundancy
 */
class StatsVisitor
{
public:
    // Configuration
    struct Config
    {
        size_t top_n_accounts = 100;    // Top N accounts to track
        size_t top_n_currencies = 50;   // Top N currencies to track
        size_t top_n_amounts = 100;     // Top N amounts to track
        size_t top_n_fields = 200;      // Top N field combinations
        bool track_field_pairs = true;  // Track which fields appear together
        bool track_size_histograms = true;  // Track size distributions
    };

    explicit StatsVisitor(const Protocol& protocol)
        : protocol_(protocol)
        , config_()
        , start_time_(std::chrono::steady_clock::now())
    {
    }

    explicit StatsVisitor(const Protocol& protocol, const Config& config)
        : protocol_(protocol)
        , config_(config)
        , start_time_(std::chrono::steady_clock::now())
    {
    }

    // SliceVisitor interface implementation
    bool
    visit_object_start(const FieldPath& path, const FieldDef& field)
    {
        depth_stats_.current_depth = path.size() + 1;
        depth_stats_.max_depth =
            std::max(depth_stats_.max_depth, depth_stats_.current_depth);

        // Track object type distribution
        if (path.empty())
        {
            // Root object type
            root_object_types_[field.name]++;
            current_root_type_ = field.name;
        }

        // Track nesting patterns (which objects contain which)
        if (!path.empty() && path.back().field)
        {
            std::string parent_child = std::string(path.back().field->name) +
                " -> " + std::string(field.name);
            nesting_patterns_[parent_child]++;
        }

        current_object_fields_.clear();
        return true;  // Always descend
    }

    void
    visit_object_end(const FieldPath& path, const FieldDef& field)
    {
        // Track field combinations that appear together
        if (config_.track_field_pairs && !current_object_fields_.empty())
        {
            // Sort fields to ensure consistent ordering
            std::sort(
                current_object_fields_.begin(), current_object_fields_.end());

            // Create a key from the field combination
            std::string combo_key;
            for (const auto& f : current_object_fields_)
            {
                if (!combo_key.empty())
                    combo_key += ",";
                combo_key += f;
            }

            field_combinations_[combo_key]++;

            // Track pairs (for co-occurrence analysis)
            for (size_t i = 0; i < current_object_fields_.size(); ++i)
            {
                for (size_t j = i + 1; j < current_object_fields_.size(); ++j)
                {
                    std::string pair = current_object_fields_[i] + " + " +
                        current_object_fields_[j];
                    field_pairs_[pair]++;
                }
            }
        }

        depth_stats_.current_depth = path.size();
    }

    bool
    visit_array_start(const FieldPath& path, const FieldDef& field)
    {
        array_stats_[field.name].count++;
        current_array_field_ = &field;
        current_array_size_ = 0;
        return true;
    }

    void
    visit_array_end(const FieldPath& path, const FieldDef& field)
    {
        // Record array size
        array_stats_[field.name].sizes.push_back(current_array_size_);
        current_array_field_ = nullptr;
    }

    bool
    visit_array_element(const FieldPath& path, size_t index)
    {
        current_array_size_++;
        return true;
    }

    void
    visit_field(const FieldPath& path, const FieldSlice& fs)
    {
        const auto& field = fs.get_field();
        auto& stats = field_stats_[field.name];

        stats.count++;
        stats.total_size += fs.data.size();

        // Track size distribution
        if (config_.track_size_histograms)
        {
            stats.size_histogram[fs.data.size()]++;
        }

        // Track in current object's field list
        if (path.size() > 0)
        {
            current_object_fields_.push_back(std::string(field.name));
        }

        // Track transaction types
        if (field.name == "TransactionType" &&
            field.meta.type == FieldTypes::UInt16 && fs.data.size() >= 2)
        {
            // TransactionType is stored as UInt16
            SliceCursor cursor{fs.data};
            uint16_t tx_type_code = cursor.read_uint16_be();

            // Use protocol to get the transaction type name
            auto tx_name = protocol_.get_transaction_type_name(tx_type_code);
            transaction_types_[tx_name.value_or(
                "Unknown_" + format_hex_u16(tx_type_code))]++;
        }

        // Analyze specific field types for compression opportunities
        analyze_field_content(field, fs);

        // Track field depth distribution
        stats.depth_histogram[path.size()]++;

        // Update global stats
        total_fields_++;
        total_bytes_ += fs.header.size() + fs.data.size();
    }

    // Track key usage (for both reads and deletes)
    void
    track_key_use(const Slice& key, bool is_delete = false)
    {
        // Keys are always 32 bytes (256-bit hashes)
        if (key.size() != 32)
            return;

        // Store as raw bytes to avoid string allocation
        std::array<uint8_t, 32> key_bytes;
        std::memcpy(key_bytes.data(), key.data(), 32);

        key_frequency_[key_bytes]++;

        if (is_delete)
        {
            deletion_key_frequency_[key_bytes]++;
        }
    }

    // Set ledger range
    void
    set_ledger_range(uint32_t first, uint32_t last)
    {
        first_ledger_ = first;
        last_ledger_ = last;
        ledger_count_ = last - first + 1;
    }

    // Generate JSON statistics report
    std::string
    to_json(bool pretty = true) const
    {
        std::stringstream ss;
        const std::string indent = pretty ? "  " : "";
        const std::string nl = pretty ? "\n" : "";

        ss << "{" << nl;

        // Summary stats
        ss << indent << "\"summary\": {" << nl;
        ss << indent << indent << "\"total_fields\": " << total_fields_ << ","
           << nl;
        ss << indent << indent << "\"total_bytes\": " << total_bytes_ << ","
           << nl;
        ss << indent << indent << "\"unique_fields\": " << field_stats_.size()
           << "," << nl;
        ss << indent << indent << "\"max_depth\": " << depth_stats_.max_depth
           << "," << nl;
        ss << indent << indent << "\"first_ledger\": " << first_ledger_ << ","
           << nl;
        ss << indent << indent << "\"last_ledger\": " << last_ledger_ << ","
           << nl;
        ss << indent << indent << "\"ledger_count\": " << ledger_count_ << ","
           << nl;
        ss << indent << indent
           << "\"total_key_accesses\": " << get_total_key_accesses() << ","
           << nl;
        ss << indent << indent
           << "\"unique_keys_accessed\": " << key_frequency_.size() << ","
           << nl;
        ss << indent << indent
           << "\"deletion_count\": " << get_total_deletions() << "," << nl;
        ss << indent << indent << "\"duration_ms\": " << get_duration_ms()
           << nl;
        ss << indent << "}," << nl;

        // Top accounts (most compressible via dictionary)
        ss << indent << "\"top_accounts\": "
           << format_top_n_bytes<20>(
                  account_frequency_, config_.top_n_accounts, pretty)
           << "," << nl;

        // Top currencies (dictionary candidates)
        ss << indent << "\"top_currencies\": "
           << format_top_n_currencies(
                  currency_frequency_, config_.top_n_currencies, pretty)
           << "," << nl;

        // Top amounts (special encoding candidates)
        ss << indent << "\"top_amounts\": "
           << format_top_n_amounts(
                  amount_frequency_, config_.top_n_amounts, pretty)
           << "," << nl;

        // Field usage stats
        ss << indent << "\"field_usage\": " << format_field_stats(pretty) << ","
           << nl;

        // Field combinations (for grouping/ordering optimization)
        if (config_.track_field_pairs)
        {
            ss << indent << "\"field_combinations\": "
               << format_top_n(field_combinations_, 20, pretty) << "," << nl;

            ss << indent
               << "\"field_pairs\": " << format_top_n(field_pairs_, 20, pretty)
               << "," << nl;
        }

        // Object type distribution
        ss << indent << "\"object_types\": "
           << format_frequency_map(root_object_types_, pretty) << "," << nl;

        // Transaction type distribution
        ss << indent << "\"transaction_types\": "
           << format_frequency_map(transaction_types_, pretty) << "," << nl;

        // Array statistics
        ss << indent << "\"array_stats\": " << format_array_stats(pretty) << ","
           << nl;

        // Key access patterns
        ss << indent << "\"key_access_patterns\": {" << nl;
        ss << indent << indent << "\"top_accessed_keys\": "
           << format_top_n_bytes<32>(key_frequency_, 20, pretty) << "," << nl;
        ss << indent << indent << "\"top_deleted_keys\": "
           << format_top_n_bytes<32>(deletion_key_frequency_, 10, pretty) << nl;
        ss << indent << "}," << nl;

        // Compression opportunities summary
        ss << indent << "\"compression_opportunities\": "
           << analyze_compression_opportunities(pretty) << nl;

        ss << "}";

        return ss.str();
    }

private:
    const Protocol& protocol_;
    Config config_;
    std::chrono::steady_clock::time_point start_time_;

    // Global counters
    uint64_t total_fields_ = 0;
    uint64_t total_bytes_ = 0;
    uint32_t first_ledger_ = 0;
    uint32_t last_ledger_ = 0;
    uint32_t ledger_count_ = 0;

    // Depth tracking
    struct
    {
        size_t current_depth = 0;
        size_t max_depth = 0;
    } depth_stats_;

    // Field statistics
    struct FieldStats
    {
        uint64_t count = 0;
        uint64_t total_size = 0;
        std::unordered_map<size_t, uint64_t> size_histogram;
        std::unordered_map<size_t, uint64_t> depth_histogram;
    };
    std::unordered_map<std::string, FieldStats> field_stats_;

    // Array statistics
    struct ArrayStats
    {
        uint64_t count = 0;
        std::vector<size_t> sizes;
    };
    std::unordered_map<std::string, ArrayStats> array_stats_;

    // Frequency maps for compression analysis
    // Store raw bytes instead of hex strings to avoid allocations in hot path
    std::unordered_map<std::array<uint8_t, 20>, uint64_t, ArrayHasher<20>>
        account_frequency_;
    std::unordered_map<std::array<uint8_t, 20>, uint64_t, ArrayHasher<20>>
        currency_frequency_;
    std::map<std::string, uint64_t>
        amount_frequency_;  // Keep as string, not hot path
    std::unordered_map<std::string, uint64_t> field_combinations_;
    std::unordered_map<std::string, uint64_t> field_pairs_;
    std::unordered_map<std::string, uint64_t> root_object_types_;
    std::unordered_map<std::string, uint64_t> nesting_patterns_;
    std::unordered_map<std::string, uint64_t>
        transaction_types_;  // Track tx type distribution

    // State for current parse
    std::string current_root_type_;
    std::vector<std::string> current_object_fields_;
    const FieldDef* current_array_field_ = nullptr;
    size_t current_array_size_ = 0;

    // Key usage tracking (keys are always 32 bytes)
    std::unordered_map<std::array<uint8_t, 32>, uint64_t, ArrayHasher<32>>
        key_frequency_;
    std::unordered_map<std::array<uint8_t, 32>, uint64_t, ArrayHasher<32>>
        deletion_key_frequency_;

    // Analyze specific field content for patterns
    void
    analyze_field_content(const FieldDef& field, const FieldSlice& fs)
    {
        // Account frequency analysis
        if (field.meta.type == FieldTypes::AccountID && fs.data.size() >= 20)
        {
            // Store raw bytes directly
            std::array<uint8_t, 20> account;
            std::memcpy(account.data(), fs.data.data(), 20);
            account_frequency_[account]++;
        }

        // Currency code analysis
        else if (
            field.meta.type == FieldTypes::Currency && fs.data.size() >= 20)
        {
            // Store raw bytes directly
            std::array<uint8_t, 20> currency;
            std::memcpy(currency.data(), fs.data.data(), 20);
            currency_frequency_[currency]++;
        }

        // Amount analysis
        else if (field.meta.type == FieldTypes::Amount && fs.data.size() >= 8)
        {
            analyze_amount(fs.data);

            // Also track the currency from Amount fields!
            if (!is_xrp_amount(fs.data))
            {
                Slice currency = get_currency_raw(fs.data);
                std::array<uint8_t, 20> currency_bytes;
                std::memcpy(currency_bytes.data(), currency.data(), 20);
                currency_frequency_[currency_bytes]++;
            }
            // For XRP amounts, we could optionally track XRP_CURRENCY
            // but it's probably not useful for compression analysis
        }

        // Hash fields - check for common patterns (e.g., zero hashes)
        else if (field.meta.type == FieldTypes::Hash256 && fs.data.size() == 32)
        {
            bool is_zero = true;
            for (size_t i = 0; i < 32; ++i)
            {
                if (fs.data.data()[i] != 0)
                {
                    is_zero = false;
                    break;
                }
            }
            if (is_zero)
            {
                // Track zero hashes (could be useful for compression)
                // We're already incrementing field_stats in visit_field
            }
        }
    }

    void
    analyze_amount(const Slice& data)
    {
        if (data.empty())
            return;

        uint8_t first_byte = data.data()[0];
        bool is_xrp = (first_byte & 0x80) == 0;

        if (is_xrp && data.size() >= 8)
        {
            // XRP amount: 8 bytes total
            uint64_t drops = 0;
            for (size_t i = 0; i < 8; ++i)
            {
                drops = (drops << 8) | data.data()[i];
            }

            // Clear the XRP bit for actual value
            drops &= ~(1ULL << 62);

            // Track round XRP amounts (divisible by 1,000,000)
            if (drops % 1000000 == 0)
            {
                uint64_t xrp = drops / 1000000;
                amount_frequency_["XRP:" + std::to_string(xrp)]++;
            }
            else
            {
                // Track common drop amounts
                amount_frequency_["drops:" + std::to_string(drops)]++;
            }
        }
        else if (!is_xrp && data.size() >= 48)
        {
            // IOU amount: Track if it's a round number
            // This is simplified - real implementation would parse
            // mantissa/exponent
            amount_frequency_["IOU"]++;
        }
    }

    std::string
    to_hex(const Slice& data) const
    {
        static const char hex_chars[] = "0123456789abcdef";
        std::string result;
        result.reserve(data.size() * 2);
        for (size_t i = 0; i < data.size(); ++i)
        {
            uint8_t byte = data.data()[i];
            result.push_back(hex_chars[byte >> 4]);
            result.push_back(hex_chars[byte & 0xF]);
        }
        return result;
    }

    std::string
    format_hex_u16(uint16_t value) const
    {
        std::stringstream ss;
        ss << "0x" << std::hex << std::uppercase << std::setfill('0')
           << std::setw(4) << value;
        return ss.str();
    }

    // Format top N for byte arrays (accounts)
    template <size_t N>
    std::string
    format_top_n_bytes(
        const std::
            unordered_map<std::array<uint8_t, N>, uint64_t, ArrayHasher<N>>&
                map,
        size_t n,
        bool pretty) const
    {
        // Sort by frequency
        std::vector<std::pair<std::array<uint8_t, N>, uint64_t>> sorted;
        for (const auto& [key, count] : map)
        {
            sorted.emplace_back(key, count);
        }
        std::sort(
            sorted.begin(), sorted.end(), [](const auto& a, const auto& b) {
                return a.second > b.second;
            });

        std::stringstream ss;
        const std::string indent = pretty ? "    " : "";
        const std::string nl = pretty ? "\n" : "";

        ss << "[" << nl;
        size_t count = 0;
        for (const auto& [bytes, freq] : sorted)
        {
            if (count >= n)
                break;
            if (count > 0)
                ss << "," << nl;

            // Convert bytes to hex string
            std::string hex_str = to_hex(Slice(bytes.data(), N));
            ss << indent << "{\"value\": \"" << hex_str
               << "\", \"count\": " << freq << "}";
            count++;
        }
        ss << nl << "  ]";

        return ss.str();
    }

    // Format top N currencies (handles both standard and non-standard)
    std::string
    format_top_n_currencies(
        const std::
            unordered_map<std::array<uint8_t, 20>, uint64_t, ArrayHasher<20>>&
                map,
        size_t n,
        bool pretty) const
    {
        // Sort by frequency
        std::vector<std::pair<std::array<uint8_t, 20>, uint64_t>> sorted;
        for (const auto& [key, count] : map)
        {
            sorted.emplace_back(key, count);
        }
        std::sort(
            sorted.begin(), sorted.end(), [](const auto& a, const auto& b) {
                return a.second > b.second;
            });

        std::stringstream ss;
        const std::string indent = pretty ? "    " : "";
        const std::string nl = pretty ? "\n" : "";

        ss << "[" << nl;
        size_t count = 0;
        for (const auto& [bytes, freq] : sorted)
        {
            if (count >= n)
                break;
            if (count > 0)
                ss << "," << nl;

            // Check if standard currency (first 12 bytes are 0)
            bool is_standard = true;
            for (size_t i = 0; i < 12; ++i)
            {
                if (bytes[i] != 0)
                {
                    is_standard = false;
                    break;
                }
            }

            std::string value;
            if (is_standard)
            {
                // Standard 3-char currency code at bytes 12-14
                value =
                    std::string(reinterpret_cast<const char*>(&bytes[12]), 3);
                // Trim any trailing nulls
                while (!value.empty() && value.back() == '\0')
                {
                    value.pop_back();
                }
            }
            else
            {
                // Non-standard currency, use full hex
                value = to_hex(Slice(bytes.data(), 20));
            }

            ss << indent << "{\"value\": \"" << value
               << "\", \"count\": " << freq << ", \"type\": \""
               << (is_standard ? "standard" : "non-standard") << "\"}";
            count++;
        }
        ss << nl << "  ]";

        return ss.str();
    }

    template <typename Map>
    std::string
    format_top_n(const Map& map, size_t n, bool pretty) const
    {
        // Sort by frequency
        std::vector<std::pair<std::string, uint64_t>> sorted;
        for (const auto& [key, count] : map)
        {
            sorted.emplace_back(key, count);
        }
        std::sort(
            sorted.begin(), sorted.end(), [](const auto& a, const auto& b) {
                return a.second > b.second;
            });

        std::stringstream ss;
        const std::string indent = pretty ? "    " : "";
        const std::string nl = pretty ? "\n" : "";

        ss << "[" << nl;
        size_t count = 0;
        for (const auto& [key, freq] : sorted)
        {
            if (count >= n)
                break;
            if (count > 0)
                ss << "," << nl;
            ss << indent << "{\"value\": \"" << key << "\", \"count\": " << freq
               << "}";
            count++;
        }
        ss << nl << "  ]";

        return ss.str();
    }

    std::string
    format_top_n_amounts(
        const std::map<std::string, uint64_t>& amounts,
        size_t n,
        bool pretty) const
    {
        // Special handling for amounts to show both raw and percentage
        std::vector<std::pair<std::string, uint64_t>> sorted(
            amounts.begin(), amounts.end());
        std::sort(
            sorted.begin(), sorted.end(), [](const auto& a, const auto& b) {
                return a.second > b.second;
            });

        uint64_t total_amounts = 0;
        for (const auto& [_, count] : amounts)
        {
            total_amounts += count;
        }

        std::stringstream ss;
        const std::string indent = pretty ? "    " : "";
        const std::string nl = pretty ? "\n" : "";

        ss << "[" << nl;
        size_t count = 0;
        for (const auto& [amount, freq] : sorted)
        {
            if (count >= n)
                break;
            if (count > 0)
                ss << "," << nl;

            double percentage =
                total_amounts > 0 ? (100.0 * freq / total_amounts) : 0.0;

            ss << indent << "{\"amount\": \"" << amount
               << "\", \"count\": " << freq
               << ", \"percentage\": " << std::fixed << std::setprecision(2)
               << percentage << "}";
            count++;
        }
        ss << nl << "  ]";

        return ss.str();
    }

    std::string
    format_frequency_map(
        const std::unordered_map<std::string, uint64_t>& map,
        bool pretty) const
    {
        std::stringstream ss;
        const std::string indent = pretty ? "    " : "";
        const std::string nl = pretty ? "\n" : "";

        ss << "{" << nl;
        bool first = true;
        for (const auto& [key, count] : map)
        {
            if (!first)
                ss << "," << nl;
            ss << indent << "\"" << key << "\": " << count;
            first = false;
        }
        ss << nl << "  }";

        return ss.str();
    }

    std::string
    format_field_stats(bool pretty) const
    {
        std::stringstream ss;
        const std::string indent = pretty ? "    " : "";
        const std::string nl = pretty ? "\n" : "";

        // Sort fields by frequency
        std::vector<std::pair<std::string, uint64_t>> sorted;
        for (const auto& [name, stats] : field_stats_)
        {
            sorted.emplace_back(name, stats.count);
        }
        std::sort(
            sorted.begin(), sorted.end(), [](const auto& a, const auto& b) {
                return a.second > b.second;
            });

        ss << "[" << nl;
        bool first = true;
        for (const auto& [name, count] : sorted)
        {
            if (!first)
                ss << "," << nl;
            first = false;

            const auto& stats = field_stats_.at(name);
            double avg_size =
                stats.count > 0 ? (double)stats.total_size / stats.count : 0.0;

            ss << indent << "{";
            ss << "\"field\": \"" << name << "\", ";
            ss << "\"count\": " << count << ", ";
            ss << "\"total_bytes\": " << stats.total_size << ", ";
            ss << "\"avg_size\": " << std::fixed << std::setprecision(2)
               << avg_size;

            if (config_.track_size_histograms && !stats.size_histogram.empty())
            {
                ss << ", \"common_sizes\": [";
                // Show top 3 most common sizes
                std::vector<std::pair<size_t, uint64_t>> sizes(
                    stats.size_histogram.begin(), stats.size_histogram.end());
                std::sort(
                    sizes.begin(),
                    sizes.end(),
                    [](const auto& a, const auto& b) {
                        return a.second > b.second;
                    });

                for (size_t i = 0; i < std::min(size_t{3}, sizes.size()); ++i)
                {
                    if (i > 0)
                        ss << ", ";
                    ss << "{\"size\": " << sizes[i].first
                       << ", \"count\": " << sizes[i].second << "}";
                }
                ss << "]";
            }

            ss << "}";
        }
        ss << nl << "  ]";

        return ss.str();
    }

    std::string
    format_array_stats(bool pretty) const
    {
        std::stringstream ss;
        const std::string indent = pretty ? "    " : "";
        const std::string nl = pretty ? "\n" : "";

        ss << "[" << nl;
        bool first = true;
        for (const auto& [name, stats] : array_stats_)
        {
            if (!first)
                ss << "," << nl;
            first = false;

            // Calculate size statistics
            double avg_size = 0;
            size_t min_size = SIZE_MAX;
            size_t max_size = 0;

            if (!stats.sizes.empty())
            {
                size_t total = 0;
                for (size_t s : stats.sizes)
                {
                    total += s;
                    min_size = std::min(min_size, s);
                    max_size = std::max(max_size, s);
                }
                avg_size = (double)total / stats.sizes.size();
            }

            ss << indent << "{";
            ss << "\"array\": \"" << name << "\", ";
            ss << "\"count\": " << stats.count << ", ";
            ss << "\"avg_size\": " << std::fixed << std::setprecision(2)
               << avg_size << ", ";
            ss << "\"min_size\": " << (min_size == SIZE_MAX ? 0 : min_size)
               << ", ";
            ss << "\"max_size\": " << max_size;
            ss << "}";
        }
        ss << nl << "  ]";

        return ss.str();
    }

    std::string
    analyze_compression_opportunities(bool pretty) const
    {
        std::stringstream ss;
        const std::string indent = pretty ? "    " : "";
        const std::string nl = pretty ? "\n" : "";

        ss << "{" << nl;

        // Dictionary encoding opportunities
        ss << indent << "\"dictionary_candidates\": {" << nl;

        // Accounts that appear frequently enough for dictionary
        size_t dict_accounts = 0;
        uint64_t account_savings = 0;
        for (const auto& [acc, count] : account_frequency_)
        {
            if (count > 10)
            {  // Threshold for dictionary benefit
                dict_accounts++;
                // 20 bytes per account, could be reduced to 1-2 bytes with
                // dictionary
                account_savings += count * 18;  // Conservative estimate
            }
        }

        ss << indent << indent << "\"accounts\": {";
        ss << "\"count\": " << dict_accounts << ", ";
        ss << "\"potential_savings_bytes\": " << account_savings << "}," << nl;

        // Similar for currencies
        size_t dict_currencies = 0;
        uint64_t currency_savings = 0;
        for (const auto& [curr, count] : currency_frequency_)
        {
            if (count > 20)
            {
                dict_currencies++;
                currency_savings += count * 19;  // 20 bytes -> 1 byte
            }
        }

        ss << indent << indent << "\"currencies\": {";
        ss << "\"count\": " << dict_currencies << ", ";
        ss << "\"potential_savings_bytes\": " << currency_savings << "}" << nl;

        ss << indent << "}," << nl;

        // Field ordering optimization
        ss << indent << "\"field_ordering\": {" << nl;
        ss << indent << indent << "\"frequent_pairs\": " << field_pairs_.size()
           << "," << nl;
        ss << indent << indent
           << "\"frequent_combinations\": " << field_combinations_.size() << nl;
        ss << indent << "}," << nl;

        // Special value encoding
        size_t zero_amounts = 0;
        size_t round_amounts = 0;
        for (const auto& [amount, count] : amount_frequency_)
        {
            if (amount == "XRP:0" || amount == "drops:0")
            {
                zero_amounts += count;
            }
            else if (amount.starts_with("XRP:"))
            {
                round_amounts += count;
            }
        }

        ss << indent << "\"special_values\": {" << nl;
        ss << indent << indent << "\"zero_amounts\": " << zero_amounts << ","
           << nl;
        ss << indent << indent << "\"round_xrp_amounts\": " << round_amounts
           << nl;
        ss << indent << "}" << nl;

        ss << "  }";

        return ss.str();
    }

    uint64_t
    get_duration_ms() const
    {
        auto now = std::chrono::steady_clock::now();
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   now - start_time_)
            .count();
    }

    uint64_t
    get_total_key_accesses() const
    {
        uint64_t total = 0;
        for (const auto& [key, count] : key_frequency_)
        {
            total += count;
        }
        return total;
    }

    uint64_t
    get_total_deletions() const
    {
        uint64_t total = 0;
        for (const auto& [key, count] : deletion_key_frequency_)
        {
            total += count;
        }
        return total;
    }
};

}  // namespace catl::xdata