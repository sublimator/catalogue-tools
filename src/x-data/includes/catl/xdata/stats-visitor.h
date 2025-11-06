#pragma once

#include "catl/base58/base58.h"
#include "catl/core/types.h"  // For Slice, Hash256
#include "catl/xdata/fields.h"
#include "catl/xdata/protocol.h"
#include "catl/xdata/slice-cursor.h"
#include "catl/xdata/slice-visitor.h"
#include "catl/xdata/types.h"
#include "catl/xdata/types/amount.h"
#include "catl/xdata/types/iou-value.h"
#include <algorithm>
#include <array>
#include <boost/json.hpp>
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
        bool track_size_histograms = true;         // Track size distributions
        std::string native_currency_code = "XAH";  // Native currency code
    };

    void
    find_fields()
    {
        // Currency fields
        taker_pays_currency_field_code =
            protocol_.find_field("TakerPaysCurrency").value().code;
        taker_gets_currency_field_code =
            protocol_.find_field("TakerGetsCurrency").value().code;

        // Type fields
        transaction_type_field_code =
            protocol_.find_field("TransactionType").value().code;
        ledger_entry_type_field_code =
            protocol_.find_field("LedgerEntryType").value().code;
    }

    explicit StatsVisitor(const Protocol& protocol)
        : protocol_(protocol)
        , config_()
        , start_time_(std::chrono::steady_clock::now())
    {
        find_fields();
    }

    explicit StatsVisitor(const Protocol& protocol, const Config& config)
        : protocol_(protocol)
        , config_(config)
        , start_time_(std::chrono::steady_clock::now())
    {
        find_fields();
    }

    // SliceVisitor interface implementation
    bool
    visit_object_start(const FieldPath& path, const FieldSlice& fs)
    {
        const auto& field = fs.get_field();

        // Check if this is an array element
        if (!path.empty() && path.back().is_array_element())
        {
            current_array_size_++;
        }

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
    visit_object_end(
        const FieldPath& path,
        [[maybe_unused]] const FieldSlice& fs)
    {
        // Track field combinations that appear together
        if (config_.track_field_pairs && !current_object_fields_.empty())
        {
            // Sort fields to ensure consistent ordering
            std::sort(
                current_object_fields_.begin(), current_object_fields_.end());

            // Create a key from the field combination
            std::string combo_key;
            for (const auto& code : current_object_fields_)
            {
                if (!combo_key.empty())
                    combo_key += ",";
                // Convert field code to name for output
                auto it = field_stats_.find(code);
                if (it != field_stats_.end())
                {
                    combo_key += it->second.field_name;
                }
                else
                {
                    combo_key += "field_" + std::to_string(code);
                }
            }

            field_combinations_[combo_key]++;

            // Track pairs (for co-occurrence analysis)
            for (size_t i = 0; i < current_object_fields_.size(); ++i)
            {
                for (size_t j = i + 1; j < current_object_fields_.size(); ++j)
                {
                    std::string pair;
                    // Convert field codes to names
                    auto it1 = field_stats_.find(current_object_fields_[i]);
                    auto it2 = field_stats_.find(current_object_fields_[j]);

                    if (it1 != field_stats_.end())
                        pair = it1->second.field_name;
                    else
                        pair = "field_" +
                            std::to_string(current_object_fields_[i]);

                    pair += " + ";

                    if (it2 != field_stats_.end())
                        pair += it2->second.field_name;
                    else
                        pair += "field_" +
                            std::to_string(current_object_fields_[j]);

                    field_pairs_[pair]++;
                }
            }
        }

        depth_stats_.current_depth = path.size();
    }

    bool
    visit_array_start(
        [[maybe_unused]] const FieldPath& path,
        const FieldSlice& fs)
    {
        const auto& field = fs.get_field();
        array_stats_[field.name].count++;
        current_array_field_ = &field;
        current_array_size_ = 0;
        return true;
    }

    void
    visit_array_end(
        [[maybe_unused]] const FieldPath& path,
        const FieldSlice& fs)
    {
        const auto& field = fs.get_field();
        // Record array size
        array_stats_[field.name].sizes.push_back(current_array_size_);
        current_array_field_ = nullptr;
    }

    void
    visit_field(const FieldPath& path, const FieldSlice& fs)
    {
        const auto& field = fs.get_field();
        auto& stats = field_stats_[field.code];

        // Initialize field name on first use
        if (stats.field_name.empty())
        {
            stats.field_name = field.name;
        }

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
            current_object_fields_.push_back(field.code);
        }

        // Track transaction types
        if (field.code == transaction_type_field_code && fs.data.size() >= 2)
        {
            // TransactionType is stored as UInt16
            SliceCursor cursor{fs.data};
            uint16_t type_code = cursor.read_uint16_be();

            // Use protocol to get the transaction type name
            auto type_name = protocol_.get_transaction_type_name(type_code);
            transaction_types_[type_name.value_or(
                "Unknown_" + format_hex_u16(type_code))]++;
        }

        // Track ledger entry types
        else if (
            field.code == ledger_entry_type_field_code && fs.data.size() >= 2)
        {
            // LedgerEntryType is stored as UInt16
            SliceCursor cursor{fs.data};
            uint16_t type_code = cursor.read_uint16_be();

            // Use protocol to get the ledger entry type name
            auto type_name = protocol_.get_ledger_entry_type_name(type_code);
            ledger_entry_types_[type_name.value_or(
                "Unknown_" + format_hex_u16(type_code))]++;
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
    to_json([[maybe_unused]] bool pretty = true) const
    {
        namespace json = boost::json;

        json::object result;

        // Summary stats
        json::object summary;
        summary["total_fields"] = total_fields_;
        summary["total_bytes"] = total_bytes_;
        summary["unique_fields"] = field_stats_.size();
        summary["max_depth"] = depth_stats_.max_depth;
        summary["first_ledger"] = first_ledger_;
        summary["last_ledger"] = last_ledger_;
        summary["ledger_count"] = ledger_count_;
        summary["total_key_accesses"] = get_total_key_accesses();
        summary["unique_keys_accessed"] = key_frequency_.size();
        summary["deletion_count"] = get_total_deletions();
        summary["duration_ms"] = get_duration_ms();
        result["summary"] = std::move(summary);

        // Top accounts (most compressible via dictionary)
        result["top_accounts"] =
            format_top_n_bytes<20>(account_frequency_, config_.top_n_accounts);

        // Top currencies (dictionary candidates)
        result["top_currencies"] = format_top_n_currencies(
            currency_frequency_, config_.top_n_currencies);

        // Top amounts (special encoding candidates)
        result["top_amounts"] =
            format_top_n_amounts(amount_frequency_, config_.top_n_amounts);

        // Field usage stats
        result["field_usage"] = format_field_stats();

        // Field combinations (for grouping/ordering optimization)
        if (config_.track_field_pairs)
        {
            result["field_combinations"] =
                format_top_n(field_combinations_, 20);
            result["field_pairs"] = format_top_n(field_pairs_, 20);
        }

        // Object type distribution
        result["object_types"] = format_frequency_map(root_object_types_);

        // Transaction type distribution
        result["transaction_types"] = format_frequency_map(transaction_types_);

        // Ledger entry type distribution
        result["ledger_entry_types"] =
            format_frequency_map(ledger_entry_types_);

        // Array statistics
        result["array_stats"] = format_array_stats();

        // Key access patterns
        json::object key_patterns;
        key_patterns["top_accessed_keys"] =
            format_top_n_bytes<32>(key_frequency_, 20);
        key_patterns["top_deleted_keys"] =
            format_top_n_bytes<32>(deletion_key_frequency_, 10);
        result["key_access_patterns"] = std::move(key_patterns);

        // Compression opportunities summary
        result["compression_opportunities"] =
            analyze_compression_opportunities();

        return json::serialize(result, {});
    }

private:
    const Protocol& protocol_;
    Config config_;
    std::chrono::steady_clock::time_point start_time_;

    // Cached field codes for performance
    uint32_t taker_pays_currency_field_code;
    uint32_t taker_gets_currency_field_code;
    uint32_t transaction_type_field_code;
    uint32_t ledger_entry_type_field_code;

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
        std::string field_name;  // Store field name for output
        uint64_t count = 0;
        uint64_t total_size = 0;
        std::unordered_map<size_t, uint64_t> size_histogram;
        std::unordered_map<size_t, uint64_t> depth_histogram;
    };

    std::unordered_map<uint32_t, FieldStats> field_stats_;  // Key is field code

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
    std::unordered_map<std::string, uint64_t>
        ledger_entry_types_;  // Track ledger entry type distribution

    // State for current parse
    std::string current_root_type_;
    std::vector<uint32_t>
        current_object_fields_;  // Store field codes instead of names
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

        // Amount analysis
        else if (field.meta.type == FieldTypes::Amount && fs.data.size() >= 8)
        {
            analyze_amount(fs.data);

            // Also track the currency from Amount fields!
            if (!is_native_amount(fs.data))
            {
                Slice currency = get_currency_raw(fs.data);
                std::array<uint8_t, 20> currency_bytes;
                std::memcpy(currency_bytes.data(), currency.data(), 20);
                currency_frequency_[currency_bytes]++;
            }
            else
            {
                // For Native (XRP/XAH) amounts, we could optionally track
                // NATIVE_CURRENCY but it's probably not useful for compression
                // analysis
                // std::array<uint8_t, 20> currency_bytes;
                // std::memcpy(currency_bytes.data(), NATIVE_CURRENCY, 20);
                // currency_frequency_[currency_bytes]++;

                // We do NOT want this, as we only really want taker pays etc
            }
        }
        // Track currency fields
        else if (
            field.code == taker_gets_currency_field_code ||
            field.code == taker_pays_currency_field_code)
        {
            if (fs.data.size() >= 20)
            {
                std::array<uint8_t, 20> currency_bytes;
                std::memcpy(currency_bytes.data(), fs.data.data(), 20);
                currency_frequency_[currency_bytes]++;
            }
        }
    }

    void
    analyze_amount(const Slice& data)
    {
        if (is_native_amount(data))
        {
            // Native (XRP/XAH) amount: 8 bytes total
            uint64_t drops = 0;
            for (size_t i = 0; i < 8; ++i)
            {
                drops = (drops << 8) | data.data()[i];
            }

            // Clear the Native (XRP/XAH) bit for actual value
            drops &= ~(1ULL << 62);

            // Track round Native (XRP/XAH) amounts (divisible by 1,000,000)
            if (drops % 1000000 == 0)
            {
                uint64_t native_amount = drops / 1000000;
                amount_frequency_
                    [config_.native_currency_code + ":" +
                     std::to_string(native_amount)]++;
            }
            else
            {
                // Track common drop amounts
                amount_frequency_["drops:" + std::to_string(drops)]++;
            }
        }
        else
        {
            // IOU amount: Parse the actual value
            try
            {
                IOUValue iou = parse_iou_value(data);
                std::string value_str = iou.to_string();

                // Track the IOU amount
                amount_frequency_["IOU:" + value_str]++;
            }
            catch (const IOUParseError& e)
            {
                // Invalid IOU format, just track as unknown
                amount_frequency_["IOU:invalid"]++;
            }
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
    boost::json::array
    format_top_n_bytes(
        const std::
            unordered_map<std::array<uint8_t, N>, uint64_t, ArrayHasher<N>>&
                map,
        size_t n) const
    {
        namespace json = boost::json;

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

        json::array result;
        size_t count = 0;
        for (const auto& [bytes, freq] : sorted)
        {
            if (count >= n)
                break;

            json::object item;
            item["hex"] = to_hex(Slice(bytes.data(), N));
            item["count"] = freq;

            // For 20-byte arrays (accounts), also add base58
            if constexpr (N == 20)
            {
                item["base58"] = base58::encode_account_id(bytes.data(), N);
            }

            result.push_back(std::move(item));
            count++;
        }

        return result;
    }

    // Format top N currencies (handles both standard and non-standard)
    boost::json::array
    format_top_n_currencies(
        const std::
            unordered_map<std::array<uint8_t, 20>, uint64_t, ArrayHasher<20>>&
                map,
        size_t n) const
    {
        namespace json = boost::json;

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

        json::array result;
        size_t count = 0;
        for (const auto& [bytes, freq] : sorted)
        {
            if (count >= n)
                break;

            // Check if it's all zeros (native currency)
            bool is_all_zeros = true;
            for (size_t i = 0; i < 20; ++i)
            {
                if (bytes[i] != 0)
                {
                    is_all_zeros = false;
                    break;
                }
            }

            json::object item;
            if (is_all_zeros)
            {
                // Native currency (all zeros)
                item["value"] = config_.native_currency_code;
                item["type"] = "native";
            }
            else
            {
                // Use helper to check if it's a standard currency
                char currency_code[4] = {0};  // 3 chars + null terminator
                bool is_standard = false;

                // Create a temporary slice that looks like an IOU amount
                // (8 bytes amount + 20 bytes currency)
                uint8_t temp_amount[48] = {0};
                temp_amount[0] = 0x80;  // Set IOU bit
                std::memcpy(temp_amount + 8, bytes.data(), 20);

                is_standard = get_currency_code(
                    Slice(temp_amount, 48),
                    currency_code,
                    config_.native_currency_code.c_str());

                if (is_standard)
                {
                    // Use the extracted currency code
                    std::string value(currency_code, 3);
                    // Trim any trailing nulls
                    while (!value.empty() && value.back() == '\0')
                    {
                        value.pop_back();
                    }
                    item["value"] = value;
                    item["type"] = "standard";
                }
                else
                {
                    // Non-standard currency, use full hex
                    item["value"] = to_hex(Slice(bytes.data(), 20));

                    // Also include ASCII representation for readability
                    std::string ascii_value;
                    ascii_value.reserve(20);
                    for (size_t i = 0; i < 20; ++i)
                    {
                        uint8_t ch = bytes[i];
                        if (ch >= 32 && ch <= 126)  // Printable ASCII
                        {
                            ascii_value += static_cast<char>(ch);
                        }
                        else
                        {
                            ascii_value += '?';
                        }
                    }
                    item["value_ascii"] = ascii_value;
                    item["type"] = "non-standard";
                }
            }

            item["count"] = freq;

            result.push_back(std::move(item));
            count++;
        }

        return result;
    }

    template <typename Map>
    boost::json::array
    format_top_n(const Map& map, size_t n) const
    {
        namespace json = boost::json;

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

        json::array result;
        size_t count = 0;
        for (const auto& [key, freq] : sorted)
        {
            if (count >= n)
                break;

            json::object item;
            item["value"] = key;
            item["count"] = freq;
            result.push_back(std::move(item));
            count++;
        }

        return result;
    }

    boost::json::array
    format_top_n_amounts(
        const std::map<std::string, uint64_t>& amounts,
        size_t n) const
    {
        namespace json = boost::json;

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

        json::array result;
        size_t count = 0;
        for (const auto& [amount, freq] : sorted)
        {
            if (count >= n)
                break;

            double percentage =
                total_amounts > 0 ? (100.0 * freq / total_amounts) : 0.0;

            json::object item;
            item["amount"] = amount;
            item["count"] = freq;
            item["percentage"] = percentage;
            result.push_back(std::move(item));
            count++;
        }

        return result;
    }

    boost::json::object
    format_frequency_map(
        const std::unordered_map<std::string, uint64_t>& map) const
    {
        namespace json = boost::json;

        json::object result;
        for (const auto& [key, count] : map)
        {
            result[key] = count;
        }
        return result;
    }

    boost::json::array
    format_field_stats() const
    {
        namespace json = boost::json;

        // Sort fields by frequency
        std::vector<std::pair<uint32_t, uint64_t>> sorted;
        for (const auto& [code, stats] : field_stats_)
        {
            sorted.emplace_back(code, stats.count);
        }
        std::sort(
            sorted.begin(), sorted.end(), [](const auto& a, const auto& b) {
                return a.second > b.second;
            });

        json::array result;
        for (const auto& [code, count] : sorted)
        {
            const auto& stats = field_stats_.at(code);
            double avg_size =
                stats.count > 0 ? (double)stats.total_size / stats.count : 0.0;

            json::object item;
            item["field"] = stats.field_name;
            item["code"] = code;
            item["count"] = count;
            item["total_bytes"] = stats.total_size;
            item["avg_size"] = avg_size;

            if (config_.track_size_histograms && !stats.size_histogram.empty())
            {
                // Show top 3 most common sizes
                std::vector<std::pair<size_t, uint64_t>> sizes(
                    stats.size_histogram.begin(), stats.size_histogram.end());
                std::sort(
                    sizes.begin(),
                    sizes.end(),
                    [](const auto& a, const auto& b) {
                        return a.second > b.second;
                    });

                json::array common_sizes;
                for (size_t i = 0; i < std::min(size_t{3}, sizes.size()); ++i)
                {
                    json::object size_item;
                    size_item["size"] = sizes[i].first;
                    size_item["count"] = sizes[i].second;
                    common_sizes.push_back(std::move(size_item));
                }
                item["common_sizes"] = std::move(common_sizes);
            }

            result.push_back(std::move(item));
        }

        return result;
    }

    boost::json::array
    format_array_stats() const
    {
        namespace json = boost::json;

        json::array result;
        for (const auto& [name, stats] : array_stats_)
        {
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

            json::object item;
            item["array"] = name;
            item["count"] = stats.count;
            item["avg_size"] = avg_size;
            item["min_size"] = (min_size == SIZE_MAX ? 0 : min_size);
            item["max_size"] = max_size;

            result.push_back(std::move(item));
        }

        return result;
    }

    boost::json::object
    analyze_compression_opportunities() const
    {
        namespace json = boost::json;

        json::object result;

        // Dictionary encoding opportunities
        json::object dict_candidates;

        // Accounts that appear frequently enough for dictionary
        size_t dict_accounts = 0;
        uint64_t account_savings = 0;
        for (const auto& [acc, count] : account_frequency_)
        {
            if (count > 10)
            {
                // Threshold for dictionary benefit
                dict_accounts++;
                // 20 bytes per account, could be reduced to 1-2 bytes with
                // dictionary
                account_savings += count * 18;  // Conservative estimate
            }
        }

        json::object accounts_info;
        accounts_info["count"] = dict_accounts;
        accounts_info["potential_savings_bytes"] = account_savings;
        dict_candidates["accounts"] = std::move(accounts_info);

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

        json::object currencies_info;
        currencies_info["count"] = dict_currencies;
        currencies_info["potential_savings_bytes"] = currency_savings;
        dict_candidates["currencies"] = std::move(currencies_info);

        result["dictionary_candidates"] = std::move(dict_candidates);

        // Field ordering optimization
        json::object field_ordering;
        field_ordering["frequent_pairs"] = field_pairs_.size();
        field_ordering["frequent_combinations"] = field_combinations_.size();
        result["field_ordering"] = std::move(field_ordering);

        // Special value encoding
        size_t zero_amounts = 0;
        size_t round_amounts = 0;

        auto native_0 = config_.native_currency_code + ":0";
        auto native_colon = config_.native_currency_code + ":";

        for (const auto& [amount, count] : amount_frequency_)
        {
            if (amount == native_0 || amount == "drops:0")
            {
                zero_amounts += count;
            }
            else if (amount.starts_with(native_colon))
            {
                round_amounts += count;
            }
        }

        json::object special_values;
        special_values["zero_amounts"] = zero_amounts;
        special_values["round_native_amounts"] = round_amounts;
        result["special_values"] = std::move(special_values);

        return result;
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
