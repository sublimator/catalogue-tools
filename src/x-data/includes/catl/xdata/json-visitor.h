#pragma once

#include "catl/base58/base58.h"
#include "catl/core/log-macros.h"
#include "catl/xdata/protocol.h"
#include "catl/xdata/slice-visitor.h"
#include "catl/xdata/types/amount.h"
#include "catl/xdata/types/iou-value.h"
#include "catl/xdata/types/issue.h"
#include "catl/xdata/types/number.h"
#include "catl/xdata/types/pathset.h"
#include <boost/json.hpp>
#include <sstream>
#include <stack>
#include <string>
#include <vector>

namespace catl::xdata {

/**
 * JsonVisitor builds a boost::json representation of XRPL serialized data.
 *
 * The visitor traverses the data structure and builds a JSON object that
 * represents the parsed content in a human-readable format.
 *
 * Features:
 * - Converts binary fields to appropriate JSON representations
 * - Handles special XRPL types (Amount, AccountID, Currency, etc.)
 * - Preserves the hierarchical structure of objects and arrays
 * - Provides type-aware formatting for better readability
 */
class JsonVisitor
{
public:
    explicit JsonVisitor(const Protocol& protocol) : protocol_(protocol)
    {
    }

    bool
    visit_object_start(const FieldPath& path, const FieldSlice& fs)
    {
        const auto& field = fs.get_field();

        LOGD(
            "visit_object_start: path.size()=",
            path.size(),
            " field.name=",
            field.name);

        // Create new object
        boost::json::object obj;
        stack_.push(boost::json::value(std::move(obj)));
        return true;  // Always descend
    }

    void
    visit_object_end(const FieldPath& path, const FieldSlice& fs)
    {
        LOGD(
            "visit_object_end: path.size()=",
            path.size(),
            " stack.size()=",
            stack_.size());

        if (stack_.empty())
        {
            LOGE("Stack is empty in visit_object_end!");
            return;
        }

        // Pop the completed object
        boost::json::value completed = std::move(stack_.top());
        stack_.pop();

        if (path.empty() && stack_.empty())
        {
            // This is truly the root object (empty path and nothing left on
            // stack)
            result_ = std::move(completed);
        }
        else if (!stack_.empty())
        {
            const auto& field = fs.get_field();

            // Check if we're completing an object that's part of an array
            // element
            bool is_array_element_child = false;
            if (path.size() >= 2)
            {
                // Check if any ancestor is an array element
                for (size_t i = 0; i < path.size() - 1; ++i)
                {
                    if (path[i].is_array_element())
                    {
                        is_array_element_child = true;
                        break;
                    }
                }
            }

            // Generic handling for STObject fields in arrays
            // When an STObject is a direct child of an array element,
            // wrap it in an object with the field name as the key
            if (path.size() > 1 && path[path.size() - 2].is_array_element() &&
                field.meta.type == FieldTypes::STObject &&
                stack_.top().is_array())
            {
                // Create wrapper object with field name as key
                boost::json::object wrapper;
                wrapper[field.name] = std::move(completed);
                stack_.top().as_array().push_back(std::move(wrapper));
            }
            else if (is_array_element_child && stack_.top().is_object())
            {
                // This is a nested object inside an array element (like
                // PreviousFields) Add it to the parent object
                stack_.top().as_object()[field.name] = std::move(completed);
            }
            else if (stack_.top().is_object())
            {
                // Normal object field
                stack_.top().as_object()[field.name] = std::move(completed);
            }
            else if (stack_.top().is_array())
            {
                // Direct array child without wrapper
                stack_.top().as_array().push_back(std::move(completed));
            }
            else
            {
                LOGE("Unexpected stack state: ", stack_.top().kind());
            }
        }
    }

    bool
    visit_array_start(const FieldPath& path, const FieldSlice& fs)
    {
        (void)path;  // Suppress unused parameter warning
        (void)fs;    // Suppress unused parameter warning

        LOGD("visit_array_start: path.size()=", path.size());

        // Create new array
        boost::json::array arr;
        stack_.push(boost::json::value(std::move(arr)));
        return true;  // Always descend
    }

    void
    visit_array_end(const FieldPath& path, const FieldSlice& fs)
    {
        (void)path;  // Suppress unused parameter warning

        LOGD(
            "visit_array_end: path.size()=",
            path.size(),
            " stack.size()=",
            stack_.size());

        if (stack_.empty())
        {
            LOGE("Stack is empty in visit_array_end!");
            return;
        }

        // Pop the completed array
        boost::json::value completed = std::move(stack_.top());
        stack_.pop();

        if (!stack_.empty())
        {
            // Add to parent object
            const auto& field = fs.get_field();
            if (stack_.top().is_object())
            {
                stack_.top().as_object()[field.name] = std::move(completed);
            }
            else
            {
                LOGE(
                    "Expected object on stack for array parent but got ",
                    stack_.top().kind());
            }
        }
    }

    void
    visit_field(const FieldPath& path, const FieldSlice& fs)
    {
        (void)path;  // Suppress unused parameter warning

        const auto& field = fs.get_field();
        LOGD(
            "visit_field: field.name=",
            field.name,
            " data.size()=",
            fs.data.size());

        if (stack_.empty())
        {
            // If stack is empty, we're at the root level - create a root object
            LOGD("Stack empty in visit_field, creating root object");
            boost::json::object root_obj;
            stack_.push(boost::json::value(std::move(root_obj)));
        }

        boost::json::value field_value = format_field_value(field, fs.data);

        // Add to current object
        if (stack_.top().is_object())
        {
            stack_.top().as_object()[field.name] = std::move(field_value);
        }
        else
        {
            LOGE(
                "Expected object on stack in visit_field but got ",
                stack_.top().kind());
        }
    }

    // Get the final JSON result
    boost::json::value
    get_result() const
    {
        // If no result but stack has content, return the top of the stack
        // This handles cases where only fields were visited without proper
        // object end, which is common for XRPL root objects
        if (!const_cast<std::stack<boost::json::value>&>(stack_).empty())
        {
            return const_cast<std::stack<boost::json::value>&>(stack_).top();
        }

        // If we have a result, return it
        if (!result_.is_null())
        {
            return result_;
        }

        // Return empty object if nothing was parsed
        return boost::json::object{};
    }

    // Get the result as a formatted string
    std::string
    to_string(bool pretty = true) const
    {
        if (pretty)
        {
            // Manual pretty print since boost::json doesn't have built-in
            // pretty print
            std::stringstream ss;
            ss << result_;
            return ss.str();
        }
        else
        {
            return boost::json::serialize(result_);
        }
    }

private:
    const Protocol& protocol_;
    mutable std::stack<boost::json::value> stack_;
    boost::json::value result_;

    // Format a field value based on its type
    boost::json::value
    format_field_value(const FieldDef& field, const Slice& data)
    {
        // Handle different field types
        if (field.meta.type == FieldTypes::UInt8)
        {
            if (data.size() >= 1)
                return static_cast<std::uint64_t>(data.data()[0]);
        }
        else if (field.meta.type == FieldTypes::UInt16)
        {
            if (data.size() >= 2)
            {
                uint16_t value = (static_cast<uint16_t>(data.data()[0]) << 8) |
                    static_cast<uint16_t>(data.data()[1]);

                // Special handling for type fields
                if (field.name == "TransactionType")
                {
                    auto type_name = protocol_.get_transaction_type_name(value);
                    return boost::json::string(type_name.value_or("Unknown"));
                }
                else if (field.name == "LedgerEntryType")
                {
                    auto type_name =
                        protocol_.get_ledger_entry_type_name(value);
                    return boost::json::string(type_name.value_or("Unknown"));
                }
                else if (field.name == "TransactionResult")
                {
                    // TransactionResult uses signed values
                    int16_t signed_value = static_cast<int16_t>(value);
                    return static_cast<std::int64_t>(signed_value);
                }

                return static_cast<std::uint64_t>(value);
            }
        }
        else if (field.meta.type == FieldTypes::UInt32)
        {
            if (data.size() >= 4)
            {
                uint32_t value = (static_cast<uint32_t>(data.data()[0]) << 24) |
                    (static_cast<uint32_t>(data.data()[1]) << 16) |
                    (static_cast<uint32_t>(data.data()[2]) << 8) |
                    static_cast<uint32_t>(data.data()[3]);
                return static_cast<std::uint64_t>(value);
            }
        }
        else if (field.meta.type == FieldTypes::UInt64)
        {
            if (data.size() >= 8)
            {
                uint64_t value = 0;
                for (int i = 0; i < 8; ++i)
                {
                    value = (value << 8) | data.data()[i];
                }
                return boost::json::string(
                    std::to_string(value));  // Use string for large numbers
            }
        }
        else if (field.meta.type == FieldTypes::Hash128)
        {
            return boost::json::string(to_hex(data));
        }
        else if (field.meta.type == FieldTypes::Hash160)
        {
            return boost::json::string(to_hex(data));
        }
        else if (field.meta.type == FieldTypes::Hash256)
        {
            return boost::json::string(to_hex(data));
        }
        else if (field.meta.type == FieldTypes::AccountID)
        {
            if (data.size() == 20)
            {
                // Format as base58 address
                return boost::json::string(
                    base58::encode_account_id(data.data(), data.size()));
            }
        }
        else if (field.meta.type == FieldTypes::Currency)
        {
            return format_currency(data);
        }
        else if (field.meta.type == FieldTypes::Amount)
        {
            return format_amount(data);
        }
        else if (field.meta.type == FieldTypes::Issue)
        {
            return format_issue(data);
        }
        else if (field.meta.type == FieldTypes::Number)
        {
            return format_number(data);
        }
        else if (field.meta.type == FieldTypes::PathSet)
        {
            return format_pathset(data);
        }
        else if (field.meta.type == FieldTypes::Vector256)
        {
            // Vector of 256-bit hashes
            boost::json::array arr;
            size_t count = data.size() / 32;
            for (size_t i = 0; i < count; ++i)
            {
                Slice hash_slice(data.data() + i * 32, 32);
                arr.push_back(boost::json::string(to_hex(hash_slice)));
            }
            return arr;
        }
        else if (
            field.meta.type == FieldTypes::Blob || field.meta.is_vl_encoded)
        {
            // For blobs, check if it might be ASCII text
            if (is_printable_text(data))
            {
                return boost::json::string(std::string(
                    reinterpret_cast<const char*>(data.data()), data.size()));
            }
            else
            {
                return boost::json::string(to_hex(data));
            }
        }

        // Default: hex encode
        return boost::json::string(to_hex(data));
    }

    // Format currency code
    boost::json::value
    format_currency(const Slice& data)
    {
        if (data.size() != 20)
            return boost::json::string(to_hex(data));

        // Check if it's all zeros (native currency)
        bool is_all_zeros = true;
        for (size_t i = 0; i < 20; ++i)
        {
            if (data.data()[i] != 0)
            {
                is_all_zeros = false;
                break;
            }
        }

        if (is_all_zeros)
        {
            return boost::json::string("XAH");  // Or XRP depending on network
        }

        // Check for standard currency code (3 ASCII chars)
        bool is_standard = true;
        for (size_t i = 0; i < 20; ++i)
        {
            if (i < 12 && data.data()[i] != 0)
            {
                is_standard = false;
                break;
            }
            if (i >= 15 && data.data()[i] != 0)
            {
                is_standard = false;
                break;
            }
        }

        if (is_standard)
        {
            // Extract the 3-char code
            std::string code;
            for (size_t i = 12; i < 15; ++i)
            {
                if (data.data()[i] != 0)
                    code += static_cast<char>(data.data()[i]);
            }
            return boost::json::string(code);
        }

        // Non-standard currency - return as hex
        return boost::json::string(to_hex(data));
    }

    // Format issue field
    boost::json::value
    format_issue(const Slice& data)
    {
        try
        {
            auto parsed = parse_issue(data);

            if (parsed.is_native())
            {
                // XRP/Native - just return the currency
                return format_currency(parsed.currency);
            }
            else
            {
                // Non-native - return object with currency and issuer
                boost::json::object issue_obj;
                issue_obj["currency"] = format_currency(parsed.currency);
                issue_obj["issuer"] =
                    boost::json::string(base58::encode_account_id(
                        parsed.issuer.data(), parsed.issuer.size()));
                return issue_obj;
            }
        }
        catch (const std::exception& e)
        {
            // Fall back to hex
            LOGE("Failed to parse Issue: ", e.what());
            return boost::json::string(to_hex(data));
        }
    }

    // Format amount field
    boost::json::value
    format_amount(const Slice& data)
    {
        if (is_native_amount(data))
        {
            // Native (XRP/XAH) amount - return as string of drops
            return boost::json::string(parse_native_drops_string(data));
        }
        else
        {
            // IOU amount
            try
            {
                IOUValue iou = parse_iou_value(data);

                // Extract currency
                Slice currency_slice = get_currency_raw(data);

                // Extract issuer (last 20 bytes)
                std::string issuer;
                if (data.size() >= 48)
                {
                    issuer = base58::encode_account_id(data.data() + 28, 20);
                }

                boost::json::object amount_obj;
                amount_obj["currency"] = format_currency(currency_slice);
                amount_obj["value"] = boost::json::string(iou.to_string());
                amount_obj["issuer"] = boost::json::string(issuer);
                return amount_obj;
            }
            catch (const IOUParseError& e)
            {
                // Fall back to hex
                return boost::json::string(to_hex(data));
            }
        }
    }

    // Format STNumber field
    boost::json::value
    format_number(const Slice& data)
    {
        try
        {
            STNumber number = parse_number(data);
            
            // Return as string to preserve precision
            return boost::json::string(number.to_string());
        }
        catch (const std::exception& e)
        {
            // Fall back to hex
            LOGE("Failed to parse STNumber: ", e.what());
            return boost::json::string(to_hex(data));
        }
    }

    // Format PathSet field
    boost::json::value
    format_pathset(const Slice& data)
    {
        boost::json::array paths;
        boost::json::array current_path;

        size_t pos = 0;
        while (pos < data.size())
        {
            uint8_t type_byte = data.data()[pos++];

            if (type_byte == PathSet::END_BYTE)
            {
                // End of PathSet
                if (!current_path.empty())
                {
                    paths.push_back(std::move(current_path));
                }
                break;
            }

            if (type_byte == PathSet::PATH_SEPARATOR)
            {
                // End current path, start new one
                if (!current_path.empty())
                {
                    paths.push_back(std::move(current_path));
                    current_path = boost::json::array();
                }
                continue;
            }

            // It's a hop - parse based on type bits
            boost::json::object hop;

            if (type_byte & PathSet::TYPE_ACCOUNT)
            {
                if (pos + 20 <= data.size())
                {
                    hop["account"] =
                        base58::encode_account_id(data.data() + pos, 20);
                    pos += 20;
                }
            }

            if (type_byte & PathSet::TYPE_CURRENCY)
            {
                if (pos + 20 <= data.size())
                {
                    Slice currency_slice(data.data() + pos, 20);
                    hop["currency"] = format_currency(currency_slice);
                    pos += 20;
                }
            }

            if (type_byte & PathSet::TYPE_ISSUER)
            {
                if (pos + 20 <= data.size())
                {
                    hop["issuer"] =
                        base58::encode_account_id(data.data() + pos, 20);
                    pos += 20;
                }
            }

            if (!hop.empty())
            {
                current_path.push_back(std::move(hop));
            }
        }

        // Add any remaining path
        if (!current_path.empty())
        {
            paths.push_back(std::move(current_path));
        }

        return paths;
    }

    // Convert to hex string
    std::string
    to_hex(const Slice& data) const
    {
        static const char hex_chars[] = "0123456789ABCDEF";
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

    // Check if data is printable ASCII text
    bool
    is_printable_text(const Slice& data) const
    {
        if (data.empty())
            return false;

        for (size_t i = 0; i < data.size(); ++i)
        {
            uint8_t ch = data.data()[i];
            if (ch < 32 || ch > 126)
            {
                // Allow newlines and tabs
                if (ch != '\n' && ch != '\r' && ch != '\t')
                    return false;
            }
        }
        return true;
    }
};

}  // namespace catl::xdata