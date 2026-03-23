#pragma once

#include "catl/core/log-macros.h"
#include "catl/xdata/codecs/codecs.h"
#include "catl/xdata/protocol.h"
#include "catl/xdata/slice-visitor.h"
#include <boost/json.hpp>
#include <stack>
#include <string>

namespace catl::xdata {

/**
 * JsonVisitor builds a boost::json representation of XRPL serialized data.
 *
 * The visitor traverses the data structure and builds a JSON object that
 * represents the parsed content in a human-readable format.
 *
 * Type-specific formatting (Amount, AccountID, Currency, etc.) is delegated
 * to the per-type codecs in catl/xdata/codecs/.
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

        boost::json::object obj;
        stack_.push(boost::json::value(std::move(obj)));
        return true;
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

        boost::json::value completed = std::move(stack_.top());
        stack_.pop();

        if (path.empty() && stack_.empty())
        {
            result_ = std::move(completed);
        }
        else if (!stack_.empty())
        {
            const auto& field = fs.get_field();

            bool is_array_element_child = false;
            if (path.size() >= 2)
            {
                for (size_t i = 0; i < path.size() - 1; ++i)
                {
                    if (path[i].is_array_element())
                    {
                        is_array_element_child = true;
                        break;
                    }
                }
            }

            if (path.size() > 1 && path[path.size() - 2].is_array_element() &&
                field.meta.type == FieldTypes::STObject &&
                stack_.top().is_array())
            {
                boost::json::object wrapper;
                wrapper[field.name] = std::move(completed);
                stack_.top().as_array().push_back(std::move(wrapper));
            }
            else if (is_array_element_child && stack_.top().is_object())
            {
                stack_.top().as_object()[field.name] = std::move(completed);
            }
            else if (stack_.top().is_object())
            {
                stack_.top().as_object()[field.name] = std::move(completed);
            }
            else if (stack_.top().is_array())
            {
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
        (void)path;
        (void)fs;

        LOGD("visit_array_start: path.size()=", path.size());

        boost::json::array arr;
        stack_.push(boost::json::value(std::move(arr)));
        return true;
    }

    void
    visit_array_end(const FieldPath& path, const FieldSlice& fs)
    {
        (void)path;

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

        boost::json::value completed = std::move(stack_.top());
        stack_.pop();

        if (!stack_.empty())
        {
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
        (void)path;

        const auto& field = fs.get_field();
        LOGD(
            "visit_field: field.name=",
            field.name,
            " data.size()=",
            fs.data.size());

        if (stack_.empty())
        {
            LOGD("Stack empty in visit_field, creating root object");
            boost::json::object root_obj;
            stack_.push(boost::json::value(std::move(root_obj)));
        }

        boost::json::value field_value =
            codecs::decode_field_value(field, fs.data, protocol_);

        if (stack_.top().is_object())
        {
            auto& obj = stack_.top().as_object();
            obj[field.name] = std::move(field_value);

            // Add lowercase _ascii hint for printable Blob fields
            if ((field.meta.type == FieldTypes::Blob ||
                 field.meta.is_vl_encoded) &&
                is_printable_text(fs.data))
            {
                // Convert CamelCase to snake_case + _ascii
                // e.g. MemoType → memo_type_ascii
                std::string ascii_key;
                for (size_t i = 0; i < field.name.size(); ++i)
                {
                    char c = field.name[i];
                    if (i > 0 && c >= 'A' && c <= 'Z')
                        ascii_key += '_';
                    ascii_key +=
                        static_cast<char>(c >= 'A' && c <= 'Z' ? c + 32 : c);
                }
                ascii_key += "_ascii";
                obj[ascii_key] = boost::json::string(std::string(
                    reinterpret_cast<const char*>(fs.data.data()),
                    fs.data.size()));
            }
        }
        else
        {
            LOGE(
                "Expected object on stack in visit_field but got ",
                stack_.top().kind());
        }
    }

    boost::json::value
    get_result() const
    {
        if (!const_cast<std::stack<boost::json::value>&>(stack_).empty())
        {
            return const_cast<std::stack<boost::json::value>&>(stack_).top();
        }
        if (!result_.is_null())
        {
            return result_;
        }
        return boost::json::object{};
    }

    std::string
    to_string(bool pretty = true) const
    {
        if (pretty)
        {
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
};

}  // namespace catl::xdata
