#pragma once

#include "catl/base58/base58.h"
#include "catl/xdata/codec-error.h"
#include "catl/xdata/protocol.h"
#include "catl/xdata/serializer.h"
#include <algorithm>
#include <boost/json.hpp>
#include <vector>

namespace catl::xdata::codecs {

// Forward declaration — defined in codecs.h after all codecs are available
template <ByteSink Sink>
void
encode_field_value(
    Serializer<Sink>& s,
    FieldDef const& field,
    boost::json::value const& v,
    Protocol const& protocol,
    std::string const& path);

size_t
field_value_encoded_size(
    FieldDef const& field,
    boost::json::value const& v,
    Protocol const& protocol);

struct STObjectCodec
{
    // Size requires walking all fields
    static size_t
    encoded_size(
        boost::json::value const& v,
        Protocol const& protocol,
        bool include_end_marker = false)
    {
        size_t size = 0;
        auto const& obj = v.as_object();

        for (auto const& [key, val] : obj)
        {
            auto field_opt = protocol.find_field(std::string(key));
            if (!field_opt || !field_opt->meta.is_serialized)
                continue;

            // Field header size
            auto type_code = get_field_type_code(field_opt->code);
            auto field_id = get_field_id(field_opt->code);
            if (type_code < 16 && field_id < 16)
                size += 1;
            else if (type_code < 16 || field_id < 16)
                size += 2;
            else
                size += 3;

            // Value size
            size_t val_size =
                field_value_encoded_size(*field_opt, val, protocol);

            // VL prefix size for VL-encoded fields
            if (field_opt->meta.is_vl_encoded)
            {
                if (val_size <= 192)
                    size += 1;
                else if (val_size <= 12480)
                    size += 2;
                else
                    size += 3;
            }

            size += val_size;

            // Container end markers
            if (field_opt->meta.type == FieldTypes::STObject)
                size += 1;
            else if (field_opt->meta.type == FieldTypes::STArray)
                size += 1;
        }

        if (include_end_marker)
            size += 1;

        return size;
    }

    // Encode an STObject from JSON. Sorts fields by field code.
    // Expand X-addresses in a JSON object into classic addresses + tag fields.
    // Returns a new object if any X-addresses were found, nullopt otherwise.
    static std::optional<boost::json::object>
    expand_xaddresses(
        boost::json::object const& obj,
        std::string const& path = {})
    {
        std::optional<boost::json::object> expanded;

        auto check_field = [&](char const* addr_field,
                               char const* tag_field,
                               boost::json::value const& val) {
            if (!val.is_string())
                return;
            auto sv = std::string_view(val.as_string());
            if (sv.empty() || (sv[0] != 'X' && sv[0] != 'T'))
                return;
            auto decoded = base58::decode_xaddress(sv);
            if (!decoded)
                return;

            // Lazy-create expanded object
            if (!expanded)
                expanded = obj;

            // Replace with classic address
            auto classic = base58::encode_account_id(
                decoded->account_id.data(), decoded->account_id.size());
            (*expanded)[addr_field] = classic;

            // Inject tag if present
            if (decoded->tag)
            {
                if (expanded->contains(tag_field))
                {
                    auto existing = expanded->at(tag_field);
                    uint32_t existing_tag = existing.is_uint64()
                        ? static_cast<uint32_t>(existing.as_uint64())
                        : static_cast<uint32_t>(existing.as_int64());
                    if (existing_tag != *decoded->tag)
                    {
                        throw EncodeError(
                            CodecErrorCode::invalid_value,
                            "STObject",
                            std::string("X-address tag conflicts with ") +
                                tag_field,
                            path);
                    }
                }
                (*expanded)[tag_field] =
                    static_cast<std::uint64_t>(*decoded->tag);
            }
        };

        if (obj.contains("Account"))
            check_field("Account", "SourceTag", obj.at("Account"));
        if (obj.contains("Destination"))
            check_field("Destination", "DestinationTag", obj.at("Destination"));

        return expanded;
    }

    template <ByteSink Sink>
    static void
    encode(
        Serializer<Sink>& s,
        boost::json::value const& v,
        Protocol const& protocol,
        bool only_signing = false,
        std::string const& path = {})
    {
        auto const& orig_obj = v.as_object();

        // Expand X-addresses if enabled on Protocol
        auto expanded = protocol.should_expand_xaddresses()
            ? expand_xaddresses(orig_obj, path)
            : std::nullopt;
        auto const& obj = expanded ? *expanded : orig_obj;

        struct FieldEntry
        {
            FieldDef def;
            boost::json::value const* val;
        };

        std::vector<FieldEntry> fields;
        fields.reserve(obj.size());

        for (auto const& [key, val] : obj)
        {
            auto field_opt = protocol.find_field(std::string(key));
            if (!field_opt)
                continue;
            if (!field_opt->meta.is_serialized)
                continue;
            if (only_signing && !field_opt->meta.is_signing_field)
                continue;
            fields.push_back({*field_opt, &val});
        }

        std::sort(
            fields.begin(), fields.end(), [](auto const& a, auto const& b) {
                return a.def.code < b.def.code;
            });

        for (auto const& entry : fields)
        {
            auto field_path =
                path.empty() ? entry.def.name : path + "." + entry.def.name;

            s.add_field_header(entry.def);

            if (entry.def.meta.is_vl_encoded)
            {
                size_t val_size =
                    field_value_encoded_size(entry.def, *entry.val, protocol);
                s.add_vl_prefix(val_size);
            }

            encode_field_value(s, entry.def, *entry.val, protocol, field_path);

            if (entry.def.meta.type == FieldTypes::STObject)
            {
                s.add_object_end_marker();
            }
            else if (entry.def.meta.type == FieldTypes::STArray)
            {
                s.add_array_end_marker();
            }
        }
    }
};

}  // namespace catl::xdata::codecs
