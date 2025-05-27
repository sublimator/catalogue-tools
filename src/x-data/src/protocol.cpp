#include "catl/xdata/protocol.h"
#include "catl/xdata/parser.h"
#include <boost/json.hpp>
#include <cstring>  // for std::memset
#include <fstream>
#include <iostream>  // for logging
#include <sstream>
#include <stdexcept>

namespace json = boost::json;

namespace catl::xdata {

Protocol
Protocol::load_from_file(const std::string& path, ProtocolOptions opts)
{
    // Read the entire file
    std::ifstream file(path);
    if (!file.is_open())
    {
        throw std::runtime_error("Failed to open protocol file: " + path);
    }

    std::stringstream buffer;
    buffer << file.rdbuf();
    std::string content = buffer.str();

    // Parse JSON
    json::error_code ec;
    json::value jv = json::parse(content, ec);
    if (ec)
    {
        throw std::runtime_error("Failed to parse JSON: " + ec.message());
    }

    // Handle wrapped result format
    if (jv.is_object() && jv.as_object().contains("result"))
    {
        jv = jv.at("result");
    }

    if (!jv.is_object())
    {
        throw std::runtime_error("Protocol JSON must be an object");
    }

    Protocol protocol;
    protocol.network_id_ = opts.network_id;
    const auto& obj = jv.as_object();

    // Parse TYPES mapping first (but don't validate yet)
    if (obj.contains("TYPES"))
    {
        const auto& types = obj.at("TYPES").as_object();
        for (const auto& [key, value] : types)
        {
            uint16_t code = static_cast<uint16_t>(value.as_int64());
            std::string name(key);
            protocol.types_[name] = code;
            protocol.typeCodeToName_[code] = name;
        }
    }

    // Parse FIELDS array (required)
    if (!obj.contains("FIELDS"))
    {
        throw std::runtime_error("Protocol JSON must contain FIELDS array");
    }

    const auto& fields = obj.at("FIELDS").as_array();

    for (const auto& field : fields)
    {
        const auto& fieldArray = field.as_array();
        if (fieldArray.size() != 2)
        {
            throw std::runtime_error(
                "Field definition must be a 2-element array");
        }

        FieldDef def;
        def.name = fieldArray[0].as_string().c_str();

        const auto& metadata = fieldArray[1].as_object();
        def.meta.is_serialized = metadata.at("isSerialized").as_bool();
        def.meta.is_signing_field = metadata.at("isSigningField").as_bool();
        def.meta.is_vl_encoded = metadata.at("isVLEncoded").as_bool();
        def.meta.nth = static_cast<uint16_t>(metadata.at("nth").as_int64());

        // Look up the FieldType from the type name
        auto typeName = metadata.at("type").as_string();
        if (auto ft = FieldTypes::from_name(typeName))
        {
            def.meta.type = *ft;
        }
        else
        {
            // Unknown type - get its code from TYPES mapping
            auto typeCode = protocol.get_type_code(std::string(typeName));
            if (typeCode)
            {
                // Create a temporary FieldType for unknown types
                // We'll validate it later after all fields are loaded
                def.meta.type = FieldType{typeName, *typeCode};
            }
            else
            {
                throw std::runtime_error(
                    "Field references unknown type: " + std::string(typeName) +
                    " (not in TYPES mapping)");
            }
        }

        // Add to protocol
        protocol.fieldNameIndex_[def.name] = protocol.fields_.size();

        // Set the field code
        def.code = make_field_code(def.meta.type.code, def.meta.nth);
        protocol.fieldCodeIndex_[def.code] = protocol.fields_.size();

        protocol.fields_.push_back(std::move(def));
    }

    // Build the fast lookup table
    protocol.build_fast_lookup();

    // Now validate all types after fields are loaded
    for (const auto& [code, name] : protocol.typeCodeToName_)
    {
        protocol.validate_type(code, opts);
    }

    // Parse LEDGER_ENTRY_TYPES mapping
    if (obj.contains("LEDGER_ENTRY_TYPES"))
    {
        const auto& types = obj.at("LEDGER_ENTRY_TYPES").as_object();
        for (const auto& [key, value] : types)
        {
            protocol.ledgerEntryTypes_[std::string(key)] =
                static_cast<uint16_t>(value.as_int64());
        }
    }

    // Parse TRANSACTION_TYPES mapping
    if (obj.contains("TRANSACTION_TYPES"))
    {
        const auto& types = obj.at("TRANSACTION_TYPES").as_object();
        for (const auto& [key, value] : types)
        {
            protocol.transactionTypes_[std::string(key)] =
                static_cast<uint16_t>(value.as_int64());
        }
    }

    // Parse TRANSACTION_RESULTS mapping
    if (obj.contains("TRANSACTION_RESULTS"))
    {
        const auto& results = obj.at("TRANSACTION_RESULTS").as_object();
        for (const auto& [key, value] : results)
        {
            protocol.transactionResults_[std::string(key)] =
                static_cast<int32_t>(value.as_int64());
        }
    }

    return protocol;
}

void
Protocol::validate_type(uint16_t type_code, const ProtocolOptions& opts)
{
    auto known_type = find_known_type(type_code);

    if (!known_type)
    {
        // Unknown type - check if we can safely infer VL encoding
        if (opts.allow_vl_inference && can_infer_vl_type(type_code))
        {
            std::cerr << "[INFO] Inferred type " << type_code
                      << " as VL-encoded based on field metadata\n";
            inferred_vl_types_.insert(type_code);
        }
        else
        {
            throw std::runtime_error(
                "Unknown type " + std::to_string(type_code) +
                " - cannot parse safely. All fields of this type must have "
                "is_vl_encoded=true to continue.");
        }
    }
    else if (opts.network_id.has_value())
    {
        // Known type - verify network compatibility
        if (!known_type->matches_network(opts.network_id.value()))
        {
            throw std::runtime_error(
                "Type " + std::string(known_type->name) + " (code " +
                std::to_string(type_code) + ") not valid for network " +
                std::to_string(opts.network_id.value()));
        }
    }
}

bool
Protocol::can_infer_vl_type(uint16_t type_code) const
{
    // This should only be called AFTER all fields are loaded
    if (fields_.empty())
    {
        throw std::logic_error(
            "can_infer_vl_type called before fields are loaded");
    }

    size_t vl_count = 0, total_count = 0;

    for (const auto& field : fields_)
    {
        if (field.meta.type.code == type_code)
        {
            total_count++;
            if (field.meta.is_vl_encoded)
                vl_count++;
        }
    }

    // Safe ONLY if ALL fields of this type are VL-encoded
    return total_count > 0 && vl_count == total_count;
}

std::optional<FieldType>
Protocol::find_known_type(uint16_t type_code) const
{
    // Search through all known types
    for (const auto& ft : FieldTypes::ALL)
    {
        if (ft.code == type_code)
        {
            return ft;
        }
    }
    return std::nullopt;
}

std::optional<FieldDef>
Protocol::find_field(const std::string& name) const
{
    auto it = fieldNameIndex_.find(name);
    if (it != fieldNameIndex_.end())
    {
        return fields_[it->second];
    }
    return std::nullopt;
}

std::optional<FieldDef>
Protocol::get_field(const std::string& type, uint16_t field_id) const
{
    // Convert type name to FieldType
    auto field_type = FieldTypes::from_name(type);
    if (!field_type)
    {
        return std::nullopt;
    }

    uint32_t field_code = make_field_code(field_type->code, field_id);
    return get_field_by_code_opt(field_code);
}

const FieldDef*
Protocol::get_field_by_code(uint32_t field_code) const
{
    uint16_t type_code = get_field_type_code(field_code);
    uint16_t field_id = get_field_id(field_code);

    // Fast path for common cases
    if (type_code < 256 && field_id < 256)
    {
        return fast_lookup_[type_code][field_id];
    }

    // Slow path for rare cases
    auto it = fieldCodeIndex_.find(field_code);
    if (it != fieldCodeIndex_.end())
    {
        return &fields_[it->second];
    }
    return nullptr;
}

std::optional<FieldDef>
Protocol::get_field_by_code_opt(uint32_t field_code) const
{
    const FieldDef* field = get_field_by_code(field_code);
    if (field)
    {
        return *field;
    }
    return std::nullopt;
}

std::optional<uint16_t>
Protocol::get_type_code(const std::string& typeName) const
{
    auto it = types_.find(typeName);
    if (it != types_.end())
    {
        return it->second;
    }
    return std::nullopt;
}

std::optional<std::string>
Protocol::get_type_name(uint16_t typeCode) const
{
    auto it = typeCodeToName_.find(typeCode);
    if (it != typeCodeToName_.end())
    {
        return it->second;
    }
    return std::nullopt;
}

std::optional<std::string>
Protocol::get_transaction_type_name(uint16_t txTypeCode) const
{
    // Reverse lookup in transactionTypes_
    for (const auto& [name, code] : transactionTypes_)
    {
        if (code == txTypeCode)
        {
            return name;
        }
    }
    return std::nullopt;
}

void
Protocol::build_fast_lookup()
{
    // Clear the lookup table
    std::memset(fast_lookup_, 0, sizeof(fast_lookup_));

    // Populate the fast lookup table
    for (const auto& field : fields_)
    {
        uint16_t type_code = get_field_type_code(field.code);
        uint16_t field_id = get_field_id(field.code);

        if (type_code < 256 && field_id < 256)
        {
            fast_lookup_[type_code][field_id] = &field;
        }
    }
}

}  // namespace catl::xdata
