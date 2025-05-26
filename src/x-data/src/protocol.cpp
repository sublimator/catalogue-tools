#include "catl/xdata/protocol.h"
#include "catl/xdata/parser.h"
#include <boost/json.hpp>
#include <fstream>
#include <stdexcept>
#include <sstream>
#include <cstring>  // for std::memset

namespace json = boost::json;

namespace catl::xdata {

Protocol Protocol::load_from_file(const std::string& path) {
    // Read the entire file
    std::ifstream file(path);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open protocol file: " + path);
    }
    
    std::stringstream buffer;
    buffer << file.rdbuf();
    std::string content = buffer.str();
    
    // Parse JSON
    json::error_code ec;
    json::value jv = json::parse(content, ec);
    if (ec) {
        throw std::runtime_error("Failed to parse JSON: " + ec.message());
    }
    
    if (!jv.is_object()) {
        throw std::runtime_error("Protocol JSON must be an object");
    }
    
    Protocol protocol;
    const auto& obj = jv.as_object();
    
    // Parse FIELDS array
    if (obj.contains("FIELDS")) {
        const auto& fields = obj.at("FIELDS").as_array();
        
        for (const auto& field : fields) {
            const auto& fieldArray = field.as_array();
            if (fieldArray.size() != 2) {
                throw std::runtime_error("Field definition must be a 2-element array");
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
            if (auto ft = FieldTypes::from_name(typeName)) {
                def.meta.type = *ft;
            } else {
                throw std::runtime_error("Unknown field type: " + std::string(typeName));
            }
            
            // Add to protocol
            protocol.fieldNameIndex_[def.name] = protocol.fields_.size();
            
            // Set code to 0 initially (will be updated after TYPES are loaded)
            def.code = 0;
            
            protocol.fields_.push_back(std::move(def));
        }
    }
    
    // Parse TYPES mapping
    if (obj.contains("TYPES")) {
        const auto& types = obj.at("TYPES").as_object();
        for (const auto& [key, value] : types) {
            uint16_t code = static_cast<uint16_t>(value.as_int64());
            std::string name(key);
            protocol.types_[name] = code;
            protocol.typeCodeToName_[code] = name;
        }
        
        // Now update field codes using the FieldType codes directly
        for (size_t i = 0; i < protocol.fields_.size(); ++i) {
            auto& field = protocol.fields_[i];
            field.code = make_field_code(field.meta.type.code, field.meta.nth);
            protocol.fieldCodeIndex_[field.code] = i;
        }
        
        // Build the fast lookup table
        protocol.build_fast_lookup();
    }
    
    // Parse LEDGER_ENTRY_TYPES mapping
    if (obj.contains("LEDGER_ENTRY_TYPES")) {
        const auto& types = obj.at("LEDGER_ENTRY_TYPES").as_object();
        for (const auto& [key, value] : types) {
            protocol.ledgerEntryTypes_[std::string(key)] = 
                static_cast<uint16_t>(value.as_int64());
        }
    }
    
    // Parse TRANSACTION_TYPES mapping
    if (obj.contains("TRANSACTION_TYPES")) {
        const auto& types = obj.at("TRANSACTION_TYPES").as_object();
        for (const auto& [key, value] : types) {
            protocol.transactionTypes_[std::string(key)] = 
                static_cast<uint16_t>(value.as_int64());
        }
    }
    
    // Parse TRANSACTION_RESULTS mapping
    if (obj.contains("TRANSACTION_RESULTS")) {
        const auto& results = obj.at("TRANSACTION_RESULTS").as_object();
        for (const auto& [key, value] : results) {
            protocol.transactionResults_[std::string(key)] = 
                static_cast<int32_t>(value.as_int64());
        }
    }
    
    return protocol;
}

std::optional<FieldDef> Protocol::find_field(const std::string& name) const {
    auto it = fieldNameIndex_.find(name);
    if (it != fieldNameIndex_.end()) {
        return fields_[it->second];
    }
    return std::nullopt;
}

std::optional<FieldDef> Protocol::get_field(const std::string& type,
                                                  uint16_t field_id) const {
    // Convert type name to FieldType
    auto field_type = FieldTypes::from_name(type);
    if (!field_type) {
        return std::nullopt;
    }
    
    uint32_t field_code = make_field_code(field_type->code, field_id);
    return get_field_by_code_opt(field_code);
}

const FieldDef* Protocol::get_field_by_code(uint32_t field_code) const {
    uint16_t type_code = get_field_type_code(field_code);
    uint16_t field_id = get_field_id(field_code);
    
    // Fast path for common cases
    if (type_code < 256 && field_id < 256) {
        return fast_lookup_[type_code][field_id];
    }
    
    // Slow path for rare cases
    auto it = fieldCodeIndex_.find(field_code);
    if (it != fieldCodeIndex_.end()) {
        return &fields_[it->second];
    }
    return nullptr;
}

std::optional<FieldDef> Protocol::get_field_by_code_opt(uint32_t field_code) const {
    const FieldDef* field = get_field_by_code(field_code);
    if (field) {
        return *field;
    }
    return std::nullopt;
}

std::optional<uint16_t> Protocol::get_type_code(const std::string& typeName) const {
    auto it = types_.find(typeName);
    if (it != types_.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::optional<std::string> Protocol::get_type_name(uint16_t typeCode) const {
    auto it = typeCodeToName_.find(typeCode);
    if (it != typeCodeToName_.end()) {
        return it->second;
    }
    return std::nullopt;
}

void Protocol::build_fast_lookup() {
    // Clear the lookup table
    std::memset(fast_lookup_, 0, sizeof(fast_lookup_));
    
    // Populate the fast lookup table
    for (const auto& field : fields_) {
        uint16_t type_code = get_field_type_code(field.code);
        uint16_t field_id = get_field_id(field.code);
        
        if (type_code < 256 && field_id < 256) {
            fast_lookup_[type_code][field_id] = &field;
        }
    }
}

} // namespace catl::xdata
