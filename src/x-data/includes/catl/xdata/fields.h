#pragma once

#include "catl/xdata/types.h"
#include <string>
#include <cstdint>

namespace catl::xdata {

// Forward declaration
class TypeParser;

// Field metadata matching TypeScript interface
struct FieldMeta {
    bool is_serialized;
    bool is_signing_field;
    bool is_vl_encoded;
    uint16_t nth;  // field ID within its type
    FieldType type;  // The actual type (was string)
};

// Field definition with name and metadata
struct FieldDef {
    std::string name;
    FieldMeta meta;
    uint32_t code;  // Combined type code (upper 16 bits) and field ID (lower 16 bits)
};

// Helper to calculate field code from type and field ID
inline uint32_t make_field_code(uint16_t typeCode, uint16_t fieldId) {
    return (static_cast<uint32_t>(typeCode) << 16) | fieldId;
}

// Helper to extract type code from field code
inline uint16_t get_field_type_code(uint32_t fieldCode) {
    return static_cast<uint16_t>(fieldCode >> 16);
}

// Helper to extract field ID from field code
inline uint16_t get_field_id(uint32_t fieldCode) {
    return static_cast<uint16_t>(fieldCode & 0xFFFF);
}

} // namespace catl::xdata
