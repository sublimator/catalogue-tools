#pragma once

#include "catl/xdata/fields.h"
#include "catl/xdata/types.h"
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace catl::xdata {

// Protocol loading options
struct ProtocolOptions
{
    std::optional<uint32_t> network_id;  // Which network we're parsing for
    bool allow_vl_inference = true;      // Safe unknown type handling
};

// Protocol definitions matching XRPLDefinitions interface
class Protocol
{
public:
    // Load definitions from JSON file with options
    static Protocol
    load_from_file(const std::string& path, ProtocolOptions opts = {});

    // Access field definitions
    const std::vector<FieldDef>&
    fields() const
    {
        return fields_;
    }

    // Find field by name
    std::optional<FieldDef>
    find_field(const std::string& name) const;

    // Get field by type and field ID
    std::optional<FieldDef>
    get_field(const std::string& type, uint16_t fieldId) const;

    // Get field by field code (optimized with lookup table)
    const FieldDef*
    get_field_by_code(uint32_t field_code) const;
    std::optional<FieldDef>
    get_field_by_code_opt(uint32_t field_code) const;

    // Access type mappings
    const std::unordered_map<std::string, uint16_t>&
    types() const
    {
        return types_;
    }

    // Access ledger entry types
    const std::unordered_map<std::string, uint16_t>&
    ledgerEntryTypes() const
    {
        return ledgerEntryTypes_;
    }

    // Access transaction types
    const std::unordered_map<std::string, uint16_t>&
    transactionTypes() const
    {
        return transactionTypes_;
    }

    // Access transaction results
    const std::unordered_map<std::string, int32_t>&
    transactionResults() const
    {
        return transactionResults_;
    }

    // Get type code for type name
    std::optional<uint16_t>
    get_type_code(const std::string& typeName) const;

    // Get type name for type code
    std::optional<std::string>
    get_type_name(uint16_t typeCode) const;

    // Check if a type was inferred as VL-encoded
    bool
    is_inferred_vl_type(uint16_t type_code) const
    {
        return inferred_vl_types_.count(type_code) > 0;
    }

private:
    // Network this protocol was loaded for (if specified)
    std::optional<uint32_t> network_id_;

    // Field definitions array
    std::vector<FieldDef> fields_;

    // Types that were inferred as VL-encoded during loading
    std::unordered_set<uint16_t> inferred_vl_types_;

    // Fast lookup table for common cases (type < 256, field_id < 256)
    // Uses raw pointers for speed, nullptr = not present
    const FieldDef* fast_lookup_[256][256] = {};

    // Type name to code mappings
    std::unordered_map<std::string, uint16_t> types_;

    // Reverse mapping for type lookup
    std::unordered_map<uint16_t, std::string> typeCodeToName_;

    // Ledger entry type mappings
    std::unordered_map<std::string, uint16_t> ledgerEntryTypes_;

    // Transaction type mappings
    std::unordered_map<std::string, uint16_t> transactionTypes_;

    // Transaction result mappings
    std::unordered_map<std::string, int32_t> transactionResults_;

    // Field lookup indices for performance
    std::unordered_map<std::string, size_t> fieldNameIndex_;
    std::unordered_map<uint32_t, size_t> fieldCodeIndex_;  // key: field code

    // Build the fast lookup table after loading
    void
    build_fast_lookup();

    // Validate type during loading
    void
    validate_type(uint16_t type_code, const ProtocolOptions& opts);

    // Check if all fields of a type are VL-encoded
    bool
    can_infer_vl_type(uint16_t type_code) const;

    // Find known type in FieldTypes::ALL
    std::optional<FieldType>
    find_known_type(uint16_t type_code) const;
};

}  // namespace catl::xdata
