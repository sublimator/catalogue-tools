#pragma once

#include <string_view>
#include <cstdint>
#include <array>
#include <optional>

namespace catl::xdata {

struct FieldType {
    std::string_view name;
    uint16_t code;
    
    constexpr bool operator==(uint16_t c) const { return code == c; }
    constexpr bool operator==(const FieldType& other) const { return code == other.code; }
};

namespace FieldTypes {
    // Special types
    constexpr FieldType NotPresent{"NotPresent", 0};
    constexpr FieldType Unknown{"Unknown", 65534};
    constexpr FieldType Done{"Done", 65535};
    
    // Common types (1-8)
    constexpr FieldType UInt16{"UInt16", 1};
    constexpr FieldType UInt32{"UInt32", 2};
    constexpr FieldType UInt64{"UInt64", 3};
    constexpr FieldType Hash128{"Hash128", 4};  // was STI_UINT128
    constexpr FieldType Hash256{"Hash256", 5};  // was STI_UINT256
    constexpr FieldType Amount{"Amount", 6};    // SPECIAL: 8 or 48 bytes
    constexpr FieldType Blob{"Blob", 7};        // was STI_VL
    constexpr FieldType AccountID{"AccountID", 8}; // was STI_ACCOUNT
    
    // 9-13 reserved
    constexpr FieldType Number{"Number", 9};
    
    // Container types
    constexpr FieldType STObject{"STObject", 14};
    constexpr FieldType STArray{"STArray", 15};
    
    // Uncommon types (16-26)
    constexpr FieldType UInt8{"UInt8", 16};
    constexpr FieldType Hash160{"Hash160", 17};    // was STI_UINT160
    constexpr FieldType PathSet{"PathSet", 18};    // SPECIAL: state machine termination
    constexpr FieldType Vector256{"Vector256", 19}; // VARIABLE: VL count + n*32 bytes
    constexpr FieldType UInt96{"UInt96", 20};
    constexpr FieldType Hash192{"Hash192", 21};    // was STI_UINT192
    constexpr FieldType UInt384{"UInt384", 22};
    constexpr FieldType UInt512{"UInt512", 23};
    constexpr FieldType Issue{"Issue", 24};
    constexpr FieldType XChainBridge{"XChainBridge", 25};
    constexpr FieldType Currency{"Currency", 26};
    
    // High level types (cannot be serialized inside other types)
    constexpr FieldType Transaction{"Transaction", 10001};
    constexpr FieldType LedgerEntry{"LedgerEntry", 10002};
    constexpr FieldType Validation{"Validation", 10003};
    constexpr FieldType Metadata{"Metadata", 10004};
    
    // Helper array for iteration/lookup
    constexpr std::array ALL = {
        NotPresent, UInt16, UInt32, UInt64, Hash128, Hash256, Amount, Blob, 
        AccountID, Number, STObject, STArray, UInt8, Hash160, PathSet, 
        Vector256, UInt96, Hash192, UInt384, UInt512, Issue, XChainBridge, 
        Currency, Transaction, LedgerEntry, Validation, Metadata, Unknown, Done
    };
    
    // Efficient lookup by code
    constexpr std::optional<FieldType> from_code(uint16_t code) {
        for (const auto& ft : ALL) {
            if (ft.code == code) {
                return ft;
            }
        }
        return std::nullopt;
    }
    
    // Lookup by name (less common, so string comparison is ok)
    inline std::optional<FieldType> from_name(std::string_view name) {
        for (const auto& ft : ALL) {
            if (ft.name == name) {
                return ft;
            }
        }
        return std::nullopt;
    }
}

} // namespace catl::xdata
