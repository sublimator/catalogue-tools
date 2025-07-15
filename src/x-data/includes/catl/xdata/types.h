#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

namespace catl::xdata {

// Network identifiers
namespace Networks {
inline constexpr uint32_t XRPL = 0;
inline constexpr uint32_t XAHAU = 21337;
// Add more networks as needed
}  // namespace Networks

struct FieldType
{
    std::string_view name;
    uint16_t code;
    std::optional<std::vector<uint32_t>> network_ids =
        std::nullopt;       // Default: universal
    size_t fixed_size = 0;  // 0 = variable/special handling required

    bool
    matches_network(uint32_t net_id) const
    {
        if (!network_ids.has_value())
            return true;  // Universal type
        const auto& ids = network_ids.value();
        return std::ranges::find(ids, net_id) != ids.end();
    }

    bool
    operator==(uint16_t c) const
    {
        return code == c;
    }
    bool
    operator==(const FieldType& other) const
    {
        return code == other.code;
    }
};

namespace FieldTypes {
// Special types
inline const FieldType NotPresent{"NotPresent", 0, std::nullopt, 0};
inline const FieldType Unknown{"Unknown", 65534, std::nullopt, 0};
inline const FieldType Done{
    "Done",
    65535,
    {{Networks::XRPL}},
    0};  // XRPL specific

// Common types (1-8) - Universal by default
inline const FieldType UInt16{"UInt16", 1, std::nullopt, 2};
inline const FieldType UInt32{"UInt32", 2, std::nullopt, 4};
inline const FieldType UInt64{"UInt64", 3, std::nullopt, 8};
inline const FieldType Hash128{"Hash128", 4, std::nullopt, 16};
inline const FieldType Hash256{"Hash256", 5, std::nullopt, 32};
inline const FieldType Amount{
    "Amount",
    6,
    std::nullopt,
    0};  // SPECIAL: 8 or 48 bytes
inline const FieldType Blob{"Blob", 7, std::nullopt, 0};  // VL encoded
inline const FieldType AccountID{"AccountID", 8, std::nullopt, 0};

// 9-13 reserved
inline const FieldType Number{
    "Number",
    9,
    {{Networks::XRPL}},
    12};  // XRPL specific, 12 bytes (8 byte mantissa + 4 byte exponent)

// Container types - Universal
inline const FieldType STObject{"STObject", 14, std::nullopt, 0};
inline const FieldType STArray{"STArray", 15, std::nullopt, 0};

// Uncommon types (16-26)
inline const FieldType UInt8{"UInt8", 16, std::nullopt, 1};
inline const FieldType Hash160{"Hash160", 17, std::nullopt, 20};
inline const FieldType PathSet{
    "PathSet",
    18,
    std::nullopt,
    0};  // SPECIAL: state machine termination
inline const FieldType Vector256{
    "Vector256",
    19,
    std::nullopt,
    0};  // VARIABLE: VL count + n*32 bytes
inline const FieldType UInt96{"UInt96", 20, std::nullopt, 12};
inline const FieldType Hash192{"Hash192", 21, std::nullopt, 24};
inline const FieldType UInt384{"UInt384", 22, std::nullopt, 48};
inline const FieldType UInt512{"UInt512", 23, std::nullopt, 64};

// Network-specific types
inline const FieldType Issue{
    "Issue",
    24,
    {{Networks::XRPL}},
    0};  // SPECIAL: 20 bytes (XRP) or 40 bytes (currency + issuer)
inline const FieldType XChainBridge{
    "XChainBridge",
    25,
    {{Networks::XRPL}},
    40};  // 20 + 20 account IDs

// TODO: we don't actually know this type yet, it might not be the 160-bit
// encoding per the Amount
inline const FieldType Currency{"Currency", 26, {{Networks::XRPL}}, 20};

// High level types (cannot be serialized inside other types)
inline const FieldType Transaction{"Transaction", 10001, std::nullopt, 0};
inline const FieldType LedgerEntry{"LedgerEntry", 10002, std::nullopt, 0};
inline const FieldType Validation{"Validation", 10003, std::nullopt, 0};
inline const FieldType Metadata{"Metadata", 10004, std::nullopt, 0};

// Helper array for iteration/lookup
inline const std::array ALL = {
    NotPresent,  UInt16,     UInt32,    UInt64,       Hash128,  Hash256,
    Amount,      Blob,       AccountID, Number,       STObject, STArray,
    UInt8,       Hash160,    PathSet,   Vector256,    UInt96,   Hash192,
    UInt384,     UInt512,    Issue,     XChainBridge, Currency, Transaction,
    LedgerEntry, Validation, Metadata,  Unknown,      Done};

// Efficient lookup by code
inline std::optional<FieldType>
from_code(uint16_t code)
{
    for (const auto& ft : ALL)
    {
        if (ft.code == code)
        {
            return ft;
        }
    }
    return std::nullopt;
}

// Lookup by name (less common, so string comparison is ok)
inline std::optional<FieldType>
from_name(std::string_view name)
{
    for (const auto& ft : ALL)
    {
        if (ft.name == name)
        {
            return ft;
        }
    }
    return std::nullopt;
}
}  // namespace FieldTypes

}  // namespace catl::xdata
