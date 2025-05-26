# X-Data Module Development Log

## Mission Statement
Build a generic XRPL/Xahau binary parser for ground-truth data analysis. Stop guessing what patterns exist in blockchain data - parse it and measure directly.

## Current Status (2025-05-25)

### What We've Built
- **Protocol Definition Loader** - Parses `definitions.json` to understand XRPL field structure
- **Field Type System** - Efficient constexpr type definitions matching protocol.json
- **Optimized Field Lookup** - 256x256 lookup table for O(1) field access (99% of cases)
- **Field Lookup System** - Fast access by name, type+ID, or field code
- **Binary Parser** - Complete zero-copy parser with visitor pattern
- **CLI Tool** - Basic protocol exploration (`--list-fields`, `--find-field`, `--stats`)

### Why We're Building This

After extensive compression experiments, we discovered:
1. **Bulk compression achieves 7.15x** on catalogue files
2. **Per-leaf compression only gets 1.5x** with ZSTD dictionaries
3. **The gap is cross-leaf patterns** - accounts/currencies repeating thousands of times

ZSTD can't capture these patterns because it treats each leaf independently. We need to:
- Know exactly what's repeating and how often
- Understand the true distribution of fields and values
- Design compression that leverages XRPL domain knowledge

### Immediate Goals

1. **Binary Parser** - Read actual ledger data using protocol definitions
2. **Statistics Collection** - Track:
   - Account frequency (which appear 50k+ times?)
   - Currency distribution (USD/XRP dominate by how much?)
   - Field usage patterns (which fields appear in 90%+ of objects?)
   - Structural patterns (payment vs DEX vs escrow transactions)
3. **Evolution Analysis** - How patterns change from ledger 1 to 5M+

### Data Structure

```cpp
// Field types from protocol.json (constexpr for efficiency)
namespace FieldTypes {
    constexpr FieldType UInt32{"UInt32", 2};
    constexpr FieldType Amount{"Amount", 6};
    constexpr FieldType AccountID{"AccountID", 8};
    // ... all 29 types
}

// Field codes are 32-bit: [TypeCode:16][FieldID:16]
// Example: AccountID = 0x80001 (type 8, field 1)
struct FieldDef {
    std::string name;        // "AccountID"
    FieldMeta metadata;      // serialization info
    uint32_t code;          // 0x80001
};
```

### Performance Optimizations

**256x256 Lookup Table**
- Since type codes and field IDs are small integers (< 256 for common fields)
- Direct array access: `fast_lookup_[type_code][field_id]`
- Avoids hash map overhead for 99% of field lookups
- 256KB stays hot in L2 cache during parsing
- Falls back to hash map for rare high-numbered fields

**Parser optimizations**:
- Zero-copy SliceCursor over immutable data
- Single-pass parsing (no pre-scanning)
- Skip functions only called when needed
- Template-based visitor for inlined calls

### Next Steps

1. **Implement Binary Parser**
   - VL (Variable Length) decoding for amounts, arrays
   - STObject recursive parsing
   - Handle unknown fields gracefully

2. **Add Statistics Collectors**
   ```cpp
   class FieldStats {
       std::unordered_map<uint32_t, uint64_t> fieldCounts;
       std::unordered_map<Hash256, uint64_t> accountFreq;
       std::unordered_map<Currency, uint64_t> currencyFreq;
   };
   ```

3. **Process Test Data**
   - Start with that 16GB slice (ledgers 4.5M-5M)
   - Generate frequency reports
   - Validate compression hypotheses

### Future Vision

Once we have real data:
- **Option A**: Custom XRPL compressor with domain-specific dictionaries
- **Option B**: Semantic compression via new field codes (STI_AMOUNT_COMPRESSED)
- **Option C**: Hybrid approach leveraging both strategies

The key insight: **Generic compression loses to domain knowledge**. By understanding XRPL data patterns at a deep level, we can achieve the 7x+ compression of bulk methods while maintaining random access.

### Technical Notes

- Using Boost.JSON for protocol loading (already in the project)
- Protocol-agnostic design - works with both XRPL and Xahau
- Building on catl::core types (Hash256, Slice, etc.)

### Technical Decisions

- **Renamed structs for brevity**: `FieldMetadata` → `FieldMeta`, `FieldDefinition` → `FieldDef`
- **Field codes are 32-bit**: Upper 16 bits = type code, lower 16 bits = field ID
- **Type system**: Constexpr FieldType structs for compile-time efficiency
- **Fast lookup optimization**: 256x256 array for O(1) field access (256KB, fits in L2 cache)
- **Protocol.json is authoritative**: All names/codes match the JSON definitions exactly
- **Protocol-agnostic**: Works with any protocol following the XRPL definitions format

### Parser Architecture

**Path-based visitor pattern**:
- Parser maintains a FieldPath (stack of fields) as it descends
- Visitors get full context at each callback (e.g., "Payment/Amount" vs "Fee/Amount")
- No wasted work - parse once, skip only when visitor returns false
- Template-based for zero virtual dispatch overhead

**Special type handling**:
- Amount: Variable size (8 or 48 bytes) based on first byte
- PathSet: State machine with 0x00 terminator, not VL-encoded
- Vector256: Standard VL-encoded, no special handling needed

### Commands

```bash
# Build
cmake --build build --target catl_xdata_cli

# Run
./build/src/x-data/catl_xdata_cli --protocol definitions.json --stats
./build/src/x-data/catl_xdata_cli --list-fields | grep Account
```

### Protocol.json Philosophy
*Date: 2025-05-25*

**Historical context**: Protocol.json was originally created as a validation tool - to check if ripple-lib-java had declared all the fields that rippled had. It was never meant to be a complete parsing specification.

**What protocol.json provides**:
- Field definitions (what fields exist)
- Type codes (name to number mappings)
- Basic metadata (is_serialized, is_signing, etc.)

**What it DOESN'T provide**:
- How to parse each type
- Size information (Amount can be 8 or 48 bytes)
- Special parsing rules (PathSet state machine)
- Which types are VL-encoded

**The realization**: You can't truly have a protocol-agnostic parser because parsing logic is inherently type-specific. Protocol.json is asking "do you know how to parse these types?" not telling you how.

**Our approach**:
- Load fields dynamically from protocol.json ✅
- Hardcode type parsing logic ✅  
- Throw on unknown types (fail fast) ✅

This is the right balance - flexible for fields, correct for types, safe for unknowns.

### Session Summary: Parser Implementation Complete
*Date: 2025-05-25*

**What we built today**:
1. Complete binary parser with path-tracking visitor pattern
2. Zero-copy SliceCursor for efficient traversal  
3. Special handling for XRPL types (Amount, PathSet)
4. Single-pass parsing with skip-on-demand
5. Template-based visitors for zero overhead

**Key design decisions**:
- Path-based context (visitors know where they are in the tree)
- Parse once, skip only when visitor returns false
- Domain-specific type handling baked in
- Fail fast on unknown types

**Ready for**: Building statistics collectors on top of this foundation. The parser can handle whatever analysis we throw at it - account frequency, currency distribution, pattern mining, etc.

---

*"Sometimes you have to parse a few billion ledger entries to realize the answer was domain-specific compression all along."* - The journey continues...
