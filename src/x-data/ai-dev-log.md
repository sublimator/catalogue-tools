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


# X-Data Network-Aware Type System Design Sketch

## Core Architecture

### Type Identity
The true identity of a type is the **`(network_ids, type_code)` composite**, NOT the string name.
- Type code 25 might mean "XChainBridge" on XRPL but "HookDefinition" on another network
- The same type code could have different meanings on different networks
- A type's network scope (universal vs network-specific) is part of its identity

### Network IDs
```cpp
namespace Networks {
    constexpr uint32_t XRPL = 0;
    constexpr uint32_t XAHAU = 21337;
    // Add more networks as needed
}
```
- **std::nullopt**: Universal types that work on all networks
- Fields can belong to multiple networks via vector

## Implementation

### Type Definition
```cpp
// types.h
struct FieldType {
    std::string_view name;
    uint16_t code;
    std::optional<std::vector<uint32_t>> network_ids = std::nullopt;  // Default: universal
    
    bool matches_network(uint32_t net_id) const {
        if (!network_ids.has_value()) return true;  // Universal type
        const auto& ids = network_ids.value();
        return std::find(ids.begin(), ids.end(), net_id) != ids.end();
    }
};

namespace FieldTypes {
    // Universal types (nullopt by default - no need to specify)
    constexpr FieldType UInt32{"UInt32", 2};
    constexpr FieldType Hash256{"Hash256", 5};
    constexpr FieldType Amount{"Amount", 6};
    
    // Single network types
    constexpr FieldType XChainBridge{"XChainBridge", 25, {{Networks::XRPL}}};
    constexpr FieldType HookData{"HookData", 27, {{Networks::XAHAU}}};
    
    // Multi-network types (belongs to multiple networks)
    constexpr FieldType SomeField{"SomeField", 30, {{Networks::XRPL, Networks::XAHAU}}};
    
    // Registry of all parseable types
    constexpr std::array ALL = { /* all types we know how to parse */ };
}
```

### Protocol Loading Options
```cpp
// protocol.h - Clean and simple
struct ProtocolOptions {
    std::optional<uint32_t> network_id;  // Which network we're parsing for
    bool allow_vl_inference = true;      // Safe unknown type handling
};

class Protocol {
public:
    static Protocol load_from_file(const std::string& path, 
                                   ProtocolOptions opts = {});
};
```

## The Two-Layer Architecture

### Layer 1: Parseable Types (types.h)
- Types your parser **knows how to parse**
- Hardcoded with their parsing logic
- Each can belong to all networks (nullopt) or specific networks (vector)
- The `ALL` array is the registry of parseable types

### Layer 2: Declared Types (protocol.json)
- Types that **exist in the protocol**
- Loaded dynamically at runtime
- Safe VL inference for unknown types (if all instances are VL-encoded)
- Strict validation against Layer 1 types

## Validation Implementation

```cpp
class Protocol {
private:
    void validate_type(uint16_t type_code, const ProtocolOptions& opts) {
        auto known_type = find_known_type(type_code);
        
        if (!known_type) {
            // Unknown type - check if we can safely infer VL encoding
            if (opts.allow_vl_inference && can_infer_vl_type(type_code)) {
                log_info("Inferred type {} as VL-encoded", type_code);
                add_inferred_vl_type(type_code);
            } else {
                throw std::runtime_error("Unknown type " + std::to_string(type_code) + 
                    " - cannot parse safely");
            }
        } else if (opts.network_id.has_value()) {
            // Known type - verify network compatibility
            if (!known_type->matches_network(opts.network_id.value())) {
                throw std::runtime_error("Type " + std::string(known_type->name) + 
                    " not valid for network " + std::to_string(opts.network_id.value()));
            }
        }
    }
    
    bool can_infer_vl_type(uint16_t type_code) {
        // Check ALL fields with this type
        size_t vl_count = 0, total_count = 0;
        for (const auto& field : fields_) {
            if (field.meta.type.code == type_code) {
                total_count++;
                if (field.meta.is_vl_encoded) vl_count++;
            }
        }
        // Safe ONLY if ALL fields of this type are VL-encoded
        return total_count > 0 && vl_count == total_count;
    }
};
```

## Critical Limitation: Unknown Types

**YOU CANNOT SKIP UNKNOWN TYPES** in the XRPL binary format because:
- No length prefixes on fields
- No unique delimiter sequences
- End markers are just more fields (which you need to parse to find)
- Size depends on type (fixed, VL-encoded, Amount-special, PathSet-special)

Once you hit an unknown type, parsing is IMPOSSIBLE. Your only options:

1. **Know the type** (it's in the ALL array)
2. **Safely infer VL-encoding** (if ALL fields of that type have `is_vl_encoded=true`)
3. **Crash** (the safe default)

There is no "skip and continue" option. This is a fundamental limitation of the binary format.

## Usage Examples

```cpp
// Default - strict with safe VL inference
auto protocol = Protocol::load_from_file("xrpl.json", {.network_id = Networks::XRPL});

// Disable VL inference for maximum strictness
auto protocol = Protocol::load_from_file("xrpl.json", {
    .network_id = Networks::XRPL,
    .allow_vl_inference = false  // Crash on ANY unknown type
});

// Load all networks (for analysis tools)
auto protocol = Protocol::load_from_file("definitions.json");
// network_id is nullopt - accepts all networks
```

## Validation Flow

1. Load protocol.json with specific options
2. For each type in JSON:
   - Check if it exists in `FieldTypes::ALL`
   - If found: verify network compatibility
   - If not found and `allow_vl_inference`: check if safely inferable
   - Otherwise: crash (strict by default)
3. For unknown types with VL inference enabled:
   - Check if ALL fields with that type have `is_vl_encoded=true`
   - If yes: add to runtime VL type registry
   - If no: crash (unsafe to parse)
4. Build fast lookup tables for known types

## Key Design Decisions

1. **Strict by default**: Unknown types crash unless safely VL-inferable
2. **Network scoping**: Types can belong to one, many, or all networks
3. **Simple options**: Just network_id and allow_vl_inference flag
4. **No unsafe modes**: Removed YOLO/adult mode - either it's safe or it crashes
5. **Clean syntax**: Implicit defaults, network names not numbers
6. **Multi-network support**: Fields can belong to multiple networks via vector

## The Bottom Line

This design provides:
- Type safety (parser knows exactly what it can handle)
- Network awareness (proper multi-network support)
- Future-proofing (safe VL inference for new fields)
- Clear failure modes (no silent corruption)
- Clean, professional API (no joke fields)

But remember: **once you hit an unknown type during parsing, you're stuck unless it's VL-encoded**. This isn't a limitation of our design - it's a fundamental property of the XRPL binary format. The only safe escape hatch is VL inference, and we use it by default.



### Session: Performance Deep Dive - The std::string Tax
*Date: 2025-05-26*

**The Quest for 200 MB/s Debug Output**

Started with a simple goal: make debug output faster. Ended up discovering why Jon Skinner wrote custom string classes for Sublime Text.

**Performance Hierarchy Discovered**:
- **650 MB/s** - SimpleSliceEmitter (just calling a lambda with field slices)
- **498 MB/s** - CountingVisitor (calculating output sizes)
- **430 MB/s** - CountingVisitor + hex encoding to scratch buffer
- **375 MB/s** - CountingVisitor + full formatted output (raw char* pointers)
- **39 MB/s** - DebugTreeVisitor + full formatted output (std::string)

That's a **10x performance penalty** just for using std::string instead of raw pointers!

**Optimizations Attempted**:
1. **Fast hex encoding** - 256-entry lookup table (uint16_t per byte)
2. **Pre-computed indentation** - Static lookup table for indent strings
3. **1MB output buffer** - Reduce write() syscalls
4. **Direct-to-buffer hex** - Skip intermediate string creation
5. **SIMD experiments** - SSE/NEON for hex encoding (marginal gains)

**The Smoking Gun**: CountingVisitor
Built a test visitor that does ALL the work (formatting, hex encoding, building complete output) but writes to a fixed char buffer instead of std::string. Result: 375 MB/s vs 39 MB/s for identical output.

**Where std::string Falls Down**:
- `resize()` - Capacity checks and potential reallocation
- `append()` - Length tracking and bounds checking
- Small string optimization checks
- Multiple abstraction layers
- Safety features we don't need here

**Key Insight**: The actual work (hex encoding, formatting) is blazing fast. It's the string manipulation that's the bottleneck. When processing gigabytes of data, a 10x slowdown is the difference between usable and unusable.

**Future Ideas**:
- Pre-compute common string fragments ("Account: ", "Amount: ", etc.)
- Arena allocator for all string data
- Custom string class optimized for append-only operations
- Direct memory-mapped output files

**Lesson Learned**: Don't accept "that's just how fast text output is" as an answer. Sometimes there's a 10x speedup hiding behind the standard library abstractions.

---

*"375 MB/s with raw pointers, 39 MB/s with std::string. That's disgusting, isn't it?"* - The realization that safe defaults aren't always the right defaults.
