# Serialized Inner Nodes Design Document

## Overview

This document explores an efficient binary serialization format for SHAMap inner nodes that enables:
- Compact on-disk representation
- Fast deserialization with parallel loading support
- Structural sharing between related trees (Copy-on-Write)
- Efficient memory-mapped access patterns

## Core Design Decisions

### 1. 6-Byte Inner Node Format

We chose 6 bytes (2+4) over the minimal 5 bytes (1+4) for several reasons:

- **Alignment**: Better CPU memory access efficiency through word alignment
- **Cache Efficiency**: 10 nodes fit perfectly in a 64-byte cache line
- **Future Extensibility**: 10 reserved bits for future features
- **Padding Reduction**: Avoids wasted space in arrays
- **Acceptable Overhead**: Only 20% size increase for significant benefits

### 2. Depth-First Serialization

The depth-first approach provides:

- **Locality of Reference**: Related nodes stored near each other
- **Cache Efficiency**: Tree traversal benefits from spatial locality
- **Natural Recursion**: Matches tree algorithm patterns
- **Predictable Access**: Enables efficient prefetching

### 3. 64-bit Offsets

Despite the "640K ought to be enough" temptation to use 32-bit offsets:

- **Large File Support**: Modern ledger histories exceed 4GB
- **Cross-Map References**: Can point to nodes in previous ledgers
- **Future-Proof**: Avoids painful migration later
- **Structural Sharing**: Enables references across time

## Serialization Strategy

### Node Layout

```
[Inner Node]
├── Header (6 bytes)
│   ├── Depth + Flags (2 bytes)
│   └── Child Types (4 bytes, 2 bits × 16)
├── Child Slots (variable)
│   └── Only for non-empty children
└── Leaf Children (inline)
    └── Stored immediately after parent
```

### Key Insights

1. **Leaves First vs. Inners First**: We chose to interleave leaves with their parent inner nodes rather than separate sections. This maintains locality but sacrifices some parallelism potential.

2. **Parallel Loading**: To enable parallel deserialization, we need bookmarks at key boundaries (e.g., depth=1 nodes) so threads can independently process subtrees.

3. **Structural Sharing**: Nodes can reference data from previous ledgers via offsets, enabling efficient incremental storage.

## File Format Architecture

```
[Header]
  ├── Magic, Version, Metadata
  ├── Body Size
  └── Footer Size

[Body]
  └── For each ledger:
      ├── LedgerInfo
      ├── Account State Map
      │   └── Depth-first serialized nodes
      └── Transaction Map
          └── Depth-first serialized nodes

[Footer]
  ├── Ledger Lookup Table (sorted)
  └── Optional: Key-prefix index
```

## Integration with Copy-on-Write

The serialization format works seamlessly with CoW:

1. **Immutable Nodes**: Once serialized, nodes never change
2. **Offset References**: New trees can reference existing nodes
3. **Incremental Storage**: Only serialize changed nodes
4. **Snapshot Support**: Multiple versions can share serialized data

## Performance Considerations

### Memory Usage

- **Inner Node**: 6 bytes + (populated_children × 8 bytes for offsets)
- **Leaf Node**: 32 bytes (key) + variable data
- **Typical Compression**: 60-80% size reduction from in-memory format

### Access Patterns

1. **Sequential Scan**: Optimal for full tree traversal
2. **Random Access**: Requires offset index for efficiency
3. **Partial Loading**: Can load subtrees independently
4. **Memory Mapping**: Direct access without deserialization

## Implementation Roadmap

### Phase 1: Serialization (Current)
- [x] Define binary format structures
- [x] Implement depth-first traversal
- [ ] Write actual binary output
- [ ] Add offset bookkeeping

### Phase 2: Deserialization
- [ ] Sequential reader implementation
- [ ] Parallel loading with bookmarks
- [ ] Memory-mapped access layer
- [ ] Structural sharing support

### Phase 3: Integration
- [ ] CATL file format integration
- [ ] CoW compatibility layer
- [ ] Performance benchmarking
- [ ] Production hardening

## Open Questions

1. **Compression**: Should we compress leaf data? Per-node or per-block?
2. **Indexing**: What key-prefix granularity for random access?
3. **Versioning**: How to handle format evolution?
4. **Checksums**: Per-node or per-block integrity checks?

## Conclusion

This serialization format promises significant improvements in storage efficiency and load times while maintaining compatibility with the existing SHAMap structure. The depth-first layout with inline leaves provides an optimal balance between space efficiency and access performance.