# CATL (Catalogue) Tools

A C++ toolkit for working with Xahaud catalogue files and SHAMap data structures.

## Overview

The CATL Tools project provides libraries and utilities for processing, validating, and analyzing CATL (Catalogue) files from Xahau. These files contain ledger data in a compact format, and the tools in this project allow for efficient processing and manipulation of this data. The project is designed to work with catalogue files as defined in [Xahaud's Catalogue implementation](https://github.com/Xahau/xahaud/blob/dev/src/ripple/rpc/handlers/Catalogue.cpp).

## Core Components

### Libraries / Modules

- **core**: Core types and utilities
- **shamap**: Implementation of SHAMap data structure, including snapshotting and support for hash compatible, collapsed trees
- **hasher**: Tools for verifying state map hashes in catalogue files, buildings trees from mmap'd leaf keys/data.
- **utils**: Tools for working with catalogue files

### Executables

- [**catl-hasher**](src/hasher/src/catl-hasher.cpp): Verify ledger state map hashes in CATL files
- [**catl-validator**](src/utils/src/catl-validator.cpp): Validate the integrity of CATL files
- [**catl-decomp**](src/utils/src/catl-decomp.cpp): Convert compressed CATL files to uncompressed format

## Key Concepts

### CATL v1 File Overview

A format for storing Xahau Ledger data with the following structure:

#### Header Structure
At the beginning of the file, the header structure is defined as follows:

```cpp
#pragma pack(push, 1)
struct CATLHeader
{
    uint32_t magic;              // "CATL" in LE
    uint32_t min_ledger;
    uint32_t max_ledger;
    uint16_t version;
    uint16_t network_id;
    uint64_t filesize;           // Total size of the file including header
    std::array<uint8_t, 64> hash;// SHA-512 hash
};
#pragma pack(pop)
```

For each ledger between min_ledger and max_ledger, the following structure is used:

#### Ledger Records
```cpp
struct LedgerInfo
{
    uint32_t sequence;
    uint8_t hash[32];
    uint8_t txHash[32];
    uint8_t accountHash[32];
    uint8_t parentHash[32];
    uint64_t drops;
    uint32_t closeFlags;
    uint32_t closeTimeResolution;
    uint64_t closeTime;
    uint64_t parentCloseTime;
};
```

#### SHAMap Structure

Each ledger contains two key data structures:

1. **State Map**: Contains account state data
    - For the first ledger in a file, this is a complete SHAMap with all account states
    - For subsequent ledgers, these are deltas (only changes from the previous ledger)

2. **Transaction Map**: Contains transaction data for that ledger

Both use a binary prefix tree (trie) structure where:
- Keys are 32-byte identifiers
- Tree nodes are selected by traversing 4-bit nibbles of the key
- Each inner node can have up to 16 child nodes (one for each nibble value)

#### Node Serialization Format

In CATL files, only leaf nodes and removal markers are explicitly serialized. Inner nodes are implicitly reconstructed when building the tree. Each serialized node follows this format:

1. **Node Type** (1 byte): Identifies what kind of node this is
   ```cpp
   enum SHAMapNodeType : uint8_t {
       tnINNER = 1,           // Inner node type (in-memory only, not serialized)
       tnTRANSACTION_NM = 2,  // Transaction, no metadata
       tnTRANSACTION_MD = 3,  // Transaction, with metadata
       tnACCOUNT_STATE = 4,   // Account state data
       tnREMOVE = 254,        // Removal marker (used in deltas)
       tnTERMINAL = 255       // End of serialization marker
   };
   ```

2. **Key** (32 bytes): The 32-byte identifier for this node
    - For leaf nodes, this is the actual item key
    - For tnREMOVE nodes, this identifies the key to remove

3. **Data Size** (4 bytes): A uint32_t specifying the size of the following data block
    - This is only present for leaf nodes (not for tnREMOVE nodes)

4. **Data** (variable length): The actual data for this node
    - For transactions, this contains the serialized transaction data
    - For account states, this contains the serialized account state data

Each map (state and transaction) ends with a tnTERMINAL node type marker to indicate the end of the serialized data.

For the state map deltas in subsequent ledgers, tnREMOVE nodes are used to indicate entries that should be removed from the previous state map.

## Building the Project

### Prerequisites

- CMake 3.12+
- C++20 compatible compiler (GCC 13+ or Clang 16+)
- Conan package manager (version 2)

### Building with Make

```bash
# Clone the repository
git clone [repository-url]
cd catalogue-tools

# Create build directory
mkdir -p build
cd build

# Run the Conan script to install dependencies
BUILD_TYPE=Debug ../scripts/conan.sh

# Build using CMake with Make
cmake -DCMAKE_TOOLCHAIN_FILE=conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Debug ..
make
```

### Building with Ninja

```bash
# Clone the repository
git clone [repository-url]
cd catalogue-tools

# Create build directory
mkdir -p build
cd build

# Run the Conan script to install dependencies
BUILD_TYPE=Debug ../scripts/conan.sh

# Build using CMake with Ninja
cmake -G "Ninja" -DCMAKE_TOOLCHAIN_FILE=conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Debug ..
ninja
```

## Usage Examples

### Verifying ledger state map hashes in a CATL file

```bash
./bin/catl-hasher my_file.catl
```

### Validating a CATL file

```bash
./bin/catl-validator my_file.catl
```

### Decompressing a compressed CATL file

```bash
./bin/catl-decomp my_file.catl my_file_decompressed.catl
```

## Testing

The project includes a test suite using Google Test:

```bash
# Build and run tests
cd build
ninja shamap_tests
./bin/shamap_tests
```

## Project Structure

- **./src/core**: Core types and utilities
- **./src/shamap**: SHAMap implementation
- **./src/hasher**: CATL file processing tools
- **./src/utils**: Additional utilities
- **./tests**: Test suite
- **./scripts**: Build and maintenance scripts
