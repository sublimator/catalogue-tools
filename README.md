# Catalogue Tools

A C++ toolkit for working with Xahaud catalogue files and SHAMap data structures.

## Overview

This project provides libraries and utilities for processing, validating, and analyzing CATL (Catalogue) files from Xahau. These files contain ledger data in a compact format, allowing for efficient storage and retrieval of blockchain state.

This project aims to explore three key approaches to blockchain data handling:

1. **Immutability as an optimization opportunity**: Since blockchain history never changes, we can design specialized data structures that take advantage of this property. Our implementation uses copy-on-write techniques that allow multiple ledger versions to share unchanged data, drastically reducing memory usage.

2. **Direct memory access by design**: Rippled/Xahaud in practice ends up loading the entirety of the latest ledger state into memory anyway, despite theoretical lazy-loading capabilities. Rather than fighting this reality, we embrace it with memory-mapped files and compact binary formats that are efficient both on disk and in RAM.

3. **Smart tree compaction**: We optimize storage by collapsing chains of inner nodes with single children, while maintaining compatibility with standard hash verification. This technique significantly reduces both storage requirements and traversal costs.

These approaches together enable rapid data access, reduced memory footprint, and efficient blockchain state verification.

## Core Components

- **SHAMap**: Binary prefix tree implementation with copy-on-write for efficient snapshots
- **Catalogue-v1**: Implementation of CATL file format parsing and validation
- **Hasher**: Tools for verifying state map hashes in catalogue files

## Executables

- **catl1-hasher**: Verify ledger state map hashes in CATL files
- **catl1-validator**: Validate the integrity of CATL files
- **catl1-decomp**: Convert compressed CATL files to uncompressed format

## Building the Project

### Prerequisites

- CMake 3.12+
- C++20 compatible compiler (GCC 13+ or Clang 16+)
- Conan package manager (version 2)

### Setup Python Environment

The project uses a Python virtual environment for some tooling:

```bash
# Create and activate the Python environment
source scripts/setup-catenv.sh
```

### Building with Ninja (recommended)

```bash
# Create build directory
mkdir -p build
cd build

# Install dependencies and configure
BUILD_TYPE=Debug ../scripts/conan.sh
cmake -G "Ninja" -DCMAKE_TOOLCHAIN_FILE=conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Debug ..

# Build the project
ninja
```

### Building with Make

```bash
# Create build directory
mkdir -p build
cd build

# Install dependencies and configure
BUILD_TYPE=Debug ../scripts/conan.sh
cmake -DCMAKE_TOOLCHAIN_FILE=conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Debug ..

# Build the project
make
```

## Running Tests

```bash
# Build and run all tests
cd build
ninja all_tests
./tests/all_tests
```

## Project Structure

- **src/core**: Core types and utilities
- **src/shamap**: SHAMap implementation
- **src/catalogue-common**: Common types for catalogue formats
- **src/catalogue-v1**: CATL v1 format implementation
- **src/hasher**: State map hash verification tools
- **src/utils**: Additional utilities
- **tests**: Test suite
