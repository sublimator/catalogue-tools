# Catalogue Tools

A C++ toolkit for working with Xahaud catalogue files and SHAMap data structures.

## Overview

This project provides libraries and utilities for processing, validating, and analyzing CATL (Catalogue) files from Xahau. These files contain ledger data in a compact format, allowing for efficient storage and retrieval of blockchain state.

## Core Components

- **SHAMap**: Binary prefix tree implementation with copy-on-write for efficient snapshots
- **Catalogue-v1**: Implementation of CATL file format parsing and validation
- **Hasher**: Tools for verifying state map hashes in catalogue files

## Executables

- **catl-hasher**: Verify ledger state map hashes in CATL files
- **catl-validator**: Validate the integrity of CATL files
- **catl-decomp**: Convert compressed CATL files to uncompressed format

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
