# xdata-json

JSON parsing helpers for the xdata module.

## Purpose

This module provides reusable JSON parsing functionality that was previously duplicated across multiple tools (catl1-to-catl2, nudb-exp, etc.).

By separating JSON-specific code from the core `xdata` module, we:
- Keep xdata dependency-free (just visitor pattern)
- Centralize all JSON parsing logic in one place
- Eliminate code duplication
- Provide a clean API for common parsing tasks

## Components

### parse_leaf.h
Parse account state/SLE leaf nodes to JSON.

```cpp
#include "catl/xdata-json/parse_leaf.h"

boost::json::value json = catl::xdata::json::parse_leaf(leaf_data, protocol);
```

### parse_transaction.h
Parse transaction leaf nodes (with metadata) to JSON.

```cpp
#include "catl/xdata-json/parse_transaction.h"

// Returns: {"tx": {...}, "meta": {...}}
boost::json::value json = catl::xdata::json::parse_transaction(tx_data, protocol);
```

### pretty_print.h
Pretty-print JSON values with proper indentation.

```cpp
#include "catl/xdata-json/pretty_print.h"

catl::xdata::json::pretty_print(std::cout, json_value);
```

## Dependencies

- `catl_xdata` (core xdata parsing)
- `catl_base58` (for address encoding)
- `Boost::json`

## Usage

Link against `catl_xdata_json`:

```cmake
target_link_libraries(your_target
    PRIVATE catl_xdata_json
)
```

This transitively brings in `catl_xdata`, `catl_base58`, and `Boost::json`.
