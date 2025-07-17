#!/usr/bin/env python3
"""
Generate C++ code from protocol definitions JSON files.
This script creates a C++ header file with embedded JSON definitions.
"""

import json
import sys
import argparse
from pathlib import Path
from typing import Dict, Any


def load_json_file(filepath: Path) -> Dict[str, Any]:
    """Load and parse a JSON file."""
    with open(filepath, "r") as f:
        return json.load(f)


def generate_cpp_header(
    definitions: Dict[str, Any], namespace: str = "catl::xdata"
) -> str:
    """Generate C++ header content with embedded JSON definition."""

    # Convert the dictionary back to a formatted JSON string
    json_str = json.dumps(definitions, indent=2)

    # Escape the JSON string for C++ string literal
    escaped_json = (
        json_str.replace("\\", "\\\\").replace('"', '\\"').replace("\n", '\\n"\n    "')
    )

    header_content = f"""// Auto-generated file. DO NOT EDIT.
// Generated from protocol definitions JSON

#pragma once

#include <string_view>

namespace {namespace} {{

// Embedded protocol definitions as a string literal
constexpr std::string_view EMBEDDED_DEFINITIONS = R"json({json_str})json";

}} // namespace {namespace}
"""

    return header_content


def main():
    parser = argparse.ArgumentParser(
        description="Generate C++ code from protocol definitions JSON"
    )
    parser.add_argument("input", type=Path, help="Input JSON file path")
    parser.add_argument("output", type=Path, help="Output C++ header file path")
    parser.add_argument(
        "--namespace",
        default="catl::xdata",
        help="C++ namespace (default: catl::xdata)",
    )

    args = parser.parse_args()

    if not args.input.exists():
        print(f"Error: Input file '{args.input}' does not exist", file=sys.stderr)
        sys.exit(1)

    try:
        # Load the JSON definitions
        definitions = load_json_file(args.input)

        # Generate the C++ header
        cpp_content = generate_cpp_header(definitions, args.namespace)

        # Ensure output directory exists
        args.output.parent.mkdir(parents=True, exist_ok=True)

        # Write the output file
        with open(args.output, "w") as f:
            f.write(cpp_content)

        print(f"Generated {args.output} from {args.input}")

    except json.JSONDecodeError as e:
        print(f"Error: Failed to parse JSON: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
