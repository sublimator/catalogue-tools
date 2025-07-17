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
from datetime import datetime


def load_json_file(filepath: Path) -> Dict[str, Any]:
    """Load and parse a JSON file."""
    with open(filepath, "r") as f:
        return json.load(f)


def generate_cpp_header(
    definitions: Dict[str, Any], namespace: str = "catl::xdata"
) -> str:
    """Generate C++ header content with embedded JSON definition."""

    # Convert the dictionary to a compact JSON string (no indentation)
    json_str = json.dumps(definitions, separators=(',', ':'))
    
    # Split into chunks of reasonable size
    # Target chunk size around 32000 chars to stay well under 65536 limit
    chunk_size = 32000
    chunks = []
    
    for i in range(0, len(json_str), chunk_size):
        chunk = json_str[i:i+chunk_size]
        chunks.append(f'    R"json({chunk})json"')
    
    # Wrap chunks in std::string() for concatenation
    wrapped_chunks = [f'    std::string({chunk})' for chunk in chunks]
    
    # Join chunks with string concatenation
    json_literal = '\n    +\n'.join(wrapped_chunks)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
    
    header_content = f"""// Auto-generated file. DO NOT EDIT.
// Generated from protocol definitions JSON
// Generated at: {timestamp}

#pragma once

#include <string>

namespace {namespace} {{

// Embedded protocol definitions as a string
// Split into chunks to avoid compiler string literal length limits
inline const std::string EMBEDDED_DEFINITIONS = 
{json_literal};

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
