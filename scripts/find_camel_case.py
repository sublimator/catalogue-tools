#!/usr/bin/env python3
"""
Find camelCase variables in C++ source files

This script scans C++ source files for variable names that use camelCase naming
convention (starting with lowercase, contains uppercase, not snake_case).

Usage:
    python find_camel_case.py [path]

If path is not provided, it defaults to 'src'
"""

import os
import re
import sys
from pathlib import Path
import concurrent.futures
import argparse

# Regular expressions for camelCase detection
# Looks for variables that start with lowercase and contain uppercase
# Main pattern for variables: handles Type varName patterns
VAR_PATTERN = re.compile(
    r"(^|\s+)([a-zA-Z0-9_:]+(?:<[^>]+>)?(?:::)?[a-zA-Z0-9_:]+)\s+([a-z][a-z0-9_]*[A-Z][a-zA-Z0-9_]*)\s*[;=({]"
)

# Additional pattern for member variables with this->camelCase
THIS_PATTERN = re.compile(r"this->([a-z][a-z0-9_]*[A-Z][a-zA-Z0-9_]*)")

# Pattern for class members: captures className::methodName() { ... varName = ... }
CLASS_METHOD_VAR_PATTERN = re.compile(
    r"([a-zA-Z0-9_]+)::([a-zA-Z0-9_]+)\s*\([^)]*\)\s*\{[^}]*\b([a-z][a-z0-9_]*[A-Z][a-zA-Z0-9_]*)\b"
)

# Filter out common false positives
FALSE_POSITIVE_PATTERNS = [
    re.compile(r"\breturn\b"),
    re.compile(r"\bthrow\b"),
    re.compile(r"\bif\b"),
    re.compile(r"\belse\b"),
    re.compile(r"\bfor\b"),
    re.compile(r"\bwhile\b"),
    re.compile(r"\bnamespace\b"),
    re.compile(r"\btypedef\b"),
    re.compile(r"\btemplate\b"),
    re.compile(r"#include"),
    re.compile(r"\bcatch\b"),
    re.compile(r"\bclass\b"),
    re.compile(r"\busing\b"),
    re.compile(r"\benum\b"),
    re.compile(r"\bstruct\b"),
]

# List of standard container types to check for in angle brackets
CONTAINER_TYPES = [
    "vector",
    "map",
    "unordered_map",
    "set",
    "unordered_set",
    "list",
    "deque",
    "queue",
    "stack",
    "array",
    "tuple",
    "pair",
    "optional",
    "variant",
]

# Common standard library typedefs that might contain uppercase chars
STD_TYPEDEFS = [
    "string",
    "wstring",
    "u8string",
    "u16string",
    "u32string",
    "stringstream",
    "istringstream",
    "ostringstream",
    "wstringstream",
    "iostream",
    "istream",
    "ostream",
    "ifstream",
    "ofstream",
    "size_t",
    "ptrdiff_t",
    "int8_t",
    "int16_t",
    "int32_t",
    "int64_t",
    "uint8_t",
    "uint16_t",
    "uint32_t",
    "uint64_t",
    "uintptr_t",
    "intptr_t",
]

# Skip files or directories containing these substrings
SKIP_PATHS = [
    "/build/",
    "/deps/",
    "/lib/",
    "/third_party/",
    "/external/",
    "/vendor/",
    "/test/",
]


def is_cpp_file(file_path):
    """Check if a file is a C++ source or header file."""
    extensions = {".cpp", ".cc", ".cxx", ".c", ".hpp", ".h", ".hxx", ".inl"}
    return file_path.suffix.lower() in extensions


def is_false_positive(line):
    """Check if a line is likely a false positive."""
    for pattern in FALSE_POSITIVE_PATTERNS:
        if pattern.search(line):
            return True
    return False


def is_template_parameter(var_name, line):
    """Check if the variable might be a template parameter."""
    template_pattern = re.compile(r"template\s*<.*\b" + re.escape(var_name) + r"\b.*>")
    return template_pattern.search(line)


def process_file(file_path):
    """Process a single file and return camelCase findings."""
    results = []
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()

            # Process the file line by line
            for i, line in enumerate(lines):
                line_num = i + 1

                # Skip preprocessor directives and false positives
                if line.strip().startswith("#") or is_false_positive(line):
                    continue

                # Find variable declarations using camelCase
                for match in VAR_PATTERN.finditer(line):
                    var_type, var_name = match.group(2), match.group(3)

                    # Check if it's a std type (not a variable)
                    if var_name in STD_TYPEDEFS or any(
                        container in var_type for container in CONTAINER_TYPES
                    ):
                        continue

                    # Check if it's a template parameter
                    if is_template_parameter(var_name, line):
                        continue

                    results.append((line_num, line.strip(), var_name))

                # Find 'this->camelCase' usage
                for match in THIS_PATTERN.finditer(line):
                    var_name = match.group(1)
                    results.append((line_num, line.strip(), var_name))

                # Find local variables in method implementations
                for match in CLASS_METHOD_VAR_PATTERN.finditer(
                    "\n".join(lines[max(0, i - 10) : min(len(lines), i + 10)])
                ):
                    class_name, method_name, var_name = (
                        match.group(1),
                        match.group(2),
                        match.group(3),
                    )
                    if (
                        i - 10 <= match.start() <= i + 10
                    ):  # Ensure the match is near the current line
                        results.append(
                            (
                                line_num,
                                f"In {class_name}::{method_name}: {line.strip()}",
                                var_name,
                            )
                        )
    except Exception as e:
        print(f"Error processing {file_path}: {e}", file=sys.stderr)

    return file_path, results


def find_camel_case_variables(base_dir):
    """Find all camelCase variables in C++ files under the given directory."""
    all_results = {}
    files_to_process = []

    # Find all C++ files
    for root, dirs, files in os.walk(base_dir):
        # Skip specified directories
        dirs[:] = [
            d
            for d in dirs
            if not any(skip in os.path.join(root, d) for skip in SKIP_PATHS)
        ]

        path = Path(root)
        for file in files:
            file_path = path / file
            if is_cpp_file(file_path) and not any(
                skip in str(file_path) for skip in SKIP_PATHS
            ):
                files_to_process.append(file_path)

    print(f"Processing {len(files_to_process)} files...")

    # Process files in parallel
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for file_path, results in executor.map(process_file, files_to_process):
            if results:
                all_results[file_path] = results

    return all_results


def display_results(results):
    """Display the results in a readable format."""
    total_vars = 0

    for file_path, findings in sorted(results.items()):
        if findings:
            print(f"\n{file_path}:")
            print("-" * 80)
            for line_num, line_text, var_name in findings:
                print(f"  Line {line_num}: {var_name}")
                print(f"    {line_text}")
            total_vars += len(findings)

    print("\n" + "=" * 80)
    print(f"Found {total_vars} camelCase variables in {len(results)} files")


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Find camelCase variables in C++ source files"
    )
    parser.add_argument(
        "path", nargs="?", default="src", help="Directory to scan (default: src)"
    )
    return parser.parse_args()


def main():
    """Main function."""
    args = parse_arguments()
    base_dir = args.path

    if not os.path.isdir(base_dir):
        print(f"Error: {base_dir} is not a valid directory", file=sys.stderr)
        return 1

    print(f"Scanning {base_dir} for camelCase variables...")
    results = find_camel_case_variables(base_dir)
    display_results(results)
    return 0


if __name__ == "__main__":
    sys.exit(main())
