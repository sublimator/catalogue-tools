#!/usr/bin/env python3
"""
Verbose std::cout to LOGI converter with detailed logging
"""

import re
import sys


def convert_cout_to_logi(content):
    # Find all std::cout << ... ; patterns (including multiline)
    pattern = r"std::cout\s*<<\s*(.*?);"

    matches_found = []

    def replace_cout(match):
        # Log the match
        full_match = match.group(0)
        inner = match.group(1)

        print(f"\n{'='*80}")
        print(f"FOUND MATCH:")
        print(f"  Full: {repr(full_match)}")
        print(f"  Inner content: {repr(inner)}")

        # Replace << with ,
        parts = inner.split("<<")
        print(f"\nSPLIT INTO {len(parts)} PARTS:")
        for i, part in enumerate(parts):
            print(f"  [{i}]: {repr(part.strip())}")

        # Clean up each part (strip whitespace)
        parts = [p.strip() for p in parts if p.strip()]

        # Filter out std::endl, std::hex, std::dec, std::setw, etc
        filtered = []
        print(f"\nFILTERING:")
        for part in parts:
            # Skip stream manipulators
            if part in ["std::endl", "std::hex", "std::dec", "std::oct", "std::fixed"]:
                print(f"  SKIP manipulator: {part}")
                continue
            if (
                part.startswith("std::setw(")
                or part.startswith("std::setfill(")
                or part.startswith("std::setprecision(")
            ):
                print(f"  SKIP formatting: {part}")
                continue
            # Remove \n from end of string literals
            if part.endswith('\\n"') and part.count('"') == 2:
                original = part
                part = part[:-3] + '"'
                print(f"  TRIM \\n: {repr(original)} -> {repr(part)}")
            if part == '"\\n"':
                print(f"  SKIP newline: {part}")
                continue
            print(f"  KEEP: {repr(part)}")
            filtered.append(part)

        if not filtered:
            result = "LOGI();"
            print(f"\nRESULT: {result} (empty)")
        else:
            result = f'LOGI({", ".join(filtered)});'
            print(f"\nRESULT: {result}")

        matches_found.append(
            {
                "original": full_match,
                "result": result,
                "parts": parts,
                "filtered": filtered,
            }
        )

        return result

    # Count total matches first
    all_matches = list(re.finditer(pattern, content, flags=re.DOTALL))
    print(f"\nTOTAL MATCHES FOUND: {len(all_matches)}")

    # Replace all matches
    converted = re.sub(pattern, replace_cout, content, flags=re.DOTALL)

    # Summary
    print(f"\n{'='*80}")
    print(f"CONVERSION SUMMARY:")
    print(f"  Total matches: {len(matches_found)}")
    print(f"  Successful conversions: {sum(1 for m in matches_found if m['filtered'])}")
    print(f"  Empty results: {sum(1 for m in matches_found if not m['filtered'])}")

    # Show some examples
    print(f"\nEXAMPLE CONVERSIONS:")
    for i, match in enumerate(matches_found[:5]):  # Show first 5
        print(f"\n  Example {i+1}:")
        print(
            f"    FROM: {repr(match['original'][:60])}{'...' if len(match['original']) > 60 else ''}"
        )
        print(
            f"    TO:   {repr(match['result'][:60])}{'...' if len(match['result']) > 60 else ''}"
        )

    return converted


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python cout-to-logi-verbose.py <file>")
        sys.exit(1)

    filename = sys.argv[1]

    print(f"Processing file: {filename}")

    with open(filename, "r") as f:
        content = f.read()

    print(f"File size: {len(content)} bytes")
    print(f"Lines: {content.count(chr(10))}")
    print(f"std::cout occurrences: {content.count('std::cout')}")

    converted = convert_cout_to_logi(content)

    output_file = filename + ".converted"
    with open(output_file, "w") as f:
        f.write(converted)

    print(f"\n{'='*80}")
    print(f"OUTPUT WRITTEN TO: {output_file}")
    print(f"Output size: {len(converted)} bytes")
    print(f"Size difference: {len(converted) - len(content):+d} bytes")
    print(f"\nTo review changes:")
    print(f"  diff -u {filename} {output_file} | less")
    print(f"\nTo apply changes:")
    print(f"  mv {output_file} {filename}")
