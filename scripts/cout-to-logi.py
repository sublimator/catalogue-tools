#!/usr/bin/env python3
"""
Dead simple std::cout to LOGI converter
Just find std::cout << x << y << z; and make it LOGI(x, y, z);
"""

import re
import sys


def convert_cout_to_logi(content):
    # Find all std::cout << ... ; patterns (including multiline)
    pattern = r"std::cout\s*<<\s*(.*?);"

    def replace_cout(match):
        # Get the content between std::cout << and ;
        inner = match.group(1)

        # Replace << with ,
        parts = inner.split("<<")

        # Clean up each part (strip whitespace)
        parts = [p.strip() for p in parts if p.strip()]

        # Filter out std::endl, std::hex, std::dec, std::setw, etc
        filtered = []
        for part in parts:
            # Skip stream manipulators
            if part in ["std::endl", "std::hex", "std::dec", "std::oct", "std::fixed"]:
                continue
            if (
                part.startswith("std::setw(")
                or part.startswith("std::setfill(")
                or part.startswith("std::setprecision(")
            ):
                continue
            # Remove \n from end of string literals
            if part.endswith('\\n"') and part.count('"') == 2:
                part = part[:-3] + '"'
            if part == '"\\n"':
                continue
            filtered.append(part)

        if not filtered:
            return "LOGI();"

        return f'LOGI({", ".join(filtered)});'

    # Replace all matches
    return re.sub(pattern, replace_cout, content, flags=re.DOTALL)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python cout-to-logi.py <file>")
        sys.exit(1)

    filename = sys.argv[1]

    with open(filename, "r") as f:
        content = f.read()

    converted = convert_cout_to_logi(content)

    with open(filename + ".converted", "w") as f:
        f.write(converted)

    print(f"Done! Output: {filename}.converted")
    print(f"Review: diff -u {filename} {filename}.converted")
