#!/usr/bin/env python3

import subprocess
import sys
import os
from pathlib import Path
from typing import List, Tuple, Optional, NamedTuple
import shutil
import json
import argparse
import tempfile


class IWYUResult(NamedTuple):
    """Result of running IWYU on a file."""

    filepath: Path
    success: bool
    output: str
    exit_code: int


class IWYUScanner:
    """Runs include-what-you-use on C++ header files."""

    def __init__(self, executable: str = "include-what-you-use"):
        """Initialize the scanner with the IWYU executable path."""
        self.executable: str = executable
        self._check_executable()

    def _check_executable(self) -> None:
        """Check if IWYU is installed and accessible."""
        if not shutil.which(self.executable):
            print(f"Error: '{self.executable}' not found in PATH", file=sys.stderr)
            print("Please install include-what-you-use:", file=sys.stderr)
            print("  macOS: brew install include-what-you-use", file=sys.stderr)
            print("  Ubuntu: sudo apt install iwyu", file=sys.stderr)
            sys.exit(1)

    def find_header_files(self, src_dir: Path) -> List[Path]:
        """Find all .h files under the given directory."""
        return list(src_dir.rglob("*.h"))

    def run_iwyu(
        self, file_path: Path, includes: Optional[List[str]] = None
    ) -> IWYUResult:
        """Run IWYU on a single file."""
        cmd: List[str] = [self.executable]

        # Tell IWYU this is a C++ file
        cmd.extend(["-x", "c++"])

        # Add standard and custom include paths
        if includes:
            for include_path in includes:
                cmd.extend(["-I", include_path])

        # Add C++ standard - must come after -x c++
        cmd.extend(["-std=c++20"])

        # Add the file to analyze
        cmd.append(str(file_path))

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,  # Don't raise exception on non-zero exit
            )

            # IWYU returns non-zero exit code when it finds issues
            # but we consider that a successful run
            success = True if result.returncode in [0, 1, 2] else False

            return IWYUResult(
                filepath=file_path,
                success=success,
                output=result.stderr,  # IWYU outputs to stderr
                exit_code=result.returncode,
            )
        except Exception as e:
            return IWYUResult(
                filepath=file_path, success=False, output=str(e), exit_code=-1
            )

    def analyze_directory(
        self, src_dir: Path, includes: Optional[List[str]] = None
    ) -> List[IWYUResult]:
        """Run IWYU on all header files in the directory."""
        header_files: List[Path] = self.find_header_files(src_dir)
        results: List[IWYUResult] = []

        print(f"Found {len(header_files)} header files to analyze")

        for i, file_path in enumerate(header_files, 1):
            print(f"[{i}/{len(header_files)}] Analyzing {file_path}...")
            result = self.run_iwyu(file_path, includes)
            results.append(result)

        return results


def print_summary(results: List[IWYUResult]) -> None:
    """Print a summary of the IWYU analysis."""
    total_files: int = len(results)
    files_with_issues: int = sum(1 for r in results if r.exit_code == 1)
    files_with_errors: int = sum(1 for r in results if r.exit_code not in [0, 1])

    print("\n" + "=" * 80)
    print(f"IWYU Analysis Summary:")
    print(f"  Total files analyzed: {total_files}")
    print(f"  Files with include issues: {files_with_issues}")
    print(f"  Files with analysis errors: {files_with_errors}")
    print("=" * 80 + "\n")

    if files_with_issues > 0:
        print("Files with include issues:")
        for result in results:
            if result.exit_code == 1:
                print(f"  {result.filepath}")

    if files_with_errors > 0:
        print("\nFiles with analysis errors:")
        for result in results:
            if result.exit_code not in [0, 1]:
                print(f"  {result.filepath} (exit code: {result.exit_code})")


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run include-what-you-use on C++ header files"
    )
    parser.add_argument(
        "--src-dir",
        type=str,
        default="src",
        help="Source directory to scan (default: src)",
    )
    parser.add_argument(
        "--include-dirs", type=str, nargs="*", help="Additional include directories"
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Show verbose output for each file"
    )
    parser.add_argument("--json", type=str, help="Output results to JSON file")

    args = parser.parse_args()

    src_dir: Path = Path(args.src_dir)
    if not src_dir.exists():
        print(f"Error: Source directory '{src_dir}' does not exist", file=sys.stderr)
        sys.exit(1)

    # Initialize scanner
    scanner = IWYUScanner()

    # Run analysis
    results: List[IWYUResult] = scanner.analyze_directory(src_dir, args.include_dirs)

    # Print verbose output if requested
    if args.verbose:
        for result in results:
            print(f"\n{'='*80}")
            print(f"File: {result.filepath}")
            print(f"Exit code: {result.exit_code}")
            print(f"Output:\n{result.output}")

    # Output to JSON if requested
    if args.json:
        json_results = [
            {
                "filepath": str(r.filepath),
                "success": r.success,
                "exit_code": r.exit_code,
                "output": r.output,
            }
            for r in results
        ]
        with open(args.json, "w") as f:
            json.dump(json_results, f, indent=2)
        print(f"Results saved to {args.json}")

    # Print summary
    print_summary(results)

    # Exit with error if any issues were found
    if any(r.exit_code == 1 for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
