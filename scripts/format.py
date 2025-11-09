#!/usr/bin/env python3

import argparse
import subprocess
import sys
import os
import shutil
from pathlib import Path
from typing import List, Set, Dict, Callable, Optional


def get_git_root() -> Path:
    """Get the root directory of the git repository."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            check=True,
        )
        return Path(result.stdout.strip())
    except subprocess.CalledProcessError:
        print("Error: Not a git repository", file=sys.stderr)
        sys.exit(1)


def get_all_files_by_type(root_dir: Path) -> Dict[str, List[Path]]:
    """Get all files by type under src, tests, and scripts directories."""
    files: Dict[str, List[Path]] = {"cpp": [], "shell": [], "python": [], "cmake": []}

    # Directories to search
    search_dirs = ["src", "tests", "scripts", "cmake"]

    # File patterns by type
    patterns = {
        "cpp": [".h", ".cpp"],
        "shell": [".sh"],
        "python": [".py"],
        "cmake": ["CMakeLists.txt", ".cmake"],
    }

    for subdir in search_dirs:
        dir_path = root_dir / subdir
        if not dir_path.exists():
            continue

        # Find files of each type
        for file_type, extensions in patterns.items():
            for ext in extensions:
                files[file_type].extend(dir_path.rglob(f"*{ext}"))

    return files


def _filter_files_by_type(root_dir: Path, files: Set[str]) -> Dict[str, List[Path]]:
    """Helper function to filter files by type."""
    filtered_files: Dict[str, List[Path]] = {
        "cpp": [],
        "shell": [],
        "python": [],
        "cmake": [],
    }

    search_dirs = ["src/", "tests/", "scripts/", "cmake/", "builds/", "CMakeLists.txt"]

    for filename in files:
        file_path = root_dir / filename
        if not file_path.exists():
            continue

        # Check if file is in one of our target directories
        if not any(filename.startswith(dir) for dir in search_dirs):
            continue

        # Categorize by file type
        if filename.endswith(".h") or filename.endswith(".cpp"):
            filtered_files["cpp"].append(file_path)
        elif filename.endswith(".sh"):
            filtered_files["shell"].append(file_path)
        elif filename.endswith(".py"):
            filtered_files["python"].append(file_path)
        elif filename.endswith("CMakeLists.txt") or filename.endswith(".cmake"):
            filtered_files["cmake"].append(file_path)

    return filtered_files


def get_git_commit_files(
    root_dir: Path, commit_ish: str = "HEAD"
) -> Dict[str, List[Path]]:
    """Get all files changed in a specific commit by file type.

    Args:
        root_dir: Root directory of the git repository
        commit_ish: Any valid git commit-ish (HEAD, HEAD~2, hash, branch, etc.)
    """
    os.chdir(root_dir)

    files: Set[str] = set()

    # Get files from specified commit
    result = subprocess.run(
        ["git", "diff-tree", "--no-commit-id", "--name-only", "-r", commit_ish],
        capture_output=True,
        text=True,
        check=True,
    )
    if result.stdout.strip():
        files.update(result.stdout.strip().split("\n"))

    # Filter files by type (reuse the filtering logic below)
    return _filter_files_by_type(root_dir, files)


def get_git_dirty_files(root_dir: Path) -> Dict[str, List[Path]]:
    """Get all dirty files (staged, unstaged, and untracked) in git by file type."""
    os.chdir(root_dir)

    files: Set[str] = set()

    # Get staged files
    result = subprocess.run(
        ["git", "diff", "--cached", "--name-only"],
        capture_output=True,
        text=True,
        check=True,
    )
    if result.stdout.strip():
        files.update(result.stdout.strip().split("\n"))

    # Get unstaged files
    result = subprocess.run(
        ["git", "diff", "--name-only"], capture_output=True, text=True, check=True
    )
    if result.stdout.strip():
        files.update(result.stdout.strip().split("\n"))

    # Get untracked files
    result = subprocess.run(
        ["git", "ls-files", "--others", "--exclude-standard"],
        capture_output=True,
        text=True,
        check=True,
    )
    if result.stdout.strip():
        files.update(result.stdout.strip().split("\n"))

    # Filter files by type
    return _filter_files_by_type(root_dir, files)


def format_cpp_file(file_path: Path) -> bool:
    """Format a C++ file using clang-format."""
    print(f"Formatting C++ file: {file_path}")
    try:
        subprocess.run(["clang-format", "-i", str(file_path)], check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error formatting {file_path}: {e}", file=sys.stderr)
        return False
    except FileNotFoundError:
        print(
            "Error: clang-format not found. Please install it first.", file=sys.stderr
        )
        sys.exit(1)


def format_shell_file(file_path: Path) -> bool:
    """Format a shell file using shfmt."""
    print(f"Formatting shell file: {file_path}")
    try:
        # Use shfmt with sensible defaults
        # -i 2: indent with 2 spaces
        # -w: write to file instead of stdout
        subprocess.run(["shfmt", "-i", "2", "-w", str(file_path)], check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error formatting {file_path}: {e}", file=sys.stderr)
        return False
    except FileNotFoundError:
        print("Error: shfmt not found. Please install it first.", file=sys.stderr)
        print(
            "Install with: brew install shfmt (macOS) or apt-get install shfmt (Linux)",
            file=sys.stderr,
        )
        sys.exit(1)


def format_python_file(file_path: Path, root_dir: Path) -> bool:
    """Format a Python file using black from the virtual environment."""
    print(f"Formatting Python file: {file_path}")

    # Path to black in the virtual environment
    black_path = root_dir / "catenv" / "bin" / "black"

    # Check if black exists in the virtual environment
    if not black_path.exists():
        print(f"Error: black not found at {black_path}", file=sys.stderr)
        print(
            "Make sure you've run scripts/setup-catenv.sh to set up the environment",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        # Use black with default settings
        subprocess.run([str(black_path), str(file_path)], check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error formatting {file_path}: {e}", file=sys.stderr)
        return False


def format_cmake_file(file_path: Path, root_dir: Path) -> bool:
    """Format a CMake file using cmake-format from the virtual environment."""
    print(f"Formatting CMake file: {file_path}")

    # First check if cmake-format is on PATH
    cmake_format_path = shutil.which("cmake-format")

    # If not, look for it in the virtual environment
    if not cmake_format_path:
        cmake_format_path = str(root_dir / "catenv" / "bin" / "cmake-format")
        if not os.path.exists(cmake_format_path):
            print(
                f"Error: cmake-format not found in PATH or at {cmake_format_path}",
                file=sys.stderr,
            )
            print(
                "Make sure you've run source scripts/setup-catenv.sh to set up the environment",
                file=sys.stderr,
            )
            sys.exit(1)

    try:
        # Use cmake-format with default settings
        subprocess.run([cmake_format_path, "-i", str(file_path)], check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error formatting {file_path}: {e}", file=sys.stderr)
        return False


def main() -> None:
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Format C++, shell, and Python files")
    parser.add_argument("--all", action="store_true", help="Format all files")
    parser.add_argument(
        "--from-commit",
        nargs="?",
        const="HEAD",
        metavar="COMMIT",
        help="Format files from a commit (default: HEAD). Accepts any git commit-ish (HEAD~2, hash, HEAD^, etc.)",
    )
    parser.add_argument("--cpp-only", action="store_true", help="Format only C++ files")
    parser.add_argument(
        "--shell-only", action="store_true", help="Format only shell files"
    )
    parser.add_argument(
        "--python-only", action="store_true", help="Format only Python files"
    )
    parser.add_argument(
        "--cmake-only", action="store_true", help="Format only CMake files"
    )
    args = parser.parse_args()

    root_dir: Path = get_git_root()

    # Determine which file types to format
    format_only_one_type = (
        args.cpp_only or args.shell_only or args.python_only or args.cmake_only
    )
    format_cpp = args.cpp_only or not format_only_one_type
    format_shell = args.shell_only or not format_only_one_type
    format_python = args.python_only or not format_only_one_type
    format_cmake = args.cmake_only or not format_only_one_type

    if args.all:
        # Explicit request for all files
        files_by_type = get_all_files_by_type(root_dir)
        print("Formatting mode: all files")
    elif args.from_commit is not None:
        # Format files from specified commit
        commit_ish = args.from_commit
        files_by_type = get_git_commit_files(root_dir, commit_ish)
        cpp_count = len(files_by_type["cpp"]) if format_cpp else 0
        shell_count = len(files_by_type["shell"]) if format_shell else 0
        python_count = len(files_by_type["python"]) if format_python else 0
        cmake_count = len(files_by_type["cmake"]) if format_cmake else 0
        total_count = cpp_count + shell_count + python_count + cmake_count

        if total_count > 0:
            print(f"Formatting mode: commit {commit_ish} ({total_count} files found)")
        else:
            print(f"No files in commit {commit_ish} to format")
            return
    else:
        # Default behavior: format dirty files only
        files_by_type = get_git_dirty_files(root_dir)
        cpp_count = len(files_by_type["cpp"]) if format_cpp else 0
        shell_count = len(files_by_type["shell"]) if format_shell else 0
        python_count = len(files_by_type["python"]) if format_python else 0
        cmake_count = len(files_by_type["cmake"]) if format_cmake else 0
        total_count = cpp_count + shell_count + python_count + cmake_count

        if total_count > 0:
            print(f"Formatting mode: dirty files only ({total_count} files found)")
        else:
            print("No dirty files to format")
            return

    # Set up formatters by file type
    # Python and CMake formatters need the root_dir to find the virtual environment
    python_formatter = lambda path: format_python_file(path, root_dir)
    cmake_formatter = lambda path: format_cmake_file(path, root_dir)

    formatters = {
        "cpp": format_cpp_file,
        "shell": format_shell_file,
        "python": python_formatter,
        "cmake": cmake_formatter,
    }

    # Track overall success
    success: bool = True
    files_formatted = 0

    # Format all requested file types
    for file_type, formatter in formatters.items():
        # Skip if this file type is not requested
        if (
            (file_type == "cpp" and not format_cpp)
            or (file_type == "shell" and not format_shell)
            or (file_type == "python" and not format_python)
            or (file_type == "cmake" and not format_cmake)
        ):
            continue

        for file_path in files_by_type[file_type]:
            if not formatter(file_path):
                success = False
            else:
                files_formatted += 1

    if files_formatted > 0:
        print(f"Successfully formatted {files_formatted} files")
    else:
        print("No files were formatted")

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
