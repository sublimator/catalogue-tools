#!/usr/bin/env python3

import argparse
import subprocess
import sys
import os
from pathlib import Path
from typing import List, Set

def get_git_root() -> Path:
    """Get the root directory of the git repository."""
    try:
        result = subprocess.run(['git', 'rev-parse', '--show-toplevel'],
                              capture_output=True, text=True, check=True)
        return Path(result.stdout.strip())
    except subprocess.CalledProcessError:
        print("Error: Not a git repository", file=sys.stderr)
        sys.exit(1)

def get_all_cpp_files(root_dir: Path) -> List[Path]:
    """Get all .h and .cpp files under src directory."""
    src_dir = root_dir / 'src'
    if not src_dir.exists():
        return []
    
    files: List[Path] = []
    for ext in ['.h', '.cpp']:
        files.extend(src_dir.rglob(f'*{ext}'))
    return files

def get_git_dirty_files(root_dir: Path) -> List[Path]:
    """Get all dirty files (staged, unstaged, and untracked) in git."""
    os.chdir(root_dir)
    
    files: Set[str] = set()
    
    # Get staged files
    result = subprocess.run(['git', 'diff', '--cached', '--name-only'],
                          capture_output=True, text=True, check=True)
    if result.stdout.strip():
        files.update(result.stdout.strip().split('\n'))
    
    # Get unstaged files
    result = subprocess.run(['git', 'diff', '--name-only'],
                          capture_output=True, text=True, check=True)
    if result.stdout.strip():
        files.update(result.stdout.strip().split('\n'))
    
    # Get untracked files
    result = subprocess.run(['git', 'ls-files', '--others', '--exclude-standard'],
                          capture_output=True, text=True, check=True)
    if result.stdout.strip():
        files.update(result.stdout.strip().split('\n'))
    
    # Filter for C++ files in src directory
    cpp_files: List[Path] = []
    for filename in files:
        if filename.startswith('src/') and (filename.endswith('.h') or filename.endswith('.cpp')):
            file_path = root_dir / filename
            if file_path.exists():
                cpp_files.append(file_path)
    
    return cpp_files

def format_file(file_path: Path) -> bool:
    """Format a file using clang-format."""
    print(f"Formatting {file_path}")
    try:
        subprocess.run(['clang-format', '-i', str(file_path)], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error formatting {file_path}: {e}", file=sys.stderr)
        return False
    except FileNotFoundError:
        print("Error: clang-format not found. Please install it first.", file=sys.stderr)
        sys.exit(1)
    return True

def main() -> None:
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Format C++ files with clang-format')
    parser.add_argument('--all', action='store_true',
                        help='Format all files in src directory')
    args = parser.parse_args()
    
    root_dir: Path = get_git_root()
    files_to_format: List[Path] = []
    
    if args.all:
        # Explicit request for all files
        files_to_format = get_all_cpp_files(root_dir)
        print("Formatting mode: all files")
    else:
        # Default behavior: format dirty files only
        files_to_format = get_git_dirty_files(root_dir)
        if files_to_format:
            print(f"Formatting mode: dirty files only ({len(files_to_format)} files found)")
        else:
            print("No dirty files to format")
            return
    
    if not files_to_format:
        print("No files to format")
        return
    
    print(f"Formatting {len(files_to_format)} files...")
    success: bool = True
    for file_path in files_to_format:
        if not format_file(file_path):
            success = False
    
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()