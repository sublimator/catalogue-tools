#!/usr/bin/env python3
import subprocess
import os
import argparse
import sys


def get_changed_files(target_branch, file_pattern=None):
    """Get list of files changed compared to target branch"""
    try:
        # Get names of files that differ from target branch
        cmd = ["git", "diff", "--name-only", target_branch]

        if file_pattern:
            cmd.append("--")
            cmd.append(file_pattern)

        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return [file.strip() for file in result.stdout.splitlines() if file.strip()]
    except subprocess.CalledProcessError as e:
        print(f"Error running git diff: {e}", file=sys.stderr)
        return []


def read_file_content(file_path):
    """Read the content of a file"""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()
    except UnicodeDecodeError:
        try:
            # Try Latin-1 if UTF-8 fails (for binary files)
            with open(file_path, "r", encoding="latin-1") as f:
                return f.read()
        except Exception as e:
            print(f"Error reading file {file_path}: {e}", file=sys.stderr)
            return f"<Could not read file: {file_path}>"
    except Exception as e:
        print(f"Error reading file {file_path}: {e}", file=sys.stderr)
        return f"<Could not read file: {file_path}>"


def concatenate_files_with_headers(files, header_format="\n\n--- {file} ---\n\n"):
    """Concatenate file contents with headers"""
    all_content = []

    for file in files:
        header = header_format.format(file=file)
        content = read_file_content(file)
        all_content.append(header + content)

    return "".join(all_content)


def copy_to_clipboard(content):
    """Copy content to clipboard using pbcopy"""
    try:
        proc = subprocess.Popen(["pbcopy"], stdin=subprocess.PIPE)
        proc.stdin.write(content.encode("utf-8"))
        proc.stdin.close()
        proc.wait()
        return True
    except Exception as e:
        print(f"Error copying to clipboard: {e}", file=sys.stderr)
        return False


def write_to_file(content, output_file):
    """Write content to a file"""
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(content)
        return True
    except Exception as e:
        print(f"Error writing to file {output_file}: {e}", file=sys.stderr)
        return False


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Concatenate changed files in git repository and copy to clipboard or save to file"
    )

    parser.add_argument(
        "--target-branch",
        "-b",
        default="origin/main",
        help="Target branch to compare against (default: origin/main)",
    )

    parser.add_argument(
        "--pattern",
        "-p",
        help='File pattern to filter (e.g., "*.py" for Python files only)',
    )

    parser.add_argument(
        "--output",
        "-o",
        help="Output file (if not specified, output goes to clipboard)",
    )

    parser.add_argument(
        "--header-format",
        default="\n\n--- {file} ---\n\n",
        help='Format for file headers (default: "\\n\\n--- {file} ---\\n\\n")',
    )

    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Show verbose output"
    )

    return parser.parse_args()


def main():
    args = parse_args()

    # Get changed files compared to target branch
    if args.verbose:
        print(f"Finding files changed compared to {args.target_branch}...")

    changed_files = get_changed_files(args.target_branch, args.pattern)

    if not changed_files:
        print(f"No changed files found compared to {args.target_branch}")
        return

    if args.verbose:
        print(f"Found {len(changed_files)} changed files:")
        for file in changed_files:
            print(f"  - {file}")
    else:
        print(
            f"Found {len(changed_files)} changed files compared to {args.target_branch}"
        )

    # Concatenate files with headers
    concatenated_content = concatenate_files_with_headers(
        changed_files, args.header_format
    )

    # Output the content
    if args.output:
        if write_to_file(concatenated_content, args.output):
            print(f"Concatenated content saved to {args.output}")
    else:
        if copy_to_clipboard(concatenated_content):
            print("Concatenated content copied to clipboard successfully")


if __name__ == "__main__":
    main()
