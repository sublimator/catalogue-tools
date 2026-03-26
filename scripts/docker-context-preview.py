#!/usr/bin/env python3
"""Preview what files Docker/Cloud Build will upload as build context.

Parses .dockerignore using the same glob rules Docker uses and shows
the files that WOULD be uploaded, with total size.

Usage:
  ./scripts/docker-context-preview.py
  ./scripts/docker-context-preview.py --top 20
"""

import argparse
import os
from pathlib import Path


def parse_dockerignore(root: Path) -> list[str]:
    """Read .dockerignore patterns."""
    ignore_file = root / ".dockerignore"
    if not ignore_file.exists():
        return []
    patterns = []
    for line in ignore_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            patterns.append(line)
    return patterns


def matches_pattern(rel_path: str, pattern: str) -> bool:
    """Simple dockerignore pattern matching.

    Supports: dir/, *.ext, exact names, and paths with /.
    Does NOT support ! (negation) or ** (recursive glob).
    """
    import fnmatch

    # Directory pattern (trailing /)
    clean = pattern.rstrip("/")

    # Check if any path component matches
    parts = rel_path.split("/")

    # Pattern with / means match from root
    if "/" in clean:
        return fnmatch.fnmatch(rel_path, clean) or rel_path.startswith(clean + "/")

    # Pattern without / matches any component or the filename
    for part in parts:
        if fnmatch.fnmatch(part, clean):
            return True

    return fnmatch.fnmatch(rel_path, clean)


def is_ignored(rel_path: str, patterns: list[str]) -> bool:
    """Check if a path is ignored by any pattern."""
    for pattern in patterns:
        if matches_pattern(rel_path, pattern):
            return True
    return False


def main():
    ap = argparse.ArgumentParser(description="Preview Docker build context")
    ap.add_argument("--top", type=int, default=10, help="Show top N largest files")
    ap.add_argument("--all", action="store_true", help="List all files")
    args = ap.parse_args()

    root = Path(".")
    patterns = parse_dockerignore(root)

    print(f"Patterns from .dockerignore ({len(patterns)}):")
    for p in patterns:
        print(f"  {p}")
    print()

    files: list[tuple[int, str]] = []
    total = 0
    ignored_count = 0

    for dirpath, dirnames, filenames in os.walk(root):
        rel_dir = os.path.relpath(dirpath, root)
        if rel_dir == ".":
            rel_dir = ""

        # Prune ignored directories
        dirnames[:] = [
            d
            for d in dirnames
            if not is_ignored(
                (rel_dir + "/" + d if rel_dir else d),
                patterns,
            )
        ]

        for f in filenames:
            rel = (rel_dir + "/" + f if rel_dir else f)
            if is_ignored(rel, patterns):
                ignored_count += 1
                continue
            size = os.path.getsize(os.path.join(dirpath, f))
            files.append((size, rel))
            total += size

    files.sort(reverse=True)

    def fmt(n: int) -> str:
        if n >= 1_000_000_000:
            return f"{n / 1_000_000_000:.1f}G"
        if n >= 1_000_000:
            return f"{n / 1_000_000:.1f}M"
        if n >= 1_000:
            return f"{n / 1_000:.0f}K"
        return f"{n}B"

    print(f"Context: {len(files)} files, {fmt(total)} total ({ignored_count} ignored)")
    print()

    show = files if args.all else files[: args.top]
    for size, path in show:
        print(f"  {fmt(size):>8}  {path}")

    if not args.all and len(files) > args.top:
        print(f"  ... and {len(files) - args.top} more")


if __name__ == "__main__":
    main()
