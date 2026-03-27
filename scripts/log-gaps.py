#!/usr/bin/env python3
"""Find gaps in timestamped log lines and show context around them.

Usage:
  ./scripts/log-gaps.py server-logs.txt
  ./scripts/log-gaps.py server-logs.txt --threshold 2.0
  ./scripts/log-gaps.py server-logs.txt --context 5
  cat server-logs.txt | ./scripts/log-gaps.py -
"""

import argparse
import re
import sys
from datetime import datetime

TS_RE = re.compile(r"\[(\d{2}:\d{2}:\d{2}\.\d{3})\]")


def parse_ts(s: str) -> float | None:
    m = TS_RE.search(s)
    if not m:
        return None
    t = datetime.strptime(m.group(1), "%H:%M:%S.%f")
    return t.hour * 3600 + t.minute * 60 + t.second + t.microsecond / 1e6


def main() -> None:
    ap = argparse.ArgumentParser(description="Find gaps in log timestamps")
    ap.add_argument("file", help="Log file (or - for stdin)")
    ap.add_argument(
        "--threshold", "-t", type=float, default=1.0, help="Gap threshold in seconds (default: 1.0)"
    )
    ap.add_argument("--context", "-C", type=int, default=3, help="Context lines around gap (default: 3)")
    args = ap.parse_args()

    f = sys.stdin if args.file == "-" else open(args.file)
    lines = f.readlines()
    if f is not sys.stdin:
        f.close()

    # Parse timestamps
    entries: list[tuple[float, int, str]] = []
    for i, line in enumerate(lines):
        ts = parse_ts(line)
        if ts is not None:
            entries.append((ts, i, line))

    # Find gaps
    gaps: list[tuple[float, int, int]] = []
    for j in range(1, len(entries)):
        prev_ts, prev_i, _ = entries[j - 1]
        curr_ts, curr_i, _ = entries[j]
        delta = curr_ts - prev_ts
        # Handle midnight wrap
        if delta < -43200:
            delta += 86400
        if delta >= args.threshold:
            gaps.append((delta, prev_i, curr_i))

    if not gaps:
        print(f"No gaps >= {args.threshold}s found in {len(entries)} log lines")
        return

    print(f"Found {len(gaps)} gap(s) >= {args.threshold}s in {len(entries)} log lines\n")

    for gi, (delta, before_i, after_i) in enumerate(gaps):
        print(f"{'=' * 70}")
        print(f"  GAP #{gi + 1}: {delta:.3f}s between lines {before_i + 1} and {after_i + 1}")
        print(f"{'=' * 70}")

        # Context before
        start = max(0, before_i - args.context + 1)
        for i in range(start, before_i + 1):
            print(f"  {i + 1:6d}  {lines[i]}", end="")

        print(f"  {'--- gap: ' + f'{delta:.3f}s ' + '-' * 50}")

        # Context after
        end = min(len(lines), after_i + args.context)
        for i in range(after_i, end):
            print(f"  {i + 1:6d}  {lines[i]}", end="")

        print()


if __name__ == "__main__":
    main()
