#!/usr/bin/env python3
"""Filter xproof server logs by partition, level, time range, and text.

Usage:
    ./scripts/log-filter.py server-logs.txt --partition node-cache
    ./scripts/log-filter.py server-logs.txt --partition node-cache,engine,xproof
    ./scripts/log-filter.py server-logs.txt --level WARN,ERROR
    ./scripts/log-filter.py server-logs.txt --after 10:11:08 --before 10:11:10
    ./scripts/log-filter.py server-logs.txt --grep "walk_to\|ensure"
    ./scripts/log-filter.py server-logs.txt --partition node-cache --tail 50
    ./scripts/log-filter.py server-logs.txt --last 30s
"""

import argparse
import re
import sys
from datetime import datetime, timedelta

LOG_RE = re.compile(
    r'\[(\d{2}:\d{2}:\d{2}\.\d{3})\]\s+'
    r'\[(\w+)\]\s+'
    r'\[([^\]]+)\]\s+'
    r'(.*)'
)

def parse_time(s):
    return datetime.strptime(s, "%H:%M:%S.%f" if '.' in s else "%H:%M:%S")

def parse_duration(s):
    """Parse '30s', '5m', '1h' into timedelta."""
    if s.endswith('s'):
        return timedelta(seconds=int(s[:-1]))
    if s.endswith('m'):
        return timedelta(minutes=int(s[:-1]))
    if s.endswith('h'):
        return timedelta(hours=int(s[:-1]))
    return timedelta(seconds=int(s))

def main():
    p = argparse.ArgumentParser(description='Filter xproof server logs')
    p.add_argument('logfile', help='Log file to filter')
    p.add_argument('-p', '--partition', help='Comma-separated partition names')
    p.add_argument('-l', '--level', help='Comma-separated levels (DEBUG,INFO,WARN,ERROR)')
    p.add_argument('-a', '--after', help='Only lines after this time (HH:MM:SS)')
    p.add_argument('-b', '--before', help='Only lines before this time (HH:MM:SS)')
    p.add_argument('-g', '--grep', help='Regex pattern to match in message')
    p.add_argument('-t', '--tail', type=int, help='Show last N matching lines')
    p.add_argument('--last', help='Show lines from the last N seconds/minutes (e.g. 30s, 5m)')
    p.add_argument('-v', '--invert', action='store_true', help='Invert partition match (exclude)')
    p.add_argument('-c', '--count', action='store_true', help='Count lines per partition')
    args = p.parse_args()

    partitions = set(args.partition.split(',')) if args.partition else None
    levels = set(args.level.upper().split(',')) if args.level else None
    after = parse_time(args.after) if args.after else None
    before = parse_time(args.before) if args.before else None
    grep_re = re.compile(args.grep) if args.grep else None

    # For --last, we need to scan the file to find the last timestamp
    last_delta = parse_duration(args.last) if args.last else None

    lines = []
    counts = {}

    with open(args.logfile) as f:
        all_lines = f.readlines()

    # If --last, find the last timestamp and compute cutoff
    if last_delta:
        for line in reversed(all_lines):
            m = LOG_RE.match(line.strip())
            if m:
                last_time = parse_time(m.group(1))
                after = last_time - last_delta
                break

    for line in all_lines:
        line = line.rstrip()
        m = LOG_RE.match(line)
        if not m:
            # Non-matching lines (continuations) — include if we're not filtering
            if not partitions and not levels and not grep_re:
                lines.append(line)
            continue

        time_str, level, partition, msg = m.groups()

        if partitions:
            in_set = partition in partitions
            if args.invert:
                if in_set:
                    continue
            else:
                if not in_set:
                    continue

        if levels and level.upper() not in levels:
            continue

        if after or before:
            t = parse_time(time_str)
            if after and t < after:
                continue
            if before and t > before:
                continue

        if grep_re and not grep_re.search(msg):
            continue

        if args.count:
            counts[partition] = counts.get(partition, 0) + 1
            continue

        lines.append(line)

    if args.count:
        for part, cnt in sorted(counts.items(), key=lambda x: -x[1]):
            print(f"  {part:20s} {cnt}")
        return

    if args.tail:
        lines = lines[-args.tail:]

    for line in lines:
        print(line)

if __name__ == '__main__':
    main()
