#!/usr/bin/env -S .ai-docs/.venv/bin/python
"""Run one command with bounded time and bounded console output.

The complete combined output is written to an artifact.  The console receives
only a one-line result and, on failure, a small tail.  This is intended for
long-running local test loops where unbounded terminal output is both noisy
and poor evidence.
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=float, default=120.0, help="seconds")
    parser.add_argument(
        "--stop-grace", type=float, default=2.0, help="seconds before forced stop"
    )
    parser.add_argument("--tail-lines", type=int, default=12)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.command[:1] == ["--"]:
        args.command = args.command[1:]
    if not args.command:
        parser.error("a command is required after --")
    if args.timeout <= 0 or args.stop_grace < 0 or args.tail_lines < 0:
        parser.error("time limits must be positive and tail-lines non-negative")
    return args


def stop_process_group(process: subprocess.Popen[bytes], grace: float) -> bool:
    """Request a stop, then force it only if the process does not exit."""
    if process.poll() is not None:
        return False
    os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=grace)
        return False
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait()
        return True


def main() -> int:
    args = parse_args()
    args.artifact.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    timed_out = False
    forced = False

    with args.artifact.open("wb") as output:
        process = subprocess.Popen(
            args.command,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            return_code = process.wait(timeout=args.timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            forced = stop_process_group(process, args.stop_grace)
            return_code = process.returncode

    elapsed = time.monotonic() - started
    status = "timeout" if timed_out else ("pass" if return_code == 0 else "fail")
    print(
        f"bounded-run status={status} exit={return_code} elapsed={elapsed:.3f}s "
        f"forced={str(forced).lower()} artifact={args.artifact}"
    )

    if status != "pass" and args.tail_lines:
        lines = args.artifact.read_text(encoding="utf-8", errors="replace").splitlines()
        tail = lines[-args.tail_lines :]
        if tail:
            print(f"bounded-run tail-lines={len(tail)}")
            print("\n".join(tail))

    if timed_out:
        return 124
    return return_code if return_code >= 0 else 128 - return_code


if __name__ == "__main__":
    sys.exit(main())
