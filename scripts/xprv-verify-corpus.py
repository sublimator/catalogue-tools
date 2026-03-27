#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["rich>=13.0"]
# ///
"""
Corpus verification test: prove each tx in the corpus via the local
xprv server (JSON + binary), then verify each proof using verify-xprv.py.

Runs up to 8 concurrent requests. Prints a summary at the end.

Usage:
  ./scripts/xprv-verify-corpus.py [corpus.json] [--host localhost:8080] [--max N]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn


def do_prove(
    host: str,
    tx_hash: str,
    label: str,
    fmt: str,
    ledger_index: int = 0,
) -> tuple[str, str, float, bytes | None, str]:
    """Prove a tx. Returns (label, fmt, elapsed, data_or_none, error)."""
    url = f"http://{host}/prove?tx={tx_hash}&max_anchor_age=120"
    if ledger_index:
        url += f"&ledger_index={ledger_index}"
    if fmt == "bin":
        url += "&format=bin"

    start = time.monotonic()
    try:
        req = Request(url)
        with urlopen(req, timeout=120) as resp:
            data = resp.read()
        return label, fmt, time.monotonic() - start, data, ""
    except HTTPError as e:
        return label, fmt, time.monotonic() - start, None, f"HTTP {e.code}: {e.read().decode()[:200]}"
    except (URLError, TimeoutError) as e:
        return label, fmt, time.monotonic() - start, None, f"request failed: {e}"


def do_verify(
    label: str,
    fmt: str,
    data: bytes,
    verifier: Path,
) -> tuple[str, str, bool, str]:
    """Verify proof data. Returns (label, fmt, ok, message)."""
    suffix = ".xprv.json" if fmt == "json" else ".xprv.bin"
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as f:
        f.write(data)
        tmp_path = f.name

    try:
        result = subprocess.run(
            ["uv", "run", str(verifier), tmp_path],
            capture_output=True,
            text=True,
            timeout=60,
        )
        ok = result.returncode == 0
        if ok:
            msg = "PASS"
        else:
            lines = [l.strip() for l in (result.stdout + result.stderr).split("\n") if l.strip()]
            fail_lines = [l for l in lines if "FAIL" in l or "ERROR" in l or "error" in l.lower()]
            msg = fail_lines[0] if fail_lines else (lines[-1] if lines else "unknown error")
        return label, fmt, ok, msg
    except subprocess.TimeoutExpired:
        return label, fmt, False, "verify timed out"
    finally:
        Path(tmp_path).unlink(missing_ok=True)


async def prove_worker(
    prove_queue: asyncio.Queue,
    verify_queue: asyncio.Queue,
    host: str,
    results: list,
    results_lock: asyncio.Lock,
    loop: asyncio.AbstractEventLoop,
    progress: Progress,
    prove_task_id: int,
) -> None:
    while True:
        item = await prove_queue.get()
        if item is None:
            prove_queue.task_done()
            break
        tx_hash, label, fmt, ledger_index = item
        label, fmt, elapsed, data, error = await loop.run_in_executor(
            None, do_prove, host, tx_hash, label, fmt, ledger_index
        )
        async with results_lock:
            idx = len(results)
            if data is None:
                results.append((label, fmt, False, error, elapsed))
            else:
                results.append((label, fmt, True, "pending", elapsed))
                await verify_queue.put((label, fmt, data, idx))
        progress.advance(prove_task_id)
        prove_queue.task_done()


async def verify_worker(
    verify_queue: asyncio.Queue,
    verifier: Path,
    results: list,
    results_lock: asyncio.Lock,
    loop: asyncio.AbstractEventLoop,
    progress: Progress,
    verify_task_id: int,
    counters: dict,
) -> None:
    while True:
        item = await verify_queue.get()
        if item is None:
            verify_queue.task_done()
            break
        label, fmt, data, result_idx = item
        label, fmt, ok, msg = await loop.run_in_executor(
            None, do_verify, label, fmt, data, verifier
        )
        async with results_lock:
            results[result_idx] = (label, fmt, ok, msg, results[result_idx][4])
            if ok:
                counters["passed"] += 1
            else:
                counters["failed"] += 1
        progress.advance(verify_task_id)
        verify_queue.task_done()


async def main_async(
    corpus_path: str,
    host: str,
    max_tx: int,
    concurrency: int,
) -> int:
    corpus = json.loads(Path(corpus_path).read_text())
    verifier = Path(__file__).parent / "verify-xprv.py"
    if not verifier.exists():
        print(f"Verifier not found: {verifier}", file=sys.stderr)
        return 1

    # Collect all tx hashes (hot + cold sets)
    txs: list[tuple[str, str, int]] = []
    for entry in corpus.get("hot_set", []):
        txs.append((entry["tx_hash"], entry["label"], entry.get("ledger_index", 0)))
    for entry in corpus.get("cold_set", []):
        txs.append((entry["tx_hash"], entry["label"], entry.get("ledger_index", 0)))

    if max_tx > 0:
        txs = txs[:max_tx]

    console = Console()
    total_tasks = len(txs) * 2  # json + bin per tx

    console.print(f"[bold]Corpus:[/] {len(txs)} txs x 2 formats = {total_tasks} proofs, concurrency={concurrency}")
    console.print(f"[bold]Server:[/] {host}")
    console.print(f"[bold]Verifier:[/] {verifier}\n")

    prove_queue: asyncio.Queue = asyncio.Queue()
    verify_queue: asyncio.Queue = asyncio.Queue()

    for tx_hash, label, ledger_index in txs:
        for fmt in ("json", "bin"):
            prove_queue.put_nowait((tx_hash, label, fmt, ledger_index))

    for _ in range(concurrency):
        prove_queue.put_nowait(None)

    loop = asyncio.get_event_loop()
    results: list = []
    results_lock = asyncio.Lock()
    counters = {"passed": 0, "failed": 0}

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        console=console,
    )

    prove_tid = progress.add_task("[cyan]Proving", total=total_tasks)
    verify_tid = progress.add_task("[green]Verifying", total=total_tasks)

    with progress:
        prove_workers = [
            asyncio.create_task(
                prove_worker(prove_queue, verify_queue, host, results,
                             results_lock, loop, progress, prove_tid))
            for _ in range(concurrency)
        ]
        verify_workers = [
            asyncio.create_task(
                verify_worker(verify_queue, verifier, results,
                              results_lock, loop, progress, verify_tid,
                              counters))
            for _ in range(concurrency)
        ]

        await asyncio.gather(*prove_workers)

        for _ in range(concurrency):
            await verify_queue.put(None)
        await asyncio.gather(*verify_workers)

    # Summary
    console.print()
    errors: list[tuple[str, str, str]] = []
    for label, fmt, ok, msg, elapsed in results:
        if not ok:
            errors.append((label, fmt, msg))

    passed = counters["passed"]
    failed = counters["failed"]
    # Count prove failures (never made it to verify)
    prove_fails = sum(1 for _, _, ok, msg, _ in results if not ok and msg != "pending")
    failed += prove_fails

    if failed == 0:
        console.print(f"[bold green]  {passed} passed, 0 failed[/]")
    else:
        console.print(f"[bold red]  {passed} passed, {failed} failed[/]")
        for label, fmt, msg in errors:
            console.print(f"    [red]{label} ({fmt}): {msg}[/]")

    return 0 if failed == 0 else 1


def main() -> None:
    ap = argparse.ArgumentParser(description="Corpus verification test")
    ap.add_argument(
        "corpus",
        nargs="?",
        default="scripts/xprv-tx-corpus.generated.json",
        help="Path to corpus JSON",
    )
    ap.add_argument("--host", default="localhost:8080", help="xprv server host:port")
    ap.add_argument("--max", type=int, default=0, help="Max transactions to test (0=all)")
    ap.add_argument("--concurrency", type=int, default=8, help="Max concurrent requests")
    args = ap.parse_args()

    sys.exit(asyncio.run(main_async(args.corpus, args.host, args.max, args.concurrency)))


if __name__ == "__main__":
    main()
