#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.14"
# dependencies = ["pytest>=8.3,<9"]
# ///
"""Run the canonical live xprv proof gate in an isolated workspace.

The lower-level ``xprv-serve-smoke-v2.py`` owns server lifecycle and the
individual checks. This wrapper owns the repeated run preparation:

- create one durable temporary run directory;
- snapshot a warm peer database consistently, including live WAL state;
- select the real peer plus JSON, SSE, and binary proof paths;
- bind the run receipt to the exact binary, runner, database, and invocation;
- preserve every artifact whether the gate passes or fails.

Example:

    scripts/xprv-live-proof-gate.py \
      --xprv /private/tmp/catalogue-build/src/xprv/xprv \
      --peer-cache-seed /private/tmp/warm/peer-endpoints.sqlite3
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shlex
import sqlite3
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

CANONICAL_CASES = (
    "server_connects_peers",
    "prove_json_shape_and_network",
    "prove_sse_shape_and_done",
    "prove_binary_header_and_network",
)
CANONICAL_FILTER = " or ".join(CANONICAL_CASES)
DEFAULT_TMP_ROOT = Path(os.environ.get("XPRV_LIVE_GATE_TMP_ROOT", "/private/tmp"))


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def snapshot_sqlite(source: Path, destination: Path) -> None:
    """Create and validate a consistent SQLite snapshot."""
    source = source.resolve()
    destination = destination.resolve()
    if not source.is_file():
        raise ValueError(f"peer cache seed is not a file: {source}")
    if destination.exists():
        raise ValueError(f"peer cache snapshot already exists: {destination}")

    destination.parent.mkdir(parents=True, exist_ok=True)
    with (
        sqlite3.connect(f"{source.as_uri()}?mode=ro", uri=True) as source_db,
        sqlite3.connect(destination) as destination_db,
    ):
        source_db.backup(destination_db)

    with sqlite3.connect(f"{destination.as_uri()}?mode=ro", uri=True) as check_db:
        result = check_db.execute("PRAGMA quick_check").fetchone()
    if result != ("ok",):
        raise RuntimeError(f"peer cache snapshot failed SQLite quick_check: {result!r}")


def prepare_run_dir(run_dir: Path | None, tmp_root: Path) -> Path:
    if run_dir is None:
        tmp_root.mkdir(parents=True, exist_ok=True)
        return Path(
            tempfile.mkdtemp(
                prefix="catalogue-xprv-live-gate.",
                dir=tmp_root,
            )
        ).resolve()

    resolved = run_dir.resolve()
    if resolved.exists() and any(resolved.iterdir()):
        raise ValueError(f"run directory is not empty: {resolved}")
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def build_command(
    *,
    runner: Path,
    xprv: Path,
    peer_cache: Path,
    artifact_dir: Path,
    network: int,
    threads: int,
    pytest_args: list[str],
) -> list[str]:
    return [
        sys.executable,
        str(runner),
        "--xprv",
        str(xprv),
        "--network",
        str(network),
        "--peer-cache-path",
        str(peer_cache),
        "--threads",
        str(threads),
        "--artifact-dir",
        str(artifact_dir),
        "-q",
        "-k",
        CANONICAL_FILTER,
        *pytest_args,
    ]


def write_receipt(path: Path, receipt: dict[str, Any]) -> None:
    candidate = path.with_suffix(".json.tmp")
    candidate.write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    candidate.replace(path)


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def run_self_test() -> int:
    checks: list[tuple[str, bool]] = []
    checks.append(
        (
            "canonical case population",
            len(CANONICAL_CASES) == 4 and len(set(CANONICAL_CASES)) == 4,
        )
    )
    checks.append(
        (
            "canonical filter is total",
            all(case in CANONICAL_FILTER for case in CANONICAL_CASES),
        )
    )

    with tempfile.TemporaryDirectory(prefix="xprv-live-proof-gate-self-test.") as temp:
        root = Path(temp)
        source = root / "source.sqlite3"
        snapshot = root / "snapshot.sqlite3"
        with sqlite3.connect(source) as database:
            database.execute("CREATE TABLE peer (id INTEGER PRIMARY KEY, value TEXT)")
            database.execute("INSERT INTO peer(value) VALUES ('warm')")
        snapshot_sqlite(source, snapshot)
        with sqlite3.connect(snapshot) as database:
            copied = database.execute("SELECT value FROM peer").fetchall()
        checks.append(("SQLite snapshot retains rows", copied == [("warm",)]))

        command = build_command(
            runner=root / "runner.py",
            xprv=root / "xprv",
            peer_cache=snapshot,
            artifact_dir=root / "artifacts",
            network=0,
            threads=2,
            pytest_args=["--maxfail=1"],
        )
        checks.append(
            (
                "command binds canonical inputs",
                command.count("-k") == 1
                and CANONICAL_FILTER in command
                and str(snapshot) in command
                and "--maxfail=1" in command,
            )
        )

    failures = [name for name, passed in checks if not passed]
    if failures:
        for name in failures:
            print(f"self-test failed: {name}", file=sys.stderr)
        return 1
    print(f"self-test: {len(checks)}/{len(checks)}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--xprv",
        default=os.environ.get("XPRV_BIN", "build/src/xprv/xprv"),
        help="Exact xprv binary to exercise",
    )
    parser.add_argument(
        "--peer-cache-seed",
        default=os.environ.get("XPRV_PEER_CACHE_SEED"),
        help=("Warm peer-endpoints.sqlite3 to snapshot (or set XPRV_PEER_CACHE_SEED)"),
    )
    parser.add_argument(
        "--run-dir",
        type=Path,
        help="Use this empty directory instead of allocating under --tmp-root",
    )
    parser.add_argument(
        "--tmp-root",
        type=Path,
        default=DEFAULT_TMP_ROOT,
        help=f"Temporary run parent (default: {DEFAULT_TMP_ROOT})",
    )
    parser.add_argument("--network", type=int, default=0)
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Test preparation mechanics without starting xprv",
    )
    parser.add_argument(
        "pytest_args",
        nargs=argparse.REMAINDER,
        help="Additional pytest arguments after --",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.self_test:
        return run_self_test()
    if not args.peer_cache_seed:
        parser.error(
            "--peer-cache-seed or XPRV_PEER_CACHE_SEED is required; "
            "the canonical live gate must not silently start with an empty cache"
        )
    if args.threads < 1:
        parser.error("--threads must be at least 1")

    project_root = Path(__file__).resolve().parent.parent
    runner = project_root / "scripts" / "xprv-serve-smoke-v2.py"
    xprv = Path(args.xprv).resolve()
    peer_cache_seed = Path(args.peer_cache_seed).resolve()
    if not runner.is_file():
        parser.error(f"smoke runner not found: {runner}")
    if not xprv.is_file() or not os.access(xprv, os.X_OK):
        parser.error(f"xprv binary is not executable: {xprv}")

    try:
        run_dir = prepare_run_dir(args.run_dir, args.tmp_root)
        peer_cache = run_dir / "peer-endpoints.sqlite3"
        artifact_dir = run_dir / "smoke"
        artifact_dir.mkdir()
        snapshot_sqlite(peer_cache_seed, peer_cache)
    except (OSError, sqlite3.Error, ValueError, RuntimeError) as error:
        parser.error(str(error))

    pytest_args = list(args.pytest_args)
    if pytest_args[:1] == ["--"]:
        pytest_args.pop(0)
    command = build_command(
        runner=runner,
        xprv=xprv,
        peer_cache=peer_cache,
        artifact_dir=artifact_dir,
        network=args.network,
        threads=args.threads,
        pytest_args=pytest_args,
    )

    receipt_path = run_dir / "run.json"
    receipt: dict[str, Any] = {
        "artifact_dir": str(artifact_dir),
        "canonical_cases": list(CANONICAL_CASES),
        "command": command,
        "finished_at": None,
        "network": args.network,
        "peer_cache_seed": str(peer_cache_seed),
        "peer_cache_seed_sha256": file_sha256(peer_cache_seed),
        "peer_cache_snapshot": str(peer_cache),
        "peer_cache_snapshot_sha256": file_sha256(peer_cache),
        "result": "running",
        "runner": str(runner),
        "runner_sha256": file_sha256(runner),
        "run_dir": str(run_dir),
        "started_at": utc_now(),
        "threads": args.threads,
        "xprv": str(xprv),
        "xprv_sha256": file_sha256(xprv),
    }
    write_receipt(receipt_path, receipt)

    print(f"RUN_DIR: {run_dir}", flush=True)
    print(f"XPRV: {xprv}", flush=True)
    print(f"PEER_CACHE: {peer_cache}", flush=True)
    print(f"COMMAND: {shlex.join(command)}", flush=True)

    started = time.monotonic()
    try:
        completed = subprocess.run(command, check=False)
        return_code = completed.returncode
        result = "passed" if return_code == 0 else "failed"
    except KeyboardInterrupt:
        return_code = 130
        result = "interrupted"

    receipt.update(
        {
            "duration_seconds": round(time.monotonic() - started, 3),
            "exit_code": return_code,
            "finished_at": utc_now(),
            "result": result,
        }
    )
    write_receipt(receipt_path, receipt)

    print(f"RESULT: {result} (exit {return_code})", flush=True)
    print(f"SERVER_LOG: {artifact_dir / 'server.log'}", flush=True)
    print(f"RUNNER_LOG: {artifact_dir / 'runner.log'}", flush=True)
    print(f"INDEX: {artifact_dir / 'index.jsonl'}", flush=True)
    print(f"RECEIPT: {receipt_path}", flush=True)
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
