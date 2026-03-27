#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.14"
# dependencies = []
# ///
"""
Run one isolated xprv-serve case with immediate, readable output.

This is intentionally not pytest. It is a debug-first runner:
- starts a fresh xprv serve process for each invocation
- runs exactly one named case
- prints the exact HTTP failure and a server-log tail on error
- leaves artifacts in a single run directory

Examples:
    scripts/xprv-serve-check.py health --xprv build/src/xprv/xprv
    scripts/xprv-serve-check.py prove-json --threads 4 --tx-corpus scripts/xprv-tx-corpus.generated.json
    scripts/xprv-serve-check.py verify-json-roundtrip --tx 8AF2DE8804721B0EC9E11FD66B9EF3C30962E71DC0D707CA1F4CDE4655751420
    scripts/xprv-serve-check.py concurrent-prove --concurrency 8 --threads 4
"""

from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
import http.client
import json
import os
import shlex
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_TX_HASH = (
    "8AF2DE8804721B0EC9E11FD66B9EF3C30962E71DC0D707CA1F4CDE4655751420"
)


class CaseFailure(RuntimeError):
    pass


@dataclass(frozen=True)
class TxCase:
    tx_hash: str
    label: str
    ledger_index: int | None = None


@dataclass(frozen=True)
class HttpResult:
    status: int
    headers: dict[str, str]
    body: bytes


@dataclass(frozen=True)
class RunConfig:
    xprv: Path
    artifact_dir: Path
    server_log: Path
    bind_address: str
    port: int
    network: int
    rpc: str | None
    peer: str | None
    peer_cache_path: Path | None
    startup_timeout: float
    request_timeout: float
    shutdown_timeout: float
    threads: int
    no_cache: bool
    keep_server: bool
    tx_case: TxCase
    concurrency: int
    server_log_tail: int
    tmux: bool
    lldb: bool

    @property
    def bind(self) -> str:
        return f"{self.bind_address}:{self.port}"


class RunningServer:
    def __init__(self, config: RunConfig) -> None:
        self.config = config
        self.proc: subprocess.Popen[bytes] | None = None
        self.log_handle = None

    def start(self) -> None:
        cmd = [
            str(self.config.xprv),
            "serve",
            "--bind",
            self.config.bind,
            "--network",
            str(self.config.network),
        ]
        if self.config.rpc:
            cmd += ["--rpc", self.config.rpc]
        if self.config.peer:
            cmd += ["--peer", self.config.peer]
        if self.config.peer_cache_path:
            cmd += ["--peer-cache-path", str(self.config.peer_cache_path)]
        if self.config.threads > 1:
            cmd += ["--threads", str(self.config.threads)]
        if self.config.no_cache:
            cmd += ["--no-cache"]

        self.config.artifact_dir.mkdir(parents=True, exist_ok=True)

        if self.config.tmux:
            self._start_tmux(cmd)
        else:
            self._start_direct(cmd)

        self._wait_for_port()
        self._wait_for_health()

    def _start_direct(self, cmd: list[str]) -> None:
        self.log_handle = self.config.server_log.open("wb")
        self.proc = subprocess.Popen(
            cmd,
            cwd=Path.cwd(),
            stdout=self.log_handle,
            stderr=subprocess.STDOUT,
            bufsize=0,
        )

    def _start_tmux(self, cmd: list[str]) -> None:
        import shlex

        self.tmux_session = "xprv"
        server_cmd = shlex.join(cmd) + f" 2>&1 | tee {self.config.server_log}"
        if self.config.lldb:
            server_cmd = f"lldb -o run -- {shlex.join(cmd)}"

        # Kill existing session if any
        subprocess.run(
            ["tmux", "kill-session", "-t", self.tmux_session],
            capture_output=True,
        )
        subprocess.run(
            ["tmux", "new-session", "-d", "-s", self.tmux_session, server_cmd],
            check=True,
        )
        # Get the tmux pane's PID for cleanup
        result = subprocess.run(
            ["tmux", "list-panes", "-t", self.tmux_session, "-F", "#{pane_pid}"],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0 and result.stdout.strip():
            # Store a fake Popen-like for stop()
            self._tmux_pid = int(result.stdout.strip())

        print(
            f"TMUX: tmux attach -t {self.tmux_session}",
            file=sys.stderr,
        )

    def stop(self) -> None:
        if self.config.keep_server:
            print(
                f"[xprv-serve-check] leaving server running on {self.config.bind}",
                file=sys.stderr,
            )
            if self.config.tmux:
                print(
                    f"TMUX: tmux attach -t {getattr(self, 'tmux_session', '?')}",
                    file=sys.stderr,
                )
            return

        if self.config.tmux and hasattr(self, "tmux_session"):
            subprocess.run(
                ["tmux", "send-keys", "-t", self.tmux_session, "C-c", ""],
                capture_output=True,
            )
            time.sleep(1)
            subprocess.run(
                ["tmux", "kill-session", "-t", self.tmux_session],
                capture_output=True,
            )
        elif self.proc is not None:
            if self.proc.poll() is None:
                self.proc.send_signal(signal.SIGINT)
                try:
                    self.proc.wait(timeout=self.config.shutdown_timeout)
                except subprocess.TimeoutExpired:
                    self.proc.terminate()
                    try:
                        self.proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        self.proc.kill()
                        self.proc.wait(timeout=5)

        if self.log_handle is not None:
            self.log_handle.close()

    def request(
        self,
        method: str,
        path: str,
        *,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
        connection: http.client.HTTPConnection | None = None,
        timeout: float | None = None,
    ) -> HttpResult:
        close_conn = connection is None
        conn = connection or http.client.HTTPConnection(
            self.config.bind_address,
            self.config.port,
            timeout=timeout or self.config.request_timeout,
        )
        try:
            conn.request(method, path, body=body, headers=headers or {})
            resp = conn.getresponse()
            data = resp.read()
            return HttpResult(
                status=resp.status,
                headers={k.lower(): v for k, v in resp.getheaders()},
                body=data,
            )
        finally:
            if close_conn:
                conn.close()

    def failure_context(self) -> str:
        rc = None if self.proc is None else self.proc.poll()
        return (
            f"bind={self.config.bind}\n"
            f"artifact_dir={self.config.artifact_dir}\n"
            f"server_log={self.config.server_log}\n"
            f"server_rc={rc}\n"
            f"[server log tail]\n{self.tail_logs()}"
        )

    def tail_logs(self) -> str:
        try:
            text = self.config.server_log.read_text(encoding="utf-8", errors="replace")
        except FileNotFoundError:
            return ""
        if len(text) <= self.config.server_log_tail:
            return text
        return text[-self.config.server_log_tail :]

    def _wait_for_port(self) -> None:
        deadline = time.monotonic() + self.config.startup_timeout
        while time.monotonic() < deadline:
            if self.proc is not None and self.proc.poll() is not None:
                raise CaseFailure(
                    "xprv serve exited before opening the port\n"
                    + self.failure_context()
                )
            with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
                sock.settimeout(0.5)
                if sock.connect_ex((self.config.bind_address, self.config.port)) == 0:
                    return
            time.sleep(0.1)
        raise CaseFailure(
            f"xprv serve never opened {self.config.bind} within "
            f"{self.config.startup_timeout:.1f}s\n{self.failure_context()}"
        )

    def _wait_for_health(self) -> None:
        deadline = time.monotonic() + self.config.startup_timeout
        last_error = "unknown error"
        while time.monotonic() < deadline:
            if self.proc is not None and self.proc.poll() is not None:
                raise CaseFailure(
                    "xprv serve exited before /health became ready\n"
                    + self.failure_context()
                )
            try:
                result = self.request("GET", "/health", timeout=2.0)
                if result.status == 200:
                    decode_json(result.body, context="/health startup")
                    return
                last_error = f"status={result.status} body={truncate_bytes(result.body)}"
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
            time.sleep(0.25)
        raise CaseFailure(f"/health never became ready: {last_error}\n{self.failure_context()}")


def truncate_bytes(data: bytes, limit: int = 500) -> str:
    preview = data[:limit]
    return preview.decode("utf-8", errors="replace")


def decode_json(data: bytes, *, context: str) -> Any:
    try:
        return json.loads(data)
    except json.JSONDecodeError as exc:
        raise CaseFailure(f"{context}: invalid JSON: {exc}\nraw={truncate_bytes(data)}") from exc


def assert_status(result: HttpResult, expected: int, *, context: str) -> None:
    if result.status != expected:
        raise CaseFailure(
            f"{context}: expected HTTP {expected}, got {result.status}\n"
            f"headers={result.headers}\nbody={truncate_bytes(result.body)}"
        )


def assert_json_result(result: HttpResult, *, context: str) -> dict[str, Any]:
    assert_status(result, 200, context=context)
    content_type = result.headers.get("content-type", "")
    if "application/json" not in content_type:
        raise CaseFailure(f"{context}: expected JSON content-type, got {content_type!r}")
    body = decode_json(result.body, context=context)
    if not isinstance(body, dict):
        raise CaseFailure(f"{context}: expected JSON object, got {type(body).__name__}")
    return body


def assert_octet_stream(result: HttpResult, *, context: str) -> None:
    assert_status(result, 200, context=context)
    content_type = result.headers.get("content-type", "")
    if "application/octet-stream" not in content_type:
        raise CaseFailure(
            f"{context}: expected octet-stream content-type, got {content_type!r}"
        )


def binary_network_id(data: bytes) -> int:
    if len(data) < 10:
        raise CaseFailure(f"binary proof too short: {len(data)} bytes")
    return int.from_bytes(data[6:10], "little")


def find_free_port(host: str) -> int:
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.bind((host, 0))
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return int(sock.getsockname()[1])


def prove_path(tx_hash: str) -> str:
    return f"/prove?tx={urllib.parse.quote(tx_hash, safe='')}"


def run_concurrently(count: int, fn) -> list[Any]:
    with concurrent.futures.ThreadPoolExecutor(max_workers=count) as executor:
        futures = [executor.submit(fn, index) for index in range(count)]
        return [future.result() for future in futures]


def case_health(server: RunningServer) -> None:
    body = assert_json_result(server.request("GET", "/health"), context="GET /health")
    if "peer_count" not in body or "vl_loaded" not in body:
        raise CaseFailure(f"/health missing expected keys: {body}")


def case_prove_json(server: RunningServer) -> None:
    result = server.request("GET", prove_path(server.config.tx_case.tx_hash))
    body = assert_json_result(result, context="GET /prove JSON")
    if body.get("network_id") != server.config.network:
        raise CaseFailure(
            f"/prove JSON network mismatch: expected {server.config.network}, got {body.get('network_id')}"
        )
    steps = body.get("steps")
    if not isinstance(steps, list) or not steps:
        raise CaseFailure(f"/prove JSON missing non-empty steps array: {body}")


def case_prove_bin(server: RunningServer) -> None:
    result = server.request("GET", prove_path(server.config.tx_case.tx_hash) + "&format=bin")
    assert_octet_stream(result, context="GET /prove bin")
    if not result.body.startswith(b"XPRV"):
        raise CaseFailure("binary proof missing XPRV magic")
    if binary_network_id(result.body) != server.config.network:
        raise CaseFailure(
            f"binary proof network mismatch: expected {server.config.network}, "
            f"got {binary_network_id(result.body)}"
        )


def case_verify_json_roundtrip(server: RunningServer) -> None:
    prove = server.request("GET", prove_path(server.config.tx_case.tx_hash))
    prove_body = assert_json_result(prove, context="GET /prove JSON")
    verify = server.request(
        "POST",
        "/verify",
        body=prove.body,
        headers={"Content-Type": "application/json"},
    )
    verify_body = assert_json_result(verify, context="POST /verify JSON")
    if verify_body.get("verified") is not True:
        raise CaseFailure(f"/verify JSON returned failure: {verify_body}")
    if prove_body.get("network_id") != server.config.network:
        raise CaseFailure(f"/prove JSON network mismatch: {prove_body}")


def case_verify_bin_roundtrip(server: RunningServer) -> None:
    prove = server.request("GET", prove_path(server.config.tx_case.tx_hash) + "&format=bin")
    assert_octet_stream(prove, context="GET /prove bin")
    verify = server.request(
        "POST",
        "/verify",
        body=prove.body,
        headers={"Content-Type": "application/octet-stream"},
    )
    verify_body = assert_json_result(verify, context="POST /verify bin")
    if verify_body.get("verified") is not True:
        raise CaseFailure(f"/verify bin returned failure: {verify_body}")


def case_keep_alive(server: RunningServer) -> None:
    conn = http.client.HTTPConnection(
        server.config.bind_address,
        server.config.port,
        timeout=server.config.request_timeout,
    )
    try:
        first = server.request("GET", "/health", connection=conn)
        second = server.request("GET", "/nope", connection=conn)
        third = server.request("GET", "/health", connection=conn)
    finally:
        conn.close()

    assert_status(first, 200, context="keep-alive first /health")
    assert_status(second, 404, context="keep-alive /nope")
    assert_status(third, 200, context="keep-alive third /health")


def case_concurrent_prove(server: RunningServer) -> None:
    tx_hash = server.config.tx_case.tx_hash

    def do_request(index: int) -> None:
        path = prove_path(tx_hash)
        if index % 2 == 1:
            path += "&format=bin"
        result = server.request("GET", path)
        if index % 2 == 0:
            body = assert_json_result(result, context=f"concurrent prove JSON {index}")
            if body.get("network_id") != server.config.network:
                raise CaseFailure(f"concurrent prove JSON network mismatch: {body}")
        else:
            assert_octet_stream(result, context=f"concurrent prove bin {index}")

    run_concurrently(server.config.concurrency, do_request)


def case_concurrent_verify(server: RunningServer) -> None:
    json_proof = server.request("GET", prove_path(server.config.tx_case.tx_hash))
    assert_json_result(json_proof, context="prefetch prove JSON")
    bin_proof = server.request("GET", prove_path(server.config.tx_case.tx_hash) + "&format=bin")
    assert_octet_stream(bin_proof, context="prefetch prove bin")

    def do_request(index: int) -> None:
        if index % 2 == 0:
            result = server.request(
                "POST",
                "/verify",
                body=json_proof.body,
                headers={"Content-Type": "application/json"},
            )
            body = assert_json_result(result, context=f"concurrent verify JSON {index}")
        else:
            result = server.request(
                "POST",
                "/verify",
                body=bin_proof.body,
                headers={"Content-Type": "application/octet-stream"},
            )
            body = assert_json_result(result, context=f"concurrent verify bin {index}")
        if body.get("verified") is not True:
            raise CaseFailure(f"concurrent verify failed: {body}")

    run_concurrently(server.config.concurrency, do_request)


CASES = {
    "health": case_health,
    "prove-json": case_prove_json,
    "prove-bin": case_prove_bin,
    "verify-json-roundtrip": case_verify_json_roundtrip,
    "verify-bin-roundtrip": case_verify_bin_roundtrip,
    "keep-alive": case_keep_alive,
    "concurrent-prove": case_concurrent_prove,
    "concurrent-verify": case_concurrent_verify,
}


def load_tx_case(
    *,
    tx_hash: str | None,
    tx_corpus: Path | None,
    corpus_set: str,
    corpus_index: int,
) -> TxCase:
    if tx_corpus is None:
        return TxCase(tx_hash=(tx_hash or DEFAULT_TX_HASH).upper(), label="direct")

    raw = json.loads(tx_corpus.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        items = raw
    else:
        items = raw.get(f"{corpus_set}_set", [])
    if corpus_index >= len(items):
        raise CaseFailure(
            f"tx corpus {tx_corpus} has no {corpus_set}[{corpus_index}] entry"
        )
    item = items[corpus_index]
    if isinstance(item, str):
        return TxCase(tx_hash=item.upper(), label=f"{corpus_set}-{corpus_index}")
    return TxCase(
        tx_hash=str(item["tx_hash"]).upper(),
        label=str(item.get("label") or f"{corpus_set}-{corpus_index}"),
        ledger_index=int(item["ledger_index"]) if item.get("ledger_index") is not None else None,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("case", nargs="?", help="Case name to run")
    parser.add_argument("--list-cases", action="store_true", help="List available cases and exit")
    parser.add_argument("--xprv", default="build/src/xprv/xprv", help="Path to xprv binary")
    parser.add_argument("--artifact-dir", help="Artifact directory for this run")
    parser.add_argument("--bind-address", default="127.0.0.1", help="Server bind address")
    parser.add_argument("--port", type=int, help="Server port (default: pick a free one)")
    parser.add_argument("--network", type=int, default=0, help="Network ID")
    parser.add_argument("--rpc", help="Override RPC endpoint host:port")
    parser.add_argument("--peer", help="Override peer endpoint host:port")
    parser.add_argument("--peer-cache-path", help="Override peer cache path")
    parser.add_argument("--threads", type=int, default=1, help="Server I/O thread count")
    parser.add_argument("--no-cache", action="store_true", help="Disable proof cache")
    parser.add_argument("--startup-timeout", type=float, default=20.0, help="Seconds to wait for server startup")
    parser.add_argument("--request-timeout", type=float, default=120.0, help="Seconds to allow per request")
    parser.add_argument("--shutdown-timeout", type=float, default=10.0, help="Seconds to wait after SIGINT")
    parser.add_argument("--keep-server", action="store_true", help="Leave server running after the case")
    parser.add_argument("--tmux", action="store_true", help="Run server in a tmux session you can attach to")
    parser.add_argument("--lldb", action="store_true", help="Run server under lldb in tmux (implies --tmux --keep-server)")
    parser.add_argument("--tx", help="Transaction hash to use")
    parser.add_argument("--tx-corpus", help="Optional tx corpus JSON file")
    parser.add_argument(
        "--corpus-set",
        choices=("hot", "cold", "bad"),
        default="hot",
        help="Which tx corpus set to pull from",
    )
    parser.add_argument("--corpus-index", type=int, default=0, help="Index within the selected tx corpus set")
    parser.add_argument("--concurrency", type=int, default=8, help="Concurrency for concurrent-* cases")
    parser.add_argument("--server-log-tail", type=int, default=16000, help="Characters of server log to print on failure")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.list_cases:
        for name in sorted(CASES):
            print(name)
        return 0

    if not args.case:
        parser.error("missing case name; use --list-cases to see options")
    if args.case not in CASES:
        parser.error(f"unknown case {args.case!r}; use --list-cases")

    xprv = Path(args.xprv)
    if not xprv.exists():
        raise SystemExit(f"xprv binary not found: {xprv}")

    artifact_dir = (
        Path(args.artifact_dir)
        if args.artifact_dir
        else Path(tempfile.mkdtemp(prefix="xprv-serve-check-"))
    )
    artifact_dir.mkdir(parents=True, exist_ok=True)

    port = args.port or find_free_port(args.bind_address)
    tx_case = load_tx_case(
        tx_hash=args.tx,
        tx_corpus=Path(args.tx_corpus) if args.tx_corpus else None,
        corpus_set=args.corpus_set,
        corpus_index=args.corpus_index,
    )

    peer_cache_path = (
        Path(args.peer_cache_path)
        if args.peer_cache_path
        else artifact_dir / "peer-endpoints.sqlite3"
    )
    config = RunConfig(
        xprv=xprv,
        artifact_dir=artifact_dir,
        server_log=artifact_dir / "server.log",
        bind_address=args.bind_address,
        port=port,
        network=args.network,
        rpc=args.rpc,
        peer=args.peer,
        peer_cache_path=peer_cache_path,
        startup_timeout=args.startup_timeout,
        request_timeout=args.request_timeout,
        shutdown_timeout=args.shutdown_timeout,
        threads=args.threads,
        no_cache=args.no_cache,
        keep_server=args.keep_server or args.lldb,
        tx_case=tx_case,
        concurrency=max(1, args.concurrency),
        server_log_tail=args.server_log_tail,
        tmux=args.tmux or args.lldb,
        lldb=args.lldb,
    )

    print(f"CASE: {args.case}", flush=True)
    print(f"XPRV: {config.xprv}", flush=True)
    print(f"BIND: {config.bind}", flush=True)
    print(f"TX: {config.tx_case.tx_hash} ({config.tx_case.label})", flush=True)
    print(f"ARTIFACT_DIR: {config.artifact_dir}", flush=True)
    print(f"SERVER_LOG: {config.server_log}", flush=True)
    if config.peer_cache_path is not None:
        print(f"PEER_CACHE: {config.peer_cache_path}", flush=True)
    print(f"TAIL: tail -f {shlex.quote(str(config.server_log))}", flush=True)

    server = RunningServer(config)
    try:
        server.start()
        started = time.time()
        CASES[args.case](server)
        duration = time.time() - started
        print(f"PASS: {args.case} ({duration:.2f}s)")
        print(f"SERVER_LOG: {config.server_log}")
        return 0
    except CaseFailure as exc:
        print(f"FAIL: {args.case}", file=sys.stderr)
        print(str(exc), file=sys.stderr)
        print(server.failure_context(), file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {args.case}: {exc}", file=sys.stderr)
        print(server.failure_context(), file=sys.stderr)
        raise
    finally:
        server.stop()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
