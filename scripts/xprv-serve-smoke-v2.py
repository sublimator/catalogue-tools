#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.14"
# dependencies = ["pytest>=8.3,<9"]
# ///
"""
Pytest integration suite for `xprv serve` with searchable log artifacts.

This file is both:
- an executable UV script that calls `pytest.main()` on itself
- a real pytest module with fixtures and parametrized integration tests

Compared to v1, this version writes durable artifacts:
- one session-wide `server.log`
- one session-wide `runner.log`
- one `tests/<slug>/server.log` slice per test
- one `tests/<slug>/meta.json` per test
- one `index.jsonl` manifest for grepping and tooling

Examples:
    scripts/xprv-serve-smoke-v2.py --xprv build/src/xprv/xprv -q
    scripts/xprv-serve-smoke-v2.py --one prove_json_shape --threads 4
    scripts/xprv-serve-smoke-v2.py --xprv build/src/xprv/xprv \
      --trace-http --log-level DEBUG --harsh-winds -q
    scripts/xprv-serve-smoke-v2.py --tx-corpus scripts/xprv-tx-corpus.sample.json -q
"""

from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
import hashlib
import http.client
import json
import logging
import os
import random
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any

import pytest


DEFAULT_TX_HASH = (
    "8AF2DE8804721B0EC9E11FD66B9EF3C30962E71DC0D707CA1F4CDE4655751420"
)
MAX_REQUEST_BODY = 512 * 1024
LOG_FORMAT = "[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s"
LOG = logging.getLogger("xprv-serve-smoke-v2")


def configure_logging(
    level_name: str | None = None,
    *,
    log_path: Path | None = None,
) -> None:
    level_text = (
        level_name or os.environ.get("XPRV_TEST_LOG_LEVEL", "INFO")
    ).upper()
    level = getattr(logging, level_text, logging.INFO)
    root = logging.getLogger()
    formatter = logging.Formatter(LOG_FORMAT, datefmt="%H:%M:%S")

    if not root.handlers:
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        root.addHandler(console)

    root.setLevel(level)
    LOG.setLevel(level)

    if log_path is not None:
        resolved = str(log_path.resolve())
        for handler in root.handlers:
            if isinstance(handler, logging.FileHandler):
                if getattr(handler, "_xprv_log_path", None) == resolved:
                    handler.setLevel(level)
                    return

        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(level)
        file_handler._xprv_log_path = resolved  # type: ignore[attr-defined]
        root.addHandler(file_handler)


configure_logging()


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str) -> int | None:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return None
    return int(raw)


def env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return float(raw)


def harsh_default(normal: int, harsh: int) -> int:
    return harsh if env_bool("XPRV_HARSH_WINDS") else normal


def sample_fd_count(pid: int) -> int | None:
    proc_fd = Path(f"/proc/{pid}/fd")
    if proc_fd.exists():
        try:
            return len(list(proc_fd.iterdir()))
        except OSError:
            return None

    lsof = shutil.which("lsof")
    if not lsof:
        return None

    try:
        result = subprocess.run(
            [lsof, "-n", "-P", "-p", str(pid)],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None

    if result.returncode != 0 or not result.stdout:
        return None

    return max(0, len(result.stdout.splitlines()) - 1)


def find_free_port(host: str) -> int:
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.bind((host, 0))
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return int(sock.getsockname()[1])


def slugify_nodeid(nodeid: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9._-]+", "-", nodeid).strip("-")
    if len(safe) <= 180:
        return safe or "test"
    digest = hashlib.sha1(nodeid.encode("utf-8")).hexdigest()[:12]
    return f"{safe[:160]}-{digest}"


def run_concurrently(
    count: int,
    *,
    max_workers: int | None = None,
    fn,
) -> list[Any]:
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max_workers or count
    ) as executor:
        futures = [executor.submit(fn, index) for index in range(count)]
        return [future.result() for future in futures]


def assert_json_content_type(result: "HttpResult") -> None:
    ctype = result.headers.get("content-type", "")
    assert "application/json" in ctype, (
        f"expected application/json content-type, got {ctype!r}"
    )


def assert_octet_stream_content_type(result: "HttpResult") -> None:
    ctype = result.headers.get("content-type", "")
    assert "application/octet-stream" in ctype, (
        f"expected application/octet-stream content-type, got {ctype!r}"
    )


def decode_json_bytes(data: bytes, *, context: str) -> Any:
    try:
        return json.loads(data)
    except json.JSONDecodeError as exc:
        pytest.fail(f"{context}: invalid JSON body: {exc}\nraw={data[:500]!r}")


def binary_network_id(data: bytes) -> int:
    assert len(data) >= 10, f"binary proof too short for v2 header: {len(data)} bytes"
    return int.from_bytes(data[6:10], "little")


def normalize_tx_case(
    raw: str | dict[str, Any],
    *,
    default_label_prefix: str,
    index: int,
) -> TxCase:
    if isinstance(raw, str):
        tx_hash = raw
        label = f"{default_label_prefix}-{index + 1}"
        ledger_index = None
    elif isinstance(raw, dict):
        tx_hash = str(raw["tx_hash"])
        label = str(raw.get("label") or f"{default_label_prefix}-{index + 1}")
        ledger_raw = raw.get("ledger_index")
        ledger_index = int(ledger_raw) if ledger_raw is not None else None
    else:
        raise ValueError(f"unsupported tx corpus entry: {raw!r}")

    if len(tx_hash) != 64 or any(ch not in "0123456789abcdefABCDEF" for ch in tx_hash):
        raise ValueError(f"invalid tx hash in corpus: {tx_hash!r}")

    return TxCase(
        tx_hash=tx_hash.upper(),
        label=label,
        ledger_index=ledger_index,
    )


def dedupe_tx_cases(cases: list[TxCase]) -> tuple[TxCase, ...]:
    seen: set[str] = set()
    deduped: list[TxCase] = []
    for case in cases:
        if case.tx_hash in seen:
            continue
        seen.add(case.tx_hash)
        deduped.append(case)
    return tuple(deduped)


def load_tx_corpus(
    path: Path | None,
    *,
    fallback_tx_hash: str,
) -> TxCorpus:
    if path is None:
        return TxCorpus(
            network_id=None,
            hot_set=(
                TxCase(
                    tx_hash=fallback_tx_hash.upper(),
                    label="default-hot-1",
                    ledger_index=None,
                ),
            ),
            cold_set=(),
            bad_set=(),
        )

    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        hot = [normalize_tx_case(item, default_label_prefix="hot", index=i) for i, item in enumerate(raw)]
        return TxCorpus(
            network_id=None,
            hot_set=dedupe_tx_cases(hot),
            cold_set=(),
            bad_set=(),
        )

    if not isinstance(raw, dict):
        raise ValueError(f"tx corpus must be a JSON object or array, got {type(raw).__name__}")

    hot_raw = raw.get("hot_set", [])
    cold_raw = raw.get("cold_set", [])
    bad_raw = raw.get("bad_set", [])

    if not hot_raw:
        hot_raw = [{"tx_hash": fallback_tx_hash, "label": "default-hot-1"}]

    corpus = TxCorpus(
        network_id=int(raw["network_id"]) if raw.get("network_id") is not None else None,
        hot_set=dedupe_tx_cases(
            [normalize_tx_case(item, default_label_prefix="hot", index=i) for i, item in enumerate(hot_raw)]
        ),
        cold_set=dedupe_tx_cases(
            [normalize_tx_case(item, default_label_prefix="cold", index=i) for i, item in enumerate(cold_raw)]
        ),
        bad_set=dedupe_tx_cases(
            [normalize_tx_case(item, default_label_prefix="bad", index=i) for i, item in enumerate(bad_raw)]
        ),
    )
    if not corpus.hot_set:
        raise ValueError("tx corpus hot_set cannot be empty")
    return corpus


def prove_path_for_case(case: TxCase) -> str:
    encoded_tx = urllib.parse.quote(case.tx_hash, safe="")
    return f"/prove?tx={encoded_tx}"


def cache_stats_from_health(body: dict[str, Any]) -> dict[str, int]:
    cache = body.get("proof_cache") or body.get("cache")
    assert isinstance(cache, dict), f"/health missing proof_cache object: {body}"
    return {
        "entries": int(cache["entries"]),
        "max_entries": int(cache["max_entries"]),
        "hits": int(cache["hits"]),
        "misses": int(cache["misses"]),
    }


def default_rpc_endpoint_for_network(network_id: int) -> str:
    if network_id == 21337:
        return "xahau.network:443"
    return "s1.ripple.com:443"


def rpc_url_from_endpoint(endpoint: str | None, network_id: int) -> str:
    raw = endpoint or default_rpc_endpoint_for_network(network_id)
    if "://" in raw:
        return raw.rstrip("/") + "/"
    scheme = "https" if raw.endswith(":443") else "http"
    return f"{scheme}://{raw}/"


def rpc_call(url: str, method: str, params: list[dict[str, Any]]) -> dict[str, Any]:
    payload = json.dumps({"method": method, "params": params}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        data = json.loads(resp.read())
    if "result" not in data:
        raise RuntimeError(f"RPC response missing result for method={method}: {data!r}")
    return data["result"]


def gather_latest_validated_ledger(url: str) -> int:
    result = rpc_call(url, "server_info", [{}])
    info = result.get("info")
    if not isinstance(info, dict):
        raise RuntimeError(f"server_info missing info object: {result!r}")
    validated = info.get("validated_ledger")
    if not isinstance(validated, dict) or "seq" not in validated:
        raise RuntimeError(f"server_info missing validated_ledger.seq: {result!r}")
    return int(validated["seq"])


def extract_ledger_tx_hashes(result: dict[str, Any], ledger_index: int) -> list[str]:
    ledger = result.get("ledger")
    if not isinstance(ledger, dict):
        return []
    txns = ledger.get("transactions", [])
    if not isinstance(txns, list):
        return []
    hashes: list[str] = []
    for tx in txns:
        tx_hash = tx if isinstance(tx, str) else tx.get("hash") if isinstance(tx, dict) else None
        if not isinstance(tx_hash, str):
            continue
        if len(tx_hash) == 64:
            hashes.append(tx_hash.upper())
        else:
            LOG.debug("ignoring malformed tx hash from ledger %s: %r", ledger_index, tx_hash)
    return hashes


def gather_corpus_file(
    output_path: Path,
    *,
    rpc_endpoint: str | None,
    network_id: int,
    hot_count: int,
    cold_count: int,
    min_ledger: int,
    max_ledger: int | None,
    max_attempts: int,
    seed: int | None,
) -> None:
    rng = random.Random(seed)
    rpc_url = rpc_url_from_endpoint(rpc_endpoint, network_id)
    latest_validated = gather_latest_validated_ledger(rpc_url)
    effective_max = min(max_ledger or max(latest_validated - 128, min_ledger), latest_validated)
    if effective_max < min_ledger:
        raise RuntimeError(
            f"invalid gather range: min_ledger={min_ledger} max_ledger={effective_max} "
            f"latest_validated={latest_validated}"
        )

    needed = hot_count + cold_count
    found: dict[str, TxCase] = {}
    sampled_ledgers: set[int] = set()
    attempts = 0

    LOG.info(
        "gathering tx corpus: rpc_url=%s network=%s min_ledger=%s max_ledger=%s needed=%s seed=%s",
        rpc_url,
        network_id,
        min_ledger,
        effective_max,
        needed,
        seed,
    )

    while len(found) < needed and attempts < max_attempts:
        ledger_index = rng.randint(min_ledger, effective_max)
        attempts += 1
        sampled_ledgers.add(ledger_index)

        result = rpc_call(
            rpc_url,
            "ledger",
            [{"ledger_index": ledger_index, "transactions": True}],
        )
        tx_hashes = extract_ledger_tx_hashes(result, ledger_index)
        LOG.info(
            "gather attempt=%s ledger=%s txs=%s unique=%s/%s",
            attempts,
            ledger_index,
            len(tx_hashes),
            len(found),
            needed,
        )
        for tx_hash in tx_hashes:
            if tx_hash in found:
                continue
            ordinal = len(found) + 1
            found[tx_hash] = TxCase(
                tx_hash=tx_hash,
                label=f"ledger-{ledger_index}-tx-{ordinal}",
                ledger_index=ledger_index,
            )
            if len(found) >= needed:
                break

    if len(found) < needed:
        raise RuntimeError(
            f"only gathered {len(found)} unique tx hashes after {attempts} attempts "
            f"(needed {needed})"
        )

    cases = list(found.values())
    hot_set = cases[:hot_count]
    cold_set = cases[hot_count : hot_count + cold_count]
    corpus = {
        "network_id": network_id,
        "generated_at_unix": int(time.time()),
        "rpc_url": rpc_url,
        "latest_validated_ledger": latest_validated,
        "min_ledger": min_ledger,
        "max_ledger": effective_max,
        "sampled_ledgers": sorted(sampled_ledgers),
        "hot_set": [asdict(case) for case in hot_set],
        "cold_set": [asdict(case) for case in cold_set],
        "bad_set": [],
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(corpus, indent=2) + "\n", encoding="utf-8")
    LOG.info(
        "wrote tx corpus: %s hot=%s cold=%s sampled_ledgers=%s",
        output_path,
        len(hot_set),
        len(cold_set),
        len(sampled_ledgers),
    )


@dataclass(frozen=True)
class TestConfig:
    xprv: Path
    tx_hash: str
    tx_corpus_path: Path | None
    bind_address: str
    port: int
    network: int
    rpc: str | None
    peer: str | None
    peer_cache_path: Path | None
    artifact_dir: Path
    session_server_log: Path
    session_runner_log: Path
    index_path: Path
    tests_dir: Path
    startup_timeout: float
    request_timeout: float
    shutdown_timeout: float
    keep_server: bool
    mirror_server_output: bool
    trace_http: bool
    health_burst_count: int
    mixed_burst_count: int
    verify_concurrency: int
    prove_concurrency: int
    soak_seconds: float
    server_threads: int
    no_cache: bool
    server_scope: str
    explicit_peer_cache_path: bool

    @property
    def bind(self) -> str:
        return f"{self.bind_address}:{self.port}"


@dataclass
class HttpResult:
    status: int
    headers: dict[str, str]
    body: bytes


@dataclass
class TestCapture:
    nodeid: str
    slug: str
    file_path: str
    line: int
    test_dir: Path
    server_log_path: Path
    meta_path: Path
    start_offset: int
    started_at: float


@dataclass(frozen=True)
class TxCase:
    tx_hash: str
    label: str
    ledger_index: int | None = None


@dataclass(frozen=True)
class TxCorpus:
    network_id: int | None
    hot_set: tuple[TxCase, ...]
    cold_set: tuple[TxCase, ...]
    bad_set: tuple[TxCase, ...]


class ServerLogPump:
    def __init__(
        self,
        stream,
        destination: Path,
        *,
        mirror: bool = False,
    ) -> None:
        self._stream = stream
        self._destination = destination
        self._mirror = mirror
        self._destination.parent.mkdir(parents=True, exist_ok=True)
        self._file = destination.open("ab")
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._stop = threading.Event()
        self._lock = threading.Lock()

    def start(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        assert self._stream is not None
        try:
            while not self._stop.is_set():
                chunk = self._stream.readline()
                if not chunk:
                    break
                with self._lock:
                    self._file.write(chunk)
                    self._file.flush()
                if self._mirror:
                    sys.stderr.write(chunk.decode("utf-8", errors="replace"))
                    sys.stderr.flush()
        finally:
            with contextlib.suppress(Exception):
                self._file.flush()
            with contextlib.suppress(Exception):
                self._file.close()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        self._thread.join(timeout=timeout)

    def size(self) -> int:
        try:
            return self._destination.stat().st_size
        except FileNotFoundError:
            return 0


class RunningServer:
    def __init__(self, config: TestConfig) -> None:
        self.config = config
        self.proc: subprocess.Popen[bytes] | None = None
        self.health: dict[str, Any] | None = None
        self.log_pump: ServerLogPump | None = None

    def start(self) -> None:
        if self.proc is not None:
            return

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
        if self.config.server_threads > 1:
            cmd += ["--threads", str(self.config.server_threads)]
        if self.config.no_cache:
            cmd += ["--no-cache"]

        LOG.info("starting xprv serve: %s", shlex.join(cmd))
        self.proc = subprocess.Popen(
            cmd,
            cwd=Path.cwd(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=0,
        )
        if self.proc.stdout is None:
            pytest.fail("failed to capture xprv serve stdout")

        self.log_pump = ServerLogPump(
            self.proc.stdout,
            self.config.session_server_log,
            mirror=self.config.mirror_server_output,
        )
        self.log_pump.start()

        LOG.info(
            "spawned xprv serve pid=%s bind=%s artifact_dir=%s fd_count=%s",
            self.proc.pid,
            self.config.bind,
            self.config.artifact_dir,
            self.fd_count(),
        )

        try:
            self._wait_for_port()
            self.health = self._wait_for_health()
            self.log_snapshot("server ready")
        except Exception:
            self._terminate(force=True)
            raise

    def stop(self) -> None:
        if self.config.keep_server:
            self.log_snapshot("server left running")
            print(
                f"[xprv-serve-smoke-v2] leaving server running on {self.config.bind}",
                file=sys.stderr,
            )
            return
        self.log_snapshot("server stopping")
        self._terminate(force=False)

    def _terminate(self, *, force: bool) -> None:
        proc = self.proc
        if proc is None:
            return
        if proc.poll() is not None:
            if self.log_pump is not None:
                self.log_pump.stop()
            return

        if not force:
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=self.config.shutdown_timeout)
                if self.log_pump is not None:
                    self.log_pump.stop()
                return
            except subprocess.TimeoutExpired:
                pass

        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        finally:
            if self.log_pump is not None:
                self.log_pump.stop()

    def log_size(self) -> int:
        if self.log_pump is not None:
            return self.log_pump.size()
        try:
            return self.config.session_server_log.stat().st_size
        except FileNotFoundError:
            return 0

    def slice_logs(self, start: int, end: int, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        with self.config.session_server_log.open("rb") as src:
            src.seek(start)
            data = src.read(max(0, end - start))
        with destination.open("wb") as out:
            out.write(data)

    def tail_logs(self, limit: int = 12000) -> str:
        try:
            data = self.config.session_server_log.read_bytes()
        except FileNotFoundError:
            return ""
        if not data:
            return ""
        text = data.decode("utf-8", errors="replace")
        if len(text) <= limit:
            return text
        return text[-limit:]

    def all_logs(self) -> str:
        try:
            return self.config.session_server_log.read_text(
                encoding="utf-8", errors="replace"
            )
        except FileNotFoundError:
            return ""

    def logs_contain(self, text: str) -> bool:
        return text in self.all_logs()

    def fd_count(self) -> int | None:
        if self.proc is None or self.proc.poll() is not None:
            return None
        return sample_fd_count(self.proc.pid)

    def log_snapshot(self, label: str) -> None:
        proc = self.proc
        LOG.info(
            "%s: pid=%s rc=%s bind=%s fd_count=%s session_server_log=%s",
            label,
            None if proc is None else proc.pid,
            None if proc is None else proc.poll(),
            self.config.bind,
            self.fd_count(),
            self.config.session_server_log,
        )

    def _failure_context(self) -> str:
        proc = self.proc
        status = "not started" if proc is None else f"pid={proc.pid} rc={proc.poll()}"
        return (
            f"server_status={status} fd_count={self.fd_count()} "
            f"session_server_log={self.config.session_server_log}\n"
            f"[xprv output tail]\n{self.tail_logs()}"
        )

    def _wait_for_port(self) -> None:
        deadline = time.monotonic() + self.config.startup_timeout
        while time.monotonic() < deadline:
            if self.proc is not None and self.proc.poll() is not None:
                pytest.fail(
                    "xprv serve exited before opening the listening port\n"
                    + self._failure_context()
                )
            with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
                sock.settimeout(0.5)
                if sock.connect_ex((self.config.bind_address, self.config.port)) == 0:
                    return
            time.sleep(0.1)

        pytest.fail(
            f"xprv serve never opened {self.config.bind} within "
            f"{self.config.startup_timeout:.1f}s\n{self._failure_context()}"
        )

    def _wait_for_health(self) -> dict[str, Any]:
        deadline = time.monotonic() + self.config.startup_timeout
        last_error = "unknown error"
        while time.monotonic() < deadline:
            if self.proc is not None and self.proc.poll() is not None:
                pytest.fail(
                    "xprv serve exited before /health became ready\n"
                    + self._failure_context()
                )
            try:
                result = self.request("GET", "/health", timeout=2.0)
                if result.status == 200:
                    body = decode_json_bytes(result.body, context="/health startup")
                    assert isinstance(body, dict), f"/health returned non-object: {body!r}"
                    return body
                last_error = f"status={result.status} body={result.body[:200]!r}"
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
            time.sleep(0.25)

        pytest.fail(
            f"/health never became ready: {last_error}\n{self._failure_context()}"
        )

    def open_connection(self, *, timeout: float | None = None) -> http.client.HTTPConnection:
        return http.client.HTTPConnection(
            self.config.bind_address,
            self.config.port,
            timeout=timeout or self.config.request_timeout,
        )

    def request(
        self,
        method: str,
        path: str,
        *,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
        connection: http.client.HTTPConnection | None = None,
    ) -> HttpResult:
        close_conn = connection is None
        conn = connection or self.open_connection(timeout=timeout)
        started = time.monotonic()
        try:
            conn.request(method, path, body=body, headers=headers or {})
            resp = conn.getresponse()
            data = resp.read()
            result = HttpResult(
                status=resp.status,
                headers={k.lower(): v for k, v in resp.getheaders()},
                body=data,
            )
            if self.config.trace_http:
                LOG.info(
                    "HTTP %s %s -> %s bytes=%s ms=%.1f fd_count=%s",
                    method,
                    path,
                    result.status,
                    len(data),
                    (time.monotonic() - started) * 1000.0,
                    self.fd_count(),
                )
            return result
        except Exception as exc:  # noqa: BLE001
            raise AssertionError(
                f"{method} {path} failed: {exc}\n{self._failure_context()}"
            ) from exc
        finally:
            if close_conn:
                conn.close()


def assert_error_response(
    result: HttpResult,
    *,
    status: int,
    message_substring: str | None = None,
) -> dict[str, Any]:
    assert result.status == status, (
        f"expected HTTP {status}, got {result.status}, body={result.body[:500]!r}"
    )
    assert_json_content_type(result)
    body = decode_json_bytes(result.body, context=f"error response {status}")
    assert isinstance(body, dict), f"error response was not an object: {body!r}"
    assert "error" in body, f"error response missing error key: {body!r}"
    if message_substring is not None:
        assert message_substring in str(body["error"]), (
            f"expected {message_substring!r} in error {body['error']!r}"
        )
    return body


def assert_no_emfile(server: RunningServer) -> None:
    if server.logs_contain("Too many open files"):
        pytest.fail("server log contains 'Too many open files'\n" + server._failure_context())


def outcome_from_node(node: pytest.Item) -> str:
    rep_setup = getattr(node, "rep_setup", None)
    rep_call = getattr(node, "rep_call", None)
    rep_teardown = getattr(node, "rep_teardown", None)

    if rep_setup is not None and rep_setup.failed:
        return "setup_failed"
    if rep_call is not None:
        if rep_call.failed:
            return "failed"
        if rep_call.skipped:
            return "skipped"
    if rep_teardown is not None and rep_teardown.failed:
        return "teardown_failed"
    return "passed"


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as out:
        out.write(json.dumps(record, sort_keys=True) + "\n")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call):
    outcome = yield
    report = outcome.get_result()
    setattr(item, f"rep_{report.when}", report)


@pytest.fixture(scope="session")
def config(tmp_path_factory: pytest.TempPathFactory) -> TestConfig:
    if not env_bool("XPRV_RUN_E2E"):
        pytest.skip(
            "set XPRV_RUN_E2E=1 or run scripts/xprv-serve-smoke-v2.py directly"
        )

    xprv = Path(os.environ.get("XPRV_BIN", "build/src/xprv/xprv"))
    if not xprv.exists():
        pytest.fail(f"xprv binary not found: {xprv}")

    artifact_env = os.environ.get("XPRV_ARTIFACT_DIR")
    artifact_dir = (
        Path(artifact_env)
        if artifact_env
        else tmp_path_factory.mktemp("xprv-serve-smoke-v2")
    )
    artifact_dir.mkdir(parents=True, exist_ok=True)
    tests_dir = artifact_dir / "tests"
    session_server_log = artifact_dir / "server.log"
    session_runner_log = artifact_dir / "runner.log"
    index_path = artifact_dir / "index.jsonl"
    configure_logging(log_path=session_runner_log)

    bind_address = os.environ.get("XPRV_BIND_ADDRESS", "127.0.0.1")
    port = env_int("XPRV_PORT")
    if port is None:
        port = find_free_port(bind_address)

    peer_cache_env = os.environ.get("XPRV_PEER_CACHE_PATH")
    if peer_cache_env:
        peer_cache_path = Path(peer_cache_env)
    else:
        peer_cache_dir = artifact_dir / "peer-cache"
        peer_cache_dir.mkdir(parents=True, exist_ok=True)
        peer_cache_path = peer_cache_dir / "peer-endpoints.sqlite3"

    tx_corpus_env = os.environ.get("XPRV_TX_CORPUS")
    tx_corpus_path = Path(tx_corpus_env) if tx_corpus_env else None

    cfg = TestConfig(
        xprv=xprv,
        tx_hash=os.environ.get("XPRV_TX_HASH", DEFAULT_TX_HASH),
        tx_corpus_path=tx_corpus_path,
        bind_address=bind_address,
        port=port,
        network=env_int("XPRV_NETWORK") or 0,
        rpc=os.environ.get("XPRV_RPC"),
        peer=os.environ.get("XPRV_PEER"),
        peer_cache_path=peer_cache_path,
        artifact_dir=artifact_dir,
        session_server_log=session_server_log,
        session_runner_log=session_runner_log,
        index_path=index_path,
        tests_dir=tests_dir,
        startup_timeout=env_float("XPRV_STARTUP_TIMEOUT", 20.0),
        request_timeout=env_float("XPRV_REQUEST_TIMEOUT", 120.0),
        shutdown_timeout=env_float("XPRV_SHUTDOWN_TIMEOUT", 10.0),
        keep_server=env_bool("XPRV_KEEP_SERVER"),
        mirror_server_output=env_bool("XPRV_LOG_OUTPUT"),
        trace_http=env_bool("XPRV_TRACE_HTTP"),
        health_burst_count=env_int("XPRV_HEALTH_BURST") or harsh_default(32, 96),
        mixed_burst_count=env_int("XPRV_MIXED_BURST") or harsh_default(24, 64),
        verify_concurrency=env_int("XPRV_VERIFY_CONCURRENCY") or harsh_default(8, 20),
        prove_concurrency=env_int("XPRV_PROVE_CONCURRENCY") or harsh_default(2, 6),
        soak_seconds=env_float("XPRV_SOAK_SECONDS", 0.0),
        server_threads=env_int("XPRV_THREADS") or 1,
        no_cache=env_bool("XPRV_NO_CACHE"),
        server_scope=os.environ.get("XPRV_SERVER_SCOPE", "session"),
        explicit_peer_cache_path=bool(peer_cache_env),
    )
    LOG.info(
        "artifact_dir=%s session_server_log=%s session_runner_log=%s index=%s",
        cfg.artifact_dir,
        cfg.session_server_log,
        cfg.session_runner_log,
        cfg.index_path,
    )
    LOG.info(
        "test config: xprv=%s bind=%s network=%s trace_http=%s tx_corpus=%s "
        "verify_concurrency=%s prove_concurrency=%s health_burst=%s mixed_burst=%s soak=%.1f",
        cfg.xprv,
        cfg.bind,
        cfg.network,
        cfg.trace_http,
        cfg.tx_corpus_path,
        cfg.verify_concurrency,
        cfg.prove_concurrency,
        cfg.health_burst_count,
        cfg.mixed_burst_count,
        cfg.soak_seconds,
    )
    return cfg


@pytest.fixture(scope="session")
def server(config: TestConfig) -> RunningServer:
    handle = RunningServer(config)
    handle.start()
    yield handle
    handle.stop()


@pytest.fixture(autouse=True)
def per_test_capture(
    request: pytest.FixtureRequest,
    config: TestConfig,
    server: RunningServer,
):
    file_path, lineno, _ = request.node.location
    slug = slugify_nodeid(request.node.nodeid)
    test_dir = config.tests_dir / slug
    test_dir.mkdir(parents=True, exist_ok=True)
    capture = TestCapture(
        nodeid=request.node.nodeid,
        slug=slug,
        file_path=file_path,
        line=lineno + 1,
        test_dir=test_dir,
        server_log_path=test_dir / "server.log",
        meta_path=test_dir / "meta.json",
        start_offset=server.log_size(),
        started_at=time.time(),
    )
    setattr(request.node, "_xprv_capture", capture)
    LOG.info(
        "START %s (%s:%s) test_dir=%s start_offset=%s",
        capture.nodeid,
        capture.file_path,
        capture.line,
        capture.test_dir,
        capture.start_offset,
    )
    yield capture

    end_offset = server.log_size()
    duration = time.time() - capture.started_at
    server.slice_logs(capture.start_offset, end_offset, capture.server_log_path)
    outcome = outcome_from_node(request.node)
    meta = {
        "nodeid": capture.nodeid,
        "slug": capture.slug,
        "outcome": outcome,
        "duration_seconds": duration,
        "file": capture.file_path,
        "line": capture.line,
        "test_dir": str(capture.test_dir),
        "server_log": str(capture.server_log_path),
        "session_server_log": str(config.session_server_log),
        "session_runner_log": str(config.session_runner_log),
        "start_offset": capture.start_offset,
        "end_offset": end_offset,
        "fd_count": server.fd_count(),
    }
    capture.meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    append_jsonl(config.index_path, meta)
    LOG.info(
        "END %s outcome=%s duration=%.2fs server_log=%s end_offset=%s",
        capture.nodeid,
        outcome,
        duration,
        capture.server_log_path,
        end_offset,
    )


@pytest.fixture(scope="session")
def health(server: RunningServer) -> dict[str, Any]:
    assert server.health is not None
    return server.health


@pytest.fixture(scope="session")
def tx_corpus(config: TestConfig) -> TxCorpus:
    corpus = load_tx_corpus(config.tx_corpus_path, fallback_tx_hash=config.tx_hash)
    if corpus.network_id is not None and corpus.network_id != config.network:
        pytest.fail(
            f"tx corpus network_id={corpus.network_id} does not match "
            f"configured network={config.network}"
        )
    LOG.info(
        "loaded tx corpus: hot=%s cold=%s bad=%s source=%s",
        len(corpus.hot_set),
        len(corpus.cold_set),
        len(corpus.bad_set),
        config.tx_corpus_path,
    )
    return corpus


@pytest.fixture(scope="session")
def hot_case(tx_corpus: TxCorpus) -> TxCase:
    return tx_corpus.hot_set[0]


@pytest.fixture(scope="session")
def hot_cases(tx_corpus: TxCorpus) -> tuple[TxCase, ...]:
    return tx_corpus.hot_set


@pytest.fixture(scope="session")
def cold_cases(tx_corpus: TxCorpus) -> tuple[TxCase, ...]:
    return tx_corpus.cold_set


@pytest.fixture(scope="session")
def prove_path(hot_case: TxCase) -> str:
    return prove_path_for_case(hot_case)


@pytest.fixture(scope="session")
def proof_json_response(
    server: RunningServer,
    config: TestConfig,
    prove_path: str,
) -> HttpResult:
    result = server.request("GET", prove_path, timeout=config.request_timeout)
    assert result.status == 200, (
        f"/prove JSON expected 200, got {result.status}, body={result.body[:500]!r}"
    )
    assert_json_content_type(result)
    return result


@pytest.fixture(scope="session")
def proof_json(proof_json_response: HttpResult) -> dict[str, Any]:
    body = decode_json_bytes(proof_json_response.body, context="/prove JSON")
    assert isinstance(body, dict), f"/prove JSON returned non-object: {body!r}"
    return body


@pytest.fixture(scope="session")
def proof_bin_response(
    server: RunningServer,
    config: TestConfig,
    prove_path: str,
) -> HttpResult:
    result = server.request(
        "GET",
        prove_path + "&format=bin",
        timeout=config.request_timeout,
    )
    assert result.status == 200, (
        f"/prove bin expected 200, got {result.status}, body={result.body[:100]!r}"
    )
    assert_octet_stream_content_type(result)
    return result


def test_health_shape(health: dict[str, Any]) -> None:
    assert "peer_count" in health, f"/health missing peer_count: {health}"
    assert "vl_loaded" in health, f"/health missing vl_loaded: {health}"
    assert isinstance(health["peer_count"], int)
    assert isinstance(health["vl_loaded"], bool)
    cache = cache_stats_from_health(health)
    assert cache["entries"] >= 0
    assert cache["max_entries"] >= 1
    assert cache["hits"] >= 0
    assert cache["misses"] >= 0
    if "latest_quorum_seq" in health:
        assert isinstance(health["latest_quorum_seq"], int)


def test_keep_alive_supports_multiple_requests(server: RunningServer) -> None:
    conn = server.open_connection(timeout=5.0)
    try:
        first = server.request("GET", "/health", connection=conn)
        second = server.request("GET", "/nope", connection=conn)
        third = server.request("GET", "/health", connection=conn)
    finally:
        conn.close()

    assert first.status == 200, f"first keep-alive request failed: {first.body[:300]!r}"
    assert_error_response(second, status=404, message_substring="not found")
    assert third.status == 200, f"third keep-alive request failed: {third.body[:300]!r}"


@pytest.mark.parametrize(
    ("method", "path", "status", "message"),
    [
        ("GET", "/prove", 400, "missing tx parameter"),
        ("GET", "/prove?tx=", 400, "missing tx parameter"),
        ("POST", "/health", 404, "not found"),
        ("GET", "/verify", 404, "not found"),
        ("GET", "/nope", 404, "not found"),
    ],
)
def test_basic_error_routes(
    server: RunningServer,
    method: str,
    path: str,
    status: int,
    message: str,
) -> None:
    result = server.request(method, path)
    assert_error_response(result, status=status, message_substring=message)


def test_verify_empty_body_returns_400(server: RunningServer) -> None:
    result = server.request(
        "POST",
        "/verify",
        body=b"",
        headers={"Content-Type": "application/octet-stream"},
    )
    assert_error_response(result, status=400, message_substring="empty request body")


def test_verify_oversized_body_returns_413(server: RunningServer) -> None:
    result = server.request(
        "POST",
        "/verify",
        body=b"x" * (MAX_REQUEST_BODY + 1),
        headers={"Content-Type": "application/octet-stream"},
        timeout=30.0,
    )
    assert_error_response(result, status=413, message_substring="request body too large")


def test_prove_json_shape_and_network(
    proof_json: dict[str, Any],
    config: TestConfig,
    hot_case: TxCase,
) -> None:
    assert proof_json["network_id"] == config.network
    assert isinstance(proof_json["steps"], list)
    assert proof_json["steps"], "/prove JSON returned an empty steps array"
    assert isinstance(proof_json["steps"][0], dict)
    assert "type" in proof_json["steps"][0]
    LOG.info(
        "hot proof case label=%s ledger_index=%s tx_hash=%s",
        hot_case.label,
        hot_case.ledger_index,
        hot_case.tx_hash,
    )


def test_prove_unknown_format_defaults_to_json(
    server: RunningServer,
    config: TestConfig,
    prove_path: str,
) -> None:
    result = server.request(
        "GET",
        prove_path + "&format=wat",
        timeout=config.request_timeout,
    )
    assert result.status == 200
    assert_json_content_type(result)
    body = decode_json_bytes(result.body, context="/prove?format=wat")
    assert isinstance(body, dict)
    assert "steps" in body


def test_prove_binary_header_and_network(
    proof_bin_response: HttpResult,
    config: TestConfig,
) -> None:
    assert proof_bin_response.body.startswith(b"XPRV"), "binary proof missing XPRV magic"
    expected_version = int((Path(__file__).parent.parent / "src/xprv/VERSION").read_text().strip())
    assert proof_bin_response.body[4] == expected_version, (
        f"expected v{expected_version} binary header, got version={proof_bin_response.body[4]}"
    )
    assert proof_bin_response.body[5] & 0x01 == 0x01, "expected zlib-compressed body"
    assert binary_network_id(proof_bin_response.body) == config.network


def test_verify_json_round_trip(
    server: RunningServer,
    config: TestConfig,
    proof_json_response: HttpResult,
) -> None:
    result = server.request(
        "POST",
        "/verify",
        body=proof_json_response.body,
        headers={"Content-Type": "application/json"},
        timeout=config.request_timeout,
    )
    assert result.status == 200
    assert_json_content_type(result)
    body = decode_json_bytes(result.body, context="/verify JSON round-trip")
    assert body.get("verified") is True, f"/verify JSON failed: {body}"


def test_verify_legacy_array_json_round_trip(
    server: RunningServer,
    config: TestConfig,
    proof_json: dict[str, Any],
) -> None:
    legacy_json = json.dumps(proof_json["steps"]).encode("utf-8")
    result = server.request(
        "POST",
        "/verify",
        body=legacy_json,
        headers={"Content-Type": "application/json"},
        timeout=config.request_timeout,
    )
    assert result.status == 200
    assert_json_content_type(result)
    body = decode_json_bytes(result.body, context="/verify legacy JSON")
    assert body.get("verified") is True, f"/verify legacy JSON failed: {body}"


def test_verify_binary_round_trip(
    server: RunningServer,
    config: TestConfig,
    proof_bin_response: HttpResult,
) -> None:
    result = server.request(
        "POST",
        "/verify",
        body=proof_bin_response.body,
        headers={"Content-Type": "application/octet-stream"},
        timeout=config.request_timeout,
    )
    assert result.status == 200
    assert_json_content_type(result)
    body = decode_json_bytes(result.body, context="/verify binary round-trip")
    assert body.get("verified") is True, f"/verify binary failed: {body}"


def test_concurrent_verify_requests(
    server: RunningServer,
    config: TestConfig,
    proof_json_response: HttpResult,
    proof_bin_response: HttpResult,
) -> None:
    request_count = config.verify_concurrency

    def do_request(index: int) -> bool:
        if index % 2 == 0:
            payload = proof_json_response.body
            content_type = "application/json"
        else:
            payload = proof_bin_response.body
            content_type = "application/octet-stream"

        result = server.request(
            "POST",
            "/verify",
            body=payload,
            headers={"Content-Type": content_type},
            timeout=config.request_timeout,
        )
        assert result.status == 200, (
            f"concurrent /verify expected 200, got {result.status}, "
            f"body={result.body[:300]!r}"
        )
        assert_json_content_type(result)
        body = decode_json_bytes(result.body, context="concurrent /verify")
        assert body.get("verified") is True, (
            f"concurrent /verify failed for request {index}: {body}"
        )
        return True

    results = run_concurrently(
        request_count,
        max_workers=min(request_count, 12),
        fn=do_request,
    )
    assert all(results)


def test_concurrent_prove_requests(
    server: RunningServer,
    config: TestConfig,
    hot_cases: tuple[TxCase, ...],
) -> None:
    request_count = config.prove_concurrency

    def do_request(index: int) -> tuple[str, int]:
        case = hot_cases[index % len(hot_cases)]
        base_path = prove_path_for_case(case)
        path = base_path if index % 2 == 0 else base_path + "&format=bin"
        result = server.request(
            "GET",
            path,
            timeout=config.request_timeout,
        )
        assert result.status == 200, (
            f"concurrent /prove expected 200, got {result.status}, "
            f"body={result.body[:300]!r}"
        )

        if index % 2 == 0:
            assert_json_content_type(result)
            body = decode_json_bytes(result.body, context="concurrent /prove JSON")
            assert isinstance(body, dict)
            assert body["network_id"] == config.network
            assert isinstance(body["steps"], list) and body["steps"]
            return ("json", len(body["steps"]))

        assert_octet_stream_content_type(result)
        assert result.body.startswith(b"XPRV"), "concurrent binary proof missing magic"
        assert binary_network_id(result.body) == config.network
        return ("bin", len(result.body))

    results = run_concurrently(
        request_count,
        max_workers=min(request_count, 8),
        fn=do_request,
    )
    assert len(results) == request_count
    assert any(kind == "json" for kind, _ in results)
    assert any(kind == "bin" for kind, _ in results)


def test_cache_hits_increase_for_repeated_hot_tx(
    server: RunningServer,
    config: TestConfig,
    hot_case: TxCase,
) -> None:
    prove_path = prove_path_for_case(hot_case)

    warm = server.request("GET", prove_path, timeout=config.request_timeout)
    assert warm.status == 200

    mid_health = decode_json_bytes(
        server.request("GET", "/health", timeout=5.0).body,
        context="cache mid health",
    )
    mid_cache = cache_stats_from_health(mid_health)

    repeat_count = 3
    for _ in range(repeat_count):
        result = server.request("GET", prove_path, timeout=config.request_timeout)
        assert result.status == 200

    after_health = decode_json_bytes(
        server.request("GET", "/health", timeout=5.0).body,
        context="cache after health",
    )
    after_cache = cache_stats_from_health(after_health)

    assert after_cache["hits"] >= mid_cache["hits"] + repeat_count, (
        f"expected at least {repeat_count} cache hits for repeated hot tx "
        f"{hot_case.tx_hash}, before={mid_cache}, after={after_cache}"
    )
    assert after_cache["entries"] <= after_cache["max_entries"]


def test_cold_set_misses_then_hits(
    server: RunningServer,
    config: TestConfig,
    cold_cases: tuple[TxCase, ...],
) -> None:
    if not cold_cases:
        pytest.skip("tx corpus has no cold_set entries")

    selected = cold_cases[: min(4, len(cold_cases))]
    before_health = decode_json_bytes(
        server.request("GET", "/health", timeout=5.0).body,
        context="cold before health",
    )
    before_cache = cache_stats_from_health(before_health)

    for case in selected:
        result = server.request(
            "GET",
            prove_path_for_case(case),
            timeout=config.request_timeout,
        )
        assert result.status == 200, f"cold case prove failed for {case}"

    first_health = decode_json_bytes(
        server.request("GET", "/health", timeout=5.0).body,
        context="cold first health",
    )
    first_cache = cache_stats_from_health(first_health)

    for case in selected:
        result = server.request(
            "GET",
            prove_path_for_case(case),
            timeout=config.request_timeout,
        )
        assert result.status == 200, f"cold case re-prove failed for {case}"

    second_health = decode_json_bytes(
        server.request("GET", "/health", timeout=5.0).body,
        context="cold second health",
    )
    second_cache = cache_stats_from_health(second_health)

    assert first_cache["misses"] >= before_cache["misses"] + len(selected), (
        f"expected cold set first pass to miss at least {len(selected)} times; "
        f"before={before_cache}, first={first_cache}"
    )
    assert second_cache["hits"] >= first_cache["hits"] + len(selected), (
        f"expected cold set second pass to hit at least {len(selected)} times; "
        f"first={first_cache}, second={second_cache}"
    )


def test_health_burst_survives_without_emfile(
    server: RunningServer,
    config: TestConfig,
) -> None:
    request_count = config.health_burst_count
    server.log_snapshot("before health burst")

    def do_request(index: int) -> bool:
        result = server.request(
            "GET",
            "/health",
            timeout=min(config.request_timeout, 10.0),
        )
        assert result.status == 200, (
            f"health burst request {index} failed: {result.status} "
            f"body={result.body[:300]!r}"
        )
        assert_json_content_type(result)
        body = decode_json_bytes(result.body, context=f"health burst {index}")
        assert isinstance(body, dict)
        assert "peer_count" in body
        return True

    results = run_concurrently(
        request_count,
        max_workers=min(request_count, 32),
        fn=do_request,
    )
    assert all(results)
    server.log_snapshot("after health burst")
    assert_no_emfile(server)


def test_mixed_burst_survives_without_emfile(
    server: RunningServer,
    config: TestConfig,
    proof_json_response: HttpResult,
    proof_bin_response: HttpResult,
    hot_cases: tuple[TxCase, ...],
) -> None:
    request_count = config.mixed_burst_count
    server.log_snapshot("before mixed burst")

    def do_request(index: int) -> str:
        mode = index % 6
        case = hot_cases[index % len(hot_cases)]
        prove_path = prove_path_for_case(case)
        if mode == 0:
            result = server.request("GET", "/health", timeout=min(config.request_timeout, 10.0))
            assert result.status == 200
            return "health"
        if mode == 1:
            result = server.request("GET", "/nope", timeout=min(config.request_timeout, 10.0))
            assert_error_response(result, status=404, message_substring="not found")
            return "404"
        if mode == 2:
            result = server.request(
                "POST",
                "/verify",
                body=proof_json_response.body,
                headers={"Content-Type": "application/json"},
                timeout=config.request_timeout,
            )
            body = decode_json_bytes(result.body, context="mixed burst verify json")
            assert result.status == 200 and body.get("verified") is True
            return "verify-json"
        if mode == 3:
            result = server.request(
                "POST",
                "/verify",
                body=proof_bin_response.body,
                headers={"Content-Type": "application/octet-stream"},
                timeout=config.request_timeout,
            )
            body = decode_json_bytes(result.body, context="mixed burst verify bin")
            assert result.status == 200 and body.get("verified") is True
            return "verify-bin"
        if mode == 4:
            result = server.request(
                "GET",
                prove_path,
                timeout=config.request_timeout,
            )
            assert result.status == 200
            assert_json_content_type(result)
            body = decode_json_bytes(result.body, context="mixed burst prove json")
            assert body["network_id"] == config.network
            return "prove-json"

        result = server.request(
            "GET",
            prove_path + "&format=bin",
            timeout=config.request_timeout,
        )
        assert result.status == 200
        assert_octet_stream_content_type(result)
        assert binary_network_id(result.body) == config.network
        return "prove-bin"

    results = run_concurrently(
        request_count,
        max_workers=min(request_count, 24),
        fn=do_request,
    )
    assert len(results) == request_count
    server.log_snapshot("after mixed burst")
    assert_no_emfile(server)


def test_optional_soak_loop(
    server: RunningServer,
    config: TestConfig,
    proof_json_response: HttpResult,
    proof_bin_response: HttpResult,
) -> None:
    if config.soak_seconds <= 0:
        pytest.skip("set XPRV_SOAK_SECONDS > 0 to enable soak mode")

    deadline = time.monotonic() + config.soak_seconds
    iterations = 0
    last_snapshot = 0.0
    server.log_snapshot("before soak loop")

    while time.monotonic() < deadline:
        health = server.request("GET", "/health", timeout=min(config.request_timeout, 10.0))
        assert health.status == 200

        json_verify = server.request(
            "POST",
            "/verify",
            body=proof_json_response.body,
            headers={"Content-Type": "application/json"},
            timeout=config.request_timeout,
        )
        json_body = decode_json_bytes(json_verify.body, context="soak verify json")
        assert json_verify.status == 200 and json_body.get("verified") is True

        bin_verify = server.request(
            "POST",
            "/verify",
            body=proof_bin_response.body,
            headers={"Content-Type": "application/octet-stream"},
            timeout=config.request_timeout,
        )
        bin_body = decode_json_bytes(bin_verify.body, context="soak verify bin")
        assert bin_verify.status == 200 and bin_body.get("verified") is True

        iterations += 1
        now = time.monotonic()
        if now - last_snapshot >= 5.0:
            server.log_snapshot(f"soak loop iteration={iterations}")
            last_snapshot = now

    LOG.info("completed soak loop iterations=%s", iterations)
    server.log_snapshot("after soak loop")
    assert_no_emfile(server)


def test_verify_malformed_json_reports_failure(server: RunningServer) -> None:
    result = server.request(
        "POST",
        "/verify",
        body=b'{"steps":',
        headers={"Content-Type": "application/json"},
    )
    assert result.status == 200
    assert_json_content_type(result)
    body = decode_json_bytes(result.body, context="/verify malformed JSON")
    assert body.get("verified") is False, f"malformed JSON unexpectedly verified: {body}"
    assert "error" in body


def test_verify_bad_binary_reports_failure(server: RunningServer) -> None:
    result = server.request(
        "POST",
        "/verify",
        body=b"XPRV\xff\x00",  # invalid version
        headers={"Content-Type": "application/octet-stream"},
    )
    assert result.status == 200
    assert_json_content_type(result)
    body = decode_json_bytes(result.body, context="/verify bad binary")
    assert body.get("verified") is False, f"bad binary unexpectedly verified: {body}"
    assert "error" in body


def build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--xprv",
        help="Path to the xprv binary (maps to XPRV_BIN)",
    )
    parser.add_argument(
        "--tx-hash",
        help="Transaction hash to prove (maps to XPRV_TX_HASH)",
    )
    parser.add_argument(
        "--bind-address",
        help="Address to bind the test server to (maps to XPRV_BIND_ADDRESS)",
    )
    parser.add_argument(
        "--port",
        type=int,
        help="Port to bind the test server to (maps to XPRV_PORT)",
    )
    parser.add_argument(
        "--network",
        type=int,
        help="Network ID for the server (maps to XPRV_NETWORK)",
    )
    parser.add_argument("--rpc", help="Override RPC endpoint host:port")
    parser.add_argument("--peer", help="Override peer endpoint host:port")
    parser.add_argument("--peer-cache-path", help="Override peer cache path")
    parser.add_argument("--tx-corpus", help="Path to a tx corpus JSON file to consume")
    parser.add_argument("--artifact-dir", help="Artifact directory for runner.log/server.log/test slices")
    parser.add_argument(
        "--gather-corpus",
        help="Write a tx corpus JSON file and exit instead of running pytest",
    )
    parser.add_argument(
        "--gather-hot-count",
        type=int,
        default=4,
        help="Number of hot-set transactions to gather",
    )
    parser.add_argument(
        "--gather-cold-count",
        type=int,
        default=8,
        help="Number of cold-set transactions to gather",
    )
    parser.add_argument(
        "--gather-min-ledger",
        type=int,
        default=500000,
        help="Minimum ledger index for corpus gathering",
    )
    parser.add_argument(
        "--gather-max-ledger",
        type=int,
        help="Maximum ledger index for corpus gathering",
    )
    parser.add_argument(
        "--gather-attempts",
        type=int,
        default=64,
        help="Maximum random ledger attempts when gathering a corpus",
    )
    parser.add_argument(
        "--gather-seed",
        type=int,
        help="Optional RNG seed for reproducible corpus gathering",
    )
    parser.add_argument(
        "--startup-timeout",
        type=float,
        help="Seconds to wait for /health to come up",
    )
    parser.add_argument(
        "--request-timeout",
        type=float,
        help="Seconds to allow for prove/verify requests",
    )
    parser.add_argument(
        "--shutdown-timeout",
        type=float,
        help="Seconds to wait for xprv serve to exit after SIGINT",
    )
    parser.add_argument(
        "--keep-server",
        action="store_true",
        help="Leave xprv serve running after the test session",
    )
    parser.add_argument(
        "--log-output",
        action="store_true",
        help="Mirror server stdout/stderr to the console while still capturing it",
    )
    parser.add_argument(
        "--trace-http",
        action="store_true",
        help="Log every test-side HTTP request/response",
    )
    parser.add_argument(
        "--log-level",
        help="Python-side log level (DEBUG, INFO, WARNING, ERROR)",
    )
    parser.add_argument(
        "--health-burst",
        type=int,
        help="Concurrent request count for the /health burst test",
    )
    parser.add_argument(
        "--mixed-burst",
        type=int,
        help="Concurrent request count for the mixed burst test",
    )
    parser.add_argument(
        "--verify-concurrency",
        type=int,
        help="Concurrent request count for the /verify concurrency test",
    )
    parser.add_argument(
        "--prove-concurrency",
        type=int,
        help="Concurrent request count for the /prove concurrency test",
    )
    parser.add_argument(
        "--soak-seconds",
        type=float,
        help="Optional soak duration in seconds",
    )
    parser.add_argument(
        "--harsh-winds",
        action="store_true",
        help="Increase default burst sizes for harsher concurrency testing",
    )
    parser.add_argument(
        "--threads",
        type=int,
        help="Number of server I/O threads (maps to XPRV_THREADS)",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable proof cache on the server (maps to XPRV_NO_CACHE)",
    )
    return parser


def set_env_if(name: str, value: str | int | float | None) -> None:
    if value is None:
        return
    os.environ[name] = str(value)


def main(argv: list[str] | None = None) -> int:
    parser = build_cli_parser()
    args, pytest_args = parser.parse_known_args(argv)

    configure_logging(args.log_level)
    os.environ["XPRV_RUN_E2E"] = "1"
    set_env_if("XPRV_BIN", args.xprv)
    set_env_if("XPRV_TX_HASH", args.tx_hash)
    set_env_if("XPRV_BIND_ADDRESS", args.bind_address)
    set_env_if("XPRV_PORT", args.port)
    set_env_if("XPRV_NETWORK", args.network)
    set_env_if("XPRV_RPC", args.rpc)
    set_env_if("XPRV_PEER", args.peer)
    set_env_if("XPRV_PEER_CACHE_PATH", args.peer_cache_path)
    set_env_if("XPRV_TX_CORPUS", args.tx_corpus)
    set_env_if("XPRV_ARTIFACT_DIR", args.artifact_dir)
    set_env_if("XPRV_STARTUP_TIMEOUT", args.startup_timeout)
    set_env_if("XPRV_REQUEST_TIMEOUT", args.request_timeout)
    set_env_if("XPRV_SHUTDOWN_TIMEOUT", args.shutdown_timeout)
    set_env_if("XPRV_TEST_LOG_LEVEL", args.log_level)
    set_env_if("XPRV_HEALTH_BURST", args.health_burst)
    set_env_if("XPRV_MIXED_BURST", args.mixed_burst)
    set_env_if("XPRV_VERIFY_CONCURRENCY", args.verify_concurrency)
    set_env_if("XPRV_PROVE_CONCURRENCY", args.prove_concurrency)
    set_env_if("XPRV_SOAK_SECONDS", args.soak_seconds)
    set_env_if("XPRV_THREADS", args.threads)
    if args.no_cache:
        set_env_if("XPRV_NO_CACHE", "1")
    if args.keep_server:
        os.environ["XPRV_KEEP_SERVER"] = "1"
    if args.log_output:
        os.environ["XPRV_LOG_OUTPUT"] = "1"
    if args.trace_http:
        os.environ["XPRV_TRACE_HTTP"] = "1"
    if args.harsh_winds:
        os.environ["XPRV_HARSH_WINDS"] = "1"

    if args.gather_corpus:
        output_path = Path(args.gather_corpus)
        gather_corpus_file(
            output_path,
            rpc_endpoint=args.rpc,
            network_id=args.network or 0,
            hot_count=args.gather_hot_count,
            cold_count=args.gather_cold_count,
            min_ledger=args.gather_min_ledger,
            max_ledger=args.gather_max_ledger,
            max_attempts=args.gather_attempts,
            seed=args.gather_seed,
        )
        return 0

    LOG.info(
        "invoking pytest args=%s harsh_winds=%s trace_http=%s artifact_dir=%s tx_corpus=%s",
        pytest_args,
        args.harsh_winds,
        args.trace_http,
        args.artifact_dir,
        args.tx_corpus,
    )

    return pytest.main([__file__, *pytest_args])


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
