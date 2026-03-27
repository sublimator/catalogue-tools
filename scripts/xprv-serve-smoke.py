#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.14"
# dependencies = ["pytest>=8.3,<9"]
# ///
"""
Pytest integration suite for `xprv serve`.

This file is both:
- an executable UV script that calls `pytest.main()` on itself
- a real pytest module with fixtures and parametrized integration tests

Run directly:
    scripts/xprv-serve-smoke.py -q
    scripts/xprv-serve-smoke.py --xprv build/src/xprv/xprv -k verify

Useful environment variables:
    XPRV_RUN_E2E=1
    XPRV_BIN=build/src/xprv/xprv
    XPRV_TX_HASH=8AF2DE8804721B0EC9E11FD66B9EF3C30962E71DC0D707CA1F4CDE4655751420
    XPRV_NETWORK=0
    XPRV_RPC=host:port
    XPRV_PEER=host:port
"""

from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
import http.client
import json
import logging
import os
import signal
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest


DEFAULT_TX_HASH = (
    "8AF2DE8804721B0EC9E11FD66B9EF3C30962E71DC0D707CA1F4CDE4655751420"
)
MAX_REQUEST_BODY = 512 * 1024
LOG_FORMAT = "[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s"
LOG = logging.getLogger("xprv-serve-smoke")


def configure_logging(level_name: str | None = None) -> None:
    level_text = (level_name or os.environ.get("XPRV_TEST_LOG_LEVEL", "INFO")).upper()
    level = getattr(logging, level_text, logging.INFO)
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(level=level, format=LOG_FORMAT, datefmt="%H:%M:%S")
    root.setLevel(level)
    LOG.setLevel(level)


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


def find_free_port(host: str) -> int:
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.bind((host, 0))
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return int(sock.getsockname()[1])


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

    lines = result.stdout.splitlines()
    return max(0, len(lines) - 1)


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


@dataclass(frozen=True)
class TestConfig:
    xprv: Path
    tx_hash: str
    bind_address: str
    port: int
    network: int
    rpc: str | None
    peer: str | None
    peer_cache_path: Path | None
    startup_timeout: float
    request_timeout: float
    shutdown_timeout: float
    keep_server: bool
    log_output: bool
    trace_http: bool
    health_burst_count: int
    mixed_burst_count: int
    soak_seconds: float

    @property
    def bind(self) -> str:
        return f"{self.bind_address}:{self.port}"


@dataclass
class HttpResult:
    status: int
    headers: dict[str, str]
    body: bytes


class RunningServer:
    def __init__(self, config: TestConfig) -> None:
        self.config = config
        self.proc: subprocess.Popen[bytes] | None = None
        self.log_file: tempfile.TemporaryFile[bytes] | None = None
        self.health: dict[str, Any] | None = None

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

        stdout = None
        stderr = None
        if not self.config.log_output:
            self.log_file = tempfile.TemporaryFile()
            stdout = self.log_file
            stderr = self.log_file

        LOG.info("starting xprv serve: %s", shlex.join(cmd))
        self.proc = subprocess.Popen(
            cmd,
            cwd=Path.cwd(),
            stdout=stdout,
            stderr=stderr,
        )
        LOG.info(
            "spawned xprv serve pid=%s bind=%s fd_count=%s",
            self.proc.pid,
            self.config.bind,
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
                f"[xprv-serve-smoke] leaving server running on {self.config.bind}",
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
            return

        if not force:
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=self.config.shutdown_timeout)
                return
            except subprocess.TimeoutExpired:
                pass

        proc.terminate()
        try:
            proc.wait(timeout=5)
            return
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)

    def tail_logs(self, limit: int = 8000) -> str:
        if self.log_file is None:
            return ""
        self.log_file.flush()
        self.log_file.seek(0)
        data = self.log_file.read()
        if not data:
            return ""
        text = data.decode("utf-8", errors="replace")
        if len(text) <= limit:
            return text
        return text[-limit:]

    def all_logs(self) -> str:
        if self.log_file is None:
            return ""
        self.log_file.flush()
        self.log_file.seek(0)
        data = self.log_file.read()
        if not data:
            return ""
        return data.decode("utf-8", errors="replace")

    def logs_contain(self, text: str) -> bool:
        if self.log_file is None:
            return False
        return text in self.all_logs()

    def fd_count(self) -> int | None:
        if self.proc is None or self.proc.poll() is not None:
            return None
        return sample_fd_count(self.proc.pid)

    def log_snapshot(self, label: str) -> None:
        proc = self.proc
        LOG.info(
            "%s: pid=%s rc=%s bind=%s fd_count=%s",
            label,
            None if proc is None else proc.pid,
            None if proc is None else proc.poll(),
            self.config.bind,
            self.fd_count(),
        )

    def _failure_context(self) -> str:
        proc = self.proc
        status = "not started" if proc is None else f"pid={proc.pid} rc={proc.poll()}"
        fd_count = self.fd_count()
        logs = self.tail_logs()
        if not logs:
            return f"server_status={status} fd_count={fd_count}"
        return f"server_status={status} fd_count={fd_count}\n[xprv output]\n{logs}"

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
                elapsed_ms = (time.monotonic() - started) * 1000.0
                LOG.info(
                    "HTTP %s %s -> %s bytes=%s ms=%.1f fd_count=%s",
                    method,
                    path,
                    result.status,
                    len(data),
                    elapsed_ms,
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


@pytest.fixture(scope="session")
def config(tmp_path_factory: pytest.TempPathFactory) -> TestConfig:
    if not env_bool("XPRV_RUN_E2E"):
        pytest.skip(
            "set XPRV_RUN_E2E=1 or run scripts/xprv-serve-smoke.py directly"
        )

    xprv = Path(os.environ.get("XPRV_BIN", "build/src/xprv/xprv"))
    if not xprv.exists():
        pytest.fail(f"xprv binary not found: {xprv}")

    bind_address = os.environ.get("XPRV_BIND_ADDRESS", "127.0.0.1")
    port = env_int("XPRV_PORT")
    if port is None:
        port = find_free_port(bind_address)

    peer_cache_env = os.environ.get("XPRV_PEER_CACHE_PATH")
    if peer_cache_env:
        peer_cache_path = Path(peer_cache_env)
    else:
        peer_cache_dir = tmp_path_factory.mktemp("xprv-serve-peer-cache")
        peer_cache_path = peer_cache_dir / "peer-endpoints.sqlite3"

    cfg = TestConfig(
        xprv=xprv,
        tx_hash=os.environ.get("XPRV_TX_HASH", DEFAULT_TX_HASH),
        bind_address=bind_address,
        port=port,
        network=env_int("XPRV_NETWORK") or 0,
        rpc=os.environ.get("XPRV_RPC"),
        peer=os.environ.get("XPRV_PEER"),
        peer_cache_path=peer_cache_path,
        startup_timeout=env_float("XPRV_STARTUP_TIMEOUT", 20.0),
        request_timeout=env_float("XPRV_REQUEST_TIMEOUT", 120.0),
        shutdown_timeout=env_float("XPRV_SHUTDOWN_TIMEOUT", 10.0),
        keep_server=env_bool("XPRV_KEEP_SERVER"),
        log_output=env_bool("XPRV_LOG_OUTPUT"),
        trace_http=env_bool("XPRV_TRACE_HTTP"),
        health_burst_count=env_int("XPRV_HEALTH_BURST") or harsh_default(32, 96),
        mixed_burst_count=env_int("XPRV_MIXED_BURST") or harsh_default(24, 64),
        soak_seconds=env_float("XPRV_SOAK_SECONDS", 0.0),
    )
    LOG.info(
        "test config: xprv=%s bind=%s network=%s trace_http=%s "
        "health_burst=%s mixed_burst=%s soak_seconds=%.1f",
        cfg.xprv,
        cfg.bind,
        cfg.network,
        cfg.trace_http,
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
def log_test_case(request: pytest.FixtureRequest) -> None:
    file_path, lineno, _ = request.node.location
    started = time.monotonic()
    LOG.info(
        "START %s (%s:%s)",
        request.node.nodeid,
        file_path,
        lineno + 1,
    )
    yield
    LOG.info(
        "END %s duration=%.2fs",
        request.node.nodeid,
        time.monotonic() - started,
    )


@pytest.fixture(scope="session")
def health(server: RunningServer) -> dict[str, Any]:
    assert server.health is not None
    return server.health


@pytest.fixture(scope="session")
def prove_path(config: TestConfig) -> str:
    encoded_tx = urllib.parse.quote(config.tx_hash, safe="")
    return f"/prove?tx={encoded_tx}"


@pytest.fixture(scope="session")
def proof_json_response(server: RunningServer, config: TestConfig, prove_path: str) -> HttpResult:
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
def proof_bin_response(server: RunningServer, config: TestConfig, prove_path: str) -> HttpResult:
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
        pytest.fail(
            "server log contains 'Too many open files'\n" + server._failure_context()
        )


def test_health_shape(health: dict[str, Any]) -> None:
    assert "peer_count" in health, f"/health missing peer_count: {health}"
    assert "vl_loaded" in health, f"/health missing vl_loaded: {health}"
    assert isinstance(health["peer_count"], int)
    assert isinstance(health["vl_loaded"], bool)
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
) -> None:
    assert proof_json["network_id"] == config.network
    assert isinstance(proof_json["steps"], list)
    assert proof_json["steps"], "/prove JSON returned an empty steps array"
    assert isinstance(proof_json["steps"][0], dict)
    assert "type" in proof_json["steps"][0]


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
    assert proof_bin_response.body[4] == 0x02, (
        f"expected v2 binary header, got version={proof_bin_response.body[4]}"
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
    request_count = env_int("XPRV_VERIFY_CONCURRENCY") or 8

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
        max_workers=min(request_count, 8),
        fn=do_request,
    )
    assert all(results)


def test_concurrent_prove_requests(
    server: RunningServer,
    config: TestConfig,
    prove_path: str,
) -> None:
    request_count = env_int("XPRV_PROVE_CONCURRENCY") or 2

    def do_request(index: int) -> tuple[str, int]:
        path = prove_path if index % 2 == 0 else prove_path + "&format=bin"
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
        max_workers=min(request_count, 4),
        fn=do_request,
    )
    assert len(results) == request_count
    assert any(kind == "json" for kind, _ in results)
    assert any(kind == "bin" for kind, _ in results)


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
    prove_path: str,
) -> None:
    request_count = config.mixed_burst_count
    server.log_snapshot("before mixed burst")

    def do_request(index: int) -> str:
        mode = index % 6
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
        body=b"XPRV\x03\x00",
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
        help="Inherit server stdout/stderr instead of capturing it",
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
        "--soak-seconds",
        type=float,
        help="Optional soak duration in seconds",
    )
    parser.add_argument(
        "--harsh-winds",
        action="store_true",
        help="Increase default burst sizes for harsher concurrency testing",
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
    set_env_if("XPRV_STARTUP_TIMEOUT", args.startup_timeout)
    set_env_if("XPRV_REQUEST_TIMEOUT", args.request_timeout)
    set_env_if("XPRV_SHUTDOWN_TIMEOUT", args.shutdown_timeout)
    set_env_if("XPRV_TEST_LOG_LEVEL", args.log_level)
    set_env_if("XPRV_HEALTH_BURST", args.health_burst)
    set_env_if("XPRV_MIXED_BURST", args.mixed_burst)
    set_env_if("XPRV_SOAK_SECONDS", args.soak_seconds)
    if args.keep_server:
        os.environ["XPRV_KEEP_SERVER"] = "1"
    if args.log_output:
        os.environ["XPRV_LOG_OUTPUT"] = "1"
    if args.trace_http:
        os.environ["XPRV_TRACE_HTTP"] = "1"
    if args.harsh_winds:
        os.environ["XPRV_HARSH_WINDS"] = "1"

    LOG.info(
        "invoking pytest args=%s harsh_winds=%s trace_http=%s",
        pytest_args,
        args.harsh_winds,
        args.trace_http,
    )

    return pytest.main([__file__, *pytest_args])


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
