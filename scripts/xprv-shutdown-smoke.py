#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# ///
"""Measure xprv shutdown at explicit live process boundaries.

This is intentionally separate from the broad pytest serve suite.  It owns one
server per row, establishes the requested boundary before signalling, writes
the row immediately to JSONL, and treats forced termination as failure rather
than latency evidence.
"""

from __future__ import annotations

import argparse
import contextlib
import http.client
import json
import math
import os
import re
import signal
import socket
import subprocess
import tempfile
import time
import urllib.parse
import urllib.request
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

ENGINE_FENCE = re.compile(r"\[engine\]\s+\[([^]]+)\]\s+Engine draining\.\.\.")
HEARTBEAT_EFFECT = re.compile(r"\[quorum-tracker\]\s+\[([^]]+)\]\s+no quorum for ")
SERVICE_FORCED = "Service shutdown forced before all drains completed"
STARTUP_FORBIDDEN = ("Peer pool", "Resolved RPC", "Engine started")


@dataclass
class RunRow:
    case: str
    threads: int
    run: int
    runs: int
    build_id: str
    networks: list[str]
    signal_to_exit_ms: float | None
    graceful: bool
    forced: bool
    graceful_budget_seconds: float
    boundary_witness: dict[str, Any]
    shutdown_events: list[dict[str, Any]]
    log_path: str
    exit_code: int | None
    network_fences: dict[str, dict[str, Any]]
    status: str
    error: str | None = None


@dataclass
class RunningProcess:
    proc: subprocess.Popen[bytes]
    log_file: Any
    log_path: Path
    base_url: str

    def close_log(self) -> None:
        with contextlib.suppress(Exception):
            self.log_file.flush()
        with contextlib.suppress(Exception):
            self.log_file.close()


class BoundaryUnavailable(RuntimeError):
    pass


def find_free_port(host: str) -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


def get_health(base_url: str, timeout: float = 2.0) -> dict[str, Any]:
    with urllib.request.urlopen(f"{base_url}/health", timeout=timeout) as response:
        payload = json.load(response)
    if not isinstance(payload, dict):
        raise TypeError("/health did not return an object")
    return payload


def wait_for_health(
    running: RunningProcess,
    timeout: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_error: BaseException | None = None
    while time.monotonic() < deadline:
        if running.proc.poll() is not None:
            raise RuntimeError(
                f"xprv exited during startup with {running.proc.returncode}; "
                f"see {running.log_path}"
            )
        try:
            return get_health(running.base_url)
        except (OSError, ValueError) as exc:
            last_error = exc
            time.sleep(0.05)
    raise RuntimeError(f"/health did not become ready: {last_error}")


def enabled_labels(health: dict[str, Any]) -> list[str]:
    networks = health.get("networks")
    if not isinstance(networks, dict) or not networks:
        raise RuntimeError("/health has no non-empty networks object")
    return sorted(str(label) for label in networks)


def network_log_label(network_id: int) -> str:
    if network_id == 0:
        return "xrpl"
    if network_id == 21337:
        return "xahau"
    return f"net-{network_id}"


def network_health_slug(network_id: int) -> str:
    if network_id == 0:
        return "xrpl-mainnet"
    if network_id == 21337:
        return "xahau-mainnet"
    return str(network_id)


def enabled_log_labels(health: dict[str, Any]) -> list[str]:
    networks = health.get("networks")
    if not isinstance(networks, dict) or not networks:
        raise RuntimeError("/health has no non-empty networks object")
    labels = []
    for slug, item in networks.items():
        if not isinstance(item, dict) or not isinstance(item.get("network_id"), int):
            raise TypeError(f"/health network {slug!r} has no integer network_id")
        labels.append(network_log_label(item["network_id"]))
    if len(set(labels)) != len(labels):
        raise RuntimeError(f"network log labels are not unique: {labels}")
    return sorted(labels)


def active_proofs(health: dict[str, Any]) -> int:
    requests = health.get("requests")
    if not isinstance(requests, dict):
        raise TypeError("/health has no requests object")
    active = requests.get("active_proofs")
    if not isinstance(active, int):
        raise TypeError("/health requests.active_proofs is not an integer")
    return active


def start_server(
    *,
    xprv: Path,
    host: str,
    port: int,
    primary_network: int,
    network_ids: list[int],
    threads: int,
    run_dir: Path,
    trace_shutdown: bool,
) -> RunningProcess:
    run_dir.mkdir(parents=True, exist_ok=True)
    log_path = run_dir / "server.log"
    log_file = log_path.open("wb")
    peer_cache = run_dir / "peer-endpoints.sqlite3"
    command = [str(xprv)]
    if trace_shutdown:
        command.extend(
            [
                "--log",
                "engine=DEBUG",
                "--log",
                "vl-cache=DEBUG",
                "--log",
                "vl-client=DEBUG",
                "--log",
                "peer-set=DEBUG",
                "--log",
                "peer-crawl=DEBUG",
            ]
        )
    command.extend(
        [
            "serve",
            "--bind",
            f"{host}:{port}",
            "--network",
            str(primary_network),
            "--threads",
            str(threads),
            "--peer-cache-path",
            str(peer_cache),
        ]
    )
    env = os.environ.copy()
    env["XPRV_NETWORKS"] = ",".join(str(item) for item in network_ids)
    try:
        proc = subprocess.Popen(
            command,
            cwd=Path.cwd(),
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
    except Exception:
        log_file.close()
        raise
    return RunningProcess(
        proc=proc,
        log_file=log_file,
        log_path=log_path,
        base_url=f"http://{host}:{port}",
    )


def establish_keepalive(
    host: str, port: int
) -> tuple[http.client.HTTPConnection, dict[str, Any]]:
    connection = http.client.HTTPConnection(host, port, timeout=3.0)
    try:
        connection.request("GET", "/health", headers={"Connection": "keep-alive"})
        response = connection.getresponse()
        body = response.read()
        if response.status != 200:
            raise RuntimeError(f"keep-alive /health returned {response.status}")
        if response.will_close:
            raise RuntimeError("server did not admit a persistent HTTP session")
    except Exception:
        connection.close()
        raise
    return connection, {
        "status": response.status,
        "connection_will_close": response.will_close,
        "body_bytes": len(body),
    }


def establish_active_proof(
    *,
    host: str,
    port: int,
    base_url: str,
    network: str,
    tx_hash: str,
    timeout: float,
) -> tuple[http.client.HTTPConnection, http.client.HTTPResponse, dict[str, Any]]:
    route = (
        f"/network/{urllib.parse.quote(network, safe='')}/prove?"
        + urllib.parse.urlencode({"tx": tx_hash, "xrid": "shutdown-smoke"})
    )
    connection = http.client.HTTPConnection(host, port, timeout=timeout)
    response: http.client.HTTPResponse | None = None
    try:
        connection.request(
            "GET",
            route,
            headers={"Accept": "text/event-stream", "Connection": "close"},
        )
        response = connection.getresponse()
        if response.status != 200:
            raise BoundaryUnavailable(f"proof response status was {response.status}")
        content_type = response.getheader("content-type", "")
        if "text/event-stream" not in content_type:
            raise BoundaryUnavailable(f"proof content-type was {content_type!r}")

        event = ""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            line = response.readline()
            if not line:
                break
            text = line.decode("utf-8", errors="replace").strip()
            if text.startswith("data:"):
                event = text
                break
        if not event:
            raise BoundaryUnavailable("proof stream produced no SSE data event")

        health = get_health(base_url)
        active = active_proofs(health)
        if active <= 0:
            raise BoundaryUnavailable(
                "proof emitted an event but was no longer active at signal boundary"
            )
    except Exception:
        if response is not None:
            response.close()
        connection.close()
        raise
    return (
        connection,
        response,
        {
            "sse_event": event[:500],
            "active_proofs": active,
            "network": network,
        },
    )


def parse_network_fences(
    text: str, expected: Iterable[str]
) -> dict[str, dict[str, Any]]:
    lines = text.splitlines()
    first_fence: dict[str, int] = {}
    effects: dict[str, list[int]] = {}
    for index, line in enumerate(lines, 1):
        if match := ENGINE_FENCE.search(line):
            first_fence.setdefault(match.group(1), index)
        if match := HEARTBEAT_EFFECT.search(line):
            effects.setdefault(match.group(1), []).append(index)

    result: dict[str, dict[str, Any]] = {}
    for label in expected:
        fence = first_fence.get(label)
        post = (
            []
            if fence is None
            else [line for line in effects.get(label, []) if line > fence]
        )
        result[label] = {
            "engine_fence_line": fence,
            "heartbeat_effect_lines": effects.get(label, []),
            "post_fence_heartbeat_lines": post,
        }
    return result


def fences_pass(fences: dict[str, dict[str, Any]]) -> tuple[bool, str | None]:
    missing = [
        label for label, item in fences.items() if item["engine_fence_line"] is None
    ]
    post = {
        label: item["post_fence_heartbeat_lines"]
        for label, item in fences.items()
        if item["post_fence_heartbeat_lines"]
    }
    if missing:
        return False, f"missing engine drain fence for networks: {', '.join(missing)}"
    if post:
        return False, f"post-fence validation heartbeat effects: {post}"
    return True, None


def server_forced_shutdown(log_text: str) -> bool:
    return SERVICE_FORCED in log_text


def record_shutdown_event(
    events: list[dict[str, Any]], action: str, reason: str
) -> None:
    events.append(
        {
            "action": action,
            "reason": reason,
            "monotonic_ns": time.monotonic_ns(),
        }
    )


def force_cleanup(
    running: RunningProcess,
    events: list[dict[str, Any]],
    reason: str,
) -> tuple[bool, int | None, str | None]:
    proc = running.proc
    code = proc.poll()
    if code is not None:
        return False, code, None

    forced = True
    record_shutdown_event(events, "SIGTERM", reason)
    try:
        os.kill(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        return forced, proc.poll(), "process exited before SIGTERM delivery"
    except OSError as exc:
        return forced, proc.poll(), f"SIGTERM failed: {exc}"

    try:
        return forced, proc.wait(timeout=5.0), None
    except subprocess.TimeoutExpired:
        record_shutdown_event(events, "SIGKILL", f"SIGTERM did not exit: {reason}")
        try:
            os.kill(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            return forced, proc.poll(), None
        except OSError as exc:
            return forced, proc.poll(), f"SIGKILL failed: {exc}"
        try:
            return forced, proc.wait(timeout=5.0), None
        except subprocess.TimeoutExpired:
            return forced, proc.poll(), "process remained alive after SIGKILL"


def terminate_after_budget(
    running: RunningProcess,
    budget: float,
    events: list[dict[str, Any]],
) -> tuple[float | None, bool, bool, int | None, str | None]:
    if (code := running.proc.poll()) is not None:
        return None, False, False, code, "process exited before SIGINT delivery"

    record_shutdown_event(events, "SIGINT", "measured graceful shutdown")
    try:
        os.kill(running.proc.pid, signal.SIGINT)
    except ProcessLookupError:
        return (
            None,
            False,
            False,
            running.proc.poll(),
            "process exited before SIGINT delivery",
        )
    except OSError as exc:
        return None, False, False, running.proc.poll(), f"SIGINT failed: {exc}"
    signalled_ns = time.monotonic_ns()

    try:
        exit_code = running.proc.wait(timeout=budget)
        elapsed_ms = (time.monotonic_ns() - signalled_ns) / 1_000_000.0
        return elapsed_ms, exit_code == 0, False, exit_code, None
    except subprocess.TimeoutExpired:
        elapsed_ms = (time.monotonic_ns() - signalled_ns) / 1_000_000.0

    forced, exit_code, cleanup_error = force_cleanup(
        running, events, f"exceeded graceful budget of {budget}s"
    )
    error = f"exceeded graceful budget of {budget}s"
    if cleanup_error:
        error += f"; cleanup: {cleanup_error}"
    return (
        elapsed_ms,
        False,
        forced,
        exit_code,
        error,
    )


def append_row(path: Path, row: RunRow) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(asdict(row), sort_keys=True) + "\n")
        handle.flush()


def run_shutdown_case(
    *,
    args: argparse.Namespace,
    case: str,
    threads: int,
    ordinal: int,
    artifact_root: Path,
) -> RunRow:
    run_dir = artifact_root / f"{case}-t{threads}-r{ordinal:03d}"
    running: RunningProcess | None = None
    resources: list[Any] = []
    build_id = f"unresolved:{case}:t{threads}:r{ordinal}"
    labels: list[str] = []
    log_labels: list[str] = []
    witness: dict[str, Any] = {}
    shutdown_events: list[dict[str, Any]] = []
    status = "failed"
    error: str | None = None
    elapsed_ms: float | None = None
    graceful = False
    forced = False
    exit_code: int | None = None
    fences: dict[str, dict[str, Any]] = {}
    try:
        port = find_free_port(args.host)
        running = start_server(
            xprv=args.xprv,
            host=args.host,
            port=port,
            primary_network=args.network_ids[0],
            network_ids=args.network_ids,
            threads=threads,
            run_dir=run_dir,
            trace_shutdown=args.trace_shutdown,
        )
        health = wait_for_health(running, args.startup_timeout)
        raw_build_id = health.get("build_id")
        if not isinstance(raw_build_id, str) or not raw_build_id.strip():
            raise RuntimeError("/health has no non-empty string build_id")
        build_id = raw_build_id
        labels = enabled_labels(health)
        log_labels = enabled_log_labels(health)
        if case == "idle":
            witness = {"health_status": 200}
        elif case == "keepalive":
            connection, witness = establish_keepalive(args.host, port)
            resources.append(connection)
        elif case == "active-proof":
            if not args.tx_hash:
                raise BoundaryUnavailable("--tx-hash was not supplied")
            proof_network = args.proof_network or network_health_slug(
                args.network_ids[0]
            )
            if proof_network not in labels:
                raise BoundaryUnavailable(
                    f"proof network {proof_network!r} is not enabled; enabled={labels}"
                )
            connection, response, witness = establish_active_proof(
                host=args.host,
                port=port,
                base_url=running.base_url,
                network=proof_network,
                tx_hash=args.tx_hash,
                timeout=args.proof_timeout,
            )
            resources.extend([response, connection])
        else:
            raise AssertionError(f"unknown shutdown case {case}")

        elapsed_ms, graceful, forced, exit_code, error = terminate_after_budget(
            running, args.graceful_budget, shutdown_events
        )
        if error is None and not graceful:
            error = f"xprv exited with status {exit_code} after SIGINT"
        running.close_log()
        text = running.log_path.read_text(encoding="utf-8", errors="replace")
        if server_forced_shutdown(text):
            graceful = False
            forced = True
            error = "server forced shutdown after its service drain deadline"
        fences = parse_network_fences(text, log_labels)
        fence_ok, fence_error = fences_pass(fences)
        if error is None:
            error = fence_error
        status = "passed" if graceful and fence_ok else "failed"
    except BoundaryUnavailable as exc:
        status = "unavailable"
        error = str(exc)
    except Exception as exc:  # noqa: BLE001 - every run must publish a result row
        error = f"{type(exc).__name__}: {exc}"
    finally:
        for resource in resources:
            with contextlib.suppress(Exception):
                resource.close()
        if running is not None:
            cleanup_forced, cleanup_code, cleanup_error = force_cleanup(
                running, shutdown_events, f"cleanup after {status} run"
            )
            forced = forced or cleanup_forced
            if exit_code is None:
                exit_code = cleanup_code
            if cleanup_error:
                error = f"{error}; cleanup: {cleanup_error}" if error else cleanup_error
            running.close_log()

    return RunRow(
        case=case,
        threads=threads,
        run=ordinal,
        runs=args.runs,
        build_id=build_id,
        networks=labels,
        signal_to_exit_ms=elapsed_ms,
        graceful=graceful,
        forced=forced,
        graceful_budget_seconds=args.graceful_budget,
        boundary_witness=witness,
        shutdown_events=shutdown_events,
        log_path=str(running.log_path if running else run_dir / "server.log"),
        exit_code=exit_code,
        network_fences=fences,
        status=status,
        error=error,
    )


def run_bind_conflict(args: argparse.Namespace, artifact_root: Path) -> RunRow:
    run_dir = artifact_root / "bind-conflict"
    run_dir.mkdir(parents=True, exist_ok=True)
    port = find_free_port(args.host)
    log_path = run_dir / "server.log"
    shutdown_events: list[dict[str, Any]] = []
    return_code: int | None = None
    forced = False
    launch_error: str | None = None
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as owner:
        owner.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        owner.bind((args.host, port))
        owner.listen(1)
        with log_path.open("wb") as log_file:
            env = os.environ.copy()
            env["XPRV_NETWORKS"] = ",".join(str(item) for item in args.network_ids)
            command = [
                str(args.xprv),
                "serve",
                "--bind",
                f"{args.host}:{port}",
                "--network",
                str(args.network_ids[0]),
                "--peer-cache-path",
                str(run_dir / "peer-endpoints.sqlite3"),
            ]
            try:
                proc = subprocess.Popen(
                    command,
                    cwd=Path.cwd(),
                    env=env,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                )
                try:
                    return_code = proc.wait(timeout=args.startup_timeout)
                except subprocess.TimeoutExpired:
                    running = RunningProcess(
                        proc=proc,
                        log_file=log_file,
                        log_path=log_path,
                        base_url="",
                    )
                    forced, return_code, cleanup_error = force_cleanup(
                        running,
                        shutdown_events,
                        "bind-conflict process did not exit",
                    )
                    launch_error = "bind-conflict process did not exit"
                    if cleanup_error:
                        launch_error += f"; cleanup: {cleanup_error}"
            except OSError as exc:
                launch_error = f"failed to launch xprv: {exc}"

    text = log_path.read_text(encoding="utf-8", errors="replace")
    endpoint = f"{args.host}:{port}"
    bad_start = [token for token in STARTUP_FORBIDDEN if token in text]
    checks = {
        "exit_code_is_2": return_code == 2,
        "endpoint_named": endpoint in text,
        "no_abort_banner": "libc++abi" not in text and "terminate called" not in text,
        "no_engine_start_evidence": not bad_start,
    }
    passed = all(checks.values())
    return RunRow(
        case="bind-conflict",
        threads=0,
        run=1,
        runs=1,
        build_id="not-started",
        networks=[],
        signal_to_exit_ms=None,
        graceful=passed,
        forced=forced,
        graceful_budget_seconds=args.graceful_budget,
        boundary_witness={"endpoint": endpoint, "checks": checks},
        shutdown_events=shutdown_events,
        log_path=str(log_path),
        exit_code=return_code,
        network_fences={},
        status="passed" if passed else "failed",
        error=None
        if passed
        else launch_error
        or f"bind-conflict assertions failed; startup evidence={bad_start}",
    )


def nearest_rank_p99(values: list[float]) -> float | None:
    if len(values) < 100:
        return None
    ordered = sorted(values)
    return ordered[math.ceil(0.99 * len(ordered)) - 1]


def summarize(rows: list[RunRow]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, int, str], list[RunRow]] = {}
    for row in rows:
        groups.setdefault((row.case, row.threads, row.build_id), []).append(row)
    result = []
    for (case, threads, build_id), group_rows in sorted(groups.items()):
        values = [
            row.signal_to_exit_ms
            for row in group_rows
            if row.graceful
            and not row.forced
            and row.exit_code == 0
            and row.signal_to_exit_ms is not None
        ]
        result.append(
            {
                "case": case,
                "threads": threads,
                "build_id": build_id,
                "rows": len(group_rows),
                "passed_rows": sum(row.status == "passed" for row in group_rows),
                "failed_rows": sum(row.status == "failed" for row in group_rows),
                "unavailable_rows": sum(
                    row.status == "unavailable" for row in group_rows
                ),
                "forced_rows": sum(row.forced for row in group_rows),
                "graceful_runs": len(values),
                "max_ms": max(values) if values else None,
                "mean_ms": sum(values) / len(values) if values else None,
                "p99_ms": nearest_rank_p99(values),
            }
        )
    return result


def aggregate_exit_code(rows: list[RunRow], requested_cases: list[str]) -> int:
    if any(row.status == "failed" for row in rows):
        return 1
    cases_without_pass = [
        case
        for case in requested_cases
        if not any(row.case == case and row.status == "passed" for row in rows)
    ]
    return 2 if cases_without_pass else 0


def self_test() -> None:
    assert nearest_rank_p99(list(range(99))) is None
    assert nearest_rank_p99(list(range(100))) == 98
    assert nearest_rank_p99(list(range(101))) == 99
    log = """[engine] [xrpl] Engine draining...
[quorum-tracker] [xahau] no quorum for 1s
[engine] [xahau] Engine draining...
[quorum-tracker] [xrpl] no quorum for 1s"""
    parsed = parse_network_fences(log, ["xrpl", "xahau"])
    assert parsed["xrpl"]["post_fence_heartbeat_lines"] == [4]
    assert parsed["xahau"]["post_fence_heartbeat_lines"] == []
    ok, reason = fences_pass(parsed)
    assert not ok and reason and "xrpl" in reason
    clean = parse_network_fences(
        "[engine] [xrpl] Engine draining...\n[engine] [xahau] Engine draining...",
        ["xrpl", "xahau"],
    )
    assert fences_pass(clean) == (True, None)
    missing = parse_network_fences(
        "[engine] [xrpl] Engine draining...", ["xrpl", "xahau"]
    )
    assert fences_pass(missing)[0] is False
    assert network_health_slug(0) == "xrpl-mainnet"
    assert server_forced_shutdown(
        "Service shutdown forced before all drains completed: "
        "service shutdown deadline expired"
    )
    assert not server_forced_shutdown("Graceful service drain complete")

    def row(
        *,
        build_id: str,
        status: str,
        duration: float | None,
        graceful: bool,
        forced: bool,
    ) -> RunRow:
        return RunRow(
            case="idle",
            threads=1,
            run=1,
            runs=1,
            build_id=build_id,
            networks=["xrpl-mainnet"],
            signal_to_exit_ms=duration,
            graceful=graceful,
            forced=forced,
            graceful_budget_seconds=35.0,
            boundary_witness={},
            shutdown_events=[],
            log_path="test.log",
            exit_code=0 if graceful else -signal.SIGTERM,
            network_fences={},
            status=status,
        )

    rows = [
        row(build_id="a", status="passed", duration=10.0, graceful=True, forced=False),
        row(build_id="a", status="failed", duration=20.0, graceful=True, forced=False),
        row(build_id="a", status="failed", duration=30.0, graceful=False, forced=True),
        row(build_id="b", status="passed", duration=40.0, graceful=True, forced=False),
    ]
    summaries = summarize(rows)
    summary_a = next(item for item in summaries if item["build_id"] == "a")
    summary_b = next(item for item in summaries if item["build_id"] == "b")
    assert summary_a["graceful_runs"] == 2 and summary_a["max_ms"] == 20.0
    assert summary_a["forced_rows"] == 1
    assert summary_b["graceful_runs"] == 1 and summary_b["max_ms"] == 40.0
    unavailable = row(
        build_id="a",
        status="unavailable",
        duration=None,
        graceful=False,
        forced=True,
    )
    assert aggregate_exit_code([unavailable], ["idle"]) == 2
    assert aggregate_exit_code([rows[0], unavailable], ["idle"]) == 0
    assert aggregate_exit_code(rows, ["idle"]) == 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--xprv", type=Path, default=Path("build/src/xprv/xprv"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument(
        "--networks",
        default="0,21337",
        help="Comma-separated network IDs; first is the primary network",
    )
    parser.add_argument(
        "--threads", default="1,4", help="Comma-separated IO runner counts"
    )
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument(
        "--cases",
        default="bind-conflict,idle,keepalive",
        help="Comma-separated: bind-conflict,idle,keepalive,active-proof",
    )
    parser.add_argument("--tx-hash")
    parser.add_argument("--proof-network", help="Health network slug for active-proof")
    parser.add_argument("--startup-timeout", type=float, default=20.0)
    parser.add_argument("--proof-timeout", type=float, default=20.0)
    parser.add_argument("--graceful-budget", type=float, default=35.0)
    parser.add_argument(
        "--trace-shutdown",
        action="store_true",
        help="enable engine, VL, peer-set, and peer-crawl DEBUG diagnostics",
    )
    parser.add_argument("--artifact-dir", type=Path)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    args.network_ids = [int(item) for item in args.networks.split(",") if item]
    args.thread_counts = [int(item) for item in args.threads.split(",") if item]
    args.case_names = [item for item in args.cases.split(",") if item]
    allowed = {"bind-conflict", "idle", "keepalive", "active-proof"}
    unknown = set(args.case_names) - allowed
    if unknown:
        parser.error(f"unknown cases: {', '.join(sorted(unknown))}")
    if not args.case_names:
        parser.error("--cases must contain at least one case")
    if not args.network_ids:
        parser.error("--networks must contain at least one network ID")
    if len(set(args.network_ids)) != len(args.network_ids):
        parser.error("--networks must not contain duplicates")
    if not args.thread_counts:
        parser.error("--threads must contain at least one runner count")
    if any(item < 1 for item in args.thread_counts):
        parser.error("--threads values must be positive")
    if len(set(args.thread_counts)) != len(args.thread_counts):
        parser.error("--threads must not contain duplicates")
    if len(set(args.case_names)) != len(args.case_names):
        parser.error("--cases must not contain duplicates")
    if args.runs < 1:
        parser.error("--runs must be positive")
    for name in ("startup_timeout", "proof_timeout", "graceful_budget"):
        value = getattr(args, name)
        if not math.isfinite(value) or value <= 0:
            parser.error(f"--{name.replace('_', '-')} must be finite and positive")
    return args


def main() -> int:
    args = parse_args()
    if args.self_test:
        self_test()
        print("xprv-shutdown-smoke self-test: PASS")
        return 0
    if not args.xprv.is_file():
        raise SystemExit(f"xprv binary not found: {args.xprv}")

    if args.artifact_dir is not None:
        artifact_root = args.artifact_dir
        if artifact_root.exists() and any(artifact_root.iterdir()):
            raise SystemExit(f"artifact directory is not empty: {artifact_root}")
    else:
        artifact_root = Path(tempfile.mkdtemp(prefix="xprv-shutdown-smoke."))
    artifact_root.mkdir(parents=True, exist_ok=True)
    jsonl_path = artifact_root / "results.jsonl"
    rows: list[RunRow] = []
    if "bind-conflict" in args.case_names:
        row = run_bind_conflict(args, artifact_root)
        append_row(jsonl_path, row)
        rows.append(row)
        print(json.dumps(asdict(row), sort_keys=True), flush=True)

    for case in args.case_names:
        if case == "bind-conflict":
            continue
        for threads in args.thread_counts:
            for ordinal in range(1, args.runs + 1):
                row = run_shutdown_case(
                    args=args,
                    case=case,
                    threads=threads,
                    ordinal=ordinal,
                    artifact_root=artifact_root,
                )
                append_row(jsonl_path, row)
                rows.append(row)
                print(json.dumps(asdict(row), sort_keys=True), flush=True)

    summary = summarize(rows)
    (artifact_root / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"artifacts: {artifact_root}")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return aggregate_exit_code(rows, args.case_names)


if __name__ == "__main__":
    raise SystemExit(main())
