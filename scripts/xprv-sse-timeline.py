#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# ///
"""Capture a timestamped timeline from the xprv proof SSE endpoint."""

from __future__ import annotations

import argparse
import http.client
import json
import socket
import sys
import time
import urllib.parse
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class TimelineEvent:
    elapsed_seconds: float
    delta_seconds: float
    event_type: str
    detail: str
    payload: Any


def event_detail(payload: Any) -> tuple[str, str]:
    if not isinstance(payload, dict):
        return "data", repr(payload)

    event_type = str(payload.get("type", "data"))
    if event_type == "log":
        return event_type, str(payload.get("msg", ""))
    if event_type == "step":
        step = payload.get("step")
        if isinstance(step, dict):
            parts = [f"index={payload.get('index', '?')}"]
            if step.get("type") is not None:
                parts.append(f"type={step['type']}")
            if step.get("tree") is not None:
                parts.append(f"tree={step['tree']}")
            return event_type, " ".join(parts)
        return event_type, f"index={payload.get('index', '?')}"
    if event_type == "done":
        return event_type, f"network_id={payload.get('network_id', '?')}"
    if event_type == "error":
        return event_type, str(payload.get("error", "unknown error"))
    return event_type, json.dumps(payload, separators=(",", ":"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Timestamp every event emitted by xprv's /prove SSE path."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:18080")
    parser.add_argument("--tx", required=True, help="Transaction hash to prove")
    parser.add_argument("--ledger-index", type=int)
    parser.add_argument("--max-anchor-age", type=int)
    parser.add_argument("--xrid", default=f"sse-{uuid.uuid4().hex[:8]}")
    parser.add_argument(
        "--timeout",
        type=float,
        default=90.0,
        help="Overall request deadline in seconds",
    )
    parser.add_argument(
        "--inactivity-timeout",
        type=float,
        default=15.0,
        help="Maximum seconds without receiving an SSE line",
    )
    parser.add_argument(
        "--json-output",
        type=Path,
        help="Optional path for the complete machine-readable timeline",
    )
    return parser.parse_args()


def build_path(args: argparse.Namespace) -> str:
    query: dict[str, str] = {"tx": args.tx, "xrid": args.xrid}
    if args.ledger_index is not None:
        query["ledger_index"] = str(args.ledger_index)
    if args.max_anchor_age is not None:
        query["max_anchor_age"] = str(args.max_anchor_age)
    return "/prove?" + urllib.parse.urlencode(query)


def make_connection(
    parsed: urllib.parse.SplitResult, timeout: float
) -> http.client.HTTPConnection:
    if parsed.scheme == "https":
        return http.client.HTTPSConnection(
            parsed.hostname, parsed.port or 443, timeout=timeout
        )
    if parsed.scheme == "http":
        return http.client.HTTPConnection(
            parsed.hostname, parsed.port or 80, timeout=timeout
        )
    raise ValueError(f"unsupported URL scheme: {parsed.scheme!r}")


def write_json(
    path: Path | None,
    *,
    args: argparse.Namespace,
    status: int | None,
    headers_elapsed: float | None,
    terminal: str,
    events: list[TimelineEvent],
) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    body = {
        "base_url": args.base_url,
        "tx": args.tx,
        "ledger_index": args.ledger_index,
        "max_anchor_age": args.max_anchor_age,
        "xrid": args.xrid,
        "http_status": status,
        "headers_elapsed_seconds": headers_elapsed,
        "terminal": terminal,
        "events": [asdict(event) for event in events],
    }
    path.write_text(json.dumps(body, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    if args.timeout <= 0 or args.inactivity_timeout <= 0:
        raise SystemExit("timeouts must be greater than zero")
    parsed = urllib.parse.urlsplit(args.base_url)
    if not parsed.hostname:
        raise SystemExit("--base-url must include a hostname")

    request_path = (parsed.path.rstrip("/") if parsed.path else "") + build_path(args)
    started = time.perf_counter()
    previous = started
    headers_elapsed: float | None = None
    status: int | None = None
    terminal = "request-failed"
    events: list[TimelineEvent] = []
    connection = make_connection(
        parsed, min(args.inactivity_timeout, args.timeout)
    )

    print(f"xrid={args.xrid} path={request_path}", flush=True)
    try:
        connection.request(
            "GET",
            request_path,
            headers={"Accept": "text/event-stream", "Cache-Control": "no-cache"},
        )
        response = connection.getresponse()
        status = response.status
        now = time.perf_counter()
        headers_elapsed = now - started
        previous = now
        content_type = response.getheader("Content-Type", "")
        print(
            f"+{headers_elapsed:8.3f}s headers status={status} "
            f"content-type={content_type!r}",
            flush=True,
        )
        if status != 200 or "text/event-stream" not in content_type.lower():
            body = response.read(4096).decode("utf-8", errors="replace")
            terminal = "invalid-response"
            print(body, file=sys.stderr)
            return 2

        data_lines: list[str] = []
        while True:
            elapsed = time.perf_counter() - started
            if elapsed >= args.timeout:
                terminal = "overall-timeout"
                print(
                    f"+{elapsed:8.3f}s timeout overall={args.timeout:.3f}s",
                    file=sys.stderr,
                    flush=True,
                )
                return 3

            remaining = args.timeout - elapsed
            if connection.sock is not None:
                connection.sock.settimeout(
                    min(args.inactivity_timeout, remaining)
                )

            try:
                raw_line = response.readline()
            except (TimeoutError, socket.timeout):
                elapsed = time.perf_counter() - started
                if elapsed >= args.timeout:
                    terminal = "overall-timeout"
                    print(
                        f"+{elapsed:8.3f}s timeout "
                        f"overall={args.timeout:.3f}s",
                        file=sys.stderr,
                        flush=True,
                    )
                    return 3
                terminal = "inactivity-timeout"
                print(
                    f"+{elapsed:8.3f}s timeout inactivity="
                    f"{args.inactivity_timeout:.3f}s",
                    file=sys.stderr,
                    flush=True,
                )
                return 4

            if not raw_line:
                terminal = "eof"
                print(
                    f"+{time.perf_counter() - started:8.3f}s eof",
                    file=sys.stderr,
                    flush=True,
                )
                return 5

            line = raw_line.decode("utf-8", errors="replace").rstrip("\r\n")
            if line.startswith("data:"):
                data_lines.append(line[5:].lstrip())
                continue
            if line != "" or not data_lines:
                continue

            data = "\n".join(data_lines)
            data_lines.clear()
            now = time.perf_counter()
            try:
                payload: Any = json.loads(data)
            except json.JSONDecodeError:
                payload = data
            event_type, detail = event_detail(payload)
            event = TimelineEvent(
                elapsed_seconds=now - started,
                delta_seconds=now - previous,
                event_type=event_type,
                detail=detail,
                payload=payload,
            )
            previous = now
            events.append(event)
            print(
                f"+{event.elapsed_seconds:8.3f}s "
                f"Δ{event.delta_seconds:7.3f}s {event_type:<6} {detail}",
                flush=True,
            )

            if event_type == "done":
                terminal = "done"
                return 0
            if event_type == "error":
                terminal = "error"
                return 6
    except KeyboardInterrupt:
        terminal = "interrupted"
        print("interrupted", file=sys.stderr)
        return 130
    except (OSError, http.client.HTTPException) as exc:
        terminal = "request-failed"
        print(f"request failed: {exc}", file=sys.stderr)
        return 7
    finally:
        connection.close()
        write_json(
            args.json_output,
            args=args,
            status=status,
            headers_elapsed=headers_elapsed,
            terminal=terminal,
            events=events,
        )


if __name__ == "__main__":
    raise SystemExit(main())
