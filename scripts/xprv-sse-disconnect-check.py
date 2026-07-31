#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
"""Disconnect one live SSE proof at an observed suspension boundary.

The check refuses to infer that it hit the intended path from timing alone. It
waits for an exact SSE payload marker, proves the server counted the proof as
active, closes that connection, and then requires the active-proof count to
return to its baseline within a bounded interval.
"""

from __future__ import annotations

import argparse
import http.client
import json
import time
import urllib.parse
import urllib.request
from typing import Any


def health(base_url: str) -> dict[str, Any]:
    with urllib.request.urlopen(f"{base_url}/health", timeout=2.0) as response:
        payload = json.load(response)
    if not isinstance(payload, dict):
        raise RuntimeError("/health did not return an object")
    return payload


def active_proofs(base_url: str) -> int:
    payload = health(base_url)
    requests = payload.get("requests")
    if not isinstance(requests, dict):
        raise RuntimeError("/health has no requests object")
    active = requests.get("active_proofs")
    if not isinstance(active, int):
        raise RuntimeError("/health requests.active_proofs is not an integer")
    return active


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:18099")
    parser.add_argument("--network", required=True)
    parser.add_argument("--tx", required=True)
    parser.add_argument(
        "--wait-for",
        default="searching for peer advertising ledger",
        help="Exact text that must appear in an SSE data line before disconnect",
    )
    parser.add_argument("--stream-timeout", type=float, default=20.0)
    parser.add_argument("--settle-timeout", type=float, default=2.0)
    args = parser.parse_args()

    parsed = urllib.parse.urlsplit(args.base_url)
    if parsed.scheme != "http" or not parsed.hostname:
        parser.error("--base-url must be an http URL with a hostname")

    baseline = active_proofs(args.base_url)
    route = (
        f"/network/{urllib.parse.quote(args.network, safe='')}/prove?"
        + urllib.parse.urlencode(
            {
                "tx": args.tx,
                "xrid": "sse-disconnect-check",
            }
        )
    )
    connection = http.client.HTTPConnection(
        parsed.hostname,
        parsed.port or 80,
        timeout=args.stream_timeout,
    )
    response: http.client.HTTPResponse | None = None
    started = time.monotonic()
    marker_line = ""
    try:
        connection.request(
            "GET",
            route,
            headers={
                "Accept": "text/event-stream",
                "Connection": "close",
            },
        )
        response = connection.getresponse()
        if response.status != 200:
            raise RuntimeError(f"SSE response status was {response.status}")
        content_type = response.getheader("content-type", "")
        if "text/event-stream" not in content_type:
            raise RuntimeError(
                f"SSE response content-type was {content_type!r}"
            )

        deadline = started + args.stream_timeout
        while time.monotonic() < deadline:
            line = response.readline()
            if not line:
                break
            text = line.decode("utf-8", errors="replace")
            if args.wait_for in text:
                marker_line = text.strip()
                break
        if not marker_line:
            raise RuntimeError(
                f"SSE stream ended without marker {args.wait_for!r}"
            )

        suspended_active = active_proofs(args.base_url)
        if suspended_active <= baseline:
            raise RuntimeError(
                "marker was observed but active_proofs did not rise: "
                f"baseline={baseline}, suspended={suspended_active}"
            )
    finally:
        if response is not None:
            response.close()
        connection.close()

    disconnected = time.monotonic()
    deadline = disconnected + args.settle_timeout
    settled_active = active_proofs(args.base_url)
    while settled_active > baseline and time.monotonic() < deadline:
        time.sleep(0.025)
        settled_active = active_proofs(args.base_url)

    settled = time.monotonic()
    if settled_active > baseline:
        raise RuntimeError(
            "active proof did not retire after SSE disconnect: "
            f"baseline={baseline}, active={settled_active}, "
            f"waited={settled - disconnected:.3f}s"
        )

    print(
        json.dumps(
            {
                "marker": args.wait_for,
                "marker_elapsed_seconds": disconnected - started,
                "baseline_active_proofs": baseline,
                "suspended_active_proofs": suspended_active,
                "settled_active_proofs": settled_active,
                "disconnect_settle_seconds": settled - disconnected,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
